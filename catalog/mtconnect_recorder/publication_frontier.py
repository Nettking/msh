"""Durable bounded discovery for recorder publication.

The raw MTConnect archive remains primary evidence.  This frontier is only a
small discovery index telling the publication reconciler which already-written
raw batches may still need representation in the Federation outbox.

Normal publication enumerates this backlog rather than recursively scanning the
lifetime raw archive.  Records are retired only after the corresponding batch is
durably represented by the outbox.  A separate initialized marker lets upgraded
installations pay one explicit legacy archive scan before switching to the
incremental path.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .model import MtconnectProtocolError, RawBatchRef, _slug, _write_json_atomic
from .storage import DurableRecorderStore, _confined_storage_path

PENDING_PUBLICATION_SCHEMA = "fcp.mtconnect.pending_publication.v1"
PUBLICATION_FRONTIER_STATE_SCHEMA = "fcp.mtconnect.publication_frontier.v1"
PUBLICATION_FRONTIER_RECORD_MAX_BYTES = 4096
PUBLICATION_FRONTIER_RECORD_INODES = 1
PUBLICATION_FRONTIER_STATE_MAX_BYTES = 1024
PUBLICATION_FRONTIER_STATE_INODES = 1


def _required_text(value: object, field: str, *, max_bytes: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MtconnectProtocolError(f"Publication frontier {field} must be non-empty text.")
    normalized = value.strip()
    if len(normalized.encode("utf-8")) > max_bytes or any(ord(c) < 32 for c in normalized):
        raise MtconnectProtocolError(f"Publication frontier {field} is not bounded printable text.")
    return normalized


def _basename(value: object, field: str) -> str:
    text = _required_text(value, field, max_bytes=255)
    if Path(text).name != text or text in {".", ".."}:
        raise MtconnectProtocolError(f"Publication frontier {field} must be one file name.")
    return text


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MtconnectProtocolError(f"Publication frontier {field} must be a non-negative integer.")
    return value


def _digest(value: object) -> str:
    text = _required_text(value, "raw_sha256", max_bytes=64).lower()
    if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
        raise MtconnectProtocolError("Publication frontier raw_sha256 is invalid.")
    return text


def _day(value: object) -> str:
    text = _required_text(value, "day", max_bytes=10)
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise MtconnectProtocolError("Publication frontier day is invalid.")
    return text


@dataclass(frozen=True)
class PendingPublication:
    """One bounded discovery record and its reconstructed raw identity."""

    source_name: str
    archive_source_name: str
    instance_id: int
    ref: RawBatchRef
    record_path: Path


class RecorderPublicationFrontier:
    """Own pending-publication discovery without making it evidence authority."""

    def __init__(self, store: DurableRecorderStore) -> None:
        self.store = store
        self.pending_root = store.root / "publication_pending"
        self.state_root = store.root / "publication_state"

    def _source_pending_root(self, source_name: str, instance_id: int) -> Path:
        return _confined_storage_path(
            self.pending_root,
            _slug(source_name),
            str(int(instance_id)),
        )

    def _state_path(self, source_name: str, instance_id: int) -> Path:
        return _confined_storage_path(
            self.state_root,
            _slug(source_name),
            str(int(instance_id)),
            ".initialized.json",
        )

    @staticmethod
    def _record_name(ref: RawBatchRef) -> str:
        return (
            f"seq-{int(ref.first_sequence)}-{int(ref.last_sequence)}-"
            f"next-{int(ref.next_sequence)}-{_digest(ref.raw_sha256)}.json"
        )

    def _record_path(self, source_name: str, instance_id: int, ref: RawBatchRef) -> Path:
        return _confined_storage_path(
            self._source_pending_root(source_name, instance_id),
            self._record_name(ref),
        )

    def mark_pending(
        self,
        *,
        source_name: str,
        archive_source_name: str,
        instance_id: int,
        ref: RawBatchRef,
    ) -> Path:
        """Durably expose one raw batch to incremental publication discovery."""

        source = _required_text(source_name, "source_name")
        archive_source = _required_text(archive_source_name, "archive_source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        day = _day(ref.manifest_path.parent.name)
        raw_name = _basename(ref.raw_path.name, "raw_name")
        manifest_name = _basename(ref.manifest_path.name, "manifest_name")
        payload = {
            "schema": PENDING_PUBLICATION_SCHEMA,
            "source_name": source,
            "archive_source_name": archive_source,
            "agent_instance_id": instance_id,
            "first_sequence": int(ref.first_sequence),
            "last_sequence": int(ref.last_sequence),
            "next_sequence": int(ref.next_sequence),
            "observation_count": int(ref.observation_count),
            "raw_sha256": _digest(ref.raw_sha256),
            "manifest_schema": _required_text(ref.manifest_schema, "manifest_schema"),
            "received_at": ref.received_at,
            "day": day,
            "raw_name": raw_name,
            "manifest_name": manifest_name,
        }
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
        if len(encoded) > PUBLICATION_FRONTIER_RECORD_MAX_BYTES:
            raise MtconnectProtocolError("Publication frontier record exceeds its fixed bound.")
        path = self._record_path(source, instance_id, ref)
        _write_json_atomic(path, payload)
        return path

    def _decode(self, path: Path, *, source_name: str, instance_id: int) -> PendingPublication:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MtconnectProtocolError("Publication frontier record is unreadable.") from exc
        if not isinstance(payload, dict) or payload.get("schema") != PENDING_PUBLICATION_SCHEMA:
            raise MtconnectProtocolError("Publication frontier record schema is unsupported.")
        source = _required_text(payload.get("source_name"), "source_name")
        archive_source = _required_text(payload.get("archive_source_name"), "archive_source_name")
        encoded_instance = _positive_int(payload.get("agent_instance_id"), "agent_instance_id")
        if source != source_name or encoded_instance != instance_id:
            raise MtconnectProtocolError("Publication frontier record identity mismatch.")
        first = _positive_int(payload.get("first_sequence"), "first_sequence")
        last = _positive_int(payload.get("last_sequence"), "last_sequence")
        next_sequence = _positive_int(payload.get("next_sequence"), "next_sequence")
        observation_count = _positive_int(payload.get("observation_count"), "observation_count")
        if first > last or next_sequence <= last or observation_count <= 0:
            raise MtconnectProtocolError("Publication frontier sequence identity is invalid.")
        raw_sha256 = _digest(payload.get("raw_sha256"))
        day = _day(payload.get("day"))
        raw_name = _basename(payload.get("raw_name"), "raw_name")
        manifest_name = _basename(payload.get("manifest_name"), "manifest_name")
        archive_root = _confined_storage_path(
            self.store.raw_root,
            _slug(archive_source),
            str(instance_id),
            day,
        )
        raw_path = _confined_storage_path(archive_root, raw_name)
        manifest_path = _confined_storage_path(archive_root, manifest_name)
        received_at = payload.get("received_at")
        if received_at is not None and not isinstance(received_at, str):
            raise MtconnectProtocolError("Publication frontier received_at is invalid.")
        ref = RawBatchRef(
            raw_path=raw_path,
            manifest_path=manifest_path,
            raw_sha256=raw_sha256,
            requested_from=first,
            first_sequence=first,
            last_sequence=last,
            next_sequence=next_sequence,
            observation_count=observation_count,
            manifest_schema=_required_text(payload.get("manifest_schema"), "manifest_schema"),
            received_at=received_at,
            source_name=archive_source,
        )
        if path.name != self._record_name(ref):
            raise MtconnectProtocolError("Publication frontier file name does not match its identity.")
        return PendingPublication(
            source_name=source,
            archive_source_name=archive_source,
            instance_id=instance_id,
            ref=ref,
            record_path=path,
        )

    def pending(self, *, source_name: str, instance_id: int) -> tuple[PendingPublication, ...]:
        """Enumerate only unreconciled discovery records for one source/instance."""

        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        root = self._source_pending_root(source, instance_id)
        if not root.exists():
            return ()
        values = [
            self._decode(path, source_name=source, instance_id=instance_id)
            for path in sorted(root.glob("seq-*.json"))
        ]
        values.sort(
            key=lambda item: (
                item.ref.first_sequence,
                item.ref.last_sequence,
                item.ref.next_sequence,
                item.ref.raw_sha256,
                item.archive_source_name,
            )
        )
        return tuple(values)

    def retire(self, pending: PendingPublication) -> None:
        """Remove only the discovery record; never remove recorder evidence."""

        expected = self._record_path(pending.source_name, pending.instance_id, pending.ref)
        if expected != pending.record_path:
            raise MtconnectProtocolError("Publication frontier retirement identity mismatch.")
        expected.unlink(missing_ok=True)

    def initialized(self, *, source_name: str, instance_id: int) -> bool:
        path = self._state_path(source_name, instance_id)
        if not path.exists():
            return False
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MtconnectProtocolError("Publication frontier state is unreadable.") from exc
        return bool(
            isinstance(payload, dict)
            and payload.get("schema") == PUBLICATION_FRONTIER_STATE_SCHEMA
            and payload.get("state") == "initialized"
            and payload.get("source_name") == source_name
            and payload.get("agent_instance_id") == instance_id
        )

    def mark_initialized(self, *, source_name: str, instance_id: int) -> Path:
        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        payload = {
            "schema": PUBLICATION_FRONTIER_STATE_SCHEMA,
            "state": "initialized",
            "source_name": source,
            "agent_instance_id": instance_id,
        }
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
        if len(encoded) > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
            raise MtconnectProtocolError("Publication frontier state exceeds its fixed bound.")
        path = self._state_path(source, instance_id)
        _write_json_atomic(path, payload)
        return path


__all__ = [
    "PENDING_PUBLICATION_SCHEMA",
    "PUBLICATION_FRONTIER_RECORD_INODES",
    "PUBLICATION_FRONTIER_RECORD_MAX_BYTES",
    "PUBLICATION_FRONTIER_STATE_INODES",
    "PUBLICATION_FRONTIER_STATE_MAX_BYTES",
    "PUBLICATION_FRONTIER_STATE_SCHEMA",
    "PendingPublication",
    "RecorderPublicationFrontier",
]

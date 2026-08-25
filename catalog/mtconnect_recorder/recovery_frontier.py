"""Fixed-size crash-recovery frontier for recorder raw transactions.

The raw archive remains the immutable source of truth, but ordinary capture must
not recursively walk its lifetime history on every poll just to discover whether
the previous process died between raw publication and checkpoint commit.  This
module keeps one small atomic pointer per source/Agent instance to the most
recent raw transaction that started.  A stale pointer after a successful
checkpoint is harmless and is self-healed from the durable checkpoint sequence.

A missing pointer is intentionally distinguishable from an explicit ``clear``
record so upgraded installations can perform one legacy archive scan and then
migrate to bounded lookup without deleting historical evidence.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

from .model import (
    MtconnectProtocolError,
    ParsedBatch,
    RawBatchRef,
    _slug,
    _utc_now,
    _write_json_atomic,
)
from .storage import DurableRecorderStore, _confined_storage_path, _observation_storage_day

RECOVERY_FRONTIER_SCHEMA = "fcp.mtconnect.raw_recovery_frontier.v1"
RECOVERY_FRONTIER_FILENAME = ".recovery-frontier.json"
_DAY_PATTERN = re.compile(r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
_DIGEST_PATTERN = re.compile(r"\A[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class RecoveryLookup:
    """Result of one constant-path recovery lookup.

    ``initialized`` is false only when an upgraded archive has no frontier yet.
    ``ref`` is present only when the frontier describes an exact raw manifest
    whose first sequence still equals the durable checkpoint frontier.
    """

    initialized: bool
    ref: RawBatchRef | None = None


def _integer(payload: dict[str, Any], field: str, *, minimum: int = 0) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise MtconnectProtocolError(
            f"Recorder recovery frontier field {field!r} is invalid."
        )
    return value


def _component(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."}:
        raise MtconnectProtocolError(
            f"Recorder recovery frontier field {field!r} is invalid."
        )
    if Path(value).name != value or "/" in value or "\\" in value:
        raise MtconnectProtocolError(
            f"Recorder recovery frontier field {field!r} is not a path component."
        )
    return value


class RecorderRecoveryFrontier:
    """Read and atomically replace one recovery pointer per raw archive instance."""

    def __init__(self, store: DurableRecorderStore) -> None:
        self.store = store

    def path(self, *, source_name: str, instance_id: int) -> Path:
        return _confined_storage_path(
            self.store.raw_root,
            _slug(source_name),
            str(int(instance_id)),
            RECOVERY_FRONTIER_FILENAME,
        )

    def mark_pending(
        self,
        *,
        source_name: str,
        requested_from: int,
        xml_text: str,
        batch: ParsedBatch,
    ) -> Path:
        """Publish the recovery pointer before the corresponding raw write starts."""

        if not batch.observations:
            raise ValueError("Cannot prepare recovery for an empty MTConnect batch.")
        first = batch.first_observation_sequence
        last = batch.last_observation_sequence
        if first is None or last is None:
            raise ValueError("Recovery batch does not contain sequence numbers.")
        digest = sha256(xml_text.encode("utf-8")).hexdigest()
        day = _observation_storage_day(batch)
        manifest_name = (
            f"seq-{first}-{last}-next-{batch.header.next_sequence}-"
            f"{digest}.xml.manifest.json"
        )
        path = self.path(
            source_name=source_name,
            instance_id=batch.header.instance_id,
        )
        _write_json_atomic(
            path,
            {
                "schema": RECOVERY_FRONTIER_SCHEMA,
                "state": "pending",
                "source_name": source_name,
                "agent_instance_id": batch.header.instance_id,
                "requested_from": int(requested_from),
                "first_sequence": int(first),
                "last_sequence": int(last),
                "next_sequence": int(batch.header.next_sequence),
                "observation_count": len(batch.observations),
                "raw_sha256": digest,
                "day": day,
                "manifest_name": manifest_name,
                "updated_at": _utc_now(),
            },
        )
        return path

    def mark_clear(
        self,
        *,
        source_name: str,
        instance_id: int,
        next_sequence: int,
    ) -> Path:
        """Record that no raw transaction precedes the durable checkpoint frontier."""

        path = self.path(source_name=source_name, instance_id=instance_id)
        _write_json_atomic(
            path,
            {
                "schema": RECOVERY_FRONTIER_SCHEMA,
                "state": "clear",
                "source_name": source_name,
                "agent_instance_id": int(instance_id),
                "next_sequence": int(next_sequence),
                "updated_at": _utc_now(),
            },
        )
        return path

    def lookup(
        self,
        *,
        source_name: str,
        instance_id: int,
        expected: int,
    ) -> RecoveryLookup:
        """Resolve at most one crash candidate without walking archive history."""

        path = self.path(source_name=source_name, instance_id=instance_id)
        if not path.exists():
            return RecoveryLookup(initialized=False)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier is unreadable: {path}"
            ) from exc
        if not isinstance(payload, dict) or payload.get("schema") != RECOVERY_FRONTIER_SCHEMA:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier has an unsupported schema: {path}"
            )
        if payload.get("source_name") != source_name:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier source identity mismatch: {path}"
            )
        recorded_instance = _integer(payload, "agent_instance_id")
        if recorded_instance != int(instance_id):
            raise MtconnectProtocolError(
                f"Recorder recovery frontier Agent instance mismatch: {path}"
            )

        state = payload.get("state")
        if state == "clear":
            recorded_next = _integer(payload, "next_sequence")
            if recorded_next != int(expected):
                # The checkpoint is authoritative. A clear pointer has no raw
                # transaction to recover, so sequence drift is safe to heal in
                # constant work (for example after restoring a newer state file).
                self.mark_clear(
                    source_name=source_name,
                    instance_id=instance_id,
                    next_sequence=expected,
                )
            return RecoveryLookup(initialized=True)
        if state != "pending":
            raise MtconnectProtocolError(
                f"Recorder recovery frontier state is invalid: {path}"
            )

        requested_from = _integer(payload, "requested_from")
        first = _integer(payload, "first_sequence")
        last = _integer(payload, "last_sequence")
        next_sequence = _integer(payload, "next_sequence")
        observation_count = _integer(payload, "observation_count", minimum=1)
        if requested_from != first or last < first or next_sequence <= last:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier sequence bounds are invalid: {path}"
            )

        if int(expected) != first:
            if int(expected) >= next_sequence:
                # Crash after checkpoint commit but before the tiny pointer was
                # cleared. The checkpoint proves this transaction committed.
                self.mark_clear(
                    source_name=source_name,
                    instance_id=instance_id,
                    next_sequence=expected,
                )
                return RecoveryLookup(initialized=True)
            raise MtconnectProtocolError(
                "Recorder recovery frontier is ahead of or overlaps the durable "
                "checkpoint; refusing to skip an unresolved sequence range."
            )

        raw_digest = payload.get("raw_sha256")
        if not isinstance(raw_digest, str) or not _DIGEST_PATTERN.fullmatch(raw_digest):
            raise MtconnectProtocolError(
                f"Recorder recovery frontier raw digest is invalid: {path}"
            )
        day = _component(payload.get("day"), "day")
        try:
            if not _DAY_PATTERN.fullmatch(day):
                raise ValueError("day is not YYYY-MM-DD")
            date.fromisoformat(day)
        except ValueError as exc:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier day is invalid: {path}"
            ) from exc
        manifest_name = _component(payload.get("manifest_name"), "manifest_name")
        expected_manifest_name = (
            f"seq-{first}-{last}-next-{next_sequence}-{raw_digest}.xml.manifest.json"
        )
        if manifest_name != expected_manifest_name:
            raise MtconnectProtocolError(
                f"Recorder recovery frontier manifest identity is invalid: {path}"
            )

        instance_root = _confined_storage_path(
            self.store.raw_root,
            _slug(source_name),
            str(int(instance_id)),
        )
        manifest_path = _confined_storage_path(instance_root, day, manifest_name)
        if not manifest_path.exists():
            # The process can die after publishing the pointer but before the
            # raw/manifest pair. Refetching the same checkpoint sequence is the
            # correct bounded recovery; no archive search is required.
            return RecoveryLookup(initialized=True)

        manifest, issue = self.store._read_manifest_payload(manifest_path)
        if manifest is None:
            detail = issue.detail if issue is not None else "manifest is invalid"
            raise MtconnectProtocolError(
                f"Recorder recovery manifest is not usable: {detail}"
            )
        try:
            manifest_source = manifest["source_name"]
            manifest_instance = int(manifest["agent_instance_id"])
            manifest_requested = int(manifest["requested_from"])
            manifest_first = int(manifest["first_observation_sequence"])
            manifest_last = int(manifest["last_observation_sequence"])
            manifest_next = int(manifest["next_sequence"])
            manifest_count = int(manifest["observation_count"])
            manifest_digest = str(manifest["raw_sha256"])
            manifest_schema = str(manifest["schema"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MtconnectProtocolError(
                f"Recorder recovery manifest fields are invalid: {manifest_path}"
            ) from exc
        if (
            manifest_source != source_name
            or manifest_instance != int(instance_id)
            or manifest_requested != requested_from
            or manifest_first != first
            or manifest_last != last
            or manifest_next != next_sequence
            or manifest_count != observation_count
            or manifest_digest != raw_digest
        ):
            raise MtconnectProtocolError(
                f"Recorder recovery manifest does not match its durable frontier: {manifest_path}"
            )

        raw_path = next(
            (
                candidate
                for candidate in self.store._local_raw_candidates(manifest_path)
                if candidate.is_file()
            ),
            None,
        )
        if raw_path is None:
            # The writer publishes raw before manifest. A manifest without its
            # local raw sibling is therefore corruption, not a safe retry gap.
            raise MtconnectProtocolError(
                f"Recorder recovery manifest has no local raw payload: {manifest_path}"
            )
        received_at = manifest.get("received_at")
        return RecoveryLookup(
            initialized=True,
            ref=RawBatchRef(
                raw_path=raw_path,
                manifest_path=manifest_path,
                raw_sha256=raw_digest,
                requested_from=requested_from,
                first_sequence=first,
                last_sequence=last,
                next_sequence=next_sequence,
                observation_count=observation_count,
                manifest_schema=manifest_schema,
                received_at=(
                    str(received_at) if isinstance(received_at, str) else None
                ),
                source_name=source_name,
            ),
        )


__all__ = [
    "RECOVERY_FRONTIER_FILENAME",
    "RECOVERY_FRONTIER_SCHEMA",
    "RecorderRecoveryFrontier",
    "RecoveryLookup",
]

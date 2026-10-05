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
import os
import re
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from heapq import nsmallest
from pathlib import Path

from .model import (
    MtconnectProtocolError,
    RawBatchRef,
    _fsync_directory,
    _slug,
    _write_json_atomic,
)
from .storage import DurableRecorderStore, _confined_storage_path

PENDING_PUBLICATION_SCHEMA = "fcp.mtconnect.pending_publication.v1"
PUBLICATION_FRONTIER_STATE_SCHEMA = "fcp.mtconnect.publication_frontier.v1"
PUBLICATION_FRONTIER_RECORD_MAX_BYTES = 4096
PUBLICATION_FRONTIER_RECORD_INODES = 1
PUBLICATION_FRONTIER_STATE_MAX_BYTES = 1024
PUBLICATION_FRONTIER_STATE_INODES = 1
_RECORD_NAME_PATTERN = re.compile(
    r"\Aseq-(?P<first>[0-9]+)-(?P<last>[0-9]+)-"
    r"next-(?P<next>[0-9]+)-(?P<digest>[0-9a-f]{64})\.json\Z"
)
_MISSING_ARCHIVE_ROOT_IDENTITY = "missing"


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
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise MtconnectProtocolError("Publication frontier day is invalid.") from exc
    return text


def _is_reparse_point(path: Path) -> bool:
    """Detect link-like path components on both POSIX and Windows."""

    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(is_junction is not None and is_junction())


def _ensure_confined_without_reparse(
    path: Path,
    *,
    root: Path,
    label: str,
) -> None:
    """Reject a managed path that escapes or traverses a link-like component."""

    # Capture may retain a relative raw_file while its publisher uses an
    # absolute data directory. Compare equivalent lexical representations,
    # without resolving links before the reparse checks below. The evidence
    # paths and their recorded provenance remain unchanged.
    try:
        path = path.absolute()
        root = root.absolute()
    except OSError as exc:
        raise MtconnectProtocolError(
            f"Publication frontier {label} identity is unavailable."
        ) from exc
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise MtconnectProtocolError(
            f"Publication frontier {label} is outside its durable root."
        ) from exc
    current = path
    while True:
        try:
            if _is_reparse_point(current):
                raise MtconnectProtocolError(
                    f"Publication frontier {label} must not traverse a symlink or reparse point."
                )
        except OSError as exc:
            raise MtconnectProtocolError(
                f"Publication frontier {label} identity is unavailable."
            ) from exc
        if current == root:
            return
        parent = current.parent
        if parent == current:
            raise MtconnectProtocolError(
                f"Publication frontier {label} is outside its durable root."
            )
        current = parent


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
        path = _confined_storage_path(
            self.pending_root,
            _slug(source_name),
            str(int(instance_id)),
        )
        _ensure_confined_without_reparse(
            path,
            root=self.store.root,
            label="pending root",
        )
        return path

    def _state_path(self, source_name: str, instance_id: int) -> Path:
        path = _confined_storage_path(
            self.state_root,
            _slug(source_name),
            str(int(instance_id)),
            ".initialized.json",
        )
        _ensure_confined_without_reparse(
            path,
            root=self.store.root,
            label="migration state",
        )
        return path

    def _archive_root(self, source_name: str, instance_id: int) -> Path:
        path = _confined_storage_path(
            self.store.raw_root,
            _slug(source_name),
            str(int(instance_id)),
        )
        _ensure_confined_without_reparse(
            path,
            root=self.store.root,
            label="archive root",
        )
        return path

    def _archive_root_identity(self, source_name: str, instance_id: int) -> str:
        """Return a stable identity for the archive directory being migrated.

        The initialized marker is useful only while it still describes the
        archive root that was scanned. A recreated directory at the same path
        must therefore trigger a new compatibility scan; otherwise historical
        batches restored into that directory would be invisible forever.
        """

        root = self._archive_root(source_name, instance_id)
        try:
            if _is_reparse_point(root):
                raise MtconnectProtocolError(
                    "Publication frontier archive root must not be a symlink or reparse point."
                )
            stat = root.stat()
        except FileNotFoundError:
            return _MISSING_ARCHIVE_ROOT_IDENTITY
        except OSError as exc:
            raise MtconnectProtocolError(
                "Publication frontier archive root identity is unavailable."
            ) from exc
        if not root.is_dir():
            raise MtconnectProtocolError(
                "Publication frontier archive root is not a directory."
            )
        return f"{int(stat.st_dev)}:{int(stat.st_ino)}"

    @staticmethod
    def _record_name(ref: RawBatchRef) -> str:
        return (
            f"seq-{int(ref.first_sequence)}-{int(ref.last_sequence)}-"
            f"next-{int(ref.next_sequence)}-{_digest(ref.raw_sha256)}.json"
        )

    def _record_path(self, source_name: str, instance_id: int, ref: RawBatchRef) -> Path:
        path = _confined_storage_path(
            self._source_pending_root(source_name, instance_id),
            self._record_name(ref),
        )
        _ensure_confined_without_reparse(
            path,
            root=self.store.root,
            label="pending record",
        )
        return path

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
        recorded_source = _required_text(ref.source_name, "ref.source_name")
        if recorded_source != archive_source:
            raise MtconnectProtocolError(
                "Publication frontier archive source does not match raw evidence."
            )
        first = _positive_int(ref.first_sequence, "first_sequence")
        last = _positive_int(ref.last_sequence, "last_sequence")
        next_sequence = _positive_int(ref.next_sequence, "next_sequence")
        requested_from = _positive_int(ref.requested_from, "requested_from")
        observation_count = _positive_int(ref.observation_count, "observation_count")
        if first > last or next_sequence <= last or observation_count <= 0:
            raise MtconnectProtocolError(
                "Publication frontier sequence identity is invalid."
            )
        day = _day(ref.manifest_path.parent.name)
        raw_name = _basename(ref.raw_path.name, "raw_name")
        manifest_name = _basename(ref.manifest_path.name, "manifest_name")
        if ref.raw_path.parent.name != day or ref.manifest_path.parent.name != day:
            raise MtconnectProtocolError(
                "Publication frontier raw and manifest locations disagree."
            )
        expected_manifest_name = Path(raw_name).with_suffix(".manifest.json").name
        if manifest_name != expected_manifest_name:
            raise MtconnectProtocolError(
                "Publication frontier manifest name does not match raw evidence."
            )
        archive_root = _confined_storage_path(
            self.store.raw_root,
            _slug(archive_source),
            str(instance_id),
            day,
        )
        canonical_raw_path = _confined_storage_path(archive_root, raw_name)
        canonical_manifest_path = _confined_storage_path(archive_root, manifest_name)
        for evidence_path, label in (
            (ref.raw_path, "raw evidence"),
            (ref.manifest_path, "raw manifest"),
            (canonical_raw_path, "canonical raw evidence"),
            (canonical_manifest_path, "canonical raw manifest"),
        ):
            _ensure_confined_without_reparse(
                evidence_path,
                root=self.store.root,
                label=label,
            )
        for evidence_path, label in (
            (ref.raw_path, "raw evidence"),
            (ref.manifest_path, "raw manifest"),
            (canonical_raw_path, "canonical raw evidence"),
            (canonical_manifest_path, "canonical raw manifest"),
        ):
            try:
                if evidence_path.is_symlink() or not evidence_path.is_file():
                    raise MtconnectProtocolError(
                        f"Publication frontier {label} is not a regular file."
                    )
            except OSError as exc:
                raise MtconnectProtocolError(
                    f"Publication frontier {label} is unavailable."
                ) from exc
        try:
            if ref.raw_path.resolve() != canonical_raw_path.resolve():
                raise MtconnectProtocolError(
                    "Publication frontier raw evidence is outside its archive identity."
                )
            if ref.manifest_path.resolve() != canonical_manifest_path.resolve():
                raise MtconnectProtocolError(
                    "Publication frontier manifest is outside its archive identity."
                )
        except OSError as exc:
            raise MtconnectProtocolError(
                "Publication frontier manifest identity is unavailable."
            ) from exc
        self._archive_root_identity(archive_source, instance_id)
        received_at = ref.received_at
        if received_at is not None:
            _required_text(received_at, "received_at")
        payload = {
            "schema": PENDING_PUBLICATION_SCHEMA,
            "source_name": source,
            "archive_source_name": archive_source,
            "agent_instance_id": instance_id,
            "requested_from": requested_from,
            "first_sequence": first,
            "last_sequence": last,
            "next_sequence": next_sequence,
            "observation_count": observation_count,
            "raw_sha256": _digest(ref.raw_sha256),
            "manifest_schema": _required_text(ref.manifest_schema, "manifest_schema"),
            "received_at": received_at,
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
        _ensure_confined_without_reparse(
            path,
            root=self.store.root,
            label="pending record",
        )
        try:
            if _is_reparse_point(path) or not path.is_file():
                raise MtconnectProtocolError(
                    "Publication frontier record is not a regular file."
                )
            if path.stat().st_size > PUBLICATION_FRONTIER_RECORD_MAX_BYTES:
                raise MtconnectProtocolError(
                    "Publication frontier record exceeds its fixed bound."
                )
        except OSError as exc:
            raise MtconnectProtocolError("Publication frontier record is unavailable.") from exc
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
        requested_from = _positive_int(
            payload.get("requested_from", first),
            "requested_from",
        )
        observation_count = _positive_int(payload.get("observation_count"), "observation_count")
        if first > last or next_sequence <= last or observation_count <= 0:
            raise MtconnectProtocolError("Publication frontier sequence identity is invalid.")
        raw_sha256 = _digest(payload.get("raw_sha256"))
        day = _day(payload.get("day"))
        raw_name = _basename(payload.get("raw_name"), "raw_name")
        manifest_name = _basename(payload.get("manifest_name"), "manifest_name")
        if manifest_name != Path(raw_name).with_suffix(".manifest.json").name:
            raise MtconnectProtocolError(
                "Publication frontier manifest name does not match raw evidence."
            )
        archive_root = _confined_storage_path(
            self.store.raw_root,
            _slug(archive_source),
            str(instance_id),
            day,
        )
        raw_path = _confined_storage_path(archive_root, raw_name)
        manifest_path = _confined_storage_path(archive_root, manifest_name)
        _ensure_confined_without_reparse(
            manifest_path,
            root=self.store.root,
            label="raw manifest",
        )
        _ensure_confined_without_reparse(
            raw_path,
            root=self.store.root,
            label="raw evidence",
        )
        received_at = payload.get("received_at")
        if received_at is not None:
            _required_text(received_at, "received_at")
        ref = RawBatchRef(
            raw_path=raw_path,
            manifest_path=manifest_path,
            raw_sha256=raw_sha256,
            requested_from=requested_from,
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

    @staticmethod
    def _record_sort_key(path: Path) -> tuple[int, int, int, str]:
        match = _RECORD_NAME_PATTERN.fullmatch(path.name)
        if match is None:
            raise MtconnectProtocolError(
                "Publication frontier file name is not a bounded record identity."
            )
        return (
            int(match.group("first")),
            int(match.group("last")),
            int(match.group("next")),
            match.group("digest"),
        )

    def pending(
        self,
        *,
        source_name: str,
        instance_id: int,
        limit: int | None = None,
    ) -> tuple[PendingPublication, ...]:
        """Enumerate unreconciled discovery records for one source/instance.

        ``limit`` bounds the number of record files decoded by one normal
        reconciliation pass. Directory enumeration still looks at names, but
        it does not parse or retain the contents of the rest of a large
        backlog. The default remains unbounded for compatibility/migration
        callers that explicitly need a complete view.
        """

        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
        ):
            raise MtconnectProtocolError("Publication frontier limit is invalid.")
        root = self._source_pending_root(source, instance_id)
        if not root.exists():
            return ()
        try:
            paths = [path for path in root.iterdir() if path.name.startswith("seq-")]
        except OSError as exc:
            raise MtconnectProtocolError(
                "Publication frontier pending directory is unavailable."
            ) from exc
        if limit is not None:
            paths = nsmallest(limit, paths, key=self._record_sort_key)
        else:
            paths.sort(key=self._record_sort_key)
        values = [
            self._decode(path, source_name=source, instance_id=instance_id)
            for path in paths
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
        if not expected.exists():
            return
        current = self._decode(
            expected,
            source_name=pending.source_name,
            instance_id=pending.instance_id,
        )
        if current != pending:
            raise MtconnectProtocolError(
                "Publication frontier record changed before retirement."
            )
        expected.unlink()

    def migration_state(self, *, source_name: str, instance_id: int) -> str:
        """Return ``missing``, ``initialized``, or an explicit blocked state."""

        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        path = self._state_path(source, instance_id)
        if not path.exists():
            return "missing"
        try:
            if _is_reparse_point(path) or not path.is_file():
                raise MtconnectProtocolError(
                    "Publication frontier state is not a regular file."
                )
            if path.stat().st_size > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
                raise MtconnectProtocolError(
                    "Publication frontier state exceeds its fixed bound."
                )
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MtconnectProtocolError("Publication frontier state is unreadable.") from exc
        if not isinstance(payload, dict) or payload.get("schema") != PUBLICATION_FRONTIER_STATE_SCHEMA:
            raise MtconnectProtocolError("Publication frontier state schema is unsupported.")
        if payload.get("source_name") != source:
            raise MtconnectProtocolError("Publication frontier state identity mismatch.")
        encoded_instance = payload.get("agent_instance_id")
        if (
            isinstance(encoded_instance, bool)
            or not isinstance(encoded_instance, int)
            or encoded_instance != instance_id
        ):
            raise MtconnectProtocolError("Publication frontier state identity mismatch.")
        state = payload.get("state")
        if state not in {"initialized", "blocked"}:
            raise MtconnectProtocolError("Publication frontier state is invalid.")
        root_identity = payload.get("archive_root_identity")
        if not isinstance(root_identity, str) or not root_identity.strip():
            # Markers written before archive-root identity binding are safely
            # migrated by one compatibility scan.
            return "missing"
        if root_identity != self._archive_root_identity(source, instance_id):
            return "missing"
        if state == "blocked":
            issue_count = payload.get("issue_count")
            issue_sample = payload.get("issue_sample")
            if (
                isinstance(issue_count, bool)
                or not isinstance(issue_count, int)
                or issue_count <= 0
                or not isinstance(issue_sample, str)
                or not issue_sample.strip()
            ):
                raise MtconnectProtocolError("Publication frontier blocked state is invalid.")
        return str(state)

    def initialized(self, *, source_name: str, instance_id: int) -> bool:
        return self.migration_state(source_name=source_name, instance_id=instance_id) == "initialized"

    def retry_blocked_migration(
        self,
        *,
        source_name: str,
        instance_id: int,
        expected_state_sha256: str,
    ) -> Path:
        """Explicitly retry one repaired legacy migration, retaining its refusal.

        The operator must hold the supported host mutation lease and prove the
        publication writer is stopped before calling this method. It is not a
        live control endpoint and does not provide a filesystem compare-and-swap
        against an uncoordinated writer. Capture evidence and outbox rows are
        never changed. Only a validated blocked marker with the exact expected
        bytes may be retired; initialized markers cannot be reset.

        A same-directory, exclusive, fsynced copy preserves the complete original
        marker before its removal. Interrupted removal can be retried with the
        same hash. A partial or conflicting receipt fails closed, preserving the
        marker for investigation. Ordinary reconciliation performs the one-time
        migration again and retains any remaining real issue as blocked.
        """
        expected = _digest(expected_state_sha256)
        source = _required_text(source_name, "source_name")
        instance = _positive_int(instance_id, "instance_id")
        path = self._state_path(source, instance)
        receipt = path.with_name(f".blocked-{expected}.json")
        _ensure_confined_without_reparse(
            receipt, root=self.store.root, label="migration retry receipt"
        )

        def read_exact(candidate: Path) -> bytes:
            if _is_reparse_point(candidate) or not candidate.is_file():
                raise MtconnectProtocolError("Publication frontier retry input is not a regular file.")
            if candidate.stat().st_size > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
                raise MtconnectProtocolError("Publication frontier retry input exceeds its fixed bound.")
            value = candidate.read_bytes()
            if len(value) > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
                raise MtconnectProtocolError("Publication frontier retry input exceeds its fixed bound.")
            if sha256(value).hexdigest() != expected:
                raise MtconnectProtocolError("Publication frontier retry state hash mismatch.")
            return value

        try:
            if not path.exists():
                # Replay after successful removal proves the retained original,
                # rather than inventing a new blocked or initialized marker.
                value = read_exact(receipt)
                payload = json.loads(value)
                if (
                    payload.get("schema") != PUBLICATION_FRONTIER_STATE_SCHEMA
                    or payload.get("state") != "blocked"
                    or payload.get("source_name") != source
                    or type(payload.get("agent_instance_id")) is not int
                    or payload["agent_instance_id"] != instance
                    or payload.get("archive_root_identity")
                    != self._archive_root_identity(source, instance)
                    or type(payload.get("issue_count")) is not int
                    or payload["issue_count"] <= 0
                    or not isinstance(payload.get("issue_sample"), str)
                    or not payload["issue_sample"].strip()
                ):
                    raise MtconnectProtocolError("Publication frontier retry receipt identity mismatch.")
                return receipt
            if self.migration_state(source_name=source, instance_id=instance) != "blocked":
                raise MtconnectProtocolError("Publication frontier retry requires a blocked migration.")
            value = read_exact(path)
            try:
                with receipt.open("xb") as handle:
                    handle.write(value)
                    handle.flush()
                    os.fsync(handle.fileno())
                _fsync_directory(receipt.parent)
            except FileExistsError:
                # A prior call may have durably copied the refusal but failed
                # before removing it. Never overwrite an existing receipt.
                if read_exact(receipt) != value:
                    raise MtconnectProtocolError("Publication frontier retry receipt conflict.")
            # Windows FlushFileBuffers needs a handle opened for writing; the
            # retained bytes are never rewritten through this handle.
            with receipt.open("r+b") as handle:
                os.fsync(handle.fileno())
            _fsync_directory(receipt.parent)
            read_exact(path)
            path.unlink()
            _fsync_directory(path.parent)
            return receipt
        except (OSError, ValueError, AttributeError) as exc:
            raise MtconnectProtocolError("Publication frontier migration retry is unavailable.") from exc

    def mark_initialized(self, *, source_name: str, instance_id: int) -> Path:
        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        payload = {
            "schema": PUBLICATION_FRONTIER_STATE_SCHEMA,
            "state": "initialized",
            "source_name": source,
            "agent_instance_id": instance_id,
            "archive_root_identity": self._archive_root_identity(source, instance_id),
        }
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
        if len(encoded) > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
            raise MtconnectProtocolError("Publication frontier state exceeds its fixed bound.")
        path = self._state_path(source, instance_id)
        _write_json_atomic(path, payload)
        return path

    def mark_blocked(
        self,
        *,
        source_name: str,
        instance_id: int,
        issue_count: int,
        issue_sample: str,
    ) -> Path:
        """Persist a completed scan that found unrepresentable legacy items.

        Valid references are still seeded before this state is written. The
        explicit blocked state prevents a permanently malformed legacy item
        from causing an accidental lifetime rescan on every steady-state
        cycle, while retaining a bounded operator-visible reason to repair.
        """

        source = _required_text(source_name, "source_name")
        if isinstance(instance_id, bool) or not isinstance(instance_id, int) or instance_id < 0:
            raise MtconnectProtocolError("Publication frontier instance_id is invalid.")
        count = _positive_int(issue_count, "issue_count")
        if count <= 0:
            raise MtconnectProtocolError("Publication frontier issue_count is invalid.")
        sample = _required_text(issue_sample, "issue_sample", max_bytes=512)
        payload = {
            "schema": PUBLICATION_FRONTIER_STATE_SCHEMA,
            "state": "blocked",
            "source_name": source,
            "agent_instance_id": instance_id,
            "archive_root_identity": self._archive_root_identity(source, instance_id),
            "issue_count": count,
            "issue_sample": sample,
        }
        encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
        if len(encoded) > PUBLICATION_FRONTIER_STATE_MAX_BYTES:
            raise MtconnectProtocolError("Publication frontier blocked state exceeds its fixed bound.")
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

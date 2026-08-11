"""Verified local materialization of committed federated recorder telemetry.

The mirror is deliberately narrower than a general federation file cache.  It
accepts only manifest-backed MTConnect recorder batches, stores their canonical
content under locally derived hashes, and exposes only rebuilt wide snapshots
to the application's artifact scanner.  Remote identities are data, never
filesystem paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from .errors import FederationValidationError
from .models import CommitState
from .recorder_payload import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    RECORDER_CONTENT_SCHEMA,
    RECORDER_DATASET_SCHEMA_NAME,
    RECORDER_DATASET_SCHEMA_VERSION,
    RECORDER_OBSERVATION_SCHEMA,
    validate_recorder_payload,
)
from .storage_catalog import CommittedBatchReference

RECORDER_SNAPSHOT_SCHEMA = "fcp.mtconnect.snapshot.v2"

DEFAULT_MAX_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_MATERIALIZATION_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_SIGNALS_PER_MACHINE = 2_048

_DATABASE_SCHEMA_VERSION = 1
_DATASET_PATTERN = re.compile(
    r"mtconnect:(?P<node>[A-Za-z0-9][A-Za-z0-9._-]{0,127}):"
    r"(?P<source>[A-Za-z0-9][A-Za-z0-9._-]{0,127})"
)
_GENERATED_SNAPSHOT_FILENAME = re.compile(r"[0-9a-f]{64}\.jsonl")

_SNAPSHOT_RESERVED_KEYS = frozenset(
    {
        "schema",
        "source",
        "source_name",
        "source_record_id",
        "machine",
        "machine_name",
        "machine_id",
        "agent_instance_id",
        "sequence",
        "timestamp",
        "received_at",
        "changed_data_item_id",
        "changed_name",
        "changed_category",
    }
)
_SIGNAL_ESCAPE_PREFIX = "__fcp_signal__"


def _validation(code: str, field: str, message: str) -> FederationValidationError:
    return FederationValidationError(code, field, message)


def _required_text(value: Any, field: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _validation(
            "invalid-telemetry-field",
            field,
            f"must be non-empty printable text no longer than {maximum} bytes",
        )
    return value.strip()


def _canonical_bytes(value: Any, field: str = "content") -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _validation(
            "invalid-telemetry-json",
            field,
            "must contain only canonical JSON-compatible values",
        ) from exc


def _json_without_duplicate_keys(encoded: bytes, field: str) -> Any:
    def build_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(encoded.decode("utf-8"), object_pairs_hook=build_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _validation(
            "corrupt-telemetry-blob",
            field,
            "stored batch is not unambiguous UTF-8 JSON",
        ) from exc


def _identity_digest(*values: object) -> str:
    encoded = json.dumps(
        list(values),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _signal_key(record: Mapping[str, Any]) -> str:
    raw = record.get("name") or record.get("data_item_id") or record.get(
        "observation_type"
    )
    key = _required_text(raw, "observation.signal_identity", maximum=256)
    if (
        key in _SNAPSHOT_RESERVED_KEYS
        or key.startswith(("federation_", _SIGNAL_ESCAPE_PREFIX))
    ):
        return f"{_SIGNAL_ESCAPE_PREFIX}{key}"
    return key


def _signal_value(record: Mapping[str, Any]) -> Any:
    if record.get("category") == "CONDITION":
        return record.get("condition_level") or record.get("native_value")
    return record.get("value")


class TelemetryMirrorIngestState(str, Enum):
    STORED = "stored"
    ALREADY_STORED = "already-stored"


@dataclass(frozen=True)
class TelemetryMirrorIngestResult:
    state: TelemetryMirrorIngestState
    inserted_observations: int
    deduplicated_observations: int
    telemetry_path: Path


@dataclass(frozen=True)
class TelemetryMirrorRebuildResult:
    batches: int
    observations: int
    materializations: int


@dataclass(frozen=True)
class _ValidatedBatch:
    reference: CommittedBatchReference
    content: dict[str, Any]
    encoded: bytes
    source_name: str
    source_slug: str
    recorder_node_id: str
    instance_id: int
    first_sequence: int
    last_sequence: int
    observations: tuple[dict[str, Any], ...]


class FederatedTelemetryMirror:
    """Durable, content-verified mirror for committed recorder batches."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
        max_records_per_batch: int = DEFAULT_MAX_RECORDS_PER_BATCH,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        max_materialization_bytes: int = DEFAULT_MAX_MATERIALIZATION_BYTES,
        max_signals_per_machine: int = DEFAULT_MAX_SIGNALS_PER_MACHINE,
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self.max_batch_bytes = self._positive_bound(
            max_batch_bytes, "max_batch_bytes"
        )
        self.max_records_per_batch = self._positive_bound(
            max_records_per_batch, "max_records_per_batch"
        )
        self.max_total_bytes = self._positive_bound(
            max_total_bytes, "max_total_bytes"
        )
        self.max_materialization_bytes = self._positive_bound(
            max_materialization_bytes, "max_materialization_bytes"
        )
        self.max_signals_per_machine = self._positive_bound(
            max_signals_per_machine, "max_signals_per_machine"
        )
        self.root.mkdir(parents=True, exist_ok=True)
        self._assert_managed(self.root, "root")
        self.batch_root = self._managed_directory("batches/v1")
        self.telemetry_root = self._managed_directory("telemetry")
        self.database_path = self._managed_path("mirror.sqlite3", "database")
        self._initialize_database()

    @staticmethod
    def _positive_bound(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise _validation(
                "invalid-telemetry-mirror-config",
                field,
                "must be a positive integer",
            )
        return value

    def _assert_managed(self, path: Path, field: str) -> Path:
        try:
            resolved = path.resolve()
            resolved.relative_to(self.root)
        except (OSError, ValueError) as exc:
            raise _validation(
                "unsafe-telemetry-path",
                field,
                "must remain inside the managed telemetry mirror root",
            ) from exc
        return path

    def _managed_path(self, relative: str, field: str) -> Path:
        candidate = self.root.joinpath(*Path(relative).parts)
        return self._assert_managed(candidate, field)

    def _managed_directory(self, relative: str) -> Path:
        directory = self._managed_path(relative, "directory")
        directory.mkdir(parents=True, exist_ok=True)
        self._assert_managed(directory, "directory")
        return directory

    def _connect(self) -> sqlite3.Connection:
        self._assert_managed(self.database_path, "database")
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS mirror_meta (
                    schema_version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    schema_name TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    source_id TEXT NOT NULL,
                    first_sequence INTEGER NOT NULL,
                    last_sequence INTEGER NOT NULL,
                    source_name TEXT NOT NULL,
                    instance_id INTEGER NOT NULL,
                    reference_json TEXT NOT NULL,
                    manifest_revision INTEGER NOT NULL,
                    blob_path TEXT NOT NULL,
                    mirrored_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, group_id, dataset_id, batch_id)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS observations (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    instance_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    machine_id TEXT NOT NULL,
                    observation_hash TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    PRIMARY KEY (
                        session_id, group_id, dataset_id, instance_id, sequence
                    )
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS observations_materialize_idx
                    ON observations (
                        session_id, group_id, dataset_id, instance_id, sequence
                    );
                CREATE TABLE IF NOT EXISTS materializations (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    instance_id INTEGER NOT NULL,
                    relative_path TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, group_id, dataset_id, instance_id)
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS quarantines (
                    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    identity_digest TEXT NOT NULL,
                    received_hash TEXT NOT NULL,
                    existing_hash TEXT,
                    reason TEXT NOT NULL,
                    quarantined_at TEXT NOT NULL
                );
                """
            )
            versions = connection.execute(
                "SELECT schema_version FROM mirror_meta"
            ).fetchall()
            if not versions:
                connection.execute(
                    "INSERT INTO mirror_meta(schema_version) VALUES (?)",
                    (_DATABASE_SCHEMA_VERSION,),
                )
            elif len(versions) != 1 or versions[0][0] != _DATABASE_SCHEMA_VERSION:
                raise _validation(
                    "unsupported-telemetry-mirror-schema",
                    "mirror.sqlite3",
                    "database schema version is not supported",
                )

    def _validate(
        self,
        reference: CommittedBatchReference,
        content: object,
    ) -> _ValidatedBatch:
        if not isinstance(reference, CommittedBatchReference):
            raise _validation(
                "invalid-committed-batch-reference",
                "reference",
                "must be a CommittedBatchReference",
            )
        if reference.commit_state is not CommitState.COMMITTED:
            raise _validation(
                "batch-not-committed",
                "reference.commit_state",
                "only committed manifest batches may be mirrored",
            )
        validated = validate_recorder_payload(
            session_id=reference.session_id,
            dataset_id=reference.dataset_id,
            batch_id=reference.batch_id,
            idempotency_key=reference.idempotency_key,
            dataset_schema_name=reference.schema_name,
            dataset_schema_version=reference.schema_version,
            content=content,
            expected_source_id=reference.source_id,
            expected_first_sequence=reference.first_sequence,
            expected_last_sequence=reference.last_sequence,
            expected_content_hash=reference.content_hash,
            expected_size_bytes=reference.size_bytes,
            max_batch_bytes=self.max_batch_bytes,
            max_records_per_batch=self.max_records_per_batch,
        )
        return _ValidatedBatch(
            reference=reference,
            content=validated.content,
            encoded=validated.encoded,
            source_name=validated.source_name,
            source_slug=validated.source_slug,
            recorder_node_id=validated.recorder_node_id,
            instance_id=validated.instance_id,
            first_sequence=validated.first_sequence,
            last_sequence=validated.last_sequence,
            observations=validated.observations,
        )

    def _blob_relative_path(self, batch: _ValidatedBatch) -> str:
        reference = batch.reference
        digest = _identity_digest(
            reference.session_id,
            reference.group_id,
            reference.dataset_id,
            reference.batch_id,
            reference.content_hash,
        )
        return f"batches/v1/{digest[:2]}/{digest[2:4]}/{digest}.json"

    def _telemetry_relative_path(
        self,
        session_id: str,
        group_id: str,
        dataset_id: str,
        instance_id: int,
    ) -> str:
        digest = _identity_digest(session_id, group_id, dataset_id, instance_id)
        return f"telemetry/{digest}.jsonl"

    def _write_immutable_blob(self, relative_path: str, encoded: bytes) -> Path:
        final_path = self._managed_path(relative_path, "blob_path")
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self._assert_managed(final_path.parent, "blob_path")
        if final_path.exists():
            try:
                existing = final_path.read_bytes()
            except OSError as exc:
                raise _validation(
                    "telemetry-blob-unavailable",
                    "blob_path",
                    "could not read an existing immutable batch",
                ) from exc
            if existing != encoded:
                raise _validation(
                    "telemetry-blob-integrity-failed",
                    "blob_path",
                    "existing immutable batch bytes do not match verified content",
                )
            return final_path

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{final_path.stem}-",
            suffix=".tmp",
            dir=final_path.parent,
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, final_path)
            _fsync_directory(final_path.parent)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
        return final_path

    def _record_quarantine(
        self,
        connection: sqlite3.Connection,
        *,
        batch: _ValidatedBatch,
        existing_hash: str | None,
        reason: str,
    ) -> None:
        reference = batch.reference
        connection.execute(
            """
            INSERT INTO quarantines(
                identity_digest, received_hash, existing_hash, reason, quarantined_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                _identity_digest(
                    reference.session_id,
                    reference.group_id,
                    reference.dataset_id,
                    reference.batch_id,
                ),
                reference.content_hash,
                existing_hash,
                reason,
                _utc_now(),
            ),
        )

    @staticmethod
    def _existing_batch_matches(
        existing: sqlite3.Row,
        batch: _ValidatedBatch,
        blob_relative: str,
    ) -> bool:
        reference = batch.reference
        return bool(
            existing["idempotency_key"] == reference.idempotency_key
            and existing["content_hash"] == reference.content_hash
            and existing["size_bytes"] == reference.size_bytes
            and existing["schema_name"] == reference.schema_name
            and existing["schema_version"] == reference.schema_version
            and existing["source_id"] == reference.dataset_id
            and existing["first_sequence"] == batch.first_sequence
            and existing["last_sequence"] == batch.last_sequence
            and existing["source_name"] == batch.source_name
            and existing["instance_id"] == batch.instance_id
            and existing["blob_path"] == blob_relative
        )

    def contains(self, reference: CommittedBatchReference) -> bool:
        """Check the full immutable identity in SQLite without reading a blob."""

        if not isinstance(reference, CommittedBatchReference):
            raise _validation(
                "invalid-committed-batch-reference",
                "reference",
                "must be a CommittedBatchReference",
            )
        metadata = self._metadata_from_reference_identity(reference)
        if metadata is None:
            return False
        source_id, instance_id, first_sequence, last_sequence = metadata
        expected_blob = self._blob_relative_path_from_reference(reference)
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT * FROM batches
                WHERE session_id = ? AND group_id = ? AND dataset_id = ? AND batch_id = ?
                """,
                (
                    reference.session_id,
                    reference.group_id,
                    reference.dataset_id,
                    reference.batch_id,
                ),
            ).fetchone()
        return bool(
            existing is not None
            and existing["idempotency_key"] == reference.idempotency_key
            and existing["content_hash"] == reference.content_hash
            and existing["size_bytes"] == reference.size_bytes
            and existing["schema_name"] == reference.schema_name
            and existing["schema_version"] == reference.schema_version
            and existing["source_id"] == source_id
            and existing["instance_id"] == instance_id
            and existing["first_sequence"] == first_sequence
            and existing["last_sequence"] == last_sequence
            and existing["blob_path"] == expected_blob
        )

    @staticmethod
    def _metadata_from_reference_identity(
        reference: CommittedBatchReference,
    ) -> tuple[str, int, int, int] | None:
        """Derive legacy recorder coverage from its authenticated batch ID."""

        if (
            reference.commit_state is not CommitState.COMMITTED
            or reference.schema_name != RECORDER_DATASET_SCHEMA_NAME
            or reference.schema_version != RECORDER_DATASET_SCHEMA_VERSION
        ):
            return None
        dataset_match = _DATASET_PATTERN.fullmatch(reference.dataset_id)
        if dataset_match is None:
            return None
        source_slug = dataset_match.group("source")
        parts = reference.batch_id.split(":")
        if len(parts) != 5 or parts[0] != source_slug:
            return None
        try:
            instance_id = int(parts[1])
            first_sequence = int(parts[2])
            last_sequence = int(parts[3])
        except ValueError:
            return None
        if (
            instance_id < 0
            or first_sequence < 0
            or last_sequence < first_sequence
            or re.fullmatch(r"[0-9a-f]{64}", parts[4]) is None
            or reference.source_id not in {None, reference.dataset_id}
            or (
                reference.first_sequence is not None
                and (
                    reference.first_sequence != first_sequence
                    or reference.last_sequence != last_sequence
                )
            )
        ):
            return None
        return reference.dataset_id, instance_id, first_sequence, last_sequence

    def ingest(
        self,
        reference: CommittedBatchReference,
        content: object,
    ) -> TelemetryMirrorIngestResult:
        """Verify, durably index, and materialize one committed batch."""

        batch = self._validate(reference, content)
        blob_relative = self._blob_relative_path(batch)
        inserted = 0
        deduplicated = 0
        already_stored = False

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT * FROM batches
                WHERE session_id = ? AND group_id = ? AND dataset_id = ? AND batch_id = ?
                """,
                (
                    reference.session_id,
                    reference.group_id,
                    reference.dataset_id,
                    reference.batch_id,
                ),
            ).fetchone()
            if existing is not None:
                if not self._existing_batch_matches(existing, batch, blob_relative):
                    self._record_quarantine(
                        connection,
                        batch=batch,
                        existing_hash=existing["content_hash"],
                        reason="immutable-batch-identity-conflict",
                    )
                    connection.commit()
                    raise _validation(
                        "telemetry-mirror-batch-conflict",
                        "reference.batch_id",
                        "an immutable batch identity already has different content",
                    )
                already_stored = True
            else:
                total_bytes = connection.execute(
                    "SELECT COALESCE(SUM(size_bytes), 0) FROM batches"
                ).fetchone()[0]
                if total_bytes + reference.size_bytes > self.max_total_bytes:
                    connection.rollback()
                    raise _validation(
                        "telemetry-mirror-quota-exceeded",
                        "reference.size_bytes",
                        "verified batch would exceed the configured mirror quota",
                    )

                prepared: list[tuple[int, str, str, str]] = []
                sequence_conflict: tuple[str, int] | None = None
                for observation in batch.observations:
                    sequence = observation["sequence"]
                    observation_json = _canonical_bytes(
                        observation, f"observation[{sequence}]"
                    ).decode("utf-8")
                    observation_hash = "sha256:" + hashlib.sha256(
                        observation_json.encode("utf-8")
                    ).hexdigest()
                    current = connection.execute(
                        """
                        SELECT observation_hash FROM observations
                        WHERE session_id = ? AND group_id = ? AND dataset_id = ?
                          AND instance_id = ? AND sequence = ?
                        """,
                        (
                            reference.session_id,
                            reference.group_id,
                            reference.dataset_id,
                            batch.instance_id,
                            sequence,
                        ),
                    ).fetchone()
                    if current is not None:
                        if current["observation_hash"] != observation_hash:
                            sequence_conflict = (current["observation_hash"], sequence)
                            break
                        deduplicated += 1
                        continue
                    prepared.append(
                        (
                            sequence,
                            _required_text(
                                observation.get("machine_id"),
                                f"observation[{sequence}].machine_id",
                            ),
                            observation_hash,
                            observation_json,
                        )
                    )
                if sequence_conflict is not None:
                    existing_hash, sequence = sequence_conflict
                    self._record_quarantine(
                        connection,
                        batch=batch,
                        existing_hash=existing_hash,
                        reason="sequence-content-conflict",
                    )
                    connection.commit()
                    raise _validation(
                        "telemetry-mirror-sequence-conflict",
                        f"content.observations[{sequence}]",
                        "this source, agent instance, and sequence already has different data",
                    )

                self._write_immutable_blob(blob_relative, batch.encoded)
                reference_json = _canonical_bytes(reference.to_dict(), "reference").decode(
                    "utf-8"
                )
                connection.execute(
                    """
                    INSERT INTO batches(
                        session_id, group_id, dataset_id, batch_id, idempotency_key,
                        content_hash, size_bytes, schema_name, schema_version, source_id,
                        first_sequence, last_sequence, source_name, instance_id,
                        reference_json, manifest_revision, blob_path, mirrored_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reference.session_id,
                        reference.group_id,
                        reference.dataset_id,
                        reference.batch_id,
                        reference.idempotency_key,
                        reference.content_hash,
                        reference.size_bytes,
                        reference.schema_name,
                        reference.schema_version,
                        reference.dataset_id,
                        batch.first_sequence,
                        batch.last_sequence,
                        batch.source_name,
                        batch.instance_id,
                        reference_json,
                        reference.manifest_revision,
                        blob_relative,
                        _utc_now(),
                    ),
                )
                for sequence, machine_id, observation_hash, observation_json in prepared:
                    connection.execute(
                        """
                        INSERT INTO observations(
                            session_id, group_id, dataset_id, instance_id, sequence,
                            machine_id, observation_hash, observation_json, batch_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            reference.session_id,
                            reference.group_id,
                            reference.dataset_id,
                            batch.instance_id,
                            sequence,
                            machine_id,
                            observation_hash,
                            observation_json,
                            reference.batch_id,
                        ),
                    )
                    inserted += 1
            if already_stored and reference.manifest_revision > existing["manifest_revision"]:
                connection.execute(
                    """
                    UPDATE batches SET reference_json = ?, manifest_revision = ?
                    WHERE session_id = ? AND group_id = ? AND dataset_id = ? AND batch_id = ?
                    """,
                    (
                        _canonical_bytes(reference.to_dict(), "reference").decode("utf-8"),
                        reference.manifest_revision,
                        reference.session_id,
                        reference.group_id,
                        reference.dataset_id,
                        reference.batch_id,
                    ),
                )
            connection.commit()

        # Even idempotent retries re-check the immutable local blob and repair a
        # materialization that may have been interrupted after the DB commit.
        self._write_immutable_blob(blob_relative, batch.encoded)
        telemetry_path = self._materialize(
            reference.session_id,
            reference.group_id,
            reference.dataset_id,
            batch.instance_id,
        )
        return TelemetryMirrorIngestResult(
            state=(
                TelemetryMirrorIngestState.ALREADY_STORED
                if already_stored
                else TelemetryMirrorIngestState.STORED
            ),
            inserted_observations=inserted,
            deduplicated_observations=(
                len(batch.observations) if already_stored else deduplicated
            ),
            telemetry_path=telemetry_path,
        )

    def _snapshot_row(
        self,
        observation: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> dict[str, Any]:
        source_name = str(observation["source_name"])
        return {
            "schema": RECORDER_SNAPSHOT_SCHEMA,
            "source": "mtconnect_recorder",
            "source_name": source_name,
            "source_record_id": f"federated-snapshot:{observation['source_record_id']}",
            "machine": observation.get("machine") or source_name,
            "machine_name": (
                observation.get("machine_name")
                or observation.get("machine")
                or source_name
            ),
            "machine_id": observation["machine_id"],
            "agent_instance_id": observation["agent_instance_id"],
            "sequence": observation["sequence"],
            "timestamp": observation["timestamp"],
            "received_at": observation["received_at"],
            "changed_data_item_id": observation.get("data_item_id"),
            "changed_name": observation.get("name"),
            "changed_category": observation.get("category"),
            **state,
        }

    def _materialize(
        self,
        session_id: str,
        group_id: str,
        dataset_id: str,
        instance_id: int,
    ) -> Path:
        relative_path = self._telemetry_relative_path(
            session_id, group_id, dataset_id, instance_id
        )
        final_path = self._managed_path(relative_path, "telemetry_path")
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self._assert_managed(final_path.parent, "telemetry_path")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT observation_json FROM observations
                WHERE session_id = ? AND group_id = ? AND dataset_id = ? AND instance_id = ?
                ORDER BY sequence
                """,
                (session_id, group_id, dataset_id, instance_id),
            )
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{final_path.stem}-",
                suffix=".tmp",
                dir=final_path.parent,
            )
            temporary_path = Path(temporary_name)
            states: dict[str, dict[str, Any]] = {}
            output_hash = hashlib.sha256()
            output_size = 0
            row_count = 0
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    for row in rows:
                        observation = json.loads(row["observation_json"])
                        machine_id = str(observation["machine_id"])
                        state = states.setdefault(machine_id, {})
                        key = _signal_key(observation)
                        if key not in state and len(state) >= self.max_signals_per_machine:
                            raise _validation(
                                "telemetry-signal-limit-exceeded",
                                "content.observations",
                                "materialized machine state exceeds the configured signal limit",
                            )
                        state[key] = _signal_value(observation)
                        encoded_row = _canonical_bytes(
                            self._snapshot_row(observation, state), "snapshot"
                        ) + b"\n"
                        output_size += len(encoded_row)
                        if output_size > self.max_materialization_bytes:
                            raise _validation(
                                "telemetry-materialization-too-large",
                                "telemetry_path",
                                "snapshot exceeds the configured materialization limit",
                            )
                        handle.write(encoded_row)
                        output_hash.update(encoded_row)
                        row_count += 1
                    handle.flush()
                    os.fsync(handle.fileno())
                if row_count == 0:
                    raise _validation(
                        "telemetry-materialization-empty",
                        "telemetry_path",
                        "cannot expose an empty recorder materialization",
                    )
                other_materialized_bytes = connection.execute(
                    """
                    SELECT COALESCE(SUM(size_bytes), 0) FROM materializations
                    WHERE NOT (
                        session_id = ? AND group_id = ?
                        AND dataset_id = ? AND instance_id = ?
                    )
                    """,
                    (session_id, group_id, dataset_id, instance_id),
                ).fetchone()[0]
                if other_materialized_bytes + output_size > self.max_materialization_bytes:
                    raise _validation(
                        "telemetry-materialization-quota-exceeded",
                        "telemetry_path",
                        "snapshot would exceed the total materialization quota",
                    )
                os.replace(temporary_path, final_path)
                _fsync_directory(final_path.parent)
                connection.execute(
                    """
                    INSERT INTO materializations(
                        session_id, group_id, dataset_id, instance_id, relative_path,
                        row_count, size_bytes, content_hash, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, group_id, dataset_id, instance_id)
                    DO UPDATE SET
                        relative_path = excluded.relative_path,
                        row_count = excluded.row_count,
                        size_bytes = excluded.size_bytes,
                        content_hash = excluded.content_hash,
                        updated_at = excluded.updated_at
                    """,
                    (
                        session_id,
                        group_id,
                        dataset_id,
                        instance_id,
                        relative_path,
                        row_count,
                        output_size,
                        f"sha256:{output_hash.hexdigest()}",
                        _utc_now(),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                temporary_path.unlink(missing_ok=True)
                raise
        return final_path

    def rebuild(self) -> TelemetryMirrorRebuildResult:
        """Revalidate immutable blobs, rebuild the index, and rewrite snapshots."""

        batches: list[_ValidatedBatch] = []
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT reference_json, blob_path FROM batches
                ORDER BY session_id, group_id, dataset_id, first_sequence, batch_id
                """
            ).fetchall()
        for row in rows:
            reference_value = _json_without_duplicate_keys(
                row["reference_json"].encode("utf-8"), "reference_json"
            )
            reference = CommittedBatchReference.from_dict(reference_value)
            expected_relative = self._blob_relative_path_from_reference(reference)
            if row["blob_path"] != expected_relative:
                raise _validation(
                    "unsafe-telemetry-path",
                    "blob_path",
                    "stored blob path is not derived from its immutable identity",
                )
            blob_path = self._managed_path(expected_relative, "blob_path")
            try:
                stat = blob_path.stat()
                if stat.st_size > self.max_batch_bytes:
                    raise _validation(
                        "telemetry-batch-too-large",
                        "blob_path",
                        "stored batch exceeds the configured batch limit",
                    )
                encoded = blob_path.read_bytes()
            except OSError as exc:
                raise _validation(
                    "telemetry-blob-unavailable",
                    "blob_path",
                    "could not read an indexed immutable batch",
                ) from exc
            content = _json_without_duplicate_keys(encoded, "blob_path")
            batch = self._validate(reference, content)
            if encoded != batch.encoded:
                raise _validation(
                    "telemetry-blob-integrity-failed",
                    "blob_path",
                    "immutable batch is not stored in canonical form",
                )
            batches.append(batch)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TEMP TABLE rebuilt_observations (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    instance_id INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,
                    machine_id TEXT NOT NULL,
                    observation_hash TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    PRIMARY KEY (
                        session_id, group_id, dataset_id, instance_id, sequence
                    )
                ) WITHOUT ROWID
                """
            )
            observation_count = 0
            for batch in batches:
                reference = batch.reference
                for observation in batch.observations:
                    observation_json = _canonical_bytes(observation).decode("utf-8")
                    observation_hash = "sha256:" + hashlib.sha256(
                        observation_json.encode("utf-8")
                    ).hexdigest()
                    existing = connection.execute(
                        """
                        SELECT observation_hash FROM rebuilt_observations
                        WHERE session_id = ? AND group_id = ? AND dataset_id = ?
                          AND instance_id = ? AND sequence = ?
                        """,
                        (
                            reference.session_id,
                            reference.group_id,
                            reference.dataset_id,
                            batch.instance_id,
                            observation["sequence"],
                        ),
                    ).fetchone()
                    if existing is not None:
                        if existing["observation_hash"] != observation_hash:
                            connection.rollback()
                            raise _validation(
                                "telemetry-mirror-sequence-conflict",
                                "blob_path",
                                "immutable blobs disagree about one source sequence",
                            )
                        continue
                    connection.execute(
                        """
                        INSERT INTO rebuilt_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            reference.session_id,
                            reference.group_id,
                            reference.dataset_id,
                            batch.instance_id,
                            observation["sequence"],
                            observation["machine_id"],
                            observation_hash,
                            observation_json,
                            reference.batch_id,
                        ),
                    )
                    observation_count += 1
            connection.execute("DELETE FROM observations")
            connection.execute(
                "INSERT INTO observations SELECT * FROM rebuilt_observations"
            )
            connection.commit()

        keys = {
            (
                batch.reference.session_id,
                batch.reference.group_id,
                batch.reference.dataset_id,
                batch.instance_id,
            )
            for batch in batches
        }
        expected_paths: set[Path] = set()
        for key in sorted(keys):
            expected_paths.add(self._materialize(*key))
        for path in self.telemetry_root.glob("*.jsonl"):
            self._assert_managed(path, "telemetry_path")
            if (
                _GENERATED_SNAPSHOT_FILENAME.fullmatch(path.name) is not None
                and path not in expected_paths
            ):
                path.unlink()
        return TelemetryMirrorRebuildResult(
            batches=len(batches),
            observations=observation_count,
            materializations=len(keys),
        )

    def _blob_relative_path_from_reference(
        self, reference: CommittedBatchReference
    ) -> str:
        digest = _identity_digest(
            reference.session_id,
            reference.group_id,
            reference.dataset_id,
            reference.batch_id,
            reference.content_hash,
        )
        return f"batches/v1/{digest[:2]}/{digest[2:4]}/{digest}.json"

    def rebuild_materializations(self) -> int:
        """Rewrite scan-visible snapshots from the verified SQLite index."""

        with self._connect() as connection:
            keys = connection.execute(
                """
                SELECT DISTINCT session_id, group_id, dataset_id, instance_id
                FROM observations
                ORDER BY session_id, group_id, dataset_id, instance_id
                """
            ).fetchall()
        for row in keys:
            self._materialize(
                row["session_id"],
                row["group_id"],
                row["dataset_id"],
                row["instance_id"],
            )
        return len(keys)

    def quarantine_count(self) -> int:
        """Return the number of rejected immutable-identity conflicts."""

        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM quarantines").fetchone()[0])


__all__ = [
    "DEFAULT_MAX_BATCH_BYTES",
    "DEFAULT_MAX_MATERIALIZATION_BYTES",
    "DEFAULT_MAX_RECORDS_PER_BATCH",
    "DEFAULT_MAX_SIGNALS_PER_MACHINE",
    "DEFAULT_MAX_TOTAL_BYTES",
    "RECORDER_CONTENT_SCHEMA",
    "RECORDER_DATASET_SCHEMA_NAME",
    "RECORDER_DATASET_SCHEMA_VERSION",
    "RECORDER_OBSERVATION_SCHEMA",
    "RECORDER_SNAPSHOT_SCHEMA",
    "FederatedTelemetryMirror",
    "TelemetryMirrorIngestResult",
    "TelemetryMirrorIngestState",
    "TelemetryMirrorRebuildResult",
]

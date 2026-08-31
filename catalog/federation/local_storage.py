"""Local D1 storage service and filesystem provider for ``fcp-storage-v1``.

The implementation is deliberately in-process.  It proves provider conformance,
immutable ingest, atomic publication, and durable idempotency without networking,
coordinator integration, PostgreSQL, or recorder migration.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from .errors import FederationValidationError
from .host_resources import HostResourceRefused, ProcessResourceAdmission
from .process_resource_admission import PROCESS_RESOURCE_ADMISSION
from .sqlite_schema import SQLiteMigration, ensure_sqlite_schema
from .storage_allocation import StorageAllocation
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    BatchIngestState,
    StorageError,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    classify_idempotent_ingest,
)


class BatchStorageProvider(Protocol):
    """Provider-neutral D1 contract used by the local dispatcher."""

    def ingest(self, request: BatchIngestRequest) -> BatchIngestResult: ...

    def exists(self, *, session_id: str, group_id: str, batch_id: str) -> bool: ...

    def committed_identity(
        self,
        *,
        session_id: str,
        group_id: str,
        batch_id: str,
    ) -> CommittedBatchIdentity | None: ...

    def read(
        self, *, session_id: str, group_id: str, batch_id: str
    ) -> object | None: ...

    def describe(self) -> dict[str, object]: ...

    def health(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class CommittedBatchIdentity:
    dataset_id: str
    dataset_schema_name: str
    dataset_schema_version: int
    batch_id: str
    idempotency_key: str
    content_hash: str


STORAGE_INDEX_SCHEMA_NAME = "federation.filesystem_batch_storage"
STORAGE_INDEX_SCHEMA_VERSION = 2
_STORAGE_INDEX_BASE_COLUMNS = frozenset(
    {
        "session_id",
        "group_id",
        "dataset_id",
        "batch_id",
        "idempotency_key",
        "content_hash",
        "relative_path",
        "created_at",
    }
)
_STORAGE_INDEX_SCHEMA_COLUMNS = frozenset(
    {"dataset_schema_name", "dataset_schema_version"}
)
_STORAGE_SQLITE_RESERVE_BYTES = 2 * 1024 * 1024
_STORAGE_SQLITE_RESERVE_INODES = 4
_STORAGE_PUBLICATION_RESERVE_INODES = 3


def _storage_index_table_exists(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='committed_batches'"
        ).fetchone()
        is not None
    )


def _storage_index_columns(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(committed_batches)")
    }


def _detect_storage_index_version(connection: sqlite3.Connection) -> int:
    if not _storage_index_table_exists(connection):
        return 0
    columns = _storage_index_columns(connection)
    missing_base = _STORAGE_INDEX_BASE_COLUMNS.difference(columns)
    if missing_base:
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            f"legacy storage index is missing required columns: {sorted(missing_base)!r}",
        )
    return (
        STORAGE_INDEX_SCHEMA_VERSION
        if _STORAGE_INDEX_SCHEMA_COLUMNS.issubset(columns)
        else 1
    )


def _create_storage_index_v1(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE committed_batches (
            session_id TEXT NOT NULL,
            group_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            relative_path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (session_id, group_id, batch_id),
            UNIQUE (session_id, group_id, idempotency_key)
        )
        """
    )


def _upgrade_storage_index_v2(connection: sqlite3.Connection) -> None:
    columns = _storage_index_columns(connection)
    if "dataset_schema_name" not in columns:
        connection.execute(
            """ALTER TABLE committed_batches
               ADD COLUMN dataset_schema_name TEXT NOT NULL
               DEFAULT 'fcp.storage.dataset.opaque'"""
        )
    if "dataset_schema_version" not in columns:
        connection.execute(
            """ALTER TABLE committed_batches
               ADD COLUMN dataset_schema_version INTEGER NOT NULL
               DEFAULT 1 CHECK(dataset_schema_version > 0)"""
        )


def _validate_storage_index(connection: sqlite3.Connection) -> None:
    if not _storage_index_table_exists(connection):
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            "committed_batches table is missing",
        )
    missing = (_STORAGE_INDEX_BASE_COLUMNS | _STORAGE_INDEX_SCHEMA_COLUMNS).difference(
        _storage_index_columns(connection)
    )
    if missing:
        raise FederationValidationError(
            "unsupported-storage-index-schema",
            "schema",
            f"storage index is missing required columns: {sorted(missing)!r}",
        )


class FilesystemBatchStorageProvider:
    """Immutable JSON batch provider with a durable SQLite identity catalogue.

    Batch files are published with ``os.replace``.  The catalogue exposes only
    committed batches, so interrupted writes cannot become visible through the
    provider API.  Orphan temporary/final files may be reconciled later without
    weakening the D1 visibility guarantee.
    """

    def __init__(
        self,
        root: Path,
        *,
        allocation: StorageAllocation | None = None,
        resource_admission: ProcessResourceAdmission | None = None,
    ) -> None:
        self.root = Path(root)
        self.batch_root = self.root / "batches"
        self.database_path = self.root / "storage-index.sqlite3"
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
        # StorageAllocation remains the contribution/failover budget. The
        # process-wide admission is an additional host guard for every provider,
        # including providers without a private allocation.
        self.allocation = allocation
        with self._reserve_resources(
            (
                (self.root, _STORAGE_SQLITE_RESERVE_BYTES, _STORAGE_SQLITE_RESERVE_INODES),
                (self.batch_root, 0, 2),
            )
        ):
            self.batch_root.mkdir(parents=True, exist_ok=True)
            self._initialize()

    @contextmanager
    def _reserve_resources(
        self,
        requirements: Iterable[tuple[Path, int, int]],
    ) -> Iterator[tuple[object, ...]]:
        reserve_many = getattr(self.resource_admission, "reserve_many", None)
        if callable(reserve_many):
            with reserve_many(requirements) as reservations:
                yield tuple(reservations)
            return
        with ExitStack() as stack:
            reservations = tuple(
                stack.enter_context(
                    self.resource_admission.reserve(
                        path,
                        bytes_required=bytes_required,
                        inodes_required=inodes_required,
                    )
                )
                for path, bytes_required, inodes_required in requirements
            )
            yield reservations

    def _claim(self, nbytes: int) -> AbstractContextManager[None]:
        """Hold the bytes a batch needs, or refuse before anything is written."""

        if self.allocation is None:
            return nullcontext()
        return self.allocation.claimed(nbytes)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        # Keep the journal from growing without bound between ingest calls.
        # The durable catalogue itself is retained for idempotency; its WAL is
        # only an implementation journal and may be checkpointed safely.
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA wal_autocheckpoint = 100")
        connection.execute("PRAGMA journal_size_limit = 1048576")
        return connection

    def _assert_resource_identity(
        self,
        reservations: tuple[object, ...],
        *paths: Path,
    ) -> None:
        """Fail closed if mkdir/replace crossed to another backing resource."""
        resource_ids = {
            str(getattr(reservation, "resource_id")) for reservation in reservations
        }
        measure = getattr(self.resource_admission, "_measure", None)
        if not callable(measure):
            return
        for path in paths:
            measurement = measure(path)
            if not measurement.available or measurement.resource_id not in resource_ids:
                raise FederationValidationError(
                    "storage-backing-resource-changed",
                    "path",
                    "storage path no longer resolves to the admitted backing resource",
                )

    def _initialize(self) -> None:
        with self._connect() as connection:
            ensure_sqlite_schema(
                connection,
                schema_name=STORAGE_INDEX_SCHEMA_NAME,
                target_version=STORAGE_INDEX_SCHEMA_VERSION,
                migrations=(
                    SQLiteMigration(1, _create_storage_index_v1),
                    SQLiteMigration(2, _upgrade_storage_index_v2),
                ),
                detect_legacy_version=_detect_storage_index_version,
                validate=_validate_storage_index,
            )

    def _relative_path(self, request: BatchIngestRequest) -> Path:
        # Storage identifiers are deliberately opaque protocol values. Recorder
        # dataset and batch IDs contain colons, and callers may legitimately use
        # characters that are reserved path syntax on one of our supported
        # platforms. Never project those values into a backend path. Hash the
        # complete logical identity and retain the originals only in SQLite.
        identity = json.dumps(
            [
                request.authority.session_id,
                request.authority.group_id,
                request.dataset_id,
                request.batch_id,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = sha256(identity).hexdigest()
        return Path("v2") / digest[:2] / digest[2:4] / f"{digest}.json"

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        """Persist an atomic rename on platforms that expose directory fsync."""

        if os.name == "nt":
            # Windows does not allow opening a directory with os.open(). The
            # file itself is flushed before ReplaceFile semantics are used.
            return
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _stored_path(self, relative_path: object) -> Path:
        if not isinstance(relative_path, str) or not relative_path:
            raise FederationValidationError(
                "stored-batch-integrity-failed",
                "relative_path",
                "stored batch catalogue path is malformed",
            )
        candidate = (self.batch_root / relative_path).resolve()
        try:
            candidate.relative_to(self.batch_root.resolve())
        except ValueError as exc:
            raise FederationValidationError(
                "stored-batch-integrity-failed",
                "relative_path",
                "stored batch catalogue path escapes the managed storage root",
            ) from exc
        return candidate

    def _by_idempotency(
        self,
        connection: sqlite3.Connection,
        request: BatchIngestRequest,
    ) -> CommittedBatchIdentity | None:
        row = connection.execute(
            """SELECT dataset_id, dataset_schema_name,
                      dataset_schema_version, batch_id, idempotency_key,
                      content_hash
               FROM committed_batches
               WHERE session_id = ? AND group_id = ? AND idempotency_key = ?""",
            (
                request.authority.session_id,
                request.authority.group_id,
                request.idempotency_key,
            ),
        ).fetchone()
        return (
            None
            if row is None
            else CommittedBatchIdentity(
                row["dataset_id"],
                row["dataset_schema_name"],
                int(row["dataset_schema_version"]),
                row["batch_id"],
                row["idempotency_key"],
                row["content_hash"],
            )
        )

    def ingest(self, request: BatchIngestRequest) -> BatchIngestResult:
        """Admit filesystem, SQLite, and WAL growth before opening the writer."""
        request.validate_content_hash()
        payload = json.dumps(
            request.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        try:
            with self._reserve_resources(
                (
                    (
                        self.root,
                        len(payload) + _STORAGE_SQLITE_RESERVE_BYTES,
                        _STORAGE_SQLITE_RESERVE_INODES,
                    ),
                    (
                        self.batch_root,
                        len(payload),
                        _STORAGE_PUBLICATION_RESERVE_INODES,
                    ),
                )
            ) as reservations:
                return self._ingest_unadmitted(request, reservations)
        except HostResourceRefused as exc:
            raise FederationValidationError(
                "storage-resource-pressure",
                "resource",
                "storage ingest refused under host resource pressure",
            ) from exc

    def _ingest_unadmitted(
        self,
        request: BatchIngestRequest,
        reservations: tuple[object, ...],
    ) -> BatchIngestResult:
        request.validate_content_hash()
        relative_path = self._relative_path(request)
        final_path = self._stored_path(relative_path.as_posix())
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self._assert_resource_identity(reservations, final_path.parent, self.root)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._by_idempotency(connection, request)
            if existing is not None:
                if existing.dataset_id != request.dataset_id:
                    raise FederationValidationError(
                        StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                        "dataset_id",
                        "idempotency identity was previously committed to another dataset",
                    )
                if (
                    existing.dataset_schema_name != request.dataset_schema_name
                    or existing.dataset_schema_version != request.dataset_schema_version
                ):
                    raise FederationValidationError(
                        StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                        "dataset_schema_name",
                        "dataset schema changed for an immutable batch",
                    )
                state = classify_idempotent_ingest(
                    existing_batch_id=existing.batch_id,
                    existing_content_hash=existing.content_hash,
                    requested_batch_id=request.batch_id,
                    requested_content_hash=request.content_hash,
                )
                return BatchIngestResult(
                    request.batch_id,
                    request.idempotency_key,
                    request.content_hash,
                    state,
                )

            by_batch = connection.execute(
                """SELECT dataset_id, dataset_schema_name,
                          dataset_schema_version, idempotency_key, content_hash
                   FROM committed_batches
                   WHERE session_id = ? AND group_id = ? AND batch_id = ?""",
                (
                    request.authority.session_id,
                    request.authority.group_id,
                    request.batch_id,
                ),
            ).fetchone()
            if by_batch is not None:
                field = (
                    "dataset_id"
                    if by_batch["dataset_id"] != request.dataset_id
                    else (
                        "dataset_schema_name"
                        if (
                            by_batch["dataset_schema_name"]
                            != request.dataset_schema_name
                            or int(by_batch["dataset_schema_version"])
                            != request.dataset_schema_version
                        )
                        else "batch_id"
                    )
                )
                raise FederationValidationError(
                    StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                    field,
                    "was previously committed with different immutable batch identity",
                )

            payload = json.dumps(
                request.to_dict(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            # Claimed before the temporary file exists, so a refusal leaves no
            # partial batch, no catalogue row and no consumed bytes. Both
            # idempotent returns above have already happened, so re-delivering
            # a batch this device already holds is never charged twice.
            with self._claim(len(payload.encode("utf-8"))):
                fd, temporary_name = tempfile.mkstemp(
                    prefix=f".{final_path.stem}-",
                    suffix=".tmp",
                    dir=final_path.parent,
                )
                temporary_path = Path(temporary_name)
                try:
                    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary_path, final_path)
                    self._assert_resource_identity(
                        reservations,
                        final_path.parent,
                        self.database_path.parent,
                    )
                    self._fsync_directory(final_path.parent)
                    connection.execute(
                        """INSERT INTO committed_batches
                           (session_id, group_id, dataset_id, dataset_schema_name,
                            dataset_schema_version, batch_id, idempotency_key,
                            content_hash, relative_path, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            request.authority.session_id,
                            request.authority.group_id,
                            request.dataset_id,
                            request.dataset_schema_name,
                            request.dataset_schema_version,
                            request.batch_id,
                            request.idempotency_key,
                            request.content_hash,
                            relative_path.as_posix(),
                            request.created_at.isoformat(),
                        ),
                    )
                    connection.commit()
                    try:
                        connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    except sqlite3.Error:
                        # A concurrent reader may defer a checkpoint; the WAL
                        # autochekpoint and journal-size limit remain active.
                        pass
                except Exception:
                    temporary_path.unlink(missing_ok=True)
                    if final_path.exists():
                        final_path.unlink(missing_ok=True)
                    connection.rollback()
                    raise

        return BatchIngestResult(
            request.batch_id,
            request.idempotency_key,
            request.content_hash,
            BatchIngestState.STORED,
        )

    def exists(self, *, session_id: str, group_id: str, batch_id: str) -> bool:
        return (
            self.committed_identity(
                session_id=session_id,
                group_id=group_id,
                batch_id=batch_id,
            )
            is not None
        )

    def committed_identity(
        self,
        *,
        session_id: str,
        group_id: str,
        batch_id: str,
    ) -> CommittedBatchIdentity | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT dataset_id, dataset_schema_name,
                          dataset_schema_version, batch_id, idempotency_key,
                          content_hash
                   FROM committed_batches
                   WHERE session_id = ? AND group_id = ? AND batch_id = ?""",
                (session_id, group_id, batch_id),
            ).fetchone()
        return (
            None
            if row is None
            else CommittedBatchIdentity(
                row["dataset_id"],
                row["dataset_schema_name"],
                int(row["dataset_schema_version"]),
                row["batch_id"],
                row["idempotency_key"],
                row["content_hash"],
            )
        )

    def read(self, *, session_id: str, group_id: str, batch_id: str) -> object | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT dataset_id, dataset_schema_name,
                          dataset_schema_version, idempotency_key, content_hash,
                          relative_path
                   FROM committed_batches
                   WHERE session_id = ? AND group_id = ? AND batch_id = ?""",
                (session_id, group_id, batch_id),
            ).fetchone()
        if row is None:
            return None
        path = self._stored_path(row["relative_path"])
        with path.open("r", encoding="utf-8") as handle:
            request = BatchIngestRequest.from_dict(json.load(handle))
        request.validate_content_hash()
        if (
            request.authority.session_id != session_id
            or request.authority.group_id != group_id
            or request.dataset_id != row["dataset_id"]
            or request.dataset_schema_name != row["dataset_schema_name"]
            or request.dataset_schema_version != int(row["dataset_schema_version"])
            or request.batch_id != batch_id
            or request.idempotency_key != row["idempotency_key"]
            or request.content_hash != row["content_hash"]
        ):
            raise FederationValidationError(
                "stored-batch-integrity-failed",
                "batch_id",
                "stored batch content does not match its committed catalogue identity",
            )
        return request.content

    def describe(self) -> dict[str, object]:
        return {
            "protocol": STORAGE_PROTOCOL,
            "protocol_version": STORAGE_PROTOCOL_VERSION,
            "backend": "filesystem",
        }

    def health(self) -> dict[str, object]:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return {"status": "ready", "durable": True}
        except sqlite3.Error as exc:
            return {"status": "unavailable", "durable": True, "detail": str(exc)}


class LocalStorageService:
    """Provider-neutral in-process dispatcher for D1-supported operations."""

    def __init__(self, provider: BatchStorageProvider) -> None:
        self.provider = provider

    def dispatch(self, envelope: StorageRequestEnvelope) -> StorageResponseEnvelope:
        try:
            result = self._dispatch(envelope)
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=True,
                result=result,
            )
        except FederationValidationError as exc:
            try:
                code = StorageErrorCode(exc.code)
            except ValueError:
                code = StorageErrorCode.INVALID_REQUEST
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(code=code, message=exc.message, field=exc.field),
            )
        except (OSError, sqlite3.Error) as exc:
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.INTERNAL_ERROR,
                    message=str(exc),
                    retryable=True,
                ),
            )

    def _dispatch(self, envelope: StorageRequestEnvelope) -> dict[str, object]:
        if envelope.operation is StorageOperation.DESCRIBE:
            return self.provider.describe()
        if envelope.operation is StorageOperation.HEALTH:
            return self.provider.health()
        if envelope.operation is StorageOperation.BATCH_INGEST:
            request = BatchIngestRequest.from_dict(envelope.payload)
            if request.authority.session_id != envelope.session_id:
                raise FederationValidationError(
                    "invalid-request",
                    "session_id",
                    "envelope and authority session differ",
                )
            if request.authority.actor_node_id != envelope.actor_node_id:
                raise FederationValidationError(
                    "invalid-request",
                    "actor_node_id",
                    "envelope and authority actor differ",
                )
            return self.provider.ingest(request).to_dict()
        if envelope.operation in {
            StorageOperation.BATCH_EXISTS,
            StorageOperation.BATCH_READ,
        }:
            group_id = str(envelope.payload.get("group_id", ""))
            batch_id = str(envelope.payload.get("batch_id", ""))
            if not group_id or not batch_id:
                raise FederationValidationError(
                    "missing-field", "payload", "group_id and batch_id are required"
                )
            exists = self.provider.exists(
                session_id=envelope.session_id, group_id=group_id, batch_id=batch_id
            )
            if envelope.operation is StorageOperation.BATCH_EXISTS:
                return {"exists": exists}
            if not exists:
                raise FederationValidationError(
                    StorageErrorCode.BATCH_NOT_FOUND.value,
                    "batch_id",
                    "batch does not exist",
                )
            return {
                "content": self.provider.read(
                    session_id=envelope.session_id, group_id=group_id, batch_id=batch_id
                )
            }
        raise FederationValidationError(
            "unsupported-operation", "operation", "operation is deferred beyond D1"
        )

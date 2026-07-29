"""Local D1 storage service and filesystem provider for ``msh-storage-v1``.

The implementation is deliberately in-process.  It proves provider conformance,
immutable ingest, atomic publication, and durable idempotency without networking,
coordinator integration, PostgreSQL, or recorder migration.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .errors import FederationValidationError
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

    def read(self, *, session_id: str, group_id: str, batch_id: str) -> object | None: ...

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


class FilesystemBatchStorageProvider:
    """Immutable JSON batch provider with a durable SQLite identity catalogue.

    Batch files are published with ``os.replace``.  The catalogue exposes only
    committed batches, so interrupted writes cannot become visible through the
    provider API.  Orphan temporary/final files may be reconciled later without
    weakening the D1 visibility guarantee.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.batch_root = self.root / "batches"
        self.batch_root.mkdir(parents=True, exist_ok=True)
        self.database_path = self.root / "storage-index.sqlite3"
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS committed_batches (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    dataset_schema_name TEXT NOT NULL
                        DEFAULT 'msh.storage.dataset.opaque',
                    dataset_schema_version INTEGER NOT NULL DEFAULT 1
                        CHECK(dataset_schema_version > 0),
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
            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(committed_batches)"
                )
            }
            if "dataset_schema_name" not in columns:
                connection.execute(
                    """ALTER TABLE committed_batches
                       ADD COLUMN dataset_schema_name TEXT NOT NULL
                       DEFAULT 'msh.storage.dataset.opaque'"""
                )
            if "dataset_schema_version" not in columns:
                connection.execute(
                    """ALTER TABLE committed_batches
                       ADD COLUMN dataset_schema_version INTEGER NOT NULL
                       DEFAULT 1 CHECK(dataset_schema_version > 0)"""
                )

    @staticmethod
    def _safe_component(value: str) -> str:
        if value in {".", ".."} or "/" in value or "\\" in value:
            raise FederationValidationError("invalid-storage-id", "identifier", "must not contain path separators")
        return value

    def _relative_path(self, request: BatchIngestRequest) -> Path:
        values = (
            request.authority.session_id,
            request.authority.group_id,
            request.dataset_id,
            request.batch_id,
        )
        session_id, group_id, dataset_id, batch_id = map(self._safe_component, values)
        return Path(session_id) / group_id / dataset_id / f"{batch_id}.json"

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
            (request.authority.session_id, request.authority.group_id, request.idempotency_key),
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
        request.validate_content_hash()
        relative_path = self._relative_path(request)
        final_path = self.batch_root / relative_path
        final_path.parent.mkdir(parents=True, exist_ok=True)

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
                    existing.dataset_schema_name
                    != request.dataset_schema_name
                    or existing.dataset_schema_version
                    != request.dataset_schema_version
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
                return BatchIngestResult(request.batch_id, request.idempotency_key, request.content_hash, state)

            by_batch = connection.execute(
                """SELECT dataset_id, dataset_schema_name,
                          dataset_schema_version, idempotency_key, content_hash
                   FROM committed_batches
                   WHERE session_id = ? AND group_id = ? AND batch_id = ?""",
                (request.authority.session_id, request.authority.group_id, request.batch_id),
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

            payload = json.dumps(request.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            fd, temporary_name = tempfile.mkstemp(prefix=f".{request.batch_id}-", suffix=".tmp", dir=final_path.parent)
            temporary_path = Path(temporary_name)
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_path, final_path)
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
                """SELECT relative_path FROM committed_batches
                   WHERE session_id = ? AND group_id = ? AND batch_id = ?""",
                (session_id, group_id, batch_id),
            ).fetchone()
        if row is None:
            return None
        path = self.batch_root / row["relative_path"]
        with path.open("r", encoding="utf-8") as handle:
            return BatchIngestRequest.from_dict(json.load(handle)).content

    def describe(self) -> dict[str, object]:
        return {"protocol": STORAGE_PROTOCOL, "protocol_version": STORAGE_PROTOCOL_VERSION, "backend": "filesystem"}

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
                error=StorageError(code=StorageErrorCode.INTERNAL_ERROR, message=str(exc), retryable=True),
            )

    def _dispatch(self, envelope: StorageRequestEnvelope) -> dict[str, object]:
        if envelope.operation is StorageOperation.DESCRIBE:
            return self.provider.describe()
        if envelope.operation is StorageOperation.HEALTH:
            return self.provider.health()
        if envelope.operation is StorageOperation.BATCH_INGEST:
            request = BatchIngestRequest.from_dict(envelope.payload)
            if request.authority.session_id != envelope.session_id:
                raise FederationValidationError("invalid-request", "session_id", "envelope and authority session differ")
            if request.authority.actor_node_id != envelope.actor_node_id:
                raise FederationValidationError("invalid-request", "actor_node_id", "envelope and authority actor differ")
            return self.provider.ingest(request).to_dict()
        if envelope.operation in {StorageOperation.BATCH_EXISTS, StorageOperation.BATCH_READ}:
            group_id = str(envelope.payload.get("group_id", ""))
            batch_id = str(envelope.payload.get("batch_id", ""))
            if not group_id or not batch_id:
                raise FederationValidationError("missing-field", "payload", "group_id and batch_id are required")
            exists = self.provider.exists(session_id=envelope.session_id, group_id=group_id, batch_id=batch_id)
            if envelope.operation is StorageOperation.BATCH_EXISTS:
                return {"exists": exists}
            if not exists:
                raise FederationValidationError(StorageErrorCode.BATCH_NOT_FOUND.value, "batch_id", "batch does not exist")
            return {"content": self.provider.read(session_id=envelope.session_id, group_id=group_id, batch_id=batch_id)}
        raise FederationValidationError("unsupported-operation", "operation", "operation is deferred beyond D1")

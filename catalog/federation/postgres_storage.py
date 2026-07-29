"""Transactional PostgreSQL provider for the D1 batch-storage contract.

The provider stores immutable JSON batches and their idempotency identity in one
PostgreSQL transaction. It is intentionally local-only in D2: connection details
are supplied by the process and are never advertised through the storage protocol.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .errors import FederationValidationError
from .local_storage import CommittedBatchIdentity
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    BatchIngestState,
    StorageErrorCode,
    classify_idempotent_ingest,
)


class PostgreSQLBatchStorageProvider:
    """PostgreSQL implementation of ``BatchStorageProvider``.

    A caller supplies a DSN or keyword connection string. The value remains local
    process configuration and is never returned from ``describe`` or ``health``.
    """

    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("a non-empty PostgreSQL DSN is required")
        self._dsn = dsn
        self._initialize()

    def _connect(self) -> psycopg.Connection[dict[str, Any]]:
        return psycopg.connect(self._dsn, row_factory=dict_row)

    def _initialize(self) -> None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS msh_storage_batches (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    dataset_schema_name TEXT NOT NULL
                        DEFAULT 'msh.storage.dataset.opaque',
                    dataset_schema_version INTEGER NOT NULL
                        DEFAULT 1 CHECK(dataset_schema_version > 0),
                    batch_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    content JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (session_id, group_id, batch_id),
                    UNIQUE (session_id, group_id, idempotency_key)
                )
                """
            )
            cursor.execute(
                """ALTER TABLE msh_storage_batches
                   ADD COLUMN IF NOT EXISTS dataset_schema_name TEXT NOT NULL
                   DEFAULT 'msh.storage.dataset.opaque'"""
            )
            cursor.execute(
                """ALTER TABLE msh_storage_batches
                   ADD COLUMN IF NOT EXISTS dataset_schema_version
                   INTEGER NOT NULL DEFAULT 1
                   CHECK(dataset_schema_version > 0)"""
            )

    def ingest(self, request: BatchIngestRequest) -> BatchIngestResult:
        request.validate_content_hash()
        authority = request.authority

        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"{authority.session_id}:{authority.group_id}",),
            )
            cursor.execute(
                """
                SELECT dataset_id, dataset_schema_name,
                       dataset_schema_version, batch_id, content_hash
                FROM msh_storage_batches
                WHERE session_id = %s AND group_id = %s AND idempotency_key = %s
                """,
                (
                    authority.session_id,
                    authority.group_id,
                    request.idempotency_key,
                ),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing["dataset_id"] != request.dataset_id:
                    raise FederationValidationError(
                        StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                        "dataset_id",
                        "idempotency identity was previously committed to another dataset",
                    )
                if (
                    existing["dataset_schema_name"]
                    != request.dataset_schema_name
                    or int(existing["dataset_schema_version"])
                    != request.dataset_schema_version
                ):
                    raise FederationValidationError(
                        StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                        "dataset_schema_name",
                        "dataset schema changed for an immutable batch",
                    )
                state = classify_idempotent_ingest(
                    existing_batch_id=existing["batch_id"],
                    existing_content_hash=existing["content_hash"],
                    requested_batch_id=request.batch_id,
                    requested_content_hash=request.content_hash,
                )
                return BatchIngestResult(
                    request.batch_id,
                    request.idempotency_key,
                    request.content_hash,
                    state,
                )

            cursor.execute(
                """
                SELECT dataset_id, dataset_schema_name,
                       dataset_schema_version, idempotency_key, content_hash
                FROM msh_storage_batches
                WHERE session_id = %s AND group_id = %s AND batch_id = %s
                """,
                (authority.session_id, authority.group_id, request.batch_id),
            )
            by_batch = cursor.fetchone()
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

            cursor.execute(
                """
                INSERT INTO msh_storage_batches (
                    session_id, group_id, dataset_id, dataset_schema_name,
                    dataset_schema_version, batch_id,
                    idempotency_key, content_hash, content, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    authority.session_id,
                    authority.group_id,
                    request.dataset_id,
                    request.dataset_schema_name,
                    request.dataset_schema_version,
                    request.batch_id,
                    request.idempotency_key,
                    request.content_hash,
                    json.dumps(
                        request.content,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ),
                    request.created_at,
                ),
            )

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
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT dataset_id, dataset_schema_name,
                       dataset_schema_version, batch_id, idempotency_key,
                       content_hash
                FROM msh_storage_batches
                WHERE session_id = %s AND group_id = %s AND batch_id = %s
                """,
                (session_id, group_id, batch_id),
            )
            row = cursor.fetchone()
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
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT content FROM msh_storage_batches
                WHERE session_id = %s AND group_id = %s AND batch_id = %s
                """,
                (session_id, group_id, batch_id),
            )
            row = cursor.fetchone()
            return None if row is None else row["content"]

    def describe(self) -> dict[str, object]:
        return {
            "protocol": STORAGE_PROTOCOL,
            "protocol_version": STORAGE_PROTOCOL_VERSION,
            "backend": "postgresql",
        }

    def health(self) -> dict[str, object]:
        try:
            with self._connect() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return {"status": "ready", "durable": True}
        except psycopg.Error as exc:
            return {
                "status": "unavailable",
                "durable": True,
                "detail": type(exc).__name__,
            }

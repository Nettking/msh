"""Durable acknowledgement tracking for Phase D storage commits."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .acknowledgement import AcknowledgementPolicy
from .errors import FederationValidationError
from .storage_protocol import BatchIngestRequest


def _time(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "now", "must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class StorageCommitStatus:
    session_id: str
    group_id: str
    batch_id: str
    primary_committed: bool
    required_replica_acks: int
    assigned_replica_ids: tuple[str, ...]
    acknowledged_replica_ids: tuple[str, ...]

    @property
    def committed(self) -> bool:
        return self.primary_committed and (
            len(self.acknowledged_replica_ids) >= self.required_replica_acks
        )


class DurableAcknowledgementStore:
    """SQLite-backed acknowledgement state keyed by immutable logical batch identity."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_commits (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    required_replica_acks INTEGER NOT NULL CHECK(required_replica_acks >= 0),
                    primary_committed INTEGER NOT NULL DEFAULT 0 CHECK(primary_committed IN (0,1)),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id, batch_id),
                    UNIQUE(session_id, group_id, idempotency_key)
                );
                CREATE TABLE IF NOT EXISTS storage_commit_replicas (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0,1)),
                    acknowledged_at TEXT,
                    PRIMARY KEY(session_id, group_id, batch_id, provider_id),
                    FOREIGN KEY(session_id, group_id, batch_id)
                        REFERENCES storage_commits(session_id, group_id, batch_id)
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS storage_commit_replica_pending
                    ON storage_commit_replicas(provider_id, acknowledged, session_id, group_id, batch_id);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def prepare(
        self,
        request: BatchIngestRequest,
        *,
        policy: AcknowledgementPolicy,
        replica_provider_ids: tuple[str, ...],
        now: datetime,
    ) -> StorageCommitStatus:
        replicas = tuple(dict.fromkeys(replica_provider_ids))
        if len(replicas) != len(replica_provider_ids) or any(
            not isinstance(provider_id, str) or not provider_id for provider_id in replicas
        ):
            raise FederationValidationError(
                "invalid-assignment", "replica_provider_ids", "replica providers must be unique non-empty IDs"
            )
        required = policy.required_replica_acks(len(replicas))
        authority = request.authority
        stamp = _time(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM storage_commits
                       WHERE session_id=? AND group_id=? AND batch_id=?""",
                    (authority.session_id, authority.group_id, request.batch_id),
                ).fetchone()
                if row is None:
                    by_key = connection.execute(
                        """SELECT batch_id, content_hash FROM storage_commits
                           WHERE session_id=? AND group_id=? AND idempotency_key=?""",
                        (authority.session_id, authority.group_id, request.idempotency_key),
                    ).fetchone()
                    if by_key is not None:
                        raise FederationValidationError(
                            "idempotency-conflict",
                            "idempotency_key",
                            "identity was already committed for another batch or content hash",
                        )
                    connection.execute(
                        """INSERT INTO storage_commits
                           (session_id, group_id, batch_id, idempotency_key, content_hash,
                            required_replica_acks, primary_committed, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)""",
                        (
                            authority.session_id,
                            authority.group_id,
                            request.batch_id,
                            request.idempotency_key,
                            request.content_hash,
                            required,
                            stamp,
                            stamp,
                        ),
                    )
                    for provider_id in replicas:
                        connection.execute(
                            """INSERT INTO storage_commit_replicas
                               (session_id, group_id, batch_id, provider_id, acknowledged)
                               VALUES (?, ?, ?, ?, 0)""",
                            (authority.session_id, authority.group_id, request.batch_id, provider_id),
                        )
                else:
                    if (
                        row["idempotency_key"] != request.idempotency_key
                        or row["content_hash"] != request.content_hash
                        or int(row["required_replica_acks"]) != required
                    ):
                        raise FederationValidationError(
                            "idempotency-conflict",
                            "batch_id",
                            "commit identity or acknowledgement policy changed for an existing batch",
                        )
                    existing = tuple(
                        item["provider_id"]
                        for item in connection.execute(
                            """SELECT provider_id FROM storage_commit_replicas
                               WHERE session_id=? AND group_id=? AND batch_id=?
                               ORDER BY provider_id""",
                            (authority.session_id, authority.group_id, request.batch_id),
                        )
                    )
                    if existing != tuple(sorted(replicas)):
                        raise FederationValidationError(
                            "replica-set-changed",
                            "replica_provider_ids",
                            "assigned replica set changed while the batch was pending",
                        )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        status = self.status(authority.session_id, authority.group_id, request.batch_id)
        assert status is not None
        return status

    def mark_primary_committed(
        self,
        session_id: str,
        group_id: str,
        batch_id: str,
        *,
        now: datetime,
    ) -> StorageCommitStatus:
        with self._connect() as connection:
            cursor = connection.execute(
                """UPDATE storage_commits SET primary_committed=1, updated_at=?
                   WHERE session_id=? AND group_id=? AND batch_id=?""",
                (_time(now), session_id, group_id, batch_id),
            )
            if cursor.rowcount != 1:
                raise FederationValidationError("commit-not-found", "batch_id", "commit state does not exist")
        status = self.status(session_id, group_id, batch_id)
        assert status is not None
        return status

    def acknowledge(
        self,
        session_id: str,
        group_id: str,
        batch_id: str,
        provider_id: str,
        *,
        now: datetime,
    ) -> StorageCommitStatus:
        if not isinstance(provider_id, str) or not provider_id:
            raise FederationValidationError("invalid-id", "provider_id", "must be non-empty text")
        stamp = _time(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT acknowledged FROM storage_commit_replicas
                   WHERE session_id=? AND group_id=? AND batch_id=? AND provider_id=?""",
                (session_id, group_id, batch_id, provider_id),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise FederationValidationError(
                    "unexpected-replica-ack", "provider_id", "provider is not assigned to this batch"
                )
            if not bool(row["acknowledged"]):
                connection.execute(
                    """UPDATE storage_commit_replicas
                       SET acknowledged=1, acknowledged_at=?
                       WHERE session_id=? AND group_id=? AND batch_id=? AND provider_id=?""",
                    (stamp, session_id, group_id, batch_id, provider_id),
                )
                connection.execute(
                    """UPDATE storage_commits SET updated_at=?
                       WHERE session_id=? AND group_id=? AND batch_id=?""",
                    (stamp, session_id, group_id, batch_id),
                )
            connection.commit()
        status = self.status(session_id, group_id, batch_id)
        assert status is not None
        return status

    def status(self, session_id: str, group_id: str, batch_id: str) -> StorageCommitStatus | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT primary_committed, required_replica_acks FROM storage_commits
                   WHERE session_id=? AND group_id=? AND batch_id=?""",
                (session_id, group_id, batch_id),
            ).fetchone()
            if row is None:
                return None
            replicas = tuple(
                item["provider_id"]
                for item in connection.execute(
                    """SELECT provider_id FROM storage_commit_replicas
                       WHERE session_id=? AND group_id=? AND batch_id=?
                       ORDER BY provider_id""",
                    (session_id, group_id, batch_id),
                )
            )
            acknowledged = tuple(
                item["provider_id"]
                for item in connection.execute(
                    """SELECT provider_id FROM storage_commit_replicas
                       WHERE session_id=? AND group_id=? AND batch_id=? AND acknowledged=1
                       ORDER BY provider_id""",
                    (session_id, group_id, batch_id),
                )
            )
        return StorageCommitStatus(
            session_id,
            group_id,
            batch_id,
            bool(row["primary_committed"]),
            int(row["required_replica_acks"]),
            replicas,
            acknowledged,
        )

    def pending_for_provider(self, provider_id: str) -> tuple[tuple[str, str, str], ...]:
        with self._connect() as connection:
            return tuple(
                (row["session_id"], row["group_id"], row["batch_id"])
                for row in connection.execute(
                    """SELECT session_id, group_id, batch_id
                       FROM storage_commit_replicas
                       WHERE provider_id=? AND acknowledged=0
                       ORDER BY session_id, group_id, batch_id""",
                    (provider_id,),
                )
            )

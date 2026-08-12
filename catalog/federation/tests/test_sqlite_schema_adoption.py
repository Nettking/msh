from __future__ import annotations

import sqlite3
from pathlib import Path

from catalog.federation.commit_tracking import (
    ACKNOWLEDGEMENT_SCHEMA_NAME,
    ACKNOWLEDGEMENT_SCHEMA_VERSION,
    DurableAcknowledgementStore,
)
from catalog.federation.local_storage import (
    STORAGE_INDEX_SCHEMA_NAME,
    STORAGE_INDEX_SCHEMA_VERSION,
    FilesystemBatchStorageProvider,
)
from catalog.federation.sqlite_schema import SQLITE_SCHEMA_VERSION_TABLE


def _version(database: Path, schema_name: str) -> int:
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            f"SELECT version FROM {SQLITE_SCHEMA_VERSION_TABLE} WHERE schema_name=?",
            (schema_name,),
        ).fetchone()
    assert row is not None
    return int(row[0])


def test_filesystem_storage_registers_and_migrates_legacy_index(tmp_path: Path) -> None:
    database = tmp_path / "storage-index.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
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
            );
            INSERT INTO committed_batches
                (session_id, group_id, dataset_id, batch_id, idempotency_key,
                 content_hash, relative_path, created_at)
            VALUES
                ('session-1', 'storage-main', 'telemetry', 'batch-1', 'idem-1',
                 'sha256:legacy', 'legacy/batch-1.json', '2026-08-01T00:00:00+00:00');
            """
        )

    FilesystemBatchStorageProvider(tmp_path)

    assert _version(database, STORAGE_INDEX_SCHEMA_NAME) == STORAGE_INDEX_SCHEMA_VERSION
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM committed_batches").fetchone()
        assert row is not None
        assert row["dataset_id"] == "telemetry"
        assert row["dataset_schema_name"] == "fcp.storage.dataset.opaque"
        assert row["dataset_schema_version"] == 1
        assert row["idempotency_key"] == "idem-1"
        assert connection.execute("SELECT COUNT(*) FROM committed_batches").fetchone()[0] == 1


def test_acknowledgement_store_registers_and_migrates_legacy_state(tmp_path: Path) -> None:
    database = tmp_path / "acknowledgements.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE storage_commits (
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
            CREATE TABLE storage_commit_replicas (
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
            CREATE INDEX storage_commit_replica_pending
                ON storage_commit_replicas(
                    provider_id, acknowledged, session_id, group_id, batch_id
                );
            INSERT INTO storage_commits
                (session_id, group_id, batch_id, idempotency_key, content_hash,
                 required_replica_acks, primary_committed, created_at, updated_at)
            VALUES
                ('session-1', 'storage-main', 'batch-1', 'idem-1', 'sha256:legacy',
                 1, 1, '2026-08-01T00:00:00+00:00', '2026-08-01T00:00:00+00:00');
            INSERT INTO storage_commit_replicas
                (session_id, group_id, batch_id, provider_id, acknowledged, acknowledged_at)
            VALUES
                ('session-1', 'storage-main', 'batch-1', 'storage-b', 1,
                 '2026-08-01T00:01:00+00:00');
            """
        )

    store = DurableAcknowledgementStore(database)
    status = store.status("session-1", "storage-main", "batch-1")

    assert _version(database, ACKNOWLEDGEMENT_SCHEMA_NAME) == ACKNOWLEDGEMENT_SCHEMA_VERSION
    assert status is not None
    assert status.dataset_id == "legacy-unknown-dataset"
    assert status.dataset_schema_name == "fcp.storage.dataset.opaque"
    assert status.dataset_schema_version == 1
    assert status.primary_committed is True
    assert status.acknowledged_replica_ids == ("storage-b",)
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM storage_commits").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM storage_commit_replicas").fetchone() == (1,)


def test_fresh_federation_stores_record_current_schema_versions(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    FilesystemBatchStorageProvider(storage_root)
    acknowledgements = tmp_path / "ack.sqlite3"
    DurableAcknowledgementStore(acknowledgements)

    assert (
        _version(storage_root / "storage-index.sqlite3", STORAGE_INDEX_SCHEMA_NAME)
        == STORAGE_INDEX_SCHEMA_VERSION
    )
    assert _version(acknowledgements, ACKNOWLEDGEMENT_SCHEMA_NAME) == ACKNOWLEDGEMENT_SCHEMA_VERSION

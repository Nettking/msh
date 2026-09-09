"""Owned SQLite handles close deterministically without changing transactions."""

from __future__ import annotations

import builtins
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation import live_catchup, live_failover, local_storage
from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_catchup import (
    LIVE_CATCHUP_SCHEMA,
    LiveCatchupItem,
    LiveCatchupRecord,
    LiveCatchupStore,
)
from catalog.federation.live_failover import (
    LIVE_FAILOVER_SCHEMA,
    LiveFailoverRecord,
    LiveFailoverStore,
)
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.storage_protocol import (
    BatchIngestRequest,
    BatchIngestState,
    WriteAuthority,
)

NOW = datetime(2026, 9, 9, tzinfo=timezone.utc)
KINDS = ("filesystem", "catchup", "failover")


class _Connections:
    def __init__(self, connect, database_root: Path):
        self.raw_connect = connect
        self.database_root = database_root.resolve()
        self.opened: list[sqlite3.Connection] = []
        self.deny_configuration = False

    def __getattr__(self, name):
        return getattr(sqlite3, name)

    def connect(self, database, *args, **kwargs):
        # These stores use ordinary file paths. Observe only this test's databases;
        # another caller of the same store module retains normal SQLite behavior.
        owned = Path(database).resolve().is_relative_to(self.database_root)
        connection = self.raw_connect(database, *args, **kwargs)
        if not owned:
            return connection
        # Real connections stay strongly reachable so GC cannot hide missing close().
        self.opened.append(connection)
        if self.deny_configuration:
            connection.set_authorizer(
                lambda action, name, _value, _database, _trigger: (
                    sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_PRAGMA and name.lower() == "synchronous"
                    else sqlite3.SQLITE_OK
                )
            )
        return connection

    def assert_closed(self):
        assert self.opened, "the exercise did not open a real product connection"
        for connection in self.opened:
            with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
                connection.execute("SELECT 1")


@pytest.fixture
def connections(monkeypatch, tmp_path):
    retained = _Connections(sqlite3.connect, tmp_path)
    # Replace each consumer's reference, never the process-wide sqlite3 module.
    for module in (local_storage, live_catchup, live_failover):
        monkeypatch.setattr(module, "sqlite3", retained)
    try:
        yield retained
    finally:
        for connection in retained.opened:
            connection.close()


def _request(*, batch_id="batch-1", value=1):
    content = {"sequence": value}
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1", group_id="storage-main", actor_node_id="node-a",
            grant_id="grant-1", term=1, fencing_token=1,
            lease_expires_at=NOW + timedelta(minutes=5),
        ),
        dataset_id="telemetry", batch_id=batch_id, idempotency_key="idem-" + batch_id,
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content, created_at=NOW,
    )


def _catchup_record():
    request = _request()
    return LiveCatchupRecord(
        schema=LIVE_CATCHUP_SCHEMA, recovery_id="recovery-1", session_id="session-1",
        group_id="storage-main", failover_id="failover-1",
        source_provider_id="replica", source_node_id="node-b",
        returning_provider_id="former-primary", returning_node_id="node-a",
        manifest_revision=1, manifest_hash="a" * 64, grant_id="grant-2", term=2,
        fencing_token=2, lease_expires_at=NOW + timedelta(minutes=5),
        initial_report_revision=1, initial_report_hash="b" * 64, state="planned",
        items=(LiveCatchupItem(
            item_id=request.batch_id, dataset_id=request.dataset_id,
            schema_name=request.dataset_schema_name, schema_version=request.dataset_schema_version,
            idempotency_key=request.idempotency_key, content_hash=request.content_hash, status="missing",
        ),),
        final_report_revision=None, final_report_hash=None, final_report=None,
        latest_error_code=None, latest_error_reason=None, created_at=NOW, updated_at=NOW,
    )


def _failover_record():
    return LiveFailoverRecord(
        schema=LIVE_FAILOVER_SCHEMA, failover_id="failover-1", session_id="session-1",
        group_id="storage-main", failed_provider_id="former-primary", failed_node_id="node-a",
        source_grant_id="grant-1", source_term=1, source_fencing_token=1,
        promoted_provider_id="replica", promoted_node_id="node-b", selected_report_revision=1,
        selected_report_hash="b" * 64, retained_replica_provider_ids=(),
        previous_acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        effective_acknowledgement_mode=AcknowledgementMode.PRIMARY,
        source_control_revision=1, term=2, fencing_token=2, grant_id="grant-2",
        state="detected", publication=None, reason_code="primary-disconnected",
        reason_detail="original primary disconnected", detected_at=NOW, updated_at=NOW,
    )


def _create(kind, root: Path):
    if kind == "filesystem":
        store = FilesystemBatchStorageProvider(root / "provider")
        return store, _request(), store.database_path, "committed_batches"
    if kind == "catchup":
        path = root / "catchup.sqlite3"
        return LiveCatchupStore(path), _catchup_record(), path, "storage_live_catchups"
    path = root / "failover.sqlite3"
    return LiveFailoverStore(path), _failover_record(), path, "storage_live_failovers"


def _save(kind, store, record):
    if kind == "filesystem":
        return store.ingest(record)
    return store.save(record)


def _assert_stored(kind, store, record):
    if kind == "filesystem":
        identity = store.committed_identity(
            session_id="session-1", group_id="storage-main", batch_id=record.batch_id,
        )
        assert identity is not None and identity.content_hash == record.content_hash
        assert store.read(
            session_id="session-1", group_id="storage-main", batch_id=record.batch_id,
        ) == record.content
        assert store.health() == {"status": "ready", "durable": True}
    elif kind == "catchup":
        assert store.get(record.recovery_id) == record
    else:
        assert store.get(record.session_id, record.group_id, record.failover_id) == record
        assert store.active(record.session_id, record.group_id) == record


@pytest.mark.parametrize("kind", KINDS)
def test_owned_connections_close_after_commit_read_and_semantic_rejection(
    kind, tmp_path, connections,
):
    store, record, database, table = _create(kind, tmp_path)
    first = _save(kind, store, record)
    if kind == "filesystem":
        assert first.state is BatchIngestState.STORED
        assert _save(kind, store, record).state is BatchIngestState.ALREADY_STORED
        conflict = _request(value=2)
    else:
        assert first == record
        assert _save(kind, store, record) == record
        conflict = replace(record, grant_id="different-grant")
    with pytest.raises(FederationValidationError):
        _save(kind, store, conflict)
    _assert_stored(kind, store, record)
    # A separate real connection proves successful writes were committed to disk.
    with closing(connections.raw_connect(database)) as check:
        assert check.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
    connections.assert_closed()


@pytest.mark.parametrize("kind", KINDS)
def test_owned_connections_close_after_sql_failure_and_preserve_rollback(
    kind, tmp_path, connections,
):
    store, record, database, table = _create(kind, tmp_path)
    _save(kind, store, record)
    with closing(connections.raw_connect(database)) as setup, setup:
        setup.execute("CREATE TABLE transaction_probe(value TEXT NOT NULL)")
        # RAISE(FAIL) retains earlier trigger effects until the owner rolls back.
        setup.execute(
            f"""CREATE TRIGGER reject_storage_write BEFORE INSERT ON {table}
                BEGIN
                    INSERT INTO transaction_probe VALUES ('must-roll-back');
                    SELECT RAISE(FAIL, 'ownership-test-write-rejected');
                END"""
        )
    candidate = (
        _request(batch_id="batch-2", value=2)
        if kind == "filesystem"
        else replace(record, updated_at=NOW + timedelta(seconds=1))
    )
    with pytest.raises(sqlite3.IntegrityError, match="ownership-test-write-rejected"):
        _save(kind, store, candidate)
    with closing(connections.raw_connect(database)) as check:
        assert check.execute("SELECT COUNT(*) FROM transaction_probe").fetchone()[0] == 0
        assert check.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
    _assert_stored(kind, store, record)
    with closing(connections.raw_connect(database)) as setup, setup:
        setup.execute("DROP TRIGGER reject_storage_write")
    _save(kind, store, candidate)
    _assert_stored(kind, store, candidate)
    connections.assert_closed()


@pytest.mark.parametrize("kind", KINDS)
def test_connection_is_closed_when_sqlite_configuration_is_denied(
    kind, tmp_path, connections,
):
    connections.deny_configuration = True
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        _create(kind, tmp_path)
    assert len(connections.opened) == 1
    connections.assert_closed()


@pytest.mark.parametrize("kind", ("raw", *KINDS))
def test_connection_observer_preserves_unrelated_worker_databases(
    kind, tmp_path, tmp_path_factory, connections,
):
    _create("filesystem", tmp_path)
    connections.assert_closed()
    owned_before = tuple(connections.opened)
    connections.deny_configuration = True
    unrelated_root = tmp_path_factory.mktemp("unrelated-sqlite-owner")
    completed = []
    failures = []

    def exercise_unrelated_database():
        try:
            if kind == "raw":
                database = unrelated_root / "ordinary.sqlite3"
                with closing(sqlite3.connect(database)) as connection, connection:
                    connection.execute("PRAGMA synchronous=FULL")
                    connection.execute("CREATE TABLE probe(value INTEGER NOT NULL)")
                    connection.execute("INSERT INTO probe VALUES (7)")
                with closing(sqlite3.connect(database)) as connection:
                    assert connection.execute("SELECT value FROM probe").fetchone()[0] == 7
            else:
                store, record, _database, _table = _create(kind, unrelated_root)
                _save(kind, store, record)
                _assert_stored(kind, store, record)
            completed.append(True)
        except BaseException as error:  # noqa: BLE001 - report the owned worker failure after joining it
            failures.append(error)

    worker = threading.Thread(
        target=exercise_unrelated_database, name="unrelated-sqlite-owner",
    )
    worker.start()
    worker.join()
    assert not worker.is_alive()
    if failures:
        raise builtins.BaseExceptionGroup("unrelated SQLite worker failed", failures)
    assert completed == [True]
    assert sqlite3.connect is connections.raw_connect
    assert tuple(connections.opened) == owned_before
    connections.assert_closed()

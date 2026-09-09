"""Public-journal staging has one owner, one commit and fail-closed rollback."""

from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager

import pytest

from catalog.federation.control_plane_journal_store import JournalCoordinatorStore
from catalog.federation.persistence import CoordinatorStore


class _Controller:
    """Minimal real staging controller; quorum decisions belong to runtime tests."""

    def __init__(self) -> None:
        self.calls = 0

    @contextmanager
    def transaction(self, store):
        self.calls += 1
        with store.raw_transaction() as database:
            yield database


@pytest.fixture
def store(tmp_path):
    value = JournalCoordinatorStore(tmp_path / "coordinator.sqlite3")
    # Real schema rows establish FK-valid journal targets. No in-memory journal
    # or substituted SQLite result is used by these transaction-boundary tests.
    with value.projection() as database:
        database.execute(
            """
            INSERT INTO nodes(
                node_id,display_name,public_key,created_at,identity_version,enrolled_at
            ) VALUES('member','Member','public-key','2026-09-08T00:00:00+00:00',1,
                     '2026-09-08T00:00:00+00:00')
            """
        )
        database.execute(
            """
            INSERT INTO sessions(
                session_id,display_name,state,created_at,created_by_node_id,coordinator_id
            ) VALUES('session','Session','active','2026-09-08T00:00:00+00:00',
                     'member','coordinator')
            """
        )
    return value


def _insert(database, revision):
    database.execute(
        """
        INSERT INTO session_events(
            session_id,revision,event_id,request_id,event_type,
            occurred_at,actor_node_id,payload_json,content_hash
        ) VALUES('session',?,?,?,'member.note','2026-09-08T00:00:00+00:00',
                 'member','{}','payload-hash')
        """,
        (revision, f"event-{revision}", f"request-{revision}"),
    )


def _revisions(database):
    return [row[0] for row in database.execute("SELECT revision FROM session_events ORDER BY revision")]


def test_nested_stage_reads_see_writes_and_only_outer_context_commits(store):
    controller = _Controller()
    store.attach_controller(controller)
    with store.transaction() as outer:
        _insert(outer, 1)
        with store.transaction() as inner:
            assert inner is outer
            _insert(inner, 2)
            with store.read_transaction() as reader:
                assert reader is outer
                assert _revisions(reader) == [1, 2]
        # A released savepoint must not publish either staged row to another
        # connection; that connection can only observe the earlier WAL snapshot.
        with closing(sqlite3.connect(store.database)) as observer:
            assert _revisions(observer) == []
        assert controller.calls == 1
    with store.read_transaction() as reader:
        assert _revisions(reader) == [1, 2]


def test_caught_nested_failure_poisoning_prevents_partial_outer_commit(store):
    store.attach_controller(_Controller())
    with (
        pytest.raises(RuntimeError, match="rollback-only after a nested failure"),
        store.transaction() as outer,
    ):
        _insert(outer, 1)
        with pytest.raises(ValueError, match="nested refusal"), store.transaction() as inner:
            _insert(inner, 2)
            raise ValueError("nested refusal")
        # The inner savepoint is rolled back immediately, but catching its
        # error cannot turn the rest of this operation into a partial commit.
        assert _revisions(outer) == [1]
        _insert(outer, 3)
    with store.read_transaction() as reader:
        assert _revisions(reader) == []
    # Rollback-only and ownership must not leak into the next independent stage.
    with store.transaction() as database:
        _insert(database, 1)
    with store.read_transaction() as reader:
        assert _revisions(reader) == [1]


def test_outer_failure_rolls_back_successful_inner_savepoint(store):
    store.attach_controller(_Controller())
    with pytest.raises(ValueError, match="outer refusal"), store.raw_transaction() as outer:
        _insert(outer, 1)
        with store.transaction() as inner:
            _insert(inner, 2)
        raise ValueError("outer refusal")
    with store.read_transaction() as reader:
        assert _revisions(reader) == []


@pytest.mark.parametrize("access", ["plain-store", "sqlite", "unowned-journal-connection"])
@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
def test_durable_guard_blocks_unowned_journal_writes(store, access, operation):
    store.attach_controller(_Controller())
    with store.projection() as database:
        _insert(database, 1)
    if access == "plain-store":
        # This fresh ordinary store has no knowledge of the registered function.
        ordinary = CoordinatorStore(store.database)
        connection = ordinary._connect()
    elif access == "sqlite":
        connection = sqlite3.connect(store.database)
    else:
        # The function exists here, but this connection owns no stage.
        connection = store._connect()
    with closing(connection), pytest.raises(sqlite3.DatabaseError), connection:
        if operation == "insert":
            _insert(connection, 2)
        elif operation == "update":
            connection.execute("UPDATE session_events SET event_type='changed'")
        else:
            connection.execute("DELETE FROM session_events")
    with store.read_transaction() as reader:
        assert _revisions(reader) == [1]
        assert reader.execute("SELECT event_type FROM session_events").fetchone()[0] == "member.note"


def test_writer_permission_is_scoped_to_exact_active_connection(store):
    store.attach_controller(_Controller())
    with closing(store._connect()) as unowned:
        assert unowned.execute("SELECT fcp_c03_journal_writer()").fetchone()[0] == 0
        with store.raw_transaction() as owned:
            assert owned.execute("SELECT fcp_c03_journal_writer()").fetchone()[0] == 1
            assert unowned.execute("SELECT fcp_c03_journal_writer()").fetchone()[0] == 0
            _insert(owned, 1)
        assert unowned.execute("SELECT fcp_c03_journal_writer()").fetchone()[0] == 0
        with pytest.raises(sqlite3.IntegrityError, match="requires an owned transaction"):
            unowned.execute("DELETE FROM session_events")


def test_owned_raw_stage_and_projection_bypass_controller_and_allow_all_journal_writes(store):
    controller = _Controller()
    store.attach_controller(controller)
    with store.raw_transaction() as database:
        _insert(database, 1)
    with store.projection() as database:
        database.execute("UPDATE session_events SET event_type='projected'")
        with store.transaction() as nested:
            assert nested is database
            _insert(nested, 2)
        assert _revisions(database) == [1, 2]
        database.execute("DELETE FROM session_events WHERE revision=2")
    assert controller.calls == 0
    with store.read_transaction() as reader:
        assert _revisions(reader) == [1]
        assert reader.execute("SELECT event_type FROM session_events").fetchone()[0] == "projected"


def test_unattached_store_does_not_install_guards_or_change_standalone_writes(store):
    with store.transaction() as database:
        _insert(database, 1)
    ordinary = CoordinatorStore(store.database)
    with ordinary.transaction() as database:
        _insert(database, 2)
        database.execute("UPDATE session_events SET event_type='standalone'")
        database.execute("DELETE FROM session_events WHERE revision=1")
    with store.read_transaction() as reader:
        assert _revisions(reader) == [2]
        assert reader.execute("SELECT event_type FROM session_events").fetchone()[0] == "standalone"


def test_attached_guard_persists_after_reopening_without_controller(store):
    store.attach_controller(_Controller())
    reopened = JournalCoordinatorStore(store.database)
    with (
        pytest.raises(sqlite3.IntegrityError, match="requires an owned transaction"),
        reopened.transaction() as database,
    ):
        _insert(database, 1)
    reopened.attach_controller(_Controller())
    with reopened.transaction() as database:
        _insert(database, 1)
    with reopened.read_transaction() as reader:
        assert _revisions(reader) == [1]

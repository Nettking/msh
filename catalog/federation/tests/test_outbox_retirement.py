"""B03: durable retirement/tombstone semantics for the delivery outbox.

The outbox had no terminal failure state.  ``record_failure`` only rescheduled,
so a row that could never be delivered stayed pending for the life of the
recorder: retried forever, fencing its dataset forever, and visible nowhere.

Retirement is that missing state.  These tests pin the properties that make it
safe to have one -- above all that it is a *state*, never a deletion, because
the durable row is itself the record that stops reconciliation re-enqueuing the
same evidence from the same untouched archive on every scan.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.outbox import (
    SCHEMA_VERSION,
    OutboxState,
    SQLiteOutbox,
)

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 8, 25, 13, 0, tzinfo=UTC)


def _outbox(tmp_path: Path) -> SQLiteOutbox:
    return SQLiteOutbox(tmp_path / "outbox.sqlite3")


def _enqueue(
    outbox: SQLiteOutbox,
    *,
    key: str = "batch-1",
    payload: object | None = None,
    session_id: str = "session-1",
    destination_id: str = "telemetry-storage",
):
    return outbox.enqueue(
        session_id=session_id,
        destination_id=destination_id,
        schema_id="fcp.recorder.storage_delivery.v1",
        payload={"dataset_id": "mazak", "batch": key} if payload is None else payload,
        idempotency_key=key,
        content_hash=f"sha256:{key}",
        now=NOW,
    )


def _retire(outbox: SQLiteOutbox, outbox_id: int, **kwargs):
    values = {
        "reason": "payload-field-missing",
        "dataset_id": "mazak",
        "now": NOW,
    }
    values.update(kwargs)
    return outbox.retire(outbox_id, **values)


def _raw(database: Path, outbox_id: int) -> sqlite3.Row:
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(
            "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
        ).fetchone()


# ---------------------------------------------------------------------------
# Reaching the terminal state, and never forgetting it
# ---------------------------------------------------------------------------


def test_permanently_failing_work_reaches_a_durable_terminal_state(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    outbox.record_failure(entry.outbox_id, error="cannot be decoded", now=NOW)

    retired = _retire(outbox, entry.outbox_id)

    assert retired.state is OutboxState.RETIRED
    assert retired.is_terminal is True
    assert retired.retired_at == NOW
    assert retired.retirement_reason == "payload-field-missing"
    assert retired.retirement_dataset_id == "mazak"
    # The recorded cause survives, so the tombstone says why and not only that.
    assert retired.last_error == "cannot be decoded"
    assert retired.attempt_count == 1
    # It is no longer work.
    assert outbox.pending() == ()
    assert outbox.pending(now=LATER) == ()


def test_a_restart_cannot_forget_that_a_row_was_retired(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)

    # A brand new process opening the same database, which is the only thing a
    # restart is.  Nothing about the state lived in the retiring process.
    restarted = SQLiteOutbox(tmp_path / "outbox.sqlite3")

    assert restarted.pending() == ()
    survivor = restarted.get(entry.outbox_id)
    assert survivor.state is OutboxState.RETIRED
    assert survivor.retirement_reason == "payload-field-missing"
    assert survivor.retirement_dataset_id == "mazak"
    assert [row.outbox_id for row in restarted.retired()] == [entry.outbox_id]
    assert restarted.retired_summary().total == 1


def test_retirement_never_deletes_a_row(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)
    outbox.compact_retired(limit=10)

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Duplicate suppression: the tombstone is what stops the re-enqueue loop
# ---------------------------------------------------------------------------


def test_reconciliation_cannot_resurrect_retired_evidence_as_new_work(tmp_path):
    """The reason retirement is a state and not a delete.

    Reconciliation re-derives its rows from the durable archive, which is
    primary evidence and is never removed.  If the tombstone were deleted, the
    very next scan would enqueue the identical evidence as fresh pending work
    and the cycle would repeat forever.
    """

    outbox = _outbox(tmp_path)
    entry, created = _enqueue(outbox)
    assert created is True
    _retire(outbox, entry.outbox_id)

    for _ in range(3):
        again, created_again = _enqueue(outbox)
        assert created_again is False
        assert again.state is OutboxState.RETIRED

    assert outbox.pending() == ()
    assert outbox.retired_summary().total == 1


def test_a_reused_identity_carrying_different_content_still_fails_closed(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)

    with pytest.raises(FederationValidationError) as conflict:
        outbox.enqueue(
            session_id="session-1",
            destination_id="telemetry-storage",
            schema_id="fcp.recorder.storage_delivery.v1",
            payload={"dataset_id": "mazak", "batch": "batch-1"},
            idempotency_key="batch-1",
            content_hash="sha256:something-else",
            now=NOW,
        )

    assert conflict.value.code == "idempotency-conflict"


def test_a_retired_row_can_never_be_acknowledged_or_retried(tmp_path):
    """A racing worker must not be able to record a commit that never happened."""

    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)

    with pytest.raises(FederationValidationError) as acknowledged:
        outbox.acknowledge(entry.outbox_id, now=LATER)
    assert acknowledged.value.code == "outbox-retired"

    with pytest.raises(FederationValidationError) as retried:
        outbox.record_failure(entry.outbox_id, error="again", now=LATER)
    assert retried.value.code == "outbox-retired"

    assert outbox.get(entry.outbox_id).state is OutboxState.RETIRED


@pytest.mark.parametrize("blocked", ["prepared", "completed"])
def test_only_a_pending_row_can_be_retired(tmp_path, blocked: str):
    outbox = _outbox(tmp_path)
    if blocked == "prepared":
        entry, _created = outbox.prepare(
            session_id="session-1",
            destination_id="telemetry-storage",
            schema_id="fcp.recorder.storage_delivery.v1",
            payload={"dataset_id": "mazak"},
            idempotency_key="batch-1",
            content_hash="sha256:batch-1",
            now=NOW,
        )
    else:
        entry, _created = _enqueue(outbox)
        outbox.acknowledge(entry.outbox_id, now=NOW)

    with pytest.raises(FederationValidationError) as refused:
        _retire(outbox, entry.outbox_id)

    assert refused.value.code == "outbox-not-pending"
    assert outbox.get(entry.outbox_id).state is not OutboxState.RETIRED


def test_retirement_is_refused_when_the_durable_row_no_longer_justifies_it(
    tmp_path,
):
    """The re-check runs against the re-read row, inside the transaction.

    A caller that decided to retire from an earlier snapshot must not be able
    to retire a row that has since been repaired underneath it.
    """

    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)

    with pytest.raises(FederationValidationError) as refused:
        _retire(outbox, entry.outbox_id, verify=lambda _entry: False)

    assert refused.value.code == "outbox-retirement-unverified"
    reread = outbox.get(entry.outbox_id)
    assert reread.state is OutboxState.PENDING
    assert reread.retired_at is None
    assert len(outbox.pending()) == 1


def test_the_verify_callback_sees_the_durable_row_not_the_callers_copy(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    outbox.record_failure(entry.outbox_id, error="decode failure", now=NOW)
    seen: list[object] = []

    _retire(
        outbox,
        entry.outbox_id,
        verify=lambda current: seen.append(current) is None,
    )

    assert len(seen) == 1
    # Re-read from the database: it carries the failure recorded after the
    # caller's own snapshot was taken.
    assert seen[0].last_error == "decode failure"
    assert seen[0].attempt_count == 1


# ---------------------------------------------------------------------------
# Repair, and bounded terminal history
# ---------------------------------------------------------------------------


def test_repair_returns_a_retired_row_to_ordinary_retryable_delivery(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    outbox.record_failure(entry.outbox_id, error="cannot be decoded", now=NOW)
    _retire(outbox, entry.outbox_id)

    reinstated = outbox.reinstate(entry.outbox_id, now=LATER)

    assert reinstated.state is OutboxState.PENDING
    assert reinstated.retired_at is None
    assert reinstated.retirement_reason is None
    assert reinstated.retirement_dataset_id is None
    # The isolation was the thing resolved, so backoff and cause reset with it.
    assert reinstated.attempt_count == 0
    assert reinstated.last_error is None
    assert [row.outbox_id for row in outbox.pending(now=LATER)] == [entry.outbox_id]
    assert outbox.retired_summary().total == 0


def test_only_a_retired_row_can_be_reinstated(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)

    with pytest.raises(FederationValidationError) as refused:
        outbox.reinstate(entry.outbox_id, now=LATER)

    assert refused.value.code == "outbox-not-retired"


def test_compaction_bounds_a_tombstone_without_destroying_its_identity(tmp_path):
    """Bounded bytes, intact suppression.

    A row is usually retired *because* its payload is the problem, so the
    payload is exactly what terminal history must stop carrying.  Everything
    duplicate suppression and gap-reporting depend on lives in columns, so it
    survives.
    """

    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    entry, _created = _enqueue(
        outbox, payload={"dataset_id": "mazak", "content": "x" * 200_000}
    )
    _retire(outbox, entry.outbox_id)
    before = len(_raw(database, entry.outbox_id)["payload_json"])

    assert outbox.compact_retired(limit=10) == 1
    assert outbox.compact_retired(limit=10) == 0

    row = _raw(database, entry.outbox_id)
    assert len(row["payload_json"]) < 4_096 < before
    assert row["payload_compacted"] == 1
    # Identity, ordering gap and cause all survive the compaction.
    assert row["state"] == "retired"
    assert row["idempotency_key"] == "batch-1"
    assert row["content_hash"] == "sha256:batch-1"
    assert row["retirement_dataset_id"] == "mazak"
    assert row["retirement_reason"] == "payload-field-missing"
    # And the row still suppresses the identical re-enqueue.
    _again, created_again = _enqueue(outbox)
    assert created_again is False
    assert outbox.pending() == ()


def test_a_compacted_tombstone_is_refused_reinstatement_rather_than_faked(
    tmp_path,
):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)
    outbox.compact_retired(limit=10)

    with pytest.raises(FederationValidationError) as refused:
        outbox.reinstate(entry.outbox_id, now=LATER)

    assert refused.value.code == "outbox-payload-compacted"
    assert outbox.get(entry.outbox_id).state is OutboxState.RETIRED


def test_compaction_never_touches_rows_that_are_still_work(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    live, _created = _enqueue(outbox, key="live")
    doomed, _created = _enqueue(outbox, key="doomed")
    _retire(outbox, doomed.outbox_id)

    assert outbox.compact_retired(limit=10) == 1

    assert _raw(database, live.outbox_id)["payload_compacted"] == 0
    assert outbox.get(live.outbox_id).payload == {
        "dataset_id": "mazak",
        "batch": "live",
    }


def test_retired_summary_is_bounded_but_counts_every_tombstone(tmp_path):
    outbox = _outbox(tmp_path)
    for index in range(5):
        entry, _created = _enqueue(outbox, key=f"batch-{index}")
        _retire(outbox, entry.outbox_id, dataset_id=f"dataset-{index}")

    summary = outbox.retired_summary(limit=2)

    assert summary.total == 5
    assert summary.truncated is True
    assert [item.dataset_id for item in summary.datasets] == [
        "dataset-0",
        "dataset-1",
    ]
    assert outbox.retired_summary().truncated is False


def test_retired_summary_is_scoped_to_one_session_and_destination(tmp_path):
    outbox = _outbox(tmp_path)
    mine, _created = _enqueue(outbox, key="mine")
    theirs, _created = _enqueue(outbox, key="theirs", session_id="session-2")
    _retire(outbox, mine.outbox_id)
    _retire(outbox, theirs.outbox_id, dataset_id="okuma")

    scoped = outbox.retired_summary(
        session_id="session-1", destination_id="telemetry-storage"
    )

    assert scoped.total == 1
    assert [item.dataset_id for item in scoped.datasets] == ["mazak"]
    assert outbox.retired_summary().total == 2


def test_a_row_with_no_usable_dataset_is_retirable_without_claiming_a_gap(
    tmp_path,
):
    """A row that never fenced a dataset leaves no ordering gap behind it."""

    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox, payload=["not", "an", "object"])

    retired = _retire(outbox, entry.outbox_id, dataset_id=None)

    assert retired.retirement_dataset_id is None
    summary = outbox.retired_summary()
    assert summary.total == 1
    assert [item.dataset_id for item in summary.datasets] == [None]


# ---------------------------------------------------------------------------
# Corrupt retirement metadata fails closed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE outbox SET retired_at='2026-08-25T12:00:00' WHERE outbox_id=?",
        "UPDATE outbox SET retired_at='not a timestamp' WHERE outbox_id=?",
        "UPDATE outbox SET retirement_reason='Not A Token!' WHERE outbox_id=?",
        "UPDATE outbox SET retirement_reason='9leading-digit' WHERE outbox_id=?",
    ],
)
def test_malformed_retirement_metadata_fails_closed(tmp_path, corruption: str):
    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)
    with sqlite3.connect(database) as connection:
        connection.execute(corruption, (entry.outbox_id,))

    reopened = SQLiteOutbox(database)
    with pytest.raises(FederationValidationError) as malformed:
        reopened.get(entry.outbox_id)
    assert malformed.value.code == "malformed-outbox-row"

    # Whatever it now is, it is never mistaken for deliverable work, and it is
    # never laundered back into deliverable work either.
    assert reopened.pending() == ()
    with pytest.raises(FederationValidationError):
        reopened.reinstate(entry.outbox_id, now=LATER)


@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE outbox SET retired_at=NULL WHERE outbox_id=?",
        "UPDATE outbox SET retirement_reason=NULL WHERE outbox_id=?",
        (
            "UPDATE outbox SET state='pending',retired_at=NULL,"
            "retirement_reason=NULL WHERE outbox_id=?"
        ),
        "UPDATE outbox SET state='quarantined' WHERE outbox_id=?",
    ],
)
def test_an_unrepresentable_terminal_state_is_rejected_by_the_database_itself(
    tmp_path, corruption: str
):
    """Retirement is constrained as a unit, and not only in the decoder.

    Half a tombstone -- a state without its timestamp and reason, or metadata
    stranded on a row that is no longer retired -- cannot be stored at all, and
    neither can a state this schema does not define. Corruption therefore has
    to defeat the database before it can even reach a reader.
    """

    database = tmp_path / "outbox.sqlite3"
    outbox = SQLiteOutbox(database)
    entry, _created = _enqueue(outbox)
    _retire(outbox, entry.outbox_id)

    with sqlite3.connect(database) as connection, pytest.raises(
        sqlite3.IntegrityError
    ):
        connection.execute(corruption, (entry.outbox_id,))

    assert outbox.get(entry.outbox_id).state is OutboxState.RETIRED


def test_a_retirement_reason_must_be_a_bounded_token(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)

    for reason in ("", "Not A Token", "a" * 65, 7):
        with pytest.raises(FederationValidationError) as invalid:
            outbox.retire(
                entry.outbox_id,
                reason=reason,  # type: ignore[arg-type]
                dataset_id="mazak",
                now=NOW,
            )
        assert invalid.value.code == "invalid-retirement"

    assert outbox.get(entry.outbox_id).state is OutboxState.PENDING


def test_a_retirement_dataset_must_be_bounded_text(tmp_path):
    outbox = _outbox(tmp_path)
    entry, _created = _enqueue(outbox)

    for dataset_id in ("", "d" * 513, 7):
        with pytest.raises(FederationValidationError) as invalid:
            outbox.retire(
                entry.outbox_id,
                reason="operator-retired",
                dataset_id=dataset_id,  # type: ignore[arg-type]
                now=NOW,
            )
        assert invalid.value.code == "invalid-retirement"


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

_V1_SCHEMA = """
CREATE TABLE outbox_schema (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version > 0));
INSERT INTO outbox_schema(singleton, version) VALUES(1, 1);
CREATE TABLE outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL, destination_id TEXT NOT NULL,
    schema_id TEXT NOT NULL, payload_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL, content_hash TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('prepared','pending','completed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL, last_error TEXT,
    UNIQUE(session_id, destination_id, idempotency_key));
"""


def _legacy_database(tmp_path: Path) -> Path:
    database = tmp_path / "outbox.sqlite3"
    stamp = NOW.isoformat()
    with sqlite3.connect(database) as connection:
        connection.executescript(_V1_SCHEMA)
        connection.executemany(
            "INSERT INTO outbox(session_id,destination_id,schema_id,payload_json,"
            "idempotency_key,content_hash,state,created_at,updated_at,"
            "next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                (
                    "session-1",
                    "telemetry-storage",
                    "fcp.recorder.storage_delivery.v1",
                    '{"dataset_id":"mazak","batch":"done"}',
                    "done",
                    "sha256:done",
                    "completed",
                    stamp,
                    stamp,
                    stamp,
                ),
                (
                    "session-1",
                    "telemetry-storage",
                    "fcp.recorder.storage_delivery.v1",
                    '{"dataset_id":"mazak","batch":"live"}',
                    "live",
                    "sha256:live",
                    "pending",
                    stamp,
                    stamp,
                    stamp,
                ),
            ),
        )
    return database


def test_a_legacy_database_migrates_without_losing_rows_or_identity(tmp_path):
    database = _legacy_database(tmp_path)

    migrated = SQLiteOutbox(database)

    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT version FROM outbox_schema WHERE singleton=1"
            ).fetchone()[0]
            == SCHEMA_VERSION
        )
        columns = {row[1] for row in connection.execute("PRAGMA table_info(outbox)")}
        assert {
            "payload_compacted",
            "retired_at",
            "retirement_reason",
            "retirement_dataset_id",
        } <= columns
        # No durable row is dropped or renumbered by the rebuild, and the
        # autoincrement high-water mark is carried over so a later insert can
        # never collide with a migrated identity.
        assert [
            row[0]
            for row in connection.execute(
                "SELECT outbox_id FROM outbox ORDER BY outbox_id"
            )
        ] == [1, 2]
        assert (
            connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='outbox'"
            ).fetchone()[0]
            == 2
        )

    assert [row.idempotency_key for row in migrated.pending()] == ["live"]
    fresh, created = _enqueue(migrated, key="after-migration")
    assert created is True
    assert fresh.outbox_id == 3
    assert _retire(migrated, fresh.outbox_id).state is OutboxState.RETIRED


def test_migration_is_idempotent_across_repeated_opens(tmp_path):
    database = _legacy_database(tmp_path)
    entry, _created = _enqueue(SQLiteOutbox(database), key="live-2")
    _retire(SQLiteOutbox(database), entry.outbox_id)

    for _ in range(3):
        reopened = SQLiteOutbox(database)
        assert reopened.retired_summary().total == 1
        assert [row.idempotency_key for row in reopened.pending()] == ["live"]

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 3


def test_an_unsupported_future_schema_is_refused_rather_than_guessed(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    SQLiteOutbox(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE outbox_schema SET version=? WHERE singleton=1",
            (SCHEMA_VERSION + 1,),
        )

    with pytest.raises(FederationValidationError) as unsupported:
        SQLiteOutbox(database)

    assert unsupported.value.code == "unsupported-outbox-schema"

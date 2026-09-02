"""The coordinator audit ring must bound its work, not only its storage.

``audit_log`` is written on every authoritative session action and already
retires past a row bound. Retiring "everything past the window" is a single
statement, though, so on a coordinator whose history predates this ring -- or
whose bound a later release lowers -- one ordinary rejected request deletes the
entire lifetime overflow inside its own transaction.

``provider_health`` and ``provider_enrollment`` retire in batches for exactly
this reason, and both describe themselves as mirroring this ring. They mirrored
the row bound; the work bound existed only on their side.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.persistence import (
    AUDIT_MAINTENANCE_BATCH_ROWS,
    MAX_AUDIT_ROWS,
    CoordinatorStore,
)

NOW = datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc)


def _store(tmp_path: Path) -> tuple[CoordinatorStore, Path]:
    database = tmp_path / "control.sqlite3"
    return CoordinatorStore(database), database


def _seed(database: Path, rows: int) -> None:
    """Plant legacy history of the shape an upgraded coordinator already holds."""

    connection = sqlite3.connect(database)
    try:
        connection.executemany(
            """INSERT INTO audit_log(
                   occurred_at,actor_node_id,session_id,request_id,
                   action,outcome,reason,details_json
               ) VALUES(?,?,?,?,?,?,?,?)""",
            [
                (
                    "2026-01-01T00:00:00Z",
                    "node-a",
                    "session-a",
                    f"request-{index}",
                    "legacy",
                    "ok",
                    "",
                    "{}",
                )
                for index in range(rows)
            ],
        )
        connection.commit()
    finally:
        connection.close()


def _count(database: Path) -> int:
    connection = sqlite3.connect(database)
    try:
        return int(connection.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0])
    finally:
        connection.close()


def _write(store: CoordinatorStore) -> None:
    store.audit_rejection(now=NOW, action="probe", reason="probe")


def test_one_audit_write_never_retires_more_than_a_batch(tmp_path: Path) -> None:
    """The defect: a rejected request must not become lifetime-sized maintenance."""

    store, database = _store(tmp_path)
    overflow = AUDIT_MAINTENANCE_BATCH_ROWS * 5
    _seed(database, MAX_AUDIT_ROWS + overflow)
    before = _count(database)

    _write(store)

    removed = before + 1 - _count(database)
    assert removed == AUDIT_MAINTENANCE_BATCH_ROWS


def test_repeated_writes_converge_on_the_row_bound(tmp_path: Path) -> None:
    """Bounded work still has to finish: each write must strictly shrink it."""

    store, database = _store(tmp_path)
    overflow = AUDIT_MAINTENANCE_BATCH_ROWS * 3
    _seed(database, MAX_AUDIT_ROWS + overflow)

    counts = []
    # One write appends a row and retires up to a batch, so overflow falls by
    # batch-1 each pass; a handful of passes is enough to drain three batches.
    for _ in range(overflow // (AUDIT_MAINTENANCE_BATCH_ROWS - 1) + 2):
        _write(store)
        counts.append(_count(database))

    assert counts == sorted(counts, reverse=True), "each pass must shrink the table"
    assert _count(database) <= MAX_AUDIT_ROWS


def test_ordinary_traffic_below_the_bound_retires_nothing(tmp_path: Path) -> None:
    """False-positive prevention: a young coordinator does no maintenance at all."""

    store, database = _store(tmp_path)
    _seed(database, 10)

    _write(store)

    assert _count(database) == 11


def test_the_newest_history_is_what_survives(tmp_path: Path) -> None:
    """Retirement follows the table's own id order, oldest first."""

    store, database = _store(tmp_path)
    _seed(database, MAX_AUDIT_ROWS + AUDIT_MAINTENANCE_BATCH_ROWS)

    _write(store)

    connection = sqlite3.connect(database)
    try:
        oldest = connection.execute(
            "SELECT request_id FROM audit_log ORDER BY audit_id ASC LIMIT 1"
        ).fetchone()[0]
        newest_action = connection.execute(
            "SELECT action FROM audit_log ORDER BY audit_id DESC LIMIT 1"
        ).fetchone()[0]
    finally:
        connection.close()

    # The first batch of legacy rows is gone; the write that triggered the
    # retirement is retained.
    assert oldest == f"request-{AUDIT_MAINTENANCE_BATCH_ROWS}"
    assert newest_action == "probe"


def test_retirement_is_restart_safe_across_separate_stores(tmp_path: Path) -> None:
    """The frontier is the table's own id order, not in-process state.

    A coordinator that is restarted mid-catch-up must resume rather than begin
    again, which is what makes progress monotonic across process death.
    """

    _, database = _store(tmp_path)
    overflow = AUDIT_MAINTENANCE_BATCH_ROWS * 2
    _seed(database, MAX_AUDIT_ROWS + overflow)

    first = CoordinatorStore(database)
    _write(first)
    after_first = _count(database)

    # A brand new store object, as a restarted process would build.
    second = CoordinatorStore(database)
    _write(second)

    assert _count(database) < after_first

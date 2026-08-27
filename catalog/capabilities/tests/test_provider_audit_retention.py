"""B07: the recurring provider audit histories are bounded, the command records are not.

``publish_health`` republishes a provider report once its previous one goes
stale, and ``DEFAULT_REPORT_TTL_SECONDS`` is 300. A running device therefore
appends to ``provider_health_audit`` roughly every five minutes, per provider
capability, for as long as it runs -- and nothing ever removed a row.

The history is also never read in full. Its only consumer orders by
``audit_id`` descending under a cap of ``MAX_HEALTH_AUDIT_READ``, so past that
many rows every further row is unreadable by any production consumer while
still occupying pages forever. ``provider_enrollment_audit`` has the same shape
and the same absent bound.

The ``*_commands`` tables deliberately keep no bound here: they are the
idempotency records that make a replayed command return its stored result
instead of re-executing, and retiring them would need a tombstone/hash horizon
that does not exist. These tests pin that they are left alone.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from itertools import pairwise
from pathlib import Path

import pytest

from catalog.capabilities import provider_enrollment as enrollment_module
from catalog.capabilities import provider_health as health_module
from catalog.capabilities.provider_enrollment import (
    MAX_ENROLLMENT_AUDIT_READ,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    MAX_HEALTH_AUDIT_READ,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.tests.test_provider_health import (
    SESSION_ID,
    approve,
    environment,
    report,
)

TTL_SECONDS = 300


def _declared_bound(module, name: str) -> int | None:
    """Read a retained-row bound without requiring it to exist.

    These tests must collect against a tree that has no bound at all, or the
    consequence they assert could never be observed there.
    """

    return getattr(module, name, None)


def _audit_rows(database: Path, table: str) -> int:
    with sqlite3.connect(database) as connection:
        return int(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        )


def _audit_ids(database: Path, table: str) -> list[int]:
    with sqlite3.connect(database) as connection:
        return [
            int(row[0])
            for row in connection.execute(
                f"SELECT audit_id FROM {table} ORDER BY audit_id"
            ).fetchall()
        ]


# ----------------------------------------------------------------------
# The bound must be large enough for the only consumer that exists.
# ----------------------------------------------------------------------


def test_each_retained_history_outlives_every_read_that_can_ask_for_it() -> None:
    """A ring smaller than its reader's cap would silently truncate answers."""

    health = _declared_bound(health_module, "MAX_HEALTH_AUDIT_ROWS")
    enrollment = _declared_bound(
        enrollment_module, "MAX_ENROLLMENT_AUDIT_ROWS"
    )
    assert health is not None, "no retained-row bound is declared for health audit"
    assert enrollment is not None, (
        "no retained-row bound is declared for enrollment audit"
    )
    assert health >= MAX_HEALTH_AUDIT_READ
    assert enrollment >= MAX_ENROLLMENT_AUDIT_READ


def test_the_recurring_publish_rate_is_what_makes_this_history_unbounded() -> None:
    """Pin the production rate this bound exists for."""

    from catalog.capabilities.analysis.provisioning import (
        DEFAULT_REPORT_TTL_SECONDS,
    )

    assert DEFAULT_REPORT_TTL_SECONDS == TTL_SECONDS
    daily_rows = 24 * 60 * 60 // DEFAULT_REPORT_TTL_SECONDS
    # Past the reader's cap every further row is unreadable but still stored.
    assert daily_rows * 40 > MAX_HEALTH_AUDIT_READ


# ----------------------------------------------------------------------
# Accelerated: cross the real ring, using the real production writer.
# ----------------------------------------------------------------------


def test_repeated_health_publishes_stop_growing_the_audit_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The consequence: on main this history grows once per publish, forever."""

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 8, raising=False)
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )

    published = 30
    for revision in range(1, published + 1):
        health_service.publish(
            report(provider, "provider-health", revision=revision),
            actor_node_id=provider.identity.node_id,
            command_id=f"publish-provider-health-1-{revision}",
            provider_generation=1,
        )

    database = Path(health_store.database)
    retained = _audit_rows(database, "provider_health_audit")
    assert retained == 8, f"audit history was not bounded: {retained} rows"

    # The rows kept are the newest ones, which is what the only reader asks for.
    ids = _audit_ids(database, "provider_health_audit")
    assert ids == sorted(ids)
    assert len(ids) == 8
    # The reader returns exactly the retained rows, oldest-retained first.
    recent = health_store.audit_entries(limit=8)
    assert [event.audit_id for event in recent] == ids
    # Everything it can still see is from the newest publishes, not the first.
    assert min(ids) > published - 8 - 1


def test_bounding_the_audit_history_does_not_touch_the_command_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Idempotency evidence must survive a retention pass untouched."""

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 4, raising=False)
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )

    published = 20
    for revision in range(1, published + 1):
        health_service.publish(
            report(provider, "provider-health", revision=revision),
            actor_node_id=provider.identity.node_id,
            command_id=f"publish-provider-health-1-{revision}",
            provider_generation=1,
        )

    database = Path(health_store.database)
    assert _audit_rows(database, "provider_health_audit") == 4
    assert _audit_rows(database, "provider_health_commands") == published

    # The very first command still replays to its stored result rather than
    # re-executing, even though its audit row was retired long ago.
    replay = health_service.publish(
        report(provider, "provider-health", revision=1),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-1",
        provider_generation=1,
    )
    assert replay.report_revision == 1


def test_the_retention_frontier_survives_reopening_the_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The frontier is the table's own id order, so restart cannot reset it."""

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 5, raising=False)
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )
    for revision in range(1, 13):
        health_service.publish(
            report(provider, "provider-health", revision=revision),
            actor_node_id=provider.identity.node_id,
            command_id=f"publish-provider-health-1-{revision}",
            provider_generation=1,
        )

    database = Path(health_store.database)
    before = _audit_ids(database, "provider_health_audit")
    assert len(before) == 5

    reopened = SQLiteProviderHealthStore(database)
    reopened.initialize()
    after = _audit_ids(database, "provider_health_audit")
    assert after == before, "reopening the store changed the retained history"


# ----------------------------------------------------------------------
# The enrollment store carries the identical shape.
# ----------------------------------------------------------------------


def test_the_enrollment_audit_history_is_bounded_by_the_same_rule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(enrollment_module, "MAX_ENROLLMENT_AUDIT_ROWS", 6, raising=False)
    store = SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3")
    store.initialize()

    occurred = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    with store.transaction() as database:
        for index in range(25):
            store._audit(
                database,
                operation="approve",
                outcome="accepted",
                reason_code=f"reason-{index}",
                actor_node_id="node-owner",
                session_id=SESSION_ID,
                capability_id="provider-enrollment",
                record=None,
                occurred_at=occurred,
            )

    database_path = Path(store.database)
    assert _audit_rows(database_path, "provider_enrollment_audit") == 6
    assert _audit_rows(database_path, "provider_enrollment_commands") == 0


def test_retiring_by_id_leaves_no_room_for_a_clock_change_to_matter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rows are retired in insertion order regardless of their timestamps."""

    monkeypatch.setattr(enrollment_module, "MAX_ENROLLMENT_AUDIT_ROWS", 3, raising=False)
    store = SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3")
    store.initialize()

    # Deliberately hostile timestamps: the newest rows claim the oldest times.
    stamps = [
        datetime(2030, 1, 1, tzinfo=timezone.utc),
        datetime(2029, 1, 1, tzinfo=timezone.utc),
        datetime(2028, 1, 1, tzinfo=timezone.utc),
        datetime(2000, 1, 1, tzinfo=timezone.utc),
        datetime(1999, 1, 1, tzinfo=timezone.utc),
    ]
    with store.transaction() as database:
        for index, occurred in enumerate(stamps):
            store._audit(
                database,
                operation="approve",
                outcome="accepted",
                reason_code=f"reason-{index}",
                actor_node_id="node-owner",
                session_id=SESSION_ID,
                capability_id="provider-enrollment",
                record=None,
                occurred_at=occurred,
            )

    with sqlite3.connect(Path(store.database)) as connection:
        kept = [
            row[0]
            for row in connection.execute(
                "SELECT reason_code FROM provider_enrollment_audit ORDER BY audit_id"
            ).fetchall()
        ]
    # The last three inserted survive, even though they carry the oldest dates.
    assert kept == ["reason-2", "reason-3", "reason-4"]


# ----------------------------------------------------------------------
# Legacy catch-up must be finite work, not merely a finite result.
# ----------------------------------------------------------------------


def _seed_legacy_audit_rows(database: Path, table: str, rows: int) -> None:
    """Write a pre-existing history from before any bound existed."""

    connection = sqlite3.connect(database)
    try:
        columns = [
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        template = connection.execute(
            f"SELECT * FROM {table} ORDER BY audit_id DESC LIMIT 1"
        ).fetchone()
        assert template is not None, "expected at least one real audit row to copy"
        audit_index = columns.index("audit_id")
        values = list(template)
        placeholders = ",".join("?" for _ in columns)
        highest = int(
            connection.execute(f"SELECT MAX(audit_id) FROM {table}").fetchone()[0]
        )
        for offset in range(1, rows + 1):
            values[audit_index] = highest + offset
            connection.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders})",
                tuple(values),
            )
        connection.commit()
    finally:
        connection.close()


def test_one_operation_cannot_retire_unlimited_legacy_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The consequence.

    A device that ran before this bound existed can hold an arbitrarily large
    overflow. Retiring all of it in the one statement that an ordinary health
    publication runs would hold the writer lock and build a rollback journal
    proportional to lifetime history, not to the work being done. One operation
    must retire at most the maintenance batch.
    """

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 5, raising=False)
    monkeypatch.setattr(
        health_module, "AUDIT_MAINTENANCE_BATCH_ROWS", 10, raising=False
    )
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )

    # One real publish so the table has a row to model legacy history on.
    health_service.publish(
        report(provider, "provider-health", revision=1),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-1",
        provider_generation=1,
    )
    database = Path(health_store.database)
    _seed_legacy_audit_rows(database, "provider_health_audit", 200)
    before = _audit_rows(database, "provider_health_audit")
    assert before == 201

    health_service.publish(
        report(provider, "provider-health", revision=2),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-2",
        provider_generation=1,
    )

    after = _audit_rows(database, "provider_health_audit")
    # One append, at most one maintenance batch retired.
    assert after == before + 1 - 10, (
        f"one operation retired {before + 1 - after} rows, "
        "which is not bounded by the maintenance batch"
    )


def test_repeated_ordinary_operations_converge_on_the_retained_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bounded work still has to finish: overflow must shrink monotonically and
    normal recurring writes must not outrun cleanup."""

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 5, raising=False)
    monkeypatch.setattr(
        health_module, "AUDIT_MAINTENANCE_BATCH_ROWS", 10, raising=False
    )
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )

    health_service.publish(
        report(provider, "provider-health", revision=1),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-1",
        provider_generation=1,
    )
    database = Path(health_store.database)
    _seed_legacy_audit_rows(database, "provider_health_audit", 100)

    counts = [_audit_rows(database, "provider_health_audit")]
    for revision in range(2, 30):
        health_service.publish(
            report(provider, "provider-health", revision=revision),
            actor_node_id=provider.identity.node_id,
            command_id=f"publish-provider-health-1-{revision}",
            provider_generation=1,
        )
        counts.append(_audit_rows(database, "provider_health_audit"))

    # Strictly shrinking while overflow remains, then parked at the window.
    assert counts[-1] == 5, f"did not converge on the retained window: {counts}"
    shrinking = counts[: counts.index(5) + 1]
    assert all(
        later <= earlier for earlier, later in pairwise(shrinking)
    ), f"overflow did not shrink monotonically: {shrinking}"
    # Cleanup outpaces the append it accompanies, so it terminates.
    assert len(shrinking) < len(counts), "convergence never completed"


def test_command_replay_still_works_after_a_bounded_catch_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bounded catch-up must not touch idempotency evidence."""

    monkeypatch.setattr(health_module, "MAX_HEALTH_AUDIT_ROWS", 5, raising=False)
    monkeypatch.setattr(
        health_module, "AUDIT_MAINTENANCE_BATCH_ROWS", 10, raising=False
    )
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )

    first = health_service.publish(
        report(provider, "provider-health", revision=1),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-1",
        provider_generation=1,
    )
    database = Path(health_store.database)
    _seed_legacy_audit_rows(database, "provider_health_audit", 100)

    for revision in range(2, 12):
        health_service.publish(
            report(provider, "provider-health", revision=revision),
            actor_node_id=provider.identity.node_id,
            command_id=f"publish-provider-health-1-{revision}",
            provider_generation=1,
        )

    # The very first command still replays to its stored result even though its
    # own audit row was retired during catch-up.
    replayed = health_service.publish(
        report(provider, "provider-health", revision=1),
        actor_node_id=provider.identity.node_id,
        command_id="publish-provider-health-1-1",
        provider_generation=1,
    )
    assert replayed == first

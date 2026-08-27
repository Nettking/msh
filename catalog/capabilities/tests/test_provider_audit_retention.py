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

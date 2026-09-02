"""Crash-loop visibility from a service's own restart history.

Docker restarts a failed container indefinitely at a bounded rate. B06 requires
that to become an FCP-visible state rather than an opaque retry history, and
these tests pin both halves of that: a real loop is classified as one, and the
ordinary product lifecycle is not.
"""

from __future__ import annotations

import json
import os
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.service_incarnation import (
    CRASH_LOOP_THRESHOLD,
    CRASH_LOOP_WINDOW_SECONDS,
    MAX_INCARNATIONS,
    SCHEMA,
    STATE_CRASH_LOOP,
    STATE_RESTARTING,
    STATE_STABLE,
    STATE_UNKNOWN,
    STOP_COMPLETED,
    STOP_OPERATOR,
    STOP_TRIAL,
    STOP_UPDATE,
    incarnation_state_file,
    read_restart_state,
    record_service_start,
    record_service_stop,
)

NOW = datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc)


def _file(tmp_path: Path) -> Path:
    return incarnation_state_file(tmp_path, "flask")


def _crash(path: Path, moment: datetime) -> None:
    """One incarnation that was killed: it starts and never records a stop."""

    record_service_start(path, service="flask", now=moment)


def _clean(path: Path, moment: datetime, *, reason: str = STOP_COMPLETED) -> None:
    record_service_start(path, service="flask", now=moment)
    record_service_stop(path, service="flask", reason=reason, now=moment)


def _reported_failure_from_completed_finally(path: Path, moment: datetime) -> None:
    """Model Flask/recorder calling STOP_COMPLETED while an error unwinds."""

    record_service_start(path, service="flask", now=moment)
    try:
        try:
            raise RuntimeError("restart-worthy")
        finally:
            record_service_stop(
                path,
                service="flask",
                reason=STOP_COMPLETED,
                now=moment,
            )
    except RuntimeError:
        pass


def test_a_first_start_has_no_history_and_is_not_a_failure(tmp_path: Path) -> None:
    state = record_service_start(_file(tmp_path), service="flask", now=NOW)

    assert state.state == STATE_STABLE
    assert state.consecutive_unclean == 0
    assert state.since is None


def test_a_clean_stop_and_restart_is_stable(tmp_path: Path) -> None:
    path = _file(tmp_path)
    _clean(path, NOW)

    state = record_service_start(path, service="flask", now=NOW + timedelta(minutes=1))

    assert state.state == STATE_STABLE
    assert state.consecutive_unclean == 0


def test_one_killed_incarnation_is_restarting_not_a_crash_loop(
    tmp_path: Path,
) -> None:
    """A single unclean start is a power cut, not a loop. Do not cry wolf."""

    path = _file(tmp_path)
    _crash(path, NOW)

    state = record_service_start(path, service="flask", now=NOW + timedelta(seconds=5))

    assert state.state == STATE_RESTARTING
    assert state.consecutive_unclean == 1


def test_repeated_kills_in_a_short_window_are_a_crash_loop(tmp_path: Path) -> None:
    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 1):
        _crash(path, moment)
        moment += timedelta(seconds=5)

    state = read_restart_state(path, service="flask", now=moment)

    assert state.state == STATE_CRASH_LOOP
    assert state.consecutive_unclean >= CRASH_LOOP_THRESHOLD
    assert state.since is not None


def test_completed_finally_during_exception_remains_restart_worthy(
    tmp_path: Path,
) -> None:
    """A finally block must not turn an exception exit into a clean stop."""

    path = _file(tmp_path)
    _reported_failure_from_completed_finally(path, NOW)

    state = record_service_start(path, service="flask", now=NOW + timedelta(seconds=1))

    assert state.state == STATE_RESTARTING
    assert state.consecutive_unclean == 1
    assert state.last_stop_reason == STOP_COMPLETED


def test_repeated_reported_failures_become_a_crash_loop(tmp_path: Path) -> None:
    """Observed nonzero/exception exits count just like abrupt process death."""

    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 1):
        _reported_failure_from_completed_finally(path, moment)
        moment += timedelta(seconds=5)

    state = read_restart_state(path, service="flask", now=moment)

    assert state.state == STATE_CRASH_LOOP
    assert state.consecutive_unclean >= CRASH_LOOP_THRESHOLD


def test_nonintentional_reported_stop_reason_is_restart_worthy(
    tmp_path: Path,
) -> None:
    """Relay's explicit nonzero failure path must not reset restart history."""

    path = _file(tmp_path)
    record_service_start(path, service="flask", now=NOW)
    record_service_stop(
        path,
        service="flask",
        reason="relay-background-task-failed",
        now=NOW,
    )

    state = record_service_start(path, service="flask", now=NOW + timedelta(seconds=1))

    assert state.state == STATE_RESTARTING
    assert state.consecutive_unclean == 1
    assert state.last_stop_reason == "relay-background-task-failed"


def test_the_same_count_spread_over_a_year_is_not_a_crash_loop(
    tmp_path: Path,
) -> None:
    """Three power cuts since installation is not a device in trouble now."""

    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 1):
        _crash(path, moment)
        moment += timedelta(days=30)

    state = read_restart_state(
        path,
        service="flask",
        now=moment + timedelta(seconds=CRASH_LOOP_WINDOW_SECONDS + 1),
    )

    assert state.state == STATE_RESTARTING
    assert state.consecutive_unclean >= CRASH_LOOP_THRESHOLD


def test_a_recorded_stop_clears_the_loop(tmp_path: Path) -> None:
    """Recovery is observable, not sticky: one clean run ends the condition."""

    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 1):
        _crash(path, moment)
        moment += timedelta(seconds=5)
    assert read_restart_state(path, service="flask", now=moment).state == (
        STATE_CRASH_LOOP
    )

    record_service_stop(path, service="flask", reason=STOP_COMPLETED, now=moment)
    state = record_service_start(path, service="flask", now=moment + timedelta(seconds=1))

    assert state.state == STATE_STABLE
    assert state.consecutive_unclean == 0


@pytest.mark.parametrize("reason", [STOP_OPERATOR, STOP_UPDATE, STOP_TRIAL])
def test_operator_update_and_trial_stops_never_read_as_a_crash(
    tmp_path: Path,
    reason: str,
) -> None:
    """The ordinary product lifecycle must not manufacture a failure state."""

    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 2):
        _clean(path, moment, reason=reason)
        moment += timedelta(seconds=5)

    state = read_restart_state(path, service="flask", now=moment)

    assert state.state == STATE_STABLE
    assert state.consecutive_unclean == 0
    assert state.last_stop_reason == reason


@pytest.mark.parametrize("reason", [STOP_OPERATOR, STOP_UPDATE, STOP_TRIAL])
def test_intentional_stop_reason_remains_clean_inside_exception_handler(
    tmp_path: Path,
    reason: str,
) -> None:
    """An active exception alone must not turn an explicit lifecycle stop dirty."""

    path = _file(tmp_path)
    record_service_start(path, service="flask", now=NOW)
    try:
        raise KeyboardInterrupt
    except KeyboardInterrupt:
        record_service_stop(path, service="flask", reason=reason, now=NOW)

    state = record_service_start(path, service="flask", now=NOW + timedelta(seconds=1))

    assert state.state == STATE_STABLE
    assert state.consecutive_unclean == 0
    assert state.last_stop_reason == reason


def test_the_record_survives_restart_rather_than_resetting_each_time(
    tmp_path: Path,
) -> None:
    """A perpetual loop must not present as a fresh first failure forever.

    Each start reads the durable file written by the incarnation before it, so
    the count carries across process death rather than starting at zero.
    """

    path = _file(tmp_path)
    moment = NOW
    seen = []
    for _ in range(CRASH_LOOP_THRESHOLD + 2):
        seen.append(record_service_start(path, service="flask", now=moment).state)
        moment += timedelta(seconds=5)

    assert seen[0] == STATE_STABLE
    assert seen[-1] == STATE_CRASH_LOOP
    assert seen.count(STATE_CRASH_LOOP) >= 2


def test_history_stays_bounded_under_an_endless_loop(tmp_path: Path) -> None:
    """B07's rule applies to B06's own evidence: the file may not grow forever."""

    path = _file(tmp_path)
    moment = NOW
    for _ in range(MAX_INCARNATIONS * 4):
        _crash(path, moment)
        moment += timedelta(seconds=1)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema"] == SCHEMA
    assert len(payload["incarnations"]) == MAX_INCARNATIONS
    # Still classified correctly despite the oldest entries having aged out.
    assert read_restart_state(path, service="flask", now=moment).state == (
        STATE_CRASH_LOOP
    )


def test_a_missing_record_is_unknown_rather_than_healthy(tmp_path: Path) -> None:
    state = read_restart_state(_file(tmp_path), service="flask", now=NOW)

    assert state.state == STATE_UNKNOWN


def test_a_corrupt_record_is_unknown_and_never_raises(tmp_path: Path) -> None:
    """A health read must not fail because this evidence is unreadable."""

    path = _file(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    assert read_restart_state(path, service="flask", now=NOW).state == STATE_UNKNOWN
    # And the next start still works, rebuilding from nothing.
    assert record_service_start(path, service="flask", now=NOW).state == STATE_STABLE


def test_another_services_record_is_not_read_as_this_ones(tmp_path: Path) -> None:
    path = _file(tmp_path)
    moment = NOW
    for _ in range(CRASH_LOOP_THRESHOLD + 1):
        _crash(path, moment)
        moment += timedelta(seconds=5)

    assert read_restart_state(path, service="relay", now=moment).state == STATE_UNKNOWN


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink semantics")
def test_a_symlinked_record_is_refused_rather_than_followed(tmp_path: Path) -> None:
    """Ownership confinement: this boundary never follows a substituted path."""

    real = tmp_path / "elsewhere.json"
    real.write_text(
        json.dumps(
            {
                "schema": SCHEMA,
                "service": "flask",
                "incarnations": [{"started_at": "2026-09-02T09:00:00Z"}],
            }
        ),
        encoding="utf-8",
    )
    path = _file(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(real)
    assert stat.S_ISLNK(path.lstat().st_mode)

    assert read_restart_state(path, service="flask", now=NOW).state == STATE_UNKNOWN

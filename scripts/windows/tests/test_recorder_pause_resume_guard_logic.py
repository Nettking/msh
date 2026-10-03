from __future__ import annotations

from datetime import datetime, timedelta, timezone

from scripts.windows.recorder_pause_resume_guard_logic import (
    BoundedPauseResumeGuard,
    PauseGuardAction,
    PauseGuardObservation,
)

NOW = datetime(2026, 10, 3, 7, 0, tzinfo=timezone.utc)


def _guard() -> BoundedPauseResumeGuard:
    return BoundedPauseResumeGuard(
        operation_id="a" * 32,
        prior_control_operation_id="b" * 32,
        hard_deadline=NOW + timedelta(minutes=15),
        drain_timeout_seconds=30,
        controller_stale_after_seconds=10,
        baseline_capture_schedule_count=100,
    )


def _paused(**changes) -> PauseGuardObservation:
    values = {
        "runtime_binding_matches": True,
        "control_operation_id": "a" * 32,
        "control_enabled": False,
        "pause_acknowledged_operation_id": "a" * 32,
        "capture_scheduling": False,
        "inflight_capture_tasks": 0,
        "durable_boundary": True,
        "controller_heartbeat_age_seconds": 1.0,
        "copy_outcome": None,
        "capture_schedule_count": 100,
    }
    values.update(changes)
    return PauseGuardObservation(**values)


def test_guard_does_not_allow_copy_while_capture_future_is_in_flight() -> None:
    guard = _guard()
    action = guard.decide(
        _paused(
            pause_acknowledged_operation_id=None,
            inflight_capture_tasks=1,
            durable_boundary=False,
        ),
        now=NOW,
    )
    assert action is PauseGuardAction.WAIT_FOR_DRAIN


def test_guard_resumes_at_hard_deadline_even_if_drain_ack_is_missing() -> None:
    guard = _guard()
    action = guard.decide(
        _paused(
            pause_acknowledged_operation_id=None,
            inflight_capture_tasks=1,
            durable_boundary=False,
        ),
        now=NOW + timedelta(minutes=15),
    )
    assert action is PauseGuardAction.REQUEST_RESUME


def test_guard_resumes_on_copy_failure_or_controller_loss() -> None:
    for changes in (
        {"copy_outcome": "failed"},
        {"controller_heartbeat_age_seconds": 11.0},
    ):
        assert _guard().decide(_paused(**changes), now=NOW) is PauseGuardAction.REQUEST_RESUME


def test_guard_requires_runtime_identity_before_any_restoration() -> None:
    for identity in (False, None):
        assert (
            _guard().decide(
                _paused(runtime_binding_matches=identity),
                now=NOW,
            )
            is PauseGuardAction.IDENTITY_UNVERIFIED
        )


def test_guard_does_not_override_a_newer_operator_decision() -> None:
    observation = _paused(control_operation_id="c" * 32, control_enabled=False)
    assert _guard().decide(observation, now=NOW) is PauseGuardAction.SUPERSEDED

    newer_start = _paused(
        control_operation_id="c" * 32,
        control_enabled=True,
        capture_scheduling=False,
        capture_schedule_count=100,
    )
    assert _guard().decide(newer_start, now=NOW) is PauseGuardAction.VERIFY_RESUME
    resumed = _paused(
        control_operation_id="c" * 32,
        control_enabled=True,
        capture_scheduling=True,
        capture_schedule_count=101,
    )
    assert _guard().decide(resumed, now=NOW) is PauseGuardAction.RESUMED


def test_guard_waits_for_a_matching_pause_and_has_a_noop_if_it_never_started() -> None:
    guard = _guard()
    before_pause = PauseGuardObservation(
        runtime_binding_matches=True,
        control_operation_id="b" * 32,
        control_enabled=True,
    )
    assert guard.decide(before_pause, now=NOW) is PauseGuardAction.WAIT_FOR_PAUSE
    assert (
        guard.decide(before_pause, now=NOW + timedelta(minutes=15))
        is PauseGuardAction.PAUSE_NEVER_STARTED
    )


def test_guard_verifies_worker_scheduling_after_start_before_success() -> None:
    guard = _guard()
    guard.note_resume_requested("d" * 32)
    not_yet_scheduled = PauseGuardObservation(
        runtime_binding_matches=True,
        control_operation_id="d" * 32,
        control_enabled=True,
        capture_scheduling=False,
        capture_schedule_count=100,
    )
    assert guard.decide(not_yet_scheduled, now=NOW) is PauseGuardAction.VERIFY_RESUME

    scheduled = PauseGuardObservation(
        runtime_binding_matches=True,
        control_operation_id="d" * 32,
        control_enabled=True,
        capture_scheduling=True,
        capture_schedule_count=101,
    )
    assert guard.decide(scheduled, now=NOW) is PauseGuardAction.RESUMED

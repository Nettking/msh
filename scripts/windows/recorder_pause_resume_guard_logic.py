"""Fail-closed decisions for a bounded Recorder capture pause guard.

The host supervisor supplies fresh observations of the runtime, control file,
worker acknowledgement, copy controller, and copy result. This module decides
whether capture may remain paused, must be resumed, or must be left untouched
because the bound runtime or control owner changed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class PauseGuardAction(StrEnum):
    WAIT_FOR_PAUSE = "wait-for-pause"
    WAIT_FOR_DRAIN = "wait-for-drain"
    WAIT_FOR_CONTROLLER = "wait-for-controller"
    COPY_MAY_CONTINUE = "copy-may-continue"
    REQUEST_RESUME = "request-resume"
    VERIFY_RESUME = "verify-resume"
    RESUMED = "resumed"
    SUPERSEDED = "superseded"
    IDENTITY_UNVERIFIED = "identity-unverified"
    PAUSE_NEVER_STARTED = "pause-never-started"


@dataclass(frozen=True)
class PauseGuardObservation:
    runtime_binding_matches: bool | None
    control_operation_id: str | None
    control_enabled: bool | None
    runtime_restart_detected: bool = False
    pause_acknowledged_operation_id: str | None = None
    pause_acknowledged_at: datetime | None = None
    capture_scheduling: bool | None = None
    inflight_capture_tasks: int | None = None
    durable_boundary: bool | None = None
    controller_heartbeat_age_seconds: float | None = None
    copy_outcome: str | None = None
    capture_schedule_count: int | None = None


class BoundedPauseResumeGuard:
    """Make ownership, timeout, and recovery decisions for one pause token."""

    def __init__(
        self,
        *,
        operation_id: str,
        prior_control_operation_id: str | None,
        hard_deadline: datetime,
        drain_timeout_seconds: float,
        controller_stale_after_seconds: float,
        baseline_capture_schedule_count: int,
    ) -> None:
        if hard_deadline.tzinfo is None:
            raise ValueError("hard_deadline must be timezone-aware")
        if drain_timeout_seconds <= 0 or controller_stale_after_seconds <= 0:
            raise ValueError("guard timeouts must be positive")
        self.operation_id = operation_id
        self.prior_control_operation_id = prior_control_operation_id
        self.hard_deadline = hard_deadline
        self.drain_timeout_seconds = drain_timeout_seconds
        self.controller_stale_after_seconds = controller_stale_after_seconds
        self.baseline_capture_schedule_count = baseline_capture_schedule_count
        self.pause_observed_at: datetime | None = None
        self.resume_operation_id: str | None = None

    def note_pause_observed(self, observed_at: datetime) -> None:
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.pause_observed_at is None:
            self.pause_observed_at = observed_at

    def note_resume_requested(self, operation_id: str) -> None:
        if not operation_id or operation_id == self.operation_id:
            raise ValueError("resume must have a new operation identity")
        self.resume_operation_id = operation_id

    def decide(
        self,
        observation: PauseGuardObservation,
        *,
        now: datetime,
    ) -> PauseGuardAction:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if observation.runtime_binding_matches is not True:
            return PauseGuardAction.IDENTITY_UNVERIFIED

        if self.resume_operation_id is not None:
            if (
                observation.control_operation_id == self.resume_operation_id
                and observation.control_enabled is True
            ):
                if (
                    observation.capture_scheduling is True
                    and type(observation.capture_schedule_count) is int
                    and observation.capture_schedule_count
                    > self.baseline_capture_schedule_count
                ):
                    return PauseGuardAction.RESUMED
                return PauseGuardAction.VERIFY_RESUME
            if observation.control_operation_id != self.operation_id:
                if observation.control_enabled is True:
                    if (
                        observation.capture_scheduling is True
                        and type(observation.capture_schedule_count) is int
                        and observation.capture_schedule_count
                        > self.baseline_capture_schedule_count
                    ):
                        return PauseGuardAction.RESUMED
                    return PauseGuardAction.VERIFY_RESUME
                return PauseGuardAction.SUPERSEDED
            return PauseGuardAction.VERIFY_RESUME

        if observation.control_operation_id != self.operation_id:
            if (
                observation.control_operation_id == self.prior_control_operation_id
                and observation.control_enabled is True
            ):
                if now >= self.hard_deadline:
                    return PauseGuardAction.PAUSE_NEVER_STARTED
                return PauseGuardAction.WAIT_FOR_PAUSE
            if observation.control_enabled is True:
                if (
                    observation.capture_scheduling is True
                    and type(observation.capture_schedule_count) is int
                    and observation.capture_schedule_count
                    > self.baseline_capture_schedule_count
                ):
                    return PauseGuardAction.RESUMED
                return PauseGuardAction.VERIFY_RESUME
            return PauseGuardAction.SUPERSEDED
        if observation.control_enabled is not False:
            return PauseGuardAction.SUPERSEDED

        # A new Recorder process incarnation is acceptable only after the
        # host supervisor has independently matched the same container, image,
        # candidate, config bytes, and data mount. The old pause acknowledgement
        # cannot survive that restart, so cancel any copy and request Start.
        if observation.runtime_restart_detected:
            return PauseGuardAction.REQUEST_RESUME

        self.note_pause_observed(now)
        pause_acknowledged = bool(
            observation.pause_acknowledged_operation_id == self.operation_id
            and isinstance(observation.pause_acknowledged_at, datetime)
            and observation.pause_acknowledged_at.tzinfo is not None
            and observation.capture_scheduling is False
            and type(observation.inflight_capture_tasks) is int
            and observation.inflight_capture_tasks == 0
            and observation.durable_boundary is True
        )
        controller_heartbeat_age = observation.controller_heartbeat_age_seconds
        controller_lost = bool(
            controller_heartbeat_age is not None
            and controller_heartbeat_age > self.controller_stale_after_seconds
        )
        controller_start_timed_out = bool(
            controller_heartbeat_age is None
            and pause_acknowledged
            and isinstance(observation.pause_acknowledged_at, datetime)
            and observation.pause_acknowledged_at.tzinfo is not None
            and (now - observation.pause_acknowledged_at).total_seconds()
            > self.controller_stale_after_seconds
        )
        copy_ended = observation.copy_outcome in {"complete", "failed"}
        deadline_reached = now >= self.hard_deadline
        drain_timeout = (
            not pause_acknowledged
            and self.pause_observed_at is not None
            and (now - self.pause_observed_at).total_seconds()
            >= self.drain_timeout_seconds
        )
        if (
            deadline_reached
            or controller_lost
            or controller_start_timed_out
            or copy_ended
            or drain_timeout
        ):
            return PauseGuardAction.REQUEST_RESUME
        if not pause_acknowledged:
            return PauseGuardAction.WAIT_FOR_DRAIN
        if controller_heartbeat_age is None:
            return PauseGuardAction.WAIT_FOR_CONTROLLER
        return PauseGuardAction.COPY_MAY_CONTINUE

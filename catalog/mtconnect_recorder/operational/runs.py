"""S3 conservative MachineRun segmentation and exact duration accounting.

A MachineRun is observed execution history, not a production claim.  ACTIVE
starts a run, READY is the primary observed end, and temporary controller stops
remain inside the same run.  Evidence discontinuities close runs conservatively
so duration is never accounted across missing or non-monotone time evidence.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from ..canonical.observation import CanonicalObservation
from .model import (
    BoundaryConfidence,
    BoundaryReason,
    OperationalBoundary,
    canonical_sha256,
)
from .policy import SEGMENTATION_POLICY
from .timeline import DeviceTimelineReport, ExecutionStateSpan, build_device_timeline_reports


class MachineRunError(ValueError):
    """Raised when S2 timeline evidence cannot be accounted safely in S3."""


@dataclass(frozen=True)
class MachineRun:
    """One conservative same-device controller execution session."""

    run_id: str
    fingerprint: str
    segmentation_policy: str
    source_key: str
    agent_instance_id: int
    device_key: str
    start_boundary: OperationalBoundary
    end_boundary: OperationalBoundary
    wall_duration_us: int
    active_duration_us: int
    program_stopped_duration_us: int
    feed_hold_duration_us: int
    other_execution_duration_us: int
    unknown_duration_us: int

    @property
    def start_sequence(self) -> int:
        return self.start_boundary.trigger_sequence

    @property
    def end_sequence(self) -> int:
        return self.end_boundary.trigger_sequence

    @property
    def partial_start(self) -> bool:
        return self.start_boundary.confidence is not BoundaryConfidence.OBSERVED

    @property
    def partial_end(self) -> bool:
        return self.end_boundary.confidence is not BoundaryConfidence.OBSERVED

    @property
    def duration_buckets_reconcile(self) -> bool:
        return (
            self.active_duration_us
            + self.program_stopped_duration_us
            + self.feed_hold_duration_us
            + self.other_execution_duration_us
            + self.unknown_duration_us
            == self.wall_duration_us
        )


_TEMPORARY_RUN_STATES = frozenset({"PROGRAM_STOPPED", "FEED_HOLD"})


def _state(span: ExecutionStateSpan) -> str:
    return span.state.strip().upper()


def _boundary_payload(boundary: OperationalBoundary) -> dict[str, object]:
    return {
        "source_key": boundary.source_key,
        "agent_instance_id": boundary.agent_instance_id,
        "device_key": boundary.device_key,
        "reason": boundary.reason.value,
        "confidence": boundary.confidence.value,
        "trigger_sequence": boundary.trigger_sequence,
        "evidence_sequence": boundary.evidence_sequence,
        "observed_at": boundary.observed_at,
        "observed_at_us": boundary.observed_at_us,
    }


def _make_boundary(
    report: DeviceTimelineReport,
    *,
    reason: BoundaryReason,
    confidence: BoundaryConfidence,
    trigger_sequence: int,
    evidence_sequence: int,
    observed_at: str,
    observed_at_us: int,
) -> OperationalBoundary:
    return OperationalBoundary(
        source_key=report.source_key,
        agent_instance_id=report.agent_instance_id,
        device_key=report.device_key,
        reason=reason,
        confidence=confidence,
        trigger_sequence=trigger_sequence,
        evidence_sequence=evidence_sequence,
        observed_at=observed_at,
        observed_at_us=observed_at_us,
    )


def _start_boundary(
    report: DeviceTimelineReport,
    span: ExecutionStateSpan,
    *,
    previous_state: str | None,
) -> OperationalBoundary:
    reason_text = span.start_reason.strip().upper()
    if reason_text == "POST_GAP_EXECUTION":
        reason = BoundaryReason.SEQUENCE_GAP
        confidence = BoundaryConfidence.PARTIAL
    elif reason_text == "TIMESTAMP_REGRESSION":
        reason = BoundaryReason.TIMESTAMP_REGRESSION
        confidence = BoundaryConfidence.PARTIAL
    elif span.partial_start or reason_text == "CAPTURE_BEGINS_WITH_EXECUTION":
        reason = BoundaryReason.CAPTURE_BEGINS_ACTIVE
        confidence = BoundaryConfidence.PARTIAL
    elif previous_state in _TEMPORARY_RUN_STATES:
        # We observed ACTIVE again, but the preceding temporary stop implies the
        # run may have started before the capture.  Do not call that start fully
        # observed merely because the resume transition is concrete.
        reason = BoundaryReason.EXECUTION_ACTIVE
        confidence = BoundaryConfidence.STRONG_INFERENCE
    else:
        reason = BoundaryReason.EXECUTION_ACTIVE
        confidence = BoundaryConfidence.OBSERVED
    return _make_boundary(
        report,
        reason=reason,
        confidence=confidence,
        trigger_sequence=span.start_sequence,
        evidence_sequence=span.start_sequence,
        observed_at=span.start_observed_at,
        observed_at_us=span.start_observed_at_us,
    )


def _ready_boundary(
    report: DeviceTimelineReport, span: ExecutionStateSpan
) -> OperationalBoundary:
    return _make_boundary(
        report,
        reason=BoundaryReason.EXECUTION_READY,
        confidence=BoundaryConfidence.OBSERVED,
        trigger_sequence=span.start_sequence,
        evidence_sequence=span.start_sequence,
        observed_at=span.start_observed_at,
        observed_at_us=span.start_observed_at_us,
    )


def _partial_end_boundary(
    report: DeviceTimelineReport,
    span: ExecutionStateSpan,
    *,
    reason: BoundaryReason,
) -> OperationalBoundary:
    return _make_boundary(
        report,
        reason=reason,
        confidence=BoundaryConfidence.PARTIAL,
        trigger_sequence=span.end_sequence,
        evidence_sequence=span.end_sequence,
        observed_at=span.end_observed_at,
        observed_at_us=span.end_observed_at_us,
    )


def _duration_buckets(
    spans: tuple[ExecutionStateSpan, ...],
    *,
    start_us: int,
    end_us: int,
) -> tuple[int, int, int, int, int, int]:
    wall = end_us - start_us
    if wall < 0:
        raise MachineRunError("MachineRun boundary timestamps produce negative wall time")

    active = 0
    program_stopped = 0
    feed_hold = 0
    other = 0
    for span in spans:
        if span.end_observed_at_us < span.start_observed_at_us:
            raise MachineRunError("S2 execution span contains negative elapsed time")
        overlap_start = max(start_us, span.start_observed_at_us)
        overlap_end = min(end_us, span.end_observed_at_us)
        elapsed = max(0, overlap_end - overlap_start)
        state = _state(span)
        if state == "ACTIVE":
            active += elapsed
        elif state == "PROGRAM_STOPPED":
            program_stopped += elapsed
        elif state == "FEED_HOLD":
            feed_hold += elapsed
        else:
            other += elapsed

    known = active + program_stopped + feed_hold + other
    if known > wall:
        raise MachineRunError(
            "execution spans overlap or exceed the MachineRun wall interval"
        )
    unknown = wall - known
    return wall, active, program_stopped, feed_hold, other, unknown


def _finalize_run(
    report: DeviceTimelineReport,
    *,
    start: OperationalBoundary,
    end: OperationalBoundary,
    spans: list[ExecutionStateSpan],
) -> MachineRun:
    accounted = tuple(spans)
    (
        wall,
        active,
        program_stopped,
        feed_hold,
        other,
        unknown,
    ) = _duration_buckets(
        accounted,
        start_us=start.observed_at_us,
        end_us=end.observed_at_us,
    )

    identity_payload = {
        "kind": "machine-run",
        "segmentation_policy": SEGMENTATION_POLICY,
        "source_key": report.source_key,
        "agent_instance_id": report.agent_instance_id,
        "device_key": report.device_key,
        "start_sequence": start.trigger_sequence,
        "end_sequence": end.trigger_sequence,
    }
    run_id = f"machine-run:{canonical_sha256(identity_payload)}"
    content_payload = {
        **identity_payload,
        "run_id": run_id,
        "start_boundary": _boundary_payload(start),
        "end_boundary": _boundary_payload(end),
        "wall_duration_us": wall,
        "active_duration_us": active,
        "program_stopped_duration_us": program_stopped,
        "feed_hold_duration_us": feed_hold,
        "other_execution_duration_us": other,
        "unknown_duration_us": unknown,
    }
    return MachineRun(
        run_id=run_id,
        fingerprint=canonical_sha256(content_payload),
        segmentation_policy=SEGMENTATION_POLICY,
        source_key=report.source_key,
        agent_instance_id=report.agent_instance_id,
        device_key=report.device_key,
        start_boundary=start,
        end_boundary=end,
        wall_duration_us=wall,
        active_duration_us=active,
        program_stopped_duration_us=program_stopped,
        feed_hold_duration_us=feed_hold,
        other_execution_duration_us=other,
        unknown_duration_us=unknown,
    )


def _validate_span_scope(report: DeviceTimelineReport, span: ExecutionStateSpan) -> None:
    if (
        span.source_key != report.source_key
        or span.agent_instance_id != report.agent_instance_id
        or span.device_key != report.device_key
    ):
        raise MachineRunError("execution span is outside its DeviceTimelineReport scope")


def segment_machine_runs(
    report: DeviceTimelineReport,
    *,
    terminal_reason: BoundaryReason = BoundaryReason.CAPTURE_END,
) -> tuple[MachineRun, ...]:
    """Segment one S2 device timeline into conservative MachineRun objects."""

    if not report.execution_is_usable:
        return ()
    if terminal_reason not in {
        BoundaryReason.CAPTURE_END,
        BoundaryReason.AGENT_INSTANCE_END,
    }:
        raise MachineRunError("terminal_reason must describe an evidence boundary")

    spans = report.execution_spans
    runs: list[MachineRun] = []
    current_start: OperationalBoundary | None = None
    current_spans: list[ExecutionStateSpan] = []
    previous_state: str | None = None

    def close(end: OperationalBoundary) -> None:
        nonlocal current_start, current_spans
        if current_start is None:
            return
        runs.append(
            _finalize_run(
                report,
                start=current_start,
                end=end,
                spans=current_spans,
            )
        )
        current_start = None
        current_spans = []

    for index, span in enumerate(spans):
        _validate_span_scope(report, span)
        state = _state(span)
        is_last = index == len(spans) - 1

        if current_start is None:
            if state == "ACTIVE":
                current_start = _start_boundary(
                    report,
                    span,
                    previous_state=previous_state,
                )
                current_spans = [span]
        elif state == "READY":
            close(_ready_boundary(report, span))
        else:
            current_spans.append(span)

        if current_start is not None and state != "READY":
            end_reason_text = span.end_reason.strip().upper()
            forced_reason: BoundaryReason | None = None
            if end_reason_text == "SEQUENCE_GAP":
                forced_reason = BoundaryReason.SEQUENCE_GAP
            elif end_reason_text == "TIMESTAMP_REGRESSION":
                forced_reason = BoundaryReason.TIMESTAMP_REGRESSION
            elif is_last and end_reason_text in {"CAPTURE_END", "AGENT_INSTANCE_END"}:
                forced_reason = terminal_reason

            if forced_reason is not None:
                close(
                    _partial_end_boundary(
                        report,
                        span,
                        reason=forced_reason,
                    )
                )

        previous_state = state

    if current_start is not None:
        if not current_spans:
            raise MachineRunError("open MachineRun has no execution evidence")
        close(
            _partial_end_boundary(
                report,
                current_spans[-1],
                reason=terminal_reason,
            )
        )

    return tuple(runs)


def segment_machine_runs_from_reports(
    reports: Iterable[DeviceTimelineReport],
) -> tuple[MachineRun, ...]:
    """Segment S2 reports, marking known transitions between Agent instances."""

    grouped: defaultdict[tuple[str, str], list[DeviceTimelineReport]] = defaultdict(list)
    for report in reports:
        grouped[(report.source_key, report.device_key)].append(report)

    runs: list[MachineRun] = []
    for group in grouped.values():
        ordered = sorted(group, key=lambda item: item.agent_instance_id)
        for index, report in enumerate(ordered):
            terminal = (
                BoundaryReason.AGENT_INSTANCE_END
                if index < len(ordered) - 1
                else BoundaryReason.CAPTURE_END
            )
            runs.extend(segment_machine_runs(report, terminal_reason=terminal))

    return tuple(
        sorted(
            runs,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
                item.start_sequence,
                item.end_sequence,
            ),
        )
    )


def build_machine_runs(
    observations: Iterable[CanonicalObservation],
) -> tuple[MachineRun, ...]:
    """Run the complete canonical-observation → S2 → S3 projection in memory."""

    reports = build_device_timeline_reports(tuple(observations))
    return segment_machine_runs_from_reports(reports)


__all__ = [
    "MachineRun",
    "MachineRunError",
    "build_machine_runs",
    "segment_machine_runs",
    "segment_machine_runs_from_reports",
]

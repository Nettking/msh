"""S5 deterministic OperationalEpisode segmentation inside ProductionCycles.

Episodes are historical comparison units, not comparison groups. V1 boundaries are
limited to ProductionCycle edges and direct concrete same-device tool transitions.
Unknown tool context is preserved explicitly and never converted into an invented
change point.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from ..canonical.observation import CanonicalObservation
from .cycles import ProductionCycle, infer_production_cycles
from .model import BoundaryConfidence, BoundaryReason, OperationalBoundary, canonical_sha256
from .policy import SEGMENTATION_POLICY, device_key
from .roles import RoleResolutionStatus, SemanticRole
from .runs import MachineRun, segment_machine_runs_from_reports
from .timeline import ContextTransition, DeviceTimelineReport, ExecutionStateSpan, build_device_timeline_reports


class OperationalEpisodeError(ValueError):
    """Raised when S5 cannot preserve the S1-S4 evidence contract."""


class EpisodeBoundaryReason(str, Enum):
    CYCLE_START = "CYCLE_START"
    TOOL_CHANGE = "TOOL_CHANGE"
    CYCLE_END = "CYCLE_END"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    CAPTURE_END = "CAPTURE_END"
    AGENT_INSTANCE_END = "AGENT_INSTANCE_END"
    TIMESTAMP_REGRESSION = "TIMESTAMP_REGRESSION"


@dataclass(frozen=True)
class EpisodeBoundary:
    source_key: str
    agent_instance_id: int
    device_key: str
    reason: EpisodeBoundaryReason
    confidence: BoundaryConfidence
    trigger_sequence: int
    evidence_sequence: int
    observed_at: str
    observed_at_us: int
    inherited_reason: BoundaryReason | None = None


SemanticValue = float | str | None


@dataclass(frozen=True)
class EpisodeEntryContext:
    """Same-device context known at episode entry.

    S1 currently exposes one generic PROGRAM role and no independent SUBPROGRAM
    role. ``main_program`` therefore reflects the resolved PROGRAM context while
    ``subprogram`` remains explicitly unresolved instead of being guessed from
    program churn.
    """

    main_program: SemanticValue
    subprogram: SemanticValue
    program_comment: SemanticValue
    line: SemanticValue
    controller_mode: SemanticValue
    tool_number: SemanticValue
    tool_group: SemanticValue
    pallet: SemanticValue
    partial_fields: tuple[str, ...]


@dataclass(frozen=True)
class OperationalEpisode:
    episode_id: str
    fingerprint: str
    segmentation_policy: str
    production_cycle_id: str
    machine_run_id: str
    source_key: str
    agent_instance_id: int
    device_key: str
    start_boundary: EpisodeBoundary
    end_boundary: EpisodeBoundary
    first_sequence: int
    last_sequence: int
    entry_context: EpisodeEntryContext
    context_transitions: tuple[ContextTransition, ...]
    tool_identity_role: SemanticRole | None
    tool_context_partial: bool
    wall_duration_us: int
    active_duration_us: int
    program_stopped_duration_us: int
    feed_hold_duration_us: int
    other_execution_duration_us: int
    unknown_duration_us: int

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


_ENTRY_ROLES: tuple[tuple[str, SemanticRole], ...] = (
    ("MAIN_PROGRAM", SemanticRole.PROGRAM),
    ("PROGRAM_COMMENT", SemanticRole.PROGRAM_COMMENT),
    ("LINE", SemanticRole.LINE),
    ("CONTROLLER_MODE", SemanticRole.CONTROLLER_MODE),
    ("TOOL_NUMBER", SemanticRole.TOOL_NUMBER),
    ("TOOL_GROUP", SemanticRole.TOOL_GROUP),
    ("PALLET", SemanticRole.PALLET),
)

_PARTIAL_EDGE_REASONS = {
    BoundaryReason.SEQUENCE_GAP: EpisodeBoundaryReason.SEQUENCE_GAP,
    BoundaryReason.CAPTURE_END: EpisodeBoundaryReason.CAPTURE_END,
    BoundaryReason.AGENT_INSTANCE_END: EpisodeBoundaryReason.AGENT_INSTANCE_END,
    BoundaryReason.TIMESTAMP_REGRESSION: EpisodeBoundaryReason.TIMESTAMP_REGRESSION,
}


def _context_state_at_sequence(
    report: DeviceTimelineReport,
    sequence: int,
) -> dict[SemanticRole, ContextTransition]:
    state: dict[SemanticRole, ContextTransition] = {}
    gaps = sorted(report.agent_sequence_discontinuities, key=lambda item: item.next_sequence)
    gap_index = 0

    for transition in sorted(report.context_transitions, key=lambda item: item.sequence):
        if transition.sequence > sequence:
            break
        while gap_index < len(gaps) and gaps[gap_index].next_sequence <= transition.sequence:
            state.clear()
            gap_index += 1
        if transition.new_value is None:
            state.pop(transition.role, None)
        else:
            state[transition.role] = transition

    while gap_index < len(gaps) and gaps[gap_index].next_sequence <= sequence:
        state.clear()
        gap_index += 1

    return state


def _entry_context(report: DeviceTimelineReport, sequence: int) -> EpisodeEntryContext:
    state = _context_state_at_sequence(report, sequence)
    values: dict[str, SemanticValue] = {}
    partial: list[str] = ["SUBPROGRAM"]

    for field_name, role in _ENTRY_ROLES:
        resolution = report.role_resolution.role(role)
        transition = state.get(role)
        if (
            resolution.status is not RoleResolutionStatus.RESOLVED
            or transition is None
            or transition.new_value is None
        ):
            values[field_name] = None
            partial.append(field_name)
        else:
            values[field_name] = transition.new_value

    return EpisodeEntryContext(
        main_program=values["MAIN_PROGRAM"],
        subprogram=None,
        program_comment=values["PROGRAM_COMMENT"],
        line=values["LINE"],
        controller_mode=values["CONTROLLER_MODE"],
        tool_number=values["TOOL_NUMBER"],
        tool_group=values["TOOL_GROUP"],
        pallet=values["PALLET"],
        partial_fields=tuple(partial),
    )


def _tool_identity_role(report: DeviceTimelineReport) -> SemanticRole | None:
    """Select one stable tool identity source to avoid double-splitting.

    TOOL_NUMBER is preferred because the reference trace and roadmap use concrete
    numeric tool identities. TOOL_GROUP is a deterministic fallback when tool
    number is not resolved as one value.
    """

    for role in (SemanticRole.TOOL_NUMBER, SemanticRole.TOOL_GROUP):
        if report.role_resolution.role(role).status is RoleResolutionStatus.RESOLVED:
            return role
    return None


def _tool_change_transitions(
    cycle: ProductionCycle,
    report: DeviceTimelineReport,
    tool_role: SemanticRole | None,
) -> tuple[ContextTransition, ...]:
    if tool_role is None:
        return ()
    return tuple(
        transition
        for transition in report.context_transitions
        if transition.role is tool_role
        and transition.previous_value is not None
        and transition.new_value is not None
        and transition.previous_value != transition.new_value
        and cycle.start_sequence < transition.sequence < cycle.end_sequence
    )


def _episode_edge_from_cycle(
    boundary: OperationalBoundary,
    *,
    is_start: bool,
) -> EpisodeBoundary:
    reason = _PARTIAL_EDGE_REASONS.get(boundary.reason)
    if reason is None:
        reason = (
            EpisodeBoundaryReason.CYCLE_START
            if is_start
            else EpisodeBoundaryReason.CYCLE_END
        )
    return EpisodeBoundary(
        source_key=boundary.source_key,
        agent_instance_id=boundary.agent_instance_id,
        device_key=boundary.device_key,
        reason=reason,
        confidence=boundary.confidence,
        trigger_sequence=boundary.trigger_sequence,
        evidence_sequence=boundary.evidence_sequence,
        observed_at=boundary.observed_at,
        observed_at_us=boundary.observed_at_us,
        inherited_reason=boundary.reason,
    )


def _tool_boundary(
    report: DeviceTimelineReport,
    transition: ContextTransition,
) -> EpisodeBoundary:
    return EpisodeBoundary(
        source_key=report.source_key,
        agent_instance_id=report.agent_instance_id,
        device_key=report.device_key,
        reason=EpisodeBoundaryReason.TOOL_CHANGE,
        confidence=BoundaryConfidence.OBSERVED,
        trigger_sequence=transition.sequence,
        evidence_sequence=transition.sequence,
        observed_at=transition.observed_at,
        observed_at_us=transition.observed_at_us,
        inherited_reason=None,
    )


def _duration_buckets(
    spans: tuple[ExecutionStateSpan, ...],
    *,
    start_us: int,
    end_us: int,
) -> tuple[int, int, int, int, int, int]:
    wall = end_us - start_us
    if wall < 0:
        raise OperationalEpisodeError("episode boundary timestamps are non-monotone")

    active = 0
    program_stopped = 0
    feed_hold = 0
    other = 0
    intervals: list[tuple[int, int]] = []

    for span in spans:
        if span.end_observed_at_us < span.start_observed_at_us:
            raise OperationalEpisodeError("S2 execution span contains negative elapsed time")
        overlap_start = max(start_us, span.start_observed_at_us)
        overlap_end = min(end_us, span.end_observed_at_us)
        if overlap_end <= overlap_start:
            continue
        intervals.append((overlap_start, overlap_end))
        elapsed = overlap_end - overlap_start
        state = span.state.strip().upper()
        if state == "ACTIVE":
            active += elapsed
        elif state == "PROGRAM_STOPPED":
            program_stopped += elapsed
        elif state == "FEED_HOLD":
            feed_hold += elapsed
        else:
            other += elapsed

    for previous, current in zip(sorted(intervals), sorted(intervals)[1:], strict=False):
        if current[0] < previous[1]:
            raise OperationalEpisodeError("execution spans overlap inside episode")

    known = active + program_stopped + feed_hold + other
    if known > wall:
        raise OperationalEpisodeError("episode execution duration exceeds wall duration")
    unknown = wall - known
    return wall, active, program_stopped, feed_hold, other, unknown


def _boundary_payload(boundary: EpisodeBoundary) -> dict[str, object]:
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
        "inherited_reason": (
            boundary.inherited_reason.value if boundary.inherited_reason is not None else None
        ),
    }


def _context_payload(context: EpisodeEntryContext) -> dict[str, object]:
    return {
        "main_program": context.main_program,
        "subprogram": context.subprogram,
        "program_comment": context.program_comment,
        "line": context.line,
        "controller_mode": context.controller_mode,
        "tool_number": context.tool_number,
        "tool_group": context.tool_group,
        "pallet": context.pallet,
        "partial_fields": list(context.partial_fields),
    }


def _transition_payload(transition: ContextTransition) -> dict[str, object]:
    return {
        "sequence": transition.sequence,
        "observed_at": transition.observed_at,
        "observed_at_us": transition.observed_at_us,
        "role": transition.role.value,
        "previous_value": transition.previous_value,
        "new_value": transition.new_value,
        "data_item_id": transition.data_item_id,
        "establishes_context": transition.establishes_context,
    }


def _tool_context_is_partial(
    entry: EpisodeEntryContext,
    transitions: tuple[ContextTransition, ...],
    tool_role: SemanticRole | None,
) -> bool:
    if tool_role is None:
        return True
    entry_value = (
        entry.tool_number if tool_role is SemanticRole.TOOL_NUMBER else entry.tool_group
    )
    if entry_value is None:
        return True
    return any(
        transition.role is tool_role
        and (transition.previous_value is None or transition.new_value is None)
        for transition in transitions
    )


def _make_episode(
    cycle: ProductionCycle,
    report: DeviceTimelineReport,
    *,
    start_boundary: EpisodeBoundary,
    end_boundary: EpisodeBoundary,
    first_sequence: int,
    last_sequence: int,
    tool_role: SemanticRole | None,
) -> OperationalEpisode:
    if (
        start_boundary.source_key != cycle.source_key
        or end_boundary.source_key != cycle.source_key
        or start_boundary.agent_instance_id != cycle.agent_instance_id
        or end_boundary.agent_instance_id != cycle.agent_instance_id
        or start_boundary.device_key != cycle.device_key
        or end_boundary.device_key != cycle.device_key
    ):
        raise OperationalEpisodeError("episode boundary scope differs from ProductionCycle")
    if first_sequence < cycle.start_sequence or last_sequence > cycle.end_sequence:
        raise OperationalEpisodeError("episode evidence is outside ProductionCycle")
    if last_sequence < first_sequence:
        raise OperationalEpisodeError("episode evidence sequence is inverted")

    transitions = tuple(
        transition
        for transition in report.context_transitions
        if start_boundary.trigger_sequence < transition.sequence < end_boundary.trigger_sequence
    )
    entry = _entry_context(report, start_boundary.trigger_sequence)
    partial_tool = _tool_context_is_partial(entry, transitions, tool_role)
    wall, active, program_stopped, feed_hold, other, unknown = _duration_buckets(
        report.execution_spans,
        start_us=start_boundary.observed_at_us,
        end_us=end_boundary.observed_at_us,
    )

    identity_payload = {
        "kind": "operational-episode",
        "segmentation_policy": SEGMENTATION_POLICY,
        "source_key": cycle.source_key,
        "agent_instance_id": cycle.agent_instance_id,
        "device_key": cycle.device_key,
        "production_cycle_id": cycle.cycle_id,
        "first_sequence": first_sequence,
        "last_sequence": last_sequence,
        "start_boundary_sequence": start_boundary.trigger_sequence,
        "end_boundary_sequence": end_boundary.trigger_sequence,
    }
    episode_id = f"operational-episode:{canonical_sha256(identity_payload)}"
    content_payload = {
        **identity_payload,
        "episode_id": episode_id,
        "machine_run_id": cycle.machine_run_id,
        "start_boundary": _boundary_payload(start_boundary),
        "end_boundary": _boundary_payload(end_boundary),
        "entry_context": _context_payload(entry),
        "context_transitions": [_transition_payload(item) for item in transitions],
        "tool_identity_role": tool_role.value if tool_role is not None else None,
        "tool_context_partial": partial_tool,
        "wall_duration_us": wall,
        "active_duration_us": active,
        "program_stopped_duration_us": program_stopped,
        "feed_hold_duration_us": feed_hold,
        "other_execution_duration_us": other,
        "unknown_duration_us": unknown,
    }
    return OperationalEpisode(
        episode_id=episode_id,
        fingerprint=canonical_sha256(content_payload),
        segmentation_policy=SEGMENTATION_POLICY,
        production_cycle_id=cycle.cycle_id,
        machine_run_id=cycle.machine_run_id,
        source_key=cycle.source_key,
        agent_instance_id=cycle.agent_instance_id,
        device_key=cycle.device_key,
        start_boundary=start_boundary,
        end_boundary=end_boundary,
        first_sequence=first_sequence,
        last_sequence=last_sequence,
        entry_context=entry,
        context_transitions=transitions,
        tool_identity_role=tool_role,
        tool_context_partial=partial_tool,
        wall_duration_us=wall,
        active_duration_us=active,
        program_stopped_duration_us=program_stopped,
        feed_hold_duration_us=feed_hold,
        other_execution_duration_us=other,
        unknown_duration_us=unknown,
    )


def segment_operational_episodes(
    cycle: ProductionCycle,
    report: DeviceTimelineReport,
    observations: Iterable[CanonicalObservation],
) -> tuple[OperationalEpisode, ...]:
    """Split one ProductionCycle only on trustworthy direct tool transitions."""

    if (
        cycle.source_key != report.source_key
        or cycle.agent_instance_id != report.agent_instance_id
        or cycle.device_key != report.device_key
    ):
        raise OperationalEpisodeError("ProductionCycle and timeline report scopes differ")

    rows = tuple(
        sorted(
            (
                observation
                for observation in observations
                if observation.source_key == cycle.source_key
                and observation.agent_instance_id == cycle.agent_instance_id
                and device_key(observation) == cycle.device_key
                and cycle.start_sequence <= observation.sequence <= cycle.end_sequence
            ),
            key=lambda item: item.sequence,
        )
    )
    if not rows:
        raise OperationalEpisodeError("ProductionCycle has no canonical observations")

    tool_role = _tool_identity_role(report)
    changes = _tool_change_transitions(cycle, report, tool_role)
    boundaries = [
        _episode_edge_from_cycle(cycle.start_boundary, is_start=True),
        *(_tool_boundary(report, transition) for transition in changes),
        _episode_edge_from_cycle(cycle.end_boundary, is_start=False),
    ]

    episodes: list[OperationalEpisode] = []
    for index, (start, end) in enumerate(zip(boundaries, boundaries[1:], strict=False)):
        include_end = index == len(boundaries) - 2
        segment_rows = [
            row
            for row in rows
            if row.sequence >= start.trigger_sequence
            and (
                row.sequence <= end.trigger_sequence
                if include_end
                else row.sequence < end.trigger_sequence
            )
        ]
        if not segment_rows:
            raise OperationalEpisodeError("episode boundary produced an empty evidence interval")
        episodes.append(
            _make_episode(
                cycle,
                report,
                start_boundary=start,
                end_boundary=end,
                first_sequence=segment_rows[0].sequence,
                last_sequence=segment_rows[-1].sequence,
                tool_role=tool_role,
            )
        )

    for previous, current in zip(episodes, episodes[1:], strict=False):
        if previous.last_sequence >= current.first_sequence:
            raise OperationalEpisodeError("OperationalEpisodes overlap in canonical evidence")
        if previous.end_boundary.observed_at_us > current.start_boundary.observed_at_us:
            raise OperationalEpisodeError("OperationalEpisodes overlap in time")

    return tuple(episodes)


def build_operational_episodes(
    observations: Iterable[CanonicalObservation],
) -> tuple[OperationalEpisode, ...]:
    """Run canonical observations through S2-S5 deterministically in memory."""

    rows = tuple(observations)
    reports = build_device_timeline_reports(rows)
    reports_by_scope = {
        (report.source_key, report.agent_instance_id, report.device_key): report
        for report in reports
    }
    runs = segment_machine_runs_from_reports(reports)

    rows_by_scope: defaultdict[
        tuple[str, int, str], list[CanonicalObservation]
    ] = defaultdict(list)
    for observation in rows:
        rows_by_scope[
            (
                observation.source_key,
                observation.agent_instance_id,
                device_key(observation),
            )
        ].append(observation)

    episodes: list[OperationalEpisode] = []
    for run in runs:
        scope = (run.source_key, run.agent_instance_id, run.device_key)
        report = reports_by_scope.get(scope)
        if report is None:
            raise OperationalEpisodeError("MachineRun has no matching S2 report")
        scoped_rows = rows_by_scope.get(scope, ())
        cycles = infer_production_cycles(run, report, scoped_rows)
        for cycle in cycles:
            episodes.extend(segment_operational_episodes(cycle, report, scoped_rows))

    return tuple(
        sorted(
            episodes,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
                item.first_sequence,
                item.last_sequence,
            ),
        )
    )


__all__ = [
    "EpisodeBoundary",
    "EpisodeBoundaryReason",
    "EpisodeEntryContext",
    "OperationalEpisode",
    "OperationalEpisodeError",
    "build_operational_episodes",
    "segment_operational_episodes",
]

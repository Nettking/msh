"""Sequence-safe MTConnect operational timeline reconstruction.

S2 establishes Agent-wide sequence continuity before device partitioning, then
reconstructs deterministic per-device execution and manufacturing context. It
never modifies canonical evidence and does not infer runs, production cycles, or
operational episodes.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from ..canonical.observation import CanonicalObservation
from .policy import device_key
from .roles import (
    DeviceRoleResolution,
    RoleCandidate,
    RoleResolutionStatus,
    SemanticRole,
    resolve_device_roles,
)


class AgentStreamError(ValueError):
    """Raised when canonical input violates the Agent stream contract."""


class TimelineStatus(str, Enum):
    READY = "READY"
    EXECUTION_UNAVAILABLE = "EXECUTION_UNAVAILABLE"
    EXECUTION_AMBIGUOUS = "EXECUTION_AMBIGUOUS"
    EXECUTION_MULTI_CHANNEL = "EXECUTION_MULTI_CHANNEL"


@dataclass(frozen=True)
class AgentSequenceDiscontinuity:
    source_key: str
    agent_instance_id: int
    previous_sequence: int
    next_sequence: int
    missing_start_sequence: int
    missing_end_sequence: int
    missing_count: int


@dataclass(frozen=True)
class TimestampDiscontinuity:
    source_key: str
    agent_instance_id: int
    device_key: str
    previous_sequence: int
    next_sequence: int
    previous_observed_at_us: int
    next_observed_at_us: int
    reason: str = "TIMESTAMP_REGRESSION"


@dataclass(frozen=True)
class AgentStreamReport:
    source_key: str
    agent_instance_id: int
    observations: tuple[CanonicalObservation, ...]
    sequence_discontinuities: tuple[AgentSequenceDiscontinuity, ...]

    @property
    def is_sequence_continuous(self) -> bool:
        return not self.sequence_discontinuities


@dataclass(frozen=True)
class DeviceStreamPartition:
    source_key: str
    agent_instance_id: int
    device_key: str
    observations: tuple[CanonicalObservation, ...]
    agent_sequence_discontinuities: tuple[AgentSequenceDiscontinuity, ...]


SemanticValue = float | str | None


@dataclass(frozen=True)
class ContextValue:
    role: SemanticRole
    value: SemanticValue
    available: bool
    sequence: int
    observed_at: str
    observed_at_us: int
    data_item_id: str | None


@dataclass(frozen=True)
class ContextTransition:
    source_key: str
    agent_instance_id: int
    device_key: str
    sequence: int
    observed_at: str
    observed_at_us: int
    role: SemanticRole
    previous_value: SemanticValue
    new_value: SemanticValue
    data_item_id: str | None
    establishes_context: bool


@dataclass(frozen=True)
class ContextSnapshot:
    source_key: str
    agent_instance_id: int
    device_key: str
    sequence: int
    observed_at: str
    observed_at_us: int
    values: tuple[ContextValue, ...]
    pending_roles: tuple[SemanticRole, ...]

    @property
    def is_partial(self) -> bool:
        return bool(self.pending_roles)

    def value(self, role: SemanticRole) -> ContextValue | None:
        return next((item for item in self.values if item.role is role), None)


@dataclass(frozen=True)
class ExecutionStateSpan:
    source_key: str
    agent_instance_id: int
    device_key: str
    state: str
    start_sequence: int
    start_observed_at: str
    start_observed_at_us: int
    end_sequence: int
    end_observed_at: str
    end_observed_at_us: int
    start_reason: str
    end_reason: str
    partial_start: bool
    partial_end: bool
    start_data_item_id: str | None


@dataclass(frozen=True)
class DeviceTimelineReport:
    source_key: str
    agent_instance_id: int
    device_key: str
    status: TimelineStatus
    role_resolution: DeviceRoleResolution
    execution_spans: tuple[ExecutionStateSpan, ...]
    context_transitions: tuple[ContextTransition, ...]
    context_snapshots: tuple[ContextSnapshot, ...]
    timestamp_discontinuities: tuple[TimestampDiscontinuity, ...]
    agent_sequence_discontinuities: tuple[AgentSequenceDiscontinuity, ...]
    context_continuity_lost_at_end: bool

    @property
    def execution_is_usable(self) -> bool:
        return self.status is TimelineStatus.READY

    @property
    def final_context(self) -> ContextSnapshot | None:
        if self.context_continuity_lost_at_end:
            return None
        return self.context_snapshots[-1] if self.context_snapshots else None


_CONTEXT_ROLES = (
    SemanticRole.CONTROLLER_MODE,
    SemanticRole.PROGRAM,
    SemanticRole.PROGRAM_COMMENT,
    SemanticRole.LINE,
    SemanticRole.TOOL_NUMBER,
    SemanticRole.TOOL_GROUP,
    SemanticRole.PALLET,
)


def build_agent_stream_reports(
    observations: Iterable[CanonicalObservation],
) -> tuple[AgentStreamReport, ...]:
    """Order complete canonical Agent streams and surface genuine sequence gaps."""

    grouped: defaultdict[
        tuple[str, int], list[CanonicalObservation]
    ] = defaultdict(list)
    for observation in observations:
        grouped[(observation.source_key, observation.agent_instance_id)].append(
            observation
        )

    reports: list[AgentStreamReport] = []
    for (source_key, agent_instance_id), rows in sorted(grouped.items()):
        ordered = tuple(sorted(rows, key=lambda item: item.sequence))

        seen_sequences: set[int] = set()
        for observation in ordered:
            if observation.sequence in seen_sequences:
                raise AgentStreamError(
                    "duplicate canonical sequence in Agent stream: "
                    f"source={source_key!r}, instance={agent_instance_id}, "
                    f"sequence={observation.sequence}"
                )
            seen_sequences.add(observation.sequence)

        discontinuities: list[AgentSequenceDiscontinuity] = []
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current.sequence == previous.sequence + 1:
                continue
            discontinuities.append(
                AgentSequenceDiscontinuity(
                    source_key=source_key,
                    agent_instance_id=agent_instance_id,
                    previous_sequence=previous.sequence,
                    next_sequence=current.sequence,
                    missing_start_sequence=previous.sequence + 1,
                    missing_end_sequence=current.sequence - 1,
                    missing_count=current.sequence - previous.sequence - 1,
                )
            )

        reports.append(
            AgentStreamReport(
                source_key=source_key,
                agent_instance_id=agent_instance_id,
                observations=ordered,
                sequence_discontinuities=tuple(discontinuities),
            )
        )

    return tuple(reports)


def partition_agent_stream(
    report: AgentStreamReport,
) -> tuple[DeviceStreamPartition, ...]:
    """Partition a checked Agent stream without recomputing device-local gaps."""

    grouped: defaultdict[str, list[CanonicalObservation]] = defaultdict(list)
    previous_sequence: int | None = None
    for observation in report.observations:
        if (
            observation.source_key != report.source_key
            or observation.agent_instance_id != report.agent_instance_id
        ):
            raise AgentStreamError(
                "observation outside AgentStreamReport scope: "
                f"report=({report.source_key!r}, {report.agent_instance_id}), "
                f"observation=({observation.source_key!r}, "
                f"{observation.agent_instance_id}, {observation.sequence})"
            )
        if (
            previous_sequence is not None
            and observation.sequence <= previous_sequence
        ):
            raise AgentStreamError(
                "AgentStreamReport observations are not strictly sequence ordered"
            )
        previous_sequence = observation.sequence
        grouped[device_key(observation)].append(observation)

    return tuple(
        DeviceStreamPartition(
            source_key=report.source_key,
            agent_instance_id=report.agent_instance_id,
            device_key=canonical_device,
            observations=tuple(grouped[canonical_device]),
            agent_sequence_discontinuities=report.sequence_discontinuities,
        )
        for canonical_device in sorted(grouped)
    )


def partition_agent_streams(
    reports: Iterable[AgentStreamReport],
) -> tuple[DeviceStreamPartition, ...]:
    partitions: list[DeviceStreamPartition] = []
    for report in reports:
        partitions.extend(partition_agent_stream(report))
    return tuple(
        sorted(
            partitions,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
            ),
        )
    )


def _timeline_status(resolution: DeviceRoleResolution) -> TimelineStatus:
    status = resolution.role(SemanticRole.EXECUTION_STATE).status
    return {
        RoleResolutionStatus.RESOLVED: TimelineStatus.READY,
        RoleResolutionStatus.UNAVAILABLE: TimelineStatus.EXECUTION_UNAVAILABLE,
        RoleResolutionStatus.AMBIGUOUS: TimelineStatus.EXECUTION_AMBIGUOUS,
        RoleResolutionStatus.MULTI_CHANNEL: TimelineStatus.EXECUTION_MULTI_CHANNEL,
    }[status]


def _candidate_matches(
    observation: CanonicalObservation, candidate: RoleCandidate
) -> bool:
    if observation.component_path != candidate.component_path:
        return False
    if candidate.data_item_id is not None:
        return observation.data_item_id == candidate.data_item_id
    return (
        observation.data_item_id is None
        and observation.category == candidate.category
        and observation.observation_type == candidate.observation_type
    )


def _role_for_observation(
    observation: CanonicalObservation,
    resolution: DeviceRoleResolution,
    roles: tuple[SemanticRole, ...],
) -> SemanticRole | None:
    for role in roles:
        outcome = resolution.role(role)
        if outcome.status is not RoleResolutionStatus.RESOLVED:
            continue
        if any(
            _candidate_matches(observation, candidate)
            for candidate in outcome.candidates
        ):
            return role
    return None


def _execution_value(observation: CanonicalObservation) -> str | None:
    value = observation.value
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _close_execution_span(
    spans: list[ExecutionStateSpan],
    *,
    source_key: str,
    agent_instance_id: int,
    canonical_device: str,
    state: str,
    start_observation: CanonicalObservation,
    start_reason: str,
    partial_start: bool,
    start_data_item_id: str | None,
    end_observation: CanonicalObservation,
    end_reason: str,
    partial_end: bool,
) -> None:
    spans.append(
        ExecutionStateSpan(
            source_key=source_key,
            agent_instance_id=agent_instance_id,
            device_key=canonical_device,
            state=state,
            start_sequence=start_observation.sequence,
            start_observed_at=start_observation.observed_at,
            start_observed_at_us=start_observation.observed_at_us,
            end_sequence=end_observation.sequence,
            end_observed_at=end_observation.observed_at,
            end_observed_at_us=end_observation.observed_at_us,
            start_reason=start_reason,
            end_reason=end_reason,
            partial_start=partial_start,
            partial_end=partial_end,
            start_data_item_id=start_data_item_id,
        )
    )


def _snapshot(
    partition: DeviceStreamPartition,
    anchor: CanonicalObservation,
    values: dict[SemanticRole, ContextValue],
    pending_roles: set[SemanticRole],
) -> ContextSnapshot:
    return ContextSnapshot(
        source_key=partition.source_key,
        agent_instance_id=partition.agent_instance_id,
        device_key=partition.device_key,
        sequence=anchor.sequence,
        observed_at=anchor.observed_at,
        observed_at_us=anchor.observed_at_us,
        values=tuple(values[role] for role in _CONTEXT_ROLES if role in values),
        pending_roles=tuple(role for role in _CONTEXT_ROLES if role in pending_roles),
    )


def build_device_timeline(
    partition: DeviceStreamPartition,
) -> DeviceTimelineReport:
    """Reconstruct one deterministic device timeline from checked evidence."""

    if not partition.observations:
        raise AgentStreamError("cannot build a device timeline from an empty partition")

    resolutions = resolve_device_roles(partition.observations)
    if len(resolutions) != 1:
        raise AgentStreamError("device partition did not resolve to exactly one device")
    resolution = resolutions[0]
    if (
        resolution.source_key != partition.source_key
        or resolution.agent_instance_id != partition.agent_instance_id
        or resolution.device_key != partition.device_key
    ):
        raise AgentStreamError("role resolution does not match device partition scope")

    status = _timeline_status(resolution)
    execution_enabled = status is TimelineStatus.READY
    required_context_roles = {
        role
        for role in _CONTEXT_ROLES
        if resolution.role(role).status is RoleResolutionStatus.RESOLVED
    }
    pending_context_roles = set(required_context_roles)
    context_values: dict[SemanticRole, ContextValue] = {}
    context_transitions: list[ContextTransition] = []
    context_snapshots: list[ContextSnapshot] = []
    timestamp_discontinuities: list[TimestampDiscontinuity] = []
    execution_spans: list[ExecutionStateSpan] = []

    current_state: str | None = None
    state_start: CanonicalObservation | None = None
    state_start_reason = ""
    state_partial_start = False
    state_start_data_item_id: str | None = None
    ever_saw_execution = False
    gap_since_execution = False
    trailing_gap = False
    previous_observation: CanonicalObservation | None = None
    last_observation: CanonicalObservation | None = None
    gap_index = 0
    gaps = partition.agent_sequence_discontinuities

    def close_state(
        end_observation: CanonicalObservation,
        *,
        reason: str,
        partial_end: bool,
    ) -> None:
        nonlocal current_state, state_start
        if current_state is None or state_start is None:
            return
        _close_execution_span(
            execution_spans,
            source_key=partition.source_key,
            agent_instance_id=partition.agent_instance_id,
            canonical_device=partition.device_key,
            state=current_state,
            start_observation=state_start,
            start_reason=state_start_reason,
            partial_start=state_partial_start,
            start_data_item_id=state_start_data_item_id,
            end_observation=end_observation,
            end_reason=reason,
            partial_end=partial_end,
        )
        state_start = None

    def apply_gap() -> None:
        nonlocal current_state, state_start, gap_since_execution, trailing_gap
        if (
            execution_enabled
            and current_state is not None
            and last_observation is not None
        ):
            close_state(last_observation, reason="SEQUENCE_GAP", partial_end=True)
        current_state = None
        state_start = None
        gap_since_execution = True
        context_values.clear()
        pending_context_roles.clear()
        pending_context_roles.update(required_context_roles)
        trailing_gap = True

    for observation in partition.observations:
        while (
            gap_index < len(gaps)
            and gaps[gap_index].previous_sequence < observation.sequence
        ):
            apply_gap()
            gap_index += 1

        trailing_gap = False
        has_regression = (
            previous_observation is not None
            and observation.observed_at_us < previous_observation.observed_at_us
        )
        if has_regression:
            timestamp_discontinuities.append(
                TimestampDiscontinuity(
                    source_key=partition.source_key,
                    agent_instance_id=partition.agent_instance_id,
                    device_key=partition.device_key,
                    previous_sequence=previous_observation.sequence,
                    next_sequence=observation.sequence,
                    previous_observed_at_us=previous_observation.observed_at_us,
                    next_observed_at_us=observation.observed_at_us,
                )
            )

        execution_role = (
            _role_for_observation(
                observation,
                resolution,
                (SemanticRole.EXECUTION_STATE,),
            )
            if execution_enabled
            else None
        )
        context_role = _role_for_observation(
            observation,
            resolution,
            _CONTEXT_ROLES,
        )

        state_before_regression = current_state
        if (
            has_regression
            and execution_enabled
            and current_state is not None
            and previous_observation is not None
            and state_start is not None
        ):
            close_state(
                previous_observation,
                reason="TIMESTAMP_REGRESSION",
                partial_end=True,
            )
            state_start = None

        if execution_role is not None:
            new_state = _execution_value(observation)
            if new_state is None:
                if current_state is not None and state_start is not None:
                    close_state(
                        observation,
                        reason="EXECUTION_UNAVAILABLE",
                        partial_end=True,
                    )
                current_state = None
                state_start = None
                ever_saw_execution = True
            elif current_state is None or state_start is None:
                if has_regression and state_before_regression == new_state:
                    start_reason = "TIMESTAMP_REGRESSION"
                    partial_start = True
                elif gap_since_execution:
                    start_reason = "POST_GAP_EXECUTION"
                    partial_start = True
                elif not ever_saw_execution:
                    start_reason = "CAPTURE_BEGINS_WITH_EXECUTION"
                    partial_start = True
                else:
                    start_reason = "EXECUTION_OBSERVED"
                    partial_start = False
                current_state = new_state
                state_start = observation
                state_start_reason = start_reason
                state_partial_start = partial_start
                state_start_data_item_id = observation.data_item_id
                ever_saw_execution = True
                gap_since_execution = False
            elif new_state != current_state:
                close_state(
                    observation,
                    reason="EXECUTION_TRANSITION",
                    partial_end=False,
                )
                current_state = new_state
                state_start = observation
                state_start_reason = "EXECUTION_TRANSITION"
                state_partial_start = False
                state_start_data_item_id = observation.data_item_id
                ever_saw_execution = True
                gap_since_execution = False
            else:
                ever_saw_execution = True
                gap_since_execution = False
        elif (
            has_regression
            and execution_enabled
            and state_before_regression is not None
        ):
            current_state = state_before_regression
            state_start = observation
            state_start_reason = "TIMESTAMP_REGRESSION"
            state_partial_start = True
            state_start_data_item_id = None

        snapshot_needed = False
        if context_role is not None:
            previous = context_values.get(context_role)
            previous_value = previous.value if previous is not None else None
            new_value = observation.value
            available = new_value is not None
            establishes = context_role in pending_context_roles
            changed = (
                previous is None
                or previous.available != available
                or previous.value != new_value
            )
            context_values[context_role] = ContextValue(
                role=context_role,
                value=new_value,
                available=available,
                sequence=observation.sequence,
                observed_at=observation.observed_at,
                observed_at_us=observation.observed_at_us,
                data_item_id=observation.data_item_id,
            )
            pending_context_roles.discard(context_role)
            if changed or establishes:
                context_transitions.append(
                    ContextTransition(
                        source_key=partition.source_key,
                        agent_instance_id=partition.agent_instance_id,
                        device_key=partition.device_key,
                        sequence=observation.sequence,
                        observed_at=observation.observed_at,
                        observed_at_us=observation.observed_at_us,
                        role=context_role,
                        previous_value=previous_value,
                        new_value=new_value,
                        data_item_id=observation.data_item_id,
                        establishes_context=establishes,
                    )
                )
                snapshot_needed = True

        if snapshot_needed:
            context_snapshots.append(
                _snapshot(
                    partition,
                    observation,
                    context_values,
                    pending_context_roles,
                )
            )

        previous_observation = observation
        last_observation = observation

    while gap_index < len(gaps):
        apply_gap()
        gap_index += 1

    if execution_enabled and current_state is not None and state_start is not None:
        assert last_observation is not None
        close_state(last_observation, reason="CAPTURE_END", partial_end=True)

    if last_observation is not None:
        final_snapshot = _snapshot(
            partition,
            last_observation,
            context_values,
            pending_context_roles,
        )
        if not context_snapshots or context_snapshots[-1] != final_snapshot:
            context_snapshots.append(final_snapshot)

    return DeviceTimelineReport(
        source_key=partition.source_key,
        agent_instance_id=partition.agent_instance_id,
        device_key=partition.device_key,
        status=status,
        role_resolution=resolution,
        execution_spans=tuple(execution_spans),
        context_transitions=tuple(context_transitions),
        context_snapshots=tuple(context_snapshots),
        timestamp_discontinuities=tuple(timestamp_discontinuities),
        agent_sequence_discontinuities=partition.agent_sequence_discontinuities,
        context_continuity_lost_at_end=trailing_gap,
    )


def build_device_timeline_reports(
    observations: Iterable[CanonicalObservation],
) -> tuple[DeviceTimelineReport, ...]:
    """Run the complete S2 reconstruction pipeline over canonical observations."""

    agent_reports = build_agent_stream_reports(observations)
    partitions = partition_agent_streams(agent_reports)
    return tuple(build_device_timeline(partition) for partition in partitions)


__all__ = [
    "AgentSequenceDiscontinuity",
    "AgentStreamError",
    "AgentStreamReport",
    "ContextSnapshot",
    "ContextTransition",
    "ContextValue",
    "DeviceStreamPartition",
    "DeviceTimelineReport",
    "ExecutionStateSpan",
    "TimelineStatus",
    "TimestampDiscontinuity",
    "build_agent_stream_reports",
    "build_device_timeline",
    "build_device_timeline_reports",
    "partition_agent_stream",
    "partition_agent_streams",
]

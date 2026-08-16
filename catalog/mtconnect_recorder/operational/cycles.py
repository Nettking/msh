"""S4 explainable ProductionCycle inference over conservative MachineRuns.

A ProductionCycle is an inferred manufacturing interval. It never equates
``ACTIVE`` with production: v1 requires explicit process-motion evidence plus
manufacturing context, keeps insufficient evidence ``UNCERTAIN``, and uses only
strong pallet/workholding changes as optional intra-run split boundaries.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from ..canonical.observation import CanonicalObservation, VALUE_NUMBER
from .model import (
    BoundaryConfidence,
    BoundaryReason,
    OperationalBoundary,
    canonical_sha256,
)
from .policy import SEGMENTATION_POLICY, device_key
from .roles import RoleCandidate, RoleResolutionStatus, SemanticRole
from .runs import MachineRun, segment_machine_runs_from_reports
from .timeline import ContextTransition, DeviceTimelineReport, build_device_timeline_reports


class ProductionCycleError(ValueError):
    """Raised when S4 cannot preserve the S1-S3 evidence contract."""


class ProductionClassification(str, Enum):
    PRODUCTION_CANDIDATE = "PRODUCTION_CANDIDATE"
    NON_PRODUCTION = "NON_PRODUCTION"
    UNCERTAIN = "UNCERTAIN"


class ProductionEvidenceFlag(str, Enum):
    HAS_SPINDLE_MOTION = "HAS_SPINDLE_MOTION"
    HAS_PATH_FEED_MOTION = "HAS_PATH_FEED_MOTION"
    HAS_AXIS_FEED_MOTION = "HAS_AXIS_FEED_MOTION"
    HAS_TOOL_CONTEXT = "HAS_TOOL_CONTEXT"
    HAS_PROGRAM_CONTEXT = "HAS_PROGRAM_CONTEXT"
    HAS_PALLET_CONTEXT = "HAS_PALLET_CONTEXT"
    HAS_LOAD_TELEMETRY = "HAS_LOAD_TELEMETRY"


class ProductionReasonCode(str, Enum):
    PROCESS_MOTION_PRESENT = "PROCESS_MOTION_PRESENT"
    MANUFACTURING_CONTEXT_PRESENT = "MANUFACTURING_CONTEXT_PRESENT"
    AVAILABLE_MOTION_NO_PROCESS_MOTION = "AVAILABLE_MOTION_NO_PROCESS_MOTION"
    MOTION_SEMANTICS_INSUFFICIENT = "MOTION_SEMANTICS_INSUFFICIENT"
    MOTION_EVIDENCE_INSUFFICIENT = "MOTION_EVIDENCE_INSUFFICIENT"
    MANUFACTURING_CONTEXT_INSUFFICIENT = "MANUFACTURING_CONTEXT_INSUFFICIENT"


@dataclass(frozen=True)
class CycleEvidenceWitness:
    """One canonical witness establishing an existential S4 evidence flag."""

    flag: ProductionEvidenceFlag
    role: SemanticRole
    sequence: int
    observed_at: str
    observed_at_us: int
    data_item_id: str | None
    inherited: bool = False


@dataclass(frozen=True)
class ProductionCycle:
    """One classified interval fully contained in a single MachineRun."""

    cycle_id: str
    fingerprint: str
    segmentation_policy: str
    machine_run_id: str
    source_key: str
    agent_instance_id: int
    device_key: str
    start_boundary: OperationalBoundary
    end_boundary: OperationalBoundary
    classification: ProductionClassification
    evidence_flags: tuple[ProductionEvidenceFlag, ...]
    reason_codes: tuple[ProductionReasonCode, ...]
    evidence_witnesses: tuple[CycleEvidenceWitness, ...]
    available_motion_roles: tuple[SemanticRole, ...]
    observed_motion_roles: tuple[SemanticRole, ...]
    ambiguous_motion_roles: tuple[SemanticRole, ...]
    available_context_roles: tuple[SemanticRole, ...]
    wall_duration_us: int

    @property
    def start_sequence(self) -> int:
        return self.start_boundary.trigger_sequence

    @property
    def end_sequence(self) -> int:
        return self.end_boundary.trigger_sequence


@dataclass(frozen=True)
class _EvidenceSummary:
    classification: ProductionClassification
    evidence_flags: tuple[ProductionEvidenceFlag, ...]
    reason_codes: tuple[ProductionReasonCode, ...]
    witnesses: tuple[CycleEvidenceWitness, ...]
    available_motion_roles: tuple[SemanticRole, ...]
    observed_motion_roles: tuple[SemanticRole, ...]
    ambiguous_motion_roles: tuple[SemanticRole, ...]
    available_context_roles: tuple[SemanticRole, ...]


_MOTION_FLAGS = {
    SemanticRole.SPINDLE_VELOCITY: ProductionEvidenceFlag.HAS_SPINDLE_MOTION,
    SemanticRole.PATH_FEEDRATE: ProductionEvidenceFlag.HAS_PATH_FEED_MOTION,
    SemanticRole.AXIS_FEEDRATE: ProductionEvidenceFlag.HAS_AXIS_FEED_MOTION,
}
_CONTEXT_FLAG_ROLES = {
    ProductionEvidenceFlag.HAS_PROGRAM_CONTEXT: (SemanticRole.PROGRAM,),
    ProductionEvidenceFlag.HAS_TOOL_CONTEXT: (
        SemanticRole.TOOL_NUMBER,
        SemanticRole.TOOL_GROUP,
    ),
    ProductionEvidenceFlag.HAS_PALLET_CONTEXT: (SemanticRole.PALLET,),
}
_CONTEXT_ROLES = (
    SemanticRole.PROGRAM,
    SemanticRole.TOOL_NUMBER,
    SemanticRole.TOOL_GROUP,
    SemanticRole.PALLET,
)


def _candidate_matches(
    observation: CanonicalObservation,
    candidate: RoleCandidate,
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


def _in_interval(
    sequence: int,
    *,
    start_sequence: int,
    end_sequence: int,
    include_end: bool,
) -> bool:
    if sequence < start_sequence:
        return False
    if include_end:
        return sequence <= end_sequence
    return sequence < end_sequence


def _context_state_at_sequence(
    report: DeviceTimelineReport,
    sequence: int,
) -> dict[SemanticRole, ContextTransition]:
    """Return concrete resolved context known at one sequence, respecting gaps."""

    state: dict[SemanticRole, ContextTransition] = {}
    gaps = sorted(
        report.agent_sequence_discontinuities,
        key=lambda item: item.next_sequence,
    )
    gap_index = 0

    for transition in sorted(
        report.context_transitions,
        key=lambda item: item.sequence,
    ):
        if transition.sequence > sequence:
            break
        while (
            gap_index < len(gaps)
            and gaps[gap_index].next_sequence <= transition.sequence
        ):
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


def _context_evidence(
    report: DeviceTimelineReport,
    *,
    start_sequence: int,
    end_sequence: int,
    include_end: bool,
) -> tuple[
    dict[ProductionEvidenceFlag, CycleEvidenceWitness],
    tuple[SemanticRole, ...],
]:
    role_state = _context_state_at_sequence(report, start_sequence)
    witnesses: dict[ProductionEvidenceFlag, CycleEvidenceWitness] = {}

    def add_context_witness(
        flag: ProductionEvidenceFlag,
        transition: ContextTransition,
        *,
        inherited: bool,
    ) -> None:
        witnesses.setdefault(
            flag,
            CycleEvidenceWitness(
                flag=flag,
                role=transition.role,
                sequence=transition.sequence,
                observed_at=transition.observed_at,
                observed_at_us=transition.observed_at_us,
                data_item_id=transition.data_item_id,
                inherited=inherited,
            ),
        )

    for flag, roles in _CONTEXT_FLAG_ROLES.items():
        for role in roles:
            transition = role_state.get(role)
            if transition is not None and transition.new_value is not None:
                add_context_witness(flag, transition, inherited=True)
                break

    for transition in sorted(
        report.context_transitions,
        key=lambda item: item.sequence,
    ):
        if transition.sequence <= start_sequence:
            continue
        if not _in_interval(
            transition.sequence,
            start_sequence=start_sequence,
            end_sequence=end_sequence,
            include_end=include_end,
        ):
            if transition.sequence > end_sequence:
                break
            continue
        if transition.new_value is None:
            continue
        for flag, roles in _CONTEXT_FLAG_ROLES.items():
            if transition.role in roles:
                add_context_witness(flag, transition, inherited=False)

    available_roles = tuple(
        role
        for role in _CONTEXT_ROLES
        if report.role_resolution.role(role).status is RoleResolutionStatus.RESOLVED
    )
    return witnesses, available_roles


def _motion_and_load_evidence(
    report: DeviceTimelineReport,
    observations: tuple[CanonicalObservation, ...],
    *,
    start_sequence: int,
    end_sequence: int,
    include_end: bool,
) -> tuple[
    dict[ProductionEvidenceFlag, CycleEvidenceWitness],
    tuple[SemanticRole, ...],
    tuple[SemanticRole, ...],
    tuple[SemanticRole, ...],
]:
    witnesses: dict[ProductionEvidenceFlag, CycleEvidenceWitness] = {}
    available_motion_roles: list[SemanticRole] = []
    observed_motion_roles: list[SemanticRole] = []
    ambiguous_motion_roles: list[SemanticRole] = []

    roles_to_scan = (*_MOTION_FLAGS, SemanticRole.LOAD)
    for role in roles_to_scan:
        resolution = report.role_resolution.role(role)
        if role in _MOTION_FLAGS:
            if resolution.status in {
                RoleResolutionStatus.RESOLVED,
                RoleResolutionStatus.MULTI_CHANNEL,
            }:
                available_motion_roles.append(role)
            elif resolution.status is RoleResolutionStatus.AMBIGUOUS:
                ambiguous_motion_roles.append(role)

        if resolution.status not in {
            RoleResolutionStatus.RESOLVED,
            RoleResolutionStatus.MULTI_CHANNEL,
        }:
            continue

        target_flag = (
            _MOTION_FLAGS[role]
            if role in _MOTION_FLAGS
            else ProductionEvidenceFlag.HAS_LOAD_TELEMETRY
        )
        saw_numeric_motion = False
        for observation in observations:
            if not _in_interval(
                observation.sequence,
                start_sequence=start_sequence,
                end_sequence=end_sequence,
                include_end=include_end,
            ):
                continue
            if not any(
                _candidate_matches(observation, candidate)
                for candidate in resolution.candidates
            ):
                continue
            if observation.value_kind != VALUE_NUMBER:
                continue

            if role in _MOTION_FLAGS:
                if not saw_numeric_motion:
                    observed_motion_roles.append(role)
                    saw_numeric_motion = True
                if not (
                    observation.value_number is not None
                    and observation.value_number > 0
                ):
                    continue

            witnesses.setdefault(
                target_flag,
                CycleEvidenceWitness(
                    flag=target_flag,
                    role=role,
                    sequence=observation.sequence,
                    observed_at=observation.observed_at,
                    observed_at_us=observation.observed_at_us,
                    data_item_id=observation.data_item_id,
                ),
            )
            break

    return (
        witnesses,
        tuple(available_motion_roles),
        tuple(observed_motion_roles),
        tuple(ambiguous_motion_roles),
    )


def _classify_interval(
    report: DeviceTimelineReport,
    observations: tuple[CanonicalObservation, ...],
    *,
    start_sequence: int,
    end_sequence: int,
    include_end: bool,
) -> _EvidenceSummary:
    (
        motion_witnesses,
        available_motion,
        observed_motion,
        ambiguous_motion,
    ) = _motion_and_load_evidence(
        report,
        observations,
        start_sequence=start_sequence,
        end_sequence=end_sequence,
        include_end=include_end,
    )
    context_witnesses, available_context = _context_evidence(
        report,
        start_sequence=start_sequence,
        end_sequence=end_sequence,
        include_end=include_end,
    )

    witnesses = {**motion_witnesses, **context_witnesses}
    motion_present = any(flag in witnesses for flag in _MOTION_FLAGS.values())
    context_present = any(flag in witnesses for flag in _CONTEXT_FLAG_ROLES)

    if motion_present and context_present:
        classification = ProductionClassification.PRODUCTION_CANDIDATE
        reason_codes = (
            ProductionReasonCode.PROCESS_MOTION_PRESENT,
            ProductionReasonCode.MANUFACTURING_CONTEXT_PRESENT,
        )
    elif (
        not motion_present
        and observed_motion
        and not ambiguous_motion
    ):
        classification = ProductionClassification.NON_PRODUCTION
        reason_codes = (
            ProductionReasonCode.AVAILABLE_MOTION_NO_PROCESS_MOTION,
        )
    else:
        classification = ProductionClassification.UNCERTAIN
        reasons: list[ProductionReasonCode] = []
        if not motion_present:
            if not available_motion or ambiguous_motion:
                reasons.append(
                    ProductionReasonCode.MOTION_SEMANTICS_INSUFFICIENT
                )
            else:
                reasons.append(
                    ProductionReasonCode.MOTION_EVIDENCE_INSUFFICIENT
                )
        else:
            reasons.append(ProductionReasonCode.PROCESS_MOTION_PRESENT)
        if not context_present and motion_present:
            reasons.append(
                ProductionReasonCode.MANUFACTURING_CONTEXT_INSUFFICIENT
            )
        reason_codes = tuple(reasons)

    evidence_flags = tuple(
        flag for flag in ProductionEvidenceFlag if flag in witnesses
    )
    evidence_witnesses = tuple(witnesses[flag] for flag in evidence_flags)
    return _EvidenceSummary(
        classification=classification,
        evidence_flags=evidence_flags,
        reason_codes=reason_codes,
        witnesses=evidence_witnesses,
        available_motion_roles=available_motion,
        observed_motion_roles=observed_motion,
        ambiguous_motion_roles=ambiguous_motion,
        available_context_roles=available_context,
    )


def _pallet_boundary(
    report: DeviceTimelineReport,
    transition: ContextTransition,
) -> OperationalBoundary:
    return OperationalBoundary(
        source_key=report.source_key,
        agent_instance_id=report.agent_instance_id,
        device_key=report.device_key,
        reason=BoundaryReason.PALLET_CONTEXT_CHANGE,
        confidence=BoundaryConfidence.STRONG_INFERENCE,
        trigger_sequence=transition.sequence,
        evidence_sequence=transition.sequence,
        observed_at=transition.observed_at,
        observed_at_us=transition.observed_at_us,
    )


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


def _witness_payload(witness: CycleEvidenceWitness) -> dict[str, object]:
    return {
        "flag": witness.flag.value,
        "role": witness.role.value,
        "sequence": witness.sequence,
        "observed_at": witness.observed_at,
        "observed_at_us": witness.observed_at_us,
        "data_item_id": witness.data_item_id,
        "inherited": witness.inherited,
    }


def _make_cycle(
    run: MachineRun,
    *,
    start: OperationalBoundary,
    end: OperationalBoundary,
    summary: _EvidenceSummary,
) -> ProductionCycle:
    if start.source_key != run.source_key or end.source_key != run.source_key:
        raise ProductionCycleError("cycle boundary source differs from MachineRun")
    if (
        start.agent_instance_id != run.agent_instance_id
        or end.agent_instance_id != run.agent_instance_id
    ):
        raise ProductionCycleError("cycle crosses an Agent instance")
    if start.device_key != run.device_key or end.device_key != run.device_key:
        raise ProductionCycleError("cycle boundary device differs from MachineRun")
    if (
        start.trigger_sequence < run.start_sequence
        or end.trigger_sequence > run.end_sequence
        or end.trigger_sequence < start.trigger_sequence
    ):
        raise ProductionCycleError("cycle is not contained in its MachineRun")

    wall_duration_us = end.observed_at_us - start.observed_at_us
    if wall_duration_us < 0:
        raise ProductionCycleError("cycle boundary timestamps are non-monotone")

    identity_payload = {
        "kind": "production-cycle",
        "segmentation_policy": SEGMENTATION_POLICY,
        "source_key": run.source_key,
        "agent_instance_id": run.agent_instance_id,
        "device_key": run.device_key,
        "machine_run_id": run.run_id,
        "start_sequence": start.trigger_sequence,
        "end_sequence": end.trigger_sequence,
    }
    cycle_id = f"production-cycle:{canonical_sha256(identity_payload)}"
    content_payload = {
        **identity_payload,
        "cycle_id": cycle_id,
        "start_boundary": _boundary_payload(start),
        "end_boundary": _boundary_payload(end),
        "classification": summary.classification.value,
        "evidence_flags": [flag.value for flag in summary.evidence_flags],
        "reason_codes": [code.value for code in summary.reason_codes],
        "evidence_witnesses": [
            _witness_payload(witness) for witness in summary.witnesses
        ],
        "available_motion_roles": [
            role.value for role in summary.available_motion_roles
        ],
        "observed_motion_roles": [
            role.value for role in summary.observed_motion_roles
        ],
        "ambiguous_motion_roles": [
            role.value for role in summary.ambiguous_motion_roles
        ],
        "available_context_roles": [
            role.value for role in summary.available_context_roles
        ],
        "wall_duration_us": wall_duration_us,
    }
    return ProductionCycle(
        cycle_id=cycle_id,
        fingerprint=canonical_sha256(content_payload),
        segmentation_policy=SEGMENTATION_POLICY,
        machine_run_id=run.run_id,
        source_key=run.source_key,
        agent_instance_id=run.agent_instance_id,
        device_key=run.device_key,
        start_boundary=start,
        end_boundary=end,
        classification=summary.classification,
        evidence_flags=summary.evidence_flags,
        reason_codes=summary.reason_codes,
        evidence_witnesses=summary.witnesses,
        available_motion_roles=summary.available_motion_roles,
        observed_motion_roles=summary.observed_motion_roles,
        ambiguous_motion_roles=summary.ambiguous_motion_roles,
        available_context_roles=summary.available_context_roles,
        wall_duration_us=wall_duration_us,
    )


def infer_production_cycles(
    run: MachineRun,
    report: DeviceTimelineReport,
    observations: Iterable[CanonicalObservation],
) -> tuple[ProductionCycle, ...]:
    """Classify one MachineRun and apply only justified pallet splits."""

    if (
        run.source_key != report.source_key
        or run.agent_instance_id != report.agent_instance_id
        or run.device_key != report.device_key
    ):
        raise ProductionCycleError("MachineRun and timeline report scopes differ")

    rows = tuple(
        sorted(
            (
                observation
                for observation in observations
                if observation.source_key == run.source_key
                and observation.agent_instance_id == run.agent_instance_id
                and device_key(observation) == run.device_key
            ),
            key=lambda item: item.sequence,
        )
    )

    pallet_transitions = [
        transition
        for transition in report.context_transitions
        if transition.role is SemanticRole.PALLET
        and transition.previous_value is not None
        and transition.new_value is not None
        and transition.previous_value != transition.new_value
        and run.start_sequence < transition.sequence < run.end_sequence
    ]
    provisional_boundaries = [
        run.start_boundary,
        *(_pallet_boundary(report, item) for item in pallet_transitions),
        run.end_boundary,
    ]

    provisional_summaries: list[_EvidenceSummary] = []
    for index, (start, end) in enumerate(
        zip(
            provisional_boundaries,
            provisional_boundaries[1:],
            strict=False,
        )
    ):
        provisional_summaries.append(
            _classify_interval(
                report,
                rows,
                start_sequence=start.trigger_sequence,
                end_sequence=end.trigger_sequence,
                include_end=index == len(provisional_boundaries) - 2,
            )
        )

    kept_boundaries = [run.start_boundary]
    for index, boundary in enumerate(provisional_boundaries[1:-1], start=1):
        post_summary = provisional_summaries[index]
        if (
            post_summary.classification
            is ProductionClassification.PRODUCTION_CANDIDATE
        ):
            kept_boundaries.append(boundary)
    kept_boundaries.append(run.end_boundary)

    cycles: list[ProductionCycle] = []
    for index, (start, end) in enumerate(
        zip(kept_boundaries, kept_boundaries[1:], strict=False)
    ):
        summary = _classify_interval(
            report,
            rows,
            start_sequence=start.trigger_sequence,
            end_sequence=end.trigger_sequence,
            include_end=index == len(kept_boundaries) - 2,
        )
        cycles.append(
            _make_cycle(
                run,
                start=start,
                end=end,
                summary=summary,
            )
        )

    return tuple(cycles)


def build_production_cycles(
    observations: Iterable[CanonicalObservation],
) -> tuple[ProductionCycle, ...]:
    """Run canonical observations through S2, S3, and S4 deterministically."""

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

    cycles: list[ProductionCycle] = []
    for run in runs:
        scope = (run.source_key, run.agent_instance_id, run.device_key)
        report = reports_by_scope.get(scope)
        if report is None:
            raise ProductionCycleError("MachineRun has no matching S2 report")
        cycles.extend(
            infer_production_cycles(
                run,
                report,
                rows_by_scope.get(scope, ()),
            )
        )

    return tuple(
        sorted(
            cycles,
            key=lambda item: (
                item.source_key,
                item.agent_instance_id,
                item.device_key,
                item.start_sequence,
                item.end_sequence,
            ),
        )
    )


__all__ = [
    "CycleEvidenceWitness",
    "ProductionClassification",
    "ProductionCycle",
    "ProductionCycleError",
    "ProductionEvidenceFlag",
    "ProductionReasonCode",
    "build_production_cycles",
    "infer_production_cycles",
]

"""Evidence-based ranking of already-eligible provider candidates.

This module is the only place that turns learned state into a scheduling
preference, and it is deliberately replaceable: it implements the
:class:`~catalog.capabilities.provider_selection.CandidateRanker` protocol and
nothing else in the job path knows how it works.

The algorithm is a contextual multi-armed bandit in its simplest auditable
form. The *context* is the workload class; each eligible provider is an arm;
the reward model is the decayed statistics already stored per arm; and
exploration is an explicit, bounded, deterministic UCB-style bonus. Nothing
here is trained, so there is no model to serve, version, or debug, and every
number in a decision can be printed back to an operator.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, ClassVar

from catalog.federation.errors import FederationValidationError

from ..jobs import JobContract
from ..provider_reports import ProviderResourceReport
from ..provider_selection import CandidateRanking, ProviderCandidate
from .contracts import (
    MAX_FEATURES,
    ExecutionEnvironment,
    WorkloadDescriptor,
    stamp,
)
from .estimator import PerformanceEstimator
from .policy import (
    EXPLORATION_BUCKETS,
    EfficiencyPolicy,
    SchedulingObjective,
    objective_cost,
)
from .profiles import EvidenceTier, PerformanceEstimate

SCHEDULING_DECISION_SCHEMA = "fcp.scheduling-decision.v1"
CANDIDATE_EVALUATION_SCHEMA = "fcp.scheduling-candidate-evaluation.v1"

REASON_EVIDENCE = "learned-evidence-ranking"
REASON_EXPLORATION = "learned-exploration-probe"

WorkloadResolver = Callable[[JobContract], "WorkloadDescriptor | None"]
EnvironmentResolver = Callable[
    [JobContract, ProviderResourceReport], ExecutionEnvironment
]
DecisionSink = Callable[["SchedulingDecisionRecord"], None]


def default_workload_descriptor(job: JobContract) -> WorkloadDescriptor:
    """Derive a workload descriptor from an existing job contract.

    Only scalar requirement values become features, so the descriptor stays
    bounded. Deployments that know more about their jobs can pass their own
    resolver; the observation schema does not need to change for that.
    """

    requirement = job.capability
    requirements = requirement.requirements
    job_kind = requirements.get("job_kind")
    if not isinstance(job_kind, str) or not job_kind.strip():
        job_kind = requirement.protocol
    features: dict[str, Any] = {}
    for name in sorted(requirements):
        if name == "job_kind" or len(features) >= MAX_FEATURES - 1:
            continue
        value = requirements[name]
        if isinstance(value, (str, bool, int, float)):
            features[name] = value
    input_bytes = sum(
        reference.size_bytes or 0 for reference in job.inputs
    )
    if input_bytes > 0:
        features["input_bytes"] = input_bytes
    try:
        return WorkloadDescriptor(
            capability_type=requirement.capability_type,
            job_kind=job_kind,
            features=features,
        )
    except FederationValidationError:
        # A descriptor is an optimisation, never a reason to fail a job.
        return WorkloadDescriptor(
            capability_type=requirement.capability_type,
            job_kind=requirement.protocol,
        )


def default_environment(
    job: JobContract, report: ProviderResourceReport
) -> ExecutionEnvironment:
    """Derive the execution environment a candidate would run this job in.

    The model comes from the job requirement because that is what will
    actually be used; everything else comes from what the provider advertises.
    """

    attributes = report.attributes if isinstance(report.attributes, dict) else {}
    advertised = attributes.get("environment")
    advertised = advertised if isinstance(advertised, dict) else {}
    requirements = job.capability.requirements
    model = requirements.get("model")
    if not isinstance(model, str):
        models = requirements.get("models")
        model = (
            models[0]
            if isinstance(models, list) and models and isinstance(models[0], str)
            else None
        )

    def _text_or_none(value: Any) -> str | None:
        return value if isinstance(value, str) and value.strip() else None

    try:
        return ExecutionEnvironment(
            provider_kind=_text_or_none(attributes.get("provider_kind")),
            model_id=_text_or_none(model),
            software_version=_text_or_none(advertised.get("software_version")),
            runtime_version=_text_or_none(advertised.get("runtime_version")),
            hardware_class=_text_or_none(advertised.get("hardware_class")),
            accelerator=_text_or_none(advertised.get("accelerator")),
            os_family=_text_or_none(advertised.get("os_family")),
            architecture=_text_or_none(advertised.get("architecture")),
        )
    except FederationValidationError:
        return ExecutionEnvironment()


def exploration_slot(job_id: str, *, policy: EfficiencyPolicy) -> bool:
    """Whether this job may be used to gather evidence.

    Derived from the job identity rather than a global random source, so a
    replayed scheduling pass makes the same choice, the exploration share is
    bounded by ``exploration_ratio``, and ``exploration_ratio == 0`` makes
    ranking fully deterministic.
    """

    if not policy.exploration_enabled:
        return False
    digest = hashlib.sha256(
        f"{policy.exploration_seed}:{job_id}".encode("utf-8")
    ).digest()
    bucket = int.from_bytes(digest[:4], "big") % EXPLORATION_BUCKETS
    return bucket < int(policy.exploration_ratio * EXPLORATION_BUCKETS)


@dataclass(frozen=True)
class CandidateEvaluation:
    """Everything that was predicted about one candidate, for the record."""

    SCHEMA: ClassVar[str] = CANDIDATE_EVALUATION_SCHEMA

    capability_id: str
    node_id: str
    tier: EvidenceTier
    evidence: float
    observation_count: int
    success_probability: float
    expected_total_millis: float | None
    cost: float | None
    normalized_cost: float
    exploration_bonus: float
    environment_fingerprint: str
    queue_depth: int
    selected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "capability_id": self.capability_id,
            "node_id": self.node_id,
            "tier": self.tier.value,
            "evidence": round(self.evidence, 4),
            "observation_count": self.observation_count,
            "success_probability": round(self.success_probability, 4),
            "expected_total_millis": (
                None
                if self.expected_total_millis is None
                else round(self.expected_total_millis, 3)
            ),
            "cost": None if self.cost is None else round(self.cost, 3),
            "normalized_cost": round(self.normalized_cost, 6),
            "exploration_bonus": round(self.exploration_bonus, 6),
            "environment_fingerprint": self.environment_fingerprint,
            "queue_depth": self.queue_depth,
            "selected": self.selected,
        }


@dataclass(frozen=True)
class SchedulingDecisionRecord:
    """An inspectable account of one learned scheduling decision."""

    SCHEMA: ClassVar[str] = SCHEDULING_DECISION_SCHEMA

    decision_id: str
    session_id: str
    job_id: str
    objective: SchedulingObjective
    workload_class: str
    decided_at: datetime
    selected_capability_id: str | None
    selected_node_id: str | None
    candidates: tuple[CandidateEvaluation, ...]
    rejected: dict[str, tuple[str, ...]] = field(default_factory=dict)
    exploration: bool = False
    exploration_changed_selection: bool = False
    explanation: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "decision_id": self.decision_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "objective": self.objective.value,
            "workload_class": self.workload_class,
            "decided_at": stamp(self.decided_at),
            "selected_capability_id": self.selected_capability_id,
            "selected_node_id": self.selected_node_id,
            "candidates": [item.to_dict() for item in self.candidates],
            "rejected": {
                key: list(value) for key, value in sorted(self.rejected.items())
            },
            "exploration": self.exploration,
            "exploration_changed_selection": self.exploration_changed_selection,
            "explanation": self.explanation,
        }


def _seconds(millis: float | None) -> str:
    return "unknown" if millis is None else f"{millis / 1000.0:.1f} s"


def _explain(
    evaluations: tuple[CandidateEvaluation, ...],
    *,
    workload_class: str,
    objective: SchedulingObjective,
    exploration: bool,
) -> str:
    if not evaluations:
        return "No eligible candidate was ranked."
    best = evaluations[0]
    lead = (
        f"{best.node_id} was selected for {workload_class} "
        f"[{objective.value}]: {best.observation_count} comparable executions "
        f"averaged {_seconds(best.expected_total_millis)} "
        f"at a {best.success_probability:.0%} success rate "
        f"({best.tier.value} evidence)"
    )
    if len(evaluations) > 1:
        runner = evaluations[1]
        lead += (
            f", versus {_seconds(runner.expected_total_millis)} over "
            f"{runner.observation_count} executions on {runner.node_id} "
            f"at {runner.success_probability:.0%}"
        )
    if exploration:
        lead += "; this decision was an exploration probe"
    return lead + "."


class LearnedProviderRanker:
    """Rank eligible candidates by learned evidence for one objective."""

    def __init__(
        self,
        estimator: PerformanceEstimator,
        *,
        policy: EfficiencyPolicy | None = None,
        workload_resolver: WorkloadResolver = default_workload_descriptor,
        environment_resolver: EnvironmentResolver = default_environment,
        decision_sink: DecisionSink | None = None,
    ) -> None:
        self.estimator = estimator
        self.policy = policy or estimator.policy
        self.workload_resolver = workload_resolver
        self.environment_resolver = environment_resolver
        self.decision_sink = decision_sink

    # ------------------------------------------------------------------

    def _evaluate(
        self,
        job: JobContract,
        candidate: ProviderCandidate,
        workload: WorkloadDescriptor,
        *,
        evaluated_at: datetime,
    ) -> tuple[PerformanceEstimate, float | None, str]:
        report = candidate.report
        environment = self.environment_resolver(job, report)
        fingerprint = environment.fingerprint
        estimate = self.estimator.estimate(
            workload=workload,
            node_id=report.node_id,
            capability_id=report.capability_id,
            environment_fingerprint=fingerprint,
            now=evaluated_at,
            attributes=report.attributes,
        )
        cost = objective_cost(
            estimate,
            policy=self.policy,
            node_id=report.node_id,
            queue_depth=report.queue_depth,
            utilization_millis=report.utilization_millis,
        )
        return estimate, cost, fingerprint

    def rank(
        self,
        job: JobContract,
        candidates: tuple[ProviderCandidate, ...],
        *,
        evaluated_at: datetime,
    ) -> CandidateRanking | None:
        """Return a learned ordering, or ``None`` to keep existing behaviour."""

        workload = self.workload_resolver(job)
        if workload is None or len(candidates) < 2:
            return None

        estimates: list[tuple[ProviderCandidate, PerformanceEstimate, float | None, str]] = []
        for candidate in candidates:
            estimate, cost, fingerprint = self._evaluate(
                job, candidate, workload, evaluated_at=evaluated_at
            )
            estimates.append((candidate, estimate, cost, fingerprint))

        scored = [cost for _c, _e, cost, _f in estimates if cost is not None]
        if not scored:
            # Cold start: nothing is known about any candidate, so the
            # existing deterministic scheduler behaviour stands unchanged.
            return None

        # Costs are normalised so the exploration bonus is scale-free and can
        # be reasoned about as a fraction of the worst candidate.
        worst = max(scored)
        scale = worst if worst > 0 else 1.0
        total_evidence = sum(estimate.evidence for _c, estimate, _cost, _f in estimates)
        exploring = exploration_slot(job.job_id, policy=self.policy)

        rows: list[tuple[float, float, int, ProviderCandidate, CandidateEvaluation]] = []
        for index, (candidate, estimate, cost, fingerprint) in enumerate(estimates):
            # An unmeasured candidate is treated pessimistically when
            # exploiting and optimistically when exploring. That is what keeps
            # a new node from silently absorbing production traffic while
            # still guaranteeing it gets measured.
            normalized = 1.0 if cost is None else min(1.0, cost / scale)
            bonus = 0.0
            if self.policy.exploration_enabled:
                bonus = self.policy.exploration_bonus * math.sqrt(
                    math.log(total_evidence + 1.0) / (estimate.evidence + 1.0)
                )
            evaluation = CandidateEvaluation(
                capability_id=candidate.capability_id,
                node_id=candidate.report.node_id,
                tier=estimate.tier,
                evidence=estimate.evidence,
                observation_count=estimate.observation_count,
                success_probability=estimate.success_probability,
                expected_total_millis=estimate.expected_total_millis,
                cost=cost,
                normalized_cost=normalized,
                exploration_bonus=bonus,
                environment_fingerprint=fingerprint,
                queue_depth=candidate.report.queue_depth,
            )
            rows.append((normalized, normalized - bonus, index, candidate, evaluation))

        exploit_order = sorted(rows, key=lambda row: (row[0], row[2]))
        chosen_order = (
            sorted(rows, key=lambda row: (row[1], row[2])) if exploring else exploit_order
        )
        changed = exploring and chosen_order[0][3] is not exploit_order[0][3]

        ordered_ids = tuple(row[3].capability_id for row in chosen_order)
        evaluations = tuple(
            replace(row[4], selected=True) if position == 0 else row[4]
            for position, row in enumerate(chosen_order)
        )
        workload_class = workload.class_key("exact")
        explanation = _explain(
            evaluations,
            workload_class=workload_class,
            objective=self.policy.objective,
            exploration=changed,
        )
        record = SchedulingDecisionRecord(
            decision_id=f"{job.job_id}:{int(evaluated_at.timestamp() * 1000)}",
            session_id=job.session_id,
            job_id=job.job_id,
            objective=self.policy.objective,
            workload_class=workload_class,
            decided_at=evaluated_at,
            selected_capability_id=ordered_ids[0],
            selected_node_id=chosen_order[0][3].report.node_id,
            candidates=evaluations,
            exploration=exploring,
            exploration_changed_selection=changed,
            explanation=explanation,
        )
        if self.decision_sink is not None:
            try:
                self.decision_sink(record)
            except Exception:  # noqa: BLE001 - recording must not block ranking
                pass
        return CandidateRanking(
            ordered_capability_ids=ordered_ids,
            reason=REASON_EXPLORATION if changed else REASON_EVIDENCE,
            details=record.to_dict(),
        )


__all__ = [
    "CANDIDATE_EVALUATION_SCHEMA",
    "REASON_EVIDENCE",
    "REASON_EXPLORATION",
    "SCHEDULING_DECISION_SCHEMA",
    "CandidateEvaluation",
    "DecisionSink",
    "EnvironmentResolver",
    "LearnedProviderRanker",
    "SchedulingDecisionRecord",
    "WorkloadResolver",
    "default_environment",
    "default_workload_descriptor",
    "exploration_slot",
]

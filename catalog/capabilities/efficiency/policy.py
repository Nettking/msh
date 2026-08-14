"""Scheduling objectives and the bounded learning policy.

The policy is the only place where "what the federation is optimising" is
decided. Learned profiles keep every measurement; a policy turns them into one
comparable cost. Adding an objective later means adding one cost function, not
re-learning anything and not touching job execution.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import FederationValidationError

from .contracts import _text
from .profiles import PerformanceEstimate

EFFICIENCY_POLICY_SCHEMA = "fcp.efficiency-policy.v1"

MAX_HALF_LIFE_SECONDS = 365 * 24 * 60 * 60
MAX_EXPLORATION_RATIO = 0.5
MAX_EXPLORATION_BONUS = 4.0
#: Resolution of the deterministic exploration bucket.
EXPLORATION_BUCKETS = 10_000


class SchedulingObjective(str, Enum):
    """What a ranked comparison is trying to minimise.

    Every member is implemented except :data:`ENERGY_EFFICIENCY`, which is
    implemented but only produces a comparison once nodes actually report
    ``energy_millijoules``.
    """

    FASTEST_COMPLETION = "fastest-completion"
    LOWEST_QUEUE_TIME = "lowest-queue-time"
    HIGHEST_RELIABILITY = "highest-reliability"
    LOWEST_RESOURCE_COST = "lowest-resource-cost"
    MAXIMUM_THROUGHPUT = "maximum-throughput"
    BALANCED_UTILIZATION = "balanced-utilization"
    PREFERRED_LOCAL = "preferred-local"
    ENERGY_EFFICIENCY = "energy-efficiency"


def _ratio(value: Any, field_name: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FederationValidationError(
            "invalid-number", field_name, "must be a finite number"
        )
    number = float(value)
    if not math.isfinite(number) or number < 0.0 or number > maximum:
        raise FederationValidationError(
            "policy-out-of-range",
            field_name,
            f"must be between 0 and {maximum}",
        )
    return number


def _positive_int(value: Any, field_name: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field_name, "must be a positive integer"
        )
    if value > maximum:
        raise FederationValidationError(
            "policy-out-of-range", field_name, f"must not exceed {maximum}"
        )
    return value


@dataclass(frozen=True)
class EfficiencyPolicy:
    """Explicit, bounded knobs for learned scheduling.

    Defaults are conservative: a one-week half-life, a tenth of decisions
    available for exploration, and cross-environment evidence retained at a
    quarter of its weight. Setting ``exploration_ratio`` to ``0`` makes ranking
    fully deterministic.
    """

    SCHEMA: ClassVar[str] = EFFICIENCY_POLICY_SCHEMA

    objective: SchedulingObjective = SchedulingObjective.FASTEST_COMPLETION
    half_life_seconds: int = 7 * 24 * 60 * 60
    min_profile_evidence: float = 0.5
    confident_evidence: float = 3.0
    exploration_ratio: float = 0.1
    exploration_bonus: float = 0.5
    exploration_seed: str = "fcp-efficiency"
    reuse_cross_environment_evidence: bool = True
    cross_environment_evidence_weight: float = 0.25
    min_success_probability: float = 0.05
    max_observation_age_seconds: int = 90 * 24 * 60 * 60
    local_node_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "objective", SchedulingObjective(self.objective)
        )
        object.__setattr__(
            self,
            "half_life_seconds",
            _positive_int(
                self.half_life_seconds,
                "half_life_seconds",
                maximum=MAX_HALF_LIFE_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "max_observation_age_seconds",
            _positive_int(
                self.max_observation_age_seconds,
                "max_observation_age_seconds",
                maximum=MAX_HALF_LIFE_SECONDS * 10,
            ),
        )
        object.__setattr__(
            self,
            "min_profile_evidence",
            _ratio(self.min_profile_evidence, "min_profile_evidence", maximum=1e6),
        )
        object.__setattr__(
            self,
            "confident_evidence",
            _ratio(self.confident_evidence, "confident_evidence", maximum=1e6),
        )
        object.__setattr__(
            self,
            "exploration_ratio",
            _ratio(
                self.exploration_ratio,
                "exploration_ratio",
                maximum=MAX_EXPLORATION_RATIO,
            ),
        )
        object.__setattr__(
            self,
            "exploration_bonus",
            _ratio(
                self.exploration_bonus,
                "exploration_bonus",
                maximum=MAX_EXPLORATION_BONUS,
            ),
        )
        object.__setattr__(
            self, "exploration_seed", _text(self.exploration_seed, "exploration_seed")
        )
        if not isinstance(self.reuse_cross_environment_evidence, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "reuse_cross_environment_evidence",
                "must be a boolean",
            )
        object.__setattr__(
            self,
            "cross_environment_evidence_weight",
            _ratio(
                self.cross_environment_evidence_weight,
                "cross_environment_evidence_weight",
                maximum=1.0,
            ),
        )
        probability = _ratio(
            self.min_success_probability, "min_success_probability", maximum=1.0
        )
        if probability <= 0.0:
            raise FederationValidationError(
                "policy-out-of-range",
                "min_success_probability",
                "must be greater than zero",
            )
        object.__setattr__(self, "min_success_probability", probability)
        if self.local_node_id is not None:
            object.__setattr__(
                self, "local_node_id", _text(self.local_node_id, "local_node_id")
            )

    @property
    def exploration_enabled(self) -> bool:
        return self.exploration_ratio > 0.0 and self.exploration_bonus > 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "objective": self.objective.value,
            "half_life_seconds": self.half_life_seconds,
            "min_profile_evidence": self.min_profile_evidence,
            "confident_evidence": self.confident_evidence,
            "exploration_ratio": self.exploration_ratio,
            "exploration_bonus": self.exploration_bonus,
            "exploration_seed": self.exploration_seed,
            "reuse_cross_environment_evidence": (
                self.reuse_cross_environment_evidence
            ),
            "cross_environment_evidence_weight": (
                self.cross_environment_evidence_weight
            ),
            "min_success_probability": self.min_success_probability,
            "max_observation_age_seconds": self.max_observation_age_seconds,
            "local_node_id": self.local_node_id,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "efficiency_policy", "must be an object"
            )
        if value.get("schema") != cls.SCHEMA:
            raise FederationValidationError(
                "unsupported-policy-schema", "schema", f"expected {cls.SCHEMA}"
            )
        known = {item.name for item in dataclass_fields(cls)}
        return cls(**{name: value[name] for name in sorted(known) if name in value})


def expected_completion_millis(
    estimate: PerformanceEstimate, *, policy: EfficiencyPolicy
) -> float | None:
    """Failure-aware end-to-end cost.

    A node that finishes in four seconds but fails a third of the time is not
    a four-second node. Dividing by the success probability is the expected
    cost of a geometric retry sequence, which is exactly what the existing
    retry policy performs.
    """

    total = estimate.expected_total_millis
    if total is None:
        return None
    probability = max(policy.min_success_probability, estimate.success_probability)
    return total / probability


def objective_cost(
    estimate: PerformanceEstimate,
    *,
    policy: EfficiencyPolicy,
    node_id: str | None = None,
    queue_depth: int | None = None,
    utilization_millis: int | None = None,
) -> float | None:
    """Return the comparable cost of one candidate, or ``None`` when unknown.

    ``None`` means "this objective has no measurement for this candidate"; the
    caller then treats the candidate as unmeasured rather than as cheap.
    """

    objective = policy.objective
    if objective is SchedulingObjective.FASTEST_COMPLETION:
        return expected_completion_millis(estimate, policy=policy)
    if objective is SchedulingObjective.LOWEST_QUEUE_TIME:
        queue = estimate.metric("queue_millis")
        if queue is None:
            return None if queue_depth is None else float(queue_depth)
        return queue
    if objective is SchedulingObjective.HIGHEST_RELIABILITY:
        if estimate.evidence <= 0.0:
            return None
        return 1.0 - estimate.success_probability
    if objective is SchedulingObjective.LOWEST_RESOURCE_COST:
        memory = estimate.metric("memory_peak_bytes")
        vram = estimate.metric("vram_peak_bytes")
        if memory is None and vram is None:
            return None
        return float((memory or 0.0) + (vram or 0.0))
    if objective is SchedulingObjective.MAXIMUM_THROUGHPUT:
        throughput = estimate.metric("throughput_units_per_second")
        if throughput is None or throughput <= 0.0:
            return None
        return 1.0 / throughput
    if objective is SchedulingObjective.ENERGY_EFFICIENCY:
        energy = estimate.metric("energy_millijoules")
        return None if energy is None else energy
    if objective is SchedulingObjective.BALANCED_UTILIZATION:
        if utilization_millis is None and queue_depth is None:
            return None
        return float(utilization_millis or 0) + float(queue_depth or 0)
    if objective is SchedulingObjective.PREFERRED_LOCAL:
        if policy.local_node_id is None:
            return expected_completion_millis(estimate, policy=policy)
        local = node_id is not None and node_id == policy.local_node_id
        completion = expected_completion_millis(estimate, policy=policy) or 0.0
        # Local execution always sorts ahead; learned cost breaks ties within
        # each group without ever crossing the preference boundary.
        return completion if local else completion + float(2**52)
    raise FederationValidationError(
        "unsupported-objective", "objective", "no cost model for this objective"
    )


__all__ = [
    "EFFICIENCY_POLICY_SCHEMA",
    "EXPLORATION_BUCKETS",
    "MAX_EXPLORATION_BONUS",
    "MAX_EXPLORATION_RATIO",
    "MAX_HALF_LIFE_SECONDS",
    "EfficiencyPolicy",
    "SchedulingObjective",
    "expected_completion_millis",
    "objective_cost",
]

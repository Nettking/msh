"""Recency-weighted learned execution profiles.

The learned state is deliberately *sufficient statistics*, not a fitted model.
Each profile holds a small set of exponentially time-decayed moments so that

* every number can be printed and defended in a scheduling explanation,
* an update is O(1) and needs no training loop or offline corpus,
* stale evidence fades on its own, which is what makes the federation react
  to upgrades, throttling and changing load, and
* the raw observations remain the source of truth, so a different estimator
  can be recomputed later without touching job execution.

A profile never collapses to a single scalar score. The scheduling policy
chooses which measurements matter; the profile keeps all of them.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import FederationValidationError

from .contracts import (
    ExecutionObservation,
    _text,
    _utc,
    stamp,
)

DECAYED_STATISTIC_SCHEMA = "fcp.decayed-statistic.v1"
LEARNED_PROFILE_SCHEMA = "fcp.execution-profile.v1"
PERFORMANCE_ESTIMATE_SCHEMA = "fcp.performance-estimate.v1"

#: Wildcard used when a profile pools over one key dimension.
ANY = "*"

#: Learned metric name -> how it is read from one observation.
#: Adding a metric here is additive: old profiles simply lack the statistic.
METRIC_NAMES = (
    "queue_millis",
    "execution_millis",
    "total_millis",
    "cpu_utilization_millis",
    "gpu_utilization_millis",
    "memory_peak_bytes",
    "vram_peak_bytes",
    "throughput_units_per_second",
    "quality_score",
    "energy_millijoules",
    "retries",
)


class EvidenceTier(str, Enum):
    """Where one estimate came from, most trustworthy first."""

    MEASURED_EXACT = "measured-exact"
    MEASURED_SIMILAR = "measured-similar"
    MEASURED_LEGACY_ENVIRONMENT = "measured-legacy-environment"
    BENCHMARK = "benchmark"
    CAPABILITY_HEURISTIC = "capability-heuristic"
    FEDERATION_PRIOR = "federation-prior"
    NONE = "none"

    @property
    def rank(self) -> int:
        return _TIER_ORDER[self]


_TIER_ORDER = {
    EvidenceTier.MEASURED_EXACT: 0,
    EvidenceTier.MEASURED_SIMILAR: 1,
    EvidenceTier.MEASURED_LEGACY_ENVIRONMENT: 2,
    EvidenceTier.BENCHMARK: 3,
    EvidenceTier.CAPABILITY_HEURISTIC: 4,
    EvidenceTier.FEDERATION_PRIOR: 5,
    EvidenceTier.NONE: 6,
}


def _number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FederationValidationError(
            "invalid-number", field_name, "must be a finite number"
        )
    number = float(value)
    if not math.isfinite(number):
        raise FederationValidationError(
            "invalid-number", field_name, "must be a finite number"
        )
    return number


def decay_factor(
    *, elapsed_seconds: float, half_life_seconds: float
) -> float:
    """Return the exponential weight retained after ``elapsed_seconds``."""

    if half_life_seconds <= 0:
        raise FederationValidationError(
            "invalid-half-life",
            "half_life_seconds",
            "must be a positive number of seconds",
        )
    if elapsed_seconds <= 0:
        return 1.0
    # Guard against underflow to exactly zero producing NaN in later ratios.
    exponent = -elapsed_seconds / float(half_life_seconds)
    if exponent < -1_000:
        return 0.0
    return float(2.0**exponent)


@dataclass(frozen=True)
class DecayedStatistic:
    """Exponentially time-weighted count, mean and mean-square of one metric."""

    SCHEMA: ClassVar[str] = DECAYED_STATISTIC_SCHEMA

    weight: float = 0.0
    mean: float = 0.0
    mean_square: float = 0.0
    last_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("weight", "mean", "mean_square"):
            object.__setattr__(self, name, _number(getattr(self, name), name))
        if self.weight < 0:
            raise FederationValidationError(
                "invalid-weight", "weight", "must not be negative"
            )
        if self.last_at is not None:
            object.__setattr__(self, "last_at", _utc(self.last_at, "last_at"))

    def effective_weight(
        self, now: datetime, *, half_life_seconds: float
    ) -> float:
        """Return the evidence still standing at ``now`` after decay."""

        if self.last_at is None or self.weight <= 0:
            return 0.0
        elapsed = (_utc(now, "now") - self.last_at).total_seconds()
        return self.weight * decay_factor(
            elapsed_seconds=elapsed, half_life_seconds=half_life_seconds
        )

    def update(
        self, value: float, *, at: datetime, half_life_seconds: float
    ) -> DecayedStatistic:
        """Fold one measurement in without making arrival order semantic.

        ``last_at`` is the statistic's current time anchor. A newer observation
        decays the existing sufficient statistics forward to its timestamp. A
        delayed/older observation is instead decayed forward to the existing
        anchor before it is added. Because exponential weights compose this
        yields the same logical statistic as chronological replay.
        """

        observed = _number(value, "value")
        moment = _utc(at, "at")
        if self.last_at is None:
            prior_weight = 0.0
            sample_weight = 1.0
            anchor = moment
        elif moment >= self.last_at:
            elapsed = (moment - self.last_at).total_seconds()
            prior_weight = self.weight * decay_factor(
                elapsed_seconds=elapsed, half_life_seconds=half_life_seconds
            )
            sample_weight = 1.0
            anchor = moment
        else:
            elapsed = (self.last_at - moment).total_seconds()
            prior_weight = self.weight
            sample_weight = decay_factor(
                elapsed_seconds=elapsed, half_life_seconds=half_life_seconds
            )
            anchor = self.last_at
        total = prior_weight + sample_weight
        if total <= 0:
            return DecayedStatistic(last_at=anchor)
        return DecayedStatistic(
            weight=total,
            mean=(self.mean * prior_weight + observed * sample_weight) / total,
            mean_square=(
                self.mean_square * prior_weight + observed * observed * sample_weight
            )
            / total,
            last_at=anchor,
        )

    @property
    def variance(self) -> float:
        return max(0.0, self.mean_square - self.mean * self.mean)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def standard_error(self, now: datetime, *, half_life_seconds: float) -> float:
        evidence = self.effective_weight(now, half_life_seconds=half_life_seconds)
        if evidence <= 0:
            return float("inf")
        return self.stddev / math.sqrt(evidence)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "weight": self.weight,
            "mean": self.mean,
            "mean_square": self.mean_square,
            "last_at": None if self.last_at is None else stamp(self.last_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "statistic", "must be an object"
            )
        return cls(
            weight=value.get("weight", 0.0),
            mean=value.get("mean", 0.0),
            mean_square=value.get("mean_square", 0.0),
            last_at=value.get("last_at"),
        )


@dataclass(frozen=True)
class ProfileKey:
    """Identity of one learned profile.

    ``node_id``/``capability_id``/``environment_fingerprint`` may be
    :data:`ANY`, which is how the same structure stores the pooled federation
    prior and the cross-environment view used when a node changes model or
    software version.
    """

    capability_type: str
    workload_class: str
    node_id: str = ANY
    capability_id: str = ANY
    environment_fingerprint: str = ANY

    def __post_init__(self) -> None:
        for name in (
            "capability_type",
            "workload_class",
            "node_id",
            "capability_id",
            "environment_fingerprint",
        ):
            object.__setattr__(
                self, name, _text(getattr(self, name), name, maximum=512)
            )
        for name in ("capability_type", "node_id", "capability_id"):
            if "|" in getattr(self, name):
                raise FederationValidationError(
                    "invalid-profile-key",
                    name,
                    "must not contain the key separator",
                )

    def canonical(self) -> str:
        return "|".join(
            (
                self.capability_type,
                self.workload_class.replace("|", "/"),
                self.node_id,
                self.capability_id,
                self.environment_fingerprint,
            )
        )

    @property
    def pooled(self) -> bool:
        return self.node_id == ANY

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability_type": self.capability_type,
            "workload_class": self.workload_class,
            "node_id": self.node_id,
            "capability_id": self.capability_id,
            "environment_fingerprint": self.environment_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "profile_key", "must be an object"
            )
        return cls(
            capability_type=value["capability_type"],
            workload_class=value["workload_class"],
            node_id=value.get("node_id", ANY),
            capability_id=value.get("capability_id", ANY),
            environment_fingerprint=value.get("environment_fingerprint", ANY),
        )


@dataclass(frozen=True)
class PerformanceEstimate:
    """A tiered, uncertainty-aware prediction for one candidate.

    Predictions are kept as separate measurements rather than a single score so
    that a different objective can be optimised without re-learning anything.
    """

    SCHEMA: ClassVar[str] = PERFORMANCE_ESTIMATE_SCHEMA

    tier: EvidenceTier
    evidence: float = 0.0
    observation_count: int = 0
    success_probability: float = 0.5
    metrics: dict[str, float] = field(default_factory=dict)
    total_millis_stddev: float | None = None
    last_observed_at: datetime | None = None
    source_key: str | None = None
    heuristic_factor: float = 1.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "tier", EvidenceTier(self.tier))
        object.__setattr__(self, "evidence", max(0.0, _number(self.evidence, "evidence")))
        if isinstance(self.observation_count, bool) or not isinstance(
            self.observation_count, int
        ):
            raise FederationValidationError(
                "invalid-integer", "observation_count", "must be an integer"
            )
        probability = _number(self.success_probability, "success_probability")
        object.__setattr__(
            self, "success_probability", min(1.0, max(0.0, probability))
        )
        metrics = self.metrics or {}
        if not isinstance(metrics, dict):
            raise FederationValidationError(
                "invalid-object", "metrics", "must be an object"
            )
        object.__setattr__(
            self,
            "metrics",
            {
                str(name): _number(metrics[name], f"metrics.{name}")
                for name in sorted(metrics)
                if metrics[name] is not None
            },
        )
        if self.total_millis_stddev is not None:
            object.__setattr__(
                self,
                "total_millis_stddev",
                max(0.0, _number(self.total_millis_stddev, "total_millis_stddev")),
            )
        if self.last_observed_at is not None:
            object.__setattr__(
                self, "last_observed_at", _utc(self.last_observed_at, "last_observed_at")
            )
        object.__setattr__(
            self,
            "heuristic_factor",
            max(0.01, _number(self.heuristic_factor, "heuristic_factor")),
        )

    def metric(self, name: str) -> float | None:
        return self.metrics.get(name)

    @property
    def expected_total_millis(self) -> float | None:
        """End-to-end cost, derived from parts when not measured directly."""

        total = self.metrics.get("total_millis")
        if total is not None:
            return total
        execution = self.metrics.get("execution_millis")
        if execution is None:
            return None
        return execution + (self.metrics.get("queue_millis") or 0.0)

    @property
    def measured(self) -> bool:
        return self.tier in {
            EvidenceTier.MEASURED_EXACT,
            EvidenceTier.MEASURED_SIMILAR,
            EvidenceTier.MEASURED_LEGACY_ENVIRONMENT,
        }

    def scaled(self, factor: float, *, tier: EvidenceTier) -> PerformanceEstimate:
        """Return this estimate re-tiered and scaled by a heuristic factor."""

        multiplier = max(0.01, _number(factor, "factor"))
        durations = {
            "queue_millis",
            "execution_millis",
            "total_millis",
            "energy_millijoules",
        }
        metrics = {
            name: (value * multiplier if name in durations else value)
            for name, value in self.metrics.items()
        }
        return replace(
            self,
            tier=tier,
            metrics=metrics,
            heuristic_factor=multiplier,
            total_millis_stddev=(
                None
                if self.total_millis_stddev is None
                else self.total_millis_stddev * multiplier
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "tier": self.tier.value,
            "evidence": round(self.evidence, 6),
            "observation_count": self.observation_count,
            "success_probability": round(self.success_probability, 6),
            "metrics": {
                name: round(value, 6) for name, value in self.metrics.items()
            },
            "total_millis_stddev": (
                None
                if self.total_millis_stddev is None
                else round(self.total_millis_stddev, 6)
            ),
            "last_observed_at": (
                None if self.last_observed_at is None else stamp(self.last_observed_at)
            ),
            "source_key": self.source_key,
            "heuristic_factor": round(self.heuristic_factor, 6),
        }


def empty_estimate() -> PerformanceEstimate:
    """The estimate used when nothing at all is known about a candidate."""

    return PerformanceEstimate(tier=EvidenceTier.NONE)


@dataclass(frozen=True)
class LearnedProfile:
    """Decayed sufficient statistics for one ``ProfileKey``."""

    SCHEMA: ClassVar[str] = LEARNED_PROFILE_SCHEMA

    key: ProfileKey
    outcome: DecayedStatistic = field(default_factory=DecayedStatistic)
    metrics: dict[str, DecayedStatistic] = field(default_factory=dict)
    observation_count: int = 0
    first_observed_at: datetime | None = None
    last_observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, ProfileKey):
            object.__setattr__(self, "key", ProfileKey.from_dict(self.key))
        if not isinstance(self.outcome, DecayedStatistic):
            object.__setattr__(
                self, "outcome", DecayedStatistic.from_dict(self.outcome)
            )
        metrics = self.metrics or {}
        object.__setattr__(
            self,
            "metrics",
            {
                name: (
                    value
                    if isinstance(value, DecayedStatistic)
                    else DecayedStatistic.from_dict(value)
                )
                for name, value in sorted(metrics.items())
                if name in METRIC_NAMES
            },
        )
        if isinstance(self.observation_count, bool) or not isinstance(
            self.observation_count, int
        ):
            raise FederationValidationError(
                "invalid-integer", "observation_count", "must be an integer"
            )
        for name in ("first_observed_at", "last_observed_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value, name))

    @staticmethod
    def metric_values(observation: ExecutionObservation) -> dict[str, float]:
        """Extract the learnable metrics present in one observation.

        Durations are recorded for failed executions too: a failure that burns
        sixty seconds before erroring is a real scheduling cost.
        """

        measurements = observation.measurements
        values: dict[str, float] = {}
        for name in METRIC_NAMES:
            if name == "total_millis":
                raw = measurements.completion_millis
            else:
                raw = getattr(measurements, name, None)
            if raw is not None:
                values[name] = float(raw)
        return values

    def update(
        self, observation: ExecutionObservation, *, half_life_seconds: float
    ) -> LearnedProfile:
        """Return a new profile including ``observation``."""

        at = observation.observed_at
        metrics = dict(self.metrics)
        for name, value in self.metric_values(observation).items():
            statistic = metrics.get(name, DecayedStatistic())
            metrics[name] = statistic.update(
                value, at=at, half_life_seconds=half_life_seconds
            )
        outcome = self.outcome.update(
            1.0 if observation.measurements.succeeded else 0.0,
            at=at,
            half_life_seconds=half_life_seconds,
        )
        return LearnedProfile(
            key=self.key,
            outcome=outcome,
            metrics=metrics,
            observation_count=self.observation_count + 1,
            first_observed_at=min(at, self.first_observed_at or at),
            last_observed_at=max(at, self.last_observed_at or at),
        )

    def evidence(self, now: datetime, *, half_life_seconds: float) -> float:
        return self.outcome.effective_weight(
            now, half_life_seconds=half_life_seconds
        )

    def success_probability(
        self, now: datetime, *, half_life_seconds: float
    ) -> float:
        """Laplace-smoothed success rate over decayed trials.

        Smoothing keeps a single lucky success from reading as certainty and
        keeps a single failure from permanently condemning a node.
        """

        trials = self.evidence(now, half_life_seconds=half_life_seconds)
        successes = self.outcome.mean * trials
        return (successes + 1.0) / (trials + 2.0)

    def estimate(
        self,
        now: datetime,
        *,
        half_life_seconds: float,
        tier: EvidenceTier,
        evidence_scale: float = 1.0,
    ) -> PerformanceEstimate:
        """Project this profile into a tiered prediction at ``now``."""

        evidence = self.evidence(now, half_life_seconds=half_life_seconds)
        metrics = {
            name: statistic.mean
            for name, statistic in self.metrics.items()
            if statistic.weight > 0
        }
        total = self.metrics.get("total_millis")
        return PerformanceEstimate(
            tier=tier,
            evidence=evidence * max(0.0, float(evidence_scale)),
            observation_count=self.observation_count,
            success_probability=self.success_probability(
                now, half_life_seconds=half_life_seconds
            ),
            metrics=metrics,
            total_millis_stddev=None if total is None else total.stddev,
            last_observed_at=self.last_observed_at,
            source_key=self.key.canonical(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "key": self.key.to_dict(),
            "outcome": self.outcome.to_dict(),
            "metrics": {
                name: statistic.to_dict()
                for name, statistic in sorted(self.metrics.items())
            },
            "observation_count": self.observation_count,
            "first_observed_at": (
                None if self.first_observed_at is None else stamp(self.first_observed_at)
            ),
            "last_observed_at": (
                None if self.last_observed_at is None else stamp(self.last_observed_at)
            ),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "profile", "must be an object"
            )
        if value.get("schema") != cls.SCHEMA:
            raise FederationValidationError(
                "unsupported-profile-schema",
                "schema",
                f"expected {cls.SCHEMA}",
            )
        return cls(
            key=ProfileKey.from_dict(value["key"]),
            outcome=DecayedStatistic.from_dict(value.get("outcome") or {}),
            metrics={
                name: DecayedStatistic.from_dict(item)
                for name, item in (value.get("metrics") or {}).items()
            },
            observation_count=value.get("observation_count", 0),
            first_observed_at=value.get("first_observed_at"),
            last_observed_at=value.get("last_observed_at"),
        )


def observation_profile_keys(
    observation: ExecutionObservation,
) -> tuple[ProfileKey, ...]:
    """Every profile identity one observation contributes to.

    Three views are maintained per workload tier so that lookup is a direct
    key read rather than a scan:

    ``node + provider + environment``
        the exact learned behaviour.
    ``node + provider + any environment``
        used only after the node changes model/software version, at a demoted
        evidence tier.
    ``federation-wide``
        the pooled prior used for a node nobody has measured yet.
    """

    capability_type = observation.capability_type
    node_id = observation.node_id
    capability_id = observation.capability_id
    environment = observation.environment_fingerprint
    keys: list[ProfileKey] = []
    for _tier, workload_class in observation.workload.class_keys():
        keys.append(
            ProfileKey(
                capability_type=capability_type,
                workload_class=workload_class,
                node_id=node_id,
                capability_id=capability_id,
                environment_fingerprint=environment,
            )
        )
        keys.append(
            ProfileKey(
                capability_type=capability_type,
                workload_class=workload_class,
                node_id=node_id,
                capability_id=capability_id,
                environment_fingerprint=ANY,
            )
        )
        keys.append(
            ProfileKey(
                capability_type=capability_type,
                workload_class=workload_class,
            )
        )
    return tuple(keys)


__all__ = [
    "ANY",
    "DECAYED_STATISTIC_SCHEMA",
    "LEARNED_PROFILE_SCHEMA",
    "METRIC_NAMES",
    "PERFORMANCE_ESTIMATE_SCHEMA",
    "DecayedStatistic",
    "EvidenceTier",
    "LearnedProfile",
    "PerformanceEstimate",
    "ProfileKey",
    "decay_factor",
    "empty_estimate",
    "observation_profile_keys",
]

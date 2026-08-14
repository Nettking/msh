"""The cold-start hierarchy that turns evidence into one prediction.

Scheduling must work on day one, so evidence is resolved in a fixed order and
the tier that produced an answer travels with it:

1. measured behaviour of *this* workload on *this* node and environment,
2. measured behaviour of a *similar* workload on the same node/environment,
3. measured behaviour on the same node before its current model/software
   version, at a reduced weight (never reused blindly),
4. the existing federation benchmark evidence for that node,
5. hardware/capability heuristics from what the provider advertises,
6. the pooled federation prior for the workload class,
7. nothing at all, in which case the existing scheduler behaviour stands.

Only the resolution order lives here. What the numbers *mean* is the policy's
job, and how they are ranked is the ranker's.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from typing import Protocol

from catalog.federation.onboarding_models import BenchmarkResult, BenchmarkState

from .contracts import WorkloadDescriptor
from .policy import EfficiencyPolicy
from .profiles import (
    ANY,
    EvidenceTier,
    PerformanceEstimate,
    ProfileKey,
    empty_estimate,
)
from .store import SQLiteExecutionLearningStore

#: Nominal evidence attributed to one benchmark run. Deliberately small: a
#: synthetic probe is a hint, not a production measurement.
BENCHMARK_EVIDENCE = 1.0

MIN_HEURISTIC_FACTOR = 0.25
MAX_HEURISTIC_FACTOR = 4.0


class BenchmarkEvidenceSource(Protocol):
    """Existing federation benchmark evidence, as a scheduling estimate."""

    def estimate(
        self,
        *,
        node_id: str,
        capability_id: str,
        capability_type: str,
        now: datetime,
    ) -> PerformanceEstimate | None: ...


class HardwareHeuristicSource(Protocol):
    """Relative speed advertised by a provider, larger meaning faster."""

    def speed_factor(self, attributes: dict) -> float: ...


class AdvertisedHardwareHeuristics:
    """Read one bounded, explicitly advertised relative-speed attribute.

    A provider may advertise ``attributes["hardware"]["compute_units"]``.
    ``1.0`` is the federation baseline. The value is clamped so an exaggerated
    advertisement can shift ranking a little and never dominate it.
    """

    def speed_factor(self, attributes: dict) -> float:
        if not isinstance(attributes, dict):
            return 1.0
        hardware = attributes.get("hardware")
        if not isinstance(hardware, dict):
            return 1.0
        raw = hardware.get("compute_units")
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return 1.0
        value = float(raw)
        if value != value or value <= 0.0:  # NaN or non-positive
            return 1.0
        return min(MAX_HEURISTIC_FACTOR, max(MIN_HEURISTIC_FACTOR, value))


class BenchmarkResultEvidence:
    """Adapt existing :class:`BenchmarkResult` records to scheduling estimates.

    Only passing, unexpired results count, and only metric names the adapter
    recognises are mapped. Unknown benchmark metrics are ignored rather than
    guessed at.
    """

    _DURATION_MILLIS = (
        "total_millis",
        "latency_millis",
        "duration_millis",
        "elapsed_millis",
    )
    _DURATION_SECONDS = ("duration_seconds", "latency_seconds")
    _THROUGHPUT = (
        "throughput_units_per_second",
        "tokens_per_second",
        "throughput",
    )

    def __init__(self, results: Sequence[BenchmarkResult]) -> None:
        self._by_node: dict[str, list[BenchmarkResult]] = {}
        for result in results:
            self._by_node.setdefault(result.device_id, []).append(result)

    @classmethod
    def _metrics(cls, result: BenchmarkResult) -> dict[str, float]:
        metrics: dict[str, float] = {}
        source = result.metrics if isinstance(result.metrics, dict) else {}

        def _number(name: str) -> float | None:
            raw = source.get(name)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return None
            value = float(raw)
            return value if value == value and value >= 0.0 else None

        for name in cls._DURATION_MILLIS:
            value = _number(name)
            if value is not None:
                metrics["total_millis"] = value
                break
        else:
            for name in cls._DURATION_SECONDS:
                value = _number(name)
                if value is not None:
                    metrics["total_millis"] = value * 1000.0
                    break
        for name in cls._THROUGHPUT:
            value = _number(name)
            if value is not None:
                metrics["throughput_units_per_second"] = value
                break
        return metrics

    def estimate(
        self,
        *,
        node_id: str,
        capability_id: str,
        capability_type: str,
        now: datetime,
    ) -> PerformanceEstimate | None:
        candidates = [
            result
            for result in self._by_node.get(node_id, ())
            if result.state is BenchmarkState.PASSED and result.expires_at > now
        ]
        if not candidates:
            return None
        latest = max(candidates, key=lambda result: result.finished_at)
        metrics = self._metrics(latest)
        if not metrics:
            return None
        return PerformanceEstimate(
            tier=EvidenceTier.BENCHMARK,
            evidence=BENCHMARK_EVIDENCE,
            observation_count=1,
            # A passing benchmark says the capability works, not that it is
            # reliable in production. Keep the prior modest.
            success_probability=0.75,
            metrics=metrics,
            last_observed_at=latest.finished_at,
            source_key=f"benchmark:{latest.benchmark_id}:{latest.run_id}",
        )


class PerformanceEstimator:
    """Resolve the best available evidence for one candidate."""

    def __init__(
        self,
        store: SQLiteExecutionLearningStore,
        *,
        policy: EfficiencyPolicy | None = None,
        benchmarks: BenchmarkEvidenceSource | None = None,
        heuristics: HardwareHeuristicSource | None = None,
    ) -> None:
        self.store = store
        self.policy = policy or store.policy
        self.benchmarks = benchmarks
        self.heuristics = heuristics or AdvertisedHardwareHeuristics()

    # ------------------------------------------------------------------

    def _profile_estimate(
        self,
        key: ProfileKey,
        *,
        now: datetime,
        tier: EvidenceTier,
        evidence_scale: float = 1.0,
    ) -> PerformanceEstimate | None:
        profile = self.store.profile(key)
        if profile is None:
            return None
        estimate = profile.estimate(
            now,
            half_life_seconds=self.policy.half_life_seconds,
            tier=tier,
            evidence_scale=evidence_scale,
        )
        if estimate.evidence < self.policy.min_profile_evidence:
            return None
        return estimate

    def federation_prior(
        self, workload: WorkloadDescriptor, *, now: datetime
    ) -> PerformanceEstimate | None:
        """Pooled behaviour of this workload class across the whole federation.

        The pooled mean is a reasonable guess for a node nobody has measured,
        but it is not evidence *about that node*. Evidence is therefore
        returned as zero so an unmeasured candidate keeps the full exploration
        bonus instead of inheriting the federation's confidence.
        """

        for _tier, workload_class in workload.class_keys():
            estimate = self._profile_estimate(
                ProfileKey(
                    capability_type=workload.capability_type,
                    workload_class=workload_class,
                ),
                now=now,
                tier=EvidenceTier.FEDERATION_PRIOR,
            )
            if estimate is not None:
                return replace(estimate, evidence=0.0)
        return None

    def estimate(
        self,
        *,
        workload: WorkloadDescriptor,
        node_id: str,
        capability_id: str,
        environment_fingerprint: str,
        now: datetime,
        attributes: dict | None = None,
    ) -> PerformanceEstimate:
        """Return the highest-quality estimate available for one candidate."""

        capability_type = workload.capability_type
        tiers = workload.class_keys()

        # 1 and 2: measured, same node and same environment.
        for index, (_tier, workload_class) in enumerate(tiers):
            estimate = self._profile_estimate(
                ProfileKey(
                    capability_type=capability_type,
                    workload_class=workload_class,
                    node_id=node_id,
                    capability_id=capability_id,
                    environment_fingerprint=environment_fingerprint,
                ),
                now=now,
                tier=(
                    EvidenceTier.MEASURED_EXACT
                    if index == 0
                    else EvidenceTier.MEASURED_SIMILAR
                ),
            )
            if estimate is not None:
                return estimate

        # 3: measured on this node before the current model/software version.
        if self.policy.reuse_cross_environment_evidence:
            for _tier, workload_class in tiers:
                estimate = self._profile_estimate(
                    ProfileKey(
                        capability_type=capability_type,
                        workload_class=workload_class,
                        node_id=node_id,
                        capability_id=capability_id,
                        environment_fingerprint=ANY,
                    ),
                    now=now,
                    tier=EvidenceTier.MEASURED_LEGACY_ENVIRONMENT,
                    evidence_scale=self.policy.cross_environment_evidence_weight,
                )
                if estimate is not None:
                    return estimate

        # 4: existing federation benchmark evidence.
        if self.benchmarks is not None:
            benchmark = self.benchmarks.estimate(
                node_id=node_id,
                capability_id=capability_id,
                capability_type=capability_type,
                now=now,
            )
            if benchmark is not None:
                return benchmark

        prior = self.federation_prior(workload, now=now)

        # 5: hardware heuristics, expressed against the pooled prior so the
        # numbers stay in real units instead of an invented scale.
        speed = self.heuristics.speed_factor(attributes or {})
        if prior is not None and speed != 1.0:
            return replace(
                prior.scaled(1.0 / speed, tier=EvidenceTier.CAPABILITY_HEURISTIC),
                source_key=prior.source_key,
            )

        # 6: the pooled prior on its own.
        if prior is not None:
            return prior

        # 7: nothing is known.
        return empty_estimate()


__all__ = [
    "BENCHMARK_EVIDENCE",
    "MAX_HEURISTIC_FACTOR",
    "MIN_HEURISTIC_FACTOR",
    "AdvertisedHardwareHeuristics",
    "BenchmarkEvidenceSource",
    "BenchmarkResultEvidence",
    "HardwareHeuristicSource",
    "PerformanceEstimator",
]

"""Authenticated network-path evidence through explicit secure local seams."""

from __future__ import annotations

import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)

from ..inspection import InspectionContext, InspectionFinding
from ..runner import (
    BenchmarkCancelled,
    BenchmarkExecutionContext,
    BenchmarkObservation,
)
from .common import (
    MAX_ADAPTER_TARGETS,
    bounded_positive_int,
    dependency_matches,
    safe_identifier,
)

NETWORK_BENCHMARK_ID = "benchmark.network.authenticated-path.v1"
NETWORK_PREREQUISITE = "authenticated-network-path"


@dataclass(frozen=True)
class AuthenticatedPathSample:
    authenticated: bool
    latency_ms: float
    transferred_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.authenticated, bool):
            raise TypeError("authenticated must be boolean")
        if isinstance(self.latency_ms, bool) or float(self.latency_ms) < 0:
            raise ValueError("latency_ms must be non-negative")
        if (
            isinstance(self.transferred_bytes, bool)
            or not isinstance(self.transferred_bytes, int)
            or self.transferred_bytes < 0
        ):
            raise ValueError("transferred_bytes must be non-negative")


PathProbe = Callable[[bytes, BenchmarkExecutionContext], AuthenticatedPathSample]


@dataclass(frozen=True)
class AuthenticatedPathTarget:
    path_id: str
    route_kind: str
    path_fingerprint: str
    probe: PathProbe = field(repr=False, compare=False)
    sample_count: int = 1
    payload_bytes: int = 128

    def __post_init__(self) -> None:
        object.__setattr__(self, "path_id", safe_identifier(self.path_id, "path_id"))
        route_kind = safe_identifier(self.route_kind, "route_kind")
        if route_kind not in {"direct", "relay", "local-secure"}:
            raise ValueError("route_kind must identify a supported authenticated seam")
        object.__setattr__(self, "route_kind", route_kind)
        object.__setattr__(
            self,
            "path_fingerprint",
            safe_identifier(self.path_fingerprint, "path_fingerprint"),
        )
        if not callable(self.probe):
            raise TypeError("probe must be callable")
        object.__setattr__(
            self,
            "sample_count",
            bounded_positive_int(self.sample_count, "sample_count", maximum=3),
        )
        object.__setattr__(
            self,
            "payload_bytes",
            bounded_positive_int(self.payload_bytes, "payload_bytes", maximum=512),
        )


class AuthenticatedNetworkPathAdapter:
    """Measure only paths already authenticated by an existing secure seam."""

    definition = BenchmarkDefinition(
        benchmark_id=NETWORK_BENCHMARK_ID,
        capability_type="network-path",
        capability_protocol="msh-authenticated-path",
        implementation_version="1.0.0",
        max_duration_seconds=10,
        max_parallelism=2,
        prerequisites=(NETWORK_PREREQUISITE,),
        metric_names=(
            "authenticated",
            "sample_count",
            "successful_samples",
            "median_latency_ms",
            "payload_bytes",
            "transferred_bytes",
        ),
        invalidation_inputs=("path_fingerprint",),
        privacy_classification="public-summary",
    )
    # Evidence is explicitly refreshed by the operator. Keep a finite ten-year
    # safety horizon so normal restarts and idle time do not force reruns.
    result_ttl_seconds = 315_360_000

    def __init__(self, targets: Sequence[AuthenticatedPathTarget]) -> None:
        if not targets or len(targets) > MAX_ADAPTER_TARGETS:
            raise ValueError("network paths must be explicitly bounded")
        self._targets: dict[str, AuthenticatedPathTarget] = {}
        for target in targets:
            if not isinstance(target, AuthenticatedPathTarget):
                raise TypeError("targets must contain AuthenticatedPathTarget values")
            if target.path_id in self._targets:
                raise ValueError("duplicate authenticated path_id")
            self._targets[target.path_id] = target

    def dependency_inputs(self, path_id: str) -> dict[str, str]:
        return {"path_fingerprint": self._targets[path_id].path_fingerprint}

    def __call__(self, context: InspectionContext) -> InspectionFinding:
        return InspectionFinding(
            resource_observations={
                "network": {
                    "authenticated_path_count": len(self._targets),
                    "status": "secure-seam-available",
                }
            },
            detected_services=tuple(sorted(self._targets)),
            available_prerequisites=(NETWORK_PREREQUISITE,),
            recommended_benchmark_ids=(self.definition.benchmark_id,),
        )

    def benchmark(self, context: BenchmarkExecutionContext) -> BenchmarkObservation:
        target = self._targets.get(context.target_service_id)
        if target is None:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.UNSUPPORTED,
                diagnostics=("Network path is not in the authenticated local inventory",),
            )
        if not dependency_matches(context.dependency_inputs, self.dependency_inputs(target.path_id)):
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                diagnostics=("Authenticated network path changed before benchmark execution",),
            )
        payload = b"msh-cf2b-authenticated-path"[: target.payload_bytes]
        if len(payload) < target.payload_bytes:
            payload = (payload * ((target.payload_bytes // len(payload)) + 1))[
                : target.payload_bytes
            ]
        samples: list[AuthenticatedPathSample] = []
        try:
            for _ in range(target.sample_count):
                context.raise_if_cancelled()
                sample = target.probe(payload, context)
                if not isinstance(sample, AuthenticatedPathSample):
                    raise TypeError("authenticated path probe returned an invalid sample")
                samples.append(sample)
        except (BenchmarkCancelled, TimeoutError):
            raise
        except Exception:  # noqa: BLE001 - seam failures become safe evidence
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=("Authenticated network-path sampling failed",),
            )
        authenticated = all(sample.authenticated for sample in samples)
        metrics = {
            "authenticated": authenticated,
            "sample_count": target.sample_count,
            "successful_samples": len(samples),
            "median_latency_ms": round(
                statistics.median(sample.latency_ms for sample in samples), 3
            ),
            "payload_bytes": len(payload),
            "transferred_bytes": sum(sample.transferred_bytes for sample in samples),
        }
        if not authenticated:
            return BenchmarkObservation(
                metrics=metrics,
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=("Network-path sample was not authenticated",),
            )
        return BenchmarkObservation(
            metrics=metrics,
            recommendation=BenchmarkRecommendation.USABLE,
            diagnostics=("Private route descriptors are excluded from evidence",),
        )

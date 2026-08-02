"""Authority-free storage candidate inspection and cleanup-bound evidence."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)

from ..inspection import InspectionContext, InspectionFinding
from ..runner import BenchmarkCancelled, BenchmarkExecutionContext, BenchmarkObservation
from .common import (
    MAX_ADAPTER_TARGETS,
    MAX_PROBE_PAYLOAD_BYTES,
    bounded_positive_int,
    dependency_matches,
    elapsed_ms,
    safe_identifier,
    stable_fingerprint,
)

STORAGE_BENCHMARK_ID = "benchmark.storage.candidate-roundtrip.v1"
STORAGE_PREREQUISITE = "storage-probe-cleanup"

WriteProbe = Callable[[str, bytes, BenchmarkExecutionContext], object]
ReadProbe = Callable[[str, BenchmarkExecutionContext], bytes]
CleanupProbe = Callable[[str, BenchmarkExecutionContext], bool]


@dataclass(frozen=True)
class StorageCandidateTarget:
    """Explicit authority-free probe seam for one local storage candidate."""

    candidate_id: str
    display_label: str
    candidate_fingerprint: str
    write_probe: WriteProbe = field(repr=False, compare=False)
    read_probe: ReadProbe = field(repr=False, compare=False)
    cleanup_probe: CleanupProbe = field(repr=False, compare=False)
    payload_bytes: int = 256

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidate_id", safe_identifier(self.candidate_id, "candidate_id"))
        object.__setattr__(self, "display_label", safe_identifier(self.display_label, "display_label"))
        object.__setattr__(
            self,
            "candidate_fingerprint",
            safe_identifier(self.candidate_fingerprint, "candidate_fingerprint"),
        )
        for name in ("write_probe", "read_probe", "cleanup_probe"):
            if not callable(getattr(self, name)):
                raise TypeError(f"{name} must be callable")
        object.__setattr__(
            self,
            "payload_bytes",
            bounded_positive_int(
                self.payload_bytes,
                "payload_bytes",
                maximum=MAX_PROBE_PAYLOAD_BYTES,
            ),
        )


class StorageCandidateAdapter:
    """Run a bounded write/read/cleanup cycle without assigning authority."""

    definition = BenchmarkDefinition(
        benchmark_id=STORAGE_BENCHMARK_ID,
        capability_type="storage",
        capability_protocol="msh-storage-candidate",
        implementation_version="1.0.0",
        max_duration_seconds=10,
        max_parallelism=1,
        prerequisites=(STORAGE_PREREQUISITE,),
        metric_names=(
            "write_completed",
            "read_completed",
            "cleanup_completed",
            "round_trip_match",
            "payload_bytes",
            "write_latency_ms",
            "read_latency_ms",
            "cleanup_latency_ms",
        ),
        invalidation_inputs=("candidate_fingerprint",),
        privacy_classification="public-summary",
    )
    result_ttl_seconds = 900

    def __init__(
        self,
        targets: Sequence[StorageCandidateTarget],
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not targets or len(targets) > MAX_ADAPTER_TARGETS:
            raise ValueError("storage candidates must be explicitly bounded")
        self._targets: dict[str, StorageCandidateTarget] = {}
        for target in targets:
            if not isinstance(target, StorageCandidateTarget):
                raise TypeError("targets must contain StorageCandidateTarget values")
            if target.candidate_id in self._targets:
                raise ValueError("duplicate storage candidate_id")
            self._targets[target.candidate_id] = target
        self._monotonic = monotonic

    def dependency_inputs(self, candidate_id: str) -> dict[str, str]:
        return {"candidate_fingerprint": self._targets[candidate_id].candidate_fingerprint}

    def __call__(self, context: InspectionContext) -> InspectionFinding:
        return InspectionFinding(
            resource_observations={
                "storage": {
                    "candidate_count": len(self._targets),
                    "authority_state": "candidate-only",
                }
            },
            detected_services=tuple(sorted(self._targets)),
            available_prerequisites=(STORAGE_PREREQUISITE,),
            recommended_benchmark_ids=(self.definition.benchmark_id,),
        )

    def benchmark(self, context: BenchmarkExecutionContext) -> BenchmarkObservation:
        target = self._targets.get(context.target_service_id)
        if target is None:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.UNSUPPORTED,
                diagnostics=("Storage candidate is not in the trusted local probe inventory",),
            )
        if not dependency_matches(context.dependency_inputs, self.dependency_inputs(target.candidate_id)):
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                diagnostics=("Storage candidate changed before benchmark execution",),
            )

        payload_seed = stable_fingerprint(
            {"device_id": context.device_id, "candidate_id": target.candidate_id}
        ).encode("ascii")
        payload = (payload_seed * ((target.payload_bytes // len(payload_seed)) + 1))[
            : target.payload_bytes
        ]
        probe_id = stable_fingerprint(
            {"device_id": context.device_id, "candidate_id": target.candidate_id, "payload": payload.hex()}
        )
        write_completed = False
        read_completed = False
        cleanup_completed = False
        round_trip_match = False
        write_latency = read_latency = cleanup_latency = 0.0
        failure = False
        cancelled: BaseException | None = None

        try:
            context.raise_if_cancelled()
            started = self._monotonic()
            target.write_probe(probe_id, payload, context)
            write_latency = elapsed_ms(started, self._monotonic())
            write_completed = True

            context.raise_if_cancelled()
            started = self._monotonic()
            value = target.read_probe(probe_id, context)
            read_latency = elapsed_ms(started, self._monotonic())
            read_completed = isinstance(value, bytes)
            round_trip_match = read_completed and value == payload
        except (BenchmarkCancelled, TimeoutError) as exc:
            cancelled = exc
        except Exception:
            failure = True
        finally:
            try:
                started = self._monotonic()
                cleanup_completed = bool(target.cleanup_probe(probe_id, context))
                cleanup_latency = elapsed_ms(started, self._monotonic())
            except Exception:
                cleanup_completed = False
                failure = True

        if cancelled is not None:
            if isinstance(cancelled, BenchmarkCancelled):
                raise cancelled
            raise TimeoutError("storage candidate probe timed out") from cancelled

        metrics = {
            "write_completed": write_completed,
            "read_completed": read_completed,
            "cleanup_completed": cleanup_completed,
            "round_trip_match": round_trip_match,
            "payload_bytes": len(payload),
            "write_latency_ms": write_latency,
            "read_latency_ms": read_latency,
            "cleanup_latency_ms": cleanup_latency,
        }
        passed = (
            not failure
            and write_completed
            and read_completed
            and cleanup_completed
            and round_trip_match
        )
        if not passed:
            return BenchmarkObservation(
                metrics=metrics,
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=("Storage candidate round trip or cleanup failed",),
            )
        return BenchmarkObservation(
            metrics=metrics,
            recommendation=BenchmarkRecommendation.USABLE,
            diagnostics=(
                "Storage remains candidate-only; this evidence grants no authority",
            ),
        )

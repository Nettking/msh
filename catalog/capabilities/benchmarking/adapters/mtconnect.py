"""Read-only MTConnect source inspection and suitability evidence."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)

from ..inspection import InspectionContext, InspectionFinding
from ..policy import DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS
from ..runner import BenchmarkExecutionContext, BenchmarkObservation
from .common import dependency_matches, safe_identifier, stable_fingerprint

MTCONNECT_BENCHMARK_ID = "benchmark.recorder.mtconnect-source.v1"
MTCONNECT_PREREQUISITE = "mtconnect-scan-current"


class MtconnectSourceAdapter:
    """Consume an existing bounded discovery snapshot without starting a scan."""

    definition = BenchmarkDefinition(
        benchmark_id=MTCONNECT_BENCHMARK_ID,
        capability_type="recorder",
        capability_protocol="mtconnect",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=4,
        prerequisites=(MTCONNECT_PREREQUISITE,),
        metric_names=(
            "reachable",
            "protocol_compatible",
            "machine_count",
            "stable_identity_count",
            "host_fallback_count",
        ),
        invalidation_inputs=("scan_fingerprint", "source_fingerprint"),
        privacy_classification="public-summary",
    )
    result_ttl_seconds = DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS

    def __init__(self, scan_supplier: Callable[[], Mapping[str, Any]]) -> None:
        if not callable(scan_supplier):
            raise TypeError("scan_supplier must be callable")
        self._scan_supplier = scan_supplier

    def _snapshot(self) -> tuple[Mapping[str, Any], dict[str, Mapping[str, Any]]]:
        scan = self._scan_supplier()
        if not isinstance(scan, Mapping):
            raise TypeError("MTConnect scan supplier returned an invalid snapshot")
        results = scan.get("results", ())
        if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
            results = ()
        by_id: dict[str, Mapping[str, Any]] = {}
        for item in list(results)[:64]:
            if not isinstance(item, Mapping):
                continue
            try:
                source_id = safe_identifier(item.get("source_id"), "source_id")
            except ValueError:
                continue
            by_id[source_id] = item
        return scan, by_id

    @staticmethod
    def _source_fingerprint(source_id: str, source: Mapping[str, Any]) -> str:
        machines = source.get("machines", ())
        identities: list[str] = []
        if isinstance(machines, Sequence) and not isinstance(machines, (str, bytes)):
            for machine in list(machines)[:64]:
                if isinstance(machine, Mapping):
                    identities.append(
                        stable_fingerprint(
                            {
                                "kind": str(machine.get("identity_kind", "unknown")),
                                "value": str(machine.get("identity_value", "unknown")),
                            }
                        )
                    )
        return stable_fingerprint(
            {
                "source_id": source_id,
                "probe_sha256": str(source.get("probe_sha256", "")),
                "machine_identity_fingerprints": sorted(identities),
            }
        )

    def dependency_inputs(self, source_id: str) -> dict[str, str]:
        scan, sources = self._snapshot()
        source = sources[source_id]
        return {
            "scan_fingerprint": stable_fingerprint(
                {
                    "schema": str(scan.get("schema", "")),
                    "state": str(scan.get("state", "")),
                    "finished_at": str(scan.get("finished_at", "")),
                }
            ),
            "source_fingerprint": self._source_fingerprint(source_id, source),
        }

    def __call__(self, context: InspectionContext) -> InspectionFinding:
        scan, sources = self._snapshot()
        state = str(scan.get("state", "unknown"))
        machine_count = sum(
            int(item.get("machine_count", 0))
            for item in sources.values()
            if isinstance(item.get("machine_count", 0), int)
        )
        current = state == "complete" and bool(sources)
        return InspectionFinding(
            resource_observations={
                "mtconnect": {
                    "scan_state": state
                    if state in {"complete", "never", "unavailable", "running"}
                    else "unknown",
                    "source_count": len(sources),
                    "machine_count": machine_count,
                }
            },
            detected_services=("mtconnect-recorder-source",) if sources else (),
            detected_data_sources=tuple(sorted(sources)),
            available_prerequisites=(MTCONNECT_PREREQUISITE,) if current else (),
            recommended_benchmark_ids=(self.definition.benchmark_id,) if current else (),
            warnings=()
            if current
            else ("No current bounded MTConnect discovery evidence is available",),
        )

    def benchmark(self, context: BenchmarkExecutionContext) -> BenchmarkObservation:
        scan, sources = self._snapshot()
        source = sources.get(context.target_service_id)
        if source is None:
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.UNSUPPORTED,
                diagnostics=(
                    "MTConnect source is not present in the current discovery snapshot",
                ),
            )
        if not dependency_matches(
            context.dependency_inputs,
            self.dependency_inputs(context.target_service_id),
        ):
            return BenchmarkObservation(
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                diagnostics=(
                    "MTConnect discovery evidence changed before evaluation",
                ),
            )
        context.raise_if_cancelled()
        machines = source.get("machines", ())
        if not isinstance(machines, Sequence) or isinstance(machines, (str, bytes)):
            machines = ()
        kinds = [
            str(machine.get("identity_kind", "host"))
            for machine in list(machines)[:64]
            if isinstance(machine, Mapping)
        ]
        host_fallbacks = sum(kind == "host" for kind in kinds)
        stable = sum(kind in {"uuid", "serial", "device_id"} for kind in kinds)
        reachable = str(scan.get("state")) == "complete"
        compatible = bool(source.get("probe_sha256")) and bool(machines)
        if not reachable or not compatible:
            return BenchmarkObservation(
                metrics={
                    "reachable": reachable,
                    "protocol_compatible": compatible,
                    "machine_count": len(machines),
                    "stable_identity_count": stable,
                    "host_fallback_count": host_fallbacks,
                },
                state=BenchmarkState.FAILED,
                recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                diagnostics=(
                    "MTConnect source lacks current compatible probe evidence",
                ),
            )
        return BenchmarkObservation(
            metrics={
                "reachable": True,
                "protocol_compatible": True,
                "machine_count": len(machines),
                "stable_identity_count": stable,
                "host_fallback_count": host_fallbacks,
            },
            recommendation=(
                BenchmarkRecommendation.RECOMMENDED
                if host_fallbacks == 0
                else BenchmarkRecommendation.USABLE
            ),
            diagnostics=("Recorder suitability evidence does not start recording",),
        )

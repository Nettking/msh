from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from catalog.capabilities.contributions import (
    AICandidateSource,
    AICandidateSpec,
    ComputeCandidateSource,
    ContributionCandidateGenerator,
    RecorderCandidateSource,
    StorageCandidateSource,
    StorageCandidateSpec,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
    DeviceInspectionSnapshot,
)

NOW = datetime(2026, 8, 2, 20, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Descriptor:
    handler_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    descriptor_fingerprint: str


@dataclass(frozen=True)
class Binding:
    descriptor: Descriptor


class Inventory:
    def __init__(self) -> None:
        self.bindings = {
            "safe-handler": Binding(
                Descriptor(
                    "safe-handler",
                    "synthetic-compute",
                    "fcp-synthetic",
                    "1.0",
                    "sha256:" + "a" * 64,
                )
            )
        }

    def list_bindings(self):
        return tuple(self.bindings.values())

    def get(self, handler_id):
        return self.bindings.get(handler_id)


def inspection() -> DeviceInspectionSnapshot:
    return DeviceInspectionSnapshot(
        device_id="device-1",
        revision=4,
        os_family="linux",
        architecture="x86_64",
        resource_observations={},
        detected_services=("ollama-local", "storage-local"),
        registered_handlers=("safe-handler", "unregistered-handler"),
        detected_data_sources=("machine-one",),
        recommended_benchmark_ids=("ai-benchmark",),
        warnings=(),
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
    )


def test_candidates_use_inspection_and_benchmark_only_as_evidence() -> None:
    definitions = (
        BenchmarkDefinition(
            benchmark_id="ai-benchmark",
            capability_type="language-model",
            capability_protocol="ollama",
            implementation_version="1",
            max_duration_seconds=5,
            max_parallelism=1,
            prerequisites=(),
            metric_names=("latency_ms",),
            invalidation_inputs=(),
            privacy_classification="public-summary",
        ),
    )
    result = BenchmarkResult(
        run_id="run-ai",
        device_id="device-1",
        benchmark_id="ai-benchmark",
        benchmark_version="1",
        target_service_id="ollama-local",
        state=BenchmarkState.PASSED,
        metrics={"latency_ms": 100},
        recommendation=BenchmarkRecommendation.RECOMMENDED,
        dependency_fingerprint="sha256:" + "b" * 64,
        diagnostics=(),
        started_at=NOW,
        finished_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(minutes=5),
    )
    generator = ContributionCandidateGenerator(
        sources=(
            RecorderCandidateSource(),
            AICandidateSource(
                {
                    "ollama-local": AICandidateSpec(
                        "ollama-local", "ollama", "Local AI", {"models": 1}
                    )
                }
            ),
            ComputeCandidateSource(Inventory()),
            StorageCandidateSource(
                {
                    "storage-local": StorageCandidateSpec(
                        "storage-local", "fcp-storage", "Local storage", {}
                    )
                }
            ),
        ),
        benchmark_definitions=definitions,
        now=lambda: NOW,
    )
    recommendations = generator.generate(inspection(), (result,))
    candidates = {item.candidate.capability_type: item for item in recommendations}

    assert set(candidates) == {"recorder", "language-model", "compute", "storage"}
    assert candidates["language-model"].candidate.benchmark_run_ids == ("run-ai",)
    assert candidates["language-model"].candidate.capacity_envelope[
        "benchmark_metrics"
    ] == {"latency_ms": 100}
    assert candidates["language-model"].candidate.policy_state.value == "not-evaluated"
    assert candidates["compute"].logical_service_id == "safe-handler"
    assert "unregistered-handler" not in str(recommendations)
    assert candidates["storage"].candidate.capacity_envelope["authority"] == (
        "candidate-only"
    )

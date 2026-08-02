from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.benchmarking import (
    BenchmarkObservation,
    BenchmarkRegistry,
    DeviceInspector,
    InspectionFinding,
)
from catalog.federation.onboarding_models import BenchmarkDefinition


def _definition(
    benchmark_id: str,
    *,
    prerequisites: tuple[str, ...] = (),
) -> BenchmarkDefinition:
    return BenchmarkDefinition(
        benchmark_id=benchmark_id,
        capability_type="compute",
        capability_protocol="msh.compute.v1",
        implementation_version="1.0.0",
        max_duration_seconds=1,
        max_parallelism=1,
        prerequisites=prerequisites,
        metric_names=("latency_ms",),
        invalidation_inputs=(),
        privacy_classification="public-summary",
    )


def test_inspection_is_coarse_extensible_and_redacts_private_details() -> None:
    now = datetime(2026, 8, 2, 20, 0, tzinfo=timezone.utc)
    registry = BenchmarkRegistry()
    registry.register(
        _definition("benchmark.base"),
        lambda context: BenchmarkObservation(),
        result_ttl_seconds=60,
    )
    registry.register(
        _definition("benchmark.gpu", prerequisites=("gpu.available",)),
        lambda context: BenchmarkObservation(),
        result_ttl_seconds=60,
    )

    def probe(context) -> InspectionFinding:
        assert context.registered_benchmark_ids == (
            "benchmark.base",
            "benchmark.gpu",
        )
        return InspectionFinding(
            resource_observations={
                "gpu": {"status": "available", "endpoint": "http://10.0.0.3"},
                "provider_note": "http://127.0.0.1:11434",
            },
            detected_services=("ollama-local", "http://127.0.0.1:11434"),
            registered_handlers=("handler.safe",),
            detected_data_sources=("source.logical-1", "C:\\private\\source"),
            available_prerequisites=("gpu.available",),
            recommended_benchmark_ids=("benchmark.gpu", "benchmark.unknown"),
            warnings=("Bearer msh_join_private",),
        )

    def failed_probe(context) -> InspectionFinding:
        raise RuntimeError("failed at /private/config.json")

    inspector = DeviceInspector(
        registry,
        probes=(probe, failed_probe),
        inspection_ttl_seconds=120,
        now=lambda: now,
        system_observer=lambda: {
            "cpu": {"logical_cores_band": "8-15"},
            "memory": {"capacity_band": "16-31-gib"},
        },
    )

    snapshot = inspector.inspect(device_id="device-1", revision=7)
    serialized = str(snapshot.to_dict()).casefold()

    assert snapshot.revision == 7
    assert snapshot.created_at == now
    assert snapshot.expires_at == now + timedelta(seconds=120)
    assert snapshot.detected_services == ("ollama-local",)
    assert snapshot.registered_handlers == ("handler.safe",)
    assert snapshot.detected_data_sources == ("source.logical-1",)
    assert snapshot.recommended_benchmark_ids == (
        "benchmark.base",
        "benchmark.gpu",
    )
    assert snapshot.resource_observations["gpu"] == {"status": "available"}
    assert snapshot.resource_observations["provider_note"] == "[redacted]"
    assert "unregistered benchmark" in " ".join(snapshot.warnings)
    assert "RuntimeError: [redacted]" in snapshot.warnings
    assert "10.0.0.3" not in serialized
    assert "127.0.0.1" not in serialized
    assert "private\\source" not in serialized
    assert "msh_join_private" not in serialized

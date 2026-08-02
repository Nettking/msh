from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.benchmarking import (
    BenchmarkObservation,
    BenchmarkRegistry,
    BenchmarkRunRequest,
    BenchmarkRunner,
    BenchmarkValidityReason,
    SQLiteBenchmarkResultStore,
    evaluate_result,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
)


def _definition(version: str = "1.0.0") -> BenchmarkDefinition:
    return BenchmarkDefinition(
        benchmark_id="benchmark.persisted",
        capability_type="storage",
        capability_protocol="msh.storage.v1",
        implementation_version=version,
        max_duration_seconds=1,
        max_parallelism=1,
        prerequisites=(),
        metric_names=("latency_ms",),
        invalidation_inputs=("provider_version", "configuration_revision"),
        privacy_classification="public-summary",
    )


def test_store_survives_restart_and_validity_detects_expiry_and_invalidation(
    tmp_path,
) -> None:
    now = datetime(2026, 8, 2, 19, 0, tzinfo=timezone.utc)
    registry = BenchmarkRegistry()
    definition = _definition()
    registry.register(
        definition,
        lambda context: BenchmarkObservation(
            metrics={"latency_ms": 2.0},
            recommendation=BenchmarkRecommendation.USABLE,
        ),
        result_ttl_seconds=30,
    )
    path = tmp_path / "benchmarks.sqlite"
    request = BenchmarkRunRequest(
        run_id="run-persisted",
        device_id="device-1",
        benchmark_id=definition.benchmark_id,
        target_service_id="storage-1",
        dependency_inputs={
            "provider_version": "v1",
            "configuration_revision": 4,
        },
    )

    first_store = SQLiteBenchmarkResultStore(path)
    result = BenchmarkRunner(registry, first_store, now=lambda: now).run(request)
    first_store.close()

    second_store = SQLiteBenchmarkResultStore(path)
    assert second_store.load(result.run_id) == result
    assert second_store.list_results() == (result,)

    current = evaluate_result(
        result,
        definition,
        request.dependency_inputs,
        now=now + timedelta(seconds=29),
    )
    expired = evaluate_result(
        result,
        definition,
        request.dependency_inputs,
        now=now + timedelta(seconds=30),
    )
    invalidated = evaluate_result(
        result,
        definition,
        {"provider_version": "v2", "configuration_revision": 4},
        now=now + timedelta(seconds=1),
    )
    changed_definition = evaluate_result(
        result,
        _definition("2.0.0"),
        request.dependency_inputs,
        now=now + timedelta(seconds=1),
    )

    assert current.current is True
    assert current.reason == BenchmarkValidityReason.CURRENT
    assert expired.reason == BenchmarkValidityReason.EXPIRED
    assert invalidated.reason == BenchmarkValidityReason.DEPENDENCY_CHANGED
    assert changed_definition.reason == BenchmarkValidityReason.DEFINITION_CHANGED

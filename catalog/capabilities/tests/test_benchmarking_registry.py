from __future__ import annotations

import pytest

from catalog.capabilities.benchmarking import (
    BenchmarkObservation,
    BenchmarkRegistry,
    DuplicateBenchmarkError,
    MissingDependencyInputError,
    UnknownBenchmarkError,
    dependency_fingerprint,
)
from catalog.federation.onboarding_models import BenchmarkDefinition


def _definition(
    benchmark_id: str = "benchmark.echo",
    *,
    metric_names: tuple[str, ...] = ("latency_ms",),
    invalidation_inputs: tuple[str, ...] = ("software_version",),
) -> BenchmarkDefinition:
    return BenchmarkDefinition(
        benchmark_id=benchmark_id,
        capability_type="compute",
        capability_protocol="fcp.compute.v1",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=1,
        prerequisites=(),
        metric_names=metric_names,
        invalidation_inputs=invalidation_inputs,
        privacy_classification="public-summary",
    )


def test_registry_is_deterministic_and_rejects_duplicate_ids() -> None:
    registry = BenchmarkRegistry()
    registry.register(
        _definition("benchmark.z"),
        lambda context: BenchmarkObservation(),
        result_ttl_seconds=60,
    )
    registry.register(
        _definition("benchmark.a"),
        lambda context: BenchmarkObservation(),
        result_ttl_seconds=60,
    )

    assert registry.benchmark_ids() == ("benchmark.a", "benchmark.z")
    assert tuple(item.benchmark_id for item in registry.definitions()) == (
        "benchmark.a",
        "benchmark.z",
    )

    with pytest.raises(DuplicateBenchmarkError):
        registry.register(
            _definition("benchmark.a"),
            lambda context: BenchmarkObservation(),
            result_ttl_seconds=60,
        )

    with pytest.raises(UnknownBenchmarkError):
        registry.get("benchmark.missing")


def test_registry_rejects_metric_names_that_can_expose_locations() -> None:
    registry = BenchmarkRegistry()
    with pytest.raises(ValueError, match="unsafe metric name"):
        registry.register(
            _definition(metric_names=("endpoint",)),
            lambda context: BenchmarkObservation(),
            result_ttl_seconds=60,
        )


def test_dependency_fingerprint_is_stable_and_uses_only_declared_inputs() -> None:
    definition = _definition()
    first = dependency_fingerprint(
        definition,
        {"software_version": "1.2.3", "ignored": "value-a"},
    )
    second = dependency_fingerprint(
        definition,
        {"ignored": "value-b", "software_version": "1.2.3"},
    )
    changed = dependency_fingerprint(
        definition,
        {"software_version": "1.2.4"},
    )

    assert first == second
    assert first.startswith("sha256:")
    assert changed != first

    with pytest.raises(MissingDependencyInputError):
        dependency_fingerprint(definition, {})

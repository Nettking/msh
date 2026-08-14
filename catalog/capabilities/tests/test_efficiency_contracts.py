"""Execution-observation contracts: partial data, bounds and workload classes."""

from __future__ import annotations

from datetime import timedelta

import pytest

from catalog.capabilities.efficiency import (
    EXECUTION_OBSERVATION_SCHEMA,
    ExecutionEnvironment,
    ExecutionMeasurements,
    ExecutionObservation,
    WorkloadDescriptor,
    requirements_fingerprint,
)
from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

from catalog.capabilities.tests.efficiency_harness import NOW, build_observation


def test_observation_tolerates_a_measurement_only_report() -> None:
    observation = build_observation(
        node_id="node-a",
        capability_id="cap-a",
        observed_at=NOW,
        measurements=ExecutionMeasurements(succeeded=True),
    )
    assert observation.measurements.completion_millis is None
    assert observation.measurements.queue_millis is None
    assert observation.to_dict()["schema"] == EXECUTION_OBSERVATION_SCHEMA


def test_observation_round_trips_through_json() -> None:
    observation = build_observation(
        node_id="node-a",
        capability_id="cap-a",
        observed_at=NOW,
        measurements=ExecutionMeasurements(
            succeeded=True,
            queue_millis=120,
            execution_millis=880,
            total_millis=1_000,
            gpu_utilization_millis=640,
            vram_peak_bytes=8 * 1024**3,
            output_units=512,
            throughput_units_per_second=581.8,
            quality_score=0.94,
        ),
    )
    restored = ExecutionObservation.from_json(observation.to_json())
    assert restored == observation


def test_observation_rejects_an_unsupported_schema_major() -> None:
    payload = build_observation(
        node_id="node-a", capability_id="cap-a", observed_at=NOW
    ).to_dict()
    payload["schema"] = "fcp.execution-observation.v2"
    with pytest.raises(ProtocolCompatibilityError):
        ExecutionObservation.from_dict(payload)


@pytest.mark.parametrize(
    "measurements",
    [
        {"succeeded": True, "total_millis": -1},
        {"succeeded": True, "quality_score": 1.5},
        {"succeeded": True, "gpu_utilization_millis": 5_000},
        {"succeeded": True, "total_millis": 10, "execution_millis": 900},
        {"succeeded": True, "error_code": "worker-handler-failed"},
        {"succeeded": False},
    ],
)
def test_measurements_reject_implausible_values(measurements: dict) -> None:
    if measurements == {"succeeded": False}:
        # A failure without a reason is allowed; it is the only shape here
        # that must succeed, which keeps the negative cases honest.
        assert ExecutionMeasurements(**measurements).error_code is None
        return
    with pytest.raises(FederationValidationError):
        ExecutionMeasurements(**measurements)


def test_workload_features_reject_secret_material() -> None:
    with pytest.raises(FederationValidationError):
        WorkloadDescriptor(
            capability_type="language-model",
            job_kind="summarize",
            features={"api_token": "hunter2hunter2"},
        )


def test_environment_configuration_rejects_backend_locations() -> None:
    with pytest.raises(FederationValidationError):
        ExecutionEnvironment(configuration={"endpoint": "http://10.0.0.4:11434"})


def test_workload_class_tiers_coarsen_and_deduplicate() -> None:
    workload = WorkloadDescriptor(
        capability_type="language-model",
        job_kind="summarize",
        features={"model": "qwen3:4b"},
    )
    tiers = workload.class_keys()
    keys = [key for _tier, key in tiers]
    assert len(keys) == len(set(keys))
    # Categorical-only features make "exact" and "family" the same class.
    assert len(keys) == 3
    assert keys[-1] == "language-model"
    assert keys[-2] == "language-model/summarize"


def test_numeric_features_bucket_by_order_of_magnitude() -> None:
    def _key(tokens: int) -> str:
        return WorkloadDescriptor(
            capability_type="language-model",
            job_kind="summarize",
            features={"context_tokens": tokens},
        ).class_key("exact")

    assert _key(3_000) == _key(3_900)
    assert _key(3_000) != _key(30)
    # The family tier drops numeric features entirely, so a tiny and a huge
    # job still share a "similar workload" fallback.
    family = WorkloadDescriptor(
        capability_type="language-model",
        job_kind="summarize",
        features={"context_tokens": 4_000},
    ).class_key("family")
    assert "context_tokens" not in family


def test_environment_fingerprint_changes_with_model_and_version() -> None:
    base = ExecutionEnvironment(model_id="qwen3:4b", software_version="1.2.0")
    assert base.fingerprint == ExecutionEnvironment(
        model_id="qwen3:4b", software_version="1.2.0"
    ).fingerprint
    assert base.fingerprint != ExecutionEnvironment(
        model_id="qwen3:8b", software_version="1.2.0"
    ).fingerprint
    assert base.fingerprint != ExecutionEnvironment(
        model_id="qwen3:4b", software_version="1.3.0"
    ).fingerprint


def test_requirements_fingerprint_is_order_independent() -> None:
    first = requirements_fingerprint({"model": "a", "modalities": ["text"]})
    second = requirements_fingerprint({"modalities": ["text"], "model": "a"})
    assert first == second
    assert first != requirements_fingerprint({"model": "b"})


def test_observation_rejects_a_naive_timestamp() -> None:
    with pytest.raises(FederationValidationError):
        build_observation(
            node_id="node-a",
            capability_id="cap-a",
            observed_at=(NOW + timedelta(hours=1)).replace(tzinfo=None),
        )

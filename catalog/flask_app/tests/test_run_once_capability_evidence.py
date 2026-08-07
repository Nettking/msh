from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.benchmarking import (
    BenchmarkValidityReason,
    evaluate_result,
)
from catalog.capabilities.benchmarking.fingerprints import dependency_fingerprint
from catalog.capabilities.benchmarking.policy import evaluate_run_once_result
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
    DeviceInspectionSnapshot,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_benchmark_service import (
    BenchmarkPlanItem,
    get_capability_benchmark_service,
)
from catalog.flask_app.services.capability_contribution_service import (
    get_capability_contribution_service,
)
from catalog.flask_app.services.capability_inspection_service import (
    get_capability_inspection_service,
)
from catalog.flask_app.services.run_once_capability_evidence import (
    RunOnceCapabilityBenchmarkService,
    RunOnceCapabilityContributionService,
    RunOnceCapabilityInspectionService,
)

NOW = datetime(2026, 8, 7, 13, 30, tzinfo=timezone.utc)


def _definition(*, version: str = "1.0.0") -> BenchmarkDefinition:
    return BenchmarkDefinition(
        benchmark_id="benchmark.fixture.run-once.v1",
        capability_type="language-model",
        capability_protocol="fixture-model",
        implementation_version=version,
        max_duration_seconds=2,
        max_parallelism=1,
        prerequisites=(),
        metric_names=("latency_ms",),
        invalidation_inputs=("model",),
        privacy_classification="public-summary",
    )


def _expired_result(
    definition: BenchmarkDefinition,
    *,
    dependency_inputs: dict[str, object] | None = None,
) -> BenchmarkResult:
    inputs = dependency_inputs or {"model": "qwen-fixture:1"}
    started = NOW - timedelta(hours=2)
    finished = started + timedelta(seconds=1)
    return BenchmarkResult(
        run_id="legacy-run",
        device_id="device-fixture",
        benchmark_id=definition.benchmark_id,
        benchmark_version=definition.implementation_version,
        target_service_id="fixture-model-service",
        state=BenchmarkState.PASSED,
        metrics={"latency_ms": 42},
        recommendation=BenchmarkRecommendation.USABLE,
        dependency_fingerprint=dependency_fingerprint(definition, inputs),
        diagnostics=(),
        started_at=started,
        finished_at=finished,
        expires_at=finished + timedelta(minutes=15),
    )


def _expired_snapshot() -> DeviceInspectionSnapshot:
    created = NOW - timedelta(hours=2)
    return DeviceInspectionSnapshot(
        device_id="device-fixture",
        revision=1,
        os_family="windows",
        architecture="amd64",
        resource_observations={"cpu": {"logical_cores_band": "8-15"}},
        detected_services=("fixture-model-service",),
        registered_handlers=(),
        detected_data_sources=(),
        recommended_benchmark_ids=("benchmark.fixture.run-once.v1",),
        warnings=(),
        created_at=created,
        expires_at=created + timedelta(minutes=15),
    )


def test_run_once_policy_accepts_legacy_expiry_but_strict_kernel_still_expires() -> None:
    definition = _definition()
    inputs = {"model": "qwen-fixture:1"}
    result = _expired_result(definition, dependency_inputs=inputs)

    strict = evaluate_result(result, definition, inputs, now=NOW)
    product = evaluate_run_once_result(result, definition, inputs)

    assert strict.current is False
    assert strict.reason is BenchmarkValidityReason.EXPIRED
    assert product.current is True
    assert product.reason is BenchmarkValidityReason.CURRENT


def test_run_once_policy_still_invalidates_dependency_and_definition_changes() -> None:
    definition = _definition()
    result = _expired_result(definition)

    dependency_changed = evaluate_run_once_result(
        result,
        definition,
        {"model": "qwen-fixture:2"},
    )
    definition_changed = evaluate_run_once_result(
        result,
        replace(definition, implementation_version="2.0.0"),
        {"model": "qwen-fixture:1"},
    )

    assert dependency_changed.current is False
    assert dependency_changed.reason is BenchmarkValidityReason.DEPENDENCY_CHANGED
    assert definition_changed.current is False
    assert definition_changed.reason is BenchmarkValidityReason.DEFINITION_CHANGED


def test_run_once_inspection_treats_legacy_expiry_as_persisted_evidence(
    tmp_path: Path,
) -> None:
    service = RunOnceCapabilityInspectionService(
        onboarding_service=object(),
        state_database=tmp_path / "state.sqlite3",
        clock=lambda: NOW,
    )

    assert service.state(_expired_snapshot()) == "current"


def test_run_once_benchmark_card_requires_rerun_only_when_inputs_change(
    tmp_path: Path,
) -> None:
    definition = _definition()
    result = _expired_result(definition)
    service = RunOnceCapabilityBenchmarkService(
        onboarding_service=object(),
        inspection_service=object(),
        state_database=tmp_path / "state.sqlite3",
        clock=lambda: NOW,
    )
    unchanged = BenchmarkPlanItem(
        benchmark_id=definition.benchmark_id,
        target_service_id=result.target_service_id,
        definition=definition,
        dependency_inputs={"model": "qwen-fixture:1"},
        available_prerequisites=frozenset(),
        runnable=True,
    )
    changed = replace(
        unchanged,
        dependency_inputs={"model": "qwen-fixture:2"},
    )

    reused_card = service._card_model(
        item=unchanged,
        result=result,
        skipped=False,
        active=False,
        inspection_current=True,
    )
    stale_card = service._card_model(
        item=changed,
        result=result,
        skipped=False,
        active=False,
        inspection_current=True,
    )

    assert reused_card["state"] == "passed"
    assert reused_card["state_label"] == "Usable"
    assert reused_card["action_label"] == "Run again"
    assert str(reused_card["expires_label"]).startswith("Saved evidence · collected")
    assert stale_card["state"] == "stale"
    assert stale_card["state_label"] == "Changed"
    assert stale_card["action_label"] == "Rerun check"


def test_app_installs_run_once_services_lazily_from_final_config(tmp_path: Path) -> None:
    app = create_app()
    state_database = tmp_path / "configured" / "onboarding.sqlite3"
    coordinator_database = tmp_path / "configured" / "control.sqlite3"
    identity_directory = tmp_path / "configured" / "device"
    app.config.update(
        CAPABILITY_ONBOARDING_STATE_DATABASE=state_database,
        CAPABILITY_ONBOARDING_BENCHMARK_DATABASE=state_database,
        CAPABILITY_ONBOARDING_CONTRIBUTION_DATABASE=state_database,
        CAPABILITY_ONBOARDING_COORDINATOR_DATABASE=coordinator_database,
        CAPABILITY_ONBOARDING_IDENTITY_DIRECTORY=identity_directory,
    )

    assert "CAPABILITY_INSPECTION_SERVICE" not in app.config
    assert "CAPABILITY_BENCHMARK_SERVICE" not in app.config
    assert "CAPABILITY_CONTRIBUTION_SERVICE" not in app.config

    with app.app_context():
        inspection = get_capability_inspection_service()
        benchmarks = get_capability_benchmark_service()
        contributions = get_capability_contribution_service()

        assert isinstance(inspection, RunOnceCapabilityInspectionService)
        assert isinstance(benchmarks, RunOnceCapabilityBenchmarkService)
        assert isinstance(contributions, RunOnceCapabilityContributionService)
        assert inspection.store.database == state_database
        assert benchmarks.state_database == state_database
        assert contributions.state_database == state_database

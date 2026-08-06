from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask

from catalog.capabilities.benchmarking import (
    BenchmarkRegistry,
    BenchmarkRunRequest,
    BenchmarkRunner,
    DeviceInspector,
    SQLiteBenchmarkResultStore,
    register_concrete_adapters,
)
from catalog.capabilities.contributions import ContributionCandidateGenerator
from catalog.federation.onboarding_models import (
    BenchmarkState,
    ContributionActivationState,
)
from catalog.flask_app.services.local_capability_candidates import (
    local_contribution_components,
    local_inspection_adapters,
)

NOW = datetime(2026, 8, 6, 9, 0, tzinfo=timezone.utc)


def test_default_local_compute_and_storage_are_benchmarkable_and_selectable(
    tmp_path,
) -> None:
    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = (
        tmp_path / "storage-probe"
    )

    with app.app_context():
        adapters = local_inspection_adapters()
        registry = BenchmarkRegistry()
        registration = register_concrete_adapters(registry, adapters)
        inspector = DeviceInspector(
            registry,
            probes=registration.inspection_probes,
            now=lambda: NOW,
            system_observer=lambda: {},
        )
        snapshot = inspector.inspect(device_id="device-local", revision=1)

        assert snapshot.registered_handlers == ("msh-system-summary",)
        assert "msh-local-data-storage" in snapshot.detected_services
        assert "benchmark.compute.registered-handler.v1" in (
            snapshot.recommended_benchmark_ids
        )
        assert "benchmark.storage.candidate-roundtrip.v1" in (
            snapshot.recommended_benchmark_ids
        )

        results = []
        store = SQLiteBenchmarkResultStore(tmp_path / "benchmark.sqlite3")
        runner = BenchmarkRunner(registry, store, now=lambda: NOW)
        for adapter in adapters:
            target = (
                "msh-system-summary"
                if adapter.definition.capability_type == "compute"
                else "msh-local-data-storage"
            )
            result = runner.run(
                BenchmarkRunRequest(
                    run_id=f"run-{adapter.definition.capability_type}",
                    device_id=snapshot.device_id,
                    benchmark_id=adapter.definition.benchmark_id,
                    target_service_id=target,
                    dependency_inputs=adapter.dependency_inputs(target),
                    available_prerequisites=frozenset(
                        adapter.definition.prerequisites
                    ),
                )
            )
            assert result.state is BenchmarkState.PASSED
            results.append(result)

        sources, contribution_adapters = local_contribution_components()
        recommendations = ContributionCandidateGenerator(
            sources=sources,
            benchmark_definitions=registry.definitions(),
            now=lambda: NOW,
        ).generate(snapshot, results)
        candidates = {item.candidate.capability_type: item.candidate for item in recommendations}

        assert set(candidates) == {"compute", "storage"}
        compute_outcome = contribution_adapters[0].enable(candidates["compute"])
        storage_outcome = contribution_adapters[1].enable(candidates["storage"])

        assert contribution_adapters[0].candidate_only is True
        assert compute_outcome.activation_state is ContributionActivationState.PENDING
        assert contribution_adapters[1].candidate_only is True
        assert storage_outcome.activation_state is ContributionActivationState.PENDING
        assert not list((tmp_path / "storage-probe").glob("*.probe"))

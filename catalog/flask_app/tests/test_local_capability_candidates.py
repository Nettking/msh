from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

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
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_models import (
    BenchmarkState,
    ContributionActivationState,
)
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.flask_app.services.local_capability_candidates import (
    local_contribution_components,
    local_inspection_adapters,
)
from catalog.node.identity import IdentityStore

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
        candidates = {
            item.candidate.capability_type: item.candidate
            for item in recommendations
        }

        assert set(candidates) == {"compute", "storage"}
        compute_outcome = contribution_adapters[0].enable(candidates["compute"])
        storage_outcome = contribution_adapters[1].enable(candidates["storage"])

        assert contribution_adapters[0].candidate_only is True
        assert compute_outcome.activation_state is ContributionActivationState.PENDING
        assert contribution_adapters[1].candidate_only is True
        assert storage_outcome.activation_state is ContributionActivationState.PENDING
        assert not list((tmp_path / "storage-probe").glob("*.probe"))


def _enrolled_credentials(
    tmp_path,
    coordinator: SessionCoordinator,
    name: str,
):
    credentials = IdentityStore(
        tmp_path / f"identity-{name}",
        display_name=name,
    ).load_or_create(now=NOW)
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(credentials.identity, token=token)
    return credentials


def _storage_candidate():
    return SimpleNamespace(
        capability_type="storage",
        capacity_envelope={
            "authority": "candidate-only",
            "provider_id": "msh-local-data-storage",
        },
    )


def test_federation_creator_bootstraps_initial_local_storage_assignment(
    tmp_path,
) -> None:
    coordinator = SessionCoordinator(
        tmp_path / "control.sqlite3",
        clock=lambda: NOW,
    )
    credentials = _enrolled_credentials(
        tmp_path,
        coordinator,
        "Creator",
    )
    session = coordinator.create_session(
        actor_node_id=credentials.identity.node_id,
        display_name="Creator federation",
        request_id="create-federation",
    )
    context = SimpleNamespace(
        credentials=credentials,
        binding=SimpleNamespace(internal_session_id=session.session_id),
        coordinator=coordinator,
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = (
        tmp_path / "storage-probe"
    )
    with app.app_context():
        _sources, adapters = local_contribution_components(onboarding)
        storage_adapter = adapters[1]
        first = storage_adapter.enable(_storage_candidate())
        control = PhaseDControlPlane(coordinator.store.database)
        snapshot = control.snapshot(session.session_id)
        revision = snapshot.revision
        second = storage_adapter.enable(_storage_candidate())
        repeated = control.snapshot(session.session_id)

    assert first.activation_state is ContributionActivationState.ACTIVE
    assert first.authority_confirmed is True
    assert second.activation_state is ContributionActivationState.ACTIVE
    assert repeated.revision == revision
    provider = snapshot.providers["msh-local-data-storage"]
    assert provider.node_id == credentials.identity.node_id
    assert provider.protocol == STORAGE_PROTOCOL
    assert provider.protocol_version == STORAGE_PROTOCOL_VERSION
    assert provider.authorized is True
    assert provider.status == "ready"
    assert provider.assignable is True
    assert (
        snapshot.groups["msh-local-storage"].primary_provider_id
        == "msh-local-data-storage"
    )


def test_joined_member_cannot_self_approve_initial_storage(
    tmp_path,
) -> None:
    coordinator = SessionCoordinator(
        tmp_path / "control.sqlite3",
        clock=lambda: NOW,
    )
    creator = _enrolled_credentials(tmp_path, coordinator, "Creator")
    member = _enrolled_credentials(tmp_path, coordinator, "Member")
    session = coordinator.create_session(
        actor_node_id=creator.identity.node_id,
        display_name="Creator federation",
        request_id="create-federation",
    )
    invitation = coordinator.create_invitation(
        session_id=session.session_id,
        actor_node_id=creator.identity.node_id,
        request_id="invite-member",
    )
    coordinator.join_session(
        node_id=member.identity.node_id,
        token=invitation["token"],
        request_id="join-member",
        expected_session_id=session.session_id,
    )
    context = SimpleNamespace(
        credentials=member,
        binding=SimpleNamespace(internal_session_id=session.session_id),
        coordinator=coordinator,
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)

    app = Flask(__name__)
    app.config["CAPABILITY_ONBOARDING_STORAGE_PROBE_DIRECTORY"] = (
        tmp_path / "storage-probe"
    )
    with app.app_context():
        _sources, adapters = local_contribution_components(onboarding)
        outcome = adapters[1].enable(_storage_candidate())
        snapshot = PhaseDControlPlane(
            coordinator.store.database
        ).snapshot(session.session_id)

    assert outcome.activation_state is ContributionActivationState.PENDING
    assert outcome.authority_confirmed is False
    assert "federation creator" in str(outcome.reason).lower()
    assert snapshot.revision == 0
    assert snapshot.providers == {}
    assert snapshot.groups == {}

"""Integrated CF7-B product acceptance over the supported Flask application.

The tests compose the real CFI-2 through CFI-6 services and routes. The bounded
adapter fixtures stand in for physical MTConnect, Ollama, compute, and storage
services; those physical claims remain separate in ``physical_evidence.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.contributions import ContributionPolicyEvaluator
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
)
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import (
    _COMMAND_SESSION_KEY,
    _CSRF_SESSION_KEY,
)
from catalog.flask_app.services.capability_benchmark_service import (
    CapabilityBenchmarkService,
)
from catalog.flask_app.services.capability_contribution_service import (
    CapabilityContributionService,
)
from catalog.flask_app.services.capability_inspection_service import (
    CapabilityInspectionService,
)
from catalog.flask_app.services.capability_onboarding_service import (
    CapabilityOnboardingService,
)
from catalog.flask_app.services.capability_startup_transition_service import (
    CapabilityStartupTransitionService,
)
from catalog.flask_app.services.server_setup_service import default_settings
from catalog.flask_app.tests.test_capability_contribution_route import (
    AI_ID,
    BENCHMARK_ID,
    STORAGE_ID,
    AuthorityHarness,
    InspectionBenchmarkAdapter,
)

NOW = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
PRIVATE_RECORDER_SOURCE = "Fixture=http://10.20.30.40:5000"


@dataclass
class ProductStack:
    root: Path
    current: list[datetime]
    harness: AuthorityHarness
    coordinator: SessionCoordinator
    onboarding: CapabilityOnboardingService
    inspection: CapabilityInspectionService
    benchmarks: CapabilityBenchmarkService
    contributions: CapabilityContributionService
    transition: CapabilityStartupTransitionService
    settings: list[object]
    adapter: InspectionBenchmarkAdapter
    app: object


def _build_stack(
    root: Path,
    *,
    current: list[datetime],
    harness: AuthorityHarness,
    coordinator: SessionCoordinator | None = None,
    discovery_sources=(),
    settings: list[object] | None = None,
) -> ProductStack:
    root.mkdir(parents=True, exist_ok=True)
    clock = lambda: current[0]
    coordinator = coordinator or SessionCoordinator(
        root / "coordinator.sqlite3",
        clock=clock,
    )
    onboarding = CapabilityOnboardingService(
        identity_directory=root / "device",
        state_database=root / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name=f"Acceptance {root.name}",
        discovery_sources=discovery_sources,
        setup_loader=lambda: {"configured": False, "user_setup_complete": False},
        clock=clock,
    )
    adapter = InspectionBenchmarkAdapter()
    inspection = CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=root / "onboarding.sqlite3",
        adapters=(adapter,),
        setup_loader=lambda: {"configured": False, "user_setup_complete": False},
        inspection_ttl_seconds=900,
        clock=clock,
        system_observer=lambda: {
            "cpu": {"logical_cores_band": "8-15"},
            "memory": {"capacity_band": "16-31-gib"},
        },
    )
    benchmarks = CapabilityBenchmarkService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        state_database=root / "onboarding.sqlite3",
        clock=clock,
    )
    contributions = CapabilityContributionService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        benchmark_service=benchmarks,
        state_database=root / "onboarding.sqlite3",
        sources=harness.sources(),
        adapters=harness.adapters(),
        clock=clock,
    )
    settings = settings or [
        replace(
            default_settings(configured=False),
            recorder_sources=PRIVATE_RECORDER_SOURCE,
        )
    ]

    def save_settings(value) -> None:
        settings[0] = value

    transition = CapabilityStartupTransitionService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        contribution_service=contributions,
        state_database=root / "onboarding.sqlite3",
        setup_loader=lambda: settings[0],
        setup_saver=save_settings,
        clock=clock,
    )
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=onboarding,
        CAPABILITY_INSPECTION_SERVICE=inspection,
        CAPABILITY_BENCHMARK_SERVICE=benchmarks,
        CAPABILITY_CONTRIBUTION_SERVICE=contributions,
        CAPABILITY_STARTUP_TRANSITION_SERVICE=transition,
        CAPABILITY_ONBOARDING_CONTRIBUTION_POLICY=ContributionPolicyEvaluator(),
    )
    return ProductStack(
        root=root,
        current=current,
        harness=harness,
        coordinator=coordinator,
        onboarding=onboarding,
        inspection=inspection,
        benchmarks=benchmarks,
        contributions=contributions,
        transition=transition,
        settings=settings,
        adapter=adapter,
        app=app,
    )


def _browser_context(client) -> tuple[str, str]:
    response = client.get("/onboarding")
    assert response.status_code == 200
    with client.session_transaction() as browser:
        return str(browser[_CSRF_SESSION_KEY]), str(browser[_COMMAND_SESSION_KEY])


def _create_identity(client) -> tuple[str, str]:
    csrf, command_id = _browser_context(client)
    response = client.post(
        "/onboarding/identity",
        data={"_csrf_token": csrf},
    )
    assert response.status_code == 303
    return csrf, command_id


def _connect_local(client) -> tuple[str, str]:
    csrf, command_id = _create_identity(client)
    response = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    assert response.status_code == 303
    return csrf, command_id


def _join_candidate(
    client,
    candidate: FederationDiscoveryCandidate,
) -> tuple[str, str]:
    csrf, command_id = _create_identity(client)
    response = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "join",
            "discovery_id": candidate.discovery_id,
            "verification_code": candidate.verification_code,
        },
    )
    assert response.status_code == 303
    return csrf, command_id


def _inspect_and_benchmark(client, csrf: str) -> None:
    assert client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    ).status_code == 303
    assert client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": AI_ID,
        },
    ).status_code == 303


def _candidate_ids(stack: ProductStack) -> dict[str, str]:
    with stack.app.app_context():
        return {
            candidate.capability_type: candidate.candidate_id
            for candidate in stack.contributions.recommend()
        }


def _save_choices(
    client,
    *,
    csrf: str,
    command_id: str,
    candidate_ids: dict[str, str],
    recorder: str = "disabled",
    language_model: str = "disabled",
    compute: str = "disabled",
    storage: str = "disabled",
) -> None:
    choices = {
        candidate_ids["recorder"]: recorder,
        candidate_ids["language-model"]: language_model,
        candidate_ids["compute"]: compute,
        candidate_ids["storage"]: storage,
    }
    response = client.post(
        "/onboarding/contributions",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            **{
                f"contribution.{candidate_id}": desired
                for candidate_id, desired in choices.items()
            },
        },
    )
    assert response.status_code == 303


def _candidate_for_existing_federation(
    stack: ProductStack,
    *,
    actor_node_id: str,
    session_id: str,
    suffix: str,
) -> FederationDiscoveryCandidate:
    enrollment_token = stack.coordinator.create_enrollment_token(
        ttl_seconds=300,
        max_uses=1,
    )["token"]
    invitation_token = stack.coordinator.create_invitation(
        session_id=session_id,
        actor_node_id=actor_node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id=f"cf7b-invite-{suffix}",
    )["token"]
    return FederationDiscoveryCandidate(
        federation_label="CF7-B product federation",
        internal_session_id=session_id,
        federation_fingerprint="cf7b-product-fingerprint",
        transport_kind="configured-acceptance",
        verification_code=f"CF7B-{suffix.upper()}",
        discovered_at=stack.current[0],
        expires_at=stack.current[0] + timedelta(minutes=5),
        enrollment_token=enrollment_token,
        invitation_token=invitation_token,
    )


def test_fresh_product_flow_finishes_restarts_and_reruns_expired_evidence(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    current = [NOW]
    harness = AuthorityHarness()
    stack = _build_stack(
        tmp_path / "primary-device",
        current=current,
        harness=harness,
    )
    client = stack.app.test_client()
    csrf, command_id = _connect_local(client)
    _inspect_and_benchmark(client, csrf)
    ids = _candidate_ids(stack)
    binding_before = stack.onboarding.authorized_context().binding

    _save_choices(
        client,
        csrf=csrf,
        command_id=command_id,
        candidate_ids=ids,
        recorder="enabled",
        language_model="enabled",
        compute="disabled",
        storage="ask-later",
    )
    assert harness.recorder.enabled
    assert harness.ai_active == {AI_ID}

    _save_choices(
        client,
        csrf=csrf,
        command_id=command_id,
        candidate_ids=ids,
        recorder="enabled",
        language_model="disabled",
        compute="disabled",
        storage="ask-later",
    )
    assert harness.recorder.enabled
    assert not harness.ai_active

    _save_choices(
        client,
        csrf=csrf,
        command_id=command_id,
        candidate_ids=ids,
        recorder="enabled",
        language_model="enabled",
        compute="disabled",
        storage="ask-later",
    )
    assert stack.onboarding.authorized_context().binding == binding_before

    finished = client.post(
        "/onboarding/finish",
        data={"_csrf_token": csrf, "command_id": command_id},
    )
    assert finished.status_code == 303
    assert finished.location.endswith("/federation")
    state = stack.transition.load()
    assert state is not None and state.completed
    assert state.source_kind == "capability-first"
    assert state.contribution_intents["recorder"] == "enabled"
    assert state.contribution_intents["language-model"] == "enabled"
    assert state.contribution_intents["compute"] == "disabled"
    assert state.contribution_intents["storage"] == "ask-later"
    assert stack.settings[0].deployment_mode == "full-server"
    assert stack.settings[0].ai_enabled is True

    federation = client.get("/federation")
    assert federation.status_code == 200
    federation_body = federation.get_data(as_text=True)
    assert PRIVATE_RECORDER_SOURCE not in federation_body
    assert "10.20.30.40" not in federation_body

    original_identity = stack.onboarding.identity_or_none().identity
    stack.contributions.close()
    harness.reset_runtime()
    restarted = _build_stack(
        stack.root,
        current=current,
        harness=harness,
        coordinator=SessionCoordinator(
            stack.root / "coordinator.sqlite3",
            clock=lambda: current[0],
        ),
        settings=stack.settings,
    )
    restarted_client = restarted.app.test_client()
    assert restarted_client.get("/onboarding").status_code == 200
    assert harness.recorder.enabled
    assert harness.ai_active == {AI_ID}
    assert restarted.onboarding.identity_or_none().identity == original_identity
    assert restarted.onboarding.authorized_context() is not None
    returning = restarted_client.get("/")
    assert returning.status_code == 302
    assert returning.location.endswith("/federation")

    restarted_ids = _candidate_ids(restarted)
    current[0] = NOW + timedelta(minutes=16)
    with restarted.app.app_context():
        reconciled = {
            intent.candidate_id: intent
            for intent in restarted.contributions.reconcile()
        }
    assert reconciled[restarted_ids["language-model"]].activation_state is (
        ContributionActivationState.SUSPENDED
    )
    assert not harness.ai_active

    refresh_csrf, refresh_command = _browser_context(restarted_client)
    _inspect_and_benchmark(restarted_client, refresh_csrf)
    refreshed_ids = _candidate_ids(restarted)
    _save_choices(
        restarted_client,
        csrf=refresh_csrf,
        command_id=refresh_command,
        candidate_ids=refreshed_ids,
        recorder="enabled",
        language_model="enabled",
        compute="disabled",
        storage="ask-later",
    )
    assert harness.ai_active == {AI_ID}
    assert len(restarted.benchmarks.list_results()) >= 2


def test_three_device_federation_keeps_ai_compute_and_storage_authority_separate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    current = [NOW]
    owner_harness = AuthorityHarness()
    owner = _build_stack(
        tmp_path / "owner-device",
        current=current,
        harness=owner_harness,
    )
    owner_client = owner.app.test_client()
    owner_csrf, owner_command = _connect_local(owner_client)
    owner_context = owner.onboarding.authorized_context()
    assert owner_context is not None
    session_id = owner_context.binding.internal_session_id
    actor_node_id = owner_context.credentials.identity.node_id

    compute_candidate = _candidate_for_existing_federation(
        owner,
        actor_node_id=actor_node_id,
        session_id=session_id,
        suffix="compute",
    )
    storage_candidate = _candidate_for_existing_federation(
        owner,
        actor_node_id=actor_node_id,
        session_id=session_id,
        suffix="storage",
    )
    compute_harness = AuthorityHarness()
    storage_harness = AuthorityHarness()
    compute = _build_stack(
        tmp_path / "compute-device",
        current=current,
        harness=compute_harness,
        coordinator=owner.coordinator,
        discovery_sources=(
            ConfiguredFederationDiscoveryAdapter((compute_candidate,)),
        ),
    )
    storage = _build_stack(
        tmp_path / "storage-device",
        current=current,
        harness=storage_harness,
        coordinator=owner.coordinator,
        discovery_sources=(
            ConfiguredFederationDiscoveryAdapter((storage_candidate,)),
        ),
    )
    compute_client = compute.app.test_client()
    storage_client = storage.app.test_client()
    compute_csrf, compute_command = _join_candidate(
        compute_client,
        compute_candidate,
    )
    storage_csrf, storage_command = _join_candidate(
        storage_client,
        storage_candidate,
    )
    for stack in (owner, compute, storage):
        context = stack.onboarding.authorized_context()
        assert context is not None
        assert context.binding.internal_session_id == session_id

    revision_after_join = owner.coordinator.store.get_session(session_id).revision

    _inspect_and_benchmark(owner_client, owner_csrf)
    owner_ids = _candidate_ids(owner)
    _save_choices(
        owner_client,
        csrf=owner_csrf,
        command_id=owner_command,
        candidate_ids=owner_ids,
        language_model="enabled",
    )

    _inspect_and_benchmark(compute_client, compute_csrf)
    compute_ids = _candidate_ids(compute)
    _save_choices(
        compute_client,
        csrf=compute_csrf,
        command_id=compute_command,
        candidate_ids=compute_ids,
        compute="enabled",
    )

    _inspect_and_benchmark(storage_client, storage_csrf)
    storage_ids = _candidate_ids(storage)
    _save_choices(
        storage_client,
        csrf=storage_csrf,
        command_id=storage_command,
        candidate_ids=storage_ids,
        storage="enabled",
    )
    with storage.app.app_context():
        pending = {
            intent.candidate_id: intent
            for intent in storage.contributions.intents()
        }[storage_ids["storage"]]
    assert pending.activation_state is ContributionActivationState.PENDING
    assert not storage_harness.storage_assigned

    storage_harness.storage_assigned.add(STORAGE_ID)
    assert storage_client.post(
        "/onboarding/contributions/reconcile",
        data={
            "_csrf_token": storage_csrf,
            "command_id": storage_command,
        },
    ).status_code == 303
    with storage.app.app_context():
        active_storage = {
            intent.candidate_id: intent
            for intent in storage.contributions.intents()
        }[storage_ids["storage"]]
    assert active_storage.activation_state is ContributionActivationState.ACTIVE

    assert owner_harness.ai_active == {AI_ID}
    assert not owner_harness.compute_active
    assert not owner_harness.recorder.enabled
    assert compute_harness.compute_active
    assert not compute_harness.ai_active
    assert not compute_harness.recorder.enabled
    assert storage_harness.storage_assigned == {STORAGE_ID}
    assert not storage_harness.ai_active
    assert not storage_harness.compute_active
    assert owner.coordinator.store.get_session(session_id).revision == revision_after_join

    for stack in (owner, compute, storage):
        identity = stack.onboarding.identity_or_none().identity
        owner.coordinator.store.require_membership(
            session_id=session_id,
            node_id=identity.node_id,
        )

    joined_page = compute_client.get("/onboarding").get_data(as_text=True)
    assert compute_candidate.enrollment_token not in joined_page
    assert compute_candidate.invitation_token not in joined_page
    assert ContributionDesiredState.ENABLED.value == "enabled"

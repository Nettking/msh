from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.benchmarking import (
    BenchmarkObservation,
    InspectionFinding,
)
from catalog.capabilities.contributions import (
    AICandidateSource,
    AICandidateSpec,
    AIContributionAdapter,
    ComputeCandidateSource,
    ComputeContributionAdapter,
    ContributionPolicyEvaluator,
    RecorderCandidateSource,
    RecorderContributionAdapter,
    StorageCandidateSource,
    StorageCandidateSpec,
    StorageContributionAdapter,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
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

NOW = datetime(2026, 8, 3, 9, 30, tzinfo=timezone.utc)
BENCHMARK_ID = "benchmark.fixture.contribution.v1"
AI_ID = "ai-local"
STORAGE_ID = "storage-local"
HANDLER_ID = "safe-handler"


class InspectionBenchmarkAdapter:
    definition = BenchmarkDefinition(
        benchmark_id=BENCHMARK_ID,
        capability_type="language-model",
        capability_protocol="fcp-language-model",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=1,
        prerequisites=("fixture-ready",),
        metric_names=("available",),
        invalidation_inputs=("fixture_revision",),
        privacy_classification="public-summary",
    )
    result_ttl_seconds = 900

    def __init__(self) -> None:
        self.benchmark_calls = 0

    def __call__(self, _context) -> InspectionFinding:
        return InspectionFinding(
            detected_services=(AI_ID, STORAGE_ID),
            registered_handlers=(HANDLER_ID,),
            detected_data_sources=("machine-one",),
            available_prerequisites=("fixture-ready",),
            recommended_benchmark_ids=(BENCHMARK_ID,),
        )

    def dependency_inputs(self, service_id: str) -> dict[str, int]:
        if service_id != AI_ID:
            raise KeyError(service_id)
        return {"fixture_revision": 1}

    def benchmark(self, context) -> BenchmarkObservation:
        self.benchmark_calls += 1
        context.raise_if_cancelled()
        return BenchmarkObservation(
            metrics={"available": True},
            recommendation=BenchmarkRecommendation.RECOMMENDED,
        )


@dataclass(frozen=True)
class Descriptor:
    handler_id: str = HANDLER_ID
    capability_type: str = "synthetic-compute"
    protocol: str = "fcp-synthetic"
    protocol_version: str = "1.0"
    descriptor_fingerprint: str = "sha256:" + "c" * 64


@dataclass(frozen=True)
class Binding:
    descriptor: Descriptor = Descriptor()


class Inventory:
    def __init__(self) -> None:
        self.binding = Binding()

    def list_bindings(self):
        return (self.binding,)

    def get(self, handler_id):
        return self.binding if handler_id == HANDLER_ID else None


class RecorderAuthority:
    def __init__(self) -> None:
        self.enabled = False
        self.calls: list[bool] = []

    def set_enabled(self, enabled, _settings):
        self.enabled = bool(enabled)
        self.calls.append(bool(enabled))
        return True, "recorder state changed"

    def status(self, _settings):
        return {"requested_enabled": self.enabled}


class AuthorityHarness:
    def __init__(self) -> None:
        self.recorder = RecorderAuthority()
        self.ai_active: set[str] = set()
        self.compute_active: set[tuple[str, str]] = set()
        self.compute_fences: list[tuple[str, str]] = []
        self.storage_assigned: set[str] = set()
        self.storage_fences: list[str] = []
        self.inventory = Inventory()
        self.fail_ai = False

    def sources(self):
        return (
            RecorderCandidateSource(),
            AICandidateSource(
                {
                    AI_ID: AICandidateSpec(
                        service_id=AI_ID,
                        protocol="fcp-language-model",
                        display_label="Local AI",
                        capacity_envelope={"model": "fixture-model"},
                    )
                }
            ),
            ComputeCandidateSource(self.inventory),
            StorageCandidateSource(
                {
                    STORAGE_ID: StorageCandidateSpec(
                        provider_id=STORAGE_ID,
                        protocol="fcp-storage",
                        display_label="Local storage",
                        capacity_envelope={"capacity_band": "small"},
                    )
                }
            ),
        )

    def adapters(self):
        return (
            RecorderContributionAdapter(
                self.recorder,
                settings_provider=lambda: object(),
            ),
            AIContributionAdapter(
                enable_provider=self._enable_ai,
                disable_provider=self.ai_active.discard,
                is_provider_active=lambda provider_id: provider_id in self.ai_active,
            ),
            ComputeContributionAdapter(
                self.inventory,
                activate_binding=self._activate_compute,
                fence_handler=self._fence_compute,
                is_handler_active=lambda handler_id, fingerprint: (
                    (handler_id, fingerprint) in self.compute_active
                ),
            ),
            StorageContributionAdapter(
                is_assigned=lambda provider_id: provider_id in self.storage_assigned,
                fence_candidate=self.storage_fences.append,
            ),
        )

    def reset_runtime(self) -> None:
        self.recorder.enabled = False
        self.recorder.calls.clear()
        self.ai_active.clear()
        self.compute_active.clear()
        self.compute_fences.clear()
        self.storage_fences.clear()

    def _enable_ai(self, candidate) -> None:
        if self.fail_ai:
            raise RuntimeError(
                "http://10.0.0.9/private Bearer contribution-secret"
            )
        self.ai_active.add(candidate.capacity_envelope["provider_id"])

    def _activate_compute(self, binding) -> None:
        self.compute_active.add(
            (
                binding.descriptor.handler_id,
                binding.descriptor.descriptor_fingerprint,
            )
        )

    def _fence_compute(self, handler_id: str, fingerprint: str) -> None:
        identity = (handler_id, fingerprint)
        self.compute_fences.append(identity)
        self.compute_active.discard(identity)


def _services(tmp_path: Path, harness: AuthorityHarness, current: list[datetime]):
    clock = lambda: current[0]
    coordinator = SessionCoordinator(tmp_path / "coordinator.sqlite3", clock=clock)
    onboarding = CapabilityOnboardingService(
        identity_directory=tmp_path / "device",
        state_database=tmp_path / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name="Contribution laptop",
        discovery_sources=(),
        setup_loader=lambda: {"configured": False, "user_setup_complete": False},
        clock=clock,
    )
    inspection_adapter = InspectionBenchmarkAdapter()
    inspection = CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=tmp_path / "onboarding.sqlite3",
        adapters=(inspection_adapter,),
        setup_loader=lambda: {"configured": False, "user_setup_complete": False},
        inspection_ttl_seconds=900,
        clock=clock,
        system_observer=lambda: {"cpu": {"logical_cores_band": "8-15"}},
    )
    benchmarks = CapabilityBenchmarkService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        state_database=tmp_path / "onboarding.sqlite3",
        clock=clock,
    )
    contributions = CapabilityContributionService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        benchmark_service=benchmarks,
        state_database=tmp_path / "onboarding.sqlite3",
        sources=harness.sources(),
        adapters=harness.adapters(),
        clock=clock,
    )
    return onboarding, inspection, benchmarks, contributions, inspection_adapter


def _app(services):
    onboarding, inspection, benchmarks, contributions, _adapter = services
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=onboarding,
        CAPABILITY_INSPECTION_SERVICE=inspection,
        CAPABILITY_BENCHMARK_SERVICE=benchmarks,
        CAPABILITY_CONTRIBUTION_SERVICE=contributions,
        CAPABILITY_ONBOARDING_CONTRIBUTION_POLICY=ContributionPolicyEvaluator(),
    )
    return app


def _form_context(client) -> tuple[str, str]:
    response = client.get("/onboarding")
    assert response.status_code == 200
    with client.session_transaction() as browser:
        return str(browser[_CSRF_SESSION_KEY]), str(browser[_COMMAND_SESSION_KEY])


def _connect_inspect_and_benchmark(client) -> tuple[str, str]:
    csrf, command_id = _form_context(client)
    assert client.post(
        "/onboarding/identity", data={"_csrf_token": csrf}
    ).status_code == 303
    assert client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    ).status_code == 303
    assert client.post(
        "/onboarding/inspect", data={"_csrf_token": csrf}
    ).status_code == 303
    assert client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": AI_ID,
        },
    ).status_code == 303
    return csrf, command_id


def _candidate_ids(app, contributions):
    with app.app_context():
        return {
            candidate.capability_type: candidate.candidate_id
            for candidate in contributions.recommend()
        }


def _choice_form(csrf: str, command_id: str, choices: dict[str, str]):
    return {
        "_csrf_token": csrf,
        "command_id": command_id,
        **{f"contribution.{key}": value for key, value in choices.items()},
    }


def test_app_factory_registers_cfi5_before_cfi4_and_legacy(tmp_path: Path) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)

    blueprints = list(app.blueprints)
    assert blueprints.index("capability_contribution_web") < blueprints.index(
        "capability_benchmark_web"
    )
    assert "capability_contribution_web.onboarding" in app.view_functions
    assert "capability_contribution_web.save_contributions" in app.view_functions
    assert "capability_contribution_web.suspend_contribution" in app.view_functions
    assert "capability_contribution_web.reconcile_contributions" in app.view_functions


def test_contributions_remain_409_blocked_until_benchmarks_reviewed(
    tmp_path: Path,
) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _form_context(client)

    response = client.post(
        "/onboarding/contributions",
        data={"_csrf_token": csrf, "command_id": command_id},
    )
    assert response.status_code == 409

    client.post("/onboarding/identity", data={"_csrf_token": csrf})
    client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    client.post("/onboarding/inspect", data={"_csrf_token": csrf})
    response = client.post(
        "/onboarding/contributions",
        data={"_csrf_token": csrf, "command_id": command_id},
    )
    assert response.status_code == 409
    assert not harness.ai_active
    assert not harness.recorder.enabled


def test_simultaneous_choices_are_independent_and_do_not_change_membership(
    tmp_path: Path,
) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    onboarding, _inspection, _benchmarks, contributions, adapter = services
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    ids = _candidate_ids(app, contributions)

    before = onboarding.authorized_context().binding
    response = client.post(
        "/onboarding/contributions",
        data=_choice_form(
            csrf,
            command_id,
            {
                ids["recorder"]: ContributionDesiredState.ENABLED.value,
                ids["language-model"]: ContributionDesiredState.ENABLED.value,
                ids["compute"]: ContributionDesiredState.ENABLED.value,
                ids["storage"]: ContributionDesiredState.ASK_LATER.value,
            },
        ),
    )
    assert response.status_code == 303
    after = onboarding.authorized_context().binding

    assert adapter.benchmark_calls == 1
    assert harness.recorder.enabled
    assert harness.ai_active == {AI_ID}
    assert harness.compute_active
    assert not harness.storage_assigned
    assert before == after

    with app.app_context():
        intents = {item.candidate_id: item for item in contributions.intents()}
    assert intents[ids["recorder"]].activation_state is ContributionActivationState.ACTIVE
    assert intents[ids["language-model"]].activation_state is ContributionActivationState.ACTIVE
    assert intents[ids["compute"]].activation_state is ContributionActivationState.ACTIVE


def test_disable_and_reenable_fence_only_selected_contribution(tmp_path: Path) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)

    client.post(
        "/onboarding/contributions",
        data=_choice_form(
            csrf,
            command_id,
            {
                ids["language-model"]: "enabled",
                ids["compute"]: "enabled",
            },
        ),
    )
    response = client.post(
        "/onboarding/contributions",
        data=_choice_form(
            csrf,
            command_id,
            {
                ids["compute"]: "disabled",
                ids["language-model"]: "enabled",
            },
        ),
    )

    assert response.status_code == 303
    assert harness.compute_fences
    assert not harness.compute_active
    assert harness.ai_active == {AI_ID}

    response = client.post(
        "/onboarding/contributions",
        data=_choice_form(
            csrf,
            command_id,
            {
                ids["compute"]: "enabled",
                ids["language-model"]: "enabled",
            },
        ),
    )
    assert response.status_code == 303
    assert harness.compute_active
    assert harness.ai_active == {AI_ID}


def test_storage_stays_pending_until_existing_assignment_is_observed(
    tmp_path: Path,
) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)

    client.post(
        "/onboarding/contributions",
        data=_choice_form(csrf, command_id, {ids["storage"]: "enabled"}),
    )
    with app.app_context():
        pending = {item.candidate_id: item for item in contributions.intents()}[
            ids["storage"]
        ]
    assert pending.activation_state is ContributionActivationState.PENDING
    assert not harness.storage_assigned

    harness.storage_assigned.add(STORAGE_ID)
    response = client.post(
        "/onboarding/contributions/reconcile",
        data={"_csrf_token": csrf, "command_id": command_id},
    )
    assert response.status_code == 303
    with app.app_context():
        active = {item.candidate_id: item for item in contributions.intents()}[
            ids["storage"]
        ]
    assert active.activation_state is ContributionActivationState.ACTIVE


def test_restart_reconcile_restores_enabled_and_keeps_disabled_fenced(
    tmp_path: Path,
) -> None:
    harness = AuthorityHarness()
    current = [NOW]
    services = _services(tmp_path, harness, current)
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)

    client.post(
        "/onboarding/contributions",
        data=_choice_form(
            csrf,
            command_id,
            {
                ids["language-model"]: "enabled",
                ids["compute"]: "disabled",
            },
        ),
    )
    contributions.close()
    harness.reset_runtime()

    restarted = CapabilityContributionService(
        onboarding_service=services[0],
        inspection_service=services[1],
        benchmark_service=services[2],
        state_database=tmp_path / "onboarding.sqlite3",
        sources=harness.sources(),
        adapters=harness.adapters(),
        clock=lambda: current[0],
    )
    restarted_app = _app((*services[:3], restarted, services[4]))
    response = restarted_app.test_client().get("/onboarding")

    assert response.status_code == 200
    assert harness.ai_active == {AI_ID}
    assert harness.compute_fences
    assert not harness.compute_active


def test_expired_evidence_suspends_enabled_contribution(tmp_path: Path) -> None:
    harness = AuthorityHarness()
    current = [NOW]
    services = _services(tmp_path, harness, current)
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)

    client.post(
        "/onboarding/contributions",
        data=_choice_form(csrf, command_id, {ids["language-model"]: "enabled"}),
    )
    current[0] = NOW + timedelta(minutes=16)
    with app.app_context():
        reconciled = {item.candidate_id: item for item in contributions.reconcile()}

    assert reconciled[ids["language-model"]].activation_state is (
        ContributionActivationState.SUSPENDED
    )
    assert not harness.ai_active


def test_csrf_context_override_and_adapter_secrets_fail_closed(tmp_path: Path) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)

    assert client.post(
        "/onboarding/contributions",
        data={"command_id": command_id},
    ).status_code == 403
    assert client.post(
        "/onboarding/contributions",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "actor_node_id": "attacker",
        },
    ).status_code == 403

    harness.fail_ai = True
    response = client.post(
        "/onboarding/contributions",
        data=_choice_form(csrf, command_id, {ids["language-model"]: "enabled"}),
        follow_redirects=True,
    )
    body = response.get_data(as_text=True).casefold()
    assert "contribution-secret" not in body
    assert "10.0.0.9" not in body
    assert "http://" not in body


def test_corrupt_intent_state_renders_degraded_without_leakage(tmp_path: Path) -> None:
    harness = AuthorityHarness()
    services = _services(tmp_path, harness, [NOW])
    app = _app(services)
    client = app.test_client()
    csrf, command_id = _connect_inspect_and_benchmark(client)
    contributions = services[3]
    ids = _candidate_ids(app, contributions)
    client.post(
        "/onboarding/contributions",
        data=_choice_form(csrf, command_id, {ids["language-model"]: "enabled"}),
    )
    contributions.close()

    with sqlite3.connect(tmp_path / "onboarding.sqlite3") as connection:
        connection.execute(
            "UPDATE contribution_intents SET intent_json=?",
            ('{"secret":"http://10.0.0.8/private-token"}',),
        )
        connection.commit()

    response = client.get("/onboarding?step=contributions")
    body = response.get_data(as_text=True).casefold()
    assert response.status_code == 200
    assert "contribution state unavailable" in body
    assert "private-token" not in body
    assert "10.0.0.8" not in body

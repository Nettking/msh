"""Bounded synthetic environment for the ICSE FCP demonstration.

Only physical/environmental endpoints are synthetic here. The federation,
onboarding, inspection, benchmarking, contribution, policy, coordinator, and
HTTP route implementations are production FCP components.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.benchmarking import BenchmarkObservation, InspectionFinding
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
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
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

NOW = datetime(2026, 8, 3, 14, 30, tzinfo=timezone.utc)
BENCHMARK_ID = "benchmark.icse.synthetic.v1"
AI_ID = "ai-local"
STORAGE_ID = "storage-local"
HANDLER_ID = "safe-handler"


@contextmanager
def _capability_demo_app_environment():
    """Use FCP's supported development auth bypass only while creating the app.

    Human-login behavior is outside E1/E2. ``init_human_auth`` explicitly
    supports disabling human auth in development/test contexts so legacy Flask
    request surfaces can be exercised without manufacturing an administrator.
    Restore the process environment immediately because the reviewer runner may
    execute several independent scenarios in one process.
    """

    names = ("FCP_DEVELOPMENT", "FCP_AUTH_DISABLED")
    previous = {name: os.environ.get(name) for name in names}
    os.environ["FCP_DEVELOPMENT"] = "1"
    os.environ["FCP_AUTH_DISABLED"] = "1"
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class SyntheticInspectionAdapter:
    """Deterministic stand-in for physical service and hardware inspection."""

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

    def __call__(self, _context) -> InspectionFinding:
        return InspectionFinding(
            detected_services=(AI_ID, STORAGE_ID),
            registered_handlers=(HANDLER_ID,),
            detected_data_sources=("synthetic-machine",),
            available_prerequisites=("fixture-ready",),
            recommended_benchmark_ids=(BENCHMARK_ID,),
        )

    def dependency_inputs(self, service_id: str) -> dict[str, int]:
        if service_id != AI_ID:
            raise KeyError(service_id)
        return {"fixture_revision": 1}

    def benchmark(self, context) -> BenchmarkObservation:
        context.raise_if_cancelled()
        return BenchmarkObservation(
            metrics={"available": True},
            recommendation=BenchmarkRecommendation.RECOMMENDED,
        )


@dataclass(frozen=True)
class _Descriptor:
    handler_id: str = HANDLER_ID
    capability_type: str = "synthetic-compute"
    protocol: str = "fcp-synthetic"
    protocol_version: str = "1.0"
    descriptor_fingerprint: str = "sha256:" + "c" * 64


@dataclass(frozen=True)
class _Binding:
    descriptor: _Descriptor = _Descriptor()


class _Inventory:
    def __init__(self) -> None:
        self.binding = _Binding()

    def list_bindings(self):
        return (self.binding,)

    def get(self, handler_id):
        return self.binding if handler_id == HANDLER_ID else None


class _RecorderAuthority:
    def __init__(self) -> None:
        self.enabled = False

    def set_enabled(self, enabled, _settings):
        self.enabled = bool(enabled)
        return True, "recorder state changed"

    def status(self, _settings):
        return {"requested_enabled": self.enabled}


class SyntheticAuthorityHarness:
    """Visible consequences at the physical/provider authority boundaries."""

    def __init__(self) -> None:
        self.recorder = _RecorderAuthority()
        self.ai_active: set[str] = set()
        self.compute_active: set[tuple[str, str]] = set()
        self.storage_assigned: set[str] = set()
        self.storage_fences: list[str] = []
        self.inventory = _Inventory()

    def sources(self):
        return (
            RecorderCandidateSource(),
            AICandidateSource(
                {
                    AI_ID: AICandidateSpec(
                        service_id=AI_ID,
                        protocol="fcp-language-model",
                        display_label="Synthetic AI",
                        capacity_envelope={"model": "synthetic-model"},
                    )
                }
            ),
            ComputeCandidateSource(self.inventory),
            StorageCandidateSource(
                {
                    STORAGE_ID: StorageCandidateSpec(
                        provider_id=STORAGE_ID,
                        protocol="fcp-storage",
                        display_label="Synthetic storage",
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
                enable_provider=lambda candidate: self.ai_active.add(
                    candidate.capacity_envelope["provider_id"]
                ),
                disable_provider=self.ai_active.discard,
                is_provider_active=lambda provider_id: provider_id in self.ai_active,
            ),
            ComputeContributionAdapter(
                self.inventory,
                activate_binding=lambda binding: self.compute_active.add(
                    (
                        binding.descriptor.handler_id,
                        binding.descriptor.descriptor_fingerprint,
                    )
                ),
                fence_handler=lambda handler_id, fingerprint: self.compute_active.discard(
                    (handler_id, fingerprint)
                ),
                is_handler_active=lambda handler_id, fingerprint: (
                    handler_id,
                    fingerprint,
                )
                in self.compute_active,
            ),
            StorageContributionAdapter(
                is_assigned=lambda provider_id: provider_id in self.storage_assigned,
                fence_candidate=self.storage_fences.append,
            ),
        )


@dataclass
class DemoStack:
    root: Path
    current: list[datetime]
    harness: SyntheticAuthorityHarness
    coordinator: SessionCoordinator
    onboarding: CapabilityOnboardingService
    inspection: CapabilityInspectionService
    benchmarks: CapabilityBenchmarkService
    contributions: CapabilityContributionService
    app: object

    def close(self) -> None:
        self.contributions.close()


def build_stack(
    root: Path,
    *,
    current: list[datetime],
    harness: SyntheticAuthorityHarness,
    coordinator: SessionCoordinator | None = None,
    discovery_sources=(),
) -> DemoStack:
    root.mkdir(parents=True, exist_ok=True)
    clock = lambda: current[0]
    coordinator = coordinator or SessionCoordinator(root / "coordinator.sqlite3", clock=clock)
    onboarding = CapabilityOnboardingService(
        identity_directory=root / "device",
        state_database=root / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name=f"ICSE {root.name}",
        discovery_sources=discovery_sources,
        clock=clock,
    )
    inspection_adapter = SyntheticInspectionAdapter()
    inspection = CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=root / "onboarding.sqlite3",
        adapters=(inspection_adapter,),
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
    with _capability_demo_app_environment():
        app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=onboarding,
        CAPABILITY_INSPECTION_SERVICE=inspection,
        CAPABILITY_BENCHMARK_SERVICE=benchmarks,
        CAPABILITY_CONTRIBUTION_SERVICE=contributions,
        CAPABILITY_ONBOARDING_CONTRIBUTION_POLICY=ContributionPolicyEvaluator(),
    )
    return DemoStack(
        root=root,
        current=current,
        harness=harness,
        coordinator=coordinator,
        onboarding=onboarding,
        inspection=inspection,
        benchmarks=benchmarks,
        contributions=contributions,
        app=app,
    )


def browser_context(client) -> tuple[str, str]:
    response = client.get("/onboarding")
    if response.status_code != 200:
        raise RuntimeError(f"onboarding route returned {response.status_code}")
    with client.session_transaction() as browser:
        return str(browser[_CSRF_SESSION_KEY]), str(browser[_COMMAND_SESSION_KEY])


def create_identity(client) -> tuple[str, str]:
    csrf, command_id = browser_context(client)
    response = client.post("/onboarding/identity", data={"_csrf_token": csrf})
    if response.status_code != 303:
        raise RuntimeError(f"identity creation returned {response.status_code}")
    return csrf, command_id


def connect_local(client) -> tuple[str, str]:
    csrf, command_id = create_identity(client)
    client.get("/onboarding?step=federation")
    response = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    if response.status_code != 303:
        raise RuntimeError(f"local federation creation returned {response.status_code}")
    return csrf, command_id


def join_candidate(client, candidate: FederationDiscoveryCandidate) -> tuple[str, str]:
    csrf, command_id = create_identity(client)
    client.get("/onboarding?step=federation")
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
    if response.status_code != 303:
        raise RuntimeError(f"federation join returned {response.status_code}")
    return csrf, command_id


def candidate_for_existing_federation(
    stack: DemoStack,
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
        request_id=f"icse-invite-{suffix}",
    )["token"]
    return FederationDiscoveryCandidate(
        federation_label="ICSE demonstration federation",
        internal_session_id=session_id,
        federation_fingerprint="icse-demo-fingerprint",
        transport_kind="configured-demo",
        verification_code=f"ICSE-{suffix.upper()}",
        discovered_at=stack.current[0],
        expires_at=stack.current[0] + timedelta(minutes=5),
        enrollment_token=enrollment_token,
        invitation_token=invitation_token,
    )


def inspect_and_benchmark(client, csrf: str) -> None:
    if client.post("/onboarding/inspect", data={"_csrf_token": csrf}).status_code != 303:
        raise RuntimeError("inspection did not complete")
    response = client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": AI_ID,
        },
    )
    if response.status_code != 303:
        raise RuntimeError(f"benchmark returned {response.status_code}")


def candidate_ids(stack: DemoStack) -> dict[str, str]:
    with stack.app.app_context():
        return {
            candidate.capability_type: candidate.candidate_id
            for candidate in stack.contributions.recommend()
        }


def save_choices(
    client,
    *,
    csrf: str,
    command_id: str,
    ids: dict[str, str],
    recorder: str = "disabled",
    language_model: str = "disabled",
    compute: str = "disabled",
    storage: str = "disabled",
) -> None:
    choices = {
        ids["recorder"]: recorder,
        ids["language-model"]: language_model,
        ids["compute"]: compute,
        ids["storage"]: storage,
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
    if response.status_code != 303:
        raise RuntimeError(f"contribution update returned {response.status_code}")


def configured_discovery(candidate: FederationDiscoveryCandidate):
    return ConfiguredFederationDiscoveryAdapter((candidate,))

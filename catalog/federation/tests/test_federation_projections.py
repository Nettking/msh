from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.projections import (
    ActivityRecord,
    BenchmarkRecord,
    BenchmarkResultsAdapter,
    BenchmarkSnapshot,
    ContributionRecord,
    DeviceRecord,
    FederationAuthorityAdapter,
    FederationAuthoritySnapshot,
    FederationPage,
    FederationProjectionService,
    JobAuthorityAdapter,
    JobAuthoritySnapshot,
    JobRecord,
    OnboardingContractsAdapter,
    OnboardingSnapshot,
    ProjectionAdapters,
    ProviderOperatorAdapter,
    ProviderRecord,
    ProviderSnapshot,
    StorageAuthorityAdapter,
    StorageAuthoritySnapshot,
    StorageGroupRecord,
    StorageProviderRecord,
    assert_public_projection,
)

NOW = datetime(2026, 8, 2, 20, 37, tzinfo=timezone.utc)
FORBIDDEN_FRAGMENTS = (
    "internal_session_id",
    "session-secret",
    "msh_join_",
    "bearer ",
    "127.0.0.1",
    "http://",
    "https://",
    "c:\\\\",
    "/var/lib/",
    "lease-secret",
    "grant-secret",
)


class StaticAdapter:
    def __init__(self, value: object) -> None:
        self.value = value

    def snapshot(self) -> object:
        return self.value


def _complete_adapters(*, expired_benchmark: bool = False) -> ProjectionAdapters:
    contribution = ContributionRecord(
        candidate_id="candidate-ai",
        device_id="node-local",
        capability_type="language-model",
        protocol="msh-ai",
        label="Local language model",
        capacity={"use": "interactive", "concurrency": 1},
        missing_prerequisites=(),
        policy_state="allowed",
        desired_state="enabled",
        activation_state="active",
        reason=None,
    )
    storage_candidate = ContributionRecord(
        candidate_id="candidate-storage",
        device_id="node-local",
        capability_type="storage",
        protocol="msh-storage",
        label="Local storage candidate",
        capacity={"capacity_band": "32-63-gib"},
        missing_prerequisites=(),
        policy_state="allowed",
        desired_state="ask-later",
        activation_state="inactive",
        reason=None,
    )
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-workshop",
        connection_state="connected",
        trusted=True,
        device_id="node-local",
        inspection_revision=3,
        inspection_expires_at=NOW + timedelta(hours=1),
        os_family="linux",
        architecture="x86-64",
        resource_observations={
            "cpu": {"logical_cores_band": "8-15"},
            "memory": {"capacity_band": "16-31-gib"},
        },
        detected_services=("ollama",),
        registered_handlers=("image-analysis",),
        detected_data_sources=("mtconnect-machine",),
        warnings=(),
        contributions=(contribution, storage_candidate),
    )
    providers = ProviderSnapshot(
        available=True,
        reason_code="current",
        is_owner=True,
        providers=(
            ProviderRecord(
                capability_id="candidate-ai",
                node_id="node-local",
                capability_type="language-model",
                protocol="msh-ai",
                protocol_version="1.0",
                enrollment_state="approved",
                health_state="current",
                provider_status="ready",
                activation_state="eligible",
                reason_code="remote-ai-eligible",
                max_concurrent_jobs=1,
                active_jobs=0,
                queue_depth=0,
                utilization_millis=250,
                can_manage=True,
                allowed_actions=("suspend", "reconcile"),
            ),
        ),
    )
    benchmark = BenchmarkRecord(
        run_id="run-ai-1",
        device_id="node-local",
        benchmark_id="ai-response",
        target_service_id="candidate-ai",
        state="passed",
        recommendation="recommended",
        current=not expired_benchmark,
        validity_reason="current" if not expired_benchmark else "expired",
        finished_at=NOW - timedelta(minutes=3),
        expires_at=(
            NOW + timedelta(hours=6)
            if not expired_benchmark
            else NOW - timedelta(minutes=1)
        ),
        metrics={"latency_band": "interactive"},
        diagnostics=(),
    )
    federation = FederationAuthoritySnapshot(
        available=True,
        reason_code="current",
        label="Workshop federation",
        state="active",
        revision=12,
        devices=(
            DeviceRecord("node-local", "Workshop laptop", "connected", NOW, 1),
            DeviceRecord("node-recorder", "Recorder station", "connected", NOW, 1),
        ),
        activity=(
            ActivityRecord(
                12,
                NOW,
                "capability.updated",
                "Service updated",
                "A device refreshed a service announcement.",
            ),
        ),
    )
    storage = StorageAuthoritySnapshot(
        available=True,
        reason_code="current",
        revision=4,
        providers=(
            StorageProviderRecord(
                "storage-a",
                "node-recorder",
                "msh-storage",
                "1.0",
                "ready",
                True,
                True,
                ("primary",),
            ),
        ),
        groups=(StorageGroupRecord("reports", "storage-a", (), True),),
    )
    jobs = JobAuthoritySnapshot(
        available=True,
        reason_code="current",
        jobs=(
            JobRecord(
                "job-1",
                "image-analysis",
                "running",
                "candidate-ai",
                1,
                NOW,
                None,
            ),
        ),
    )
    return ProjectionAdapters(
        onboarding=StaticAdapter(onboarding),
        providers=StaticAdapter(providers),
        benchmarks=StaticAdapter(BenchmarkSnapshot(True, "current", (benchmark,))),
        federation=StaticAdapter(federation),
        storage=StaticAdapter(storage),
        jobs=StaticAdapter(jobs),
    )


def _render_all(
    service: FederationProjectionService,
    *,
    technical: bool = False,
) -> list[dict[str, Any]]:
    return [
        service.project(page, include_technical=technical).to_dict()
        for page in FederationPage
    ]


def _serialized(payload: object) -> str:
    if is_dataclass(payload):
        payload = asdict(payload)
    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    ).casefold()


def test_all_nine_pages_are_framework_neutral_and_public_safe() -> None:
    service = FederationProjectionService(_complete_adapters(), now=lambda: NOW)

    payloads = _render_all(service)

    assert {payload["page"] for payload in payloads} == {
        page.value for page in FederationPage
    }
    for payload in payloads:
        assert payload["schema"] == "msh.federation-product-view.v1"
        assert payload["active_section"] == payload["page"]
        assert len(payload["sections"]) == 9
        assert payload["technical_details"] == []
        assert_public_projection(payload)
        serialized = _serialized(payload)
        assert not any(fragment in serialized for fragment in FORBIDDEN_FRAGMENTS)
        highlighted_actions = int(payload["recommended_action"] is not None)
        highlighted_actions += int(
            payload["degraded"]["visible"]
            and payload["degraded"]["action"]["label"] != "Open overview"
        )
        assert highlighted_actions <= 1


def test_technical_details_are_opt_in_and_remain_bounded() -> None:
    service = FederationProjectionService(_complete_adapters(), now=lambda: NOW)

    ordinary = service.overview().to_dict()
    advanced = service.overview(include_technical=True).to_dict()

    assert ordinary["technical_details"] == []
    assert {detail["label"] for detail in advanced["technical_details"]} == {
        "Device ID",
        "Federation ID",
        "Federation revision",
        "Inspection revision",
    }
    serialized = _serialized(advanced["technical_details"])
    assert "session" not in serialized
    assert "endpoint" not in serialized
    assert "path" not in serialized


def test_empty_and_repair_states_offer_one_useful_action() -> None:
    empty_service = FederationProjectionService(ProjectionAdapters(), now=lambda: NOW)
    empty = empty_service.overview().to_dict()

    assert empty["state"] == "empty"
    assert empty["notice"]["kind"] == "empty"
    assert empty["notice"]["action"] is None
    assert empty["recommended_action"]["url"] == "/onboarding"

    repair_onboarding = OnboardingSnapshot(
        True,
        "current",
        federation_id="federation-workshop",
        connection_state="unavailable",
        trusted=True,
        device_id="node-local",
    )
    service = FederationProjectionService(
        ProjectionAdapters(onboarding=StaticAdapter(repair_onboarding)),
        now=lambda: NOW,
    )
    repair = service.overview().to_dict()

    assert repair["state"] == "repair"
    assert repair["degraded"]["visible"] is True
    assert repair["degraded"]["action"]["url"] == "/onboarding?repair=1"
    assert repair["recommended_action"] is None


def test_expired_benchmark_is_evidence_only_and_becomes_next_action() -> None:
    service = FederationProjectionService(
        _complete_adapters(expired_benchmark=True),
        now=lambda: NOW,
    )

    overview = service.overview().to_dict()
    benchmarks = service.benchmarks().to_dict()

    assert overview["state"] == "degraded"
    assert overview["recommended_action"]["url"] == "/federation/benchmarks"
    assert benchmarks["items"][0]["state"] == "expired"
    assert "authority" not in _serialized(benchmarks["items"][0])


class State(Enum):
    CONNECTED = "connected"
    APPROVED = "approved"
    CURRENT = "current"
    ELIGIBLE = "eligible"


def test_existing_operator_surface_adapter_omits_bound_session_context() -> None:
    class Surface:
        def view(self) -> object:
            return SimpleNamespace(
                session_id="session-secret",
                actor_node_id="node-owner",
                is_owner=True,
                providers=(
                    SimpleNamespace(
                        session_id="session-secret",
                        capability_id="provider-ai",
                        node_id="node-ai",
                        capability_type="language-model",
                        protocol="msh-ai",
                        protocol_version="1.0",
                        enrollment_state=State.APPROVED,
                        health_state=State.CURRENT,
                        provider_status="ready",
                        activation_state=State.ELIGIBLE,
                        activation_reason_code="remote-ai-eligible",
                        max_concurrent_jobs=2,
                        active_jobs=1,
                        queue_depth=0,
                        utilization_millis=400,
                        can_manage=True,
                        allowed_actions=("suspend",),
                        endpoint="http://127.0.0.1:11434",
                        credential="bearer private",
                    ),
                ),
            )

    snapshot = ProviderOperatorAdapter(Surface()).snapshot()

    assert snapshot.available is True
    assert snapshot.providers[0].capability_id == "provider-ai"
    assert not hasattr(snapshot, "session_id")
    assert not hasattr(snapshot.providers[0], "endpoint")
    assert not any(
        fragment in _serialized(snapshot) for fragment in FORBIDDEN_FRAGMENTS
    )


def test_cf1_and_cf2_adapters_redact_private_values_and_preserve_safe_evidence() -> None:
    binding = SimpleNamespace(
        federation_id="federation-workshop",
        internal_session_id="session-secret",
        device_id="node-local",
        state=State.CONNECTED,
        trusted=True,
    )
    inspection = SimpleNamespace(
        device_id="node-local",
        revision=3,
        expires_at=NOW + timedelta(hours=1),
        os_family="linux",
        architecture="x86-64",
        resource_observations={
            "cpu": {"band": "8-15"},
            "endpoint": "http://127.0.0.1:11434",
            "local_path": "/var/lib/msh",
        },
        detected_services=("ollama", "http://127.0.0.1:11434"),
        registered_handlers=("image-analysis",),
        detected_data_sources=("mtconnect",),
        warnings=("bearer private", "Inspection completed"),
    )
    onboarding = OnboardingContractsAdapter(
        binding=binding,
        inspection=inspection,
    ).snapshot()

    assert onboarding.available is True
    assert onboarding.federation_id == "federation-workshop"
    assert onboarding.resource_observations == {}
    assert onboarding.detected_services == ("ollama", "Detected service")
    assert onboarding.warnings == (
        "A local inspection warning is available",
        "Inspection completed",
    )
    assert not hasattr(onboarding, "internal_session_id")

    result = SimpleNamespace(
        run_id="run-1",
        device_id="node-local",
        benchmark_id="ai-response",
        target_service_id="provider-ai",
        state="passed",
        recommendation="recommended",
        finished_at=NOW,
        expires_at=NOW + timedelta(hours=1),
        metrics={"latency_band": "interactive", "endpoint": "http://127.0.0.1"},
        diagnostics=("C:\\private\\model.bin", "Completed safely"),
    )
    benchmark = BenchmarkResultsAdapter(
        SimpleNamespace(list_results=lambda: (result,)),
        now=lambda: NOW,
    ).snapshot()

    assert benchmark.available is True
    assert benchmark.results[0].current is True
    assert benchmark.results[0].metrics == {}
    assert benchmark.results[0].diagnostics == (
        "Benchmark detail unavailable",
        "Completed safely",
    )


def test_federation_adapter_filters_other_sessions_and_paginates() -> None:
    session = SimpleNamespace(display_name="Workshop", state="active", revision=3)
    pages = {
        None: {
            "nodes": [
                {
                    "node_id": "node-local",
                    "display_name": "Local",
                    "connection_state": "connected",
                    "last_heartbeat": NOW,
                }
            ],
            "capabilities": [],
            "pagination": {"has_more": True, "next_cursor": "opaque-cursor"},
        },
        "opaque-cursor": {
            "nodes": [
                {
                    "node_id": "node-other",
                    "display_name": "Other federation",
                    "connection_state": "connected",
                },
                {
                    "node_id": "node-recorder",
                    "display_name": "Recorder",
                    "connection_state": "connected",
                },
            ],
            "capabilities": [
                {"session_id": "session-secret", "node_id": "node-recorder"},
                {"session_id": "session-other", "node_id": "node-other"},
            ],
            "pagination": {"has_more": False, "next_cursor": None},
        },
    }
    events = (
        SimpleNamespace(
            revision=1,
            event_type="node.joined",
            occurred_at=NOW,
            payload={"node_id": "node-local"},
        ),
        SimpleNamespace(
            revision=2,
            event_type="node.joined",
            occurred_at=NOW,
            payload={"node_id": "node-recorder"},
        ),
        SimpleNamespace(
            revision=3,
            event_type="capability.updated",
            occurred_at=NOW,
            payload={"endpoint": "http://127.0.0.1"},
        ),
    )

    class Coordinator:
        store = SimpleNamespace(get_session=lambda _session_id: session)

        def status(self, *, actor_node_id: str, cursor: str | None = None) -> object:
            assert actor_node_id == "node-local"
            return pages[cursor]

        def replay_page(self, **_: object) -> tuple[tuple[object, ...], int]:
            return events, 3

    snapshot = FederationAuthorityAdapter(
        Coordinator(),
        actor_node_id="node-local",
        internal_session_id="session-secret",
    ).snapshot()

    assert snapshot.available is True
    assert [device.node_id for device in snapshot.devices] == [
        "node-local",
        "node-recorder",
    ]
    assert snapshot.devices[1].capability_count == 1
    assert "node-other" not in _serialized(snapshot)
    assert "session-secret" not in _serialized(snapshot)
    assert "127.0.0.1" not in _serialized(snapshot)


def test_storage_and_job_adapters_do_not_publish_authority_secrets() -> None:
    storage_snapshot = SimpleNamespace(
        revision=7,
        providers={
            "storage-a": SimpleNamespace(
                provider_id="storage-a",
                node_id="node-storage",
                protocol="msh-storage",
                protocol_version="1.0",
                status="ready",
                authorized=True,
                assignable=True,
                endpoint="https://storage.private",
            )
        },
        groups={
            "reports": SimpleNamespace(
                group_id="reports",
                primary_provider_id="storage-a",
                replica_provider_ids=(),
            )
        },
        leader_grants={
            "reports": {
                "grant_id": "grant-secret",
                "fencing_token": 99,
                "lease_id": "lease-secret",
            }
        },
    )
    storage = StorageAuthorityAdapter(
        SimpleNamespace(snapshot=lambda _session_id: storage_snapshot),
        internal_session_id="session-secret",
    ).snapshot()

    assert storage.available is True
    assert storage.groups[0].leader_active is True
    assert not any(
        fragment in _serialized(storage) for fragment in FORBIDDEN_FRAGMENTS
    )

    job = SimpleNamespace(
        job_id="job-1",
        capability_type="image-analysis",
        status="running",
        attempts=(SimpleNamespace(error_code=None),),
        artifact_reference={"path": "/var/lib/result.json"},
    )
    durable = SimpleNamespace(
        job=job,
        ownership=SimpleNamespace(
            owner_provider_id="provider-compute",
            lease_id="lease-secret",
            session_id="session-secret",
        ),
        updated_at=NOW,
    )
    jobs = JobAuthorityAdapter(lambda: (durable,)).snapshot()

    assert jobs.available is True
    assert jobs.jobs[0].provider_id == "provider-compute"
    assert not any(fragment in _serialized(jobs) for fragment in FORBIDDEN_FRAGMENTS)


def test_public_safety_boundary_rejects_private_fields_and_absolute_locations() -> None:
    with pytest.raises(FederationValidationError):
        assert_public_projection({"internal_session_id": "session-secret"})
    with pytest.raises(FederationValidationError):
        assert_public_projection({"endpoint": "http://127.0.0.1"})
    with pytest.raises(FederationValidationError):
        assert_public_projection({"detail_url": "https://example.com/private"})

    assert assert_public_projection(
        {"detail_url": "/federation/services", "label": "Services"}
    ) == {"detail_url": "/federation/services", "label": "Services"}

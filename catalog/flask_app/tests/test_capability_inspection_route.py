from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.benchmarking import InspectionFinding
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_models import BenchmarkDefinition
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import (
    _COMMAND_SESSION_KEY,
    _CSRF_SESSION_KEY,
)
from catalog.flask_app.services.capability_inspection_service import (
    CapabilityInspectionService,
)
from catalog.flask_app.services.capability_onboarding_service import (
    CapabilityOnboardingService,
)

NOW = datetime(2026, 8, 3, 1, 0, tzinfo=timezone.utc)


class InspectionOnlyAdapter:
    definition = BenchmarkDefinition(
        benchmark_id="benchmark.fixture.read-only.v1",
        capability_type="fixture",
        capability_protocol="fcp-fixture",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=1,
        prerequisites=("fixture-ready",),
        metric_names=("available",),
        invalidation_inputs=("fixture_revision",),
        privacy_classification="public-summary",
    )
    result_ttl_seconds = 300

    def __init__(self) -> None:
        self.inspection_calls = 0
        self.benchmark_calls = 0

    def __call__(self, _context) -> InspectionFinding:
        self.inspection_calls += 1
        return InspectionFinding(
            resource_observations={
                "fixture": {
                    "status": "available",
                    "endpoint": "http://10.0.0.9:9999/private",
                }
            },
            detected_services=("fixture-service",),
            registered_handlers=("handler-safe",),
            detected_data_sources=("machine-alpha",),
            available_prerequisites=("fixture-ready",),
            recommended_benchmark_ids=(self.definition.benchmark_id,),
            warnings=("Fixture inspection completed without running work",),
        )

    def benchmark(self, _context):
        self.benchmark_calls += 1
        raise AssertionError("CFI-3 must not execute a benchmark")


def _onboarding_service(
    tmp_path: Path,
    *,
    clock,
    coordinator: SessionCoordinator | None = None,
    setup_payload: dict[str, object] | None = None,
) -> CapabilityOnboardingService:
    coordinator = coordinator or SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=clock,
    )
    return CapabilityOnboardingService(
        identity_directory=tmp_path / "device",
        state_database=tmp_path / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name="Inspection laptop",
        discovery_sources=(),
        setup_loader=lambda: setup_payload
        or {"configured": False, "user_setup_complete": False},
        clock=clock,
    )


def _inspection_service(
    tmp_path: Path,
    onboarding: CapabilityOnboardingService,
    *,
    adapter: InspectionOnlyAdapter | None,
    clock,
    ttl: int = 900,
    setup_payload: dict[str, object] | None = None,
    scan_supplier=None,
) -> CapabilityInspectionService:
    adapters = (adapter,) if adapter is not None else None
    return CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=tmp_path / "onboarding.sqlite3",
        adapters=adapters,
        setup_loader=lambda: setup_payload
        or {"configured": False, "user_setup_complete": False},
        mtconnect_scan_supplier=scan_supplier,
        inspection_ttl_seconds=ttl,
        clock=clock,
        system_observer=lambda: {
            "cpu": {"logical_cores_band": "8-15"},
            "memory": {"capacity_band": "16-31-gib"},
            "network": {"status": "not-benchmarked"},
        },
    )


def _app(
    onboarding: CapabilityOnboardingService,
    inspection: CapabilityInspectionService,
):
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=onboarding,
        CAPABILITY_INSPECTION_SERVICE=inspection,
    )
    return app


def _form_context(client) -> tuple[str, str]:
    response = client.get("/onboarding")
    assert response.status_code == 200
    with client.session_transaction() as browser:
        return (
            str(browser[_CSRF_SESSION_KEY]),
            str(browser[_COMMAND_SESSION_KEY]),
        )


def _connect_local(client) -> tuple[str, str]:
    csrf, command_id = _form_context(client)
    created = client.post(
        "/onboarding/identity",
        data={"_csrf_token": csrf},
    )
    assert created.status_code == 303
    client.get("/onboarding?step=federation")
    connected = client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    )
    assert connected.status_code == 303
    return csrf, command_id


def test_app_factory_registers_cfi3_before_the_legacy_gate(tmp_path: Path) -> None:
    clock = lambda: NOW
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=InspectionOnlyAdapter(),
        clock=clock,
    )
    app = _app(onboarding, inspection)

    assert "capability_inspection_web" in app.blueprints
    assert "capability_inspection_web.onboarding" in app.view_functions
    assert "capability_inspection_web.inspect_device" in app.view_functions
    assert "capability_onboarding_web.create_identity" in app.view_functions
    assert "capability_onboarding_web.federation_action" in app.view_functions
    assert "web.startup" in app.view_functions

    rules = [
        rule
        for rule in app.url_map.iter_rules()
        if str(rule) == "/onboarding/inspect"
    ]
    assert any(
        rule.endpoint == "capability_inspection_web.inspect_device"
        and "POST" in rule.methods
        for rule in rules
    )


def test_inspection_requires_identity_and_trusted_federation(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = InspectionOnlyAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=adapter,
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _form_context(client)

    response = client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    )
    page = client.get("/onboarding?step=inspect").get_data(as_text=True)

    assert response.status_code == 303
    assert inspection.load() is None
    assert adapter.inspection_calls == 0
    assert "Connect this device to a trusted federation before inspection" in page


def test_inspection_persists_safe_snapshot_without_running_benchmark_or_authority(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    adapter = InspectionOnlyAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=adapter,
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)
    binding = onboarding.binding_or_none()
    assert binding is not None
    before_revision = onboarding.coordinator.store.get_session(
        binding.internal_session_id
    ).revision

    response = client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    )
    page = client.get("/onboarding?step=inspect").get_data(as_text=True)
    snapshot = inspection.load()

    assert response.status_code == 303
    assert snapshot is not None
    assert snapshot.revision == 1
    assert snapshot.detected_services == ("fixture-service",)
    assert snapshot.registered_handlers == ("handler-safe",)
    assert snapshot.detected_data_sources == ("machine-alpha",)
    assert snapshot.recommended_benchmark_ids == (
        "benchmark.fixture.read-only.v1",
    )
    assert adapter.inspection_calls == 1
    assert adapter.benchmark_calls == 0
    assert onboarding.coordinator.store.get_session(
        binding.internal_session_id
    ).revision == before_revision

    assert "Inspection current" in page
    assert "Fixture Service" in page
    assert "Compute handler: Handler Safe" in page
    assert "Machine Alpha" in page
    assert "Recommended bounded checks" in page
    assert "no benchmark has been run" in page
    assert "8-15" in page
    assert "16-31-gib" in page
    assert "10.0.0.9" not in page
    assert "/private" not in page
    assert "contributions remain blocked" in page.lower()


def test_inspection_revision_is_monotonic_and_survives_restart(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    adapter = InspectionOnlyAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=adapter,
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)

    client.post("/onboarding/inspect", data={"_csrf_token": csrf})
    client.post("/onboarding/inspect", data={"_csrf_token": csrf})
    assert inspection.load().revision == 2

    reopened_onboarding = _onboarding_service(
        tmp_path,
        clock=clock,
        coordinator=onboarding.coordinator,
    )
    reopened_inspection = _inspection_service(
        tmp_path,
        reopened_onboarding,
        adapter=InspectionOnlyAdapter(),
        clock=clock,
    )
    reopened = reopened_inspection.load()

    assert reopened is not None
    assert reopened.revision == 2
    assert reopened.device_id == onboarding.identity_or_none().identity.node_id


def test_expired_inspection_is_visible_but_not_completed(tmp_path: Path) -> None:
    now = [NOW]
    clock = lambda: now[0]
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=InspectionOnlyAdapter(),
        clock=clock,
        ttl=5,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)
    client.post("/onboarding/inspect", data={"_csrf_token": csrf})

    now[0] = NOW + timedelta(seconds=6)
    page = client.get("/onboarding?step=inspect").get_data(as_text=True)

    assert "Inspection expired" in page
    assert "This inspection has expired" in page
    assert "Run inspection again" in page


def test_corrupt_inspection_state_fails_closed_without_leaking_bytes(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=InspectionOnlyAdapter(),
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)
    client.post("/onboarding/inspect", data={"_csrf_token": csrf})

    secret = "http://192.168.50.9:11434/private-token"
    with sqlite3.connect(tmp_path / "onboarding.sqlite3") as connection:
        connection.execute(
            """
            UPDATE device_inspection_snapshot
            SET snapshot_json=? WHERE singleton=1
            """,
            (f"not-json-{secret}",),
        )
        connection.commit()

    response = client.get("/onboarding?step=inspect")
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Inspection unavailable" in page
    assert "No value was trusted" in page
    assert secret not in page
    assert "192.168.50.9" not in page


def test_inspection_rejects_csrf_and_request_context_override(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    adapter = InspectionOnlyAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=adapter,
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)

    missing = client.post("/onboarding/inspect", data={})
    override = client.post(
        "/onboarding/inspect",
        data={
            "_csrf_token": csrf,
            "device_id": "attacker-device",
            "session_id": "attacker-session",
        },
    )

    assert missing.status_code == 403
    assert override.status_code == 403
    assert adapter.inspection_calls == 0
    assert inspection.load() is None


def test_default_composition_reads_existing_mtconnect_scan_without_starting_scan(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    supplier_calls = []

    def scan_supplier():
        supplier_calls.append("read")
        return {
            "schema": "fcp.mtconnect.network_scan.v1",
            "state": "complete",
            "finished_at": "2026-08-03T01:00:00Z",
            "results": [
                {
                    "source_id": "mtconnect-source-alpha",
                    "probe_sha256": "abc123",
                    "machine_count": 1,
                    "machines": [
                        {
                            "identity_kind": "uuid",
                            "identity_value": "machine-alpha",
                        }
                    ],
                }
            ],
        }

    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=None,
        clock=clock,
        scan_supplier=scan_supplier,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)

    response = client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    )
    snapshot = inspection.load()

    assert response.status_code == 303
    assert snapshot is not None
    assert snapshot.detected_data_sources == ("mtconnect-source-alpha",)
    assert snapshot.detected_services == ("mtconnect-recorder-source",)
    assert snapshot.recommended_benchmark_ids == (
        "benchmark.recorder.mtconnect-source.v1",
    )
    assert supplier_calls == ["read"]


def test_default_ollama_composition_uses_read_only_inventory_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = lambda: NOW
    requests = []

    def requester(method, url, payload, timeout, max_bytes):
        requests.append((method, url, payload, timeout, max_bytes))
        assert method == "GET"
        assert url == "http://ollama:11434/api/tags"
        assert payload is None
        return {"models": [{"name": "qwen2.5:7b"}]}

    monkeypatch.setattr(
        "catalog.capabilities.benchmarking.adapters.ollama._bounded_json_request",
        requester,
    )
    setup_payload = {
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-workbench",
        "ai_enabled": True,
        "ai_provider_mode": "local",
        "ai_provider_name": "This computer",
        "ai_profile": "workstation-strong",
        "ai_model": "qwen2.5:7b",
        "ollama_base_url": "http://ollama:11434",
        "recorder_sources": "",
        "recorder_poll_interval": "0.2",
        "recorder_include_condition": False,
        "updated_at": "",
    }
    onboarding = _onboarding_service(
        tmp_path,
        clock=clock,
        setup_payload=setup_payload,
    )
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=None,
        clock=clock,
        setup_payload=setup_payload,
        scan_supplier=lambda: {"state": "never", "results": []},
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)

    response = client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    )
    snapshot = inspection.load()
    page = client.get("/onboarding?step=inspect").get_data(as_text=True)

    assert response.status_code == 303
    assert snapshot is not None
    assert "ollama-configured" in snapshot.detected_services
    assert "benchmark.ai.ollama-inference.v1" in (
        snapshot.recommended_benchmark_ids
    )
    assert len(requests) == 1
    assert "/api/chat" not in requests[0][1]
    assert "ollama:11434" not in page
    assert "qwen2.5:7b" not in page


def test_benchmark_requires_inspection_and_contribution_remains_blocked_after_cfi4(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapter=InspectionOnlyAdapter(),
        clock=clock,
    )
    client = _app(onboarding, inspection).test_client()
    csrf, _command_id = _connect_local(client)

    benchmark = client.post(
        "/onboarding/benchmarks/run",
        data={"_csrf_token": csrf},
    )
    contribution = client.post(
        "/onboarding/contributions",
        data={"_csrf_token": csrf},
    )

    assert benchmark.status_code == 409
    assert contribution.status_code == 409
    assert inspection.load() is None

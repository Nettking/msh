from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.benchmarking import (
    BenchmarkCancelled,
    BenchmarkObservation,
    InspectionFinding,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)
from catalog.flask_app.app import create_app
from catalog.flask_app.capability_onboarding_routes import (
    _COMMAND_SESSION_KEY,
    _CSRF_SESSION_KEY,
)
from catalog.flask_app.services.capability_benchmark_service import (
    CapabilityBenchmarkService,
)
from catalog.flask_app.services.capability_inspection_service import (
    CapabilityInspectionService,
)
from catalog.flask_app.services.capability_onboarding_service import (
    CapabilityOnboardingService,
)

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=timezone.utc)
BENCHMARK_ID = "benchmark.fixture.bounded.v1"
TARGET_ID = "fixture-service"


class BoundedBenchmarkAdapter:
    definition = BenchmarkDefinition(
        benchmark_id=BENCHMARK_ID,
        capability_type="fixture",
        capability_protocol="msh-fixture",
        implementation_version="1.0.0",
        max_duration_seconds=3,
        max_parallelism=1,
        prerequisites=("fixture-ready",),
        metric_names=("available", "revision"),
        invalidation_inputs=("fixture_revision",),
        privacy_classification="public-summary",
    )

    def __init__(self, *, result_ttl_seconds: int = 300) -> None:
        self.result_ttl_seconds = result_ttl_seconds
        self.inspection_calls = 0
        self.benchmark_calls = 0
        self.revision = 1
        self.started = threading.Event()
        self.block_until_cancelled = False
        self.block_until_released = False
        self.release = threading.Event()

    def __call__(self, _context) -> InspectionFinding:
        self.inspection_calls += 1
        return InspectionFinding(
            resource_observations={"fixture": {"status": "available"}},
            detected_services=(TARGET_ID,),
            available_prerequisites=("fixture-ready",),
            recommended_benchmark_ids=(BENCHMARK_ID,),
        )

    def dependency_inputs(self, service_id: str) -> dict[str, int]:
        if service_id != TARGET_ID:
            raise KeyError(service_id)
        return {"fixture_revision": self.revision}

    def benchmark(self, context) -> BenchmarkObservation:
        self.benchmark_calls += 1
        self.started.set()
        if self.block_until_released and not self.release.wait(timeout=3):
            raise AssertionError("benchmark release was not signalled")
        if self.block_until_cancelled:
            while not context.cancelled:
                time.sleep(0.01)
            raise BenchmarkCancelled("cancelled by local operator")
        context.raise_if_cancelled()
        return BenchmarkObservation(
            metrics={"available": True, "revision": self.revision},
            recommendation=BenchmarkRecommendation.RECOMMENDED,
            diagnostics=(
                "Safe evidence; http://10.0.0.9:9999/private-token is private",
            ),
        )


def _onboarding_service(
    tmp_path: Path,
    *,
    clock,
    setup_payload: dict[str, object] | None = None,
) -> CapabilityOnboardingService:
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=clock,
    )
    return CapabilityOnboardingService(
        identity_directory=tmp_path / "device",
        state_database=tmp_path / "onboarding.sqlite3",
        coordinator_database=coordinator,
        device_name="Benchmark laptop",
        discovery_sources=(),
        setup_loader=lambda: setup_payload
        or {"configured": False, "user_setup_complete": False},
        clock=clock,
    )


def _inspection_service(
    tmp_path: Path,
    onboarding: CapabilityOnboardingService,
    *,
    adapters,
    clock,
    setup_payload: dict[str, object] | None = None,
    scan_supplier=None,
) -> CapabilityInspectionService:
    return CapabilityInspectionService(
        onboarding_service=onboarding,
        state_database=tmp_path / "onboarding.sqlite3",
        adapters=adapters,
        setup_loader=lambda: setup_payload
        or {"configured": False, "user_setup_complete": False},
        mtconnect_scan_supplier=scan_supplier,
        inspection_ttl_seconds=900,
        clock=clock,
        system_observer=lambda: {
            "cpu": {"logical_cores_band": "8-15"},
            "memory": {"capacity_band": "16-31-gib"},
        },
    )


def _benchmark_service(
    tmp_path: Path,
    onboarding: CapabilityOnboardingService,
    inspection: CapabilityInspectionService,
    *,
    clock,
) -> CapabilityBenchmarkService:
    return CapabilityBenchmarkService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        state_database=tmp_path / "onboarding.sqlite3",
        clock=clock,
    )


def _app(
    onboarding: CapabilityOnboardingService,
    inspection: CapabilityInspectionService,
    benchmarks: CapabilityBenchmarkService,
):
    app = create_app()
    app.config.update(
        TESTING=True,
        CAPABILITY_ONBOARDING_SERVICE=onboarding,
        CAPABILITY_INSPECTION_SERVICE=inspection,
        CAPABILITY_BENCHMARK_SERVICE=benchmarks,
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


def _connect_and_inspect(client) -> tuple[str, str]:
    csrf, command_id = _form_context(client)
    assert client.post(
        "/onboarding/identity",
        data={"_csrf_token": csrf},
    ).status_code == 303
    client.get("/onboarding?step=federation")
    assert client.post(
        "/onboarding/federation",
        data={
            "_csrf_token": csrf,
            "command_id": command_id,
            "intent": "create-local",
        },
    ).status_code == 303
    assert client.post(
        "/onboarding/inspect",
        data={"_csrf_token": csrf},
    ).status_code == 303
    return csrf, command_id


def _post_run(client, csrf: str):
    return client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": TARGET_ID,
        },
    )


def test_app_factory_registers_cfi4_before_cfi3_and_legacy(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    app = _app(onboarding, inspection, benchmarks)

    assert list(app.blueprints).index("capability_benchmark_web") < list(
        app.blueprints
    ).index("capability_inspection_web")
    assert "capability_benchmark_web.onboarding" in app.view_functions
    assert "capability_benchmark_web.run_benchmark" in app.view_functions
    assert "capability_benchmark_web.skip_benchmarks" in app.view_functions
    assert "capability_benchmark_web.cancel_benchmark" in app.view_functions
    rules = [
        rule
        for rule in app.url_map.iter_rules()
        if str(rule) == "/onboarding/benchmarks/run"
    ]
    assert any(
        rule.endpoint == "capability_benchmark_web.run_benchmark"
        and "POST" in rule.methods
        for rule in rules
    )


def test_benchmark_routes_preserve_prerequisite_409_boundary(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _form_context(client)
    client.post("/onboarding/identity", data={"_csrf_token": csrf})

    run = _post_run(client, csrf)
    skip = client.post(
        "/onboarding/benchmarks/skip",
        data={"_csrf_token": csrf},
    )

    assert run.status_code == 409
    assert skip.status_code == 409
    assert b"remains blocked" in run.data
    assert b"remains blocked" in skip.data
    assert adapter.benchmark_calls == 0


def test_run_persists_safe_evidence_without_mutating_authority(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    binding = onboarding.binding_or_none()
    assert binding is not None
    before_revision = onboarding.coordinator.store.get_session(
        binding.internal_session_id
    ).revision

    response = _post_run(client, csrf)
    page = client.get("/onboarding?step=benchmarks").get_data(as_text=True)
    results = benchmarks.list_results()

    assert response.status_code == 303
    assert len(results) == 1
    assert results[0].state is BenchmarkState.PASSED
    assert results[0].device_id == onboarding.identity_or_none().identity.node_id
    assert results[0].target_service_id == TARGET_ID
    assert adapter.benchmark_calls == 1
    assert onboarding.coordinator.store.get_session(
        binding.internal_session_id
    ).revision == before_revision
    assert "Recommended" in page
    assert "Available" in page
    assert "Revision" in page
    assert "10.0.0.9" not in page
    assert "private-token" not in page
    assert "contributions remain blocked" in page.lower()
    assert client.post(
        "/onboarding/contributions",
        data={"_csrf_token": csrf},
    ).status_code == 409


def test_rerun_creates_new_immutable_result_and_clears_skip(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    snapshot = inspection.load()
    assert snapshot is not None

    skipped = client.post(
        "/onboarding/benchmarks/skip",
        data={"_csrf_token": csrf},
    )
    assert skipped.status_code == 303
    assert adapter.benchmark_calls == 0
    assert benchmarks.skip_store.list_for_revision(
        device_id=snapshot.device_id,
        inspection_revision=snapshot.revision,
    ) == frozenset({(BENCHMARK_ID, TARGET_ID)})

    assert _post_run(client, csrf).status_code == 303
    assert _post_run(client, csrf).status_code == 303
    results = benchmarks.list_results()

    assert len(results) == 2
    assert results[0].run_id != results[1].run_id
    assert all(result.state is BenchmarkState.PASSED for result in results)
    assert adapter.benchmark_calls == 2
    assert benchmarks.skip_store.list_for_revision(
        device_id=snapshot.device_id,
        inspection_revision=snapshot.revision,
    ) == frozenset()


def test_skip_after_completed_result_does_not_mark_completed_check_skipped(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    snapshot = inspection.load()
    assert snapshot is not None

    assert _post_run(client, csrf).status_code == 303
    response = client.post(
        "/onboarding/benchmarks/skip",
        data={"_csrf_token": csrf},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert b"0 optional benchmark checks skipped. No contribution was enabled." in response.data
    assert benchmarks.skip_store.list_for_revision(
        device_id=snapshot.device_id,
        inspection_revision=snapshot.revision,
    ) == frozenset()
    results = benchmarks.list_results()
    assert len(results) == 1
    assert results[0].state is BenchmarkState.PASSED
    summary, cards, complete = benchmarks.view_model(snapshot, connected=True)
    assert complete is True
    assert summary["can_skip"] is False
    assert cards[0]["state"] == "passed"


def test_skip_serializes_with_concurrent_benchmark_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    adapter.block_until_released = True
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    _csrf, _command_id = _connect_and_inspect(client)
    snapshot = inspection.load()
    assert snapshot is not None

    original_list_results = benchmarks.list_results
    original_clear = benchmarks.skip_store.clear
    list_snapshot_taken = threading.Event()
    release_list_snapshot = threading.Event()
    clear_completed = threading.Event()

    def delayed_list_results():
        results = original_list_results()
        list_snapshot_taken.set()
        if not release_list_snapshot.wait(timeout=3):
            raise AssertionError("list-result snapshot was not released")
        return results

    def tracked_clear(**kwargs) -> None:
        original_clear(**kwargs)
        clear_completed.set()

    monkeypatch.setattr(benchmarks, "list_results", delayed_list_results)
    monkeypatch.setattr(benchmarks.skip_store, "clear", tracked_clear)

    run_results = []
    run_errors = []
    skip_counts = []
    skip_errors = []

    def run_in_background() -> None:
        try:
            run_results.append(
                benchmarks.run(
                    benchmark_id=BENCHMARK_ID,
                    target_service_id=TARGET_ID,
                )
            )
        except BaseException as exc:  # noqa: BLE001 - asserted below
            run_errors.append(exc)

    def skip_in_background() -> None:
        try:
            skip_counts.append(benchmarks.skip_all())
        except BaseException as exc:  # noqa: BLE001 - asserted below
            skip_errors.append(exc)

    run_thread = threading.Thread(target=run_in_background, daemon=True)
    skip_thread = threading.Thread(target=skip_in_background, daemon=True)
    run_thread.start()
    assert adapter.started.wait(timeout=2)
    skip_thread.start()

    try:
        assert list_snapshot_taken.wait(timeout=2)
        adapter.release.set()
        assert clear_completed.wait(timeout=2)
        run_thread.join(timeout=0.5)
    finally:
        adapter.release.set()
        release_list_snapshot.set()
        skip_thread.join(timeout=3)
        run_thread.join(timeout=3)

    assert not skip_thread.is_alive()
    assert not run_thread.is_alive()
    assert skip_errors == []
    assert run_errors == []
    assert skip_counts == [0]
    assert len(run_results) == 1
    assert run_results[0].state is BenchmarkState.PASSED
    assert original_list_results() == tuple(run_results)
    assert benchmarks.skip_store.list_for_revision(
        device_id=snapshot.device_id,
        inspection_revision=snapshot.revision,
    ) == frozenset()


def test_expiry_and_dependency_change_require_rerun(tmp_path: Path) -> None:
    now = [NOW]
    clock = lambda: now[0]
    adapter = BoundedBenchmarkAdapter(result_ttl_seconds=5)
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    _post_run(client, csrf)

    now[0] = NOW + timedelta(seconds=6)
    expired = client.get("/onboarding?step=benchmarks").get_data(as_text=True)
    assert "Expired" in expired
    assert "Rerun this check" in expired

    now[0] = NOW + timedelta(seconds=1)
    adapter.revision = 2
    changed = client.get("/onboarding?step=benchmarks").get_data(as_text=True)
    assert "Changed" in changed
    assert "definition or local dependency changed" in changed


def test_active_benchmark_can_be_cancelled_through_flask(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    adapter.block_until_cancelled = True
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    result_holder = []

    def run_in_background() -> None:
        result_holder.append(
            benchmarks.run(
                benchmark_id=BENCHMARK_ID,
                target_service_id=TARGET_ID,
            )
        )

    thread = threading.Thread(target=run_in_background)
    thread.start()
    assert adapter.started.wait(timeout=2)

    cancelled = client.post(
        "/onboarding/benchmarks/cancel",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": TARGET_ID,
        },
    )
    thread.join(timeout=3)

    assert cancelled.status_code == 303
    assert not thread.is_alive()
    assert len(result_holder) == 1
    assert result_holder[0].state is BenchmarkState.CANCELLED


def test_csrf_and_server_owned_inputs_are_rejected(tmp_path: Path) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)

    missing = client.post(
        "/onboarding/benchmarks/run",
        data={"benchmark_id": BENCHMARK_ID, "target_service_id": TARGET_ID},
    )
    override = client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": BENCHMARK_ID,
            "target_service_id": TARGET_ID,
            "run_id": "attacker-run",
            "device_id": "attacker-device",
            "dependency_inputs": "attacker-inputs",
            "timeout_seconds": "999",
        },
    )

    assert missing.status_code == 403
    assert override.status_code == 403
    assert adapter.benchmark_calls == 0
    assert benchmarks.list_results() == ()


def test_corrupt_benchmark_state_fails_closed_without_leaking_bytes(
    tmp_path: Path,
) -> None:
    clock = lambda: NOW
    adapter = BoundedBenchmarkAdapter()
    onboarding = _onboarding_service(tmp_path, clock=clock)
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=(adapter,),
        clock=clock,
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)
    _post_run(client, csrf)

    secret = "http://192.168.55.9:11434/private-benchmark-token"
    with sqlite3.connect(tmp_path / "onboarding.sqlite3") as connection:
        connection.execute(
            "UPDATE benchmark_results SET result_json=?",
            (f"not-json-{secret}",),
        )
        connection.commit()

    response = client.get("/onboarding?step=benchmarks")
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Benchmark evidence unavailable" in page
    assert secret not in page
    assert "192.168.55.9" not in page


def test_default_ollama_benchmark_runs_inference_only_after_explicit_post(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = lambda: NOW
    requests = []

    def requester(method, url, payload, timeout, max_bytes):
        requests.append((method, url, payload, timeout, max_bytes))
        if method == "GET":
            return {"models": [{"name": "qwen2.5:7b"}]}
        assert method == "POST"
        assert payload["model"] == "qwen2.5:7b"
        return {
            "message": {"content": "OK"},
            "eval_count": 2,
            "eval_duration": 1_000_000_000,
        }

    monkeypatch.setattr(
        "catalog.capabilities.benchmarking.adapters.ollama._bounded_json_request",
        requester,
    )
    setup_payload = {
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-workbench",
        "ai_enabled": True,
        "ai_model": "qwen2.5:7b",
        "ollama_base_url": "http://ollama:11434",
    }
    onboarding = _onboarding_service(
        tmp_path,
        clock=clock,
        setup_payload=setup_payload,
    )
    inspection = _inspection_service(
        tmp_path,
        onboarding,
        adapters=None,
        clock=clock,
        setup_payload=setup_payload,
        scan_supplier=lambda: {"state": "never", "results": []},
    )
    benchmarks = _benchmark_service(
        tmp_path,
        onboarding,
        inspection,
        clock=clock,
    )
    client = _app(onboarding, inspection, benchmarks).test_client()
    csrf, _command_id = _connect_and_inspect(client)

    assert [item[0] for item in requests] == ["GET"]
    page_before = client.get("/onboarding?step=benchmarks").get_data(as_text=True)
    assert [item[0] for item in requests] == ["GET"]
    assert "ollama:11434" not in page_before

    response = client.post(
        "/onboarding/benchmarks/run",
        data={
            "_csrf_token": csrf,
            "benchmark_id": "benchmark.ai.ollama-inference.v1",
            "target_service_id": "ollama-configured",
        },
    )
    page_after = client.get("/onboarding?step=benchmarks").get_data(as_text=True)

    assert response.status_code == 303
    assert [item[0] for item in requests] == ["GET", "POST"]
    assert "Recommended" in page_after
    assert "ollama:11434" not in page_after
    assert "qwen2.5:7b" not in page_after

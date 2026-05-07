from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import threading

from flask import Flask

from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ArtifactCatalog, ScanSnapshot
from catalog.orchestrator.pipeline import RuntimeOrchestrator


@dataclass(frozen=True)
class _Scope:
    start_date: str | None = None
    end_date: str | None = None
    updated_at: str | None = None

    @property
    def is_active(self) -> bool:
        return False

    @property
    def label(self) -> str:
        return "No shared scope"


class _ScopeService:
    def get(self) -> _Scope:
        return _Scope()


class _Runtime:
    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, object]:
        return {
            "startup_mode": "continue_existing",
            "current_processing_phase": "historical_catch_up",
            "update_running": True,
            "view_contracts": {},
        }


class _CachedOnlyCatalog:
    scan_dirs = ["results"]

    def __init__(self) -> None:
        self.snapshot = ScanSnapshot(artifacts=[], warnings=[], scanned_at_epoch=123.0)
        self.cached_calls = 0
        self.ensure_calls = 0
        self.rescan_calls = 0

    def cached_snapshot(self, *, log_if_stale: bool = True) -> ScanSnapshot:
        self.cached_calls += 1
        return self.snapshot

    def ensure_scanned(self, *, force_signature_check: bool = False) -> ScanSnapshot:
        self.ensure_calls += 1
        raise AssertionError("page request forced an artifact scan")

    def artifact_by_path(self, path: str, *, cached: bool = True) -> dict[str, object] | None:
        return None

    def rescan(self) -> ScanSnapshot:
        self.rescan_calls += 1
        return self.snapshot


def _app(catalog: object) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = catalog
    app.register_blueprint(web)
    return app


def test_status_route_renders_cached_runtime_and_artifacts(monkeypatch) -> None:
    catalog = _CachedOnlyCatalog()
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _Runtime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _ScopeService())
    app = _app(catalog)

    response = app.test_client().get("/status")

    assert response.status_code == 200
    assert catalog.cached_calls == 1
    assert catalog.ensure_calls == 0
    assert "Historical Catch Up" in response.get_data(as_text=True)


def test_playback_route_does_not_force_full_rescan_with_cached_snapshot(monkeypatch) -> None:
    catalog = _CachedOnlyCatalog()
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _Runtime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _ScopeService())
    app = _app(catalog)

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    assert catalog.cached_calls == 1
    assert catalog.ensure_calls == 0
    assert "No playback-compatible exports" in response.get_data(as_text=True)


def test_explicit_rescan_route_still_forces_catalog_scan(monkeypatch) -> None:
    catalog = _CachedOnlyCatalog()
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _Runtime())
    app = _app(catalog)

    response = app.test_client().post("/rescan", data={"next": "/status"})

    assert response.status_code == 302
    assert response.headers["Location"] == "/status"
    assert catalog.rescan_calls == 1
    assert catalog.ensure_calls == 0


def test_state_snapshot_does_not_call_artifact_scan(monkeypatch, tmp_path: Path) -> None:
    def fail_scan(*args, **kwargs):
        raise AssertionError("state_snapshot must not scan artifacts")

    monkeypatch.setattr("catalog.orchestrator.pipeline.scan_artifacts", fail_scan)
    orchestrator = RuntimeOrchestrator.__new__(RuntimeOrchestrator)
    orchestrator._lock = threading.Lock()
    orchestrator.workflows_root = tmp_path / "workflows"
    orchestrator.workflows_root.mkdir()
    orchestrator._state = RuntimeOrchestrator._default_state(orchestrator)

    snapshot = RuntimeOrchestrator.state_snapshot(orchestrator)

    assert snapshot["current_processing_phase"] == "runtime_not_started"
    assert "view_contracts" in snapshot


def test_catalog_cached_snapshot_does_not_check_signature(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "results"
    root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(root))
    catalog = ArtifactCatalog(cached_snapshot_ttl_seconds=0.0)
    catalog.rescan()

    def fail_signature() -> object:
        raise AssertionError("cached snapshot must not walk scan roots")

    monkeypatch.setattr(catalog, "_scan_root_signature", fail_signature)

    snapshot = catalog.cached_snapshot()

    assert snapshot.artifacts == []

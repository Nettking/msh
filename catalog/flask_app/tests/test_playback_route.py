from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(".").resolve()))

import pandas as pd
from flask import Flask

from catalog.common.artifact_refresh import register_artifact_catalog_refresh, request_artifact_catalog_refresh
from catalog.common.artifact_registry import scan_artifacts
from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ArtifactCatalog, ScanSnapshot
from catalog.orchestrator.pipeline import RuntimeOrchestrator, STARTUP_MODE_CLEAN


class _ActualShapeRuntime:
    def __init__(
        self,
        workflows_root: Path,
        *,
        active_runtime_namespace: str,
        startup_mode: str = STARTUP_MODE_CLEAN,
        session_id: str | None = None,
    ) -> None:
        self._orchestrator = RuntimeOrchestrator.__new__(RuntimeOrchestrator)
        self._orchestrator._lock = threading.Lock()
        self._orchestrator.workflows_root = workflows_root
        self._orchestrator._state = RuntimeOrchestrator._default_state(self._orchestrator)
        self._orchestrator._state.active_runtime_namespace = active_runtime_namespace
        self._orchestrator._state.startup_mode = startup_mode
        self._orchestrator._state.session_id = session_id

    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, object]:
        return RuntimeOrchestrator.state_snapshot(self._orchestrator)


class _FakeRuntime:
    def __init__(
        self,
        active_runtime_namespace: str = "default",
        startup_mode: str = "continue_existing",
        session_id: str | None = None,
    ) -> None:
        self.active_runtime_namespace = active_runtime_namespace
        self.startup_mode = startup_mode
        self.session_id = session_id

    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, str | None]:
        return {
            "active_runtime_namespace": self.active_runtime_namespace,
            "startup_mode": self.startup_mode,
            "session_id": self.session_id,
        }


@dataclass
class _FakeScope:
    start_date: str | None = None
    end_date: str | None = None
    selected_session_id: str | None = None
    updated_at: str | None = None

    @property
    def is_active(self) -> bool:
        return False

    @property
    def label(self) -> str:
        return "No shared scope"


class _FakeScopeService:
    def get(self) -> _FakeScope:
        return _FakeScope()


class _FakeCatalog:
    def __init__(self, artifacts: list[dict[str, object]]) -> None:
        self._snapshot = ScanSnapshot(artifacts=artifacts, warnings=[], scanned_at_epoch=1.0)

    def ensure_scanned(self, *, force_signature_check: bool = False) -> ScanSnapshot:
        return self._snapshot

    def cached_snapshot(self, *, log_if_stale: bool = True) -> ScanSnapshot:
        return self._snapshot

    def artifact_by_path(self, path: str, *, cached: bool = True) -> dict[str, object] | None:
        for artifact in self._snapshot.artifacts:
            if artifact.get("path") == path:
                return artifact
        return None


def _app_with_catalog(catalog) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = catalog
    app.register_blueprint(web)
    return app


def _write_session_metadata(session_dir: Path, namespace: str) -> None:
    (session_dir).mkdir(parents=True, exist_ok=True)
    (session_dir / "session_state.json").write_text(
        json.dumps({"runtime": {"runtime_namespace": namespace}}) + "\n",
        encoding="utf-8",
    )


def _write_timeline(session_dir: Path, day: str, state: str) -> Path:
    export_dir = session_dir / "exports" / "timeline"
    export_dir.mkdir(parents=True, exist_ok=True)
    timeline_path = export_dir / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": [f"{day}T10:00:00Z"],
            "machine_id": ["M1"],
            "state": [state],
        }
    ).to_csv(timeline_path, index=False)
    return timeline_path


def test_playback_route_defaults_to_full_timeline_and_keeps_candidates_flagged(tmp_path: Path, monkeypatch) -> None:
    timeline_path = tmp_path / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": [
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:01Z",
                "2026-03-01T10:00:02Z",
                "2026-03-01T10:00:03Z",
            ],
            "machine_id": ["M1", "M1", "M1", "M1"],
            "state": ["active", "dense_idle", "idle", "intervention_candidate"],
            "intervention_candidate": [False, False, False, True],
        }
    ).to_csv(timeline_path, index=False)

    candidate_path = tmp_path / "candidate_events.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:03Z"],
            "machine_id": ["M1"],
            "state": ["intervention_candidate"],
            "intervention_candidate": [True],
        }
    ).to_csv(candidate_path, index=False)

    artifacts, warnings = scan_artifacts([str(tmp_path)])
    assert warnings == []
    assert {artifact["file_name"]: artifact["playback_compatible"] for artifact in artifacts} == {
        "candidate_events.csv": False,
        "timeline_rows.csv": True,
    }

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "<strong>Machine:</strong> M1" in body
    assert "candidate_events.csv" not in body
    assert "active" in body
    assert "dense_idle" in body
    assert "idle" in body
    assert "intervention_candidate" in body
    assert '"intervention_candidate": true' in body


def test_playback_route_lists_workflow_timeline_exports(tmp_path: Path, monkeypatch) -> None:
    workflow_export_dir = tmp_path / "results" / "workflows" / "session-123" / "exports" / "timeline"
    workflow_export_dir.mkdir(parents=True)
    timeline_path = workflow_export_dir / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    artifacts, warnings = scan_artifacts([str(tmp_path / "results")])
    assert warnings == []
    assert [artifact["path"] for artifact in artifacts if artifact["playback_compatible"]] == [str(timeline_path)]

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "<strong>Machine:</strong> M1" in body


def test_playback_page_does_not_render_visible_dataset_selector(tmp_path: Path, monkeypatch) -> None:
    timeline_path = tmp_path / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    artifacts, warnings = scan_artifacts([str(tmp_path)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'select name="path"' not in body
    assert "<strong>Dataset:</strong>" not in body
    assert "<strong>Machine:</strong> M1" in body
    assert "<strong>Day:</strong> 2026-03-01" in body


def test_playback_machine_day_selection_chooses_matching_timeline_artifact(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    day1_session = results_root / "workflows" / "session-day1"
    day2_session = results_root / "workflows" / "session-day2"
    _write_timeline(day1_session, "2026-03-01", "day1_state")
    _write_timeline(day2_session, "2026-03-02", "day2_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"machine": "M1", "day": "2026-03-02"})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "day2_state" in body
    assert "day1_state" not in body
    assert "<strong>Day:</strong> 2026-03-02" in body


def test_playback_loads_only_selected_artifact_after_machine_day_resolution(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    day1_session = results_root / "workflows" / "session-day1"
    day2_session = results_root / "workflows" / "session-day2"
    day1_path = _write_timeline(day1_session, "2026-03-01", "day1_state")
    day2_path = _write_timeline(day2_session, "2026-03-02", "day2_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []
    load_calls: list[str] = []

    def tracking_load(path: str):
        load_calls.append(path)
        return pd.read_csv(path), None

    monkeypatch.setattr("catalog.flask_app.routes.load_playback_frame", tracking_load)
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"machine": "M1", "day": "2026-03-02"})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "day2_state" in body
    assert "day1_state" not in body
    assert load_calls == [str(day2_path)]
    assert str(day1_path) not in load_calls


def test_playback_combines_days_from_multiple_timeline_artifacts(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    _write_timeline(results_root / "workflows" / "session-day1", "2026-03-01", "day1_state")
    _write_timeline(results_root / "workflows" / "session-day2", "2026-03-02", "day2_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert '<option value="2026-03-01" selected>' in body
    assert '<option value="2026-03-02"' in body
    assert 'select name="path"' not in body


def test_playback_prefers_active_namespace_artifact_for_duplicate_machine_day(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    stale_session = results_root / "workflows" / "stale-session"
    active_session = results_root / "workflows" / "active-session"
    _write_session_metadata(stale_session, "old_namespace")
    _write_session_metadata(active_session, "active_namespace")
    _write_timeline(stale_session, "2026-03-01", "stale_state")
    _write_timeline(active_session, "2026-03-01", "active_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("active_namespace"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"machine": "M1", "day": "2026-03-01"})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "active_state" in body
    assert "stale_state" not in body


def test_playback_route_uses_async_catalog_refresh_when_timeline_export_appears(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    results_root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(results_root))

    catalog = ArtifactCatalog()
    assert catalog.ensure_scanned().artifacts == []

    timeline_path = results_root / "workflows" / "session-123" / "exports" / "timeline" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(catalog)

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" in body
    assert "<strong>Machine:</strong> M1" not in body

    try:
        register_artifact_catalog_refresh(lambda reason: catalog.start_background_rescan_if_idle(reason=reason))
        assert request_artifact_catalog_refresh(reason="playback_export_generated") is True
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not catalog.cached_snapshot(log_if_stale=False).artifacts:
            time.sleep(0.01)
    finally:
        register_artifact_catalog_refresh(None)

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "<strong>Machine:</strong> M1" in body
    assert "active" in body


def test_clean_startup_playback_hides_stale_workflow_exports_but_scan_still_finds_them(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    stale_session = results_root / "workflows" / "auto_default_20260301_20260301"
    current_session = results_root / "workflows" / "auto_clean_20260302_20260302"
    _write_session_metadata(stale_session, "default")
    _write_session_metadata(current_session, "clean_20260302T000000Z")
    stale_path = _write_timeline(stale_session, "2026-03-01", "stale_state")
    current_path = _write_timeline(current_session, "2026-03-02", "current_state")
    raw_data = tmp_path / "data" / "raw.jsonl"
    raw_data.parent.mkdir()
    raw_data.write_text('{"timestamp":"2026-03-01T00:00:00Z"}\n', encoding="utf-8")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []
    playback_paths = {artifact["path"] for artifact in artifacts if artifact["playback_compatible"]}
    assert playback_paths == {str(stale_path), str(current_path)}

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "current_state" in body
    assert "2026-03-02" in body
    assert "stale_state" not in body
    assert "2026-03-01" not in body
    assert raw_data.exists()
    assert raw_data.read_text(encoding="utf-8") == '{"timestamp":"2026-03-01T00:00:00Z"}\n'


def test_playback_explicit_stale_workflow_path_does_not_bypass_namespace_filter(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    stale_session = results_root / "workflows" / "auto_default_20260301_20260301"
    current_session = results_root / "workflows" / "auto_clean_20260302_20260302"
    _write_session_metadata(stale_session, "default")
    _write_session_metadata(current_session, "clean_20260302T000000Z")
    stale_path = _write_timeline(stale_session, "2026-03-01", "stale_selected_state")
    _write_timeline(current_session, "2026-03-02", "current_selected_fallback_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"path": str(stale_path)})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "stale_selected_state" not in body
    assert "current_selected_fallback_state" in body


def test_clean_startup_lists_all_current_runtime_workflow_timeline_exports(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    active_namespace = "clean:2026-05-07T00:00:00Z"
    safe_namespace = "clean_2026-05-07T00_00_00Z"
    older_session = results_root / "workflows" / f"auto_{safe_namespace}_20260503_20260503"
    latest_session = results_root / "workflows" / f"auto_{safe_namespace}_20260504_20260504"
    stale_session = results_root / "workflows" / "auto_default_20260502_20260502"
    _write_session_metadata(older_session, active_namespace)
    _write_session_metadata(latest_session, active_namespace)
    _write_session_metadata(stale_session, "default")
    older_path = _write_timeline(older_session, "2026-05-03", "current_runtime_older_state")
    latest_path = _write_timeline(latest_session, "2026-05-04", "current_runtime_latest_state")
    stale_path = _write_timeline(stale_session, "2026-05-02", "stale_default_namespace_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []
    assert {artifact["path"] for artifact in artifacts if artifact["playback_compatible"]} == {
        str(older_path),
        str(latest_path),
        str(stale_path),
    }

    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager",
        lambda: _FakeRuntime("default", "start_clean", latest_session.name),
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert str(older_path) in body
    assert '<option value="2026-05-04"' in body
    assert str(stale_path) not in body
    assert "stale_default_namespace_state" not in body


def test_clean_startup_actual_runtime_snapshot_lists_all_current_namespace_workflow_exports(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    active_namespace = "clean:2026-05-07T00:00:00Z"
    safe_namespace = "clean_2026-05-07T00_00_00Z"
    older_session = results_root / "workflows" / f"auto_{safe_namespace}_20260503_20260503"
    latest_session = results_root / "workflows" / f"auto_{safe_namespace}_20260504_20260504"
    stale_session = results_root / "workflows" / "auto_default_20260502_20260502"
    _write_session_metadata(older_session, "default")
    _write_session_metadata(latest_session, active_namespace)
    _write_session_metadata(stale_session, "default")
    older_path = _write_timeline(older_session, "2026-05-03", "actual_runtime_older_state")
    latest_path = _write_timeline(latest_session, "2026-05-04", "actual_runtime_latest_state")
    stale_path = _write_timeline(stale_session, "2026-05-02", "actual_runtime_stale_default_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager",
        lambda: _ActualShapeRuntime(
            results_root / "workflows",
            active_runtime_namespace=active_namespace,
            session_id=latest_session.name,
        ),
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert str(older_path) in body
    assert '<option value="2026-05-04"' in body
    assert str(stale_path) not in body
    assert "actual_runtime_stale_default_state" not in body


def test_clean_startup_can_load_explicit_older_current_runtime_workflow_export(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    active_namespace = "clean:2026-05-07T00:00:00Z"
    safe_namespace = "clean_2026-05-07T00_00_00Z"
    older_session = results_root / "workflows" / f"auto_{safe_namespace}_20260503_20260503"
    latest_session = results_root / "workflows" / f"auto_{safe_namespace}_20260504_20260504"
    stale_session = results_root / "workflows" / "auto_default_20260502_20260502"
    _write_session_metadata(older_session, active_namespace)
    _write_session_metadata(latest_session, active_namespace)
    _write_session_metadata(stale_session, "default")
    older_path = _write_timeline(older_session, "2026-05-03", "current_runtime_older_state")
    latest_path = _write_timeline(latest_session, "2026-05-04", "current_runtime_latest_state")
    stale_path = _write_timeline(stale_session, "2026-05-02", "stale_default_namespace_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager",
        lambda: _FakeRuntime(active_namespace, "start_clean", latest_session.name),
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"path": str(older_path)})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert str(older_path) in body
    assert '<option value="2026-05-04"' in body
    assert str(stale_path) not in body
    assert "current_runtime_older_state" in body
    assert "2026-05-03" in body
    assert "current_runtime_latest_state" not in body
    assert "stale_default_namespace_state" not in body


def test_reuse_startup_playback_can_list_default_namespace_exports(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    session_dir = results_root / "workflows" / "auto_default_20260301_20260301"
    _write_session_metadata(session_dir, "default")
    timeline_path = _write_timeline(session_dir, "2026-03-01", "reused_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []
    assert [artifact["path"] for artifact in artifacts if artifact["playback_compatible"]] == [str(timeline_path)]

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("default"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "reused_state" in body
    assert "2026-03-01" in body


def test_playback_route_lists_current_clean_workflow_export_with_default_metadata_namespace(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    session_id = "auto_clean_20260302T000000Z_20260302_20260302"
    session_dir = results_root / "workflows" / session_id
    _write_session_metadata(session_dir, "default")
    _write_timeline(session_dir, "2026-03-02", "current_default_metadata_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager",
        lambda: _ActualShapeRuntime(
            results_root / "workflows",
            active_runtime_namespace="clean_20260302T000000Z",
            session_id=session_id,
        ),
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "current_default_metadata_state" in body
    assert "2026-03-02" in body
    assert "<strong>Machine:</strong> M1" in body


def test_playback_selected_dataset_is_not_empty_for_current_clean_workflow_export_with_missing_namespace(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    session_id = "auto_clean_20260302T000000Z_20260302_20260302"
    session_dir = results_root / "workflows" / session_id
    session_dir.mkdir(parents=True)
    (session_dir / "session_state.json").write_text(json.dumps({"runtime": {}}) + "\n", encoding="utf-8")
    _write_timeline(session_dir, "2026-03-02", "current_missing_namespace_state")

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr(
        "catalog.flask_app.routes.get_runtime_manager",
        lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean", session_id),
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "current_missing_namespace_state" in body
    assert "2026-03-02" in body
    assert "<strong>Machine:</strong> M1" in body


def test_clean_startup_prefers_current_workflow_export_over_stale_non_workflow_timeline(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    current_session = results_root / "workflows" / "auto_clean_20260302_20260302"
    _write_session_metadata(current_session, "clean_20260302T000000Z")
    current_path = _write_timeline(current_session, "2026-03-02", "current_workflow_state")

    non_workflow_path = results_root / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["stale_non_workflow_state"],
        }
    ).to_csv(non_workflow_path, index=False)

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []
    playback_paths = {artifact["path"] for artifact in artifacts if artifact["playback_compatible"]}
    assert playback_paths == {str(current_path), str(non_workflow_path)}

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "current_workflow_state" in body
    assert "2026-03-02" in body
    assert "stale_non_workflow_state" not in body
    assert "2026-03-01" not in body


def test_clean_startup_can_load_explicit_non_workflow_path_when_current_workflow_exists(
    tmp_path: Path, monkeypatch
) -> None:
    results_root = tmp_path / "results"
    current_session = results_root / "workflows" / "auto_clean_20260302_20260302"
    _write_session_metadata(current_session, "clean_20260302T000000Z")
    _write_timeline(current_session, "2026-03-02", "current_workflow_state")

    non_workflow_path = results_root / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["manual_non_workflow_state"],
        }
    ).to_csv(non_workflow_path, index=False)

    artifacts, warnings = scan_artifacts([str(results_root)])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"path": str(non_workflow_path)})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "manual_non_workflow_state" in body
    assert "2026-03-01" in body
    assert "current_workflow_state" not in body


def test_clean_startup_keeps_non_workflow_timeline_visible_because_it_has_no_runtime_namespace(tmp_path: Path, monkeypatch) -> None:
    timeline_path = tmp_path / "results" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["stale_non_workflow_state"],
        }
    ).to_csv(timeline_path, index=False)

    artifacts, warnings = scan_artifacts([str(tmp_path / "results")])
    assert warnings == []
    assert [artifact["path"] for artifact in artifacts if artifact["playback_compatible"]] == [str(timeline_path)]

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "stale_non_workflow_state" in body
    assert "<strong>Machine:</strong> M1" in body


def test_clean_startup_can_load_explicit_non_workflow_playback_path(tmp_path: Path, monkeypatch) -> None:
    timeline_path = tmp_path / "results" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["manual_non_workflow_state"],
        }
    ).to_csv(timeline_path, index=False)

    artifacts, warnings = scan_artifacts([str(tmp_path / "results")])
    assert warnings == []

    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime("clean_20260302T000000Z", "start_clean"))
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    app = _app_with_catalog(_FakeCatalog(artifacts))

    response = app.test_client().get("/playback", query_string={"path": str(timeline_path)})

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "No playback-compatible exports" not in body
    assert "manual_non_workflow_state" in body
    assert "<strong>Machine:</strong> M1" in body

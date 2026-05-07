from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(".").resolve()))

import pandas as pd
from flask import Flask

from catalog.common.artifact_registry import scan_artifacts
from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ScanSnapshot


class _FakeRuntime:
    def __init__(self, active_runtime_namespace: str = "default", startup_mode: str = "continue_existing") -> None:
        self.active_runtime_namespace = active_runtime_namespace
        self.startup_mode = startup_mode

    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, str]:
        return {"active_runtime_namespace": self.active_runtime_namespace, "startup_mode": self.startup_mode}


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

    def ensure_scanned(self) -> ScanSnapshot:
        return self._snapshot

    def artifact_by_path(self, path: str) -> dict[str, object] | None:
        for artifact in self._snapshot.artifacts:
            if artifact.get("path") == path:
                return artifact
        return None


def _app_with_catalog(catalog: _FakeCatalog) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = catalog
    app.register_blueprint(web)
    return app


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
    assert "<strong>Dataset:</strong> timeline_rows.csv" in body
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
    assert "<strong>Dataset:</strong> timeline_rows.csv" in body


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


def test_clean_startup_does_not_automatically_select_stale_non_workflow_timeline(tmp_path: Path, monkeypatch) -> None:
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
    assert "No playback-compatible exports" in body
    assert "stale_non_workflow_state" not in body
    assert "<strong>Dataset:</strong> timeline_rows.csv" not in body


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
    assert "<strong>Dataset:</strong> timeline_rows.csv" in body

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(".").resolve()))

from flask import Flask

from catalog.flask_app.routes import web
from catalog.flask_app.services.control_service import ControlPanelService
from catalog.runner.script_catalog import ScriptOption
from catalog.runner.session_store import SessionInfo, initialize_session_metadata, write_session_metadata


class _FakeRuntime:
    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, object]:
        return {
            "active_runtime_namespace": "test",
            "startup_mode": "clean_start",
            "session_id": None,
            "current_processing_phase": "idle",
            "update_running": False,
            "last_failure": None,
            "last_completed_step": None,
            "last_completed_date": None,
        }


@dataclass
class _FakeScope:
    start_date: str | None = None
    end_date: str | None = None
    selected_session_id: str | None = None
    updated_at: str | None = None

    @property
    def label(self) -> str:
        return "No shared scope"


class _FakeScopeService:
    def get(self) -> _FakeScope:
        return _FakeScope()


class _FakeCache:
    def __init__(self, panel: dict[str, object]) -> None:
        self.panel = panel

    def get_control_snapshot(self, *, selected_session_id: str | None = None):
        return self.panel, "test"

    def invalidate_all(self) -> None:
        return None


class _CaptureControlService:
    def __init__(self) -> None:
        self.calls: list[dict[str, str | None]] = []

    def trigger_action(self, action, *, script_key=None, selected_session_id=None, scope_mode=None, start_date=None, end_date=None):
        self.calls.append(
            {
                "action": action,
                "script_key": script_key,
                "selected_session_id": selected_session_id,
                "scope_mode": scope_mode,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        return True, "captured", selected_session_id or "manual_20260301_20260302"


class _FakeSessionIndex:
    def invalidate(self, workflows_root=None) -> None:
        return None


SCRIPT_OPTIONS = [
    ScriptOption(number=1, key="machines_active_per_day", script_path=Path("x.py"), description="Machines Active", category="Simple"),
    ScriptOption(number=2, key="data_visualizer", script_path=Path("y.py"), description="Visualizer", category="Simple"),
]


def _make_app(monkeypatch, panel: dict[str, object], control_service=None) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.register_blueprint(web)
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_page_cache", lambda: _FakeCache(panel))
    if control_service is not None:
        monkeypatch.setattr("catalog.flask_app.routes.get_control_panel_service", lambda: control_service)
        monkeypatch.setattr("catalog.flask_app.routes.get_workflow_session_index", lambda: _FakeSessionIndex())
    return app


def _metadata(session_id: str, start: str, end: str, *, updated_at: str, output: bool = False) -> dict[str, object]:
    metadata = initialize_session_metadata(
        session_id,
        date.fromisoformat(start),
        date.fromisoformat(end),
        start_hour=None,
        end_hour=None,
        script_options=SCRIPT_OPTIONS,
    )
    metadata["updated_at"] = updated_at
    metadata["filter_result"]["matched_records"] = 10
    metadata["filter_result"]["generated_at"] = updated_at
    if output:
        metadata["scripts"]["data_visualizer"]["status"] = "done"
        metadata["scripts"]["data_visualizer"]["output_path"] = "exports/timeline/timeline_rows.csv"
        metadata["scripts"]["data_visualizer"]["last_run_at"] = updated_at
    return metadata


def _session(tmp_path: Path, session_id: str, start: str, end: str, *, updated_at: str, output: bool = False) -> SessionInfo:
    session_dir = tmp_path / session_id
    session_dir.mkdir(parents=True)
    metadata = _metadata(session_id, start, end, updated_at=updated_at, output=output)
    write_session_metadata(session_dir, metadata)
    (session_dir / "data").mkdir(exist_ok=True)
    return SessionInfo(session_id=session_id, session_dir=session_dir, metadata=metadata)


def _panel(tmp_path: Path, selected_session_id: str | None, sessions: list[SessionInfo]) -> dict[str, object]:
    service = ControlPanelService.__new__(ControlPanelService)
    service._script_options = SCRIPT_OPTIONS
    service._lock = __import__("threading").Lock()
    service._active_run_id = None
    service._recent_runs = []
    service._available_date_bounds = lambda: ("2026-03-01", "2026-03-05")
    return service.snapshot(selected_session_id=selected_session_id, runtime_state=_FakeRuntime().state_snapshot(), sessions=sessions)


def test_control_renders_with_no_processed_datasets(tmp_path: Path, monkeypatch) -> None:
    app = _make_app(monkeypatch, _panel(tmp_path, None, []))

    response = app.test_client().get("/control")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Processed datasets & analysis workspaces" in body
    assert "No processed datasets are available yet" in body
    assert "No processed dataset is selected" in body


def test_control_renders_multiple_processed_datasets(tmp_path: Path, monkeypatch) -> None:
    sessions = [
        _session(tmp_path, "dataset_b", "2026-03-03", "2026-03-05", updated_at="2026-03-05T12:00:00Z", output=True),
        _session(tmp_path, "dataset_a", "2026-03-01", "2026-03-02", updated_at="2026-03-02T12:00:00Z"),
    ]
    app = _make_app(monkeypatch, _panel(tmp_path, None, sessions))

    body = app.test_client().get("/control").get_data(as_text=True)

    assert "Processed datasets" in body
    assert "dataset_a" in body
    assert "dataset_b" in body
    assert "2026-03-03..2026-03-05" in body
    assert "Playback" in body
    assert "Dataset/session" not in body
    assert "Dataset id" in body


def test_selecting_dataset_query_parameter_marks_it_selected(tmp_path: Path, monkeypatch) -> None:
    sessions = [
        _session(tmp_path, "dataset_b", "2026-03-03", "2026-03-05", updated_at="2026-03-05T12:00:00Z"),
        _session(tmp_path, "dataset_a", "2026-03-01", "2026-03-02", updated_at="2026-03-02T12:00:00Z"),
    ]
    app = _make_app(monkeypatch, _panel(tmp_path, "dataset_a", sessions))

    body = app.test_client().get("/control?session_id=dataset_a").get_data(as_text=True)

    assert '<span class="metric-value metric-value--long">dataset_a</span>' in body
    assert 'value="dataset_a"' in body
    assert 'value="dataset_b"' not in body


def test_action_forms_use_visible_selected_dataset_id(tmp_path: Path, monkeypatch) -> None:
    sessions = [_session(tmp_path, "visible_dataset", "2026-03-01", "2026-03-02", updated_at="2026-03-02T12:00:00Z")]
    app = _make_app(monkeypatch, _panel(tmp_path, "visible_dataset", sessions))

    body = app.test_client().get("/control?session_id=visible_dataset").get_data(as_text=True)

    assert "Selected dataset" in body
    assert body.count('name="selected_session_id" value="visible_dataset"') >= 4
    assert 'Run workflow on latest processed dataset (explicit latest action)' in body


def test_invalid_selected_dataset_id_shows_warning(tmp_path: Path, monkeypatch) -> None:
    sessions = [_session(tmp_path, "dataset_a", "2026-03-01", "2026-03-02", updated_at="2026-03-02T12:00:00Z")]
    app = _make_app(monkeypatch, _panel(tmp_path, "missing_dataset", sessions))

    body = app.test_client().get("/control?session_id=missing_dataset").get_data(as_text=True)

    assert "Selected dataset was not found" in body
    assert "No processed dataset is selected" in body


def test_latest_dataset_action_is_separate_from_selected_dataset_action(tmp_path: Path, monkeypatch) -> None:
    sessions = [_session(tmp_path, "dataset_a", "2026-03-01", "2026-03-02", updated_at="2026-03-02T12:00:00Z")]
    capture = _CaptureControlService()
    app = _make_app(monkeypatch, _panel(tmp_path, "dataset_a", sessions), capture)

    response = app.test_client().post("/control/action", data={"action": "rerun_latest_session_workflow"})

    assert response.status_code == 302
    assert capture.calls[-1]["action"] == "rerun_latest_session_workflow"
    assert capture.calls[-1]["selected_session_id"] is None


def test_create_reuse_date_range_dataset_still_submits_custom_range(tmp_path: Path, monkeypatch) -> None:
    capture = _CaptureControlService()
    app = _make_app(monkeypatch, _panel(tmp_path, None, []), capture)

    response = app.test_client().post(
        "/control/action",
        data={
            "action": "run_selected_session_workflow",
            "scope_mode": "custom_range",
            "start_date": "2026-03-01",
            "end_date": "2026-03-02",
        },
    )

    assert response.status_code == 302
    assert capture.calls[-1] == {
        "action": "run_selected_session_workflow",
        "script_key": None,
        "selected_session_id": None,
        "scope_mode": "custom_range",
        "start_date": "2026-03-01",
        "end_date": "2026-03-02",
    }

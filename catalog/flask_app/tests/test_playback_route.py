from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

sys.path.insert(0, str(Path(".").resolve()))

import pandas as pd
from flask import Flask

from catalog.common.artifact_registry import scan_artifacts
from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ScanSnapshot


class _FakeRuntime:
    def requires_startup_choice(self) -> bool:
        return False


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

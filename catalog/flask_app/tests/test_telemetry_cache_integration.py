from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from flask import Flask

sys.path.insert(0, str(Path(".").resolve()))

from catalog.common.telemetry_cache import rebuild_cache
from catalog.flask_app import routes as flask_routes
from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ScanSnapshot
from catalog.flask_app.services.live_service import LiveTelemetryService


@dataclass
class _FakeCatalog:
    artifacts: list[dict[str, object]]

    def cached_snapshot(self, *args, **kwargs) -> ScanSnapshot:
        return ScanSnapshot(artifacts=self.artifacts, warnings=[], scanned_at_epoch=1.0)

    def artifact_by_path(self, path: str, *, cached: bool = True) -> dict[str, object] | None:
        for artifact in self.artifacts:
            if artifact.get("path") == path:
                return artifact
        return None


def _artifact(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "signature": "sig",
        "category": "source_data",
        "status": "ready",
        "modified_at": "2026-03-23T07:00:00",
    }


def test_live_service_uses_duckdb_cache_when_fresh(tmp_path: Path, caplog) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-03-23T07:00:00Z", "machine": "M1", "execution": "READY", "Srpm": 10}),
                json.dumps({"timestamp": "2026-03-23T07:00:01Z", "machine": "M1", "execution": "ACTIVE", "Srpm": 20}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rebuild_cache(data_dir)

    service = LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=999999, rows_per_machine=5)
    with caplog.at_level(logging.INFO):
        snapshot = service.snapshot(_FakeCatalog([_artifact(source)]))

    machine = next(item for item in snapshot.machines if item["machine"] == "M1")
    assert machine["values"]["execution"] == "ACTIVE"
    assert "using DuckDB/Parquet cache" in caplog.text
    assert "JSONL fallback" not in caplog.text


def test_live_service_falls_back_to_jsonl_when_cache_missing(tmp_path: Path, caplog) -> None:
    source = tmp_path / "data" / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps({"timestamp": "2026-03-23T07:00:01Z", "machine": "M1", "execution": "STOPPED"}) + "\n",
        encoding="utf-8",
    )

    service = LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=999999, rows_per_machine=5)
    with caplog.at_level(logging.INFO):
        snapshot = service.snapshot(_FakeCatalog([_artifact(source)]))

    machine = next(item for item in snapshot.machines if item["machine"] == "M1")
    assert machine["values"]["execution"] == "STOPPED"
    assert "JSONL fallback" in caplog.text


class _FakeRuntime:
    def requires_startup_choice(self) -> bool:
        return False

    def state_snapshot(self) -> dict[str, object]:
        return {"startup_mode": "continue_existing"}


class _FakeScope:
    start_date = None
    end_date = None
    selected_session_id = None
    updated_at = None

    @property
    def is_active(self) -> bool:
        return False

    @property
    def label(self) -> str:
        return "No shared scope"


class _FakeScopeService:
    def get(self) -> _FakeScope:
        return _FakeScope()


def _app_with_catalog(catalog: _FakeCatalog) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = catalog
    app.register_blueprint(web)
    return app


def test_playback_route_can_load_timeline_from_fresh_telemetry_cache(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "2026-03-24.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-03-24T07:00:00Z", "machine": "M1", "execution": "READY", "Srpm": 0}),
                json.dumps({"timestamp": "2026-03-24T07:00:01Z", "machine": "M1", "execution": "ACTIVE", "Srpm": 900}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rebuild_cache(data_dir)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())

    app = _app_with_catalog(_FakeCatalog([]))
    response = app.test_client().get("/playback?path=telemetry-cache://timeline&machine=M1&day=2026-03-24")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "telemetry-cache://timeline" in body
    assert "M1" in body
    assert "ACTIVE" in body


def test_playback_route_first_load_discovers_and_loads_telemetry_cache_defaults(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "2026-03-26.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-03-26T07:00:00Z", "machine": "M3", "execution": "READY", "Srpm": 0}),
                json.dumps({"timestamp": "2026-03-26T07:00:01Z", "machine": "M3", "execution": "ACTIVE", "Srpm": 1200}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rebuild_cache(data_dir)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())

    app = _app_with_catalog(_FakeCatalog([]))
    cached_load_calls: list[tuple[str, str]] = []
    original_cached_loader = flask_routes.load_cached_playback_frame_for_machine_day

    def _unexpected_full_cache_load(path: str):
        raise AssertionError(f"first-load playback should resolve machine/day before loading {path}")

    def _record_cached_machine_day_load(machine_id: str, day: str):
        cached_load_calls.append((machine_id, day))
        return original_cached_loader(machine_id, day)

    monkeypatch.setattr(flask_routes, "load_playback_frame", _unexpected_full_cache_load)
    monkeypatch.setattr(flask_routes, "load_cached_playback_frame_for_machine_day", _record_cached_machine_day_load)

    with caplog.at_level(logging.INFO):
        response = app.test_client().get("/playback")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "telemetry-cache://timeline" in body
    assert "<strong>Machine:</strong> M3" in body
    assert "<strong>Day:</strong> 2026-03-26" in body
    assert "ACTIVE" in body
    assert cached_load_calls == [("M3", "2026-03-26")]
    assert "playback using DuckDB/Parquet cache machine=M3 day=2026-03-26" in caplog.text


def test_exploration_route_can_filter_from_fresh_telemetry_cache(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "2026-03-25.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        json.dumps({"timestamp": "2026-03-25T07:00:00Z", "machine": "M2", "execution": "ACTIVE", "Srpm": 700}) + "\n",
        encoding="utf-8",
    )
    rebuild_cache(data_dir)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_operator_scope_service", lambda: _FakeScopeService())

    app = _app_with_catalog(_FakeCatalog([]))
    response = app.test_client().get("/exploration?path=telemetry-cache://samples&window_start=2026-03-25&window_end=2026-03-25")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Telemetry analytics cache" in body
    assert "M2" in body
    assert "ACTIVE" in body

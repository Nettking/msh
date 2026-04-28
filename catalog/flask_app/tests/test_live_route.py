from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask

from catalog.flask_app.routes import web
from catalog.flask_app.services.catalog_service import ScanSnapshot
from catalog.flask_app.services.live_service import LiveTelemetryService


class _FakeRuntime:
    def requires_startup_choice(self) -> bool:
        return False


class _FakeCatalog:
    def __init__(self, artifacts: list[dict[str, object]]) -> None:
        self._snapshot = ScanSnapshot(artifacts=artifacts, warnings=[], scanned_at_epoch=1.0)

    def ensure_scanned(self) -> ScanSnapshot:
        return self._snapshot


def _app_with_catalog(catalog: _FakeCatalog) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "test"
    app.config["ARTIFACT_CATALOG"] = catalog
    app.register_blueprint(web)
    return app


def _artifact(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "signature": "sig",
        "category": "source_data",
        "status": "ready",
        "modified_at": "2026-03-23T07:00:00",
    }


def test_live_route_renders_with_no_data(monkeypatch) -> None:
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_live_telemetry_service", lambda: LiveTelemetryService(refresh_ttl_seconds=0))
    app = _app_with_catalog(_FakeCatalog([]))

    response = app.test_client().get("/live")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Live analysis (recorded stream)" in body
    assert "Latest recorded telemetry per machine" in body
    assert "not a direct machine connection check" in body
    assert body.count("no data") >= 1
    assert "Recent candidate events" in body
    assert "No recent candidate events detected" in body


def test_live_route_renders_with_one_machine(tmp_path: Path, monkeypatch) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = tmp_path / "data" / "QuickTurn" / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": (now).isoformat().replace("+00:00", "Z"), "machine": "QuickTurn", "execution": "READY", "mode": "AUTO", "Srpm": 0, "Sload": 0}),
                json.dumps({"timestamp": (now).isoformat().replace("+00:00", "Z"), "machine": "QuickTurn", "execution": "ACTIVE", "mode": "AUTO", "Srpm": 550, "Sload": 15}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_live_telemetry_service", lambda: LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=999999))

    app = _app_with_catalog(_FakeCatalog([_artifact(source)]))
    response = app.test_client().get("/live")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "QuickTurn" in body
    assert "ACTIVE" in body
    assert ("running/active" in body) or ("intervention_candidate" in body) or ("idle" in body)
    assert "Status" in body
    assert "Inferred state" in body


def test_live_route_renders_with_multiple_machines(tmp_path: Path, monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    source = tmp_path / "data" / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    rows = [
        {"timestamp": (now.replace(microsecond=0)).isoformat().replace("+00:00", "Z"), "machine": "IG500", "execution": "READY"},
        {"timestamp": (now.replace(microsecond=0)).isoformat().replace("+00:00", "Z"), "machine": "VTC", "execution": "ACTIVE"},
        {"timestamp": (now.replace(microsecond=0)).isoformat().replace("+00:00", "Z"), "machine": "QuickTurn", "execution": "STOPPED"},
    ]
    source.write_text("\n".join(json.dumps(item) for item in rows) + "\n", encoding="utf-8")
    monkeypatch.setattr("catalog.flask_app.routes.get_runtime_manager", lambda: _FakeRuntime())
    monkeypatch.setattr("catalog.flask_app.routes.get_live_telemetry_service", lambda: LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=999999))

    app = _app_with_catalog(_FakeCatalog([_artifact(source)]))
    response = app.test_client().get("/live")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "QuickTurn" in body
    assert "IG500" in body
    assert "VTC" in body
    assert "STOPPED" in body
    assert "READY" in body
    assert "ACTIVE" in body
    assert "Recent candidate events" in body

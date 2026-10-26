from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask

from catalog.flask_app.auth.policy import permission_for
from catalog.flask_app.digital_twin_routes import digital_twin_web
from catalog.flask_app.routes import web
from catalog.flask_app.services.digital_twin_service import DigitalTwinDataError


def _write_records(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )


def _test_app(data_root: Path) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = "digital-twin-test"
    app.config["FCP_DATA_ROOT"] = str(data_root)
    app.context_processor(
        lambda: {"capability_startup_flags": {"completed": True, "workbench": True}}
    )
    app.register_blueprint(web)
    app.register_blueprint(digital_twin_web)
    return app


def test_digital_twin_route_renders_empty_state_and_is_read_only(
    tmp_path: Path,
) -> None:
    app = _test_app(tmp_path / "data")

    response = app.test_client().get("/digital-twin")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Digital twin" in body
    assert "No machine telemetry found" in body
    assert permission_for("digital_twin_web.index", "GET") == "data.read"
    rule = next(
        rule
        for rule in app.url_map.iter_rules()
        if rule.endpoint == "digital_twin_web.index"
    )
    assert "GET" in rule.methods
    assert "POST" not in rule.methods


def test_digital_twin_route_rejects_recovery_outside_five_point_steps(
    tmp_path: Path,
) -> None:
    app = _test_app(tmp_path / "data")

    response = app.test_client().get("/digital-twin?recovery_percent=2.5")

    assert response.status_code == 400
    assert "in 5-point steps" in response.get_data(as_text=True)


def test_digital_twin_route_explains_when_telemetry_cannot_be_loaded(
    tmp_path: Path, monkeypatch
) -> None:
    class _UnavailableService:
        def analyze(self, **_kwargs):
            raise DigitalTwinDataError("The local telemetry corpus exceeds the limit.")

    monkeypatch.setattr(
        "catalog.flask_app.digital_twin_routes.get_digital_twin_service",
        lambda _root: _UnavailableService(),
    )
    app = _test_app(tmp_path / "data")

    response = app.test_client().get("/digital-twin")
    body = response.get_data(as_text=True)

    assert response.status_code == 503
    assert "Local telemetry could not be loaded" in body
    assert "No machine telemetry found" not in body


def test_digital_twin_route_renders_scenario_and_monitor_navigation(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    first = datetime(2025, 6, 1, tzinfo=timezone.utc)
    _write_records(
        data_root / "VTC" / "samples.jsonl",
        [
            {
                "timestamp": (first + timedelta(seconds=index))
                .isoformat()
                .replace("+00:00", "Z"),
                "machine": "VTC",
                "execution": "ACTIVE" if index % 2 == 0 else "STOPPED",
            }
            for index in range(31)
        ],
    )
    app = _test_app(data_root)

    response = app.test_client().get(
        "/digital-twin?machine_id=VTC&start_date=2025-06-01&end_date=2025-06-01&recovery_percent=50"
    )

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "If 50% of non-active time were recovered" in body
    assert "Simulated recovered time" in body
    assert "Telemetry continuity" in body
    assert 'href="/digital-twin"' in body

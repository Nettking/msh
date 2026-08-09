from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from catalog.flask_app import app as app_module
from catalog.flask_app import capability_product_routes
from catalog.flask_app import routes as routes_module
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_config_service import CapabilityConfig


def _config(*, sources: str = "") -> CapabilityConfig:
    return CapabilityConfig(
        ai_provider_mode="local",
        ai_provider_name="This computer",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url="http://ollama:11434",
        recorder_sources=sources,
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="test",
    )


def test_app_factory_owns_capability_product_handlers(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    app = create_app()

    for endpoint in (
        "web.status",
        "web.recorder_status_snapshot",
        "web.startup",
    ):
        assert app.view_functions[endpoint].__module__ == (
            "catalog.flask_app.capability_product_routes"
        )

    assert not hasattr(routes_module, "startup_mode_gate")
    assert not hasattr(routes_module, "status")
    assert not hasattr(routes_module, "recorder_status_snapshot")
    assert not hasattr(routes_module, "startup")
    assert not hasattr(app_module, "inject_server_setup")

    assert any(
        function is capability_product_routes.capability_startup_mode_gate
        for function in app.before_request_funcs.get(None, ())
    )
    assert any(
        function is capability_product_routes.inject_capability_product_context
        for function in app.template_context_processors.get(None, ())
    )


def test_product_routes_can_register_without_legacy_blueprint_endpoints() -> None:
    app = Flask(__name__)

    capability_product_routes.install_capability_product_routes(app)

    rules = {rule.endpoint: rule.rule for rule in app.url_map.iter_rules()}
    assert rules["web.status"] == "/status"
    assert rules["web.recorder_status_snapshot"] == "/status/recorder.json"
    assert rules["web.startup"] == "/startup"


def test_product_context_reads_legacy_settings_only_for_explicit_startup(
    monkeypatch,
) -> None:
    app = Flask(__name__)
    monkeypatch.setattr(
        capability_product_routes,
        "_load_product_config",
        lambda: (_config(), ""),
    )
    monkeypatch.setattr(
        capability_product_routes,
        "load_settings",
        lambda: (_ for _ in ()).throw(AssertionError("legacy read")),
    )
    monkeypatch.setattr(
        capability_product_routes,
        "get_recorder_control_service",
        lambda: SimpleNamespace(status=lambda _config: {"ready": False}),
    )

    with app.test_request_context("/status"):
        context = capability_product_routes.inject_capability_product_context()
    assert context["server_setup_settings"] is None

    with app.test_request_context("/startup"):
        try:
            capability_product_routes.inject_capability_product_context()
        except AssertionError as exc:
            assert str(exc) == "legacy read"
        else:
            raise AssertionError("explicit /startup must retain the legacy display read")


def test_status_template_has_no_deployment_role_adapter() -> None:
    template = (
        Path(__file__).parents[1] / "templates" / "status.html"
    ).read_text(encoding="utf-8")

    assert "deployment_mode" not in template
    assert "recorder-only" not in template
    assert "full-server" not in template
    assert "web-workbench" not in template
    assert "recorder_visible" in template


def test_completed_capability_state_bypasses_legacy_runtime_choice(monkeypatch) -> None:
    app = Flask(__name__)
    app.add_url_rule("/target", endpoint="target", view_func=lambda: "ok")
    monkeypatch.setattr(
        capability_product_routes,
        "_capability_flags",
        lambda: {"completed": True, "runtime": False},
    )
    monkeypatch.setattr(
        capability_product_routes,
        "get_runtime_manager",
        lambda: SimpleNamespace(requires_startup_choice=lambda: True),
    )

    with app.test_request_context("/target"):
        assert capability_product_routes.capability_startup_mode_gate() is None


def test_recorder_snapshot_requires_current_contribution_authority(monkeypatch) -> None:
    app = Flask(__name__)
    monkeypatch.setattr(
        capability_product_routes,
        "_capability_flags",
        lambda: {"completed": True, "runtime": True},
    )
    monkeypatch.setattr(
        capability_product_routes,
        "_active_recorder_contribution",
        lambda: False,
    )

    with app.test_request_context("/status/recorder.json"):
        response, status = capability_product_routes.recorder_status_snapshot()

    assert status == 409
    assert response.get_json()["error"] == "recorder_not_enabled"


def test_recorder_snapshot_uses_capability_config_when_authorized(monkeypatch) -> None:
    app = Flask(__name__)
    config = _config(sources="Mazak=http://192.168.200.10:5000")
    monkeypatch.setattr(
        capability_product_routes,
        "_capability_flags",
        lambda: {"completed": True, "runtime": True},
    )
    monkeypatch.setattr(
        capability_product_routes,
        "_active_recorder_contribution",
        lambda: True,
    )
    monkeypatch.setattr(
        capability_product_routes,
        "_load_product_config",
        lambda: (config, ""),
    )

    seen = {}

    class Recorder:
        def web_status(self, supplied):
            seen["config"] = supplied
            return {"schema": "msh.recorder.web_status.v1", "state": "recording"}

    monkeypatch.setattr(
        capability_product_routes,
        "get_recorder_control_service",
        lambda: Recorder(),
    )

    with app.test_request_context("/status/recorder.json"):
        response, status = capability_product_routes.recorder_status_snapshot()

    assert status == 200
    assert seen["config"] is config
    assert response.get_json()["state"] == "recording"

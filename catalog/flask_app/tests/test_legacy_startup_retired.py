from __future__ import annotations

from pathlib import Path

from flask import Flask

from catalog.flask_app import app as app_module
from catalog.flask_app import capability_startup_transition_routes as transition_routes
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_config_service import (
    default_capability_config,
)


class FakeRuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False


class FakeTransitionService:
    def capability_flags(self) -> dict[str, object]:
        return {
            "workbench": False,
            "runtime": False,
            "recorder": False,
            "language_model": False,
            "compute": False,
            "storage": False,
            "completed": False,
            "needs_migration": True,
            "source": "legacy-migration",
            "state": None,
        }


def test_explicit_legacy_startup_query_routes_to_capability_onboarding(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    manager = FakeRuntimeManager()
    transition = FakeTransitionService()
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(
        transition_routes,
        "get_capability_startup_transition_service",
        lambda: transition,
    )

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1&edit=1&step=role")

    assert response.status_code in {302, 303}
    assert response.location == "/onboarding?step=finish"
    templates = Path(app_module.__file__).with_name("templates")
    assert not (templates / "startup.html").exists()


def test_ai_model_benchmark_endpoint_remains_available(monkeypatch) -> None:
    config = default_capability_config()
    monkeypatch.setattr(
        server_setup_routes,
        "load_capability_config",
        lambda: config,
    )

    def fake_compare(selected_config):
        assert selected_config.ai_profile == "edge-small"
        return {
            "ok": True,
            "rows": [
                {
                    "profile": "edge-small",
                    "label": "Edge small",
                    "model": "smollm2:360m",
                    "installed": True,
                    "tested": True,
                    "result": {
                        "ok": True,
                        "elapsed_ms": 1200,
                        "assessment": {
                            "key": "fast",
                            "label": "Fast",
                            "description": "Comfortable.",
                        },
                    },
                }
            ],
            "recommendation": {
                "verdict": "supported",
                "hardware_supported": True,
                "recommended_profile": "edge-small",
                "recommended_model": "smollm2:360m",
                "recommended_label": "Edge small",
                "message": "Recommended model: Edge small (smollm2:360m).",
            },
            "message": "Recommended model: Edge small (smollm2:360m).",
        }

    monkeypatch.setattr(server_setup_routes, "compare_ollama_setup_models", fake_compare)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"
    app.register_blueprint(server_setup_routes.server_setup_web)

    client = app.test_client()
    with client.session_transaction() as browser_session:
        browser_session["mtconnect_discovery_csrf_token"] = "test-csrf-token"
    response = client.post(
        "/server-setup/test-ai-model",
        data={
            "_csrf_token": "test-csrf-token",
            "ai_provider_mode": "local",
            "ai_profile": "edge-small",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recommendation"]["recommended_profile"] == "edge-small"
    assert payload["model"] == "smollm2:360m"
    assert payload["assessment"]["label"] == "Edge small"
    assert "Recommended model" in payload["message"]

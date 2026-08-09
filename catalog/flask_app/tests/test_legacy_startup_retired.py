from __future__ import annotations

from dataclasses import replace

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import default_settings


class FakeRuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return True

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": True}


def _patch_runtime(monkeypatch) -> None:
    manager = FakeRuntimeManager()
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)


def _configured_settings():
    return replace(
        default_settings(configured=True),
        deployment_mode="full-server",
        ai_enabled=True,
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        recorder_sources="Machine=http://10.20.30.50:5000/current",
    )


def _patch_setup_context(monkeypatch, settings) -> None:
    monkeypatch.setattr(app_module, "load_settings", lambda: settings)
    monkeypatch.setattr(routes_module, "load_settings", lambda: settings)
    monkeypatch.setattr(server_setup_routes, "load_settings", lambda: settings)
    monkeypatch.setattr(
        app_module,
        "ollama_status",
        lambda _settings: {
            "running": True,
            "selected_model": settings.ai_model,
            "selected_model_installed": True,
            "models": [settings.ai_model],
            "installed_by_profile": {
                "edge-small": False,
                "laptop-standard": True,
                "workstation-strong": False,
            },
            "installed_by_model": {
                "smollm2:360m": False,
                "llama3.2:3b": True,
                "qwen2.5:7b": False,
            },
            "message": "Ollama is running.",
        },
    )


def test_explicit_legacy_startup_is_read_only_compatibility_notice(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    settings = _configured_settings()
    _patch_setup_context(monkeypatch, settings)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1&edit=1&step=role")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Legacy device-role setup has been retired" in html
    assert "Open capability-first onboarding" in html
    assert "Preserved compatibility settings" in html
    assert "full-server" in html
    assert settings.ai_model in html
    assert settings.recorder_sources in html

    # The old role-first authority and runtime-choice controls must not return.
    assert 'name="deployment_mode"' not in html
    assert "/server-setup/save" not in html
    assert "/startup/choose" not in html
    assert "Resume session" not in html
    assert "Start new session" not in html
    assert "Save setup and continue" not in html


def test_ai_model_benchmark_endpoint_remains_available(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    settings = _configured_settings()
    _patch_setup_context(monkeypatch, settings)

    def fake_compare(_settings):
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

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()
    with client.session_transaction() as browser_session:
        browser_session["mtconnect_discovery_csrf_token"] = "test-csrf-token"
    response = client.post(
        "/server-setup/test-ai-model",
        data={
            "_csrf_token": "test-csrf-token",
            "ai_enabled": "on",
            "ai_profile": "edge-small",
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recommendation"]["recommended_profile"] == "edge-small"
    assert payload["model"] == "smollm2:360m"
    assert payload["assessment"]["label"] == "Edge small"
    assert "Recommended model" in payload["message"]

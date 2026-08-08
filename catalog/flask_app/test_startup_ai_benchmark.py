from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import default_settings


class FakeRuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": False}


def _patch_runtime(monkeypatch) -> None:
    manager = FakeRuntimeManager()
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)


def _patch_setup_context(monkeypatch) -> None:
    settings = default_settings(configured=False)
    monkeypatch.setattr(app_module, "load_settings", lambda: settings)
    monkeypatch.setattr(
        app_module,
        "ollama_status",
        lambda _settings: {
            "running": True,
            "selected_model": "llama3.2:3b",
            "selected_model_installed": True,
            "models": ["llama3.2:3b"],
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


def test_explicit_legacy_ai_step_exposes_model_suggestion_benchmark(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1&next=%2F&step=ai")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'data-initial-step="ai"' in html
    assert "Benchmark and model suggestion" in html
    assert "Benchmark and suggest model" in html
    assert "/server-setup/test-ai-model" in html
    assert "Connected computer" in html
    assert 'name="ai_provider_mode"' in html
    assert 'name="ollama_base_url"' in html
    assert "/server-setup/test-ai-connection" in html


def test_explicit_legacy_setup_uses_focused_guided_shell(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert '<body class="setup-focus">' in html
    assert 'class="setup-shell-header"' in html
    assert "Set up this MSH device" in html
    assert "Step 1 · Device role" in html
    assert 'data-step-target="model"' in html
    assert "Choose a specialized device role" in html
    assert "site-nav--primary" not in html
    assert "Artifact scan:" not in html
    assert "rescan-form" not in html
    assert "Not sure what to choose?" not in html
    assert "Advanced: command-driven setup" not in html


def test_ai_model_benchmark_endpoint_returns_recommendation(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch)

    settings = default_settings(configured=True)
    monkeypatch.setattr(server_setup_routes, "load_settings", lambda: settings)

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
                        "assessment": {"key": "fast", "label": "Fast", "description": "Comfortable."},
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

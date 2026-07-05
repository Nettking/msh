from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.ai_model_benchmark_service import compare_ollama_setup_models
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


def test_startup_ai_step_exposes_response_time_comparison(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?next=%2F&step=ai")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'data-initial-step="ai"' in html
    assert "Response time check" in html
    assert "Test selected model speed" in html
    assert "/server-setup/test-ai-model" in html
    assert "Fast: comfortable for interactive use" in html
    assert "Too slow: choose a smaller model or stronger machine" in html


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
    response = app.test_client().post(
        "/server-setup/test-ai-model",
        data={"ai_enabled": "on", "ai_profile": "edge-small"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recommendation"]["recommended_profile"] == "edge-small"
    assert payload["model"] == "smollm2:360m"
    assert payload["assessment"]["label"] == "Edge small"
    assert "Recommended model" in payload["message"]


def test_ai_setup_comparison_marks_edge_small_too_slow_as_not_supported(monkeypatch):
    settings = default_settings(configured=True)
    monkeypatch.setattr(
        "catalog.flask_app.services.ai_model_benchmark_service.ollama_status",
        lambda _settings, timeout_seconds=2.0: {
            "running": True,
            "installed_by_profile": {
                "edge-small": True,
                "laptop-standard": False,
                "workstation-strong": False,
            },
            "models": ["smollm2:360m"],
        },
    )
    monkeypatch.setattr(
        "catalog.flask_app.services.ai_model_benchmark_service.benchmark_ollama_response_time",
        lambda _settings, *, model: {
            "ok": True,
            "model": model,
            "elapsed_ms": 41000,
            "assessment": {"key": "too_slow", "label": "Too slow", "description": "Too slow."},
            "message": "too slow",
        },
    )

    result = compare_ollama_setup_models(settings)

    assert result["recommendation"]["verdict"] == "insufficient_hardware"
    assert result["recommendation"]["hardware_supported"] is False
    assert "Edge small" in result["recommendation"]["message"]

from __future__ import annotations

from dataclasses import replace

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
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


def _configured_full_server_settings():
    return replace(
        default_settings(configured=True),
        deployment_mode="full-server",
        ai_enabled=True,
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        recorder_sources="",
    )


def _patch_setup_context(monkeypatch, settings) -> None:
    monkeypatch.setattr(app_module, "load_settings", lambda: settings)
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


def test_explicit_legacy_saved_startup_shows_runtime_landing(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch, _configured_full_server_settings())

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1&next=%2F&step=review")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Choose a starting point" in html
    assert "MSH found saved session progress" in html
    assert "Resume where MSH stopped" in html
    assert "Resume session" in html
    assert "Begin a new run" in html
    assert "Start new session" in html
    assert "Edit device setup" in html
    assert "Recorder source is missing" in html
    assert "Guided setup" not in html
    assert "MSH setup is complete" not in html
    assert "Advanced: command-driven setup" not in html
    assert 'class="setup-shell-header"' in html
    assert "site-nav--primary" not in html
    assert html.index("Resume session") < html.index("Device setup and changes")


def test_explicit_legacy_saved_startup_edit_mode_shows_wizard(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup_context(monkeypatch, _configured_full_server_settings())

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/startup?legacy=1&next=%2F&edit=1&step=ai")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Edit setup" in html
    assert "Benchmark and model suggestion" in html
    assert "Save setup and continue" in html

from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import default_settings


class FakeRuntimeManager:
    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return False

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": False}

    def state_snapshot(self) -> dict[str, object]:
        return {
            "current_processing_phase": "runtime_not_started",
            "startup_mode": "continue_existing",
            "last_failure": "",
            "update_running": False,
            "view_contracts": {},
        }


def _patch_runtime(monkeypatch) -> None:
    manager = FakeRuntimeManager()
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)


def _patch_setup(monkeypatch) -> None:
    def load_configured_settings():
        return default_settings(configured=True)

    monkeypatch.setattr(app_module, "load_settings", load_configured_settings)
    monkeypatch.setattr("catalog.flask_app.server_setup_routes.load_settings", load_configured_settings)


def test_main_navigation_pages_load(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    pages = [
        ("/", "Overview"),
        ("/guide", "How to use MSH"),
        ("/get-started", "What do you want to do first?"),
        ("/startup", "MSH is ready"),
        ("/startup?edit=1&step=ai", "Language-model capability"),
        ("/sources/", "Sources"),
        ("/status", "Diagnostics"),
        ("/operator-strategies/capture", "Capture"),
        ("/operator-strategies/review", "Review Notes"),
        ("/strategy-comparison", "Strategies"),
        ("/strategies", "Intervention Logic"),
        ("/osl-export", "OSL to SysML export"),
        ("/assist", "Assist"),
    ]

    for path, expected_text in pages:
        response = client.get(path)
        assert response.status_code == 200, path
        assert expected_text in response.get_data(as_text=True), path


def test_get_started_is_a_focused_task_handoff(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    html = app.test_client().get("/get-started").get_data(as_text=True)

    assert '<body class="setup-focus">' in html
    assert "site-nav--primary" not in html
    assert "Artifact scan:" not in html
    assert "Rescan" not in html
    assert "Capture operator knowledge" in html
    assert 'href="/operator-strategies/capture"' in html
    assert 'href="/sources/"' in html
    assert "Full workbench" in html

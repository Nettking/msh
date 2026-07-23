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
        ("/operator-strategies", "Knowledge workspace"),
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


def test_main_pages_include_mobile_navigation_but_setup_does_not(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    overview = client.get("/").get_data(as_text=True)
    setup = client.get("/startup?edit=1").get_data(as_text=True)

    assert 'data-mobile-navigation' in overview
    assert 'aria-label="Mobile primary sections"' in overview
    assert "Rescan now" in overview
    assert 'data-mobile-navigation' not in setup


def test_knowledge_navigation_opens_a_choice_page(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    overview_html = client.get("/").get_data(as_text=True)
    knowledge_response = client.get("/operator-strategies")
    knowledge_html = knowledge_response.get_data(as_text=True)

    assert knowledge_response.status_code == 200
    assert 'href="/operator-strategies"' in overview_html
    assert "Capture now or review later" in knowledge_html
    assert 'href="/operator-strategies/capture"' in knowledge_html
    assert 'href="/operator-strategies/review"' in knowledge_html
    assert 'href="/strategy-comparison"' in knowledge_html
    assert 'href="/strategies"' in knowledge_html
    assert 'href="/osl-export"' in knowledge_html

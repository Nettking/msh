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


def _patch_runtime(monkeypatch) -> None:
    manager = FakeRuntimeManager()
    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)


def _patch_setup(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "load_settings", lambda: default_settings(configured=True))


def test_guide_page_explains_knowledge_flow(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/guide")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Guide" in html
    assert "Monitor, Knowledge, and System" in html
    assert "Recommended knowledge flow" in html
    assert "Capture a raw statement" in html
    assert "Intervention Logic" in html
    assert "SysML Export" in html
    assert "Workflow" not in html

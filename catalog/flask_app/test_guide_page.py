from __future__ import annotations

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app.app import create_app


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


def test_guide_page_explains_knowledge_flow(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/guide")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "How to use FCP" in html
    assert "Three areas, three purposes" in html
    assert "Start here" in html
    assert "At the machine" in html
    assert "Recommended knowledge flow" in html
    assert "Capture a raw statement" in html
    assert "Do not try to model everything while standing at the machine" in html
    assert "Setup guide" in html
    assert "Create this device and join a Federation" in html
    assert "Review capabilities and finish" in html
    assert "Web workbench" not in html
    assert "Full server" not in html
    assert "Recorder station" not in html
    assert "Strategy" in html
    assert "Intervention Logic" in html
    assert "SysML Export" in html
    assert "MTConnect/VPN tests" in html
    assert "Test MTConnect" in html
    assert "Test VPN/network" in html
    assert "Workflow" not in html

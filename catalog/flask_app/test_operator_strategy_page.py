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


def test_operator_strategy_capture_is_fast_quick_record(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch_runtime(monkeypatch)
    _patch_setup(monkeypatch)

    app = create_app()
    app.config.update(TESTING=True)

    response = app.test_client().get("/operator-strategies")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Quick record" in html
    assert "Only the decision/action is required" in html
    assert "autofocus" in html
    assert "name=\"decision\" rows=\"5\" required" in html
    assert "Situation / path" in html
    assert "More detail / later analysis" in html
    assert "Save fast record" in html
    assert "What did the operator decide?" not in html
    assert "What should this become?" not in html

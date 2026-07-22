from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import default_settings


class FakeRuntimeManager:
    def __init__(self, *, requires_choice: bool) -> None:
        self._requires_choice = requires_choice

    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return self._requires_choice

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": self._requires_choice}


def _form() -> dict[str, str]:
    return {
        "deployment_mode": "web-workbench",
        "ai_provider_mode": "local",
        "ai_profile": "laptop-standard",
        "recorder_poll_interval": "0.2",
    }


def _app(monkeypatch, *, setup_complete: bool, requires_choice: bool):
    manager = FakeRuntimeManager(requires_choice=requires_choice)
    previous = default_settings(configured=setup_complete)
    saved: list[object] = []
    runtime_starts: list[bool] = []

    monkeypatch.setattr(app_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(routes_module, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(server_setup_routes, "get_runtime_manager", lambda: manager)
    monkeypatch.setattr(app_module, "load_settings", lambda: previous)
    monkeypatch.setattr(server_setup_routes, "load_settings", lambda: previous)
    monkeypatch.setattr(server_setup_routes, "save_settings", saved.append)
    monkeypatch.setattr(server_setup_routes, "start_runtime_background", lambda: runtime_starts.append(True))

    app = create_app()
    app.config.update(TESTING=True)
    return app, saved, runtime_starts


def test_first_setup_hands_off_to_get_started(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )

    response = app.test_client().post("/server-setup/save", data=_form())

    assert response.status_code == 302
    assert response.location == "/get-started"
    assert len(saved) == 1
    assert runtime_starts == [True]


def test_first_setup_keeps_get_started_after_session_choice(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=True,
    )

    response = app.test_client().post("/server-setup/save", data=_form())
    location = urlsplit(response.location)

    assert response.status_code == 302
    assert location.path == "/startup"
    assert parse_qs(location.query) == {"next": ["/get-started"], "step": ["runtime"]}
    assert len(saved) == 1
    assert runtime_starts == []


def test_editing_saved_setup_preserves_requested_destination(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=False,
    )

    response = app.test_client().post(
        "/server-setup/save",
        data={**_form(), "next": "/sources/"},
    )

    assert response.status_code == 302
    assert response.location == "/sources/"
    assert len(saved) == 1
    assert runtime_starts == [True]

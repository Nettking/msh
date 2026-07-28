from __future__ import annotations

from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import (
    ServerSetupError,
    default_settings,
    settings_from_form,
)


class FakeRuntimeManager:
    def __init__(self, *, requires_choice: bool) -> None:
        self._requires_choice = requires_choice

    def mark_app_started(self) -> None:
        pass

    def requires_startup_choice(self) -> bool:
        return self._requires_choice

    def startup_decision_snapshot(self) -> dict[str, bool]:
        return {"requires_choice": self._requires_choice}


class FakeDiscoveryService:
    def __init__(self) -> None:
        self.scan_calls: list[tuple[str, str]] = []
        self.selected: list[str] = []

    def scan(self, cidr: str, *, port: str):
        self.scan_calls.append((cidr, port))
        return {"machines_found": 2, "agents_found": 2}

    def merge_selected_results(self, settings, selected):
        self.selected = list(selected)
        return replace(
            settings,
            recorder_sources="MAZAK-001=http://192.168.200.249:5000",
        )


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


def _csrf_data(client, **data):
    with client.session_transaction() as browser_session:
        browser_session["mtconnect_discovery_csrf_token"] = "test-csrf-token"
    return {"_csrf_token": "test-csrf-token", **data}


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


def test_recorder_station_setup_disables_ai_and_normalizes_sources() -> None:
    settings = settings_from_form(
        {
            "deployment_mode": "recorder-only",
            "ai_enabled": "on",
            "ai_provider_mode": "local",
            "ai_profile": "laptop-standard",
            "recorder_sources": (
                "MAZAK-M7ZDA13010Z="
                "http://192.168.200.249:5000/current"
            ),
        }
    )

    assert settings.ai_enabled is False
    assert settings.deployment_mode == "recorder-only"
    assert settings.recorder_sources == (
        "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
    )


def test_duplicate_recorder_source_names_are_rejected() -> None:
    try:
        settings_from_form(
            {
                "deployment_mode": "recorder-only",
                "ai_provider_mode": "local",
                "ai_profile": "laptop-standard",
                "recorder_sources": (
                    "Mazak=http://192.168.200.249:5000;"
                    "Mazak=http://192.168.200.250:5000"
                ),
            }
        )
    except ServerSetupError as exc:
        assert "duplicated" in str(exc)
    else:
        raise AssertionError("Duplicate recorder source names should fail validation.")


def test_status_network_scan_route_is_available_before_runtime_choice(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=True,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )

    client = app.test_client()
    response = client.post(
        "/status/mtconnect-scan",
        data=_csrf_data(
            client,
            cidr="192.168.200.0/24",
            port="5000",
        ),
    )

    assert response.status_code == 302
    assert response.location == "/status#mtconnect-discovery"
    assert discovery.scan_calls == [("192.168.200.0/24", "5000")]


def test_discovered_sources_are_saved_to_persistent_setup(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, _ = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=True,
    )
    recorder_settings = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
        ai_enabled=False,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "load_settings",
        lambda: recorder_settings,
    )
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )

    client = app.test_client()
    response = client.post(
        "/status/mtconnect-sources",
        data=_csrf_data(
            client,
            source_id=["source-a", "source-b"],
        ),
    )

    assert response.status_code == 302
    assert response.location == "/status#mtconnect-discovery"
    assert discovery.selected == ["source-a", "source-b"]
    assert saved[-1].recorder_sources == (
        "MAZAK-001=http://192.168.200.249:5000"
    )


def test_mtconnect_scan_rejects_missing_or_cross_site_csrf(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=True,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )
    client = app.test_client()

    missing = client.post(
        "/status/mtconnect-scan",
        data={"cidr": "192.168.200.0/24", "port": "5000"},
    )
    cross_site = client.post(
        "/status/mtconnect-scan",
        data=_csrf_data(
            client,
            cidr="192.168.200.0/24",
            port="5000",
        ),
        headers={"Sec-Fetch-Site": "cross-site"},
    )
    remote_host = client.post(
        "/status/mtconnect-scan",
        data=_csrf_data(
            client,
            cidr="192.168.200.0/24",
            port="5000",
        ),
        headers={"Host": "192.168.200.10:5000"},
    )

    assert missing.status_code == 302
    assert cross_site.status_code == 302
    assert remote_host.status_code == 302
    assert discovery.scan_calls == []

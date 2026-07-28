from __future__ import annotations

from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

from catalog.flask_app import app as app_module
from catalog.flask_app import routes as routes_module
from catalog.flask_app import server_setup_routes
from catalog.flask_app.app import create_app
from catalog.flask_app.services.server_setup_service import (
    AI_DEPLOYMENT_MODES,
    RECORDER_DEPLOYMENT_MODES,
    ServerSetupError,
    default_settings,
    role_has_recorder,
    role_uses_ai,
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

    client = app.test_client()
    response = client.post(
        "/server-setup/save",
        data=_csrf_data(client, **_form()),
    )

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

    client = app.test_client()
    response = client.post(
        "/server-setup/save",
        data=_csrf_data(client, **_form()),
    )
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

    client = app.test_client()
    response = client.post(
        "/server-setup/save",
        data=_csrf_data(client, **_form(), next="/sources/"),
    )

    assert response.status_code == 302
    assert response.location == "/sources/"
    assert len(saved) == 1
    assert runtime_starts == [True]


def test_setup_save_repairs_an_unreadable_previous_settings_file(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )

    def fail_to_load_settings():
        raise ServerSetupError("Could not read server setup settings.")

    monkeypatch.setattr(
        server_setup_routes,
        "load_settings",
        fail_to_load_settings,
    )

    client = app.test_client()
    response = client.post(
        "/server-setup/save",
        data=_csrf_data(client, **_form()),
    )

    assert response.status_code == 302
    assert response.location == "/get-started"
    assert len(saved) == 1
    assert saved[0].deployment_mode == "web-workbench"
    assert runtime_starts == [True]


def test_role_capabilities_match_setup_fields() -> None:
    assert AI_DEPLOYMENT_MODES == {
        "full-server",
        "web-workbench",
        "web-ui-only",
    }
    assert RECORDER_DEPLOYMENT_MODES == {
        "full-server",
        "recorder-only",
    }
    assert role_uses_ai("recorder-only") is False
    assert role_has_recorder("recorder-only") is True
    assert role_uses_ai("full-server") is True
    assert role_has_recorder("full-server") is True


def test_recorder_station_setup_preserves_hidden_ai_and_normalizes_sources() -> None:
    previous = replace(
        default_settings(configured=True),
        ai_enabled=True,
        ai_provider_mode="connected",
        ai_provider_name="Existing laptop",
        ai_profile="workstation-strong",
        ai_model="qwen2.5:7b",
        ollama_base_url="http://192.168.1.50:11434",
    )
    settings = settings_from_form(
        {
            "deployment_mode": "recorder-only",
            "recorder_sources": (
                "MAZAK-M7ZDA13010Z="
                "http://192.168.200.249:5000/current"
            ),
        },
        previous,
    )

    assert settings.deployment_mode == "recorder-only"
    assert settings.ai_enabled is True
    assert settings.ai_provider_mode == "connected"
    assert settings.ai_provider_name == "Existing laptop"
    assert settings.ai_profile == "workstation-strong"
    assert settings.ai_model == "qwen2.5:7b"
    assert settings.ollama_base_url == "http://192.168.1.50:11434"
    assert settings.recorder_sources == (
        "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
    )


def test_non_recorder_setup_preserves_hidden_recorder_settings() -> None:
    previous = replace(
        default_settings(configured=True),
        recorder_sources="MAZAK-001=http://192.168.200.249:5000",
        recorder_poll_interval="1.5",
        recorder_include_condition=True,
    )

    settings = settings_from_form(
        {
            "deployment_mode": "web-workbench",
            "ai_enabled": "on",
            "ai_provider_mode": "local",
            "ai_profile": "edge-small",
        },
        previous,
    )

    assert settings.recorder_sources == previous.recorder_sources
    assert settings.recorder_poll_interval == "1.5"
    assert settings.recorder_include_condition is True


def test_disabled_ai_does_not_validate_hidden_provider_or_model_fields() -> None:
    previous = default_settings(configured=True)

    settings = settings_from_form(
        {
            "deployment_mode": "web-workbench",
            "ai_provider_mode": "connected",
            "ai_provider_name": "",
            "ollama_base_url": "not-a-url",
            "ai_profile": "not-a-profile",
        },
        previous,
    )

    assert settings.ai_enabled is False
    assert settings.ai_provider_mode == previous.ai_provider_mode
    assert settings.ai_profile == previous.ai_profile


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
    assert response.location == "/startup?edit=1&step=recorder"
    assert discovery.scan_calls == [("192.168.200.0/24", "5000")]


def test_mtconnect_scan_returns_json_for_inline_setup(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )

    client = app.test_client()
    success = client.post(
        "/status/mtconnect-scan",
        data=_csrf_data(
            client,
            cidr="192.168.200.0/24",
            port="5000",
        ),
        headers={"Accept": "application/json"},
    )
    failure = client.post(
        "/status/mtconnect-scan",
        data={"cidr": "192.168.200.0/24", "port": "5000"},
        headers={"Accept": "application/json"},
    )

    assert success.status_code == 200
    assert success.get_json() == {
        "ok": True,
        "scan": {"machines_found": 2, "agents_found": 2},
        "message": (
            "Network scan complete: 2 machine(s) from "
            "2 MTConnect Agent(s)."
        ),
    }
    assert failure.status_code == 400
    assert set(failure.get_json()) == {"ok", "message"}
    assert failure.get_json()["ok"] is False


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
    assert response.location == "/status#recorder-status"
    assert discovery.selected == ["source-a", "source-b"]
    assert saved[-1].recorder_sources == (
        "MAZAK-001=http://192.168.200.249:5000"
    )


def test_first_recorder_setup_merges_scan_selection_before_one_save(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )

    client = app.test_client()
    response = client.post(
        "/server-setup/save",
        data=_csrf_data(
            client,
            deployment_mode="recorder-only",
            recorder_sources="",
            recorder_poll_interval="0.5",
            source_id=["source-a", "source-b"],
        ),
    )

    assert response.status_code == 302
    assert response.location == "/status#recorder-status"
    assert discovery.selected == ["source-a", "source-b"]
    assert len(saved) == 1
    assert saved[0].recorder_sources == (
        "MAZAK-001=http://192.168.200.249:5000"
    )
    assert saved[0].recorder_poll_interval == "0.5"
    assert runtime_starts == []


def test_inline_setup_selection_requires_local_csrf_before_saving(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, _ = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )
    discovery = FakeDiscoveryService()
    monkeypatch.setattr(
        server_setup_routes,
        "get_mtconnect_discovery_service",
        lambda: discovery,
    )

    response = app.test_client().post(
        "/server-setup/save",
        data={
            "deployment_mode": "recorder-only",
            "source_id": "source-a",
        },
    )

    assert response.status_code == 302
    location = urlsplit(response.location)
    assert location.path == "/startup"
    assert parse_qs(location.query)["edit"] == ["1"]
    assert parse_qs(location.query)["step"] == ["review"]
    assert saved == []
    assert discovery.selected == []


def test_setup_save_requires_csrf_without_discovery_selection(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, saved, runtime_starts = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )

    response = app.test_client().post(
        "/server-setup/save",
        data=_form(),
    )

    assert response.status_code == 302
    location = urlsplit(response.location)
    assert location.path == "/startup"
    assert parse_qs(location.query)["edit"] == ["1"]
    assert parse_qs(location.query)["step"] == ["review"]
    assert saved == []
    assert runtime_starts == []


def test_recording_control_requires_csrf(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=False,
    )
    recorder_settings = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
        recorder_sources="MAZAK-001=http://192.168.200.249:5000",
    )
    calls: list[bool] = []

    class FakeRecorderControl:
        def set_enabled(self, enabled, _settings):
            calls.append(enabled)
            return True, "Recording state changed."

    monkeypatch.setattr(
        server_setup_routes,
        "load_settings",
        lambda: recorder_settings,
    )
    monkeypatch.setattr(
        server_setup_routes,
        "get_recorder_control_service",
        lambda: FakeRecorderControl(),
    )
    client = app.test_client()

    rejected = client.post("/server-setup/recording/start")
    accepted = client.post(
        "/server-setup/recording/start",
        data=_csrf_data(client),
    )

    assert rejected.status_code == 302
    assert accepted.status_code == 302
    assert calls == [True]


def test_recorder_role_rejects_ai_test_before_contacting_ollama(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=True,
        requires_choice=False,
    )
    recorder_settings = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
    )
    monkeypatch.setattr(
        server_setup_routes,
        "load_settings",
        lambda: recorder_settings,
    )

    def fail_if_ollama_is_contacted(*_args, **_kwargs):
        raise AssertionError("Recorder-only actions must not contact Ollama.")

    monkeypatch.setattr(
        server_setup_routes,
        "ollama_status",
        fail_if_ollama_is_contacted,
    )

    client = app.test_client()
    response = client.post(
        "/server-setup/test-ai-connection",
        data=_csrf_data(client, deployment_mode="recorder-only"),
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 400
    assert "not available" in response.get_json()["message"]


def test_incomplete_setup_redirects_status_to_startup(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    app, _, _ = _app(
        monkeypatch,
        setup_complete=False,
        requires_choice=False,
    )

    response = app.test_client().get("/status")

    assert response.status_code == 302
    assert urlsplit(response.location).path == "/startup"


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

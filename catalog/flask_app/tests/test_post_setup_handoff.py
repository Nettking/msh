from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

from flask import Flask

from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
)
from catalog.flask_app import server_setup_routes
from catalog.flask_app.services.capability_config_migration_service import (
    persist_capability_config_from_setup,
)
from catalog.flask_app.services.capability_config_service import (
    CAPABILITY_CONFIG_PATH,
    CapabilityConfig,
    from_legacy_settings,
    load_capability_config,
    save_capability_config,
)
from catalog.flask_app.services.server_setup_service import (
    SETTINGS_PATH,
    default_settings,
    save_settings,
)


def _client(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test-secret"
    app.register_blueprint(server_setup_routes.server_setup_web)
    return app.test_client()


def _csrf(client) -> str:
    with client.session_transaction() as browser_session:
        browser_session["mtconnect_discovery_csrf_token"] = "test-csrf-token"
    return "test-csrf-token"


def test_legacy_server_setup_save_route_is_retired(tmp_path, monkeypatch) -> None:
    client = _client(tmp_path, monkeypatch)

    response = client.post(
        "/server-setup/save",
        data={"_csrf_token": _csrf(client), "deployment_mode": "full-server"},
    )

    assert response.status_code == 404


def test_capability_config_projection_drops_role_and_authority(tmp_path) -> None:
    legacy = replace(
        default_settings(configured=True),
        deployment_mode="full-server",
        ai_enabled=False,
        ai_provider_mode="connected",
        ai_provider_name="Laptop",
        ai_profile="workstation-strong",
        ai_model="qwen2.5:7b",
        ollama_base_url="http://192.168.1.50:11434",
        recorder_sources="Mazak=http://192.168.200.10:5000",
    )

    config = from_legacy_settings(legacy)
    path = save_capability_config(config, tmp_path / "capability-config.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["schema"] == "fcp.capability_config.v1"
    assert payload["ai_provider_name"] == "Laptop"
    assert payload["recorder_sources"] == "Mazak=http://192.168.200.10:5000"
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload
    assert "configured" not in payload
    assert "user_setup_complete" not in payload

    restored = load_capability_config(
        path,
        legacy_loader=lambda: default_settings(configured=False),
    )
    assert restored.ai_model == "qwen2.5:7b"
    assert restored.recorder_sources == config.recorder_sources


def test_capability_config_persistence_leaves_legacy_document_unchanged(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    original = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
        ai_enabled=False,
        ai_provider_mode="local",
        recorder_sources="",
    )
    save_settings(original, SETTINGS_PATH)
    original_payload = SETTINGS_PATH.read_text(encoding="utf-8")
    config = CapabilityConfig(
        ai_provider_mode="connected",
        ai_provider_name="Laptop",
        ai_profile="workstation-strong",
        ai_model="qwen2.5:7b",
        ollama_base_url="http://192.168.1.50:11434",
        recorder_sources="Mazak=http://192.168.200.10:5000",
        recorder_poll_interval="0.5",
        recorder_include_condition=True,
        updated_at="2026-08-09T14:45:00Z",
    )

    save_capability_config(config)

    assert SETTINGS_PATH.read_text(encoding="utf-8") == original_payload
    saved = load_capability_config()
    assert saved.ai_provider_name == "Laptop"
    assert saved.recorder_sources == "Mazak=http://192.168.200.10:5000"


def test_capability_finish_adapter_writes_only_role_free_configuration(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    original = replace(
        default_settings(configured=True),
        deployment_mode="web-ui-only",
        ai_enabled=False,
        recorder_sources="",
    )
    save_settings(original, SETTINGS_PATH)
    original_payload = SETTINGS_PATH.read_text(encoding="utf-8")
    synthetic_completion = replace(
        original,
        deployment_mode="full-server",
        ai_enabled=True,
        ai_provider_mode="connected",
        ai_provider_name="Laptop",
        ai_profile="workstation-strong",
        ai_model="qwen2.5:7b",
        ollama_base_url="http://192.168.1.50:11434",
        recorder_sources="Mazak=http://192.168.200.10:5000/current",
        recorder_poll_interval="0.5",
        recorder_include_condition=True,
    )

    persisted = persist_capability_config_from_setup(synthetic_completion)

    assert SETTINGS_PATH.read_text(encoding="utf-8") == original_payload
    assert persisted.recorder_sources == "Mazak=http://192.168.200.10:5000"
    payload = json.loads(CAPABILITY_CONFIG_PATH.read_text(encoding="utf-8"))
    assert payload["schema"] == "fcp.capability_config.v1"
    assert payload["ai_provider_name"] == "Laptop"
    assert payload["recorder_sources"] == "Mazak=http://192.168.200.10:5000"
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload


def test_language_model_config_route_cannot_change_legacy_document(
    tmp_path,
    monkeypatch,
) -> None:
    client = _client(tmp_path, monkeypatch)
    original = replace(
        default_settings(configured=True),
        deployment_mode="recorder-only",
        ai_enabled=False,
    )
    save_settings(original, SETTINGS_PATH)
    original_payload = SETTINGS_PATH.read_text(encoding="utf-8")

    response = client.post(
        "/capabilities/config/language-model",
        data={
            "_csrf_token": _csrf(client),
            "ai_provider_mode": "connected",
            "ai_provider_name": "Laptop",
            "ollama_base_url": "http://192.168.1.50:11434",
            "ai_profile": "workstation-strong",
            # These legacy fields must have no authority if supplied anyway.
            "deployment_mode": "full-server",
            "ai_enabled": "on",
        },
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    assert response.get_json()["model"] == "qwen2.5:7b"
    payload = json.loads(CAPABILITY_CONFIG_PATH.read_text(encoding="utf-8"))
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload
    assert SETTINGS_PATH.read_text(encoding="utf-8") == original_payload


def test_recorder_config_route_persists_parameters_without_legacy_write(
    tmp_path,
    monkeypatch,
) -> None:
    client = _client(tmp_path, monkeypatch)
    original = replace(
        default_settings(configured=True),
        deployment_mode="web-workbench",
        ai_enabled=True,
    )
    save_settings(original, SETTINGS_PATH)
    original_payload = SETTINGS_PATH.read_text(encoding="utf-8")

    response = client.post(
        "/capabilities/config/recorder",
        data={
            "_csrf_token": _csrf(client),
            "recorder_sources": "Mazak=http://192.168.200.10:5000/current",
            "recorder_poll_interval": "0.5",
            "recorder_include_condition": "on",
            "deployment_mode": "recorder-only",
        },
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    assert response.get_json()["recorder_sources"] == (
        "Mazak=http://192.168.200.10:5000"
    )
    payload = json.loads(CAPABILITY_CONFIG_PATH.read_text(encoding="utf-8"))
    assert "deployment_mode" not in payload
    assert payload["recorder_sources"] == "Mazak=http://192.168.200.10:5000"
    assert payload["recorder_poll_interval"] == "0.5"
    assert payload["recorder_include_condition"] is True
    assert SETTINGS_PATH.read_text(encoding="utf-8") == original_payload


def test_completed_recorder_control_uses_current_contribution_activation(
    monkeypatch,
) -> None:
    candidate = SimpleNamespace(
        candidate_id="recorder-local",
        capability_type="recorder",
    )

    class FakeContributionService:
        def __init__(self, intent) -> None:
            self.intent = intent

        def recommend(self, *, require_benchmark_review: bool):
            assert require_benchmark_review is False
            return (candidate,)

        def intents(self):
            return (self.intent,)

    def authorized(desired_state, activation_state) -> bool:
        intent = SimpleNamespace(
            candidate_id="recorder-local",
            desired_state=desired_state,
            activation_state=activation_state,
        )
        monkeypatch.setattr(
            server_setup_routes,
            "get_capability_contribution_service",
            lambda: FakeContributionService(intent),
        )
        return server_setup_routes._active_recorder_contribution()

    assert authorized(
        ContributionDesiredState.ENABLED,
        ContributionActivationState.ACTIVE,
    ) is True
    assert authorized(
        ContributionDesiredState.DISABLED,
        ContributionActivationState.INACTIVE,
    ) is False
    assert authorized(
        ContributionDesiredState.ENABLED,
        ContributionActivationState.SUSPENDED,
    ) is False


def test_recorder_start_fails_closed_without_capability_intent(
    tmp_path,
    monkeypatch,
) -> None:
    client = _client(tmp_path, monkeypatch)

    response = client.post(
        "/server-setup/recording/start",
        data={"_csrf_token": _csrf(client), "next": "/"},
    )

    assert response.status_code == 303
    assert response.location == "/"
    assert not (
        tmp_path / "data" / "source_state" / "mtconnect_recorder_control.json"
    ).exists()

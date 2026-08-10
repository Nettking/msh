from __future__ import annotations

import json
from dataclasses import replace

import pytest
from flask import Flask

from catalog.flask_app import ai_routes, server_setup_routes
from catalog.flask_app.services import capability_ai_service
from catalog.flask_app.services.capability_ai_service import ollama_status
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfigError,
    default_capability_config,
    load_capability_config,
    normalize_ollama_base_url,
    save_capability_config,
    update_language_model_config,
)
from catalog.flask_app.services.legacy_settings_migration import (
    capability_config_from_legacy,
    load_legacy_settings,
)


def _connected_form(**overrides: str) -> dict[str, str]:
    form = {
        "ai_provider_mode": "connected",
        "ai_provider_name": "Laptop",
        "ollama_base_url": "http://192.168.1.50:11434/",
        "ai_profile": "workstation-strong",
    }
    form.update(overrides)
    return form


def _connected_config():
    return update_language_model_config(
        default_capability_config(),
        _connected_form(),
    )


def test_connected_provider_round_trips_through_capability_config(tmp_path) -> None:
    config = _connected_config()

    assert config.ai_provider_mode == "connected"
    assert config.ai_provider_name == "Laptop"
    assert config.ollama_base_url == "http://192.168.1.50:11434"
    assert config.ai_model == "qwen2.5:7b"

    path = save_capability_config(config, tmp_path / "config.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    restored = load_capability_config(path)

    assert payload["schema"] == "fcp.capability_config.v1"
    assert "user_setup_complete" not in payload
    assert restored == config


def test_explicit_legacy_projection_defaults_to_local_provider(tmp_path) -> None:
    path = tmp_path / "server_settings.json"
    path.write_text(
        json.dumps(
            {
                "schema": "fcp.server_setup.v1",
                "configured": True,
                "deployment_mode": "web-workbench",
                "ai_enabled": True,
                "ai_profile": "laptop-standard",
                "ai_model": "llama3.2:3b",
                "ollama_base_url": "http://ollama:11434",
            }
        ),
        encoding="utf-8",
    )

    legacy = load_legacy_settings(path)
    assert legacy is not None
    config = capability_config_from_legacy(legacy)

    assert config.ai_provider_mode == "local"
    assert config.ai_provider_name == "This computer"
    assert legacy.user_setup_complete is True


def test_legacy_phone_defaults_are_read_only_and_pending_browser_setup(
    tmp_path,
) -> None:
    path = tmp_path / "server_settings.json"
    original = json.dumps(
        {
            "schema": "fcp.server_setup.v2",
            "configured": True,
            "deployment_mode": "web-workbench",
            "ai_enabled": False,
            "ai_provider_mode": "local",
            "ollama_base_url": "http://ollama:11434",
            "recorder_sources": "",
            "recorder_poll_interval": "0.2",
            "recorder_include_condition": False,
        }
    )
    path.write_text(original, encoding="utf-8")

    first = load_legacy_settings(path)
    second = load_legacy_settings(path)

    assert first is not None
    assert first == second
    assert first.configured is True
    assert first.user_setup_complete is False
    assert path.read_text(encoding="utf-8") == original


def test_legacy_projection_preserves_custom_provider_without_rewriting_input(
    tmp_path,
) -> None:
    path = tmp_path / "server_settings.json"
    original = json.dumps(
        {
            "schema": "fcp.server_setup.v2",
            "configured": True,
            "deployment_mode": "web-workbench",
            "ai_enabled": True,
            "ai_provider_mode": "connected",
            "ai_provider_name": "Laptop",
            "ai_profile": "workstation-strong",
            "ollama_base_url": "http://192.168.1.50:11434",
            "recorder_sources": "",
        }
    )
    path.write_text(original, encoding="utf-8")

    legacy = load_legacy_settings(path)
    assert legacy is not None
    config = capability_config_from_legacy(legacy)

    assert config.ai_provider_name == "Laptop"
    assert config.ollama_base_url == "http://192.168.1.50:11434"
    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "url",
    [
        "ftp://192.168.1.50:11434",
        "http://user:secret@192.168.1.50:11434",
        "http://192.168.1.50:11434/api",
        "http://192.168.1.50:99999",
        "http://127.0.0.1:11434",
        "http://0.0.0.0:11434",
    ],
)
def test_connected_provider_rejects_unsafe_or_invalid_urls(url: str) -> None:
    with pytest.raises(CapabilityConfigError):
        normalize_ollama_base_url(url)


def test_connected_provider_requires_explicit_name_and_url() -> None:
    with pytest.raises(CapabilityConfigError, match="name"):
        update_language_model_config(
            default_capability_config(),
            _connected_form(ai_provider_name="", ollama_base_url=""),
        )


def test_local_provider_does_not_retain_connected_endpoint() -> None:
    previous = replace(
        default_capability_config(),
        ai_provider_mode="connected",
        ai_provider_name="Laptop",
        ollama_base_url="http://192.168.1.50:11434",
    )

    config = update_language_model_config(
        previous,
        {
            "ai_provider_mode": "local",
            "ai_provider_name": "Old laptop",
            "ollama_base_url": "http://10.0.0.8:11434",
            "ai_profile": "laptop-standard",
        },
    )

    assert config.ai_provider_name == "This computer"
    assert config.ollama_base_url == "http://ollama:11434"


def test_ollama_status_reports_connected_provider_and_models(monkeypatch) -> None:
    requested: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return b'{"models": [{"name": "qwen2.5:7b"}]}'

    def fake_urlopen(req, timeout):
        requested["url"] = req.full_url
        requested["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(capability_ai_service.request, "urlopen", fake_urlopen)
    config = _connected_config()

    status = ollama_status(config, timeout_seconds=3.0)

    assert requested == {"url": "http://192.168.1.50:11434/api/tags", "timeout": 3.0}
    assert status["running"] is True
    assert status["provider"] == {
        "capability": "language-model",
        "protocol": "ollama",
        "mode": "connected",
        "name": "Laptop",
        "base_url": "http://192.168.1.50:11434",
    }
    assert status["selected_model_installed"] is True
    assert "Laptop is connected" in status["message"]


def test_connection_endpoint_uses_unsaved_connected_provider(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(
        server_setup_routes,
        "load_capability_config",
        default_capability_config,
    )

    def fake_status(config, timeout_seconds):
        captured["config"] = config
        captured["timeout"] = timeout_seconds
        return {
            "running": True,
            "provider": {
                "capability": "language-model",
                "protocol": "ollama",
                "mode": config.ai_provider_mode,
                "name": config.ai_provider_name,
                "base_url": config.ollama_base_url,
            },
            "models": ["qwen2.5:7b"],
            "installed_by_model": {"qwen2.5:7b": True},
            "message": "Laptop is connected.",
        }

    monkeypatch.setattr(server_setup_routes, "ollama_status", fake_status)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"
    app.register_blueprint(server_setup_routes.server_setup_web)

    client = app.test_client()
    with client.session_transaction() as browser_session:
        browser_session["mtconnect_discovery_csrf_token"] = "test-csrf-token"
    response = client.post(
        "/server-setup/test-ai-connection",
        data={"_csrf_token": "test-csrf-token", **_connected_form()},
    )

    assert response.status_code == 200
    assert response.get_json()["ok"] is True
    assert captured["config"].ollama_base_url == "http://192.168.1.50:11434"
    assert captured["config"].ai_provider_name == "Laptop"
    assert captured["timeout"] == 3.0


def test_configured_connected_provider_is_not_runtime_authority(
    monkeypatch,
    tmp_path,
) -> None:
    config = _connected_config()
    monkeypatch.setattr(ai_routes, "load_capability_config", lambda: config)
    monkeypatch.setattr(ai_routes, "_active_capability_defaults", lambda: None)

    assert ai_routes._ai_defaults() == (
        "qwen2.5:7b",
        "http://192.168.1.50:11434",
        "No active AI contribution (Laptop configured)",
    )

    class FakeChunk:
        path = "README.md"

        def source_label(self) -> str:
            return "README.md:1-2"

    monkeypatch.setattr(ai_routes, "repo_root_from", lambda: tmp_path)
    monkeypatch.setattr(ai_routes, "load_or_build_chunks", lambda _root: [])
    monkeypatch.setattr(ai_routes, "build_symbols", lambda _root: {})
    monkeypatch.setattr(
        ai_routes,
        "retrieve",
        lambda *_args, **_kwargs: [FakeChunk()],
    )
    monkeypatch.setattr(ai_routes, "format_context", lambda _chunks: "context")
    monkeypatch.setattr(ai_routes, "_build_ai_runtime", lambda **_kwargs: None)

    result = ai_routes._answer_question(
        "How does FCP work?",
        model=config.ai_model,
        base_url=config.ollama_base_url,
        dry_run=False,
        extractive=False,
    )

    assert result["error_code"] == "no-active-ai-provider"
    assert "No active AI contribution" in result["error"]

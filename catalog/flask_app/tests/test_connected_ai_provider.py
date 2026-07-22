from __future__ import annotations

from dataclasses import replace
from argparse import Namespace
import json

from flask import Flask
import pytest

from catalog.flask_app import ai_routes, server_setup_routes
from catalog.flask_app.services import server_setup_service
from catalog.flask_app.services.server_setup_service import (
    ServerSetupError,
    ai_settings_from_form,
    compose_profiles_for,
    default_settings,
    load_settings,
    normalize_ollama_base_url,
    ollama_status,
    save_settings,
    settings_from_form,
)
from setup_msh import _settings_from_args


def _connected_form(**overrides: str) -> dict[str, str]:
    form = {
        "deployment_mode": "web-workbench",
        "ai_enabled": "on",
        "ai_provider_mode": "connected",
        "ai_provider_name": "Laptop",
        "ollama_base_url": "http://192.168.1.50:11434/",
        "ai_profile": "workstation-strong",
        "recorder_poll_interval": "0.2",
    }
    form.update(overrides)
    return form


def test_connected_provider_round_trips_through_saved_setup(tmp_path) -> None:
    settings = settings_from_form(_connected_form())

    assert settings.ai_provider_mode == "connected"
    assert settings.ai_provider_name == "Laptop"
    assert settings.ollama_base_url == "http://192.168.1.50:11434"
    assert settings.ai_model == "qwen2.5:7b"

    path = save_settings(settings, tmp_path / "server_settings.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    restored = load_settings(path)

    assert payload["schema"] == "msh.server_setup.v2"
    assert restored == settings


def test_legacy_setup_defaults_to_local_provider(tmp_path) -> None:
    path = tmp_path / "server_settings.json"
    path.write_text(
        json.dumps(
            {
                "schema": "msh.server_setup.v1",
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

    settings = load_settings(path)

    assert settings.ai_provider_mode == "local"
    assert settings.ai_provider_name == "This computer"


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
    with pytest.raises(ServerSetupError):
        normalize_ollama_base_url(url)


def test_connected_provider_requires_explicit_name_and_url() -> None:
    with pytest.raises(ServerSetupError, match="name"):
        ai_settings_from_form(
            _connected_form(ai_provider_name="", ollama_base_url=""),
            default_settings(configured=True),
        )


def test_local_provider_does_not_retain_connected_endpoint() -> None:
    previous = replace(
        default_settings(configured=True),
        ai_provider_mode="connected",
        ai_provider_name="Laptop",
        ollama_base_url="http://192.168.1.50:11434",
    )

    settings = ai_settings_from_form(
        {
            "ai_enabled": "on",
            "ai_provider_mode": "local",
            "ai_provider_name": "Old laptop",
            "ollama_base_url": "http://10.0.0.8:11434",
            "ai_profile": "laptop-standard",
        },
        previous,
    )

    assert settings.ai_provider_name == "This computer"
    assert settings.ollama_base_url == "http://ollama:11434"


def test_command_setup_can_select_connected_provider() -> None:
    settings = _settings_from_args(
        Namespace(
            mode="web-workbench",
            ai_profile="workstation-strong",
            no_ai=False,
            ai_provider="connected",
            ai_provider_name="Laptop",
            ollama_url="http://192.168.1.50:11434/",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
        )
    )

    assert settings.ai_provider_mode == "connected"
    assert settings.ai_provider_name == "Laptop"
    assert settings.ollama_base_url == "http://192.168.1.50:11434"
    assert "ai" not in compose_profiles_for(settings).split(",")


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

    monkeypatch.setattr(server_setup_service.request, "urlopen", fake_urlopen)
    settings = settings_from_form(_connected_form())

    status = ollama_status(settings, timeout_seconds=3.0)

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
        "load_settings",
        lambda: default_settings(configured=True),
    )

    def fake_status(settings, timeout_seconds):
        captured["settings"] = settings
        captured["timeout"] = timeout_seconds
        return {
            "running": True,
            "provider": {
                "capability": "language-model",
                "protocol": "ollama",
                "mode": settings.ai_provider_mode,
                "name": settings.ai_provider_name,
                "base_url": settings.ollama_base_url,
            },
            "models": ["qwen2.5:7b"],
            "installed_by_model": {"qwen2.5:7b": True},
            "message": "Laptop is connected.",
        }

    monkeypatch.setattr(server_setup_routes, "ollama_status", fake_status)
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "test"
    app.register_blueprint(server_setup_routes.server_setup_web)

    response = app.test_client().post("/server-setup/test-ai-connection", data=_connected_form())

    assert response.status_code == 200
    assert response.get_json()["ok"] is True
    assert captured["settings"].ollama_base_url == "http://192.168.1.50:11434"
    assert captured["settings"].ai_provider_name == "Laptop"
    assert captured["timeout"] == 3.0


def test_ai_defaults_and_chat_use_saved_connected_provider(monkeypatch, tmp_path) -> None:
    settings = settings_from_form(_connected_form())
    monkeypatch.setattr(ai_routes, "load_settings", lambda: settings)

    assert ai_routes._ai_defaults() == (
        "qwen2.5:7b",
        "http://192.168.1.50:11434",
        "Laptop",
    )

    class FakeChunk:
        path = "README.md"

        def source_label(self) -> str:
            return "README.md:1-2"

    captured = {}
    monkeypatch.setattr(ai_routes, "repo_root_from", lambda: tmp_path)
    monkeypatch.setattr(ai_routes, "load_or_build_chunks", lambda _root: [])
    monkeypatch.setattr(ai_routes, "build_symbols", lambda _root: {})
    monkeypatch.setattr(ai_routes, "retrieve", lambda *_args, **_kwargs: [FakeChunk()])
    monkeypatch.setattr(ai_routes, "format_context", lambda _chunks: "retrieved context")
    monkeypatch.setattr(ai_routes, "build_prompt", lambda *_args, **_kwargs: "prompt")

    def fake_chat(**kwargs):
        captured.update(kwargs)
        return "grounded answer"

    monkeypatch.setattr(ai_routes, "chat", fake_chat)

    result = ai_routes._answer_question(
        "How does MSH work?",
        model="qwen2.5:7b",
        base_url="http://192.168.1.50:11434",
        dry_run=False,
        extractive=False,
    )

    assert result["error"] == ""
    assert captured["model"] == "qwen2.5:7b"
    assert captured["base_url"] == "http://192.168.1.50:11434"

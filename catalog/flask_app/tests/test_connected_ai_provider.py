from __future__ import annotations

from dataclasses import replace
from argparse import Namespace
import json

from flask import Flask
import pytest

from catalog.flask_app import ai_routes, server_setup_routes
from catalog.flask_app.services import server_setup_service
from catalog.flask_app.services.server_setup_service import (
    COMMAND_ONLY_DEPLOYMENT_MODES,
    ServerSetupError,
    ai_settings_from_form,
    compose_profiles_for,
    default_settings,
    load_settings,
    migrate_legacy_phone_bootstrap,
    normalize_ollama_base_url,
    ollama_status,
    runtime_should_start,
    save_settings,
    settings_from_form,
)
import setup_msh
from setup_msh import _run_compose, _settings_from_args


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

    assert payload["schema"] == "msh.server_setup.v3"
    assert payload["user_setup_complete"] is True
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
    assert settings.user_setup_complete is True


def test_legacy_phone_defaults_become_pending_browser_setup_once(tmp_path) -> None:
    path = tmp_path / "server_settings.json"
    path.write_text(
        json.dumps(
            {
                "schema": "msh.server_setup.v2",
                "configured": True,
                "deployment_mode": "web-workbench",
                "ai_enabled": False,
                "ai_provider_mode": "local",
                "ollama_base_url": "http://ollama:11434",
                "recorder_sources": "",
                "recorder_poll_interval": "0.2",
                "recorder_include_condition": False,
            }
        ),
        encoding="utf-8",
    )

    assert migrate_legacy_phone_bootstrap(path) is True
    migrated = load_settings(path)
    assert migrated.configured is False
    assert migrated.user_setup_complete is False
    assert migrate_legacy_phone_bootstrap(path) is False


def test_phone_bootstrap_migration_preserves_custom_provider(tmp_path) -> None:
    path = tmp_path / "server_settings.json"
    path.write_text(
        json.dumps(
            {
                "schema": "msh.server_setup.v2",
                "configured": True,
                "deployment_mode": "web-workbench",
                "ai_enabled": True,
                "ai_provider_mode": "connected",
                "ai_provider_name": "Laptop",
                "ollama_base_url": "http://192.168.1.50:11434",
                "recorder_sources": "",
            }
        ),
        encoding="utf-8",
    )

    assert migrate_legacy_phone_bootstrap(path) is False
    assert load_settings(path).ai_provider_name == "Laptop"


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
    assert settings.user_setup_complete is True
    assert "ai" not in compose_profiles_for(settings).split(",")


def test_command_setup_can_create_edge_model_provider_node() -> None:
    settings = _settings_from_args(
        Namespace(
            mode="language-model-provider",
            ai_profile=None,
            no_ai=False,
            ai_provider="local",
            ai_provider_name="",
            ollama_url="",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
        )
    )

    assert "language-model-provider" in COMMAND_ONLY_DEPLOYMENT_MODES
    assert settings.deployment_mode == "language-model-provider"
    assert settings.ai_enabled is True
    assert settings.ai_provider_mode == "local"
    assert settings.ai_profile == "edge-small"
    assert settings.ai_model == "smollm2:360m"
    assert compose_profiles_for(settings) == "provider"
    assert "MSH_PROVIDER_BIND=0.0.0.0" in server_setup_service.env_lines_for(settings)
    assert "MSH_PROVIDER_MODEL=smollm2:360m" in server_setup_service.env_lines_for(settings)


def test_provider_node_starts_only_provider_and_installs_model(monkeypatch) -> None:
    settings = _settings_from_args(
        Namespace(
            mode="language-model-provider",
            ai_profile="edge-small",
            no_ai=False,
            ai_provider="local",
            ai_provider_name="",
            ollama_url="",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
        )
    )
    calls: list[tuple[list[str], dict[str, str], bool]] = []

    def fake_run(command, *, env, check):
        calls.append((command, env, check))

    monkeypatch.setattr(setup_msh.subprocess, "run", fake_run)

    _run_compose(settings, pull_model=True, start=True)

    assert [call[0] for call in calls] == [
        ["docker", "compose", "--profile", "provider", "up", "-d", "model-provider"],
        ["docker", "compose", "--profile", "provider", "run", "--rm", "model-provider-install"],
    ]
    assert all(call[2] is True for call in calls)
    assert calls[0][1]["MSH_PROVIDER_MODEL"] == "smollm2:360m"


def test_provider_node_cannot_disable_or_delegate_its_model() -> None:
    base = dict(
        mode="language-model-provider",
        ai_profile="edge-small",
        ai_provider_name="",
        ollama_url="",
        recorder_sources="",
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
    )
    with pytest.raises(SystemExit, match="--no-ai"):
        _settings_from_args(Namespace(**base, no_ai=True, ai_provider="local"))
    with pytest.raises(SystemExit, match="own local model"):
        _settings_from_args(Namespace(**base, no_ai=False, ai_provider="connected"))


def test_phone_bootstrap_can_leave_browser_setup_pending() -> None:
    settings = _settings_from_args(
        Namespace(
            mode="web-workbench",
            ai_profile="laptop-standard",
            no_ai=True,
            ai_provider="local",
            ai_provider_name="",
            ollama_url="",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
            browser_setup_pending=True,
        )
    )

    assert settings.configured is False
    assert settings.user_setup_complete is False
    assert runtime_should_start(settings) is False


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

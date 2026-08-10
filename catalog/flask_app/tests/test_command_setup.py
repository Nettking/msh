from __future__ import annotations

from argparse import Namespace
import json
import sys

import pytest

from catalog.command_setup import (
    build_command_plan,
    env_lines_for_plan,
    normalize_command_profile,
)
from catalog.flask_app.services.capability_config_service import CapabilityConfigError
import setup_msh


def _plan(**overrides):
    values = {
        "profile": "workbench",
        "ai_profile": "laptop-standard",
        "no_ai": False,
        "ai_provider": "local",
        "ai_provider_name": "",
        "ollama_url": "",
        "recorder_sources": "",
        "recorder_poll_interval": "0.2",
        "recorder_include_condition": False,
    }
    values.update(overrides)
    return build_command_plan(**values)


def test_legacy_role_names_are_only_command_profile_aliases() -> None:
    assert normalize_command_profile("full-server") == "workbench"
    assert normalize_command_profile("web-workbench") == "workbench"
    assert normalize_command_profile("web-ui-only") == "web-only"
    assert normalize_command_profile("recorder-only") == "recorder"
    assert normalize_command_profile("prep-only") == "prep"
    assert normalize_command_profile("observer-sync-only") == "observer-sync"

    plan = _plan(profile="full-server", no_ai=True)
    assert plan.profile == "workbench"
    assert plan.config.to_dict().keys() == {
        "ai_provider_mode",
        "ai_provider_name",
        "ai_profile",
        "ai_model",
        "ollama_base_url",
        "recorder_sources",
        "recorder_poll_interval",
        "recorder_include_condition",
        "updated_at",
    }
    assert all("MSH_DEPLOYMENT_MODE" not in line for line in env_lines_for_plan(plan))


def test_command_plan_can_configure_connected_provider_without_authority_fields() -> None:
    plan = _plan(
        ai_profile="workstation-strong",
        ai_provider="connected",
        ai_provider_name="Laptop",
        ollama_url="http://192.168.1.50:11434/",
    )

    assert plan.config.ai_provider_mode == "connected"
    assert plan.config.ai_provider_name == "Laptop"
    assert plan.config.ollama_base_url == "http://192.168.1.50:11434"
    assert plan.config.ai_model == "qwen2.5:7b"
    assert plan.provision_local_model is False
    payload = plan.config.to_dict()
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload
    assert "configured" not in payload
    assert "user_setup_complete" not in payload


def test_provider_profile_is_local_and_uses_only_real_compose_profile() -> None:
    plan = _plan(
        profile="language-model-provider",
        ai_profile=None,
    )

    assert plan.provider_node is True
    assert plan.compose_profile == "provider"
    assert plan.config.ai_profile == "edge-small"
    assert plan.config.ai_model == "smollm2:360m"
    assert "COMPOSE_PROFILES=provider" in env_lines_for_plan(plan)
    assert "MSH_PROVIDER_MODEL=smollm2:360m" in env_lines_for_plan(plan)


def test_provider_profile_cannot_disable_or_delegate_its_model() -> None:
    with pytest.raises(CapabilityConfigError, match="--no-ai"):
        _plan(profile="language-model-provider", no_ai=True)
    with pytest.raises(CapabilityConfigError, match="own local model"):
        _plan(
            profile="language-model-provider",
            ai_provider="connected",
            ai_provider_name="Laptop",
            ollama_url="http://192.168.1.50:11434",
        )


def test_old_mode_flag_is_retained_as_cli_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["setup_msh.py", "--mode", "web-workbench", "--no-ai"],
    )

    args = setup_msh.parse_args()

    assert args.profile == "web-workbench"
    assert args.no_ai is True


def test_fresh_command_setup_writes_capability_config_not_legacy_role(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        setup_msh,
        "parse_args",
        lambda: Namespace(
            profile="web-workbench",
            ai_profile="laptop-standard",
            no_ai=True,
            ai_provider="local",
            ai_provider_name="",
            ollama_url="",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
            web_bind="127.0.0.1",
            web_port="5001",
            pull_model=False,
            start=False,
            browser_setup_pending=False,
            migrate_legacy_phone_bootstrap=False,
        ),
    )
    calls = []
    monkeypatch.setattr(
        setup_msh,
        "_run_compose",
        lambda plan, *, pull_model, start: calls.append((plan, pull_model, start)),
    )

    assert setup_msh.main() == 0

    config_path = tmp_path / "data" / "capabilities" / "config.json"
    legacy_path = tmp_path / "data" / "server_setup" / "server_settings.json"
    env_path = tmp_path / ".env"
    assert config_path.is_file()
    assert not legacy_path.exists()
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "msh.capability_config.v1"
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload
    assert "configured" not in payload
    env_text = env_path.read_text(encoding="utf-8")
    assert "MSH_DEPLOYMENT_MODE" not in env_text
    assert "MSH_SKIP_ORCHESTRATION=0" in env_text
    assert calls[0][0].profile == "workbench"
    assert calls[0][1:] == (False, False)


def test_provider_node_starts_only_provider_and_installs_model(monkeypatch) -> None:
    plan = _plan(profile="language-model-provider", ai_profile="edge-small")
    calls: list[tuple[list[str], dict[str, str], bool]] = []

    def fake_run(command, *, env, check):
        calls.append((command, env, check))

    monkeypatch.setattr(setup_msh.subprocess, "run", fake_run)

    setup_msh._run_compose(plan, pull_model=True, start=True)

    assert [call[0] for call in calls] == [
        [
            "docker",
            "compose",
            "--profile",
            "provider",
            "up",
            "-d",
            "model-provider",
        ],
        [
            "docker",
            "compose",
            "--profile",
            "provider",
            "run",
            "--rm",
            "model-provider-install",
        ],
    ]
    assert all(call[2] is True for call in calls)
    assert calls[0][1]["MSH_PROVIDER_MODEL"] == "smollm2:360m"


def test_one_shot_profile_does_not_persist_product_configuration(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        setup_msh,
        "parse_args",
        lambda: Namespace(
            profile="prep",
            ai_profile=None,
            no_ai=True,
            ai_provider="local",
            ai_provider_name="",
            ollama_url="",
            recorder_sources="",
            recorder_poll_interval="0.2",
            recorder_include_condition=False,
            web_bind="0.0.0.0",
            web_port="5000",
            pull_model=False,
            start=False,
            browser_setup_pending=False,
            migrate_legacy_phone_bootstrap=False,
        ),
    )
    monkeypatch.setattr(setup_msh, "_run_compose", lambda *_args, **_kwargs: None)

    assert setup_msh.main() == 0

    assert not (tmp_path / "data" / "capabilities" / "config.json").exists()
    assert "COMPOSE_PROFILES=prep" in (tmp_path / ".env").read_text(encoding="utf-8")

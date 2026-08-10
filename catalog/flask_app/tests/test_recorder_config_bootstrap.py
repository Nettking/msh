from __future__ import annotations

import json
from dataclasses import replace

import pytest

from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    load_capability_config,
    save_capability_config,
)
from catalog.flask_app.services.server_setup_service import (
    default_settings,
    save_settings,
)
from catalog.mtconnect_recorder.config_bootstrap import (
    ensure_recorder_capability_config,
)


def test_upgrade_bootstrap_projects_legacy_technical_values_without_role(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    legacy = replace(
        default_settings(configured=True),
        deployment_mode="web-ui-only",
        ai_enabled=False,
        recorder_sources="Mazak=http://192.168.200.10:5000/current",
        recorder_poll_interval="0.5",
        recorder_include_condition=True,
    )
    save_settings(legacy, legacy_path)

    migrated = ensure_recorder_capability_config(
        config_path=config_path,
        legacy_path=legacy_path,
    )

    assert migrated is True
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "fcp.capability_config.v1"
    assert payload["recorder_sources"] == "Mazak=http://192.168.200.10:5000"
    assert payload["recorder_poll_interval"] == "0.5"
    assert payload["recorder_include_condition"] is True
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload


def test_upgrade_bootstrap_never_overwrites_existing_capability_config(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    current = CapabilityConfig(
        ai_provider_mode="local",
        ai_provider_name="This computer",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url="http://ollama:11434",
        recorder_sources="Current=http://10.0.0.10:5000",
        recorder_poll_interval="0.75",
        recorder_include_condition=False,
        updated_at="2026-08-10T02:00:00Z",
    )
    save_capability_config(current, config_path)
    save_settings(
        replace(
            default_settings(configured=True),
            deployment_mode="recorder-only",
            recorder_sources="Legacy=http://10.0.0.99:5000",
        ),
        legacy_path,
    )

    migrated = ensure_recorder_capability_config(
        config_path=config_path,
        legacy_path=legacy_path,
    )

    assert migrated is False
    restored = load_capability_config(config_path)
    assert restored.recorder_sources == "Current=http://10.0.0.10:5000"
    assert restored.recorder_poll_interval == "0.75"


def test_upgrade_bootstrap_is_noop_without_legacy_input(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"

    migrated = ensure_recorder_capability_config(
        config_path=config_path,
        legacy_path=tmp_path / "missing.json",
    )

    assert migrated is False
    assert not config_path.exists()


def test_upgrade_bootstrap_rejects_corrupt_legacy_input(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    legacy_path = tmp_path / "server_settings.json"
    legacy_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="could not be migrated safely"):
        ensure_recorder_capability_config(
            config_path=config_path,
            legacy_path=legacy_path,
        )

    assert not config_path.exists()

from __future__ import annotations

import json

import pytest

from catalog.flask_app.services.capability_config_migration_service import (
    persist_migrated_capability_config,
)
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    load_capability_config,
    save_capability_config,
)
from catalog.flask_app.services.legacy_settings_migration import (
    LegacySettingsMigrationError,
    load_legacy_settings,
)


def _write_legacy(path, **overrides) -> None:
    payload = {
        "schema": "fcp.server_setup.v3",
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "web-ui-only",
        "ai_enabled": False,
        "ai_provider_mode": "local",
        "ai_provider_name": "This computer",
        "ai_profile": "laptop-standard",
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
        "recorder_sources": "Mazak=http://192.168.200.10:5000/current",
        "recorder_poll_interval": "0.5",
        "recorder_include_condition": True,
        "updated_at": "2026-08-10T02:00:00Z",
    }
    payload.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_explicit_upgrade_projects_legacy_technical_values_without_role(
    tmp_path,
) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    _write_legacy(legacy_path)

    legacy = load_legacy_settings(legacy_path)
    assert legacy is not None
    migrated = persist_migrated_capability_config(legacy, path=config_path)

    assert migrated.recorder_sources == "Mazak=http://192.168.200.10:5000"
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "fcp.capability_config.v1"
    assert payload["recorder_sources"] == "Mazak=http://192.168.200.10:5000"
    assert payload["recorder_poll_interval"] == "0.5"
    assert payload["recorder_include_condition"] is True
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload


def test_explicit_upgrade_never_overwrites_existing_capability_config(
    tmp_path,
) -> None:
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
    _write_legacy(
        legacy_path,
        deployment_mode="recorder-only",
        recorder_sources="Legacy=http://10.0.0.99:5000",
    )

    legacy = load_legacy_settings(legacy_path)
    assert legacy is not None
    migrated = persist_migrated_capability_config(legacy, path=config_path)

    assert migrated == load_capability_config(config_path)
    assert migrated.recorder_sources == "Current=http://10.0.0.10:5000"
    assert migrated.recorder_poll_interval == "0.75"


def test_explicit_upgrade_is_noop_without_legacy_input(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"

    legacy = load_legacy_settings(tmp_path / "missing.json")

    assert legacy is None
    assert not config_path.exists()


def test_explicit_upgrade_rejects_corrupt_legacy_input(tmp_path) -> None:
    config_path = tmp_path / "capabilities" / "config.json"
    legacy_path = tmp_path / "server_settings.json"
    legacy_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(
        LegacySettingsMigrationError,
        match="Could not read legacy server settings",
    ):
        load_legacy_settings(legacy_path)

    assert not config_path.exists()

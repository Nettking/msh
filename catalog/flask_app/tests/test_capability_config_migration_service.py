from __future__ import annotations

import json

from catalog.flask_app.services.capability_config_migration_service import persist_migrated_capability_config
from catalog.flask_app.services.capability_config_service import CapabilityConfig, load_capability_config, save_capability_config
from catalog.flask_app.services.legacy_settings_migration import LegacySettingsSnapshot


def _legacy(**changes) -> LegacySettingsSnapshot:
    values = {
        "schema": "fcp.server_setup.v3",
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": "full-server",
        "ai_enabled": True,
        "ai_provider_mode": "connected",
        "ai_provider_name": "Laptop",
        "ai_profile": "workstation-strong",
        "ai_model": "qwen2.5:7b",
        "ollama_base_url": "http://192.168.1.50:11434",
        "recorder_sources": "Mazak=http://192.168.200.10:5000/current",
        "recorder_poll_interval": "0.5",
        "recorder_include_condition": True,
        "updated_at": "2026-08-10T01:00:00Z",
    }
    values.update(changes)
    return LegacySettingsSnapshot(**values)


def test_transition_projects_legacy_technical_values_without_role_fields(tmp_path) -> None:
    path = tmp_path / "config.json"
    persisted = persist_migrated_capability_config(_legacy(), path=path)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert persisted.ai_provider_name == "Laptop"
    assert persisted.recorder_sources == "Mazak=http://192.168.200.10:5000"
    assert payload["schema"] == "fcp.capability_config.v1"
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload
    assert "configured" not in payload
    assert "user_setup_complete" not in payload


def test_transition_preserves_existing_capability_config(tmp_path) -> None:
    path = tmp_path / "config.json"
    current = CapabilityConfig(
        ai_provider_mode="connected",
        ai_provider_name="Current provider",
        ai_profile="workstation-strong",
        ai_model="qwen2.5:7b",
        ollama_base_url="http://10.0.0.50:11434",
        recorder_sources="CurrentMachine=http://10.0.0.60:5000",
        recorder_poll_interval="0.75",
        recorder_include_condition=True,
        updated_at="2026-08-10T01:00:00Z",
    )
    save_capability_config(current, path)
    legacy = _legacy(
        deployment_mode="web-workbench",
        ai_enabled=False,
        ai_provider_name="Previous provider",
        recorder_sources="PreviousMachine=http://10.0.0.99:5000",
    )

    persisted = persist_migrated_capability_config(legacy, path=path)

    assert persisted == load_capability_config(path)
    assert persisted.ai_provider_name == "Current provider"
    assert persisted.recorder_sources == "CurrentMachine=http://10.0.0.60:5000"
    assert persisted.recorder_poll_interval == "0.75"

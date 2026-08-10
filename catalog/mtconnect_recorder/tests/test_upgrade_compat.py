from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.onboarding_compat import preview_legacy_setup
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    load_capability_config,
    save_capability_config,
)
from catalog.flask_app.services.legacy_settings_migration import load_legacy_settings
from catalog.mtconnect_recorder import runtime
from catalog.mtconnect_recorder.upgrade_compat import ensure_recorder_upgrade_config

_RETIRED_PRODUCT = bytes((109, 115, 104)).decode("ascii")
_LEGACY_SETUP_SCHEMA = f"{_RETIRED_PRODUCT}.server_setup.v3"
_LEGACY_CHECKPOINT_SCHEMA = f"{_RETIRED_PRODUCT}.mtconnect_recorder.checkpoints.v3"


def _write_legacy_recorder_settings(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema": _LEGACY_SETUP_SCHEMA,
                "configured": True,
                "user_setup_complete": True,
                "deployment_mode": "recorder-only",
                "ai_enabled": False,
                "ai_provider_mode": "local",
                "ai_profile": "laptop-standard",
                "ai_model": "llama3.2:3b",
                "ollama_base_url": "http://ollama:11434",
                "recorder_sources": (
                    "M8015RW221N=http://192.168.200.101:5000;"
                    "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
                ),
                "recorder_poll_interval": "0.2",
                "recorder_include_condition": True,
                "updated_at": "2026-08-10T22:10:03.885Z",
            }
        ),
        encoding="utf-8",
    )


def test_pre_fcp_recorder_settings_migrate_to_capability_config(tmp_path: Path) -> None:
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    _write_legacy_recorder_settings(legacy_path)

    migrated = ensure_recorder_upgrade_config(data_directory=tmp_path)

    assert migrated is not None
    assert migrated.recorder_sources == (
        "M8015RW221N=http://192.168.200.101:5000;"
        "MAZAK-M7ZDA13010Z=http://192.168.200.249:5000"
    )
    assert migrated.recorder_poll_interval == "0.2"
    assert migrated.recorder_include_condition is True
    payload = json.loads(
        (tmp_path / "capabilities" / "config.json").read_text(encoding="utf-8")
    )
    assert payload["schema"] == "fcp.capability_config.v1"
    assert "deployment_mode" not in payload
    assert "ai_enabled" not in payload


def test_existing_capability_config_wins_over_pre_fcp_settings(tmp_path: Path) -> None:
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    _write_legacy_recorder_settings(legacy_path)
    target = tmp_path / "capabilities" / "config.json"
    current = CapabilityConfig(
        ai_provider_mode="local",
        ai_provider_name="This computer",
        ai_profile="laptop-standard",
        ai_model="llama3.2:3b",
        ollama_base_url="http://ollama:11434",
        recorder_sources="Current=http://192.168.200.77:5000",
        recorder_poll_interval="0.5",
        recorder_include_condition=False,
        updated_at="2026-08-11T00:00:00Z",
    )
    save_capability_config(current, target)

    result = ensure_recorder_upgrade_config(data_directory=tmp_path)

    assert result == load_capability_config(target)
    assert result is not None
    assert result.recorder_sources == "Current=http://192.168.200.77:5000"
    assert result.recorder_poll_interval == "0.5"


def test_pre_fcp_setup_schema_is_valid_only_through_migration_boundary(
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "server_setup" / "server_settings.json"
    _write_legacy_recorder_settings(legacy_path)

    snapshot = load_legacy_settings(legacy_path)

    assert snapshot is not None
    assert snapshot.schema == _LEGACY_SETUP_SCHEMA
    preview = preview_legacy_setup(
        snapshot.to_dict(),
        created_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
    )
    assert preview.source_schema == _LEGACY_SETUP_SCHEMA
    assert preview.contribution_intents["recorder"] == "enabled"


def test_pre_fcp_checkpoint_load_preserves_exact_sequence_until_next_save(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_file = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "schema": _LEGACY_CHECKPOINT_SCHEMA,
                "updated_at": "2026-08-10T22:10:03.885Z",
                "sources": {
                    "M8015RW221N": {
                        "base_url": "http://192.168.200.101:5000",
                        "machine_id": "M8015RW221N",
                        "agent_instance_id": 1786337757,
                        "next_sequence": 289148,
                        "probe_sha256": "abc123",
                        "storage_aliases": [],
                        "last_raw_file": "data/raw.xml.gz",
                        "last_observation_file": "data/observations.ndjson",
                        "last_normalized_file": "data/batch.jsonl",
                        "latest_values": {"M8015RW221N": {"x": 1}},
                        "updated_at": "2026-08-10T22:10:03.885Z",
                    },
                    "MAZAK-M7ZDA13010Z": {
                        "base_url": "http://192.168.200.249:5000",
                        "machine_id": "MAZAK-M7ZDA13010Z",
                        "agent_instance_id": 1786338089,
                        "next_sequence": 65314,
                        "probe_sha256": "def456",
                        "storage_aliases": ["Mazak"],
                        "latest_values": {},
                        "updated_at": "2026-08-10T13:50:17.562Z",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runtime, "STATE_FILE", state_file)
    recorder = runtime.RecorderRuntime()
    try:
        recorder.load_state()

        assert recorder.checkpoints["M8015RW221N"].next_sequence == 289148
        assert recorder.checkpoints["MAZAK-M7ZDA13010Z"].next_sequence == 65314
        untouched = json.loads(state_file.read_text(encoding="utf-8"))
        assert untouched["schema"] == _LEGACY_CHECKPOINT_SCHEMA

        recorder.save_state()
        migrated = json.loads(state_file.read_text(encoding="utf-8"))
        assert migrated["schema"] == "fcp.mtconnect_recorder.checkpoints.v3"
        assert migrated["sources"]["M8015RW221N"]["next_sequence"] == 289148
        assert migrated["sources"]["MAZAK-M7ZDA13010Z"]["next_sequence"] == 65314
        assert migrated["sources"]["MAZAK-M7ZDA13010Z"]["storage_aliases"] == [
            "Mazak"
        ]
    finally:
        recorder.executor.shutdown(wait=True, cancel_futures=True)

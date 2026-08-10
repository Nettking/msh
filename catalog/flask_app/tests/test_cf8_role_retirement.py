from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.onboarding_compat import preview_legacy_setup
from catalog.flask_app.app import create_app
from catalog.flask_app.services.capability_config_migration_service import (
    persist_migrated_capability_config,
)
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    default_capability_config,
    load_capability_config,
    save_capability_config,
)
from catalog.flask_app.services.legacy_settings_migration import (
    capability_config_from_legacy,
    load_legacy_settings,
)

ROOT = Path(__file__).resolve().parents[3]
MIGRATION_ROLE_FILES = {
    ROOT / "catalog" / "federation" / "onboarding_compat.py",
    ROOT
    / "catalog"
    / "flask_app"
    / "services"
    / "legacy_settings_migration.py",
}


@pytest.mark.parametrize(
    ("mode", "expected_enabled"),
    [
        ("full-server", {"workbench", "runtime", "recorder", "language-model"}),
        ("web-workbench", {"workbench", "runtime", "language-model"}),
        ("web-ui-only", {"workbench", "language-model"}),
        ("recorder-only", {"recorder"}),
        ("language-model-provider", {"language-model"}),
    ],
)
def test_supported_legacy_fixtures_migrate_deterministically(
    tmp_path: Path,
    mode: str,
    expected_enabled: set[str],
) -> None:
    legacy_path = tmp_path / "server_settings.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema": "fcp.server_setup.v3",
                "configured": True,
                "user_setup_complete": True,
                "deployment_mode": mode,
                "ai_enabled": True,
                "ai_provider_mode": "connected",
                "ai_provider_name": "Lab model host",
                "ai_profile": "workstation-strong",
                "ai_model": "qwen2.5:7b",
                "ollama_base_url": "http://192.168.10.20:11434",
                "recorder_sources": "Machine=http://192.168.10.30:5000/current",
                "recorder_poll_interval": "0.5",
                "recorder_include_condition": True,
                "updated_at": "2026-08-10T05:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    first = load_legacy_settings(legacy_path)
    second = load_legacy_settings(legacy_path)
    assert first == second
    assert first is not None

    fixed_time = datetime(2026, 8, 10, 5, 0, tzinfo=timezone.utc)
    preview_a = preview_legacy_setup(first.to_dict(), created_at=fixed_time)
    preview_b = preview_legacy_setup(second.to_dict(), created_at=fixed_time)
    assert preview_a.to_dict() == preview_b.to_dict()
    enabled = {
        key
        for key, value in preview_a.contribution_intents.items()
        if value == "enabled"
    }
    assert enabled == expected_enabled

    config = capability_config_from_legacy(first)
    assert isinstance(config, CapabilityConfig)
    assert config.ai_model == "qwen2.5:7b"
    assert config.recorder_sources == "Machine=http://192.168.10.30:5000/current"
    assert "deployment_mode" not in config.to_dict()
    assert "configured" not in config.to_dict()


def test_untouched_phone_default_is_not_accepted_as_completed_setup(
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "server_settings.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema": "fcp.server_setup.v3",
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

    snapshot = load_legacy_settings(legacy_path)
    assert snapshot is not None
    assert snapshot.configured is True
    assert snapshot.user_setup_complete is False


def test_capability_config_never_implicitly_reads_legacy_file(tmp_path: Path) -> None:
    legacy_path = tmp_path / "server_settings.json"
    capability_path = tmp_path / "capabilities" / "config.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema": "fcp.server_setup.v3",
                "configured": True,
                "user_setup_complete": True,
                "deployment_mode": "recorder-only",
                "recorder_sources": "Legacy=http://192.168.1.20:5000/current",
            }
        ),
        encoding="utf-8",
    )

    config = load_capability_config(capability_path)
    assert config == default_capability_config()
    assert config.recorder_sources == ""
    assert not capability_path.exists()


def test_explicit_migration_is_idempotent_and_new_config_wins(tmp_path: Path) -> None:
    legacy_path = tmp_path / "server_settings.json"
    capability_path = tmp_path / "capabilities" / "config.json"
    legacy_path.write_text(
        json.dumps(
            {
                "schema": "fcp.server_setup.v3",
                "configured": True,
                "user_setup_complete": True,
                "deployment_mode": "full-server",
                "ai_enabled": True,
                "ai_profile": "edge-small",
                "ai_model": "smollm2:360m",
                "recorder_sources": "Legacy=http://192.168.1.20:5000/current",
            }
        ),
        encoding="utf-8",
    )
    legacy = load_legacy_settings(legacy_path)
    assert legacy is not None

    migrated = persist_migrated_capability_config(legacy, path=capability_path)
    assert migrated.recorder_sources.startswith("Legacy=")

    newer = CapabilityConfig(
        **{
            **migrated.to_dict(),
            "recorder_sources": "New=http://192.168.1.21:5000/current",
            "updated_at": "2026-08-10T06:00:00Z",
        }
    )
    save_capability_config(newer, capability_path)
    repeated = persist_migrated_capability_config(legacy, path=capability_path)
    assert repeated.recorder_sources.startswith("New=")


def test_product_code_has_no_legacy_role_model_or_role_derived_authority() -> None:
    banned = {
        "ServerSetupSettings",
        "DEPLOYMENT_MODES",
        "ROLE_CAPABILITIES",
        "settings_from_form",
        "role_uses_ai",
        "role_has_recorder",
        "compatibility_settings",
        "_compatibility_mode",
        "legacy_recorder_role",
        "legacy_ai_role",
        "legacy_workbench_role",
    }
    violations: list[str] = []
    for path in (ROOT / "catalog").rglob("*.py"):
        if "tests" in path.parts or path in MIGRATION_ROLE_FILES:
            continue
        text = path.read_text(encoding="utf-8")
        for token in banned:
            if token in text:
                violations.append(f"{path.relative_to(ROOT)}: {token}")
        if "deployment_mode" in text:
            violations.append(
                f"{path.relative_to(ROOT)}: deployment_mode outside migration boundary"
            )
    assert violations == []


def test_critical_product_surfaces_use_capability_config_directly() -> None:
    direct_files = [
        ROOT / "catalog" / "flask_app" / "server_setup_routes.py",
        ROOT / "catalog" / "flask_app" / "ai_routes.py",
        ROOT
        / "catalog"
        / "flask_app"
        / "services"
        / "capability_contribution_service.py",
        ROOT
        / "catalog"
        / "flask_app"
        / "services"
        / "capability_inspection_service.py",
        ROOT
        / "catalog"
        / "flask_app"
        / "services"
        / "capability_recovery_adapters.py",
        ROOT
        / "catalog"
        / "flask_app"
        / "services"
        / "recorder_control_service.py",
    ]
    for path in direct_files:
        text = path.read_text(encoding="utf-8")
        assert "server_setup_service" not in text, path
        assert "load_settings" not in text, path
        assert "deployment_mode" not in text, path


def test_startup_compatibility_ui_and_choice_bridge_are_not_supported(tmp_path: Path) -> None:
    assert not (ROOT / "catalog" / "flask_app" / "templates" / "startup.html").exists()

    app = create_app()
    app.config.update(TESTING=True)
    with app.test_client() as client:
        response = client.post("/startup/choose", data={"mode": "continue_existing"})
    assert response.status_code == 404


def test_capability_config_source_has_no_legacy_file_fallback() -> None:
    source = (
        ROOT
        / "catalog"
        / "flask_app"
        / "services"
        / "capability_config_service.py"
    ).read_text(encoding="utf-8")
    assert "server_settings.json" not in source
    assert "server_setup_service" not in source
    assert "deployment_mode" not in source

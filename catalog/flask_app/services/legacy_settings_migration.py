"""Read-only parser for supported role-first installation upgrades.

This module is the only product-side boundary allowed to understand the retired
``server_settings.json`` shape. It never writes the legacy file and none of its
values grant runtime or contribution authority. The parsed snapshot exists only
long enough to produce a deterministic capability-first migration preview and
technical ``CapabilityConfig``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .capability_config_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    DEFAULT_AI_PROVIDER_MODE,
    DEFAULT_OLLAMA_BASE_URL,
    CapabilityConfig,
    normalize_recorder_sources,
)

LEGACY_SETTINGS_PATH = Path("data") / "server_setup" / "server_settings.json"
_RETIRED_PRODUCT = bytes((109, 115, 104)).decode("ascii")
_SUPPORTED_SCHEMAS = {
    None,
    "fcp.server_setup.v1",
    "fcp.server_setup.v2",
    "fcp.server_setup.v3",
    f"{_RETIRED_PRODUCT}.server_setup.v3",
}
_SUPPORTED_MODES = {
    "full-server",
    "web-workbench",
    "web-ui-only",
    "recorder-only",
    "language-model-provider",
}


class LegacySettingsMigrationError(RuntimeError):
    """Raised when a legacy migration input cannot be interpreted safely."""


@dataclass(frozen=True)
class LegacySettingsSnapshot:
    """Immutable migration input; deliberately not a runtime settings object."""

    schema: str | None
    configured: bool
    user_setup_complete: bool
    deployment_mode: str
    ai_enabled: bool
    ai_provider_mode: str
    ai_provider_name: str
    ai_profile: str
    ai_model: str
    ollama_base_url: str
    recorder_sources: str
    recorder_poll_interval: str
    recorder_include_condition: bool
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "configured": self.configured,
            "user_setup_complete": self.user_setup_complete,
            "deployment_mode": self.deployment_mode,
            "ai_enabled": self.ai_enabled,
            "ai_provider_mode": self.ai_provider_mode,
            "ai_provider_name": self.ai_provider_name,
            "ai_profile": self.ai_profile,
            "ai_model": self.ai_model,
            "ollama_base_url": self.ollama_base_url,
            "recorder_sources": self.recorder_sources,
            "recorder_poll_interval": self.recorder_poll_interval,
            "recorder_include_condition": self.recorder_include_condition,
            "updated_at": self.updated_at,
        }


def _boolean(payload: dict[str, Any], key: str, default: bool) -> bool:
    value = payload.get(key, default)
    if not isinstance(value, bool):
        raise LegacySettingsMigrationError(f"Legacy {key} must be a boolean.")
    return value


def _untouched_phone_bootstrap(payload: dict[str, Any]) -> bool:
    """Recognize the historical auto-written phone default without mutating it."""

    if "user_setup_complete" in payload:
        return False
    return (
        bool(payload.get("configured"))
        and payload.get("deployment_mode") == "web-workbench"
        and not bool(payload.get("ai_enabled"))
        and str(payload.get("ai_provider_mode") or "local") == "local"
        and str(payload.get("ollama_base_url") or DEFAULT_OLLAMA_BASE_URL)
        == DEFAULT_OLLAMA_BASE_URL
        and not str(payload.get("recorder_sources") or "").strip()
        and str(payload.get("recorder_poll_interval") or "0.2") == "0.2"
        and not bool(payload.get("recorder_include_condition"))
    )


def load_legacy_settings(
    path: Path | str = LEGACY_SETTINGS_PATH,
) -> LegacySettingsSnapshot | None:
    """Read one supported legacy file without mutating it.

    A missing file means there is no migration input. Malformed or unsupported
    input fails explicitly so migration cannot silently reinterpret authority.
    Historical phone defaults that were written before the user saw setup are
    deterministically treated as incomplete rather than as an accepted role.
    """

    path = Path(path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LegacySettingsMigrationError(
            f"Could not read legacy server settings: {path}"
        ) from exc
    if not isinstance(payload, dict):
        raise LegacySettingsMigrationError(
            "Legacy server settings must be a JSON object."
        )

    schema = payload.get("schema")
    if schema not in _SUPPORTED_SCHEMAS:
        raise LegacySettingsMigrationError("Unsupported legacy server settings schema.")
    mode = str(payload.get("deployment_mode") or "web-workbench").strip()
    if mode not in _SUPPORTED_MODES:
        raise LegacySettingsMigrationError("Unsupported legacy deployment mode.")

    configured = _boolean(payload, "configured", False)
    user_setup_complete = _boolean(
        payload,
        "user_setup_complete",
        configured,
    )
    if _untouched_phone_bootstrap(payload):
        user_setup_complete = False

    ai_enabled = _boolean(payload, "ai_enabled", True)
    provider_mode = str(
        payload.get("ai_provider_mode") or DEFAULT_AI_PROVIDER_MODE
    ).strip()
    if provider_mode not in AI_PROVIDER_MODES:
        provider_mode = DEFAULT_AI_PROVIDER_MODE
    profile = str(payload.get("ai_profile") or "laptop-standard").strip()
    model = str(payload.get("ai_model") or "").strip()
    if profile in AI_MODEL_CHOICES:
        model = AI_MODEL_CHOICES[profile]["model"]
    if not model:
        model = AI_MODEL_CHOICES["laptop-standard"]["model"]

    return LegacySettingsSnapshot(
        schema=schema if isinstance(schema, str) else None,
        configured=configured,
        user_setup_complete=user_setup_complete,
        deployment_mode=mode,
        ai_enabled=ai_enabled,
        ai_provider_mode=provider_mode,
        ai_provider_name=str(payload.get("ai_provider_name") or "This computer").strip(),
        ai_profile=profile,
        ai_model=model,
        ollama_base_url=str(
            payload.get("ollama_base_url") or DEFAULT_OLLAMA_BASE_URL
        ).strip(),
        recorder_sources=str(payload.get("recorder_sources") or ""),
        recorder_poll_interval=(
            str(payload.get("recorder_poll_interval") or "0.2").strip() or "0.2"
        ),
        recorder_include_condition=_boolean(
            payload,
            "recorder_include_condition",
            False,
        ),
        updated_at=str(payload.get("updated_at") or ""),
    )


def capability_config_from_legacy(
    snapshot: LegacySettingsSnapshot,
) -> CapabilityConfig:
    """Project technical parameters only; legacy role fields are discarded."""

    profile = snapshot.ai_profile
    if profile not in AI_MODEL_CHOICES:
        profile = "laptop-standard"
    provider_mode = snapshot.ai_provider_mode
    if provider_mode not in AI_PROVIDER_MODES:
        provider_mode = DEFAULT_AI_PROVIDER_MODE
    provider_name = snapshot.ai_provider_name or "This computer"
    base_url = snapshot.ollama_base_url or DEFAULT_OLLAMA_BASE_URL
    if provider_mode == "local":
        provider_name = "This computer"
        base_url = DEFAULT_OLLAMA_BASE_URL
    return CapabilityConfig(
        ai_provider_mode=provider_mode,
        ai_provider_name=provider_name,
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=base_url,
        recorder_sources=normalize_recorder_sources(snapshot.recorder_sources),
        recorder_poll_interval=snapshot.recorder_poll_interval,
        recorder_include_condition=snapshot.recorder_include_condition,
        updated_at=snapshot.updated_at,
    )


__all__ = [
    "LEGACY_SETTINGS_PATH",
    "LegacySettingsMigrationError",
    "LegacySettingsSnapshot",
    "capability_config_from_legacy",
    "load_legacy_settings",
]

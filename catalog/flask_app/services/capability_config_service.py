"""Capability-scoped local configuration independent from legacy device roles.

This store owns configuration parameters only.  It deliberately does not persist
startup completion, deployment roles, contribution intent, or operational
authority.  Capability-first state remains the source of truth for whether a
recorder or language-model contribution is enabled.

The legacy ``server_settings.json`` file is accepted only as migration input when
no capability configuration has been persisted yet. Current configuration writes
never mirror values back into the legacy role document.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .server_setup_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    DEFAULT_AI_PROVIDER_MODE,
    DEFAULT_OLLAMA_BASE_URL,
    ServerSetupError,
    ServerSetupSettings,
    default_settings,
    load_settings,
    normalize_ollama_base_url,
    normalize_recorder_sources,
)

CAPABILITY_CONFIG_PATH = Path("data") / "capabilities" / "config.json"
CAPABILITY_CONFIG_SCHEMA = "msh.capability_config.v1"


class CapabilityConfigError(RuntimeError):
    """Raised when capability-local configuration is malformed or unsafe."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _bounded_text(value: object, *, field: str, maximum: int = 200) -> str:
    text = str(value or "").strip()
    if len(text) > maximum or any(ord(character) < 32 for character in text):
        raise CapabilityConfigError(f"Invalid {field}.")
    return text


@dataclass(frozen=True)
class CapabilityConfig:
    """Configuration parameters only; never contribution or runtime authority."""

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


def from_legacy_settings(settings: ServerSetupSettings) -> CapabilityConfig:
    """Project only capability parameters from the old mixed settings object."""

    provider_mode = str(settings.ai_provider_mode or DEFAULT_AI_PROVIDER_MODE)
    if provider_mode not in AI_PROVIDER_MODES:
        provider_mode = DEFAULT_AI_PROVIDER_MODE
    profile = str(settings.ai_profile or "laptop-standard")
    if profile not in AI_MODEL_CHOICES:
        profile = "laptop-standard"
    provider_name = str(settings.ai_provider_name or "This computer").strip()
    base_url = str(settings.ollama_base_url or DEFAULT_OLLAMA_BASE_URL).strip()
    if provider_mode == "local":
        provider_name = "This computer"
        base_url = DEFAULT_OLLAMA_BASE_URL
    return CapabilityConfig(
        ai_provider_mode=provider_mode,
        ai_provider_name=provider_name,
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=base_url,
        recorder_sources=normalize_recorder_sources(settings.recorder_sources),
        recorder_poll_interval=(str(settings.recorder_poll_interval or "0.2").strip() or "0.2"),
        recorder_include_condition=bool(settings.recorder_include_condition),
        updated_at=str(settings.updated_at or ""),
    )


def default_capability_config() -> CapabilityConfig:
    return from_legacy_settings(default_settings(configured=False))


def _decode(payload: object) -> CapabilityConfig:
    if not isinstance(payload, dict):
        raise CapabilityConfigError("Capability configuration must be a JSON object.")
    schema = payload.get("schema")
    if schema != CAPABILITY_CONFIG_SCHEMA:
        raise CapabilityConfigError("Unsupported capability configuration schema.")

    profile = _bounded_text(payload.get("ai_profile"), field="AI profile", maximum=80)
    if profile not in AI_MODEL_CHOICES:
        raise CapabilityConfigError("Unknown AI model profile.")
    provider_mode = _bounded_text(
        payload.get("ai_provider_mode"),
        field="AI provider mode",
        maximum=40,
    )
    if provider_mode not in AI_PROVIDER_MODES:
        raise CapabilityConfigError("Unknown AI provider mode.")

    if provider_mode == "connected":
        provider_name = _bounded_text(
            payload.get("ai_provider_name"),
            field="AI provider name",
            maximum=80,
        )
        if not provider_name:
            raise CapabilityConfigError("Connected AI provider requires a name.")
        try:
            base_url = normalize_ollama_base_url(
                str(payload.get("ollama_base_url") or "")
            )
        except ServerSetupError as exc:
            raise CapabilityConfigError(str(exc)) from exc
    else:
        provider_name = "This computer"
        base_url = DEFAULT_OLLAMA_BASE_URL

    try:
        recorder_sources = normalize_recorder_sources(
            str(payload.get("recorder_sources") or "")
        )
    except ServerSetupError as exc:
        raise CapabilityConfigError(str(exc)) from exc

    poll_interval = _bounded_text(
        payload.get("recorder_poll_interval") or "0.2",
        field="recorder poll interval",
        maximum=32,
    ) or "0.2"
    try:
        if float(poll_interval) <= 0:
            raise ValueError
    except ValueError as exc:
        raise CapabilityConfigError(
            "Recorder poll interval must be a positive number."
        ) from exc

    updated_at = _bounded_text(
        payload.get("updated_at"),
        field="updated timestamp",
        maximum=64,
    )
    return CapabilityConfig(
        ai_provider_mode=provider_mode,
        ai_provider_name=provider_name,
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=base_url,
        recorder_sources=recorder_sources,
        recorder_poll_interval=poll_interval,
        recorder_include_condition=bool(payload.get("recorder_include_condition")),
        updated_at=updated_at,
    )


def load_capability_config(
    path: Path | str = CAPABILITY_CONFIG_PATH,
    *,
    legacy_loader: Callable[[], ServerSetupSettings] = load_settings,
) -> CapabilityConfig:
    """Load the new store, falling back to a read-only legacy projection."""

    path = Path(path)
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CapabilityConfigError(
                f"Could not read capability configuration: {path}"
            ) from exc
        return _decode(payload)

    try:
        return from_legacy_settings(legacy_loader())
    except (OSError, ServerSetupError, TypeError, ValueError):
        return default_capability_config()


def save_capability_config(
    config: CapabilityConfig,
    path: Path | str = CAPABILITY_CONFIG_PATH,
) -> Path:
    if not isinstance(config, CapabilityConfig):
        raise CapabilityConfigError("Expected CapabilityConfig.")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = {
        "schema": CAPABILITY_CONFIG_SCHEMA,
        **replace(config, updated_at=config.updated_at or _utc_now()).to_dict(),
    }
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return path


def update_language_model_config(
    config: CapabilityConfig,
    form: Any,
) -> CapabilityConfig:
    profile = str(form.get("ai_profile") or config.ai_profile or "laptop-standard").strip()
    if profile not in AI_MODEL_CHOICES:
        raise CapabilityConfigError("Unknown AI model profile.")
    provider_mode = str(
        form.get("ai_provider_mode")
        or config.ai_provider_mode
        or DEFAULT_AI_PROVIDER_MODE
    ).strip()
    if provider_mode not in AI_PROVIDER_MODES:
        raise CapabilityConfigError("Unknown AI provider mode.")

    if provider_mode == "connected":
        provider_name = str(form.get("ai_provider_name") or "").strip()
        if not provider_name and config.ai_provider_mode == "connected":
            provider_name = config.ai_provider_name
        if not provider_name:
            raise CapabilityConfigError("Give the connected computer a name.")
        if len(provider_name) > 80:
            raise CapabilityConfigError(
                "The connected computer name must be 80 characters or fewer."
            )
        provider_url = str(form.get("ollama_base_url") or "").strip()
        if not provider_url and config.ai_provider_mode == "connected":
            provider_url = config.ollama_base_url
        try:
            base_url = normalize_ollama_base_url(provider_url)
        except ServerSetupError as exc:
            raise CapabilityConfigError(str(exc)) from exc
    else:
        provider_name = "This computer"
        base_url = DEFAULT_OLLAMA_BASE_URL

    return replace(
        config,
        ai_provider_mode=provider_mode,
        ai_provider_name=provider_name,
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=base_url,
        updated_at=_utc_now(),
    )


def update_recorder_config(
    config: CapabilityConfig,
    *,
    recorder_sources: str | None = None,
    recorder_poll_interval: str | None = None,
    recorder_include_condition: bool | None = None,
) -> CapabilityConfig:
    try:
        sources = normalize_recorder_sources(
            config.recorder_sources if recorder_sources is None else recorder_sources
        )
    except ServerSetupError as exc:
        raise CapabilityConfigError(str(exc)) from exc
    interval = str(
        config.recorder_poll_interval
        if recorder_poll_interval is None
        else recorder_poll_interval
    ).strip() or "0.2"
    try:
        if float(interval) <= 0:
            raise ValueError
    except ValueError as exc:
        raise CapabilityConfigError(
            "Recorder poll interval must be a positive number."
        ) from exc
    include_condition = (
        config.recorder_include_condition
        if recorder_include_condition is None
        else bool(recorder_include_condition)
    )
    return replace(
        config,
        recorder_sources=sources,
        recorder_poll_interval=interval,
        recorder_include_condition=include_condition,
        updated_at=_utc_now(),
    )


def compatibility_settings(
    config: CapabilityConfig,
    *,
    capability: str,
    enabled: bool,
    base: ServerSetupSettings | None = None,
) -> ServerSetupSettings:
    """Build an in-memory adapter for retained legacy helpers.

    The synthetic deployment mode exists only to satisfy old helper signatures;
    it is not persisted by this function and therefore cannot grant authority.
    """

    base = base or default_settings(configured=True)
    deployment_mode = "web-workbench"
    if capability == "recorder":
        deployment_mode = "recorder-only"
    return replace(
        base,
        configured=True,
        user_setup_complete=True,
        deployment_mode=deployment_mode,
        ai_enabled=bool(enabled) if capability == "language-model" else base.ai_enabled,
        ai_provider_mode=config.ai_provider_mode,
        ai_provider_name=config.ai_provider_name,
        ai_profile=config.ai_profile,
        ai_model=config.ai_model,
        ollama_base_url=config.ollama_base_url,
        recorder_sources=config.recorder_sources,
        recorder_poll_interval=config.recorder_poll_interval,
        recorder_include_condition=config.recorder_include_condition,
        updated_at=config.updated_at,
    )


__all__ = [
    "CAPABILITY_CONFIG_PATH",
    "CAPABILITY_CONFIG_SCHEMA",
    "CapabilityConfig",
    "CapabilityConfigError",
    "compatibility_settings",
    "default_capability_config",
    "from_legacy_settings",
    "load_capability_config",
    "save_capability_config",
    "update_language_model_config",
    "update_recorder_config",
]

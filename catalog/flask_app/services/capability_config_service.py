"""Capability-scoped technical configuration with no legacy role authority.

``CapabilityConfig`` is the only runtime configuration shape for AI and recorder
technical parameters.  Legacy ``server_settings.json`` is intentionally not read
here; supported upgrades are handled explicitly by ``legacy_settings_migration``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from catalog.mtconnect_recorder.model import (
    _parse_sources_text,
    _validate_source_storage_names,
)

CAPABILITY_CONFIG_PATH = Path("data") / "capabilities" / "config.json"
CAPABILITY_CONFIG_SCHEMA = "fcp.capability_config.v1"
DEFAULT_OLLAMA_BASE_URL = "http://ollama:11434"
DEFAULT_AI_PROVIDER_MODE = "local"

AI_MODEL_CHOICES: dict[str, dict[str, str]] = {
    "edge-small": {
        "label": "Edge small",
        "model": "smollm2:360m",
        "device": "Small CPU, Raspberry Pi class, or very low memory testing.",
    },
    "laptop-standard": {
        "label": "Laptop standard",
        "model": "llama3.2:3b",
        "device": "Normal laptop or small server. Default balance.",
    },
    "workstation-strong": {
        "label": "Workstation strong",
        "model": "qwen2.5:7b",
        "device": "Gaming laptop, workstation, or GPU server. Stronger answers.",
    },
}

AI_PROVIDER_MODES: dict[str, dict[str, str]] = {
    "local": {
        "label": "This computer",
        "description": "Use the Ollama service running alongside FCP.",
    },
    "connected": {
        "label": "Connected computer",
        "description": (
            "Use an Ollama language model shared by another machine on a trusted network."
        ),
    },
}


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


def normalize_ollama_base_url(value: str) -> str:
    """Validate and normalize an explicitly connected Ollama HTTP endpoint."""

    raw = str(value or "").strip().rstrip("/")
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise CapabilityConfigError(
            "The connected Ollama URL has an invalid port."
        ) from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise CapabilityConfigError(
            "The connected Ollama URL must start with http:// or https:// and include a host."
        )
    if parsed.username or parsed.password:
        raise CapabilityConfigError(
            "Credentials are not supported in the connected Ollama URL."
        )
    if parsed.hostname.lower() in {"localhost", "127.0.0.1", "0.0.0.0", "::", "::1"}:
        raise CapabilityConfigError(
            "Use the connected computer's LAN or VPN address, not localhost or 0.0.0.0."
        )
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise CapabilityConfigError(
            "Use the Ollama server root only, for example http://192.168.1.50:11434."
        )
    if port is not None and not 1 <= port <= 65535:
        raise CapabilityConfigError(
            "The connected Ollama URL has an invalid port."
        )
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def parse_recorder_sources(value: str) -> dict[str, str]:
    """Parse and validate the persisted ``Name=URL;...`` recorder format."""

    try:
        return _parse_sources_text(str(value or "").strip())
    except (TypeError, ValueError) as exc:
        raise CapabilityConfigError(str(exc)) from exc


def format_recorder_sources(sources: dict[str, str]) -> str:
    """Serialize recorder sources deterministically for durable configuration."""

    try:
        _validate_source_storage_names(sources)
    except ValueError as exc:
        raise CapabilityConfigError(str(exc)) from exc
    return ";".join(
        f"{name}={url}"
        for name, url in sorted(sources.items(), key=lambda item: item[0].casefold())
    )


def normalize_recorder_sources(value: str) -> str:
    return format_recorder_sources(parse_recorder_sources(value))


@dataclass(frozen=True)
class CapabilityConfig:
    """Technical parameters only; never contribution or runtime authority."""

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


def default_capability_config() -> CapabilityConfig:
    profile = "laptop-standard"
    return CapabilityConfig(
        ai_provider_mode=DEFAULT_AI_PROVIDER_MODE,
        ai_provider_name="This computer",
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=DEFAULT_OLLAMA_BASE_URL,
        recorder_sources="",
        recorder_poll_interval="0.2",
        recorder_include_condition=False,
        updated_at="",
    )


def _decode(payload: object) -> CapabilityConfig:
    if not isinstance(payload, dict):
        raise CapabilityConfigError("Capability configuration must be a JSON object.")
    if payload.get("schema") != CAPABILITY_CONFIG_SCHEMA:
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
        base_url = normalize_ollama_base_url(
            str(payload.get("ollama_base_url") or "")
        )
    else:
        provider_name = "This computer"
        base_url = DEFAULT_OLLAMA_BASE_URL

    recorder_sources = normalize_recorder_sources(
        str(payload.get("recorder_sources") or "")
    )
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

    return CapabilityConfig(
        ai_provider_mode=provider_mode,
        ai_provider_name=provider_name,
        ai_profile=profile,
        ai_model=AI_MODEL_CHOICES[profile]["model"],
        ollama_base_url=base_url,
        recorder_sources=recorder_sources,
        recorder_poll_interval=poll_interval,
        recorder_include_condition=bool(payload.get("recorder_include_condition")),
        updated_at=_bounded_text(
            payload.get("updated_at"),
            field="updated timestamp",
            maximum=64,
        ),
    )


def load_capability_config(
    path: Path | str = CAPABILITY_CONFIG_PATH,
) -> CapabilityConfig:
    """Load capability configuration without consulting retired role settings."""

    path = Path(path)
    if not path.exists():
        return default_capability_config()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CapabilityConfigError(
            f"Could not read capability configuration: {path}"
        ) from exc
    return _decode(payload)


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
    profile = str(
        form.get("ai_profile") or config.ai_profile or "laptop-standard"
    ).strip()
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
        base_url = normalize_ollama_base_url(provider_url)
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
    sources = normalize_recorder_sources(
        config.recorder_sources if recorder_sources is None else recorder_sources
    )
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


__all__ = [
    "AI_MODEL_CHOICES",
    "AI_PROVIDER_MODES",
    "CAPABILITY_CONFIG_PATH",
    "CAPABILITY_CONFIG_SCHEMA",
    "DEFAULT_AI_PROVIDER_MODE",
    "DEFAULT_OLLAMA_BASE_URL",
    "CapabilityConfig",
    "CapabilityConfigError",
    "default_capability_config",
    "format_recorder_sources",
    "load_capability_config",
    "normalize_ollama_base_url",
    "normalize_recorder_sources",
    "parse_recorder_sources",
    "save_capability_config",
    "update_language_model_config",
    "update_recorder_config",
]

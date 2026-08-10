"""Recovery composition for capability-first inspection."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

from flask import current_app

from catalog.capabilities.benchmarking import (
    MtconnectSourceAdapter,
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
)

from .capability_config_service import (
    CapabilityConfigError,
    load_capability_config,
)
from .local_capability_candidates import local_inspection_adapters
from .mtconnect_discovery_service import get_mtconnect_discovery_service

_NATIVE_LOCAL_OLLAMA_URL = "http://127.0.0.1:11434"


def _running_in_container() -> bool:
    explicit = str(os.environ.get("FCP_RUNTIME_CONTAINERIZED") or "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    if explicit in {"0", "false", "no", "off"}:
        return False
    return Path("/.dockerenv").exists()


def _runtime_ollama_base_url(
    *,
    configured: str,
    provider_mode: str,
) -> str:
    """Resolve the private local Ollama root for Docker and native runtimes."""

    base_url = str(os.environ.get("OLLAMA_BASE_URL") or configured).strip()
    if not base_url:
        return ""
    try:
        parsed = urlsplit(base_url)
    except ValueError:
        return base_url
    host = (parsed.hostname or "").casefold()
    if provider_mode == "local" and host == "ollama" and not _running_in_container():
        return _NATIVE_LOCAL_OLLAMA_URL
    return base_url


def fresh_capability_inspection_adapters() -> tuple[object, ...]:
    """Expose safe configured and built-in evidence without granting authority."""

    adapters: list[object] = [
        MtconnectSourceAdapter(get_mtconnect_discovery_service().last_scan),
        *local_inspection_adapters(),
    ]
    loader = current_app.config.get(
        "CAPABILITY_CONFIG_LOADER",
        load_capability_config,
    )
    try:
        config = loader()
    except (OSError, CapabilityConfigError, TypeError, ValueError):
        return tuple(adapters)

    base_url = _runtime_ollama_base_url(
        configured=config.ollama_base_url,
        provider_mode=config.ai_provider_mode,
    )
    model = str(os.environ.get("FCP_AI_MODEL") or config.ai_model or "").strip()
    if not base_url or not model:
        return tuple(adapters)
    try:
        parsed = urlsplit(base_url)
        expected_host = (parsed.hostname or "").casefold()
        target = OllamaProbeTarget(
            service_id="ollama-configured",
            display_label="Configured Ollama service",
            base_url=base_url,
            model=model,
        )
    except (TypeError, ValueError):
        return tuple(adapters)
    adapters.append(
        OllamaBenchmarkAdapter(
            (target,),
            trusted_host_predicate=(
                lambda host, expected=expected_host: (
                    bool(expected) and host.casefold() == expected
                )
            ),
        )
    )
    return tuple(adapters)


__all__ = ["fresh_capability_inspection_adapters"]

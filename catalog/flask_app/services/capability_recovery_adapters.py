"""Recovery composition for capabilities available before legacy setup is complete."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from flask import current_app

from catalog.capabilities.benchmarking import (
    MtconnectSourceAdapter,
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
)

from .local_capability_candidates import local_inspection_adapters
from .mtconnect_discovery_service import get_mtconnect_discovery_service
from .server_setup_service import ServerSetupError, load_settings

_NATIVE_LOCAL_OLLAMA_URL = "http://127.0.0.1:11434"


def _running_in_container() -> bool:
    """Return whether the supported Flask runtime is running in a container."""

    explicit = str(os.environ.get("FCP_RUNTIME_CONTAINERIZED") or "").strip().lower()
    if explicit in {"1", "true", "yes", "on"}:
        return True
    if explicit in {"0", "false", "no", "off"}:
        return False
    return Path("/.dockerenv").exists()


def _runtime_ollama_base_url(payload: Mapping[str, object]) -> str:
    """Resolve the private local Ollama root for Docker and native runtimes.

    Persisted local setup uses the Docker service name because that is the normal
    deployment. A native Flask process cannot resolve that service name and must
    reach a native Ollama process through loopback instead. Connected providers
    retain their explicit LAN or VPN endpoint.
    """

    configured = str(payload.get("ollama_base_url") or "").strip()
    base_url = str(os.environ.get("OLLAMA_BASE_URL") or configured).strip()
    if not base_url:
        return ""
    try:
        parsed = urlsplit(base_url)
    except ValueError:
        return base_url
    provider_mode = str(payload.get("ai_provider_mode") or "local").strip()
    host = (parsed.hostname or "").casefold()
    if provider_mode == "local" and host == "ollama" and not _running_in_container():
        return _NATIVE_LOCAL_OLLAMA_URL
    return base_url


def fresh_capability_inspection_adapters() -> tuple[object, ...]:
    """Expose configured and safe built-in evidence in Docker and native runs.

    Benchmark visibility is intentionally independent of contribution activation.
    The built-in compute handler is descriptor-only during inspection, and local
    storage uses only a bounded temporary round-trip probe. Neither grants runtime
    authority.
    """

    adapters: list[object] = [
        MtconnectSourceAdapter(get_mtconnect_discovery_service().last_scan),
        *local_inspection_adapters(),
    ]
    loader = current_app.config.get(
        "CAPABILITY_ONBOARDING_SETUP_LOADER",
        load_settings,
    )
    try:
        settings = loader()
    except ServerSetupError:
        return tuple(adapters)
    payload = (
        settings.to_dict()
        if callable(getattr(settings, "to_dict", None))
        else settings
    )
    if not isinstance(payload, Mapping):
        return tuple(adapters)

    base_url = _runtime_ollama_base_url(payload)
    model = str(
        os.environ.get("FCP_AI_MODEL") or payload.get("ai_model") or ""
    ).strip()
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

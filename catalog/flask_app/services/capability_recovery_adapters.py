"""Recovery composition for capabilities available before legacy setup is complete."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

from flask import current_app

from catalog.capabilities.benchmarking import (
    MtconnectSourceAdapter,
    OllamaBenchmarkAdapter,
    OllamaProbeTarget,
)

from .mtconnect_discovery_service import get_mtconnect_discovery_service
from .server_setup_service import ServerSetupError, load_settings


def fresh_capability_inspection_adapters() -> tuple[object, ...]:
    """Expose bundled/configured services during capability-first first run.

    The previous composition required the legacy role-first setup to be marked
    complete before it registered Ollama. A fresh capability-first installation
    therefore reached the benchmark step with no checks. This factory uses the
    same bounded MTConnect and Ollama adapters, but treats a configured default
    Ollama target as inspectable evidence even before legacy setup completion.
    """

    adapters: list[object] = [
        MtconnectSourceAdapter(get_mtconnect_discovery_service().last_scan)
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
    if not isinstance(payload, Mapping) or not bool(payload.get("ai_enabled")):
        return tuple(adapters)

    base_url = str(payload.get("ollama_base_url") or "").strip()
    model = str(payload.get("ai_model") or "").strip()
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

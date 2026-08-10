"""Persist technical capability configuration without reviving legacy roles.

The legacy ``server_settings.json`` document is accepted only as migration input.
Current capability-first completion uses this adapter as the retained CFI-6
``setup_saver`` seam, so the transition service may keep its old call signature
without persisting ``deployment_mode`` or ``ai_enabled`` as authority.
"""

from __future__ import annotations

from pathlib import Path

from catalog.federation.errors import FederationOperationError

from .capability_config_service import (
    CAPABILITY_CONFIG_PATH,
    CapabilityConfig,
    CapabilityConfigError,
    from_legacy_settings,
    load_capability_config,
    save_capability_config,
)
from .server_setup_service import ServerSetupError, ServerSetupSettings


def persist_capability_config_from_setup(
    settings: ServerSetupSettings,
    *,
    path: Path | str = CAPABILITY_CONFIG_PATH,
) -> CapabilityConfig:
    """Persist technical values without letting compatibility overwrite them.

    If capability-first configuration already exists, it is authoritative for
    technical parameters and is preserved verbatim. Otherwise the incoming
    legacy-shaped settings object is projected once into ``CapabilityConfig``.
    Neither path persists role, completion, or contribution authority fields.
    """

    path = Path(path)
    try:
        if path.exists():
            return load_capability_config(
                path,
                legacy_loader=lambda: settings,
            )
        config = from_legacy_settings(settings)
        save_capability_config(config, path)
    except (CapabilityConfigError, ServerSetupError, OSError, TypeError, ValueError) as exc:
        raise FederationOperationError(
            "startup-transition-capability-config-failed",
            "technical capability configuration could not be persisted safely",
            "capability_config",
        ) from exc
    return config


__all__ = ["persist_capability_config_from_setup"]

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
    save_capability_config,
)
from .server_setup_service import ServerSetupError, ServerSetupSettings


def persist_capability_config_from_setup(
    settings: ServerSetupSettings,
    *,
    path: Path | str = CAPABILITY_CONFIG_PATH,
) -> CapabilityConfig:
    """Project and persist technical values from a legacy-shaped settings object.

    The input may be an actual pre-CFI legacy setup or the synthetic object
    produced by the retained CFI-6 compatibility seam.  In both cases only the
    role-free ``CapabilityConfig`` projection is written.
    """

    try:
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

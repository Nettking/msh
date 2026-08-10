"""Deterministic one-way migration from retired role settings to CapabilityConfig."""

from __future__ import annotations

from pathlib import Path

from catalog.federation.errors import FederationOperationError

from .capability_config_service import (
    CAPABILITY_CONFIG_PATH,
    CapabilityConfig,
    CapabilityConfigError,
    load_capability_config,
    save_capability_config,
)
from .legacy_settings_migration import (
    LegacySettingsSnapshot,
    capability_config_from_legacy,
)


def persist_migrated_capability_config(
    legacy: LegacySettingsSnapshot,
    *,
    path: Path | str = CAPABILITY_CONFIG_PATH,
) -> CapabilityConfig:
    """Persist technical configuration once, never writing the legacy file.

    Existing capability configuration wins so repeating migration is idempotent and
    cannot overwrite newer capability-first choices.
    """

    target = Path(path)
    try:
        if target.exists():
            return load_capability_config(target)
        config = capability_config_from_legacy(legacy)
        save_capability_config(config, target)
        return config
    except (CapabilityConfigError, OSError, TypeError, ValueError) as exc:
        raise FederationOperationError(
            "capability-config-migration-failed",
            "legacy technical configuration could not be migrated safely",
            "configuration",
        ) from exc


__all__ = ["persist_migrated_capability_config"]

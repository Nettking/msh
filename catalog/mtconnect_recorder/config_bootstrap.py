"""One-time technical configuration handoff for upgraded recorder deployments.

The managed recorder consumes ``data/capabilities/config.json`` in steady state.
Older installations may predate that store and still have technical recorder
parameters only in ``server_settings.json``.  This bootstrap projects those
parameters once without consulting or persisting legacy role authority.
"""

from __future__ import annotations

import os
from pathlib import Path

from catalog.flask_app.services.capability_config_service import (
    CAPABILITY_CONFIG_PATH,
    CapabilityConfigError,
    from_legacy_settings,
    save_capability_config,
)
from catalog.flask_app.services.server_setup_service import (
    SETTINGS_PATH,
    ServerSetupError,
    load_settings,
)

LEGACY_RECORDER_SETTINGS_PATH = Path(
    os.getenv("MSH_RECORDER_LEGACY_SETTINGS_FILE", str(SETTINGS_PATH))
)


def ensure_recorder_capability_config(
    *,
    config_path: Path | str = CAPABILITY_CONFIG_PATH,
    legacy_path: Path | str = LEGACY_RECORDER_SETTINGS_PATH,
) -> bool:
    """Project legacy technical values once when the current config is absent.

    Returns ``True`` only when a new capability configuration file was written.
    Existing capability configuration always wins. Invalid legacy input is left
    untouched and raises so callers can fail closed rather than invent settings.
    """

    config_path = Path(config_path)
    legacy_path = Path(legacy_path)
    if config_path.exists() or not legacy_path.exists():
        return False

    try:
        legacy = load_settings(legacy_path)
        config = from_legacy_settings(legacy)
        save_capability_config(config, config_path)
    except (CapabilityConfigError, ServerSetupError, OSError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "Legacy recorder configuration could not be migrated safely."
        ) from exc
    return True


__all__ = [
    "LEGACY_RECORDER_SETTINGS_PATH",
    "ensure_recorder_capability_config",
]

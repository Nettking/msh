"""Narrow one-way compatibility bridge for upgraded recorder installations.

The active recorder reads ``CapabilityConfig`` only. Older installed recorders
may still have their technical source selection in ``server_settings.json``.
This module is called explicitly by the managed recorder entry point before the
runtime starts. It projects technical values once, never overwrites an existing
CapabilityConfig, and never derives recorder/contribution authority from the
legacy deployment role.
"""

from __future__ import annotations

from pathlib import Path

from catalog.flask_app.services.capability_config_migration_service import (
    persist_migrated_capability_config,
)
from catalog.flask_app.services.capability_config_service import (
    CapabilityConfig,
    load_capability_config,
)
from catalog.flask_app.services.legacy_settings_migration import (
    load_legacy_settings,
)


def ensure_recorder_upgrade_config(
    *,
    data_directory: Path | str = "data",
) -> CapabilityConfig | None:
    """Create current recorder technical config once from supported legacy input.

    Existing capability-first configuration always wins. A missing legacy file
    is not an error for fresh installs. Malformed/unsupported legacy input is
    allowed to fail closed so an upgraded managed recorder cannot silently start
    with a different source selection.
    """

    root = Path(data_directory)
    target = root / "capabilities" / "config.json"
    if target.exists():
        return load_capability_config(target)

    legacy_path = root / "server_setup" / "server_settings.json"
    legacy = load_legacy_settings(legacy_path)
    if legacy is None:
        return None
    return persist_migrated_capability_config(legacy, path=target)


__all__ = ["ensure_recorder_upgrade_config"]

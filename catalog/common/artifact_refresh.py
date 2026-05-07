"""Process-local artifact catalog refresh notifications.

The runtime runner and manual control actions can produce files that the Flask
artifact catalog should discover, but page requests must not synchronously scan
large results trees. This module lets artifact-producing code request a
best-effort asynchronous catalog refresh when a Flask catalog is registered,
without importing Flask application objects into runner code.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)

RefreshCallback = Callable[[str], bool]

_lock = threading.Lock()
_refresh_callback: RefreshCallback | None = None


def register_artifact_catalog_refresh(callback: RefreshCallback | None) -> None:
    """Register the process-local callback used to refresh the Flask catalog."""
    global _refresh_callback
    with _lock:
        _refresh_callback = callback


def request_artifact_catalog_refresh(*, reason: str) -> bool:
    """Request a non-blocking artifact catalog refresh if one is registered."""
    with _lock:
        callback = _refresh_callback
    if callback is None:
        LOGGER.debug("Artifact catalog refresh requested without registered catalog reason=%s", reason)
        return False
    try:
        return bool(callback(reason))
    except Exception:  # noqa: BLE001
        LOGGER.exception("Artifact catalog refresh callback failed reason=%s", reason)
        return False

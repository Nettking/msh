"""Process-local ownership for capability contribution startup reconciliation."""

from __future__ import annotations

import threading
from collections.abc import Callable, MutableMapping
from typing import TypeVar

_RECONCILE_EXTENSION_KEY = "capability_contribution_startup_reconciled"
_RECONCILE_IN_PROGRESS = "in-progress"
_RECONCILE_LOCK = threading.Lock()
_T = TypeVar("_T")


def run_startup_contribution_reconcile(
    extensions: MutableMapping[str, object],
    operation: Callable[[], _T],
) -> tuple[bool, _T | None, Exception | None]:
    """Run one reconciliation attempt without poisoning later retries.

    ``True`` in the shared extension slot means reconciliation completed for this
    process. ``in-progress`` only owns the current attempt so concurrent startup
    requests cannot activate the same authority twice. A failed attempt releases
    the slot, allowing a later request to retry after relay/Federation startup has
    caught up.
    """

    with _RECONCILE_LOCK:
        if extensions.get(_RECONCILE_EXTENSION_KEY):
            return False, None, None
        extensions[_RECONCILE_EXTENSION_KEY] = _RECONCILE_IN_PROGRESS

    try:
        result = operation()
    except Exception as exc:  # noqa: BLE001 - caller owns fail-closed logging
        with _RECONCILE_LOCK:
            if extensions.get(_RECONCILE_EXTENSION_KEY) == _RECONCILE_IN_PROGRESS:
                extensions.pop(_RECONCILE_EXTENSION_KEY, None)
        return True, None, exc

    with _RECONCILE_LOCK:
        extensions[_RECONCILE_EXTENSION_KEY] = True
    return True, result, None


__all__ = ["run_startup_contribution_reconcile"]

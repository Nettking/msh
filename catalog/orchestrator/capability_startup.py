"""Capability-first adapter for the existing workflow runtime.

The historical runtime engine still stores a namespace mode internally, but product
startup no longer asks the operator to choose one. Capability startup deterministically
preserves the active namespace and starts the runtime when the persisted capability
intent enables it.
"""

from __future__ import annotations

from .pipeline import (
    STARTUP_MODE_CONTINUE,
    RuntimeOrchestrator,
    get_runtime_manager,
    start_runtime_background,
)


def prepare_capability_runtime(
    manager: RuntimeOrchestrator | None = None,
) -> RuntimeOrchestrator:
    """Resolve historical pending runtime state without a UI compatibility choice."""

    manager = manager or get_runtime_manager()
    if not manager.requires_startup_choice():
        return manager
    with manager._lock:  # runtime owns this state; keep the transition serialized
        manager._apply_startup_mode(STARTUP_MODE_CONTINUE, source="capability")
    return manager


def start_capability_runtime_background() -> None:
    prepare_capability_runtime()
    start_runtime_background()


__all__ = [
    "prepare_capability_runtime",
    "start_capability_runtime_background",
]

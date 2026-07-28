"""Thread-safe, deliberately in-memory capability registry."""

from __future__ import annotations

from copy import deepcopy
from threading import RLock

from .errors import FederationValidationError
from .models import CapabilityAnnouncement


class LocalCapabilityRegistry:
    """Process-local registry; callers must not treat its contents as durable."""

    durable = False

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], CapabilityAnnouncement] = {}
        self._lock = RLock()

    def register(self, capability: CapabilityAnnouncement) -> CapabilityAnnouncement:
        key = (capability.session_id, capability.capability_id)
        with self._lock:
            current = self._items.get(key)
            if current is not None and current != capability:
                raise FederationValidationError("capability-identity-conflict", "capability_id",
                                                "scoped identity is already registered")
            self._items[key] = deepcopy(capability)
        return deepcopy(capability)

    def update(self, capability: CapabilityAnnouncement) -> CapabilityAnnouncement:
        key = (capability.session_id, capability.capability_id)
        with self._lock:
            current = self._items.get(key)
            if current is None:
                raise FederationValidationError("capability-not-found", "capability_id",
                                                "scoped identity is not registered")
            if current.node_id != capability.node_id or current.type != capability.type:
                raise FederationValidationError("capability-identity-conflict", "capability_id",
                                                "node and type cannot be reused")
            self._items[key] = deepcopy(capability)
        return deepcopy(capability)

    def get(self, session_id: str, capability_id: str) -> CapabilityAnnouncement | None:
        with self._lock:
            return deepcopy(self._items.get((session_id, capability_id)))

    def list(self, *, session_id: str | None = None, capability_type: str | None = None,
             node_id: str | None = None) -> tuple[CapabilityAnnouncement, ...]:
        with self._lock:
            values = tuple(self._items.values())
        return tuple(deepcopy(sorted((item for item in values
                            if (session_id is None or item.session_id == session_id)
                            and (capability_type is None or item.type == capability_type)
                            and (node_id is None or item.node_id == node_id)),
                           key=lambda item: (item.session_id, item.type, item.node_id,
                                             item.capability_id))))

    def remove(self, session_id: str, capability_id: str) -> CapabilityAnnouncement | None:
        with self._lock:
            return deepcopy(self._items.pop((session_id, capability_id), None))

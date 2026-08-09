"""Adapter for existing federation membership and event authorities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .adapter_common import (
    _enum_value,
    _public_text,
    _sequence,
    _timestamp,
    _value,
)
from .safety import safe_reason_code


@dataclass(frozen=True)
class DeviceRecord:
    node_id: str
    label: str
    state: str
    last_seen_at: datetime | None
    capability_count: int


@dataclass(frozen=True)
class FederatedCapabilityRecord:
    capability_id: str
    node_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    status: str
    last_seen_at: datetime | None


@dataclass(frozen=True)
class ActivityRecord:
    revision: int | None
    occurred_at: datetime | None
    event_type: str
    title: str
    message: str


@dataclass(frozen=True)
class FederationAuthoritySnapshot:
    available: bool
    reason_code: str
    label: str = "Federation"
    state: str = "unavailable"
    revision: int | None = None
    devices: tuple[DeviceRecord, ...] = ()
    activity: tuple[ActivityRecord, ...] = ()
    capabilities: tuple[FederatedCapabilityRecord, ...] = ()


_EVENT_COPY = {
    "session.created": (
        "Federation created",
        "The federation authority was initialized.",
    ),
    "node.joined": ("Device joined", "A trusted device joined the federation."),
    "node.left": ("Device removed", "A device membership was removed."),
    "node.revoked": ("Device revoked", "A device identity was revoked."),
    "capability.announced": (
        "Service announced",
        "A device published an available service.",
    ),
    "capability.updated": (
        "Service updated",
        "A device refreshed a service announcement.",
    ),
    "capability.registered": (
        "Service announced",
        "A trusted device published service metadata.",
    ),
    "capability.status.changed": (
        "Service updated",
        "A trusted device refreshed service metadata.",
    ),
    "capability.health.changed": (
        "Service health changed",
        "Federation service health changed.",
    ),
    "storage.group.created": (
        "Storage group created",
        "A storage group was created.",
    ),
    "storage.assignment.changed": (
        "Storage assignment changed",
        "Storage authority was reassigned.",
    ),
    "storage.leader.granted": (
        "Storage leader granted",
        "The control plane granted bounded write authority.",
    ),
    "storage.leader.revoked": (
        "Storage leader revoked",
        "The control plane revoked write authority.",
    ),
}


class FederationAuthorityAdapter:
    """Read membership/status/event authorities through a private session binding."""

    def __init__(
        self,
        coordinator: object | None,
        *,
        actor_node_id: str,
        internal_session_id: str,
        activity_limit: int = 50,
    ) -> None:
        self._coordinator = coordinator
        self._actor_node_id = actor_node_id
        self._internal_session_id = internal_session_id
        self._activity_limit = max(1, min(int(activity_limit), 100))

    def snapshot(self) -> FederationAuthoritySnapshot:
        if self._coordinator is None:
            return FederationAuthoritySnapshot(
                False,
                "federation-authority-unavailable",
            )
        try:
            store = _value(self._coordinator, "store")
            session = (
                store.get_session(self._internal_session_id)
                if store is not None and hasattr(store, "get_session")
                else None
            )
            label = _public_text(_value(session, "display_name"), "Federation")
            state = _enum_value(_value(session, "state"), "unavailable")
            revision = _value(session, "revision")
            if not isinstance(revision, int):
                revision = None

            events = self._events(revision)
            member_ids = {self._actor_node_id}
            for event in events:
                event_type = _value(event, "event_type")
                payload = _value(event, "payload", {})
                node_id = _value(payload, "node_id")
                if not isinstance(node_id, str):
                    continue
                if event_type == "node.joined":
                    member_ids.add(node_id)
                elif event_type in {"node.left", "node.revoked"}:
                    member_ids.discard(node_id)

            raw_status = self._authorized_status()
            devices = self._devices(raw_status, member_ids)
            capabilities = self._capabilities(raw_status, member_ids)
            activity = tuple(
                self._activity_record(event)
                for event in events[-self._activity_limit :]
            )
            return FederationAuthoritySnapshot(
                available=True,
                reason_code="current",
                label=label,
                state=state,
                revision=revision,
                devices=devices,
                activity=activity,
                capabilities=capabilities,
            )
        except Exception as exc:  # noqa: BLE001
            return FederationAuthoritySnapshot(
                False,
                safe_reason_code(
                    getattr(exc, "code", None),
                    "federation-projection-failed",
                ),
            )

    def _authorized_status(self) -> dict[str, Any]:
        merged: dict[str, list[Any]] = {
            "sessions": [],
            "nodes": [],
            "capabilities": [],
        }
        cursor: str | None = None
        for _ in range(32):
            page = self._coordinator.status(
                actor_node_id=self._actor_node_id,
                cursor=cursor,
            )
            for key, values in merged.items():
                values.extend(_sequence(_value(page, key, ())))
            pagination = _value(page, "pagination", {})
            if not bool(_value(pagination, "has_more", False)):
                return merged
            next_cursor = _value(pagination, "next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                raise RuntimeError("authorized status pagination is incomplete")
            cursor = next_cursor
        raise RuntimeError("authorized status exceeds the projection page bound")

    def _events(self, revision: int | None) -> tuple[Any, ...]:
        if revision is None or revision <= 0:
            return ()
        if hasattr(self._coordinator, "replay_page"):
            events: list[Any] = []
            last_revision = 0
            for _ in range(100):
                page, current_revision = self._coordinator.replay_page(
                    session_id=self._internal_session_id,
                    actor_node_id=self._actor_node_id,
                    last_applied_revision=last_revision,
                    limit=1_000,
                )
                events.extend(page)
                if not page or last_revision + len(page) >= current_revision:
                    return tuple(events)
                last_revision += len(page)
            raise RuntimeError(
                "federation event history exceeds the projection bound"
            )
        if hasattr(self._coordinator, "replay"):
            return tuple(
                self._coordinator.replay(
                    session_id=self._internal_session_id,
                    actor_node_id=self._actor_node_id,
                    last_applied_revision=0,
                )
            )
        return ()

    def _devices(
        self,
        raw_status: object,
        member_ids: set[str],
    ) -> tuple[DeviceRecord, ...]:
        capability_counts: dict[str, int] = {}
        for capability in _sequence(_value(raw_status, "capabilities", ())):
            if _value(capability, "session_id") != self._internal_session_id:
                continue
            node_id = _value(capability, "node_id")
            if isinstance(node_id, str) and node_id in member_ids:
                capability_counts[node_id] = capability_counts.get(node_id, 0) + 1

        devices: list[DeviceRecord] = []
        for candidate in _sequence(_value(raw_status, "nodes", ())):
            raw_node_id = _value(candidate, "node_id", _value(candidate, "id"))
            if raw_node_id not in member_ids:
                continue
            node_id = _public_text(raw_node_id, "unknown-device")
            label = _public_text(
                _value(candidate, "display_name", _value(candidate, "label", node_id)),
                node_id,
            )
            state = _enum_value(
                _value(
                    candidate,
                    "connection_state",
                    _value(
                        candidate,
                        "state",
                        _value(candidate, "connectivity_state", "unknown"),
                    ),
                ),
                "unknown",
            )
            devices.append(
                DeviceRecord(
                    node_id=node_id,
                    label=label,
                    state=state,
                    last_seen_at=_timestamp(
                        _value(
                            candidate,
                            "last_heartbeat",
                            _value(
                                candidate,
                                "last_seen_at",
                                _value(candidate, "last_heartbeat_at"),
                            ),
                        )
                    ),
                    capability_count=capability_counts.get(raw_node_id, 0),
                )
            )
        return tuple(sorted(devices, key=lambda item: (item.label, item.node_id)))

    def _capabilities(
        self,
        raw_status: object,
        member_ids: set[str],
    ) -> tuple[FederatedCapabilityRecord, ...]:
        records: list[FederatedCapabilityRecord] = []
        for capability in _sequence(_value(raw_status, "capabilities", ())):
            if _value(capability, "session_id") != self._internal_session_id:
                continue
            raw_node_id = _value(capability, "node_id")
            if not isinstance(raw_node_id, str) or raw_node_id not in member_ids:
                continue
            records.append(
                FederatedCapabilityRecord(
                    capability_id=_public_text(
                        _value(capability, "capability_id"),
                        "unknown-capability",
                    ),
                    node_id=_public_text(raw_node_id, "unknown-device"),
                    capability_type=_public_text(
                        _value(capability, "type"),
                        "unknown-capability",
                    ),
                    protocol=_public_text(
                        _value(capability, "protocol"),
                        "unknown-protocol",
                    ),
                    protocol_version=_public_text(
                        _value(capability, "protocol_version"),
                        "unknown",
                    ),
                    status=_enum_value(
                        _value(capability, "status"),
                        "unavailable",
                    ),
                    last_seen_at=_timestamp(
                        _value(
                            capability,
                            "last_heartbeat",
                            _value(capability, "last_heartbeat_at"),
                        )
                    ),
                )
            )
        return tuple(
            sorted(
                records,
                key=lambda item: (
                    item.node_id,
                    item.capability_type,
                    item.capability_id,
                ),
            )
        )

    @staticmethod
    def _activity_record(event: object) -> ActivityRecord:
        event_type = _public_text(
            _value(event, "event_type"),
            "federation.event",
        )
        title, message = _EVENT_COPY.get(
            event_type,
            (
                "Federation updated",
                "An authenticated federation change was recorded.",
            ),
        )
        revision = _value(event, "revision")
        return ActivityRecord(
            revision=revision if isinstance(revision, int) else None,
            occurred_at=_timestamp(_value(event, "occurred_at")),
            event_type=event_type,
            title=title,
            message=message,
        )

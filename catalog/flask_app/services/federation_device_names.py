"""Leader-owned, Federation-scoped display names for trusted devices.

Names are public-safe operator metadata carried in the authoritative Federation
event log. They never replace the Ed25519-derived ``node_id`` and therefore do
not change identity, membership, routing, update authority, provider authority,
or storage authority.
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from flask import current_app

from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.projections import FederationAuthoritySnapshot
from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter
from catalog.federation.redaction import is_nonpublic_location_text, is_secret_text

from .capability_onboarding_service import get_capability_onboarding_service

DEVICE_NAME_EVENT = "node.display-name.changed"
MAX_DEVICE_NAME_BYTES = 96
_RESERVED_NAMES = frozenset(
    {
        "this device",
        "this fcp device",
        "trusted fcp device",
    }
)


def _validated_name(value: object) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-device-name",
            "display_name",
            "must be text",
        )
    name = value.strip()
    if not name or any(ord(character) < 32 for character in name):
        raise FederationValidationError(
            "invalid-device-name",
            "display_name",
            "must be non-empty text without control characters",
        )
    if len(name.encode("utf-8")) > MAX_DEVICE_NAME_BYTES:
        raise FederationValidationError(
            "device-name-too-large",
            "display_name",
            f"must not exceed {MAX_DEVICE_NAME_BYTES} UTF-8 bytes",
        )
    if name.casefold() in _RESERVED_NAMES:
        raise FederationValidationError(
            "reserved-device-name",
            "display_name",
            "must distinguish this device from generic Federation labels",
        )
    if is_secret_text(name):
        raise FederationValidationError(
            "secret-device-name",
            "display_name",
            "must not contain credentials or secrets",
        )
    if is_nonpublic_location_text(name):
        raise FederationValidationError(
            "nonpublic-device-name",
            "display_name",
            "must not expose backend paths or physical storage addresses",
        )
    return name


def _event_aliases(
    coordinator: object,
    *,
    session_id: str,
    actor_node_id: str,
) -> dict[str, str]:
    aliases: dict[str, str] = {}
    last_revision = 0
    for _ in range(100):
        page, current_revision = coordinator.replay_page(
            session_id=session_id,
            actor_node_id=actor_node_id,
            last_applied_revision=last_revision,
            limit=1000,
        )
        for event in page:
            last_revision = int(event.revision)
            if event.event_type != DEVICE_NAME_EVENT:
                continue
            payload = event.payload
            if not isinstance(payload, dict):
                continue
            node_id = payload.get("node_id")
            display_name = payload.get("display_name")
            if not isinstance(node_id, str) or not isinstance(display_name, str):
                continue
            try:
                aliases[node_id] = _validated_name(display_name)
            except FederationValidationError:
                # Historical/malformed metadata never changes authority or blocks
                # the rest of the Federation projection.
                continue
        if not page or last_revision >= current_revision:
            break
    return aliases


class FederationDeviceNameAdapter:
    """Overlay durable Federation-scoped names on a read-only authority snapshot."""

    def __init__(
        self,
        authority: object,
        coordinator: object,
        *,
        session_id: str,
        actor_node_id: str,
    ) -> None:
        self._authority = authority
        self._coordinator = coordinator
        self._session_id = session_id
        self._actor_node_id = actor_node_id

    def snapshot(self) -> FederationAuthoritySnapshot:
        snapshot = self._authority.snapshot()
        if not isinstance(snapshot, FederationAuthoritySnapshot) or not snapshot.available:
            return snapshot
        try:
            aliases = _event_aliases(
                self._coordinator,
                session_id=self._session_id,
                actor_node_id=self._actor_node_id,
            )
        except Exception:  # noqa: BLE001 - naming projection fails open to base labels
            return snapshot
        devices = tuple(
            replace(device, label=aliases.get(device.node_id, device.label))
            for device in snapshot.devices
        )
        activity = tuple(
            replace(
                event,
                title="Device renamed",
                message="A Federation-scoped device name was changed.",
            )
            if event.event_type == DEVICE_NAME_EVENT
            else event
            for event in snapshot.activity
        )
        return replace(snapshot, devices=devices, activity=activity)


class FederationDeviceNamingService:
    """Append explicit leader-owned device-name decisions to Federation history."""

    @staticmethod
    def _context():
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise AuthorizationError(
                "federation-device-name-authority-required",
                "a trusted Federation context is required",
            )
        session = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        actor = context.credentials.identity.node_id
        if session is None or session.created_by_node_id != actor:
            raise AuthorizationError(
                "federation-device-name-owner-required",
                "only the Federation creator may assign device names",
            )
        return context, actor

    def rename(self, *, target_node_id: str, display_name: object) -> str:
        context, actor = self._context()
        name = _validated_name(display_name)
        session_id = context.binding.internal_session_id
        base = FederationAuthorityAdapter(
            context.coordinator,
            actor_node_id=actor,
            internal_session_id=session_id,
        )
        snapshot = FederationDeviceNameAdapter(
            base,
            context.coordinator,
            session_id=session_id,
            actor_node_id=actor,
        ).snapshot()
        if not snapshot.available:
            raise FederationOperationError(
                "federation-device-list-unavailable",
                "current Federation membership could not be read",
            )
        target = next(
            (device for device in snapshot.devices if device.node_id == target_node_id),
            None,
        )
        if target is None:
            raise FederationOperationError(
                "unknown-federation-device",
                "the target must be a current Federation member",
                "target_node_id",
            )
        folded = name.casefold()
        if any(
            device.node_id != target_node_id and device.label.casefold() == folded
            for device in snapshot.devices
        ):
            raise FederationValidationError(
                "duplicate-device-name",
                "display_name",
                "must be unique within this Federation",
            )
        if target.label == name:
            return name
        context.coordinator.append_event(
            session_id=session_id,
            actor_node_id=actor,
            request_id=f"device-name-{uuid.uuid4().hex}",
            event_type=DEVICE_NAME_EVENT,
            payload={"node_id": target_node_id, "display_name": name},
        )
        return name


def local_device_naming_available() -> bool:
    """Return whether the current local viewer owns Federation naming authority."""

    try:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            return False
        session = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        return (
            session is not None
            and session.created_by_node_id == context.credentials.identity.node_id
        )
    except Exception:  # noqa: BLE001 - controls fail closed
        current_app.logger.warning("Federation device naming authority unavailable")
        return False


__all__ = [
    "DEVICE_NAME_EVENT",
    "FederationDeviceNameAdapter",
    "FederationDeviceNamingService",
    "local_device_naming_available",
]

"""Federation-scoped display-name operations for trusted devices.

The current Federation leader may name any current member. Every trusted member
may also name its own device. Names are public metadata only: the Ed25519-derived
``node_id`` remains the identity and authority key.
"""

from __future__ import annotations

import uuid

from catalog.federation.device_names import (
    DEVICE_NAME_EVENT,
    MAX_DEVICE_NAME_BYTES,
    validate_device_name,
)
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.projections.authority_adapter import FederationAuthorityAdapter

from .capability_onboarding_service import get_capability_onboarding_service
from .federation_leader_authority import resolve_federation_leader


def _session_owner_id(context: object) -> str | None:
    """Compatibility name for the current operational Federation leader."""

    try:
        return resolve_federation_leader(context).leader_node_id
    except Exception:  # noqa: BLE001 - leader controls fail closed
        return None


def _append_name_event(
    context: object,
    *,
    actor_node_id: str,
    target_node_id: str,
    display_name: str,
) -> None:
    """Append through local authority or the bounded paired-runtime API."""

    coordinator = getattr(context, "coordinator", None)
    binding = getattr(context, "binding", None)
    session_id = getattr(binding, "internal_session_id", None)
    if coordinator is None or not isinstance(session_id, str) or not session_id:
        raise FederationOperationError(
            "federation-device-name-context-unavailable",
            "trusted Federation naming context is incomplete",
        )

    request_id = f"device-name-{uuid.uuid4().hex}"
    payload = {"node_id": target_node_id, "display_name": display_name}
    append = getattr(coordinator, "append_event", None)
    if callable(append):
        append(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
            event_type=DEVICE_NAME_EVENT,
            payload=payload,
        )
        return

    # RemoteCoordinatorFacade intentionally stays read-only. Its pairing runtime
    # exposes one bounded public event-append operation that verifies the bound
    # Federation session before reaching the authenticated relay client.
    runtime = getattr(coordinator, "runtime", None)
    state = getattr(coordinator, "state", None)
    append_session_event = getattr(runtime, "append_session_event", None)
    if runtime is None or state is None or not callable(append_session_event):
        raise FederationOperationError(
            "federation-device-name-write-unavailable",
            "the authenticated Federation transport cannot publish a device name",
        )
    append_session_event(
        state,
        session_id=session_id,
        event_type=DEVICE_NAME_EVENT,
        payload=payload,
        request_id=request_id,
    )


class FederationDeviceNamingService:
    """Persist bounded Federation-scoped device-name decisions."""

    @staticmethod
    def _context() -> tuple[object, str]:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            raise AuthorizationError(
                "federation-device-name-authority-required",
                "a trusted Federation context is required",
            )
        actor = context.credentials.identity.node_id
        return context, actor

    @staticmethod
    def _snapshot(context: object, actor: str):
        binding = getattr(context, "binding")
        coordinator = getattr(context, "coordinator")
        snapshot = FederationAuthorityAdapter(
            coordinator,
            actor_node_id=actor,
            internal_session_id=binding.internal_session_id,
        ).snapshot()
        if not snapshot.available:
            raise FederationOperationError(
                "federation-device-list-unavailable",
                "current Federation membership could not be read",
            )
        return snapshot

    def _verify_applied_name(
        self,
        context: object,
        actor: str,
        *,
        target_node_id: str,
        display_name: str,
    ) -> str:
        """Confirm deterministic replay accepted the just-published name claim."""

        snapshot = self._snapshot(context, actor)
        target = next(
            (device for device in snapshot.devices if device.node_id == target_node_id),
            None,
        )
        if target is None:
            raise FederationOperationError(
                "federation-device-name-verification-unavailable",
                "the renamed device disappeared before the name could be verified",
                "target_node_id",
            )
        if target.label == display_name:
            return display_name
        folded = display_name.casefold()
        if any(
            device.node_id != target_node_id and device.label.casefold() == folded
            for device in snapshot.devices
        ):
            raise FederationValidationError(
                "duplicate-device-name",
                "display_name",
                "another concurrent name claim won the Federation revision order",
            )
        raise FederationOperationError(
            "federation-device-name-not-applied",
            "the published device name was not accepted by Federation replay",
            "display_name",
        )

    def rename(self, *, target_node_id: str, display_name: object) -> str:
        context, actor = self._context()
        name = validate_device_name(display_name)
        snapshot = self._snapshot(context, actor)
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

        leader = _session_owner_id(context)
        if target_node_id != actor and leader != actor:
            raise AuthorizationError(
                "federation-device-name-owner-required",
                "a member may name only itself; the current Federation leader may name any member",
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

        _append_name_event(
            context,
            actor_node_id=actor,
            target_node_id=target_node_id,
            display_name=name,
        )
        return self._verify_applied_name(
            context,
            actor,
            target_node_id=target_node_id,
            display_name=name,
        )

    def rename_self(self, display_name: object) -> str:
        context, actor = self._context()
        name = validate_device_name(display_name)
        snapshot = self._snapshot(context, actor)
        target = next(
            (device for device in snapshot.devices if device.node_id == actor),
            None,
        )
        if target is None:
            raise FederationOperationError(
                "current-federation-device-unavailable",
                "this device is not visible in the current Federation membership",
            )
        folded = name.casefold()
        if any(
            device.node_id != actor and device.label.casefold() == folded
            for device in snapshot.devices
        ):
            raise FederationValidationError(
                "duplicate-device-name",
                "display_name",
                "must be unique within this Federation",
            )
        if target.label == name:
            return name
        _append_name_event(
            context,
            actor_node_id=actor,
            target_node_id=actor,
            display_name=name,
        )
        return self._verify_applied_name(
            context,
            actor,
            target_node_id=actor,
            display_name=name,
        )

    def current_name(self) -> str | None:
        try:
            context, actor = self._context()
            snapshot = self._snapshot(context, actor)
        except (AuthorizationError, FederationOperationError):
            return None
        target = next(
            (device for device in snapshot.devices if device.node_id == actor),
            None,
        )
        return None if target is None else target.label


def local_device_naming_available() -> bool:
    """Return whether this viewer may name other Federation members."""

    try:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            return False
        actor = context.credentials.identity.node_id
        return _session_owner_id(context) == actor
    except Exception:  # noqa: BLE001 - controls fail closed
        return False


def local_device_self_naming_available() -> bool:
    """Return whether this trusted member may publish its own device name."""

    try:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            return False
        actor = context.credentials.identity.node_id
        binding = context.binding
        return actor == binding.device_id
    except Exception:  # noqa: BLE001 - controls fail closed
        return False


def current_federation_device_name() -> str | None:
    return FederationDeviceNamingService().current_name()


__all__ = [
    "DEVICE_NAME_EVENT",
    "MAX_DEVICE_NAME_BYTES",
    "FederationDeviceNamingService",
    "current_federation_device_name",
    "local_device_naming_available",
    "local_device_self_naming_available",
]

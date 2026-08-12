"""Bind provider-management controls to transferable Federation leadership.

The F8 provider APIs predate transferable leadership and intentionally keep the
immutable session creator in their core data model.  The Flask product surface,
however, must follow the current coordinator-authored leader term just like
updates, capability requests, pairing, and device naming.

This adapter changes only provider *management* authority. Provider discovery,
provider-local requests, health, activation, storage, and capability identity
remain governed by their existing bounded authorities.
"""

from __future__ import annotations

from typing import Any

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.provider_enrollment import FederatedProviderEnrollmentService
from catalog.federation.errors import AuthorizationError

_LEGACY_REQUIRE_SESSION_OWNER = FederatedProviderEnrollmentService._require_session_owner
_LEGACY_OPERATOR_AUTHORIZED_CONTEXT = ProviderOperatorSurface._authorized_context
_INSTALLED = False


def _require_active_leader(
    self: FederatedProviderEnrollmentService,
    *,
    session_id: str,
    actor_node_id: str,
) -> None:
    """Require current leader while preserving legacy coordinator compatibility."""

    require_leader = getattr(self.coordinator, "require_session_leader", None)
    if not callable(require_leader):
        _LEGACY_REQUIRE_SESSION_OWNER(
            self,
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        return
    try:
        require_leader(session_id=session_id, actor_node_id=actor_node_id)
    except AuthorizationError as exc:
        if getattr(exc, "code", None) != "federation-leader-required":
            raise
        raise AuthorizationError(
            "provider-enrollment-not-authorized",
            "only the current Federation leader may manage provider enrollment in F8.1",
            "actor_node_id",
        ) from exc


def _active_leader_operator_context(
    self: ProviderOperatorSurface,
) -> tuple[tuple[Any, ...], bool]:
    """Expose provider-management actions only to the current leader."""

    announcements = self.enrollment.discover(
        session_id=self.session_id,
        actor_node_id=self.actor_node_id,
    )
    coordinator = self.enrollment.coordinator
    session = coordinator.store.get_session(self.session_id)
    if session is None:
        raise AuthorizationError(
            "unknown-session",
            "target session does not exist",
            "session_id",
        )
    leadership = getattr(coordinator, "session_leadership", None)
    if not callable(leadership):
        return announcements, session.created_by_node_id == self.actor_node_id
    leader = leadership(self.session_id)
    return announcements, leader.leader_node_id == self.actor_node_id


def install_active_leader_provider_runtime() -> None:
    """Install provider authority adapters once per interpreter."""

    global _INSTALLED
    if _INSTALLED:
        return
    FederatedProviderEnrollmentService._require_session_owner = _require_active_leader
    ProviderOperatorSurface._authorized_context = _active_leader_operator_context
    _INSTALLED = True


__all__ = ["install_active_leader_provider_runtime"]

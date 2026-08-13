"""Explicit provider-management adapters for transferable leadership.

The core provider APIs retain creator-pinned authority for compatibility.
Product surfaces that follow the coordinator-authored leadership term must
opt into these subclasses explicitly instead of mutating the core classes at
import time.
"""

from __future__ import annotations

from typing import Any

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.provider_enrollment import FederatedProviderEnrollmentService
from catalog.federation.errors import AuthorizationError


class ActiveLeaderProviderEnrollmentService(FederatedProviderEnrollmentService):
    """Provider enrollment whose management authority follows the current leader."""

    def _require_session_owner(
        self,
        *,
        session_id: str,
        actor_node_id: str,
    ) -> None:
        require_leader = getattr(self.coordinator, "require_session_leader", None)
        if not callable(require_leader):
            super()._require_session_owner(
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


class ActiveLeaderProviderOperatorSurface(ProviderOperatorSurface):
    """Operator projection whose management actions follow the current leader."""

    def _authorized_context(self) -> tuple[tuple[Any, ...], bool]:
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


__all__ = [
    "ActiveLeaderProviderEnrollmentService",
    "ActiveLeaderProviderOperatorSurface",
]

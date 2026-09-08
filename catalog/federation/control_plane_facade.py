"""Fail-closed relay facade for the physical-ready replicated authority."""

from __future__ import annotations

from typing import Any

from .control_plane_product import ReplicatedSessionCoordinator


class PhysicalReadyReplicatedSessionCoordinator(ReplicatedSessionCoordinator):
    """Require a live authenticated voter quorum for every durable relay write.

    Connectivity/heartbeat state remains local and ephemeral.  Anything that
    appends durable session history or requires leader authority is fenced by
    the replicated runtime before reaching the legacy coordinator view.
    """

    def require_session_leader(
        self, *, session_id: str, actor_node_id: str
    ) -> Any:
        self._leader(session_id, actor_node_id)
        return self._local.session_leadership(session_id)

    def append_event(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
    ):
        self.runtime.require_quorum_leader()
        return self._local.append_event(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
        )

    def session_leadership(self, session_id: str):
        # Keep compatibility with existing provider/auth surfaces while the C03
        # log remains the authority that materialized this projection.
        self.runtime.materialize()
        return self._local.session_leadership(session_id)


__all__ = ["PhysicalReadyReplicatedSessionCoordinator"]

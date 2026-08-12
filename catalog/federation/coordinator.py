"""Coordinator policy facade over durable Phase 2 state."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.node.identity import derive_node_id_from_public_key

from .errors import AuthenticationError, AuthorizationError
from .event_log import AuthoritativeSessionEventLog
from .models import CapabilityAnnouncement, NodeIdentity, Session, SessionEvent
from .persistence import CoordinatorStore
from .session_leadership import SessionLeadership, SessionLeadershipService


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SessionCoordinator:
    """Transaction-backed authority used by the independently runnable relay."""

    def __init__(
        self,
        database: Path | str | CoordinatorStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.store = (
            database if isinstance(database, CoordinatorStore) else CoordinatorStore(database)
        )
        self.event_log = AuthoritativeSessionEventLog(self.store)
        self.leadership = SessionLeadershipService(self.store)
        self._clock = clock

    @property
    def coordinator_id(self) -> str:
        return self.store.coordinator_id

    def create_enrollment_token(
        self, *, ttl_seconds: int = 600, max_uses: int = 1
    ) -> dict[str, Any]:
        return self.store.create_enrollment_token(
            now=self._clock(),
            ttl_seconds=ttl_seconds,
            max_uses=max_uses,
        )

    def create_pairing_material(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        ttl_seconds: int = 600,
        request_id: str,
    ) -> dict[str, Any]:
        """Issue bounded enrollment + invitation material for the active leader."""

        self.leadership.require_leader(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        enrollment = self.store.create_enrollment_token(
            now=self._clock(), ttl_seconds=ttl_seconds, max_uses=1,
            created_by=f"federation-leader:{actor_node_id}",
        )
        invitation = self.leadership.create_invitation(
            session_id=session_id,
            actor_node_id=actor_node_id,
            now=self._clock(),
            ttl_seconds=ttl_seconds,
            max_uses=1,
            request_id=request_id,
        )
        return {"enrollment": enrollment, "invitation": invitation}

    def enroll_node(self, identity: NodeIdentity, *, token: str) -> NodeIdentity:
        if derive_node_id_from_public_key(identity.public_key) != identity.node_id:
            self.store.audit_rejection(
                now=self._clock(),
                action="node.enroll",
                reason="public-identity-node-id-mismatch",
                actor_node_id=identity.node_id,
            )
            raise AuthenticationError(
                "public-identity-node-id-mismatch",
                "node ID is not derived from the submitted public identity",
                "node_id",
            )
        return self.store.enroll_node(identity, raw_token=token, now=self._clock())

    def public_identity(self, node_id: str) -> NodeIdentity:
        return self.store.public_identity(node_id)

    def create_session(
        self,
        *,
        actor_node_id: str,
        display_name: str,
        request_id: str,
        session_id: str | None = None,
    ) -> Session:
        return self.store.create_session(
            actor_node_id=actor_node_id,
            display_name=display_name,
            request_id=request_id,
            session_id=session_id,
            now=self._clock(),
        )

    def session_leadership(self, session_id: str) -> SessionLeadership:
        return self.leadership.current(session_id)

    def require_session_leader(self, *, session_id: str, actor_node_id: str) -> SessionLeadership:
        return self.leadership.require_leader(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )

    def transfer_session_leader(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        target_node_id: str,
        request_id: str,
    ) -> tuple[SessionLeadership, SessionEvent | None]:
        return self.leadership.transfer(
            session_id=session_id,
            actor_node_id=actor_node_id,
            target_node_id=target_node_id,
            request_id=request_id,
            now=self._clock(),
        )

    def create_invitation(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        ttl_seconds: int = 600,
        max_uses: int = 1,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return self.leadership.create_invitation(
            session_id=session_id,
            actor_node_id=actor_node_id,
            ttl_seconds=ttl_seconds,
            max_uses=max_uses,
            request_id=request_id or f"leader-invite-{self.store._id_factory()}",
            now=self._clock(),
        )

    def join_session(
        self,
        *,
        node_id: str,
        token: str,
        request_id: str,
        expected_session_id: str | None = None,
    ) -> Session:
        return self.store.join_session(
            node_id=node_id,
            raw_token=token,
            request_id=request_id,
            expected_session_id=expected_session_id,
            now=self._clock(),
        )

    def remove_member(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        target_node_id: str,
        request_id: str,
        reason: str,
    ) -> bool:
        leadership = self.leadership.current(session_id)
        if actor_node_id == target_node_id:
            if actor_node_id == leadership.leader_node_id:
                raise AuthorizationError(
                    "leader-self-removal-requires-transfer",
                    "transfer Federation leadership before removing the active leader",
                    "target_node_id",
                )
            return self.store.remove_member(
                session_id=session_id,
                actor_node_id=actor_node_id,
                target_node_id=target_node_id,
                request_id=request_id,
                reason=reason,
                now=self._clock(),
            )
        return self.leadership.remove_member_as_leader(
            session_id=session_id,
            actor_node_id=actor_node_id,
            target_node_id=target_node_id,
            request_id=request_id,
            reason=reason,
            now=self._clock(),
        )

    def revoke_node(
        self,
        *,
        node_id: str,
        reason: str,
        request_id: str,
        revoked_by: str = "local-admin",
    ) -> bool:
        return self.store.revoke_node(
            node_id=node_id,
            revoked_by=revoked_by,
            reason=reason,
            request_id=request_id,
            now=self._clock(),
        )

    def announce_capability(
        self,
        capability: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        request_id: str,
    ) -> tuple[CapabilityAnnouncement, bool]:
        return self.store.upsert_capability(
            capability,
            actor_node_id=actor_node_id,
            request_id=request_id,
            now=self._clock(),
        )

    def heartbeat(self, node_id: str) -> None:
        self.store.heartbeat(node_id=node_id, now=self._clock())

    def connected(self, *, node_id: str, connection_id: str) -> tuple[SessionEvent, ...]:
        now = self._clock()
        self.store.mark_connected(
            node_id=node_id, connection_id=connection_id, now=now
        )
        events: list[SessionEvent] = []
        for session_id in self.store.session_ids_for_node(node_id):
            _leadership, event = self.leadership.ensure_available(
                session_id=session_id,
                now=now,
                reason="leader-unavailable-on-connect",
            )
            if event is not None:
                events.append(event)
        return tuple(events)

    def disconnected(
        self, *, node_id: str, error: str | None = None
    ) -> tuple[SessionEvent, ...]:
        now = self._clock()
        session_ids = self.store.session_ids_for_node(node_id)
        events = list(
            self.store.mark_disconnected(
                node_id=node_id,
                now=now,
                error=error,
            )
        )
        for session_id in session_ids:
            leadership = self.leadership.current(session_id)
            if leadership.leader_node_id != node_id:
                continue
            _updated, event = self.leadership.ensure_available(
                session_id=session_id,
                now=now,
                reason="leader-disconnected",
            )
            if event is not None:
                events.append(event)
        return tuple(events)

    def relay_started(self) -> None:
        self.store.mark_all_disconnected(now=self._clock())

    def sweep_stale(
        self, *, heartbeat_timeout_seconds: float
    ) -> tuple[tuple[str, ...], tuple[SessionEvent, ...]]:
        now = self._clock()
        stale, original_events = self.store.sweep_stale_with_events(
            now=now,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )
        events = list(original_events)
        affected_sessions: set[str] = set()
        for node_id in stale:
            affected_sessions.update(self.store.session_ids_for_node(node_id))
        for session_id in sorted(affected_sessions):
            leadership = self.leadership.current(session_id)
            if leadership.leader_node_id not in stale:
                continue
            _updated, event = self.leadership.ensure_available(
                session_id=session_id,
                now=now,
                reason="leader-heartbeat-expired",
            )
            if event is not None:
                events.append(event)
        return stale, tuple(events)

    def append_event(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> tuple[SessionEvent, bool]:
        return self.event_log.append(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
            event_type=event_type,
            payload=payload,
            now=self._clock(),
        )

    def replay(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
    ) -> tuple[SessionEvent, ...]:
        return self.event_log.replay(
            session_id=session_id,
            last_applied_revision=last_applied_revision,
            actor_node_id=actor_node_id,
        )

    def replay_page(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[SessionEvent, ...], int]:
        return self.event_log.replay_page(
            session_id=session_id,
            last_applied_revision=last_applied_revision,
            actor_node_id=actor_node_id,
            limit=limit,
        )

    def event_for_request(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        request_id: str,
    ) -> SessionEvent | None:
        return self.store.event_for_request(
            session_id=session_id,
            actor_node_id=actor_node_id,
            request_id=request_id,
        )

    def session_ids_for_node(self, node_id: str) -> tuple[str, ...]:
        return self.store.session_ids_for_node(node_id)

    def revocation_events(self, node_id: str) -> tuple[SessionEvent, ...]:
        return self.store.revocation_events(node_id)

    def authorize_route(
        self, *, session_id: str, actor_node_id: str, target_node_id: str
    ) -> dict[str, Any]:
        self.store.require_membership(session_id=session_id, node_id=actor_node_id)
        try:
            self.store.require_membership(
                session_id=session_id,
                node_id=target_node_id,
            )
        except AuthenticationError as exc:
            raise AuthorizationError(
                "target-unavailable",
                "target node is not available for session routing",
                "target_node_id",
            ) from exc
        return {
            "kind": "validated-session-membership",
            "session_id": session_id,
            "actor_node_id": actor_node_id,
            "target_node_id": target_node_id,
            "validated_by": self.coordinator_id,
        }

    def accept_request(
        self,
        *,
        actor_node_id: str,
        request_id: str,
        message_type: str,
        request_hash: str,
        session_id: str | None = None,
        target_node_id: str | None = None,
    ) -> bool:
        return self.store.accept_request(
            actor_node_id=actor_node_id,
            request_id=request_id,
            message_type=message_type,
            request_hash=request_hash,
            now=self._clock(),
            session_id=session_id,
            target_node_id=target_node_id,
        )

    def status(
        self,
        *,
        actor_node_id: str,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return self.store.authorized_status(
            actor_node_id=actor_node_id,
            cursor=cursor,
        )

    def audit_rejection(
        self,
        *,
        action: str,
        reason: str,
        actor_node_id: str | None = None,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self.store.audit_rejection(
            now=self._clock(),
            action=action,
            reason=reason,
            actor_node_id=actor_node_id,
            session_id=session_id,
            request_id=request_id,
        )

    def require_active_node(self, node_id: str) -> None:
        self.store.require_active_node(node_id)

    def audit_entries(
        self,
        *,
        limit: int = 1_000,
        actor_node_id: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        return self.store.audit_entries(
            limit=limit,
            actor_node_id=actor_node_id,
        )

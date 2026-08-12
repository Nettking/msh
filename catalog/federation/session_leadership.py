"""Durable Federation leader selection and fencing over session event history.

The session creator remains immutable provenance. Operational leader authority
is a separate monotonic term recorded by coordinator-authored events. Ordinary
members cannot self-promote: only the authoritative coordinator may append a
leadership transition, either as an explicit handover requested by the current
leader or after it observes the current leader disconnect while another current
member remains connected.

This is control-plane leadership inside one durable coordinator authority. It
does not claim cross-host consensus if the coordinator database/relay host itself
is unavailable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .errors import AuthorizationError, FederationOperationError, FederationValidationError
from .models import CapabilityStatus, SessionEvent
from .persistence import CoordinatorStore, _request_key, _time, _token_hash
from .redaction import redact_secrets

LEADER_CHANGED_EVENT = "session.leader.changed"
LEADERSHIP_SCHEMA = "fcp.session-leadership.v1"
INITIAL_TERM = 1
_CONNECTED = frozenset({"connected"})


@dataclass(frozen=True)
class SessionLeadership:
    session_id: str
    creator_node_id: str
    leader_node_id: str
    term: int
    leader_connected: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": LEADERSHIP_SCHEMA,
            "session_id": self.session_id,
            "creator_node_id": self.creator_node_id,
            "leader_node_id": self.leader_node_id,
            "term": self.term,
            "leader_connected": self.leader_connected,
        }


class SessionLeadershipService:
    """Resolve and mutate one session's current operational leader."""

    def __init__(self, store: CoordinatorStore) -> None:
        self.store = store

    @staticmethod
    def _payload(
        *,
        session_id: str,
        previous_leader_node_id: str,
        leader_node_id: str,
        term: int,
        reason: str,
    ) -> dict[str, object]:
        return {
            "schema": LEADERSHIP_SCHEMA,
            "session_id": session_id,
            "previous_leader_node_id": previous_leader_node_id,
            "leader_node_id": leader_node_id,
            "term": term,
            "reason": reason[:128],
        }

    def _snapshot_tx(self, database: Any, session_id: str) -> SessionLeadership:
        session = database.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None:
            raise AuthorizationError(
                "unknown-session", "target session does not exist", "session_id"
            )
        creator = str(session["created_by_node_id"])
        leader = creator
        term = INITIAL_TERM
        rows = database.execute(
            """
            SELECT actor_node_id,payload_json FROM session_events
            WHERE session_id=? AND event_type=?
            ORDER BY revision
            """,
            (session_id, LEADER_CHANGED_EVENT),
        ).fetchall()
        for row in rows:
            if row["actor_node_id"] != self.store.coordinator_id:
                # Generic members may append arbitrary public-safe events, but a
                # similarly named member event never acquires leader authority.
                continue
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError as exc:
                raise FederationOperationError(
                    "malformed-leadership-history",
                    "coordinator leadership history contains invalid JSON",
                ) from exc
            next_leader = payload.get("leader_node_id")
            previous = payload.get("previous_leader_node_id")
            next_term = payload.get("term")
            if (
                payload.get("schema") != LEADERSHIP_SCHEMA
                or payload.get("session_id") != session_id
                or previous != leader
                or not isinstance(next_leader, str)
                or not next_leader
                or isinstance(next_term, bool)
                or not isinstance(next_term, int)
                or next_term != term + 1
            ):
                raise FederationOperationError(
                    "malformed-leadership-history",
                    "coordinator leadership history is not a contiguous monotonic chain",
                )
            leader = next_leader
            term = next_term

        connectivity = database.execute(
            """
            SELECT membership.removed_at,node.revoked_at,connectivity.state
            FROM session_memberships AS membership
            JOIN nodes AS node ON node.node_id=membership.node_id
            LEFT JOIN node_connectivity AS connectivity
              ON connectivity.node_id=membership.node_id
            WHERE membership.session_id=? AND membership.node_id=?
            """,
            (session_id, leader),
        ).fetchone()
        connected = bool(
            connectivity is not None
            and connectivity["removed_at"] is None
            and connectivity["revoked_at"] is None
            and str(connectivity["state"] or "").casefold() in _CONNECTED
        )
        return SessionLeadership(
            session_id=session_id,
            creator_node_id=creator,
            leader_node_id=leader,
            term=term,
            leader_connected=connected,
        )

    def current(self, session_id: str) -> SessionLeadership:
        with self.store.read_transaction() as database:
            return self._snapshot_tx(database, session_id)

    def require_leader(self, *, session_id: str, actor_node_id: str) -> SessionLeadership:
        with self.store.read_transaction() as database:
            self.store._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            leadership = self._snapshot_tx(database, session_id)
        if leadership.leader_node_id != actor_node_id:
            raise AuthorizationError(
                "federation-leader-required",
                "operation requires the current Federation leader",
                "actor_node_id",
            )
        return leadership

    def _connected_candidates_tx(self, database: Any, session_id: str) -> tuple[str, ...]:
        rows = database.execute(
            """
            SELECT membership.node_id
            FROM session_memberships AS membership
            JOIN nodes AS node ON node.node_id=membership.node_id
            JOIN node_connectivity AS connectivity ON connectivity.node_id=membership.node_id
            WHERE membership.session_id=?
              AND membership.removed_at IS NULL
              AND node.revoked_at IS NULL
              AND connectivity.state='connected'
            ORDER BY membership.node_id
            """,
            (session_id,),
        ).fetchall()
        return tuple(str(row["node_id"]) for row in rows)

    def _change_tx(
        self,
        database: Any,
        *,
        leadership: SessionLeadership,
        target_node_id: str,
        reason: str,
        request_id: str,
        now: Any,
    ) -> tuple[SessionLeadership, SessionEvent | None]:
        self.store._require_membership(
            database,
            session_id=leadership.session_id,
            node_id=target_node_id,
        )
        node = database.execute(
            "SELECT revoked_at FROM nodes WHERE node_id=?", (target_node_id,)
        ).fetchone()
        if node is None or node["revoked_at"] is not None:
            raise AuthorizationError(
                "leader-target-unavailable",
                "new Federation leader must be an active current member",
                "target_node_id",
            )
        if target_node_id == leadership.leader_node_id:
            return leadership, None
        next_term = leadership.term + 1
        event, created = self.store._append_event_tx(
            database,
            session_id=leadership.session_id,
            actor_node_id=self.store.coordinator_id,
            request_id=request_id,
            event_type=LEADER_CHANGED_EVENT,
            payload=self._payload(
                session_id=leadership.session_id,
                previous_leader_node_id=leadership.leader_node_id,
                leader_node_id=target_node_id,
                term=next_term,
                reason=reason,
            ),
            now=now,
            authorize_actor=False,
        )
        if created:
            self.store._audit(
                database,
                now=now,
                action="session.leader.change",
                outcome="accepted",
                reason=reason,
                actor_node_id=leadership.leader_node_id,
                session_id=leadership.session_id,
                request_id=request_id,
                details={
                    "previous_leader_node_id": leadership.leader_node_id,
                    "leader_node_id": target_node_id,
                    "term": next_term,
                },
            )
        refreshed = self._snapshot_tx(database, leadership.session_id)
        return refreshed, event if created else None

    def transfer(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        target_node_id: str,
        request_id: str,
        now: Any,
    ) -> tuple[SessionLeadership, SessionEvent | None]:
        if not isinstance(target_node_id, str) or not target_node_id:
            raise FederationValidationError(
                "invalid-id", "target_node_id", "must be non-empty opaque text"
            )
        with self.store.transaction() as database:
            self.store._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            leadership = self._snapshot_tx(database, session_id)
            if leadership.leader_node_id != actor_node_id:
                raise AuthorizationError(
                    "federation-leader-required",
                    "only the current Federation leader may transfer leadership",
                    "actor_node_id",
                )
            connectivity = database.execute(
                "SELECT state FROM node_connectivity WHERE node_id=?",
                (target_node_id,),
            ).fetchone()
            if connectivity is None or connectivity["state"] != "connected":
                raise AuthorizationError(
                    "leader-target-offline",
                    "leadership may be transferred only to a connected member",
                    "target_node_id",
                )
            return self._change_tx(
                database,
                leadership=leadership,
                target_node_id=target_node_id,
                reason="explicit-handover",
                request_id=request_id,
                now=now,
            )

    def ensure_available(
        self,
        *,
        session_id: str,
        now: Any,
        reason: str = "leader-disconnected",
    ) -> tuple[SessionLeadership, SessionEvent | None]:
        """Promote one connected member when the current leader is not connected.

        Selection is deterministic by stable node ID. The coordinator is the
        sole writer of the transition, so temporary peer-to-peer partitions do
        not let a member self-elect. If the coordinator itself is unavailable,
        no transition can be committed and the Federation fails closed.
        """

        with self.store.transaction() as database:
            leadership = self._snapshot_tx(database, session_id)
            if leadership.leader_connected:
                return leadership, None
            candidates = self._connected_candidates_tx(database, session_id)
            if not candidates:
                return leadership, None
            target = candidates[0]
            if target == leadership.leader_node_id:
                return leadership, None
            request_id = f"leader-auto-{leadership.term + 1}-{target}"
            return self._change_tx(
                database,
                leadership=leadership,
                target_node_id=target,
                reason=reason,
                request_id=request_id,
                now=now,
            )

    def create_invitation(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        now: Any,
        ttl_seconds: int,
        max_uses: int,
        request_id: str,
    ) -> dict[str, Any]:
        """Create an invitation for the current leader, including successors."""

        leadership = self.require_leader(
            session_id=session_id, actor_node_id=actor_node_id
        )
        if leadership.creator_node_id == actor_node_id:
            return self.store.create_invitation(
                session_id=session_id,
                actor_node_id=actor_node_id,
                now=now,
                ttl_seconds=ttl_seconds,
                max_uses=max_uses,
                request_id=request_id,
            )
        # The legacy store intentionally authorizes only the immutable creator.
        # Reproduce its bounded/idempotent token mutation here after the current
        # leader check rather than rewriting historical creator provenance.
        if not 1 <= ttl_seconds <= 7 * 24 * 60 * 60:
            raise FederationValidationError(
                "invalid-token-ttl", "ttl_seconds", "is outside the supported bound"
            )
        if not 1 <= max_uses <= 100:
            raise FederationValidationError(
                "invalid-token-use-limit", "max_uses", "is outside the supported bound"
            )
        request_key = _request_key(request_id)
        if request_key is None:
            raise FederationValidationError(
                "invalid-request-id", "request_id", "must be non-empty text"
            )
        raw_token = f"fcp_join_{self.store._token_factory(32)}"
        expires_at = now + timedelta(seconds=ttl_seconds)
        replaced = False
        with self.store.transaction() as database:
            self.store._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            current = self._snapshot_tx(database, session_id)
            if current.leader_node_id != actor_node_id:
                raise AuthorizationError(
                    "federation-leader-required",
                    "leadership changed before the invitation could be committed",
                    "actor_node_id",
                )
            existing = database.execute(
                """
                SELECT * FROM session_invitations
                WHERE created_by_node_id=? AND request_id=?
                """,
                (actor_node_id, request_key),
            ).fetchone()
            if existing is not None and (
                existing["session_id"] != session_id
                or existing["requested_ttl_seconds"] != ttl_seconds
                or existing["max_uses"] != max_uses
            ):
                raise FederationOperationError(
                    "idempotency-conflict",
                    "invitation request ID was reused with different bounds",
                    "request_id",
                )
            if existing is not None and existing["use_count"] > 0:
                raise FederationOperationError(
                    "invitation-already-used",
                    "an invitation from this request has already been used",
                    "request_id",
                )
            if existing is None:
                invitation_id = f"invite-{self.store._id_factory()}"
                database.execute(
                    """
                    INSERT INTO session_invitations(
                        token_hash,invitation_id,session_id,created_by_node_id,
                        request_id,requested_ttl_seconds,created_at,expires_at,max_uses
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _token_hash(raw_token), invitation_id, session_id,
                        actor_node_id, request_key, ttl_seconds, _time(now),
                        _time(expires_at), max_uses,
                    ),
                )
            else:
                invitation_id = str(existing["invitation_id"])
                replaced = True
                database.execute(
                    """
                    UPDATE session_invitations
                    SET token_hash=?,created_at=?,expires_at=?,revoked_at=NULL
                    WHERE invitation_id=?
                    """,
                    (_token_hash(raw_token), _time(now), _time(expires_at), invitation_id),
                )
            self.store._audit(
                database,
                now=now,
                action="session-invitation.created",
                outcome="accepted",
                reason="idempotent-token-rotated" if replaced else "bounded-token-created",
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=request_id,
                details={
                    "invitation_id": invitation_id,
                    "expires_at": _time(expires_at),
                    "max_uses": max_uses,
                },
            )
        return {
            "token": raw_token,
            "invitation_id": invitation_id,
            "session_id": session_id,
            "expires_at": _time(expires_at),
            "max_uses": max_uses,
            "replaced": replaced,
        }

    def remove_member_as_leader(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        target_node_id: str,
        request_id: str,
        reason: str,
        now: Any,
    ) -> bool:
        """Remove another member while fencing stale or self-removing leaders."""

        summary = str(reason).strip()[:512] or "removed-by-federation-leader"
        if redact_secrets(summary) != summary:
            raise FederationValidationError(
                "nonpublic-removal-reason",
                "reason",
                "must not contain credentials, backend paths, or physical addresses",
            )
        with self.store.transaction() as database:
            self.store._require_membership(
                database, session_id=session_id, node_id=actor_node_id
            )
            leadership = self._snapshot_tx(database, session_id)
            if leadership.leader_node_id != actor_node_id:
                raise AuthorizationError(
                    "federation-leader-required",
                    "only the current Federation leader may remove another member",
                    "actor_node_id",
                )
            if target_node_id == actor_node_id:
                raise AuthorizationError(
                    "leader-self-removal-requires-transfer",
                    "transfer Federation leadership before removing the active leader",
                    "target_node_id",
                )
            target = database.execute(
                "SELECT * FROM session_memberships WHERE session_id=? AND node_id=?",
                (session_id, target_node_id),
            ).fetchone()
            prior = database.execute(
                """
                SELECT payload_json FROM session_events
                WHERE session_id=? AND actor_node_id=? AND request_id=?
                  AND event_type='node.left'
                """,
                (session_id, actor_node_id, _request_key(f"{request_id}:left")),
            ).fetchone()
            if prior is not None:
                prior_payload = json.loads(prior["payload_json"])
                if prior_payload != {"node_id": target_node_id, "reason": summary}:
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "membership removal request ID was reused with different content",
                        "request_id",
                    )
                if target is not None and target["removed_at"] is None:
                    raise FederationOperationError(
                        "idempotency-conflict",
                        "membership removal request belongs to an earlier membership lifecycle",
                        "request_id",
                    )
                return False
            if target is None or target["removed_at"] is not None:
                return False
            database.execute(
                """
                UPDATE session_memberships
                SET removed_at=?,removed_by=?,removal_reason=?
                WHERE session_id=? AND node_id=?
                """,
                (_time(now), actor_node_id, summary, session_id, target_node_id),
            )
            database.execute(
                "UPDATE capabilities SET status=? WHERE session_id=? AND node_id=?",
                (CapabilityStatus.REVOKED.value, session_id, target_node_id),
            )
            self.store._append_event_tx(
                database,
                session_id=session_id,
                actor_node_id=actor_node_id,
                request_id=f"{request_id}:left",
                event_type="node.left",
                payload={"node_id": target_node_id, "reason": summary},
                now=now,
                authorize_actor=False,
            )
            self.store._audit(
                database,
                now=now,
                action="session.member.remove",
                outcome="accepted",
                reason="membership-removed",
                actor_node_id=actor_node_id,
                session_id=session_id,
                request_id=request_id,
                details={"target_node_id": target_node_id},
            )
            return True


__all__ = [
    "INITIAL_TERM",
    "LEADER_CHANGED_EVENT",
    "LEADERSHIP_SCHEMA",
    "SessionLeadership",
    "SessionLeadershipService",
]

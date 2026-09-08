"""Fail-closed relay facade for the physical-ready replicated authority."""

from __future__ import annotations

import hashlib
from contextlib import nullcontext
from functools import wraps
from typing import Any

from .control_plane_product import ReplicatedSessionCoordinator
from .control_plane_replication import AuthorityCommand, ControlPlaneError, ReplicaNode
from .errors import AuthorizationError
from .persistence import _time


def _journal_operation(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        journal = getattr(self.runtime, "journal", None)
        if journal is not None and not self.runtime.ready:
            # Preserve the public leader/quorum rejection before reporting an
            # unsealed leader. Neither refusal may enter the journal transaction.
            self.runtime.require_quorum_leader()
            if not self.runtime.ready:
                raise ControlPlaneError("product authority has not completed its readiness seal")
        with journal.operation() if journal is not None else nullcontext():
            return method(self, *args, **kwargs)
    return guarded


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

    def session_authority(self, *, session_id: str, actor_node_id: str):
        """Read typed session and authority from one owned committed view."""
        lock = getattr(self.runtime, "_lifecycle_lock", None)
        with lock if lock is not None else nullcontext():
            if not self.runtime.ready:
                raise ControlPlaneError("product authority has not completed its readiness seal")
            self.runtime.require_quorum_leader()
            self.runtime.materialize()
            with self.store.read_transaction() as database:
                self.store._require_membership(database, session_id=session_id, node_id=actor_node_id)
                session = self.store._session_tx(database, session_id)
                leadership = self._local.leadership._snapshot_tx(database, session_id)
                return session, leadership

    @_journal_operation
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

    @_journal_operation
    def create_enrollment_token(self, **kwargs):
        return super().create_enrollment_token(**kwargs)

    @_journal_operation
    def create_pairing_material(self, **kwargs):
        if not hasattr(self.runtime, "journal"):
            return super().create_pairing_material(**kwargs)
        session_id, actor = kwargs["session_id"], kwargs["actor_node_id"]
        self._leader(session_id, actor)
        request_id = kwargs["request_id"]
        marker = "pairing-material:" + hashlib.sha256(
            f"{session_id}\0{actor}\0{request_id}".encode()
        ).hexdigest()
        now = self.runtime.clock()
        # Successful invitation retries intentionally rotate their raw token.
        # Rotate the matching unused enrollment grant in the same operation;
        # consumed receipts remain intact and a failed second half rolls back.
        with self.store.transaction() as database:
            database.execute(
                "UPDATE enrollment_tokens SET revoked_at=? WHERE created_by=? AND use_count=0 AND revoked_at IS NULL",
                (_time(now), marker),
            )
        enrollment = self.store.create_enrollment_token(
            now=now, ttl_seconds=kwargs.get("ttl_seconds", 600), max_uses=1, created_by=marker,
        )
        invitation = self._local.create_invitation(
            session_id=session_id, actor_node_id=actor, request_id=request_id,
            ttl_seconds=kwargs.get("ttl_seconds", 600), max_uses=1,
        )
        return {"enrollment": enrollment, "invitation": invitation}

    @_journal_operation
    def create_invitation(self, **kwargs):
        return super().create_invitation(**kwargs)

    @_journal_operation
    def enroll_node(self, identity, **kwargs):
        return super().enroll_node(identity, **kwargs)

    @_journal_operation
    def join_session(self, **kwargs):
        return super().join_session(**kwargs)

    @_journal_operation
    def announce_capability(self, capability, **kwargs):
        return super().announce_capability(capability, **kwargs)

    def _product_authority(self, kind, request_id, issued_by, payload):
        self.runtime.propose(AuthorityCommand(
            command_id=f"{kind.lower()}-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type=kind, cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=issued_by, payload=payload,
        ))

    @_journal_operation
    def create_session(self, **kwargs):
        if not hasattr(self.runtime, "journal"):
            return super().create_session(**kwargs)
        self.runtime.require_quorum_leader()
        result = self._local.create_session(**kwargs)
        self._product_authority("SESSION_CREATE", kwargs["request_id"], kwargs["actor_node_id"], {
            "session_id": result.session_id, "creator_node_id": result.created_by_node_id,
            "display_name": result.display_name, "occurred_at": result.created_at.isoformat(),
        })
        return result

    @_journal_operation
    def remove_member(self, **kwargs):
        if not hasattr(self.runtime, "journal"):
            return super().remove_member(**kwargs)
        self._leader(kwargs["session_id"], kwargs["actor_node_id"])
        result = self._local.remove_member(**kwargs)
        if result:
            self._product_authority("SESSION_MEMBER_REMOVE", kwargs["request_id"], kwargs["actor_node_id"], {
                "session_id": kwargs["session_id"], "node_id": kwargs["target_node_id"],
                "reason": kwargs["reason"], "occurred_at": self.runtime.clock().isoformat(),
            })
        return result

    @_journal_operation
    def revoke_node(self, **kwargs):
        if not hasattr(self.runtime, "journal"):
            return super().revoke_node(**kwargs)
        self.runtime.require_quorum_leader()
        result = self._local.revoke_node(**kwargs)
        if result:
            self._product_authority("NODE_REVOKE", kwargs["request_id"], self.runtime.node.voter_id, {
                "node_id": kwargs["node_id"], "reason": kwargs["reason"],
                "occurred_at": self.runtime.clock().isoformat(),
            })
        return result

    @_journal_operation
    def transfer_session_leader(self, **kwargs):
        if not hasattr(self.runtime, "journal"):
            return super().transfer_session_leader(**kwargs)
        self._leader(kwargs["session_id"], kwargs["actor_node_id"])
        result, event = self._local.transfer_session_leader(**kwargs)
        if event is not None:
            self._product_authority("LEADER_TRANSITION", kwargs["request_id"], kwargs["actor_node_id"], {
                "session_id": kwargs["session_id"],
                "previous_leader_node_id": event.payload["previous_leader_node_id"],
                "leader_node_id": event.payload["leader_node_id"], "term": event.payload["term"],
                "reason": event.payload["reason"], "occurred_at": event.occurred_at.isoformat(),
            })
        return result, event

    def _health(self, method, **kwargs):
        journal = getattr(self.runtime, "journal", None)
        if journal is None:
            return method(**kwargs)
        # Local socket liveness is useful on every relay. Only the elected
        # consensus leader can turn it into a durable public health event.
        if self.runtime.ready and self.runtime.node.role == ReplicaNode.LEADER:
            try:
                with journal.operation():
                    self.runtime.require_quorum_leader()
                    return method(**kwargs)
            except AuthorizationError as error:
                # A peer election can step us down after the outer role check.
                # Only that authority loss permits local-only health cleanup;
                # unrelated authorization failures must still reach the caller.
                if error.code != "federation-quorum-leader-required":
                    raise
            except ControlPlaneError:
                pass
        return method(**kwargs, emit_health_events=False)

    def disconnected(self, *, node_id, error=None):
        return self._health(self.store.mark_disconnected, node_id=node_id, error=error, now=self.runtime.clock())

    def relay_started(self):
        return self._health(self.store.mark_all_disconnected, now=self.runtime.clock())

    def sweep_stale(self, *, heartbeat_timeout_seconds):
        if not hasattr(self.runtime, "journal"):
            return super().sweep_stale(heartbeat_timeout_seconds=heartbeat_timeout_seconds)
        return self._health(
            self.store.sweep_stale_with_events, now=self.runtime.clock(),
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )

    def session_leadership(self, session_id: str):
        # Keep compatibility with existing provider/auth surfaces while the C03
        # log remains the authority that materialized this projection.
        self.runtime.materialize()
        return self._local.session_leadership(session_id)


__all__ = ["PhysicalReadyReplicatedSessionCoordinator"]

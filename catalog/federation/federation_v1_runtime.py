"""Final Federation v1 replicated runtime used by product and qualification.

This class is deliberately a thin deployment layer over the reviewed C03 core
and the quorum-witnessed legacy migration runtime.  It adds one product startup
property that matters on Windows and on slower hosts: explicit creation of a new
Federation may retry a bounded number of *real authenticated elections* while
peer voter sockets are coming up.

No retry is treated as a vote.  Every attempt still runs the normal C03 election
and therefore still requires the fixed 2-of-3 quorum.  If quorum is not proven
within the bound, creation fails closed and no replacement Federation is made.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from .control_plane_legacy_migration import OfflineCreatorRecoverableRuntime
from .control_plane_product import _stamp
from .control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    QuorumUnavailable,
    ReplicaNode,
)

BOOTSTRAP_ELECTION_ATTEMPTS = 6
MIN_BOOTSTRAP_RETRY_SECONDS = 0.05
MAX_BOOTSTRAP_RETRY_SECONDS = 0.25


class FederationV1Runtime(OfflineCreatorRecoverableRuntime):
    """Physical Federation v1 runtime with bounded quorum acquisition."""

    def _bootstrap_retry_delay(self) -> float:
        return max(
            MIN_BOOTSTRAP_RETRY_SECONDS,
            min(MAX_BOOTSTRAP_RETRY_SECONDS, float(self.heartbeat_seconds)),
        )

    def _acquire_bootstrap_leadership(self) -> None:
        """Become leader only through a normal authenticated 2/3 election."""

        if self.node.role == ReplicaNode.LEADER and self.node.leader_id == self.node.voter_id:
            return
        for attempt in range(BOOTSTRAP_ELECTION_ATTEMPTS):
            if self.node.start_election(self.transport):
                return
            if attempt + 1 < BOOTSTRAP_ELECTION_ATTEMPTS:
                time.sleep(self._bootstrap_retry_delay())
        raise QuorumUnavailable(
            "could not establish authenticated voter quorum for Federation bootstrap"
        )

    def bootstrap_new_federation(
        self,
        *,
        federation_id: str,
        session_id: str,
        creator_node_id: str,
        display_name: str,
    ) -> None:
        """Create one new Federation after a bounded, real quorum election.

        This intentionally does not call the parent convenience implementation,
        because that helper owns a single election attempt.  The command and
        state-machine semantics below are the same: immutable voter set, current
        authenticated voter as issuer, creator provenance carried separately,
        normal quorum commit, then the private readiness seal.
        """

        if self.node.state.get("federation_id") is not None:
            raise ControlPlaneError("replicated Federation is already initialized")
        self._acquire_bootstrap_leadership()

        nodes = [
            {
                "node_id": peer.voter_id,
                "display_name": peer.display_name,
                "public_key": peer.public_key,
            }
            for peer in self.deployment.peers
        ]
        command = AuthorityCommand(
            command_id=f"genesis-{uuid.uuid4().hex}",
            command_type="FEDERATION_GENESIS",
            cluster_id=self.node.configuration.cluster_id,
            issued_by=self.node.voter_id,
            payload={
                "federation_id": federation_id,
                "session_id": session_id,
                "creator_node_id": creator_node_id,
                "display_name": display_name,
                "voter_ids": list(self.node.configuration.voter_ids),
                "nodes": nodes,
                "members": list(self.node.configuration.voter_ids),
                "occurred_at": _stamp(self.clock()),
            },
        )
        self.node.propose(command, self.transport)
        self.materialize()

        self._seal_authority(
            federation_id=federation_id,
            session_id=session_id,
            creator_node_id=creator_node_id,
            occurred_at=_stamp(self.clock()),
        )
        self.node.synchronize(self.transport)
        self.materialize()

        state = self.node.state
        if state.get("federation_id") != federation_id:
            raise ControlPlaneError("Federation identity changed during bootstrap")
        leadership = state.get("leaders", {}).get(session_id)
        if not isinstance(leadership, dict):
            raise ControlPlaneError("Federation bootstrap produced no session leadership")
        if leadership.get("creator_node_id") != creator_node_id:
            raise ControlPlaneError("Federation creator provenance changed during bootstrap")

    def bootstrap_command_identity(self, federation_id: str, session_id: str) -> str:
        """Stable diagnostic identity; contains no secret or credential material."""

        return "sha256:" + hashlib.sha256(
            f"{self.node.configuration.cluster_id}:{federation_id}:{session_id}".encode(
                "utf-8"
            )
        ).hexdigest()


__all__ = [
    "BOOTSTRAP_ELECTION_ATTEMPTS",
    "FederationV1Runtime",
]

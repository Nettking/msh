"""Final Federation v1 replicated runtime used by product and qualification.

This class is deliberately a thin deployment layer over the reviewed C03 core
and the quorum-witnessed legacy migration runtime. It adds two release-critical
properties:

* explicit creation of a new Federation tolerates a short voter-socket startup
  race by retrying a bounded number of *real authenticated elections*; and
* offline-creator migration reconstructs revocations and capability ownership
  from the same complete event journal whose digest was signed by a voter
  quorum, before the readiness seal becomes visible.

No retry is treated as a vote. Every attempt still requires the fixed 2-of-3
quorum. Legacy history is never trusted from a single local database: the local
journal is accepted only after its complete canonical digest matches the digest
in the quorum-signed witness manifest.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from collections.abc import Mapping
from typing import Any

from .control_plane_legacy_migration import (
    LegacyMigrationError,
    OfflineCreatorRecoverableRuntime,
    _canonical as _migration_canonical,
    _provenance_stub,
    _read_event_journal,
    _text as _migration_text,
)
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
    """Physical Federation v1 runtime with fail-closed migration and bootstrap."""

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
        """Create one new Federation after a bounded, real quorum election."""

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

    def _witnessed_legacy_authority(
        self, manifest: Mapping[str, Any]
    ) -> tuple[set[str], dict[str, dict[str, str]], dict[str, dict[str, str]]]:
        """Fold security/capability state from the quorum-witnessed local journal.

        The manifest's signed ``history_digest`` covers every event. We recompute
        that digest from the local read-only journal before deriving anything,
        so a local edit after witnessing is detected and cannot become authority.
        """

        session_id = _migration_text(manifest.get("session_id"), "session_id")
        events = _read_event_journal(self.legacy_node_state_database, session_id)
        encoded = [event.to_dict() for event in events]
        digest = "sha256:" + hashlib.sha256(_migration_canonical(encoded)).hexdigest()
        if digest != manifest.get("history_digest"):
            raise LegacyMigrationError(
                "local legacy journal changed after quorum witness attestation"
            )
        if events[-1].revision != manifest.get("last_revision"):
            raise LegacyMigrationError("legacy witness revision no longer matches its journal")

        creator = _migration_text(manifest.get("creator_node_id"), "creator_node_id")
        active: set[str] = {creator}
        revocations: dict[str, dict[str, str]] = {}
        capabilities: dict[str, dict[str, str]] = {}

        for event in events:
            payload = event.payload if isinstance(event.payload, dict) else {}
            if event.event_type == "node.joined":
                node_id = payload.get("node_id")
                if isinstance(node_id, str) and node_id:
                    active.add(node_id)
                continue
            if event.event_type == "node.left":
                node_id = payload.get("node_id")
                if isinstance(node_id, str) and node_id:
                    active.discard(node_id)
                continue
            if event.event_type == "node.revoked":
                node_id = payload.get("node_id")
                reason = payload.get("reason")
                if not isinstance(node_id, str) or not node_id:
                    raise LegacyMigrationError("legacy revocation event is malformed")
                revocations[node_id] = {
                    "reason": str(reason or "legacy-revocation")[:512],
                    "occurred_at": event.occurred_at.isoformat(),
                }
                active.discard(node_id)
                continue
            if event.event_type not in {
                "capability.registered",
                "capability.status.changed",
            }:
                continue
            capability_id = payload.get("capability_id")
            owner = payload.get("node_id") or payload.get("owner_node_id")
            capability_type = payload.get("type") or payload.get("capability_type")
            if not all(
                isinstance(item, str) and item
                for item in (capability_id, owner, capability_type)
            ):
                # A status event may be a bounded withdrawal that carries only
                # the scoped capability identity. It cannot establish ownership
                # by itself, so it is ignored only when a prior registration
                # already established that identity; otherwise fail closed.
                if (
                    event.event_type == "capability.status.changed"
                    and isinstance(capability_id, str)
                    and capability_id in capabilities
                ):
                    continue
                raise LegacyMigrationError("legacy capability authority event is malformed")
            candidate = {
                "capability_id": capability_id,
                "owner_node_id": owner,
                "capability_type": capability_type,
                "occurred_at": event.occurred_at.isoformat(),
            }
            previous = capabilities.get(capability_id)
            if previous is not None and (
                previous["owner_node_id"] != owner
                or previous["capability_type"] != capability_type
            ):
                raise LegacyMigrationError("legacy capability identity changed owner or type")
            if previous is None:
                capabilities[capability_id] = candidate

        signed_active = manifest.get("active_member_ids")
        if not isinstance(signed_active, list):
            raise LegacyMigrationError("legacy active member set is malformed")
        # The v1 witness manifest predates explicit revocation folding. Its
        # active-member list therefore includes a revoked member until the
        # revocation event is considered. Require exact agreement after applying
        # the revocations covered by the same signed history digest.
        expected_active = set(str(item) for item in signed_active) - set(revocations)
        if active != expected_active:
            raise LegacyMigrationError("legacy membership summary disagrees with history")

        return active, revocations, capabilities

    def _bootstrap_from_witness_manifest(self, manifest: Mapping[str, Any]) -> None:
        """Migrate the same Federation, including witnessed security authority."""

        if self.node.role != ReplicaNode.LEADER:
            raise LegacyMigrationError("legacy migration requires the elected C03 leader")
        federation_id = _migration_text(manifest.get("federation_id"), "federation_id")
        session_id = _migration_text(manifest.get("session_id"), "session_id")
        if federation_id != self.bootstrap_federation_id or session_id != self.bootstrap_session_id:
            raise LegacyMigrationError("legacy witness targets another Federation")
        creator = _migration_text(manifest.get("creator_node_id"), "creator_node_id")
        legacy_leader = _migration_text(
            manifest.get("legacy_leader_node_id"), "legacy_leader_node_id"
        )
        term_value = manifest.get("legacy_leadership_term")
        if isinstance(term_value, bool) or not isinstance(term_value, int) or term_value < 1:
            raise LegacyMigrationError("legacy leadership term is invalid")

        active, revocations, capabilities = self._witnessed_legacy_authority(manifest)
        voter_set = set(self.node.configuration.voter_ids)
        if not voter_set <= active:
            raise LegacyMigrationError("a configured C03 voter was not an active legacy member")
        if voter_set & set(revocations):
            raise LegacyMigrationError("a configured C03 voter was revoked in legacy authority")
        extra_active = active - voter_set
        allowed_extra = {creator, legacy_leader} - voter_set
        if not extra_active <= allowed_extra:
            raise LegacyMigrationError(
                "legacy Federation has active non-voter members whose cryptographic identity cannot be reconstructed"
            )
        if manifest.get("human_auth_present") is True and self.credential_manager.best_committed() is None:
            raise LegacyMigrationError(
                "legacy Federation contains human-auth users but no recoverable quorum-certified credential snapshot"
            )

        chain_raw = manifest.get("leadership_chain")
        if not isinstance(chain_raw, list):
            raise LegacyMigrationError("legacy leadership chain is malformed")
        chain: list[dict[str, Any]] = []
        historical_ids = {creator, legacy_leader} | set(revocations)
        expected_leader = creator
        expected_term = 1
        for item in chain_raw:
            if not isinstance(item, dict):
                raise LegacyMigrationError("legacy leadership chain entry is malformed")
            previous = _migration_text(
                item.get("previous_leader_node_id"), "previous_leader_node_id"
            )
            target = _migration_text(item.get("leader_node_id"), "leader_node_id")
            term = item.get("term")
            occurred_at = _migration_text(item.get("occurred_at"), "occurred_at")
            if (
                previous != expected_leader
                or isinstance(term, bool)
                or not isinstance(term, int)
                or term != expected_term + 1
            ):
                raise LegacyMigrationError("legacy leadership chain is not contiguous")
            chain.append(
                {
                    "previous_leader_node_id": previous,
                    "leader_node_id": target,
                    "term": term,
                    "occurred_at": occurred_at,
                }
            )
            historical_ids.update({previous, target})
            expected_leader = target
            expected_term = term
        if expected_leader != legacy_leader or expected_term != term_value:
            raise LegacyMigrationError("legacy leadership summary disagrees with its chain")

        real_nodes = {
            peer.voter_id: {
                "node_id": peer.voter_id,
                "display_name": peer.display_name,
                "public_key": peer.public_key,
            }
            for peer in self.deployment.peers
        }
        nodes = list(real_nodes.values())
        for node_id in sorted(historical_ids - voter_set):
            nodes.append(_provenance_stub(node_id))

        bootstrap_members = sorted(voter_set | historical_ids)
        created_at = _migration_text(manifest.get("created_at"), "created_at")
        genesis = AuthorityCommand(
            command_id=(
                "witnessed-migration-genesis-"
                + hashlib.sha256(f"{federation_id}:{session_id}".encode()).hexdigest()
            ),
            command_type="FEDERATION_GENESIS",
            cluster_id=self.node.configuration.cluster_id,
            issued_by=self.node.voter_id,
            payload={
                "federation_id": federation_id,
                "session_id": session_id,
                "creator_node_id": creator,
                "display_name": _migration_text(manifest.get("display_name"), "display_name"),
                "voter_ids": list(self.node.configuration.voter_ids),
                "nodes": nodes,
                "members": bootstrap_members,
                "occurred_at": created_at,
            },
        )
        self.node.propose(genesis, self.transport)

        for item in chain:
            self.node.propose(
                AuthorityCommand(
                    command_id=(
                        f"witnessed-migration-leader-{session_id}-"
                        f"{item['term']}-{item['leader_node_id']}"
                    ),
                    command_type="LEADER_TRANSITION",
                    cluster_id=self.node.configuration.cluster_id,
                    issued_by=self.node.voter_id,
                    payload={
                        "session_id": session_id,
                        "previous_leader_node_id": item["previous_leader_node_id"],
                        "leader_node_id": item["leader_node_id"],
                        "term": item["term"],
                        "occurred_at": item["occurred_at"],
                        "reason": "quorum-witnessed-pre-c03-history",
                    },
                ),
                self.transport,
            )

        current = self.node.state["leaders"][session_id]
        if current["leader_node_id"] != self.node.voter_id:
            next_term = int(current["term"]) + 1
            self.node.propose(
                AuthorityCommand(
                    command_id=(
                        f"witnessed-migration-failover-{session_id}-"
                        f"{next_term}-{self.node.voter_id}"
                    ),
                    command_type="LEADER_TRANSITION",
                    cluster_id=self.node.configuration.cluster_id,
                    issued_by=self.node.voter_id,
                    payload={
                        "session_id": session_id,
                        "previous_leader_node_id": current["leader_node_id"],
                        "leader_node_id": self.node.voter_id,
                        "term": next_term,
                        "occurred_at": created_at,
                        "reason": "legacy-coordinator-unavailable-c03-quorum-recovery",
                    },
                ),
                self.transport,
            )

        # Reconstruct durable capability ownership from the quorum-witnessed
        # history. Only active, non-revoked voter owners remain operational.
        for capability_id, capability in sorted(capabilities.items()):
            owner = capability["owner_node_id"]
            if owner not in voter_set or owner not in active or owner in revocations:
                continue
            self.node.propose(
                AuthorityCommand(
                    command_id=(
                        "witnessed-migration-capability-"
                        + hashlib.sha256(
                            f"{session_id}:{capability_id}:{owner}".encode()
                        ).hexdigest()
                    ),
                    command_type="CAPABILITY_DECLARE",
                    cluster_id=self.node.configuration.cluster_id,
                    # The logical issuer is the original owner proven by the
                    # quorum-signed journal; the current C03 leader is the only
                    # process allowed to propose this migration command.
                    issued_by=owner,
                    payload={
                        "session_id": session_id,
                        "capability_id": capability_id,
                        "capability_type": capability["capability_type"],
                        "owner_node_id": owner,
                        "occurred_at": capability["occurred_at"],
                    },
                ),
                self.transport,
            )

        # Historical-only nodes existed solely to replay exact provenance. They
        # are removed and revoked before readiness. Preserve an original
        # revocation reason/timestamp where the signed journal contains one.
        for node_id in sorted(historical_ids - voter_set):
            evidence = revocations.get(node_id)
            reason = (
                evidence["reason"]
                if evidence is not None
                else "historical-provenance-only-no-authentication-key"
            )
            occurred_at = evidence["occurred_at"] if evidence is not None else created_at
            self.node.propose(
                AuthorityCommand(
                    command_id=f"witnessed-migration-remove-{session_id}-{node_id}",
                    command_type="SESSION_MEMBER_REMOVE",
                    cluster_id=self.node.configuration.cluster_id,
                    issued_by=self.node.voter_id,
                    payload={
                        "session_id": session_id,
                        "node_id": node_id,
                        "reason": "historical-provenance-only",
                        "occurred_at": occurred_at,
                    },
                ),
                self.transport,
            )
            self.node.propose(
                AuthorityCommand(
                    command_id=f"witnessed-migration-revoke-{node_id}",
                    command_type="NODE_REVOKE",
                    cluster_id=self.node.configuration.cluster_id,
                    issued_by=self.node.voter_id,
                    payload={
                        "node_id": node_id,
                        "reason": reason,
                        "occurred_at": occurred_at,
                    },
                ),
                self.transport,
            )

        self._seal_authority(
            federation_id=federation_id,
            session_id=session_id,
            creator_node_id=creator,
            occurred_at=created_at,
        )
        state = self.node.state
        if state.get("federation_id") != federation_id:
            raise LegacyMigrationError("Federation identity changed during witnessed migration")
        leadership = state["leaders"].get(session_id)
        if not isinstance(leadership, dict) or leadership.get("creator_node_id") != creator:
            raise LegacyMigrationError("creator provenance changed during witnessed migration")
        if leadership.get("leader_node_id") != self.node.voter_id:
            raise LegacyMigrationError("witnessed migration did not promote the C03 quorum leader")

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

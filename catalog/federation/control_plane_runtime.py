"""Physical-ready replicated Federation runtime.

This module turns the reviewed C03 state machine into a continuously operating
three-voter product authority. It adds lifecycle concerns above the consensus
core: authenticated heartbeats, deterministic elections, quorum-loss fencing,
non-destructive follower materialization, safe existing-Federation bootstrap,
and private human-credential continuity.

The consensus log remains the authority. No SQLite/WAL page replication is
performed and recovery never creates a replacement Federation.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.persistence import CoordinatorStore
from catalog.federation.session_leadership import LEADERSHIP_SCHEMA
from catalog.node.identity import IdentityStore

from .control_plane_credentials import (
    CredentialQuorumManager,
    CredentialReplicaServer,
    CredentialReplicaTransport,
    CredentialSnapshot,
    CredentialSnapshotStore,
    credential_endpoints_from_control,
)
from .control_plane_materialized import MaterializedReplicatedFederationRuntime
from .control_plane_product import (
    REPLICATED_COORDINATOR_ID,
    ReplicatedControlPlaneDeployment,
    _secret_file,
    _stamp,
)
from .control_plane_readiness import (
    BOOTSTRAP_SEAL_CAPABILITY_ID,
    BOOTSTRAP_SEAL_CAPABILITY_TYPE,
    authority_ready,
)
from .control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
)
from .control_plane_transport import (
    PersistentReplayGuard,
    SecureEnvelopeCodec,
    SecureReplicationServer,
    SecureSocketReplicationTransport,
    VoterEndpoint,
    VoterIdentityRegistry,
)

DEFAULT_HEARTBEAT_SECONDS = 1.0
DEFAULT_ELECTION_TIMEOUT_SECONDS = 5.0
DEFAULT_ELECTION_STAGGER_SECONDS = 2.0
DEFAULT_CREDENTIAL_SYNC_SECONDS = 2.0
QUORUM_FAILURES_BEFORE_STEPDOWN = 2
AUTH_GENERATION_FILE = "human-auth-replica-generation.json"


class ObservedReplicaNode(ReplicaNode):
    """ReplicaNode with local leader-contact observation for election timing."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._contact_lock = threading.RLock()
        self._last_leader_contact = time.monotonic()

    @property
    def last_leader_contact(self) -> float:
        with self._contact_lock:
            return self._last_leader_contact

    def _mark_leader_contact(self) -> None:
        with self._contact_lock:
            self._last_leader_contact = time.monotonic()

    def receive_append_entries(self, **request: Any):
        response = super().receive_append_entries(**request)
        if response.success:
            self._mark_leader_contact()
        return response

    def receive_vote_request(self, **request: Any):
        response = super().receive_vote_request(**request)
        if response.granted:
            self._mark_leader_contact()
        return response

    def receive_install_snapshot(self, **request: Any):
        response = super().receive_install_snapshot(**request)
        if response.success:
            self._mark_leader_contact()
        return response

    def force_follower(self) -> None:
        """Relinquish local leader authority without manufacturing a new term."""
        with self._state_lock:
            self.role = self.FOLLOWER
            self.leader_id = None
            self._next_index.clear()
            self._match_index.clear()
            self._mark_leader_contact()


class PhysicalReadyReplicatedFederationRuntime(
    MaterializedReplicatedFederationRuntime
):
    """Continuously driven replicated authority for a physical product host."""

    def __init__(
        self,
        deployment: ReplicatedControlPlaneDeployment,
        *,
        clock: Any = None,
        human_auth_database: Path | str | None = None,
        human_auth_password_salt: Path | str | None = None,
        bootstrap_federation_id: str | None = None,
        bootstrap_session_id: str | None = None,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        election_timeout_seconds: float = DEFAULT_ELECTION_TIMEOUT_SECONDS,
        election_stagger_seconds: float = DEFAULT_ELECTION_STAGGER_SECONDS,
        credential_sync_seconds: float = DEFAULT_CREDENTIAL_SYNC_SECONDS,
    ) -> None:
        # Rebuild the small parent constructor here so this runtime can use the
        # observed node without modifying the reviewed Phase-1 implementation.
        self.deployment = deployment
        from datetime import datetime, timezone

        self.clock = clock or (lambda: datetime.now(timezone.utc))
        configuration = deployment.configuration
        if deployment.local_voter_id not in configuration.voter_ids:
            raise ControlPlaneError("local voter is not in the configured voter set")

        identity_store = IdentityStore(
            deployment.identity_directory,
            display_name=deployment.local_display_name,
        )
        if (
            not identity_store.private_key_path.exists()
            or not identity_store.public_identity_path.exists()
        ):
            raise ControlPlaneError(
                "replicated voter identity is not already provisioned"
            )
        credentials = identity_store.load_or_create()
        if credentials.identity.node_id != deployment.local_voter_id:
            raise ControlPlaneError(
                "local durable identity does not match configured voter ID"
            )

        registry = VoterIdentityRegistry(
            configuration,
            {peer.voter_id: peer.public_key for peer in deployment.peers},
        )
        secret = _secret_file(deployment.transport_secret_file)
        self.node = ObservedReplicaNode(
            deployment.local_voter_id,
            PersistentReplicaStore(deployment.replica_database, configuration),
        )
        self.codec = SecureEnvelopeCodec(
            credentials,
            registry,
            secret,
            PersistentReplayGuard(deployment.replay_database),
        )
        self.server = SecureReplicationServer(
            self.node,
            self.codec,
            host=deployment.listen_host,
            port=deployment.listen_port,
        )
        endpoints = {
            peer.voter_id: VoterEndpoint(peer.host, peer.port)
            for peer in deployment.peers
            if peer.voter_id != deployment.local_voter_id
        }
        self.transport = SecureSocketReplicationTransport(
            self.codec, endpoints, connect_timeout_seconds=float(heartbeat_seconds) / 2
        )
        self.local = SessionCoordinator(
            CoordinatorStore(
                deployment.coordinator_database,
                coordinator_id=REPLICATED_COORDINATOR_ID,
            ),
            clock=self.clock,
        )

        if deployment.listen_port >= 65535:
            raise ControlPlaneError(
                "control-plane listen port leaves no private credential port"
            )
        credential_store_path = deployment.replica_database.with_name(
            "human_credentials_replica.sqlite3"
        )
        self.credential_store = CredentialSnapshotStore(credential_store_path)
        self.credential_transport = CredentialReplicaTransport(
            self.codec,
            credential_endpoints_from_control(endpoints),
        )
        self.credential_server = CredentialReplicaServer(
            self.node,
            self.codec,
            self.credential_store,
            host=deployment.listen_host,
            port=deployment.listen_port + 1,
        )
        self.credential_manager = CredentialQuorumManager(
            self.node,
            self.codec,
            self.credential_store,
            self.credential_transport,
            secret,
        )
        self.human_auth_database = (
            Path(human_auth_database) if human_auth_database is not None else None
        )
        self.human_auth_password_salt = (
            Path(human_auth_password_salt)
            if human_auth_password_salt is not None
            else None
        )
        self.bootstrap_federation_id = (
            bootstrap_federation_id.strip()
            if isinstance(bootstrap_federation_id, str)
            and bootstrap_federation_id.strip()
            else None
        )
        self.bootstrap_session_id = (
            bootstrap_session_id.strip()
            if isinstance(bootstrap_session_id, str) and bootstrap_session_id.strip()
            else None
        )
        if (self.bootstrap_federation_id is None) != (self.bootstrap_session_id is None):
            raise ControlPlaneError(
                "bootstrap federation and session IDs must be configured together"
            )
        self._auth_generation_path = (
            Path(deployment.coordinator_database).parent / AUTH_GENERATION_FILE
        )

        for name, value in (
            ("heartbeat_seconds", heartbeat_seconds),
            ("election_timeout_seconds", election_timeout_seconds),
            ("election_stagger_seconds", election_stagger_seconds),
            ("credential_sync_seconds", credential_sync_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or value <= 0
            ):
                raise ControlPlaneError(f"{name} must be positive")
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.election_timeout_seconds = float(election_timeout_seconds)
        self.election_stagger_seconds = float(election_stagger_seconds)
        self.credential_sync_seconds = float(credential_sync_seconds)
        self._stop = threading.Event()
        self._lifecycle_lock = threading.RLock()
        self._lifecycle_thread: threading.Thread | None = None
        self._quorum_failures = 0
        self._next_election_at = self._election_deadline()
        self._last_credential_sync = 0.0
        self._credential_fingerprint_value: tuple[tuple[int, int], ...] | None = None
        self._last_restored_version = -1
        self._last_error: str | None = None

    def _election_deadline(self) -> float:
        rank = self.node.configuration.voter_ids.index(self.node.voter_id)
        return (
            time.monotonic()
            + self.election_timeout_seconds
            + rank * self.election_stagger_seconds
        )

    @property
    def lifecycle_error(self) -> str | None:
        return self._last_error

    @property
    def ready(self) -> bool:
        return authority_ready(self.node.state)

    def start(self) -> None:
        self.server.start()
        self.credential_server.start()
        if self._lifecycle_thread is not None and self._lifecycle_thread.is_alive():
            return
        self._stop.clear()
        self._lifecycle_thread = threading.Thread(
            target=self._lifecycle_loop,
            name=f"fcp-c03-lifecycle-{self.node.voter_id[:16]}",
            daemon=True,
        )
        self._lifecycle_thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._lifecycle_thread is not None:
            self._lifecycle_thread.join(timeout=max(5.0, self.heartbeat_seconds * 3))
            self._lifecycle_thread = None
        self.credential_server.close()
        self.server.close()

    def _lifecycle_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_seconds):
            try:
                self._lifecycle_round()
                self._last_error = None
            except Exception as exc:  # noqa: BLE001 - bounded fail-closed driver
                self._last_error = type(exc).__name__
                if self.node.role == ReplicaNode.LEADER:
                    self.node.force_follower()
                self._next_election_at = self._election_deadline()

    def _lifecycle_round(self) -> None:
        with self._lifecycle_lock:
            self._drive_lifecycle_round()

    def _drive_lifecycle_round(self) -> None:
        self.node.apply_committed()
        applied = self.node.store.last_applied
        if applied != getattr(self, "_last_lifecycle_materialized_index", None):
            self.materialize()
            self._last_lifecycle_materialized_index = applied
        state = self.node.state

        # No unsealed authority is allowed to participate in ordinary failover.
        # This covers empty stores and interrupted migration. Recovery must use
        # the validated bootstrap path and commit the final readiness seal.
        if not authority_ready(state):
            self._attempt_existing_federation_bootstrap()
            return

        if self.node.role == ReplicaNode.LEADER:
            matched = self.node.synchronize(self.transport)
            if matched + 1 < self.node.quorum:
                self._quorum_failures += 1
                if self._quorum_failures >= QUORUM_FAILURES_BEFORE_STEPDOWN:
                    self.node.force_follower()
                    self._quorum_failures = 0
                    self._next_election_at = self._election_deadline()
                    return
            else:
                self._quorum_failures = 0
                self._sync_human_credentials_if_due()
            return

        if (
            time.monotonic() - self.node.last_leader_contact
            <= self.election_timeout_seconds
        ):
            self._next_election_at = self._election_deadline()
            return
        if time.monotonic() < self._next_election_at:
            return

        elected = self.node.start_election(self.transport)
        self._next_election_at = self._election_deadline()
        if not elected:
            return
        self._quorum_failures = 0
        self._promote_operational_sessions()
        self.node.synchronize(self.transport)
        self.materialize()
        self._restore_human_credentials_if_available()
        self._sync_human_credentials_if_due(force=True)

    def bootstrap_new_federation(
        self,
        *,
        federation_id: str,
        session_id: str,
        creator_node_id: str,
        display_name: str,
    ) -> None:
        super().bootstrap_new_federation(
            federation_id=federation_id,
            session_id=session_id,
            creator_node_id=creator_node_id,
            display_name=display_name,
        )
        self._seal_authority(
            federation_id=federation_id,
            session_id=session_id,
            creator_node_id=creator_node_id,
            occurred_at=_stamp(self.clock()),
        )
        self.node.synchronize(self.transport)
        self.materialize()

    def _attempt_existing_federation_bootstrap(self) -> None:
        if self.bootstrap_federation_id is None or self.bootstrap_session_id is None:
            return
        state = self.node.state
        existing_id = state.get("federation_id")
        if existing_id is not None and existing_id != self.bootstrap_federation_id:
            raise ControlPlaneError(
                "configured migration Federation ID conflicts with replicated authority"
            )
        if time.monotonic() < self._next_election_at:
            return
        with self.local.store.read_transaction() as database:
            session = database.execute(
                "SELECT created_by_node_id FROM sessions WHERE session_id=?",
                (self.bootstrap_session_id,),
            ).fetchone()
        if session is None:
            raise ControlPlaneError("configured bootstrap session does not exist locally")
        if str(session["created_by_node_id"]) != self.node.voter_id:
            self._next_election_at = self._election_deadline()
            return
        if self.node.role != ReplicaNode.LEADER and not self.node.start_election(self.transport):
            self._next_election_at = self._election_deadline()
            return
        self._bootstrap_existing_federation(
            federation_id=self.bootstrap_federation_id,
            session_id=self.bootstrap_session_id,
        )
        self.node.synchronize(self.transport)
        self.materialize()
        self._sync_human_credentials_if_due(force=True)

    def _bootstrap_existing_federation(
        self, *, federation_id: str, session_id: str
    ) -> None:
        if self.node.role != ReplicaNode.LEADER:
            raise ControlPlaneError("existing-Federation bootstrap requires quorum leader")

        with self.local.store.read_transaction() as database:
            session = database.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise ControlPlaneError("bootstrap session does not exist")
            creator = str(session["created_by_node_id"])
            if creator != self.node.voter_id:
                raise ControlPlaneError(
                    "only the immutable Federation creator may bootstrap C03"
                )
            old_coordinator_id = str(session["coordinator_id"])
            created_at = str(session["created_at"])
            node_rows = database.execute(
                """
                SELECT node_id,display_name,public_key,revoked_at,revocation_reason
                FROM nodes ORDER BY node_id
                """
            ).fetchall()
            membership_rows = database.execute(
                """
                SELECT node_id,joined_at,removed_at,removal_reason
                FROM session_memberships
                WHERE session_id=? ORDER BY node_id
                """,
                (session_id,),
            ).fetchall()
            capability_rows = database.execute(
                """
                SELECT capability_id,node_id,type,announced_at FROM capabilities
                WHERE session_id=? ORDER BY capability_id
                """,
                (session_id,),
            ).fetchall()
            leadership_rows = database.execute(
                """
                SELECT actor_node_id,payload_json,occurred_at FROM session_events
                WHERE session_id=? AND event_type='session.leader.changed'
                ORDER BY revision
                """,
                (session_id,),
            ).fetchall()

        nodes = [
            {
                "node_id": str(row["node_id"]),
                "display_name": str(row["display_name"]),
                "public_key": str(row["public_key"]),
            }
            for row in node_rows
        ]
        known_nodes = {item["node_id"] for item in nodes}
        active_members = [
            str(row["node_id"])
            for row in membership_rows
            if row["removed_at"] is None
        ]
        # Temporarily include historical removed members during the unsealed
        # migration so a historical leadership chain remains replayable. They
        # are removed again before the readiness seal is committed.
        bootstrap_members = [str(row["node_id"]) for row in membership_rows]
        removed_members = [
            (
                str(row["node_id"]),
                str(row["joined_at"]),
                str(row["removed_at"]),
                str(row["removal_reason"] or "pre-c03-removal"),
            )
            for row in membership_rows
            if row["removed_at"] is not None
        ]
        active_set = set(active_members)
        revoked = {
            str(row["node_id"]): (
                str(row["revoked_at"]),
                str(row["revocation_reason"] or "legacy-revocation"),
            )
            for row in node_rows
            if row["revoked_at"] is not None
        }
        for voter_id in self.node.configuration.voter_ids:
            if voter_id not in known_nodes:
                raise ControlPlaneError("C03 voter is not enrolled in the existing Federation")
            if voter_id not in active_set or voter_id in revoked:
                raise ControlPlaneError("C03 voter is not an active existing Federation member")

        genesis = AuthorityCommand(
            command_id=(
                "migration-genesis-"
                + hashlib.sha256(f"{federation_id}:{session_id}".encode()).hexdigest()
            ),
            command_type="FEDERATION_GENESIS",
            cluster_id=self.node.configuration.cluster_id,
            issued_by=creator,
            payload={
                "federation_id": federation_id,
                "session_id": session_id,
                "creator_node_id": creator,
                "display_name": str(session["display_name"]),
                "voter_ids": list(self.node.configuration.voter_ids),
                "nodes": nodes,
                "members": bootstrap_members,
                "occurred_at": created_at,
            },
        )
        self.node.propose(genesis, self.transport)

        leader = creator
        term = 1
        transitions: list[tuple[str, str, int, str]] = []
        for row in leadership_rows:
            if str(row["actor_node_id"]) != old_coordinator_id:
                continue
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError as exc:
                raise ControlPlaneError("legacy leadership history is malformed") from exc
            next_term = payload.get("term")
            next_leader = payload.get("leader_node_id")
            previous = payload.get("previous_leader_node_id")
            if (
                payload.get("schema") != LEADERSHIP_SCHEMA
                or previous != leader
                or isinstance(next_term, bool)
                or not isinstance(next_term, int)
                or next_term != term + 1
                or not isinstance(next_leader, str)
                or next_leader not in set(bootstrap_members)
            ):
                raise ControlPlaneError(
                    "legacy leadership history is not a contiguous enrolled-member chain"
                )
            transitions.append(
                (leader, next_leader, next_term, str(row["occurred_at"]))
            )
            leader = next_leader
            term = next_term

        for previous, next_leader, next_term, occurred_at in transitions:
            command = AuthorityCommand(
                command_id=f"migration-leader-{session_id}-{next_term}-{next_leader}",
                command_type="LEADER_TRANSITION",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=previous,
                payload={
                    "session_id": session_id,
                    "previous_leader_node_id": previous,
                    "leader_node_id": next_leader,
                    "term": next_term,
                    "occurred_at": occurred_at,
                    "reason": "pre-c03-leadership-import",
                },
            )
            self.node.propose(command, self.transport)

        for row in capability_rows:
            owner = str(row["node_id"])
            if owner not in active_set or owner in revoked:
                continue
            command = AuthorityCommand(
                command_id=(
                    "migration-capability-"
                    + hashlib.sha256(
                        f"{session_id}:{row['capability_id']}:{owner}".encode()
                    ).hexdigest()
                ),
                command_type="CAPABILITY_DECLARE",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=owner,
                payload={
                    "session_id": session_id,
                    "capability_id": str(row["capability_id"]),
                    "capability_type": str(row["type"]),
                    "owner_node_id": owner,
                    "occurred_at": str(row["announced_at"]),
                },
            )
            self.node.propose(command, self.transport)

        for node_id, _joined_at, removed_at, reason in removed_members:
            remove = AuthorityCommand(
                command_id=f"migration-member-remove-{session_id}-{node_id}",
                command_type="SESSION_MEMBER_REMOVE",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=creator,
                payload={
                    "session_id": session_id,
                    "node_id": node_id,
                    "reason": reason,
                    "occurred_at": removed_at,
                },
            )
            self.node.propose(remove, self.transport)

        for node_id, (revoked_at, reason) in sorted(revoked.items()):
            command = AuthorityCommand(
                command_id=f"migration-revoke-{node_id}",
                command_type="NODE_REVOKE",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=creator,
                payload={
                    "node_id": node_id,
                    "reason": reason,
                    "occurred_at": revoked_at,
                },
            )
            self.node.propose(command, self.transport)

        self._seal_authority(
            federation_id=federation_id,
            session_id=session_id,
            creator_node_id=creator,
            occurred_at=created_at,
        )
        if self.node.state.get("federation_id") != federation_id:
            raise ControlPlaneError("existing Federation identity changed during C03 bootstrap")
        if not authority_ready(self.node.state):
            raise ControlPlaneError("C03 bootstrap readiness seal did not commit")

    def _seal_authority(
        self,
        *,
        federation_id: str,
        session_id: str,
        creator_node_id: str,
        occurred_at: str,
    ) -> None:
        command = AuthorityCommand(
            command_id=(
                "bootstrap-seal-"
                + hashlib.sha256(f"{federation_id}:{session_id}".encode()).hexdigest()
            ),
            command_type="CAPABILITY_DECLARE",
            cluster_id=self.node.configuration.cluster_id,
            issued_by=creator_node_id,
            payload={
                "session_id": session_id,
                "capability_id": BOOTSTRAP_SEAL_CAPABILITY_ID,
                "capability_type": BOOTSTRAP_SEAL_CAPABILITY_TYPE,
                "owner_node_id": creator_node_id,
                "occurred_at": occurred_at,
            },
        )
        self.node.propose(command, self.transport)

    def _promote_operational_sessions(self) -> None:
        with self._lifecycle_lock:
            self._drive_operational_promotions()

    def _drive_operational_promotions(self) -> None:
        state = self.node.state
        if not authority_ready(state):
            return
        for session_id in sorted(state["sessions"]):
            membership = state["memberships"].get(session_id, {})
            if membership.get(self.node.voter_id) is not True:
                continue
            if self.node.voter_id in state["revocations"]:
                continue
            leadership = state["leaders"][session_id]
            if leadership["leader_node_id"] == self.node.voter_id:
                continue
            command_id = (
                f"auto-recover-leader-{session_id}-"
                f"{int(leadership['term']) + 1}-{self.node.voter_id}"
            )
            pending = self.node.store.entry_for_command(command_id)
            if pending is not None and pending.log_term < self.node.store.current_term:
                # Raft cannot commit an inherited entry directly. Commit an
                # idempotent enrollment of this already-enrolled voter in the
                # new consensus term; that real quorum commit also commits the
                # inherited prefix, including the pending leadership change.
                local_node = state["nodes"][self.node.voter_id]
                barrier = AuthorityCommand(
                    command_id=f"promotion-barrier-{self.node.store.current_term}-{command_id}",
                    command_type="NODE_ENROLL",
                    cluster_id=self.node.configuration.cluster_id,
                    issued_by=self.node.voter_id,
                    payload={
                        "node_id": self.node.voter_id,
                        "display_name": local_node["display_name"],
                        "public_key": local_node["public_key"],
                    },
                )
                self.node.propose(barrier, self.transport)
                state = self.node.state
                continue
            occurred_at = (
                pending.command.payload["occurred_at"]
                if pending is not None
                else _stamp(self.clock())
            )
            command = AuthorityCommand(
                command_id=command_id,
                command_type="LEADER_TRANSITION",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=self.node.voter_id,
                payload={
                    "session_id": session_id,
                    "previous_leader_node_id": leadership["leader_node_id"],
                    "leader_node_id": self.node.voter_id,
                    "term": int(leadership["term"]) + 1,
                    "occurred_at": occurred_at,
                    "reason": "replicated-quorum-auto-recovery",
                },
            )
            self.node.propose(command, self.transport)
            state = self.node.state

    def _credential_paths_ready(self) -> bool:
        return bool(
            self.human_auth_database is not None
            and self.human_auth_password_salt is not None
            and self.human_auth_database.exists()
            and self.human_auth_password_salt.exists()
        )

    def _credential_fingerprint(self) -> tuple[tuple[int, int], ...] | None:
        if not self._credential_paths_ready():
            return None
        assert self.human_auth_database is not None
        assert self.human_auth_password_salt is not None
        paths = (
            self.human_auth_database,
            Path(str(self.human_auth_database) + "-wal"),
            self.human_auth_password_salt,
        )
        values: list[tuple[int, int]] = []
        for path in paths:
            try:
                stat = path.stat()
            except FileNotFoundError:
                values.append((0, 0))
            else:
                values.append((int(stat.st_mtime_ns), int(stat.st_size)))
        return tuple(values)

    def _record_auth_generation(self, snapshot: CredentialSnapshot) -> None:
        payload = json.dumps(
            {
                "schema": "fcp.human-auth.replica-generation.v1",
                "federation_id": snapshot.federation_id,
                "session_id": snapshot.session_id,
                "snapshot_id": snapshot.snapshot_id,
                "version": snapshot.version,
                "term": snapshot.term,
                "leader_id": snapshot.leader_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self._auth_generation_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._auth_generation_path.with_suffix(".tmp")
        temporary.write_text(payload + "\n", encoding="utf-8")
        os.replace(temporary, self._auth_generation_path)

    def _primary_session_id(self) -> str | None:
        sessions = sorted(self.node.state.get("sessions", {}))
        return sessions[0] if len(sessions) == 1 else None

    def _restore_human_credentials_if_available(self) -> CredentialSnapshot | None:
        if self.node.role != ReplicaNode.LEADER or not authority_ready(self.node.state):
            return None
        if self.human_auth_database is None or self.human_auth_password_salt is None:
            return None
        federation_id = self.node.state.get("federation_id")
        session_id = self._primary_session_id()
        if not isinstance(federation_id, str) or session_id is None:
            return None
        best = self.credential_manager.best_committed()
        if best is None:
            return None
        snapshot, _certificate = best
        if snapshot.version <= self._last_restored_version:
            return snapshot
        restored = self.credential_manager.restore_best(
            database_path=self.human_auth_database,
            password_salt_path=self.human_auth_password_salt,
            expected_federation_id=federation_id,
            expected_session_id=session_id,
        )
        self._last_restored_version = restored.version
        self._credential_fingerprint_value = self._credential_fingerprint()
        self._record_auth_generation(restored)
        return restored

    def _sync_human_credentials_if_due(
        self, *, force: bool = False
    ) -> CredentialSnapshot | None:
        if self.node.role != ReplicaNode.LEADER or not authority_ready(self.node.state):
            return None
        now = time.monotonic()
        if not force and now - self._last_credential_sync < self.credential_sync_seconds:
            return None
        self._last_credential_sync = now
        fingerprint = self._credential_fingerprint()
        if fingerprint is None:
            return None
        if not force and fingerprint == self._credential_fingerprint_value:
            return None
        federation_id = self.node.state.get("federation_id")
        session_id = self._primary_session_id()
        if not isinstance(federation_id, str) or session_id is None:
            return None
        assert self.human_auth_database is not None
        assert self.human_auth_password_salt is not None
        matched = self.node.synchronize(self.transport)
        if matched + 1 < self.node.quorum:
            raise QuorumUnavailable(
                "human credential publication cannot prove control-plane quorum"
            )
        snapshot = self.credential_manager.publish(
            database_path=self.human_auth_database,
            password_salt_path=self.human_auth_password_salt,
            federation_id=federation_id,
            session_id=session_id,
        )
        self._credential_fingerprint_value = self._credential_fingerprint()
        self._record_auth_generation(snapshot)
        return snapshot


__all__ = [
    "AUTH_GENERATION_FILE",
    "ObservedReplicaNode",
    "PhysicalReadyReplicatedFederationRuntime",
]

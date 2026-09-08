"""Physical-ready replicated Federation runtime.

This module turns the reviewed C03 state machine into a continuously operating
three-voter product authority.  It adds only lifecycle concerns above the
consensus core:

* periodic authenticated AppendEntries heartbeats;
* deterministic staggered election after leader silence;
* fail-closed step-down after repeated quorum loss;
* non-destructive materialization of committed authority on followers;
* automatic operational-leader transition after a quorum election; and
* private quorum-certified human-credential snapshot publication/restoration.

The consensus log remains the authority.  No SQLite/WAL page replication is
performed and no replacement Federation is created during recovery.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.persistence import CoordinatorStore
from catalog.node.identity import IdentityStore

from .control_plane_credentials import (
    CredentialQuorumManager,
    CredentialReplicaServer,
    CredentialReplicaTransport,
    CredentialReplicationError,
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
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        election_timeout_seconds: float = DEFAULT_ELECTION_TIMEOUT_SECONDS,
        election_stagger_seconds: float = DEFAULT_ELECTION_STAGGER_SECONDS,
        credential_sync_seconds: float = DEFAULT_CREDENTIAL_SYNC_SECONDS,
    ) -> None:
        # Rebuild the small parent constructor here so the product runtime can
        # use ObservedReplicaNode without modifying the reviewed Phase-1 core.
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
        self.transport = SecureSocketReplicationTransport(self.codec, endpoints)
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
            except Exception as exc:  # noqa: BLE001 - fail closed and retry bounded round
                self._last_error = type(exc).__name__
                # A lifecycle exception must never leave an isolated node
                # believing it can continue leader-only product mutations.
                if self.node.role == ReplicaNode.LEADER:
                    self.node.force_follower()
                self._next_election_at = self._election_deadline()

    def _lifecycle_round(self) -> None:
        self.node.apply_committed()
        self.materialize()

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

        # A valid leader heartbeat updates ObservedReplicaNode.  Do not start a
        # competing election while that contact is within the timeout window.
        if (
            self.node.leader_id is not None
            and time.monotonic() - self.node.last_leader_contact
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

    def _promote_operational_sessions(self) -> None:
        state = self.node.state
        if state.get("federation_id") is None:
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
            command = AuthorityCommand(
                command_id=(
                    f"auto-recover-leader-{session_id}-"
                    f"{int(leadership['term']) + 1}-{self.node.voter_id}"
                ),
                command_type="LEADER_TRANSITION",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=self.node.voter_id,
                payload={
                    "session_id": session_id,
                    "previous_leader_node_id": leadership["leader_node_id"],
                    "leader_node_id": self.node.voter_id,
                    "term": int(leadership["term"]) + 1,
                    "occurred_at": _stamp(self.clock()),
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
        import json

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
        if self.node.role != ReplicaNode.LEADER:
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
        if self.node.role != ReplicaNode.LEADER:
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
        # Synchronize first so every reachable credential server has observed
        # this leader/term before accepting a private prepare.
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

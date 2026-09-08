"""Product bridge from replicated C03 authority to the existing Federation relay.

The replicated command log is the authority.  The legacy ``CoordinatorStore``
is retained as a local materialized view because the existing relay, projection
and capability surfaces already speak that API.  On leader recovery this module
rebuilds those authority tables from committed replicated state; transient
connectivity, request queues and browser sessions are deliberately not
replicated.

This is state-machine replication, not SQLite/WAL replication.  Tokens and
other one-shot local material are re-issued by the current quorum leader after
failover.  Immutable Federation identity, creator provenance, membership,
revocation, operational leadership and capability ownership come only from the
committed C03 state.
"""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError, FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, NodeIdentity, Session
from catalog.federation.persistence import CoordinatorStore
from catalog.node.identity import IdentityStore

from .control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    FencingToken,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
    VoterConfiguration,
)
from .control_plane_transport import (
    PersistentReplayGuard,
    SecureEnvelopeCodec,
    SecureReplicationServer,
    SecureSocketReplicationTransport,
    TransportSecurityError,
    VoterEndpoint,
    VoterIdentityRegistry,
)

DEPLOYMENT_SCHEMA = "fcp.control-plane.deployment.v1"
REPLICATED_COORDINATOR_ID = "fcp-replicated-coordinator-v1"
DEFAULT_VOTER_PORT = 8875
MAX_DEPLOYMENT_CONFIG_BYTES = 256 * 1024
MAX_SECRET_FILE_BYTES = 4096


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime | None = None) -> str:
    return (value or _now()).astimezone(timezone.utc).isoformat()


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(ch) < 32 for ch in value):
        raise ControlPlaneError(f"{field} must be non-empty bounded text")
    if len(value.encode("utf-8")) > 4096:
        raise ControlPlaneError(f"{field} exceeds its size bound")
    return value.strip()


def _canonical(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ControlPlaneError("product authority value is not canonical JSON") from exc
    if len(encoded) > MAX_DEPLOYMENT_CONFIG_BYTES:
        raise ControlPlaneError("product authority value exceeds its size bound")
    return encoded


def _event_time(value: object, fallback: str) -> str:
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return fallback
        if parsed.tzinfo is not None and parsed.utcoffset() is not None:
            return parsed.astimezone(timezone.utc).isoformat()
    return fallback


def _secret_file(path: Path) -> bytes:
    data = path.read_bytes()
    if not data or len(data) > MAX_SECRET_FILE_BYTES:
        raise TransportSecurityError("control-plane transport secret file is invalid")
    stripped = data.strip()
    if stripped.startswith(b"base64url:"):
        value = stripped.removeprefix(b"base64url:").decode("ascii")
        padded = value + "=" * (-len(value) % 4)
        try:
            data = base64.urlsafe_b64decode(padded)
        except (ValueError, UnicodeError) as exc:
            raise TransportSecurityError("transport secret file is malformed") from exc
    if len(data) < 32:
        raise TransportSecurityError("control-plane transport secret must contain 32 bytes")
    return data


@dataclass(frozen=True)
class DeploymentPeer:
    voter_id: str
    public_key: str
    host: str
    port: int
    display_name: str


@dataclass(frozen=True)
class ReplicatedControlPlaneDeployment:
    cluster_id: str
    local_voter_id: str
    identity_directory: Path
    local_display_name: str
    replica_database: Path
    replay_database: Path
    coordinator_database: Path
    transport_secret_file: Path
    listen_host: str
    listen_port: int
    peers: tuple[DeploymentPeer, ...]

    @classmethod
    def from_file(cls, path: Path | str) -> ReplicatedControlPlaneDeployment:
        config_path = Path(path)
        raw = config_path.read_bytes()
        if not raw or len(raw) > MAX_DEPLOYMENT_CONFIG_BYTES:
            raise ControlPlaneError("replicated control-plane config exceeds its bound")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ControlPlaneError("replicated control-plane config is not JSON") from exc
        if not isinstance(value, dict) or value.get("schema") != DEPLOYMENT_SCHEMA:
            raise ControlPlaneError("unsupported replicated control-plane config schema")
        required = {
            "schema",
            "cluster_id",
            "local_voter_id",
            "identity_directory",
            "local_display_name",
            "replica_database",
            "replay_database",
            "coordinator_database",
            "transport_secret_file",
            "listen",
            "peers",
        }
        if set(value) != required:
            raise ControlPlaneError("replicated control-plane config has unexpected fields")
        listen = value.get("listen")
        peers = value.get("peers")
        if not isinstance(listen, dict) or set(listen) != {"host", "port"}:
            raise ControlPlaneError("replicated control-plane listen config is malformed")
        if not isinstance(peers, list) or len(peers) != 3:
            raise ControlPlaneError("replicated control-plane requires exactly three peers")
        parsed: list[DeploymentPeer] = []
        for peer in peers:
            if not isinstance(peer, dict) or set(peer) != {
                "voter_id",
                "public_key",
                "host",
                "port",
                "display_name",
            }:
                raise ControlPlaneError("replicated voter entry is malformed")
            port = peer.get("port")
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ControlPlaneError("replicated voter port is invalid")
            parsed.append(
                DeploymentPeer(
                    voter_id=_text(peer.get("voter_id"), "voter_id"),
                    public_key=_text(peer.get("public_key"), "public_key"),
                    host=_text(peer.get("host"), "host"),
                    port=port,
                    display_name=_text(peer.get("display_name"), "display_name"),
                )
            )
        listen_port = listen.get("port")
        if (
            isinstance(listen_port, bool)
            or not isinstance(listen_port, int)
            or not 0 <= listen_port <= 65535
        ):
            raise ControlPlaneError("replicated listen port is invalid")
        return cls(
            cluster_id=_text(value.get("cluster_id"), "cluster_id"),
            local_voter_id=_text(value.get("local_voter_id"), "local_voter_id"),
            identity_directory=Path(_text(value.get("identity_directory"), "identity_directory")),
            local_display_name=_text(value.get("local_display_name"), "local_display_name"),
            replica_database=Path(_text(value.get("replica_database"), "replica_database")),
            replay_database=Path(_text(value.get("replay_database"), "replay_database")),
            coordinator_database=Path(_text(value.get("coordinator_database"), "coordinator_database")),
            transport_secret_file=Path(
                _text(value.get("transport_secret_file"), "transport_secret_file")
            ),
            listen_host=_text(listen.get("host"), "listen.host"),
            listen_port=listen_port,
            peers=tuple(parsed),
        )

    @property
    def configuration(self) -> VoterConfiguration:
        return VoterConfiguration(self.cluster_id, tuple(peer.voter_id for peer in self.peers))


@dataclass(frozen=True)
class RecoveryReport:
    federation_id: str
    consensus_leader_id: str
    term: int
    fencing_epoch: int
    sessions_promoted: tuple[str, ...]


class ReplicatedFederationRuntime:
    """One product host's replicated authority runtime and materialized view."""

    def __init__(
        self,
        deployment: ReplicatedControlPlaneDeployment,
        *,
        clock: Any = _now,
    ) -> None:
        self.deployment = deployment
        self.clock = clock
        configuration = deployment.configuration
        if deployment.local_voter_id not in configuration.voter_ids:
            raise ControlPlaneError("local voter is not in the configured voter set")
        identity_store = IdentityStore(
            deployment.identity_directory,
            display_name=deployment.local_display_name,
        )
        # A production voter must already possess the durable identity whose
        # public key is pinned by the other voters. Never fabricate one during
        # recovery merely because a directory is empty.
        if not identity_store.private_key_path.exists() or not identity_store.public_identity_path.exists():
            raise ControlPlaneError("replicated voter identity is not already provisioned")
        credentials = identity_store.load_or_create()
        if credentials.identity.node_id != deployment.local_voter_id:
            raise ControlPlaneError("local durable identity does not match configured voter ID")
        registry = VoterIdentityRegistry(
            configuration,
            {peer.voter_id: peer.public_key for peer in deployment.peers},
        )
        secret = _secret_file(deployment.transport_secret_file)
        self.node = ReplicaNode(
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
            clock=clock,
        )

    @property
    def registry(self) -> VoterIdentityRegistry:
        return self.codec.registry

    @property
    def state(self) -> dict[str, Any]:
        return self.node.state

    def start(self) -> None:
        self.server.start()

    def close(self) -> None:
        self.server.close()

    def require_quorum_leader(self) -> FencingToken:
        if self.node.role != ReplicaNode.LEADER or self.node.leader_id != self.node.voter_id:
            raise AuthorizationError(
                "federation-quorum-leader-required",
                "operation requires the current replicated Federation leader",
                "actor_node_id",
            )
        matched = self.node.synchronize(self.transport)
        if matched + 1 < self.node.quorum:
            raise QuorumUnavailable("current leader cannot prove an authenticated voter quorum")
        if self.node.role != ReplicaNode.LEADER:
            raise QuorumUnavailable("leadership changed during quorum verification")
        return self.node.fencing_token

    def propose(self, command: AuthorityCommand) -> None:
        self.require_quorum_leader()
        self.node.propose(command, self.transport)
        self.materialize()

    def bootstrap_new_federation(
        self,
        *,
        federation_id: str,
        session_id: str,
        creator_node_id: str,
        display_name: str,
    ) -> None:
        if self.node.state["federation_id"] is not None:
            raise ControlPlaneError("replicated Federation is already initialized")
        if not self.node.start_election(self.transport):
            raise QuorumUnavailable("could not establish quorum for Federation genesis")
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
                "federation_id": _text(federation_id, "federation_id"),
                "session_id": _text(session_id, "session_id"),
                "creator_node_id": _text(creator_node_id, "creator_node_id"),
                "display_name": _text(display_name, "display_name"),
                "voter_ids": list(self.node.configuration.voter_ids),
                "nodes": nodes,
                "members": list(self.node.configuration.voter_ids),
                "occurred_at": _stamp(self.clock()),
            },
        )
        self.node.propose(command, self.transport)
        self.materialize()

    def recover_same_federation(self) -> RecoveryReport:
        """Elect from surviving voters and keep the exact committed Federation ID."""

        self.start()
        before = self.node.state
        federation_id = before.get("federation_id")
        if not isinstance(federation_id, str) or not federation_id:
            raise FederationOperationError(
                "replicated-authority-uninitialized",
                "no authentic committed Federation state exists on this voter",
            )
        if not self.node.start_election(self.transport):
            raise QuorumUnavailable("surviving voters could not elect a quorum leader")
        promoted: list[str] = []
        state = self.node.state
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
                command_id=f"recover-leader-{session_id}-{leadership['term'] + 1}-{self.node.voter_id}",
                command_type="LEADER_TRANSITION",
                cluster_id=self.node.configuration.cluster_id,
                issued_by=self.node.voter_id,
                payload={
                    "session_id": session_id,
                    "previous_leader_node_id": leadership["leader_node_id"],
                    "leader_node_id": self.node.voter_id,
                    "term": int(leadership["term"]) + 1,
                    "occurred_at": _stamp(self.clock()),
                    "reason": "replicated-quorum-recovery",
                },
            )
            self.node.propose(command, self.transport)
            promoted.append(session_id)
            state = self.node.state
        self.materialize()
        final = self.node.state
        if final.get("federation_id") != federation_id:
            raise ControlPlaneError("Federation identity changed during quorum recovery")
        token = self.node.fencing_token
        return RecoveryReport(
            federation_id=federation_id,
            consensus_leader_id=self.node.voter_id,
            term=token.cluster_term,
            fencing_epoch=token.fencing_epoch,
            sessions_promoted=tuple(promoted),
        )

    def assert_fencing(self, token: FencingToken) -> None:
        current = self.node.fencing_token
        if (
            self.node.role != ReplicaNode.LEADER
            or token.cluster_id != current.cluster_id
            or token.cluster_term != current.cluster_term
            or token.leader_id != self.node.voter_id
            or token.fencing_epoch != current.fencing_epoch
        ):
            raise AuthorizationError(
                "stale-federation-leader",
                "operation carries a stale replicated Federation fencing token",
                "fencing_token",
            )

    def materialize(self) -> None:
        """Atomically rebuild local durable authority tables from committed state."""

        state = self.node.state
        if state.get("federation_id") is None:
            return
        now = self.clock()
        now_text = _stamp(now)
        with self.local.store.transaction() as database:
            # Nodes: preserve exact public identity, never overwrite a conflicting
            # key with state from another cluster.
            for node_id, node in sorted(state["nodes"].items()):
                existing = database.execute(
                    "SELECT public_key FROM nodes WHERE node_id=?", (node_id,)
                ).fetchone()
                if existing is not None and existing["public_key"] != node["public_key"]:
                    raise ControlPlaneError("local coordinator node identity conflicts with replicated authority")
                database.execute(
                    """
                    INSERT OR IGNORE INTO nodes(
                        node_id,display_name,public_key,created_at,identity_version,enrolled_at
                    ) VALUES(?,?,?,?,1,?)
                    """,
                    (node_id, node["display_name"], node["public_key"], now_text, now_text),
                )
                database.execute(
                    "UPDATE nodes SET display_name=?,public_key=? WHERE node_id=?",
                    (node["display_name"], node["public_key"], node_id),
                )
                database.execute(
                    """
                    INSERT OR IGNORE INTO node_connectivity(node_id,state)
                    VALUES(?, 'disconnected')
                    """,
                    (node_id,),
                )
            for node_id, revocation in sorted(state["revocations"].items()):
                database.execute(
                    """
                    UPDATE nodes SET revoked_at=COALESCE(revoked_at,?),
                        revoked_by=?,revocation_reason=? WHERE node_id=?
                    """,
                    (
                        _event_time(revocation.get("occurred_at"), now_text),
                        REPLICATED_COORDINATOR_ID,
                        str(revocation.get("reason") or "replicated-revocation")[:512],
                        node_id,
                    ),
                )
                database.execute(
                    "UPDATE node_connectivity SET state='revoked' WHERE node_id=?",
                    (node_id,),
                )

            for session_id, session in sorted(state["sessions"].items()):
                events = tuple(state["session_events"].get(session_id, ()))
                created_at = (
                    _event_time(events[0].get("occurred_at"), now_text)
                    if events
                    else now_text
                )
                database.execute(
                    """
                    INSERT INTO sessions(
                        session_id,display_name,state,revision,created_at,
                        created_by_node_id,coordinator_id
                    ) VALUES(?,?,'active',?,?,?,?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        display_name=excluded.display_name,
                        state='active',revision=excluded.revision,
                        created_by_node_id=excluded.created_by_node_id,
                        coordinator_id=excluded.coordinator_id
                    """,
                    (
                        session_id,
                        session["display_name"],
                        len(events),
                        created_at,
                        session["creator_node_id"],
                        REPLICATED_COORDINATOR_ID,
                    ),
                )
                members = state["memberships"].get(session_id, {})
                for node_id, active in sorted(members.items()):
                    database.execute(
                        """
                        INSERT OR IGNORE INTO session_memberships(
                            session_id,node_id,joined_at
                        ) VALUES(?,?,?)
                        """,
                        (session_id, node_id, created_at),
                    )
                    if active:
                        database.execute(
                            """
                            UPDATE session_memberships SET removed_at=NULL,
                                removed_by=NULL,removal_reason=NULL
                            WHERE session_id=? AND node_id=?
                            """,
                            (session_id, node_id),
                        )
                    else:
                        database.execute(
                            """
                            UPDATE session_memberships SET removed_at=COALESCE(removed_at,?),
                                removed_by=?,removal_reason=COALESCE(removal_reason,'replicated-removal')
                            WHERE session_id=? AND node_id=?
                            """,
                            (now_text, REPLICATED_COORDINATOR_ID, session_id, node_id),
                        )

                # The replicated event stream is authoritative. Rewrite only
                # leadership-event actor identity to the stable materialized
                # coordinator ID because SessionLeadershipService intentionally
                # trusts only coordinator-authored leader transitions.
                database.execute("DELETE FROM session_events WHERE session_id=?", (session_id,))
                for event in events:
                    revision = int(event["revision"])
                    event_type = str(event["event_type"])
                    actor = str(event["actor_node_id"])
                    if event_type == "session.leader.changed":
                        actor = REPLICATED_COORDINATOR_ID
                    payload_json = _canonical(event.get("payload", {})).decode("utf-8")
                    stable = hashlib.sha256(
                        f"{session_id}:{revision}:{event_type}".encode()
                    ).hexdigest()
                    database.execute(
                        """
                        INSERT INTO session_events(
                            session_id,revision,event_id,request_id,event_type,
                            occurred_at,actor_node_id,payload_json,content_hash
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            session_id,
                            revision,
                            f"replicated-{stable[:32]}",
                            f"sha256:{stable}",
                            event_type,
                            _event_time(event.get("occurred_at"), now_text),
                            actor,
                            payload_json,
                            f"sha256:{hashlib.sha256(payload_json.encode('utf-8')).hexdigest()}",
                        ),
                    )

                authoritative_caps = state["capabilities"].get(session_id, {})
                rows = database.execute(
                    "SELECT * FROM capabilities WHERE session_id=?", (session_id,)
                ).fetchall()
                existing_caps = {str(row["capability_id"]): row for row in rows}
                for capability_id, capability in sorted(authoritative_caps.items()):
                    row = existing_caps.get(capability_id)
                    protocol = str(row["protocol"]) if row is not None else "replicated-authority"
                    protocol_version = str(row["protocol_version"]) if row is not None else "1"
                    status = str(row["status"]) if row is not None else "ready"
                    properties_json = str(row["properties_json"]) if row is not None else "{}"
                    announced_at = str(row["announced_at"]) if row is not None else now_text
                    database.execute(
                        """
                        INSERT INTO capabilities(
                            session_id,capability_id,node_id,type,protocol,
                            protocol_version,status,properties_json,announced_at,last_heartbeat_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(session_id,capability_id) DO UPDATE SET
                            node_id=excluded.node_id,type=excluded.type,
                            protocol=excluded.protocol,protocol_version=excluded.protocol_version,
                            status=excluded.status,properties_json=excluded.properties_json
                        """,
                        (
                            session_id,
                            capability_id,
                            capability["owner_node_id"],
                            capability["capability_type"],
                            protocol,
                            protocol_version,
                            status,
                            properties_json,
                            announced_at,
                            now_text,
                        ),
                    )
                if authoritative_caps:
                    placeholders = ",".join("?" for _ in authoritative_caps)
                    database.execute(
                        f"DELETE FROM capabilities WHERE session_id=? AND capability_id NOT IN ({placeholders})",
                        (session_id, *authoritative_caps.keys()),
                    )
                else:
                    database.execute("DELETE FROM capabilities WHERE session_id=?", (session_id,))

    def replicated_leader(self, session_id: str) -> tuple[str, int]:
        leader = self.node.state["leaders"].get(session_id)
        if leader is None:
            raise AuthorizationError("unknown-session", "target session does not exist", "session_id")
        return str(leader["leader_node_id"]), int(leader["term"])


class ReplicatedSessionCoordinator:
    """Relay-compatible coordinator facade whose authority writes require quorum."""

    def __init__(self, runtime: ReplicatedFederationRuntime) -> None:
        self.runtime = runtime
        self._local = runtime.local
        self.store = self._local.store
        self.event_log = self._local.event_log
        self.leadership = self._local.leadership

    @property
    def coordinator_id(self) -> str:
        return REPLICATED_COORDINATOR_ID

    def __getattr__(self, name: str) -> Any:
        return getattr(self._local, name)

    def _leader(self, session_id: str, actor_node_id: str) -> None:
        leader, _term = self.runtime.replicated_leader(session_id)
        if actor_node_id != leader:
            raise AuthorizationError(
                "federation-leader-required",
                "operation requires the current replicated Federation leader",
                "actor_node_id",
            )
        if leader != self.runtime.node.voter_id:
            raise AuthorizationError(
                "federation-leader-not-local",
                "leader-only operation must be served by the elected voter",
                "actor_node_id",
            )
        self.runtime.require_quorum_leader()

    def create_enrollment_token(self, *, ttl_seconds: int = 600, max_uses: int = 1) -> dict[str, Any]:
        self.runtime.require_quorum_leader()
        return self._local.create_enrollment_token(ttl_seconds=ttl_seconds, max_uses=max_uses)

    def create_pairing_material(self, *, session_id: str, actor_node_id: str, ttl_seconds: int = 600, request_id: str) -> dict[str, Any]:
        self._leader(session_id, actor_node_id)
        return self._local.create_pairing_material(
            session_id=session_id,
            actor_node_id=actor_node_id,
            ttl_seconds=ttl_seconds,
            request_id=request_id,
        )

    def create_invitation(self, *, session_id: str, actor_node_id: str, ttl_seconds: int = 600, max_uses: int = 1, request_id: str | None = None) -> dict[str, Any]:
        self._leader(session_id, actor_node_id)
        return self._local.create_invitation(
            session_id=session_id,
            actor_node_id=actor_node_id,
            ttl_seconds=ttl_seconds,
            max_uses=max_uses,
            request_id=request_id,
        )

    def enroll_node(self, identity: NodeIdentity, *, token: str) -> NodeIdentity:
        self.runtime.require_quorum_leader()
        enrolled = self._local.enroll_node(identity, token=token)
        command = AuthorityCommand(
            command_id=f"enroll-{hashlib.sha256(identity.node_id.encode()).hexdigest()}",
            command_type="NODE_ENROLL",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=self.runtime.node.voter_id,
            payload={
                "node_id": identity.node_id,
                "display_name": identity.display_name,
                "public_key": identity.public_key,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        try:
            self.runtime.propose(command)
        except BaseException:
            self.runtime.materialize()
            raise
        return enrolled

    def create_session(self, *, actor_node_id: str, display_name: str, request_id: str, session_id: str | None = None) -> Session:
        self.runtime.require_quorum_leader()
        session_id = session_id or f"session-{uuid.uuid4().hex}"
        command = AuthorityCommand(
            command_id=f"session-create-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type="SESSION_CREATE",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=actor_node_id,
            payload={
                "session_id": session_id,
                "creator_node_id": actor_node_id,
                "display_name": display_name,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        self.runtime.propose(command)
        session = self.store.get_session(session_id)
        if session is None:
            raise ControlPlaneError("materialized session is unavailable after commit")
        return session

    def join_session(self, *, node_id: str, token: str, request_id: str, expected_session_id: str | None = None) -> Session:
        self.runtime.require_quorum_leader()
        session = self._local.join_session(
            node_id=node_id,
            token=token,
            request_id=request_id,
            expected_session_id=expected_session_id,
        )
        command = AuthorityCommand(
            command_id=f"member-add-{hashlib.sha256((node_id + ':' + request_id).encode()).hexdigest()}",
            command_type="SESSION_MEMBER_ADD",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=self.runtime.node.voter_id,
            payload={
                "session_id": session.session_id,
                "node_id": node_id,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        try:
            self.runtime.propose(command)
        except BaseException:
            self.runtime.materialize()
            raise
        materialized = self.store.get_session(session.session_id)
        return materialized or session

    def remove_member(self, *, session_id: str, actor_node_id: str, target_node_id: str, request_id: str, reason: str) -> bool:
        self._leader(session_id, actor_node_id)
        state = self.runtime.node.state
        active = state["memberships"].get(session_id, {}).get(target_node_id) is True
        if not active:
            return False
        command = AuthorityCommand(
            command_id=f"member-remove-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type="SESSION_MEMBER_REMOVE",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=actor_node_id,
            payload={
                "session_id": session_id,
                "node_id": target_node_id,
                "reason": reason,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        self.runtime.propose(command)
        return True

    def revoke_node(self, *, node_id: str, reason: str, request_id: str, revoked_by: str = "local-admin") -> bool:
        del revoked_by
        self.runtime.require_quorum_leader()
        if node_id in self.runtime.node.state["revocations"]:
            return False
        command = AuthorityCommand(
            command_id=f"node-revoke-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type="NODE_REVOKE",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=self.runtime.node.voter_id,
            payload={
                "node_id": node_id,
                "reason": reason,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        self.runtime.propose(command)
        return True

    def transfer_session_leader(self, *, session_id: str, actor_node_id: str, target_node_id: str, request_id: str):
        self._leader(session_id, actor_node_id)
        current, term = self.runtime.replicated_leader(session_id)
        command = AuthorityCommand(
            command_id=f"leader-transfer-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type="LEADER_TRANSITION",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=actor_node_id,
            payload={
                "session_id": session_id,
                "previous_leader_node_id": current,
                "leader_node_id": target_node_id,
                "term": term + 1,
                "occurred_at": _stamp(self.runtime.clock()),
                "reason": "explicit-handover",
            },
        )
        self.runtime.propose(command)
        return self._local.session_leadership(session_id), None

    def announce_capability(self, capability: CapabilityAnnouncement, *, actor_node_id: str, request_id: str):
        self.runtime.require_quorum_leader()
        # Preserve the full already-public capability metadata in the local view;
        # the consensus state owns identity/type/owner and therefore decides
        # whether this row survives materialization after failover.
        result = self._local.announce_capability(
            capability,
            actor_node_id=actor_node_id,
            request_id=request_id,
        )
        command = AuthorityCommand(
            command_id=f"capability-{hashlib.sha256(request_id.encode()).hexdigest()}",
            command_type="CAPABILITY_DECLARE",
            cluster_id=self.runtime.node.configuration.cluster_id,
            issued_by=actor_node_id,
            payload={
                "session_id": capability.session_id,
                "capability_id": capability.capability_id,
                "capability_type": capability.type,
                "owner_node_id": capability.node_id,
                "occurred_at": _stamp(self.runtime.clock()),
            },
        )
        try:
            self.runtime.propose(command)
        except BaseException:
            self.runtime.materialize()
            raise
        return result


__all__ = [
    "DEPLOYMENT_SCHEMA",
    "REPLICATED_COORDINATOR_ID",
    "RecoveryReport",
    "ReplicatedControlPlaneDeployment",
    "ReplicatedFederationRuntime",
    "ReplicatedSessionCoordinator",
]

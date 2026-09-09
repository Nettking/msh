"""Quorum-witnessed migration of a legacy Federation whose creator is offline.

Pre-C03 paired members already retain two pieces of durable evidence:

* the trusted ``RemotePairingState`` containing the Federation/session binding;
* their gap-free ``NodeState`` replay journal containing the authoritative
  session event stream they actually received from the old coordinator.

This module lets two authenticated C03 voters independently summarize and sign
that retained history.  Migration proceeds only when a voter quorum signs the
*same* canonical manifest.  The old creator ID is retained as immutable
provenance, but an unavailable historical public key is never invented as
working authentication material: historical-only identities receive an
explicit deterministic ``provenance-unavailable`` tombstone and are removed and
revoked before the C03 readiness seal is committed.

The fixed C03 voter identities remain the only cryptographic identities trusted
for post-migration authority.  Any active legacy member outside the voter set
other than the old creator/current legacy leader makes migration fail closed,
because its missing public key cannot safely be reconstructed from member
replay state.
"""

from __future__ import annotations

import hashlib
import json
import socket
import socketserver
import sqlite3
import struct
import threading
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from catalog.federation.human_auth import AUTHORITY_EVENT, USER_EVENT
from catalog.federation.models import SessionEvent
from catalog.federation.session_leadership import LEADERSHIP_SCHEMA
from catalog.flask_app.services.federation_pairing_service import RemotePairingStore
from catalog.node.identity import verify_signature

from .control_plane_replication import AuthorityCommand, ControlPlaneError, ReplicaNode
from .control_plane_runtime import PhysicalReadyReplicatedFederationRuntime
from .control_plane_transport import (
    MAX_WIRE_BYTES,
    SecureEnvelopeCodec,
    VoterEndpoint,
)

MIGRATION_MANIFEST_SCHEMA = "fcp.control-plane.legacy-migration-manifest.v1"
MIGRATION_ATTESTATION_SCHEMA = "fcp.control-plane.legacy-migration-attestation.v1"
MIGRATION_RPC = "legacy_migration.attest"
MIGRATION_PORT_OFFSET = 2
MAX_LEGACY_EVENTS = 20_000
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_HEADER = struct.Struct("!I")


class LegacyMigrationError(ControlPlaneError):
    """Legacy authority could not be reconstructed without fabrication."""


def _canonical(value: object, *, maximum: int = MAX_MANIFEST_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LegacyMigrationError("legacy migration value is not canonical JSON") from exc
    if len(encoded) > maximum:
        raise LegacyMigrationError("legacy migration value exceeds its size bound")
    return encoded


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or any(ord(ch) < 32 for ch in value):
        raise LegacyMigrationError(f"{field} is missing or malformed")
    if len(value.encode("utf-8")) > 512:
        raise LegacyMigrationError(f"{field} exceeds its size bound")
    return value


def _read_event_journal(database: Path, session_id: str) -> tuple[SessionEvent, ...]:
    if not database.is_file():
        raise LegacyMigrationError("legacy member NodeState database is absent")
    # Encode a filesystem name, not a URL authority. Windows extended paths
    # are valid SQLite filenames, but as_uri() turns their prefix into an
    # invalid URI hostname. Keep the legacy database strictly read-only.
    uri = "file:" + quote(str(database.resolve())) + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        session = connection.execute(
            "SELECT * FROM joined_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if session is None or session["membership_state"] != "joined":
            raise LegacyMigrationError("legacy witness is not an active session member")
        if bool(session["replaying"]):
            raise LegacyMigrationError("legacy witness replay is incomplete")
        revision = int(session["last_applied_revision"])
        rows = connection.execute(
            """
            SELECT revision,event_json FROM applied_events
            WHERE session_id=? ORDER BY revision
            """,
            (session_id,),
        ).fetchall()
    except sqlite3.Error as exc:
        raise LegacyMigrationError("legacy member NodeState database is unreadable") from exc
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass
    if revision < 1 or len(rows) != revision or len(rows) > MAX_LEGACY_EVENTS:
        raise LegacyMigrationError("legacy witness journal is incomplete or exceeds its bound")
    events: list[SessionEvent] = []
    for expected, row in enumerate(rows, start=1):
        if int(row["revision"]) != expected:
            raise LegacyMigrationError("legacy witness journal has a revision gap")
        try:
            event = SessionEvent.from_dict(json.loads(row["event_json"]))
        except Exception as exc:  # model owns detailed validation
            raise LegacyMigrationError("legacy witness contains a malformed event") from exc
        if event.session_id != session_id or event.revision != expected:
            raise LegacyMigrationError("legacy witness event identity is inconsistent")
        events.append(event)
    return tuple(events)


def _fold_manifest(
    *,
    federation_id: str,
    session_id: str,
    witness_voter_id: str,
    node_state_database: Path,
    pairing_state_path: Path,
) -> dict[str, Any]:
    saved = RemotePairingStore(pairing_state_path).load()
    if saved is None:
        raise LegacyMigrationError("trusted saved Federation binding is absent")
    binding = saved.binding
    if (
        binding.federation_id != federation_id
        or binding.internal_session_id != session_id
        or binding.device_id != witness_voter_id
        or binding.trusted is not True
    ):
        raise LegacyMigrationError("legacy saved Federation binding does not match this voter")

    events = _read_event_journal(node_state_database, session_id)
    first = events[0]
    if first.event_type != "session.created":
        raise LegacyMigrationError("legacy authority journal does not begin with session.created")
    creator = _text(first.actor_node_id, "creator_node_id")
    display_name = first.payload.get("display_name") if isinstance(first.payload, dict) else None
    display_name = _text(display_name, "display_name")

    active_members: set[str] = {creator}
    leader = creator
    leadership_term = 1
    leadership_chain: list[dict[str, Any]] = []
    human_auth_present = False

    for event in events:
        payload = event.payload if isinstance(event.payload, dict) else {}
        if event.event_type == "node.joined":
            node_id = payload.get("node_id")
            if isinstance(node_id, str) and node_id:
                active_members.add(node_id)
        elif event.event_type == "node.left":
            node_id = payload.get("node_id")
            if isinstance(node_id, str) and node_id:
                active_members.discard(node_id)
        elif event.event_type == "session.leader.changed":
            next_term = payload.get("term")
            previous = payload.get("previous_leader_node_id")
            next_leader = payload.get("leader_node_id")
            if (
                payload.get("schema") != LEADERSHIP_SCHEMA
                or previous != leader
                or isinstance(next_term, bool)
                or not isinstance(next_term, int)
                or next_term != leadership_term + 1
                or not isinstance(next_leader, str)
                or not next_leader
            ):
                raise LegacyMigrationError("legacy leadership history is not contiguous")
            leadership_chain.append(
                {
                    "previous_leader_node_id": leader,
                    "leader_node_id": next_leader,
                    "term": next_term,
                    "occurred_at": event.occurred_at.isoformat(),
                }
            )
            leader = next_leader
            leadership_term = next_term
        elif event.event_type in {AUTHORITY_EVENT, USER_EVENT}:
            human_auth_present = True

    encoded_events = [event.to_dict() for event in events]
    history_digest = "sha256:" + hashlib.sha256(_canonical(encoded_events)).hexdigest()
    manifest = {
        "schema": MIGRATION_MANIFEST_SCHEMA,
        "federation_id": federation_id,
        "session_id": session_id,
        "creator_node_id": creator,
        "display_name": display_name,
        "created_at": first.occurred_at.isoformat(),
        "last_revision": events[-1].revision,
        "history_digest": history_digest,
        "legacy_leader_node_id": leader,
        "legacy_leadership_term": leadership_term,
        "leadership_chain": leadership_chain,
        "active_member_ids": sorted(active_members),
        "human_auth_present": human_auth_present,
    }
    _canonical(manifest)
    return manifest


@dataclass(frozen=True)
class LegacyMigrationAttestation:
    voter_id: str
    manifest: dict[str, Any]
    signature: str

    def verify(self, codec: SecureEnvelopeCodec) -> None:
        if self.voter_id not in codec.registry.configuration.voter_ids:
            raise LegacyMigrationError("legacy migration attestation is not from a voter")
        try:
            valid = verify_signature(
                codec.registry.public_key(self.voter_id),
                _canonical(self.manifest),
                self.signature,
            )
        except Exception as exc:
            raise LegacyMigrationError("legacy migration attestation signature is malformed") from exc
        if not valid:
            raise LegacyMigrationError("legacy migration attestation signature is invalid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": MIGRATION_ATTESTATION_SCHEMA,
            "voter_id": self.voter_id,
            "manifest": self.manifest,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, value: object) -> LegacyMigrationAttestation:
        if not isinstance(value, dict) or value.get("schema") != MIGRATION_ATTESTATION_SCHEMA:
            raise LegacyMigrationError("legacy migration attestation schema is invalid")
        manifest = value.get("manifest")
        if not isinstance(manifest, dict) or manifest.get("schema") != MIGRATION_MANIFEST_SCHEMA:
            raise LegacyMigrationError("legacy migration manifest schema is invalid")
        return cls(
            voter_id=_text(value.get("voter_id"), "voter_id"),
            manifest=dict(manifest),
            signature=_text(value.get("signature"), "signature"),
        )


class _WitnessTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class LegacyMigrationWitnessServer:
    """Authenticated read-only endpoint exposing only a signed migration manifest."""

    def __init__(
        self,
        codec: SecureEnvelopeCodec,
        *,
        node_state_database: Path,
        pairing_state_path: Path,
        host: str,
        port: int,
    ) -> None:
        self.codec = codec
        self.node_state_database = Path(node_state_database)
        self.pairing_state_path = Path(pairing_state_path)
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                try:
                    header = owner._recv_exact(self.request, _HEADER.size)
                    length = _HEADER.unpack(header)[0]
                    if length < 1 or length > MAX_WIRE_BYTES:
                        return
                    opened = owner.codec.open(owner._recv_exact(self.request, length))
                    if opened.rpc != MIGRATION_RPC:
                        return
                    federation_id = _text(opened.payload.get("federation_id"), "federation_id")
                    session_id = _text(opened.payload.get("session_id"), "session_id")
                    attestation = owner.attest(federation_id, session_id)
                    body = {
                        "ok": True,
                        "correlation_id": opened.request_id,
                        "response": attestation.to_dict(),
                    }
                    sealed = owner.codec.seal(
                        opened.sender_id,
                        f"{opened.rpc}.response",
                        body,
                    )
                    self.request.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
                except (OSError, ControlPlaneError, sqlite3.Error, ValueError, TypeError):
                    return

        self._server = _WitnessTCPServer((host, port), Handler)
        self._thread: threading.Thread | None = None

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        parts: list[bytes] = []
        remaining = size
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise OSError("legacy migration peer closed the connection")
            parts.append(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    def attest(self, federation_id: str, session_id: str) -> LegacyMigrationAttestation:
        manifest = _fold_manifest(
            federation_id=federation_id,
            session_id=session_id,
            witness_voter_id=self.codec.voter_id,
            node_state_database=self.node_state_database,
            pairing_state_path=self.pairing_state_path,
        )
        return LegacyMigrationAttestation(
            voter_id=self.codec.voter_id,
            manifest=manifest,
            signature=self.codec.credentials.sign(_canonical(manifest)),
        )

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"fcp-legacy-witness-{self.codec.voter_id[:16]}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        if self._thread is None:
            self._server.server_close()
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)
        self._thread = None


class LegacyMigrationWitnessTransport:
    def __init__(
        self,
        codec: SecureEnvelopeCodec,
        endpoints: Mapping[str, VoterEndpoint],
        *,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.codec = codec
        self.endpoints = dict(endpoints)
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        parts: list[bytes] = []
        remaining = size
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise OSError("legacy migration peer closed the connection")
            parts.append(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    def attest(self, target: str, federation_id: str, session_id: str) -> LegacyMigrationAttestation:
        endpoint = self.endpoints[target]
        sealed = self.codec.seal(
            target,
            MIGRATION_RPC,
            {"federation_id": federation_id, "session_id": session_id},
        )
        with socket.create_connection(
            (endpoint.host, endpoint.port), timeout=self.timeout_seconds
        ) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
            length = _HEADER.unpack(self._recv_exact(connection, _HEADER.size))[0]
            if length < 1 or length > MAX_WIRE_BYTES:
                raise OSError("legacy migration response exceeded its frame bound")
            opened = self.codec.open(
                self._recv_exact(connection, length),
                expected_sender=target,
                expected_rpc=f"{MIGRATION_RPC}.response",
            )
        if opened.payload.get("correlation_id") != sealed.request_id or opened.payload.get("ok") is not True:
            raise LegacyMigrationError("legacy migration witness rejected the request")
        attestation = LegacyMigrationAttestation.from_dict(opened.payload.get("response"))
        if attestation.voter_id != target:
            raise LegacyMigrationError("legacy migration witness identity mismatch")
        attestation.verify(self.codec)
        return attestation


def _migration_endpoints(
    endpoints: Mapping[str, VoterEndpoint],
) -> dict[str, VoterEndpoint]:
    result: dict[str, VoterEndpoint] = {}
    for voter_id, endpoint in endpoints.items():
        port = endpoint.port + MIGRATION_PORT_OFFSET
        if port > 65535:
            raise LegacyMigrationError("control-plane port leaves no migration witness port")
        result[voter_id] = VoterEndpoint(endpoint.host, port)
    return result


def _provenance_stub(node_id: str) -> dict[str, str]:
    digest = hashlib.sha256(node_id.encode("utf-8")).hexdigest()
    return {
        "node_id": node_id,
        "display_name": "Historical Federation provenance",
        "public_key": f"provenance-unavailable-sha256:{digest}",
    }


class OfflineCreatorRecoverableRuntime(PhysicalReadyReplicatedFederationRuntime):
    """Physical runtime that can migrate a legacy Federation after creator loss."""

    def __init__(
        self,
        *args: Any,
        legacy_node_state_database: Path | str = "/app/data/federation/device/node_state.sqlite3",
        legacy_pairing_state_path: Path | str = "/app/data/federation/onboarding/remote_pairing.json",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        with ExitStack() as construction:
            construction.callback(super().close)
            self.legacy_node_state_database = Path(legacy_node_state_database)
            self.legacy_pairing_state_path = Path(legacy_pairing_state_path)
            if self.deployment.listen_port > 65533:
                raise LegacyMigrationError("control-plane port leaves no migration witness port")
            peer_endpoints = {
                peer.voter_id: VoterEndpoint(peer.host, peer.port)
                for peer in self.deployment.peers
                if peer.voter_id != self.node.voter_id
            }
            self.legacy_witness_server = LegacyMigrationWitnessServer(
                self.codec,
                node_state_database=self.legacy_node_state_database,
                pairing_state_path=self.legacy_pairing_state_path,
                host=self.deployment.listen_host,
                port=self.deployment.listen_port + MIGRATION_PORT_OFFSET,
            )
            construction.callback(self.legacy_witness_server.close)
            self.legacy_witness_transport = LegacyMigrationWitnessTransport(
                self.codec,
                _migration_endpoints(peer_endpoints),
            )
            construction.pop_all()

    def start(self) -> None:
        self.legacy_witness_server.start()
        try:
            super().start()
        except BaseException:
            self.legacy_witness_server.close()
            raise

    def close(self) -> None:
        try:
            super().close()
        finally:
            self.legacy_witness_server.close()

    def _matching_witness_quorum(self, federation_id: str, session_id: str) -> dict[str, Any]:
        local = self.legacy_witness_server.attest(federation_id, session_id)
        local.verify(self.codec)
        manifest_bytes = _canonical(local.manifest)
        voters = {local.voter_id}
        for target in self.node.peers:
            try:
                remote = self.legacy_witness_transport.attest(target, federation_id, session_id)
            except (OSError, ControlPlaneError, TimeoutError):
                continue
            if _canonical(remote.manifest) != manifest_bytes:
                continue
            voters.add(remote.voter_id)
        if len(voters) < self.node.quorum:
            raise LegacyMigrationError(
                "legacy Federation migration lacks two matching authenticated member witnesses"
            )
        return dict(local.manifest)

    def _attempt_existing_federation_bootstrap(self) -> None:
        if self.bootstrap_federation_id is None or self.bootstrap_session_id is None:
            return
        state = self.node.state
        existing_id = state.get("federation_id")
        if existing_id is not None and existing_id != self.bootstrap_federation_id:
            raise LegacyMigrationError(
                "configured migration Federation ID conflicts with replicated authority"
            )
        if existing_id is not None:
            return
        if self.node.role != ReplicaNode.LEADER and not self.node.start_election(self.transport):
            self._next_election_at = self._election_deadline()
            return
        manifest = self._matching_witness_quorum(
            self.bootstrap_federation_id,
            self.bootstrap_session_id,
        )
        self._bootstrap_from_witness_manifest(manifest)
        self.node.synchronize(self.transport)
        self.materialize()
        self._restore_human_credentials_if_available()
        self._sync_human_credentials_if_due(force=True)

    def _seal_authority(
        self,
        *,
        federation_id: str,
        session_id: str,
        creator_node_id: str,
        occurred_at: str,
    ) -> None:
        # The seal proves replicated readiness, not historical authorship.  Its
        # owner must therefore be the authenticated current voter leader.
        command = AuthorityCommand(
            command_id=(
                "bootstrap-seal-"
                + hashlib.sha256(f"{federation_id}:{session_id}".encode()).hexdigest()
            ),
            command_type="CAPABILITY_DECLARE",
            cluster_id=self.node.configuration.cluster_id,
            issued_by=self.node.voter_id,
            payload={
                "session_id": session_id,
                "capability_id": "fcp.control-plane.bootstrap-complete",
                "capability_type": "fcp-control-plane-bootstrap-seal",
                "owner_node_id": self.node.voter_id,
                "occurred_at": occurred_at,
            },
        )
        self.node.propose(command, self.transport)

    def _bootstrap_from_witness_manifest(self, manifest: Mapping[str, Any]) -> None:
        if self.node.role != ReplicaNode.LEADER:
            raise LegacyMigrationError("legacy migration requires the elected C03 leader")
        federation_id = _text(manifest.get("federation_id"), "federation_id")
        session_id = _text(manifest.get("session_id"), "session_id")
        if federation_id != self.bootstrap_federation_id or session_id != self.bootstrap_session_id:
            raise LegacyMigrationError("legacy witness targets another Federation")
        creator = _text(manifest.get("creator_node_id"), "creator_node_id")
        legacy_leader = _text(manifest.get("legacy_leader_node_id"), "legacy_leader_node_id")
        term_value = manifest.get("legacy_leadership_term")
        if isinstance(term_value, bool) or not isinstance(term_value, int) or term_value < 1:
            raise LegacyMigrationError("legacy leadership term is invalid")
        active_raw = manifest.get("active_member_ids")
        if not isinstance(active_raw, list) or not all(isinstance(item, str) and item for item in active_raw):
            raise LegacyMigrationError("legacy active member set is malformed")
        active = set(active_raw)
        voter_set = set(self.node.configuration.voter_ids)
        if not voter_set <= active:
            raise LegacyMigrationError("a configured C03 voter was not an active legacy member")
        extra_active = active - voter_set
        allowed_extra = {creator, legacy_leader} - voter_set
        if not extra_active <= allowed_extra:
            raise LegacyMigrationError(
                "legacy Federation has active non-voter members whose cryptographic identity cannot be reconstructed"
            )
        # Pre-C03 journals contain no passwords; recovery requires an existing
        # quorum-certified snapshot instead of resetting the users.
        if (
            manifest.get("human_auth_present") is True
            and self.credential_manager.best_committed() is None
        ):
            raise LegacyMigrationError(
                "legacy Federation contains human-auth users but no recoverable quorum-certified credential snapshot"
            )

        chain_raw = manifest.get("leadership_chain")
        if not isinstance(chain_raw, list):
            raise LegacyMigrationError("legacy leadership chain is malformed")
        chain: list[dict[str, Any]] = []
        historical_ids = {creator, legacy_leader}
        expected_leader = creator
        expected_term = 1
        for item in chain_raw:
            if not isinstance(item, dict):
                raise LegacyMigrationError("legacy leadership chain entry is malformed")
            previous = _text(item.get("previous_leader_node_id"), "previous_leader_node_id")
            target = _text(item.get("leader_node_id"), "leader_node_id")
            term = item.get("term")
            occurred_at = _text(item.get("occurred_at"), "occurred_at")
            if previous != expected_leader or isinstance(term, bool) or not isinstance(term, int) or term != expected_term + 1:
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
        created_at = _text(manifest.get("created_at"), "created_at")
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
                "display_name": _text(manifest.get("display_name"), "display_name"),
                "voter_ids": list(self.node.configuration.voter_ids),
                "nodes": nodes,
                "members": bootstrap_members,
                "occurred_at": created_at,
            },
        )
        self.node.propose(genesis, self.transport)

        for item in chain:
            transition = AuthorityCommand(
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
            )
            self.node.propose(transition, self.transport)

        current = self.node.state["leaders"][session_id]
        if current["leader_node_id"] != self.node.voter_id:
            next_term = int(current["term"]) + 1
            self.node.propose(
                AuthorityCommand(
                    command_id=f"witnessed-migration-failover-{session_id}-{next_term}-{self.node.voter_id}",
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

        # Historical-only nodes existed solely so the exact old leadership
        # chain could be replayed. They are explicitly stripped of active
        # membership and revoked before readiness, so their tombstone key can
        # never grant authentication authority.
        for node_id in sorted(historical_ids - voter_set):
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
                        "occurred_at": created_at,
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
                        "reason": "historical-provenance-only-no-authentication-key",
                        "occurred_at": created_at,
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
        if self.node.state.get("federation_id") != federation_id:
            raise LegacyMigrationError("Federation identity changed during witnessed migration")


__all__ = [
    "LegacyMigrationAttestation",
    "LegacyMigrationError",
    "LegacyMigrationWitnessServer",
    "LegacyMigrationWitnessTransport",
    "OfflineCreatorRecoverableRuntime",
]

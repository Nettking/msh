"""Authenticated encrypted inter-host transport for replicated Federation authority.

The consensus core deliberately knows only voter IDs.  This module supplies the
missing deployment boundary: each configured voter ID is pinned to the
repository's durable Ed25519 node identity, every RPC is signed by that
identity, and every payload is encrypted with an AEAD key derived from a
separately provisioned Federation control-plane transport secret.

Security properties:

* a peer cannot impersonate another voter by changing ``candidate_id`` or
  ``leader_id`` text; the signed transport identity must match those fields;
* AES-GCM protects RPC payload confidentiality and integrity in transit;
* Ed25519 signatures bind sender, recipient, RPC kind, request ID, nonce and
  ciphertext to the configured voter identity;
* accepted request IDs are persisted in SQLite so authenticated replay remains
  rejected across process restart;
* malformed/authentication failures are fail-closed and are never counted as
  consensus communication success.

The transport secret is intentionally not serialized, logged, represented or
placed in public Federation state.  Deployment is responsible for provisioning
the same high-entropy secret to the three eligible voters through a private
operator channel.  Rotating that secret requires a coordinated restart of the
fixed v1 voter set; voter reconfiguration remains out of scope for v1.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import socketserver
import sqlite3
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from catalog.node.identity import (
    NodeCredentials,
    derive_node_id_from_public_key,
    verify_signature,
)

from .control_plane_replication import (
    AppendResponse,
    ControlPlaneError,
    LogEntry,
    ReplicaNode,
    ReplicationTransport,
    Snapshot,
    SnapshotResponse,
    VoteResponse,
    VoterConfiguration,
)

TRANSPORT_SCHEMA = "fcp.control-plane.secure-rpc.v1"
MAX_WIRE_BYTES = 16 * 1024 * 1024
MAX_RPC_PAYLOAD_BYTES = 12 * 1024 * 1024
DEFAULT_SOCKET_TIMEOUT_SECONDS = 5.0
DEFAULT_REPLAY_RECORDS = 16_384
_MIN_SECRET_BYTES = 32
_NONCE_BYTES = 12
_REQUEST_ID_BYTES = 18
_HEADER = struct.Struct("!I")
_ALLOWED_REQUEST_RPCS = frozenset({"request_vote", "append_entries", "install_snapshot"})


class TransportSecurityError(ControlPlaneError):
    """An RPC failed the authenticated/encrypted transport contract."""


class ReplayRejected(TransportSecurityError):
    """A previously authenticated transport request was replayed."""


def _canonical(value: object, *, maximum: int = MAX_RPC_PAYLOAD_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TransportSecurityError("transport value is not canonical JSON") from exc
    if len(encoded) > maximum:
        raise TransportSecurityError("transport value exceeds its size bound")
    return encoded


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: object, *, field: str, expected: int | None = None) -> bytes:
    if not isinstance(value, str) or not value:
        raise TransportSecurityError(f"{field} must be base64url text")
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise TransportSecurityError(f"{field} is not canonical base64url") from exc
    if _b64(decoded) != value or (expected is not None and len(decoded) != expected):
        raise TransportSecurityError(f"{field} has invalid encoded length")
    return decoded


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or any(ord(ch) < 32 for ch in value):
        raise TransportSecurityError(f"{field} must be non-empty text")
    if len(value.encode("utf-8")) > 1024:
        raise TransportSecurityError(f"{field} exceeds its size bound")
    return value


def _uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TransportSecurityError(f"{field} must be a non-negative integer")
    return value


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise TransportSecurityError(f"{field} must be boolean")
    return value


def _transport_key(cluster_id: str, secret: bytes) -> bytes:
    if not isinstance(secret, bytes) or len(secret) < _MIN_SECRET_BYTES:
        raise TransportSecurityError(
            f"control-plane transport secret must contain at least {_MIN_SECRET_BYTES} bytes"
        )
    salt = hashlib.sha256((TRANSPORT_SCHEMA + "\0" + cluster_id).encode("utf-8")).digest()
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=b"MSH FCP Federation v1 replicated authority transport",
    ).derive(secret)


@dataclass(frozen=True)
class VoterIdentityRegistry:
    """Immutable trust anchor from consensus voter ID to Ed25519 public identity."""

    configuration: VoterConfiguration
    public_keys: Mapping[str, str]

    def __post_init__(self) -> None:
        keys = dict(self.public_keys)
        expected = set(self.configuration.voter_ids)
        if set(keys) != expected:
            raise TransportSecurityError("voter identity registry must cover the exact voter set")
        for voter_id, public_key in keys.items():
            try:
                derived = derive_node_id_from_public_key(public_key)
            except Exception as exc:  # node identity module owns detailed validation
                raise TransportSecurityError("voter public identity is malformed") from exc
            if derived != voter_id:
                raise TransportSecurityError(
                    "configured voter ID is not derived from its pinned public identity"
                )
        object.__setattr__(self, "public_keys", keys)

    def public_key(self, voter_id: str) -> str:
        try:
            return self.public_keys[voter_id]
        except KeyError as exc:
            raise TransportSecurityError("unknown voter identity") from exc


@dataclass(frozen=True)
class SealedEnvelope:
    request_id: str
    wire: bytes


@dataclass(frozen=True)
class OpenedEnvelope:
    sender_id: str
    recipient_id: str
    rpc: str
    request_id: str
    payload: dict[str, Any]


class PersistentReplayGuard:
    """Durably reject authenticated request-ID replay across process restart."""

    def __init__(self, database: Path | str, *, max_records: int = DEFAULT_REPLAY_RECORDS) -> None:
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records < 1024:
            raise TransportSecurityError("replay record bound is too small")
        self.database = Path(database)
        self.max_records = max_records
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS authenticated_rpc_replay(
                    sender_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    accepted_at_ns INTEGER NOT NULL,
                    PRIMARY KEY(sender_id, request_id)
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=5.0, isolation_level=None)
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def claim(self, sender_id: str, request_id: str) -> None:
        now = time.time_ns()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO authenticated_rpc_replay(sender_id,request_id,accepted_at_ns) VALUES(?,?,?)",
                    (sender_id, request_id, now),
                )
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise ReplayRejected("authenticated control-plane RPC replayed") from exc
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM authenticated_rpc_replay"
                ).fetchone()[0]
            )
            excess = count - self.max_records
            if excess > 0:
                connection.execute(
                    """
                    DELETE FROM authenticated_rpc_replay
                    WHERE rowid IN (
                        SELECT rowid FROM authenticated_rpc_replay
                        ORDER BY accepted_at_ns, rowid
                        LIMIT ?
                    )
                    """,
                    (excess,),
                )
            connection.commit()
        finally:
            connection.close()


class SecureEnvelopeCodec:
    """Sign, encrypt, authenticate and decrypt one-voter RPC envelopes."""

    def __init__(
        self,
        credentials: NodeCredentials,
        registry: VoterIdentityRegistry,
        transport_secret: bytes,
        replay_guard: PersistentReplayGuard,
    ) -> None:
        voter_id = credentials.identity.node_id
        if voter_id not in registry.configuration.voter_ids:
            raise TransportSecurityError("local identity is not an eligible voter")
        if credentials.identity.public_key != registry.public_key(voter_id):
            raise TransportSecurityError("local private identity does not match voter trust anchor")
        self.credentials = credentials
        self.voter_id = voter_id
        self.registry = registry
        self.replay_guard = replay_guard
        self._key = _transport_key(registry.configuration.cluster_id, transport_secret)

    def __repr__(self) -> str:
        return f"SecureEnvelopeCodec(voter_id={self.voter_id!r})"

    def seal(self, recipient_id: str, rpc: str, payload: Mapping[str, Any]) -> SealedEnvelope:
        self.registry.public_key(recipient_id)
        _text(rpc, "rpc")
        request_id = "rpc-" + _b64(os.urandom(_REQUEST_ID_BYTES))
        nonce = os.urandom(_NONCE_BYTES)
        aad_object = {
            "schema": TRANSPORT_SCHEMA,
            "cluster_id": self.registry.configuration.cluster_id,
            "sender_id": self.voter_id,
            "recipient_id": recipient_id,
            "rpc": rpc,
            "request_id": request_id,
        }
        aad = _canonical(aad_object, maximum=8 * 1024)
        plaintext = _canonical(dict(payload))
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext, aad)
        signed = _canonical(
            {
                "aad": aad_object,
                "nonce": _b64(nonce),
                "ciphertext": _b64(ciphertext),
            },
            maximum=MAX_WIRE_BYTES,
        )
        envelope = {
            **aad_object,
            "nonce": _b64(nonce),
            "ciphertext": _b64(ciphertext),
            "signature": self.credentials.sign(signed),
        }
        wire = _canonical(envelope, maximum=MAX_WIRE_BYTES)
        return SealedEnvelope(request_id=request_id, wire=wire)

    def open(
        self,
        wire: bytes,
        *,
        expected_sender: str | None = None,
        expected_rpc: str | None = None,
    ) -> OpenedEnvelope:
        if not isinstance(wire, bytes) or not wire or len(wire) > MAX_WIRE_BYTES:
            raise TransportSecurityError("invalid secure RPC frame")
        try:
            envelope = json.loads(wire)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransportSecurityError("secure RPC frame is not JSON") from exc
        required = {
            "schema",
            "cluster_id",
            "sender_id",
            "recipient_id",
            "rpc",
            "request_id",
            "nonce",
            "ciphertext",
            "signature",
        }
        if not isinstance(envelope, dict) or set(envelope) != required:
            raise TransportSecurityError("secure RPC envelope has unexpected fields")
        if envelope.get("schema") != TRANSPORT_SCHEMA:
            raise TransportSecurityError("unsupported secure RPC schema")
        if envelope.get("cluster_id") != self.registry.configuration.cluster_id:
            raise TransportSecurityError("secure RPC cluster identity mismatch")
        sender_id = _text(envelope.get("sender_id"), "sender_id")
        recipient_id = _text(envelope.get("recipient_id"), "recipient_id")
        rpc = _text(envelope.get("rpc"), "rpc")
        request_id = _text(envelope.get("request_id"), "request_id")
        if recipient_id != self.voter_id:
            raise TransportSecurityError("secure RPC addressed to a different voter")
        if expected_sender is not None and sender_id != expected_sender:
            raise TransportSecurityError("secure RPC sender did not match the contacted voter")
        if expected_rpc is not None and rpc != expected_rpc:
            raise TransportSecurityError("secure RPC response kind mismatch")
        public_key = self.registry.public_key(sender_id)
        nonce = _unb64(envelope.get("nonce"), field="nonce", expected=_NONCE_BYTES)
        ciphertext = _unb64(envelope.get("ciphertext"), field="ciphertext")
        aad_object = {
            "schema": TRANSPORT_SCHEMA,
            "cluster_id": self.registry.configuration.cluster_id,
            "sender_id": sender_id,
            "recipient_id": recipient_id,
            "rpc": rpc,
            "request_id": request_id,
        }
        signed = _canonical(
            {
                "aad": aad_object,
                "nonce": envelope["nonce"],
                "ciphertext": envelope["ciphertext"],
            },
            maximum=MAX_WIRE_BYTES,
        )
        try:
            valid = verify_signature(public_key, signed, envelope.get("signature"))
        except Exception as exc:
            raise TransportSecurityError("secure RPC signature is malformed") from exc
        if not valid:
            raise TransportSecurityError("secure RPC signature verification failed")
        # Claim only after the sender is cryptographically authenticated. This
        # prevents unauthenticated traffic from filling the persistent window.
        self.replay_guard.claim(sender_id, request_id)
        aad = _canonical(aad_object, maximum=8 * 1024)
        try:
            plaintext = AESGCM(self._key).decrypt(nonce, ciphertext, aad)
        except InvalidTag as exc:
            raise TransportSecurityError("secure RPC authenticated decryption failed") from exc
        try:
            payload = json.loads(plaintext)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransportSecurityError("secure RPC payload is not JSON") from exc
        if not isinstance(payload, dict):
            raise TransportSecurityError("secure RPC payload must be an object")
        _canonical(payload)
        return OpenedEnvelope(sender_id, recipient_id, rpc, request_id, payload)


@dataclass(frozen=True)
class VoterEndpoint:
    host: str
    port: int

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise TransportSecurityError("voter endpoint host must be non-empty")
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not 1 <= self.port <= 65535:
            raise TransportSecurityError("voter endpoint port is invalid")


class SecureSocketReplicationTransport(ReplicationTransport):
    """Length-prefixed synchronous RPC client with end-to-end AEAD envelopes."""

    def __init__(
        self,
        codec: SecureEnvelopeCodec,
        endpoints: Mapping[str, VoterEndpoint],
        *,
        timeout_seconds: float = DEFAULT_SOCKET_TIMEOUT_SECONDS,
    ) -> None:
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
            raise TransportSecurityError("socket timeout must be positive")
        endpoint_map = dict(endpoints)
        for voter_id in codec.registry.configuration.voter_ids:
            if voter_id != codec.voter_id and voter_id not in endpoint_map:
                raise TransportSecurityError("endpoint map does not cover every peer voter")
        self.codec = codec
        self.endpoints = endpoint_map
        self.timeout_seconds = float(timeout_seconds)

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = sock.recv(remaining)
            if not chunk:
                raise OSError("secure control-plane peer closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _rpc(self, target: str, rpc: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            endpoint = self.endpoints[target]
        except KeyError as exc:
            raise OSError("secure control-plane voter endpoint unavailable") from exc
        sealed = self.codec.seal(target, rpc, payload)
        with socket.create_connection(
            (endpoint.host, endpoint.port), timeout=self.timeout_seconds
        ) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
            length = _HEADER.unpack(self._recv_exact(connection, _HEADER.size))[0]
            if length < 1 or length > MAX_WIRE_BYTES:
                raise OSError("secure control-plane response exceeded its frame bound")
            response_wire = self._recv_exact(connection, length)
        opened = self.codec.open(
            response_wire,
            expected_sender=target,
            expected_rpc=f"{rpc}.response",
        )
        body = opened.payload
        if body.get("correlation_id") != sealed.request_id:
            raise TransportSecurityError("secure RPC response correlation mismatch")
        ok = body.get("ok")
        if ok is not True:
            raise ControlPlaneError("authenticated voter rejected the RPC")
        response = body.get("response")
        if not isinstance(response, dict):
            raise TransportSecurityError("secure RPC response body is malformed")
        return response

    def request_vote(
        self,
        target: str,
        *,
        candidate_id: str,
        term: int,
        last_log_index: int,
        last_log_term: int,
        cluster_id: str,
    ) -> VoteResponse:
        response = self._rpc(
            target,
            "request_vote",
            {
                "candidate_id": candidate_id,
                "term": term,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
                "cluster_id": cluster_id,
            },
        )
        return VoteResponse(
            term=_uint(response.get("term"), "term"),
            granted=_bool(response.get("granted"), "granted"),
            voter_id=_text(response.get("voter_id"), "voter_id"),
        )

    def append_entries(
        self,
        target: str,
        *,
        leader_id: str,
        leader_term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: tuple[LogEntry, ...],
        leader_commit: int,
        cluster_id: str,
    ) -> AppendResponse:
        response = self._rpc(
            target,
            "append_entries",
            {
                "leader_id": leader_id,
                "leader_term": leader_term,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [entry.to_dict() for entry in entries],
                "leader_commit": leader_commit,
                "cluster_id": cluster_id,
            },
        )
        return AppendResponse(
            term=_uint(response.get("term"), "term"),
            success=_bool(response.get("success"), "success"),
            match_index=_uint(response.get("match_index", 0), "match_index"),
            conflict_index=_uint(response.get("conflict_index", 0), "conflict_index"),
        )

    def install_snapshot(
        self,
        target: str,
        *,
        leader_id: str,
        leader_term: int,
        snapshot: Snapshot,
        cluster_id: str,
    ) -> SnapshotResponse:
        response = self._rpc(
            target,
            "install_snapshot",
            {
                "leader_id": leader_id,
                "leader_term": leader_term,
                "snapshot": snapshot.to_dict(),
                "cluster_id": cluster_id,
            },
        )
        return SnapshotResponse(
            term=_uint(response.get("term"), "term"),
            success=_bool(response.get("success"), "success"),
            match_index=_uint(response.get("match_index", 0), "match_index"),
        )


class _ThreadingRPCServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class SecureReplicationServer:
    """Bounded threaded endpoint that authenticates before calling ``ReplicaNode``."""

    def __init__(
        self,
        node: ReplicaNode,
        codec: SecureEnvelopeCodec,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        if node.voter_id != codec.voter_id:
            raise TransportSecurityError("server identity does not match replica voter ID")
        self.node = node
        self.codec = codec
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                connection = self.request
                try:
                    header = owner._recv_exact(connection, _HEADER.size)
                    length = _HEADER.unpack(header)[0]
                    if length < 1 or length > MAX_WIRE_BYTES:
                        return
                    wire = owner._recv_exact(connection, length)
                    opened = owner.codec.open(wire)
                except (OSError, TransportSecurityError, ReplayRejected):
                    return
                try:
                    response = owner._dispatch(opened)
                    body: dict[str, Any] = {
                        "ok": True,
                        "correlation_id": opened.request_id,
                        "response": response,
                    }
                except (ControlPlaneError, sqlite3.Error, ValueError, TypeError):
                    # Return only a stable public-safe rejection marker; detailed
                    # internal exception text may contain local state.
                    body = {
                        "ok": False,
                        "correlation_id": opened.request_id,
                        "error": "rpc-rejected",
                    }
                sealed = owner.codec.seal(
                    opened.sender_id, f"{opened.rpc}.response", body
                )
                try:
                    connection.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
                except OSError:
                    return

        self._server = _ThreadingRPCServer((host, port), Handler)
        self._thread: threading.Thread | None = None

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise OSError("secure control-plane peer closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def _dispatch(self, opened: OpenedEnvelope) -> dict[str, Any]:
        payload = opened.payload
        if opened.rpc not in _ALLOWED_REQUEST_RPCS:
            raise TransportSecurityError("unsupported authenticated RPC")
        if payload.get("cluster_id") != self.node.configuration.cluster_id:
            raise TransportSecurityError("RPC cluster identity mismatch")
        if opened.rpc == "request_vote":
            if payload.get("candidate_id") != opened.sender_id:
                raise TransportSecurityError("candidate identity is not the authenticated sender")
            response = self.node.receive_vote_request(
                candidate_id=_text(payload.get("candidate_id"), "candidate_id"),
                term=_uint(payload.get("term"), "term"),
                last_log_index=_uint(payload.get("last_log_index"), "last_log_index"),
                last_log_term=_uint(payload.get("last_log_term"), "last_log_term"),
                cluster_id=_text(payload.get("cluster_id"), "cluster_id"),
            )
            return {
                "term": response.term,
                "granted": response.granted,
                "voter_id": response.voter_id,
            }
        if payload.get("leader_id") != opened.sender_id:
            raise TransportSecurityError("leader identity is not the authenticated sender")
        if opened.rpc == "append_entries":
            raw_entries = payload.get("entries")
            if not isinstance(raw_entries, list):
                raise TransportSecurityError("AppendEntries entries must be an array")
            entries = tuple(LogEntry.from_dict(item) for item in raw_entries)
            response = self.node.receive_append_entries(
                leader_id=_text(payload.get("leader_id"), "leader_id"),
                leader_term=_uint(payload.get("leader_term"), "leader_term"),
                prev_log_index=_uint(payload.get("prev_log_index"), "prev_log_index"),
                prev_log_term=_uint(payload.get("prev_log_term"), "prev_log_term"),
                entries=entries,
                leader_commit=_uint(payload.get("leader_commit"), "leader_commit"),
                cluster_id=_text(payload.get("cluster_id"), "cluster_id"),
            )
            return {
                "term": response.term,
                "success": response.success,
                "match_index": response.match_index,
                "conflict_index": response.conflict_index,
            }
        response = self.node.receive_install_snapshot(
            leader_id=_text(payload.get("leader_id"), "leader_id"),
            leader_term=_uint(payload.get("leader_term"), "leader_term"),
            snapshot=Snapshot.from_dict(payload.get("snapshot")),
            cluster_id=_text(payload.get("cluster_id"), "cluster_id"),
        )
        return {
            "term": response.term,
            "success": response.success,
            "match_index": response.match_index,
        }

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"fcp-voter-rpc-{self.node.voter_id[:16]}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def __enter__(self) -> "SecureReplicationServer":
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


__all__ = [
    "PersistentReplayGuard",
    "ReplayRejected",
    "SecureEnvelopeCodec",
    "SecureReplicationServer",
    "SecureSocketReplicationTransport",
    "TransportSecurityError",
    "VoterEndpoint",
    "VoterIdentityRegistry",
]

"""Private quorum replication for Federation human credential continuity.

Human password hashes and the password salt must never enter public Federation
state, discovery, logs or session events.  They nevertheless have to survive a
single eligible voter loss so operational leadership can move without locking
human operators out.

This module stores an encrypted snapshot of the human-auth SQLite database and
password salt on the fixed three-voter control-plane set.  Snapshot payloads are
AES-GCM ciphertext at rest and are transported inside the already authenticated
and encrypted voter envelopes.  A snapshot becomes recoverable only with a
commit certificate containing Ed25519 receipts from a voter quorum.  The
certificate allows a new leader to distinguish a committed snapshot from a
half-written prepare even when only one surviving voter still holds the newest
copy after another voter is lost.

Browser session secrets are intentionally excluded.  Failover therefore
preserves accounts, hashes, active state and roles while existing browser
sessions may be invalidated, matching the Federation v1 acceptance contract.
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
import tempfile
import threading
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from catalog.node.identity import NodeCredentials, verify_signature

from .control_plane_replication import ControlPlaneError, ReplicaNode
from .control_plane_transport import (
    MAX_WIRE_BYTES,
    SecureEnvelopeCodec,
    TransportSecurityError,
    VoterEndpoint,
    VoterIdentityRegistry,
)

CREDENTIAL_SNAPSHOT_SCHEMA = "fcp.control-plane.human-auth.snapshot.v1"
CREDENTIAL_CERTIFICATE_SCHEMA = "fcp.control-plane.human-auth.certificate.v1"
CREDENTIAL_RECEIPT_SCHEMA = "fcp.control-plane.human-auth.receipt.v1"
PRIVATE_PACKAGE_SCHEMA = "fcp.control-plane.human-auth.private-package.v1"
MAX_CREDENTIAL_SNAPSHOT_BYTES = 8 * 1024 * 1024
MAX_COMPRESSED_PACKAGE_BYTES = 6 * 1024 * 1024
MAX_PASSWORD_SALT_BYTES = 4096
CREDENTIAL_PORT_OFFSET = 1
_HEADER = struct.Struct("!I")
_NONCE_BYTES = 12


class CredentialReplicationError(ControlPlaneError):
    """Private credential continuity could not be proven or restored."""


def _canonical(value: object, *, maximum: int = MAX_CREDENTIAL_SNAPSHOT_BYTES) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CredentialReplicationError("credential value is not canonical JSON") from exc
    if len(encoded) > maximum:
        raise CredentialReplicationError("credential value exceeds its size bound")
    return encoded


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: object, field: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise CredentialReplicationError(f"{field} must be base64url text")
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise CredentialReplicationError(f"{field} is malformed") from exc
    if _b64(decoded) != value:
        raise CredentialReplicationError(f"{field} is not canonical base64url")
    return decoded


def _text(value: object, field: str, maximum: int = 4096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or any(ord(ch) < 32 for ch in value)
    ):
        raise CredentialReplicationError(f"{field} is malformed")
    return value


def _uint(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CredentialReplicationError(f"{field} must be a non-negative integer")
    return value


def _private_key(cluster_id: str, transport_secret: bytes) -> bytes:
    if not isinstance(transport_secret, bytes) or len(transport_secret) < 32:
        raise CredentialReplicationError("credential replication secret is too short")
    salt = hashlib.sha256(
        (CREDENTIAL_SNAPSHOT_SCHEMA + "\0" + cluster_id).encode("utf-8")
    ).digest()
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        info=b"MSH FCP Federation v1 private human credential replica",
    ).derive(transport_secret)


def _receipt_body(
    *,
    cluster_id: str,
    voter_id: str,
    snapshot_id: str,
    version: int,
    term: int,
    content_digest: str,
) -> bytes:
    return _canonical(
        {
            "schema": CREDENTIAL_RECEIPT_SCHEMA,
            "cluster_id": cluster_id,
            "voter_id": voter_id,
            "snapshot_id": snapshot_id,
            "version": version,
            "term": term,
            "content_digest": content_digest,
        },
        maximum=16 * 1024,
    )


@dataclass(frozen=True)
class CredentialSnapshot:
    federation_id: str
    session_id: str
    snapshot_id: str
    version: int
    term: int
    leader_id: str
    created_at: str
    content_digest: str
    nonce: str
    ciphertext: str

    def __post_init__(self) -> None:
        for field in (
            "federation_id",
            "session_id",
            "snapshot_id",
            "leader_id",
            "created_at",
            "content_digest",
            "nonce",
            "ciphertext",
        ):
            _text(getattr(self, field), field, MAX_CREDENTIAL_SNAPSHOT_BYTES)
        _uint(self.version, "version")
        _uint(self.term, "term")
        if len(_unb64(self.nonce, "nonce")) != _NONCE_BYTES:
            raise CredentialReplicationError("credential nonce length is invalid")
        _unb64(self.ciphertext, "ciphertext")
        _canonical(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": CREDENTIAL_SNAPSHOT_SCHEMA,
            "federation_id": self.federation_id,
            "session_id": self.session_id,
            "snapshot_id": self.snapshot_id,
            "version": self.version,
            "term": self.term,
            "leader_id": self.leader_id,
            "created_at": self.created_at,
            "content_digest": self.content_digest,
            "nonce": self.nonce,
            "ciphertext": self.ciphertext,
        }

    @classmethod
    def from_dict(cls, value: object) -> "CredentialSnapshot":
        if not isinstance(value, dict) or value.get("schema") != CREDENTIAL_SNAPSHOT_SCHEMA:
            raise CredentialReplicationError("unsupported credential snapshot schema")
        allowed = {
            "schema",
            "federation_id",
            "session_id",
            "snapshot_id",
            "version",
            "term",
            "leader_id",
            "created_at",
            "content_digest",
            "nonce",
            "ciphertext",
        }
        if set(value) != allowed:
            raise CredentialReplicationError("credential snapshot has unexpected fields")
        return cls(
            federation_id=_text(value.get("federation_id"), "federation_id"),
            session_id=_text(value.get("session_id"), "session_id"),
            snapshot_id=_text(value.get("snapshot_id"), "snapshot_id"),
            version=_uint(value.get("version"), "version"),
            term=_uint(value.get("term"), "term"),
            leader_id=_text(value.get("leader_id"), "leader_id"),
            created_at=_text(value.get("created_at"), "created_at"),
            content_digest=_text(value.get("content_digest"), "content_digest"),
            nonce=_text(value.get("nonce"), "nonce"),
            ciphertext=_text(value.get("ciphertext"), "ciphertext", MAX_CREDENTIAL_SNAPSHOT_BYTES),
        )


@dataclass(frozen=True)
class CredentialReceipt:
    voter_id: str
    snapshot_id: str
    version: int
    term: int
    content_digest: str
    signature: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": CREDENTIAL_RECEIPT_SCHEMA,
            "voter_id": self.voter_id,
            "snapshot_id": self.snapshot_id,
            "version": self.version,
            "term": self.term,
            "content_digest": self.content_digest,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, value: object) -> "CredentialReceipt":
        if not isinstance(value, dict) or value.get("schema") != CREDENTIAL_RECEIPT_SCHEMA:
            raise CredentialReplicationError("unsupported credential receipt schema")
        return cls(
            voter_id=_text(value.get("voter_id"), "voter_id"),
            snapshot_id=_text(value.get("snapshot_id"), "snapshot_id"),
            version=_uint(value.get("version"), "version"),
            term=_uint(value.get("term"), "term"),
            content_digest=_text(value.get("content_digest"), "content_digest"),
            signature=_text(value.get("signature"), "signature"),
        )


@dataclass(frozen=True)
class CredentialCommitCertificate:
    cluster_id: str
    snapshot_id: str
    version: int
    term: int
    content_digest: str
    receipts: tuple[CredentialReceipt, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": CREDENTIAL_CERTIFICATE_SCHEMA,
            "cluster_id": self.cluster_id,
            "snapshot_id": self.snapshot_id,
            "version": self.version,
            "term": self.term,
            "content_digest": self.content_digest,
            "receipts": [receipt.to_dict() for receipt in self.receipts],
        }

    @classmethod
    def from_dict(cls, value: object) -> "CredentialCommitCertificate":
        if not isinstance(value, dict) or value.get("schema") != CREDENTIAL_CERTIFICATE_SCHEMA:
            raise CredentialReplicationError("unsupported credential certificate schema")
        receipts = value.get("receipts")
        if not isinstance(receipts, list):
            raise CredentialReplicationError("credential certificate receipts must be an array")
        return cls(
            cluster_id=_text(value.get("cluster_id"), "cluster_id"),
            snapshot_id=_text(value.get("snapshot_id"), "snapshot_id"),
            version=_uint(value.get("version"), "version"),
            term=_uint(value.get("term"), "term"),
            content_digest=_text(value.get("content_digest"), "content_digest"),
            receipts=tuple(CredentialReceipt.from_dict(item) for item in receipts),
        )

    def verify(self, registry: VoterIdentityRegistry) -> None:
        if self.cluster_id != registry.configuration.cluster_id:
            raise CredentialReplicationError("credential certificate cluster mismatch")
        voters: set[str] = set()
        for receipt in self.receipts:
            if (
                receipt.snapshot_id != self.snapshot_id
                or receipt.version != self.version
                or receipt.term != self.term
                or receipt.content_digest != self.content_digest
                or receipt.voter_id in voters
            ):
                raise CredentialReplicationError("credential certificate receipt mismatch")
            body = _receipt_body(
                cluster_id=self.cluster_id,
                voter_id=receipt.voter_id,
                snapshot_id=self.snapshot_id,
                version=self.version,
                term=self.term,
                content_digest=self.content_digest,
            )
            try:
                valid = verify_signature(
                    registry.public_key(receipt.voter_id), body, receipt.signature
                )
            except Exception as exc:
                raise CredentialReplicationError("credential certificate signature malformed") from exc
            if not valid:
                raise CredentialReplicationError("credential certificate signature invalid")
            voters.add(receipt.voter_id)
        if len(voters) < registry.configuration.quorum:
            raise CredentialReplicationError("credential snapshot lacks a voter quorum certificate")


class CredentialSnapshotStore:
    """Durable encrypted credential snapshots; no plaintext secret is stored."""

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS credential_snapshots(
                    snapshot_id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    term INTEGER NOT NULL,
                    content_digest TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('prepared','committed')),
                    certificate_json TEXT,
                    UNIQUE(version, content_digest)
                );
                CREATE INDEX IF NOT EXISTS credential_committed_version
                ON credential_snapshots(status,version DESC);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def next_version(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(version),0) FROM credential_snapshots"
            ).fetchone()
        return int(row[0]) + 1

    def prepare(self, snapshot: CredentialSnapshot) -> None:
        encoded = _canonical(snapshot.to_dict()).decode("utf-8")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM credential_snapshots WHERE snapshot_id=?",
                (snapshot.snapshot_id,),
            ).fetchone()
            if existing is not None:
                if existing["snapshot_json"] != encoded:
                    raise CredentialReplicationError("credential snapshot ID conflict")
                return
            committed = connection.execute(
                "SELECT MAX(version) FROM credential_snapshots WHERE status='committed'"
            ).fetchone()[0]
            if committed is not None and snapshot.version < int(committed):
                raise CredentialReplicationError("credential snapshot version is stale")
            connection.execute(
                """
                INSERT INTO credential_snapshots(
                    snapshot_id,version,term,content_digest,snapshot_json,status
                ) VALUES(?,?,?,?,?,'prepared')
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.version,
                    snapshot.term,
                    snapshot.content_digest,
                    encoded,
                ),
            )
            connection.commit()

    def commit(
        self,
        snapshot: CredentialSnapshot,
        certificate: CredentialCommitCertificate,
        registry: VoterIdentityRegistry,
    ) -> None:
        certificate.verify(registry)
        if (
            certificate.snapshot_id != snapshot.snapshot_id
            or certificate.version != snapshot.version
            or certificate.term != snapshot.term
            or certificate.content_digest != snapshot.content_digest
        ):
            raise CredentialReplicationError("credential commit certificate targets another snapshot")
        self.prepare(snapshot)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE credential_snapshots
                SET status='committed',certificate_json=?
                WHERE snapshot_id=?
                """,
                (_canonical(certificate.to_dict()).decode("utf-8"), snapshot.snapshot_id),
            )
            connection.commit()

    def latest_committed(
        self,
    ) -> tuple[CredentialSnapshot, CredentialCommitCertificate] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM credential_snapshots
                WHERE status='committed' AND certificate_json IS NOT NULL
                ORDER BY version DESC LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return (
            CredentialSnapshot.from_dict(json.loads(row["snapshot_json"])),
            CredentialCommitCertificate.from_dict(json.loads(row["certificate_json"])),
        )

    def prepared(self, snapshot_id: str) -> CredentialSnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT snapshot_json FROM credential_snapshots WHERE snapshot_id=?",
                (snapshot_id,),
            ).fetchone()
        return None if row is None else CredentialSnapshot.from_dict(json.loads(row[0]))


def _snapshot_receipt(
    credentials: NodeCredentials,
    registry: VoterIdentityRegistry,
    snapshot: CredentialSnapshot,
) -> CredentialReceipt:
    voter_id = credentials.identity.node_id
    body = _receipt_body(
        cluster_id=registry.configuration.cluster_id,
        voter_id=voter_id,
        snapshot_id=snapshot.snapshot_id,
        version=snapshot.version,
        term=snapshot.term,
        content_digest=snapshot.content_digest,
    )
    return CredentialReceipt(
        voter_id=voter_id,
        snapshot_id=snapshot.snapshot_id,
        version=snapshot.version,
        term=snapshot.term,
        content_digest=snapshot.content_digest,
        signature=credentials.sign(body),
    )


def _sqlite_backup_bytes(database_path: Path) -> bytes:
    if not database_path.exists():
        raise CredentialReplicationError("human credential database is absent")
    fd, temporary = tempfile.mkstemp(prefix="fcp-auth-backup-", suffix=".sqlite3")
    os.close(fd)
    destination_path = Path(temporary)
    try:
        source = sqlite3.connect(database_path)
        destination = sqlite3.connect(destination_path)
        try:
            source.backup(destination)
            destination.execute("PRAGMA integrity_check").fetchone()
        finally:
            destination.close()
            source.close()
        value = destination_path.read_bytes()
    finally:
        destination_path.unlink(missing_ok=True)
    if not value or len(value) > MAX_CREDENTIAL_SNAPSHOT_BYTES:
        raise CredentialReplicationError("human credential database exceeds replication bound")
    return value


def build_credential_snapshot(
    *,
    database_path: Path,
    password_salt_path: Path,
    federation_id: str,
    session_id: str,
    version: int,
    term: int,
    leader_id: str,
    cluster_id: str,
    transport_secret: bytes,
    created_at: datetime | None = None,
) -> CredentialSnapshot:
    salt = password_salt_path.read_text(encoding="utf-8").strip()
    if not salt or len(salt.encode("utf-8")) > MAX_PASSWORD_SALT_BYTES:
        raise CredentialReplicationError("human password salt is absent or malformed")
    package = _canonical(
        {
            "schema": PRIVATE_PACKAGE_SCHEMA,
            "database": _b64(_sqlite_backup_bytes(database_path)),
            "password_salt": salt,
        },
        maximum=MAX_CREDENTIAL_SNAPSHOT_BYTES,
    )
    compressed = zlib.compress(package, level=9)
    if len(compressed) > MAX_COMPRESSED_PACKAGE_BYTES:
        raise CredentialReplicationError("compressed human credential state exceeds replication bound")
    digest = "sha256:" + hashlib.sha256(compressed).hexdigest()
    nonce = os.urandom(_NONCE_BYTES)
    aad = _canonical(
        {
            "schema": CREDENTIAL_SNAPSHOT_SCHEMA,
            "federation_id": federation_id,
            "session_id": session_id,
            "version": version,
            "term": term,
            "leader_id": leader_id,
            "content_digest": digest,
        },
        maximum=16 * 1024,
    )
    ciphertext = AESGCM(_private_key(cluster_id, transport_secret)).encrypt(
        nonce, compressed, aad
    )
    snapshot_id = "credential-" + hashlib.sha256(
        aad + nonce + ciphertext
    ).hexdigest()
    stamp = (created_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    return CredentialSnapshot(
        federation_id=federation_id,
        session_id=session_id,
        snapshot_id=snapshot_id,
        version=version,
        term=term,
        leader_id=leader_id,
        created_at=stamp,
        content_digest=digest,
        nonce=_b64(nonce),
        ciphertext=_b64(ciphertext),
    )


def restore_credential_snapshot(
    snapshot: CredentialSnapshot,
    *,
    cluster_id: str,
    transport_secret: bytes,
    database_path: Path,
    password_salt_path: Path,
) -> None:
    nonce = _unb64(snapshot.nonce, "nonce")
    ciphertext = _unb64(snapshot.ciphertext, "ciphertext")
    aad = _canonical(
        {
            "schema": CREDENTIAL_SNAPSHOT_SCHEMA,
            "federation_id": snapshot.federation_id,
            "session_id": snapshot.session_id,
            "version": snapshot.version,
            "term": snapshot.term,
            "leader_id": snapshot.leader_id,
            "content_digest": snapshot.content_digest,
        },
        maximum=16 * 1024,
    )
    try:
        compressed = AESGCM(_private_key(cluster_id, transport_secret)).decrypt(
            nonce, ciphertext, aad
        )
    except InvalidTag as exc:
        raise CredentialReplicationError("credential snapshot authenticated decryption failed") from exc
    if "sha256:" + hashlib.sha256(compressed).hexdigest() != snapshot.content_digest:
        raise CredentialReplicationError("credential snapshot plaintext digest mismatch")
    try:
        package = json.loads(zlib.decompress(compressed))
    except (zlib.error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CredentialReplicationError("credential snapshot private package is malformed") from exc
    if not isinstance(package, dict) or package.get("schema") != PRIVATE_PACKAGE_SCHEMA:
        raise CredentialReplicationError("credential snapshot private package schema mismatch")
    database_bytes = _unb64(package.get("database"), "database")
    salt = _text(package.get("password_salt"), "password_salt", MAX_PASSWORD_SALT_BYTES)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    password_salt_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = database_path.with_name(database_path.name + ".c03-restore.tmp")
    temporary.write_bytes(database_bytes)
    check = sqlite3.connect(temporary)
    try:
        result = check.execute("PRAGMA integrity_check").fetchone()
        if result is None or str(result[0]).lower() != "ok":
            raise CredentialReplicationError("restored human credential database failed integrity check")
    finally:
        check.close()
    os.replace(temporary, database_path)
    salt_temp = password_salt_path.with_name(password_salt_path.name + ".c03-restore.tmp")
    salt_temp.write_text(salt + "\n", encoding="utf-8")
    try:
        salt_temp.chmod(0o600)
    except OSError:
        pass
    os.replace(salt_temp, password_salt_path)


class _CredentialRPCServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class CredentialReplicaServer:
    """Authenticated private-snapshot endpoint bound to the live consensus node."""

    def __init__(
        self,
        node: ReplicaNode,
        codec: SecureEnvelopeCodec,
        store: CredentialSnapshotStore,
        *,
        host: str,
        port: int,
    ) -> None:
        if node.voter_id != codec.voter_id:
            raise CredentialReplicationError("credential server identity does not match voter")
        self.node = node
        self.codec = codec
        self.store = store
        owner = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                try:
                    header = owner._recv_exact(self.request, _HEADER.size)
                    length = _HEADER.unpack(header)[0]
                    if length < 1 or length > MAX_WIRE_BYTES:
                        return
                    opened = owner.codec.open(owner._recv_exact(self.request, length))
                    response = owner._dispatch(opened.sender_id, opened.rpc, opened.payload)
                    body = {
                        "ok": True,
                        "correlation_id": opened.request_id,
                        "response": response,
                    }
                except (OSError, ControlPlaneError, sqlite3.Error, ValueError, TypeError):
                    return
                sealed = owner.codec.seal(
                    opened.sender_id, f"{opened.rpc}.response", body
                )
                try:
                    self.request.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
                except OSError:
                    return

        self._server = _CredentialRPCServer((host, port), Handler)
        self._thread: threading.Thread | None = None

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        chunks: list[bytes] = []
        left = size
        while left:
            chunk = connection.recv(left)
            if not chunk:
                raise OSError("credential peer closed the connection")
            chunks.append(chunk)
            left -= len(chunk)
        return b"".join(chunks)

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def _require_current_leader(self, sender_id: str, term: int) -> None:
        if (
            self.node.leader_id != sender_id
            or self.node.store.current_term != term
            or sender_id not in self.node.configuration.voter_ids
        ):
            raise CredentialReplicationError("credential mutation is not from the current consensus leader")

    def _dispatch(self, sender_id: str, rpc: str, payload: dict[str, Any]) -> dict[str, Any]:
        if rpc == "human_auth.prepare":
            snapshot = CredentialSnapshot.from_dict(payload.get("snapshot"))
            self._require_current_leader(sender_id, snapshot.term)
            if snapshot.leader_id != sender_id:
                raise CredentialReplicationError("credential snapshot leader identity mismatch")
            self.store.prepare(snapshot)
            receipt = _snapshot_receipt(self.codec.credentials, self.codec.registry, snapshot)
            return {"receipt": receipt.to_dict()}
        if rpc == "human_auth.commit":
            snapshot = CredentialSnapshot.from_dict(payload.get("snapshot"))
            certificate = CredentialCommitCertificate.from_dict(payload.get("certificate"))
            self._require_current_leader(sender_id, snapshot.term)
            certificate.verify(self.codec.registry)
            self.store.commit(snapshot, certificate, self.codec.registry)
            return {"committed": True, "version": snapshot.version}
        if rpc == "human_auth.status":
            # Authenticated voters may read only metadata/certificate; no plaintext
            # or ciphertext is returned unless explicitly fetched.
            latest = self.store.latest_committed()
            if latest is None:
                return {"available": False}
            snapshot, certificate = latest
            return {
                "available": True,
                "version": snapshot.version,
                "snapshot_id": snapshot.snapshot_id,
                "term": snapshot.term,
                "content_digest": snapshot.content_digest,
                "certificate": certificate.to_dict(),
            }
        if rpc == "human_auth.fetch":
            latest = self.store.latest_committed()
            if latest is None:
                return {"available": False}
            snapshot, certificate = latest
            return {
                "available": True,
                "snapshot": snapshot.to_dict(),
                "certificate": certificate.to_dict(),
            }
        raise CredentialReplicationError("unsupported credential replication RPC")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"fcp-human-auth-replica-{self.node.voter_id[:16]}",
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


class CredentialReplicaTransport:
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
        chunks: list[bytes] = []
        left = size
        while left:
            chunk = connection.recv(left)
            if not chunk:
                raise OSError("credential peer closed the connection")
            chunks.append(chunk)
            left -= len(chunk)
        return b"".join(chunks)

    def rpc(self, target: str, rpc: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = self.endpoints[target]
        sealed = self.codec.seal(target, rpc, payload)
        with socket.create_connection(
            (endpoint.host, endpoint.port), timeout=self.timeout_seconds
        ) as connection:
            connection.settimeout(self.timeout_seconds)
            connection.sendall(_HEADER.pack(len(sealed.wire)) + sealed.wire)
            length = _HEADER.unpack(self._recv_exact(connection, _HEADER.size))[0]
            if length < 1 or length > MAX_WIRE_BYTES:
                raise OSError("credential response frame exceeded its bound")
            opened = self.codec.open(
                self._recv_exact(connection, length),
                expected_sender=target,
                expected_rpc=f"{rpc}.response",
            )
        if opened.payload.get("correlation_id") != sealed.request_id:
            raise CredentialReplicationError("credential response correlation mismatch")
        if opened.payload.get("ok") is not True:
            raise CredentialReplicationError("credential voter rejected request")
        response = opened.payload.get("response")
        if not isinstance(response, dict):
            raise CredentialReplicationError("credential response is malformed")
        return response


class CredentialQuorumManager:
    """Publish and restore quorum-certified encrypted human credential snapshots."""

    def __init__(
        self,
        node: ReplicaNode,
        codec: SecureEnvelopeCodec,
        store: CredentialSnapshotStore,
        transport: CredentialReplicaTransport,
        transport_secret: bytes,
    ) -> None:
        self.node = node
        self.codec = codec
        self.store = store
        self.transport = transport
        self.transport_secret = transport_secret

    def _require_leader(self) -> None:
        if self.node.role != ReplicaNode.LEADER or self.node.leader_id != self.node.voter_id:
            raise CredentialReplicationError("credential publication requires current consensus leader")

    def publish(
        self,
        *,
        database_path: Path,
        password_salt_path: Path,
        federation_id: str,
        session_id: str,
    ) -> CredentialSnapshot:
        self._require_leader()
        snapshot = build_credential_snapshot(
            database_path=database_path,
            password_salt_path=password_salt_path,
            federation_id=federation_id,
            session_id=session_id,
            version=self.store.next_version(),
            term=self.node.store.current_term,
            leader_id=self.node.voter_id,
            cluster_id=self.node.configuration.cluster_id,
            transport_secret=self.transport_secret,
        )
        self.store.prepare(snapshot)
        receipts = [
            _snapshot_receipt(self.codec.credentials, self.codec.registry, snapshot)
        ]
        prepared_peers: list[str] = []
        for target in self.node.peers:
            try:
                response = self.transport.rpc(
                    target, "human_auth.prepare", {"snapshot": snapshot.to_dict()}
                )
                receipt = CredentialReceipt.from_dict(response.get("receipt"))
                if receipt.voter_id != target:
                    continue
                receipts.append(receipt)
                prepared_peers.append(target)
            except (OSError, ControlPlaneError, TimeoutError):
                continue
        certificate = CredentialCommitCertificate(
            cluster_id=self.node.configuration.cluster_id,
            snapshot_id=snapshot.snapshot_id,
            version=snapshot.version,
            term=snapshot.term,
            content_digest=snapshot.content_digest,
            receipts=tuple(receipts),
        )
        certificate.verify(self.codec.registry)
        remote_commits = 0
        for target in prepared_peers:
            try:
                response = self.transport.rpc(
                    target,
                    "human_auth.commit",
                    {
                        "snapshot": snapshot.to_dict(),
                        "certificate": certificate.to_dict(),
                    },
                )
                if response.get("committed") is True:
                    remote_commits += 1
            except (OSError, ControlPlaneError, TimeoutError):
                continue
        if remote_commits + 1 < self.node.quorum:
            raise CredentialReplicationError(
                "credential snapshot could not be committed on a live voter quorum"
            )
        self.store.commit(snapshot, certificate, self.codec.registry)
        return snapshot

    def best_committed(
        self,
    ) -> tuple[CredentialSnapshot, CredentialCommitCertificate] | None:
        candidates: list[
            tuple[CredentialSnapshot, CredentialCommitCertificate]
        ] = []
        local = self.store.latest_committed()
        if local is not None:
            local[1].verify(self.codec.registry)
            candidates.append(local)
        for target in self.node.peers:
            try:
                status = self.transport.rpc(target, "human_auth.status", {})
            except (OSError, ControlPlaneError, TimeoutError):
                continue
            if status.get("available") is not True:
                continue
            try:
                certificate = CredentialCommitCertificate.from_dict(
                    status.get("certificate")
                )
                certificate.verify(self.codec.registry)
            except ControlPlaneError:
                continue
            current_version = max(
                (item[0].version for item in candidates), default=-1
            )
            if certificate.version <= current_version:
                continue
            try:
                fetched = self.transport.rpc(target, "human_auth.fetch", {})
                snapshot = CredentialSnapshot.from_dict(fetched.get("snapshot"))
                fetched_certificate = CredentialCommitCertificate.from_dict(
                    fetched.get("certificate")
                )
                fetched_certificate.verify(self.codec.registry)
                if (
                    fetched_certificate.snapshot_id != snapshot.snapshot_id
                    or fetched_certificate.content_digest != snapshot.content_digest
                ):
                    continue
                candidates.append((snapshot, fetched_certificate))
            except (OSError, ControlPlaneError, TimeoutError):
                continue
        if not candidates:
            return None
        return max(candidates, key=lambda item: item[0].version)

    def restore_best(
        self,
        *,
        database_path: Path,
        password_salt_path: Path,
        expected_federation_id: str,
        expected_session_id: str,
    ) -> CredentialSnapshot:
        self._require_leader()
        best = self.best_committed()
        if best is None:
            raise CredentialReplicationError("no quorum-certified human credential snapshot exists")
        snapshot, certificate = best
        certificate.verify(self.codec.registry)
        if (
            snapshot.federation_id != expected_federation_id
            or snapshot.session_id != expected_session_id
        ):
            raise CredentialReplicationError("credential snapshot belongs to another Federation")
        self.store.commit(snapshot, certificate, self.codec.registry)
        restore_credential_snapshot(
            snapshot,
            cluster_id=self.node.configuration.cluster_id,
            transport_secret=self.transport_secret,
            database_path=database_path,
            password_salt_path=password_salt_path,
        )
        return snapshot


def credential_endpoints_from_control(
    peers: Mapping[str, VoterEndpoint],
) -> dict[str, VoterEndpoint]:
    endpoints: dict[str, VoterEndpoint] = {}
    for voter_id, endpoint in peers.items():
        port = endpoint.port + CREDENTIAL_PORT_OFFSET
        if port > 65535:
            raise CredentialReplicationError("control-plane port leaves no credential replica port")
        endpoints[voter_id] = VoterEndpoint(endpoint.host, port)
    return endpoints


__all__ = [
    "CredentialCommitCertificate",
    "CredentialQuorumManager",
    "CredentialReplicaServer",
    "CredentialReplicaTransport",
    "CredentialReplicationError",
    "CredentialSnapshot",
    "CredentialSnapshotStore",
    "build_credential_snapshot",
    "credential_endpoints_from_control",
    "restore_credential_snapshot",
]

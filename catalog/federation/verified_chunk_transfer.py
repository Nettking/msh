"""Encrypted, acknowledged object transfer runtime for Phase F6.4.2."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import secrets
import tempfile
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .adaptive_transport import DirectTransportUnavailable
from .direct_peer import (
    DIRECT_PEER_PROTOCOL_VERSION,
    DirectPacketChannel,
    DirectPeerDescriptor,
    NodeSigningCredentials,
    PeerStreamOpen,
)
from .errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from .object_transfer import (
    DEFAULT_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_CHUNK_BYTES,
    MAX_TRANSFER_OBJECT_BYTES,
    ObjectTransferChunk,
    ObjectTransferManifest,
    ObjectTransferReceiver,
    VerifiedObjectTransfer,
)
from .object_transfer_staging import FilesystemObjectTransferChunkStore
from .peer_stream import PeerStreamFrame, peer_stream_nonce, validate_peer_public_text

DIRECT_OBJECT_TRANSFER_PACKET_SCHEMA = "msh.direct_object_transfer.packet.v1"
DIRECT_OBJECT_TRANSFER_MESSAGE_SCHEMA = "msh.direct_object_transfer.message.v1"
DIRECT_OBJECT_TRANSFER_ACK_SCHEMA = "msh.direct_object_transfer.ack.v1"
DIRECT_OBJECT_TRANSFER_PROTOCOL_VERSION = "1.0"
DIRECT_OBJECT_TRANSFER_CIPHER_CONTEXT = "msh-verified-object-transfer-v1"
MAX_DIRECT_OBJECT_TRANSFER_PACKET_BYTES = 131_072
DEFAULT_TRANSFER_ATTEMPTS = 4
DEFAULT_TRANSFER_SEQUENCE_GAP = 64
DEFAULT_INBOUND_TRANSFERS = 128
_ACTIONS = frozenset({"manifest", "chunk", "complete", "abort"})
JSON = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _bounded(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > 512
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-object-transfer-runtime-field",
            field,
            "must be non-empty bounded text without control characters",
        )
    return value


def _integer(
    value: Any,
    field: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FederationValidationError(
            "invalid-object-transfer-runtime-integer",
            field,
            f"must be an integer greater than or equal to {minimum}",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "object-transfer-runtime-limit-exceeded",
            field,
            f"must not exceed {maximum}",
        )
    return value


def _protocol(value: Any) -> str:
    value = validate_peer_public_text(value, "protocol_version")
    try:
        major = int(value.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-object-transfer-runtime-protocol",
            "protocol_version",
            "must begin with a numeric major version",
        ) from exc
    if major != 1:
        raise ProtocolCompatibilityError(
            "unsupported-object-transfer-runtime-protocol",
            "protocol_version",
            "supported major is 1",
        )
    return value


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: Any, field: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "invalid-object-transfer-key",
            field,
            "must be canonical base64url text",
        )
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-object-transfer-key",
            field,
            "must be canonical base64url text",
        ) from exc
    if len(decoded) != 32 or _b64encode(decoded) != value:
        raise FederationValidationError(
            "invalid-object-transfer-key",
            field,
            "must encode exactly 32 bytes",
        )
    return decoded


def _public_key_text(public_key: X25519PublicKey) -> str:
    raw = public_key.public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return f"x25519:{_b64encode(raw)}"


def _public_key(value: Any, field: str) -> X25519PublicKey:
    if not isinstance(value, str) or not value.startswith("x25519:"):
        raise FederationValidationError(
            "unsupported-object-transfer-key",
            field,
            "expected an X25519 public key",
        )
    return X25519PublicKey.from_public_bytes(
        _b64decode(value.removeprefix("x25519:"), field)
    )


@dataclass(frozen=True)
class ObjectTransferAck:
    transfer_id: str
    object_id: str
    action: str
    accepted: bool
    retryable: bool
    duplicate: bool = False
    chunk_index: int | None = None
    accepted_chunks: int = 0
    remaining_chunks: int = 0
    next_missing_index: int | None = None
    error_code: str | None = None
    receipt: JSON | None = None
    protocol_version: str = DIRECT_OBJECT_TRANSFER_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "transfer_id", _bounded(self.transfer_id, "transfer_id"))
        object.__setattr__(self, "object_id", _bounded(self.object_id, "object_id"))
        if self.action not in _ACTIONS:
            raise FederationValidationError(
                "invalid-object-transfer-action",
                "action",
                "unsupported transfer action",
            )
        if any(
            not isinstance(value, bool)
            for value in (self.accepted, self.retryable, self.duplicate)
        ):
            raise FederationValidationError(
                "invalid-object-transfer-ack-boolean",
                "ack",
                "flags must be booleans",
            )
        if self.chunk_index is not None:
            object.__setattr__(
                self,
                "chunk_index",
                _integer(self.chunk_index, "chunk_index"),
            )
        object.__setattr__(
            self,
            "accepted_chunks",
            _integer(self.accepted_chunks, "accepted_chunks"),
        )
        object.__setattr__(
            self,
            "remaining_chunks",
            _integer(self.remaining_chunks, "remaining_chunks"),
        )
        if self.next_missing_index is not None:
            object.__setattr__(
                self,
                "next_missing_index",
                _integer(self.next_missing_index, "next_missing_index"),
            )
        if self.error_code is not None:
            object.__setattr__(self, "error_code", _bounded(self.error_code, "error_code"))
        if self.accepted and (self.retryable or self.error_code is not None):
            raise FederationValidationError(
                "invalid-object-transfer-ack",
                "accepted",
                "accepted ack cannot request retry",
            )
        if not self.accepted and self.error_code is None:
            raise FederationValidationError(
                "invalid-object-transfer-ack",
                "error_code",
                "rejected ack requires an error",
            )
        if self.receipt is not None and (
            not isinstance(self.receipt, dict)
            or self.receipt.get("schema") != "msh.object_transfer.receipt.v1"
            or self.receipt.get("verified") is not True
            or not self.accepted
            or self.action != "complete"
        ):
            raise FederationValidationError(
                "invalid-object-transfer-receipt",
                "receipt",
                "unexpected receipt",
            )
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))

    @classmethod
    def from_dict(cls, value: Any) -> ObjectTransferAck:
        if (
            not isinstance(value, dict)
            or value.get("schema") != DIRECT_OBJECT_TRANSFER_ACK_SCHEMA
        ):
            raise ProtocolCompatibilityError(
                "unsupported-object-transfer-ack",
                "schema",
                "unexpected ack schema",
            )
        fields = {
            "protocol_version",
            "transfer_id",
            "object_id",
            "action",
            "accepted",
            "retryable",
            "duplicate",
            "chunk_index",
            "accepted_chunks",
            "remaining_chunks",
            "next_missing_index",
            "error_code",
            "receipt",
        }
        missing = sorted(fields.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{field: value[field] for field in fields})

    def to_dict(self) -> JSON:
        return {
            "schema": DIRECT_OBJECT_TRANSFER_ACK_SCHEMA,
            "protocol_version": self.protocol_version,
            "transfer_id": self.transfer_id,
            "object_id": self.object_id,
            "action": self.action,
            "accepted": self.accepted,
            "retryable": self.retryable,
            "duplicate": self.duplicate,
            "chunk_index": self.chunk_index,
            "accepted_chunks": self.accepted_chunks,
            "remaining_chunks": self.remaining_chunks,
            "next_missing_index": self.next_missing_index,
            "error_code": self.error_code,
            "receipt": self.receipt,
        }


@dataclass(frozen=True)
class _CipherContext:
    opening: PeerStreamOpen
    direction: str

    @property
    def source(self) -> str:
        if self.direction == "request":
            return self.opening.source_node_id
        return self.opening.target_node_id

    @property
    def target(self) -> str:
        if self.direction == "request":
            return self.opening.target_node_id
        return self.opening.source_node_id

    def value(self) -> JSON:
        return {
            "protocol": DIRECT_OBJECT_TRANSFER_CIPHER_CONTEXT,
            "session_id": self.opening.session_id,
            "stream_id": self.opening.stream_id,
            "request_id": self.opening.request_id,
            "source_node_id": self.source,
            "target_node_id": self.target,
            "key_id": self.opening.key_id,
            "direction": self.direction,
        }

    def key(self, shared_secret: bytes) -> bytes:
        salt = hashlib.sha256(
            _canonical(
                {
                    "session_id": self.opening.session_id,
                    "source_node_id": self.opening.source_node_id,
                    "target_node_id": self.opening.target_node_id,
                }
            )
        ).digest()
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            info=_canonical(self.value()),
        ).derive(shared_secret)

    def aad(self) -> bytes:
        return _canonical(self.value())


def _encrypt(
    opening: PeerStreamOpen,
    direction: str,
    sequence: int,
    shared_secret: bytes,
    plaintext: bytes,
    sign: Callable[[bytes], str],
) -> PeerStreamFrame:
    context = _CipherContext(opening, direction)
    nonce = peer_stream_nonce(sequence)
    ciphertext = ChaCha20Poly1305(context.key(shared_secret)).encrypt(
        nonce,
        plaintext,
        context.aad(),
    )
    return PeerStreamFrame.create(
        session_id=opening.session_id,
        stream_id=opening.stream_id,
        request_id=opening.request_id,
        source_node_id=context.source,
        target_node_id=context.target,
        sequence=sequence,
        key_id=opening.key_id,
        nonce=nonce,
        ciphertext=ciphertext,
        sign=sign,
    )


def _decrypt(
    opening: PeerStreamOpen,
    direction: str,
    frame: PeerStreamFrame,
    shared_secret: bytes,
) -> bytes:
    context = _CipherContext(opening, direction)
    if (
        frame.session_id,
        frame.stream_id,
        frame.request_id,
        frame.key_id,
        frame.source_node_id,
        frame.target_node_id,
    ) != (
        opening.session_id,
        opening.stream_id,
        opening.request_id,
        opening.key_id,
        context.source,
        context.target,
    ):
        raise FederationValidationError(
            "object-transfer-frame-binding-mismatch",
            "frame",
            "frame does not match the signed transfer stream",
        )
    try:
        return ChaCha20Poly1305(context.key(shared_secret)).decrypt(
            frame.nonce,
            frame.ciphertext,
            context.aad(),
        )
    except InvalidTag as exc:
        raise FederationValidationError(
            "object-transfer-decryption-failed",
            "ciphertext",
            "ciphertext authentication failed",
        ) from exc


def _verify_frame_signature(
    frame: PeerStreamFrame,
    source_node_id: str,
    resolve_public_key: Callable[[str], str | None],
    verify_signature: Callable[[str, bytes, str], bool],
) -> None:
    public_key = resolve_public_key(source_node_id)
    if (
        frame.source_node_id != source_node_id
        or not isinstance(public_key, str)
        or not verify_signature(public_key, frame.signing_bytes(), frame.signature)
    ):
        raise FederationValidationError(
            "invalid-object-transfer-frame-signature",
            "signature",
            "frame is not signed by the enrolled source node",
        )


@dataclass
class _InboundTransfer:
    peer_id: str
    opening: PeerStreamOpen
    shared_secret: bytes
    last_sequence: int = -1
    manifest: ObjectTransferManifest | None = None
    receiver: ObjectTransferReceiver | None = None
    receipt: VerifiedObjectTransfer | None = None
    published_path: Path | None = None


class VerifiedChunkTransferEndpoint:
    """Transfer verified files using encrypted frames and bounded retransmission."""

    def __init__(
        self,
        channel: DirectPacketChannel,
        credentials: NodeSigningCredentials,
        *,
        resolve_public_key: Callable[[str], str | None],
        verify_signature: Callable[[str, bytes, str], bool],
        publication_root: str | Path,
        staging_parent: str | Path | None = None,
        max_staged_bytes: int = MAX_TRANSFER_OBJECT_BYTES,
        max_attempts: int = DEFAULT_TRANSFER_ATTEMPTS,
        max_sequence_gap: int = DEFAULT_TRANSFER_SEQUENCE_GAP,
        max_inbound_transfers: int = DEFAULT_INBOUND_TRANSFERS,
    ) -> None:
        self.channel = channel
        self.credentials = credentials
        self.resolve_public_key = resolve_public_key
        self.verify_signature = verify_signature
        self.max_attempts = _integer(
            max_attempts,
            "max_attempts",
            minimum=1,
            maximum=32,
        )
        self.max_sequence_gap = _integer(
            max_sequence_gap,
            "max_sequence_gap",
            minimum=1,
            maximum=4096,
        )
        self.max_inbound_transfers = _integer(
            max_inbound_transfers,
            "max_inbound_transfers",
            minimum=1,
            maximum=4096,
        )
        if staging_parent is not None:
            Path(staging_parent).mkdir(parents=True, exist_ok=True)
        self._temporary = tempfile.TemporaryDirectory(
            prefix="msh-f642-",
            dir=str(staging_parent) if staging_parent is not None else None,
        )
        self.store = FilesystemObjectTransferChunkStore(
            self._temporary.name,
            publication_root,
            max_bytes=max_staged_bytes,
        )
        self._private_key = X25519PrivateKey.generate()
        self._descriptor: DirectPeerDescriptor | None = None
        self._peers: dict[str, DirectPeerDescriptor] = {}
        self._inbound: OrderedDict[tuple[str, str], _InboundTransfer] = OrderedDict()
        self._started = False

    @property
    def node_id(self) -> str:
        return self.credentials.identity.node_id

    @property
    def descriptor(self) -> DirectPeerDescriptor:
        if self._descriptor is None:
            raise RuntimeError("verified chunk transfer endpoint has not started")
        return self._descriptor

    def register_peer(self, descriptor: DirectPeerDescriptor) -> None:
        if descriptor.node_id == self.node_id:
            raise FederationValidationError(
                "object-transfer-self-route",
                "node_id",
                "cannot register the local node",
            )
        self._peers[descriptor.node_id] = descriptor

    async def start(self) -> DirectPeerDescriptor:
        if not self._started:
            ready = await self.channel.start(self._handle_packet)
            self._descriptor = DirectPeerDescriptor(
                node_id=self.node_id,
                peer_id=ready.peer_id,
                multiaddr=ready.listen_addrs[0],
                encryption_public_key=_public_key_text(self._private_key.public_key()),
                protocol_version=DIRECT_PEER_PROTOCOL_VERSION,
            )
            self._started = True
        return self.descriptor

    async def close(self) -> None:
        for inbound in self._inbound.values():
            if inbound.receiver is not None and inbound.receipt is None:
                inbound.receiver.abort()
        self._inbound.clear()
        await self.channel.close()
        self._started = False
        await asyncio.to_thread(self._temporary.cleanup)

    async def send_bytes(
        self,
        *,
        target_node_id: str,
        session_id: str,
        request_id: str,
        object_id: str,
        content: bytes,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
    ) -> VerifiedObjectTransfer:
        if not isinstance(content, bytes):
            raise FederationValidationError(
                "invalid-object-transfer-content",
                "content",
                "must be bytes",
            )
        source = await asyncio.to_thread(self._write_temporary_source, content)
        try:
            return await self.send_file(
                target_node_id=target_node_id,
                session_id=session_id,
                request_id=request_id,
                object_id=object_id,
                source_path=source,
                chunk_size=chunk_size,
            )
        finally:
            await asyncio.to_thread(source.unlink, missing_ok=True)

    async def send_file(
        self,
        *,
        target_node_id: str,
        session_id: str,
        request_id: str,
        object_id: str,
        source_path: str | Path,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
    ) -> VerifiedObjectTransfer:
        await self.start()
        target = self._peers.get(target_node_id)
        if target is None:
            raise DirectTransportUnavailable(
                "object-transfer-route-missing",
                f"no explicit direct transfer route for {target_node_id}",
            )
        source = Path(source_path)
        manifest = await asyncio.to_thread(
            self._file_manifest,
            source,
            object_id,
            chunk_size,
        )
        ephemeral = X25519PrivateKey.generate()
        opening = PeerStreamOpen.create(
            session_id=session_id,
            stream_id=f"object-{manifest.transfer_id}",
            request_id=request_id,
            source_node_id=self.node_id,
            target_node_id=target.node_id,
            key_id=f"key-{secrets.token_urlsafe(18)}",
            ephemeral_public_key=_public_key_text(ephemeral.public_key()),
            sign=self.credentials.sign,
        )
        shared_secret = ephemeral.exchange(
            _public_key(target.encryption_public_key, "encryption_public_key")
        )
        sequence = 0
        completed = False
        try:
            _, sequence = await self._send(
                target,
                opening,
                shared_secret,
                sequence,
                manifest,
                "manifest",
                {"manifest": manifest.to_dict()},
            )
            for index in range(manifest.chunk_count):
                expected_length = manifest.expected_chunk_length(index)
                data = await asyncio.to_thread(
                    self._read_source_chunk,
                    source,
                    index * manifest.chunk_size,
                    expected_length,
                )
                if len(data) != expected_length:
                    raise FederationValidationError(
                        "object-transfer-source-size-changed",
                        "source_path",
                        "source file size changed during transfer",
                    )
                chunk = ObjectTransferChunk.create(
                    manifest=manifest,
                    index=index,
                    data=data,
                )
                _, sequence = await self._send(
                    target,
                    opening,
                    shared_secret,
                    sequence,
                    manifest,
                    "chunk",
                    {"chunk": chunk.to_dict()},
                    chunk_index=index,
                )
            source_size = (await asyncio.to_thread(source.stat)).st_size
            if source_size != manifest.total_size:
                raise FederationValidationError(
                    "object-transfer-source-size-changed",
                    "source_path",
                    "source file size changed during transfer",
                )
            ack, sequence = await self._send(
                target,
                opening,
                shared_secret,
                sequence,
                manifest,
                "complete",
                {},
            )
            receipt = self._receipt(ack, manifest)
            completed = True
            return receipt
        finally:
            if not completed:
                await self._abort(
                    target,
                    opening,
                    shared_secret,
                    sequence,
                    manifest,
                )

    @staticmethod
    def _write_temporary_source(content: bytes) -> Path:
        with tempfile.NamedTemporaryFile(prefix="msh-f642-source-", delete=False) as handle:
            handle.write(content)
            handle.flush()
            return Path(handle.name)

    @staticmethod
    def _read_source_chunk(source: Path, offset: int, length: int) -> bytes:
        try:
            with source.open("rb") as handle:
                handle.seek(offset)
                return handle.read(length)
        except OSError as exc:
            raise FederationOperationError(
                "object-transfer-source-unavailable",
                "source file could not be read",
                "source_path",
            ) from exc

    @staticmethod
    def _file_manifest(
        source: Path,
        object_id: str,
        chunk_size: int,
    ) -> ObjectTransferManifest:
        chunk_size = _integer(
            chunk_size,
            "chunk_size",
            minimum=1,
            maximum=MAX_TRANSFER_CHUNK_BYTES,
        )
        digest = hashlib.sha256()
        total = 0
        try:
            with source.open("rb") as handle:
                while data := handle.read(1024 * 1024):
                    digest.update(data)
                    total += len(data)
                    if total > MAX_TRANSFER_OBJECT_BYTES:
                        raise FederationValidationError(
                            "object-transfer-source-too-large",
                            "source_path",
                            f"source must not exceed {MAX_TRANSFER_OBJECT_BYTES} bytes",
                        )
        except OSError as exc:
            raise FederationOperationError(
                "object-transfer-source-unavailable",
                "source file could not be read",
                "source_path",
            ) from exc
        return ObjectTransferManifest(
            transfer_id=f"transfer-{secrets.token_urlsafe(18)}",
            object_id=object_id,
            total_size=total,
            object_hash=f"sha256:{digest.hexdigest()}",
            chunk_size=chunk_size,
            chunk_count=math.ceil(total / chunk_size) if total else 0,
        )

    async def _send(
        self,
        target: DirectPeerDescriptor,
        opening: PeerStreamOpen,
        shared_secret: bytes,
        sequence: int,
        manifest: ObjectTransferManifest,
        action: str,
        message: Mapping[str, Any],
        *,
        chunk_index: int | None = None,
    ) -> tuple[ObjectTransferAck, int]:
        last_error: BaseException | None = None
        for _ in range(self.max_attempts):
            current_sequence = sequence
            sequence += 1
            try:
                acknowledgement = await self._send_once(
                    target,
                    opening,
                    shared_secret,
                    current_sequence,
                    action,
                    message,
                )
            except (OSError, TimeoutError, DirectTransportUnavailable) as exc:
                last_error = exc
                continue
            self._validate_ack(
                acknowledgement,
                manifest,
                action,
                chunk_index,
            )
            if acknowledgement.accepted:
                return acknowledgement, sequence
            if not acknowledgement.retryable:
                raise FederationOperationError(
                    acknowledgement.error_code or "object-transfer-rejected",
                    "remote endpoint rejected the transfer action",
                    action,
                )
            last_error = FederationOperationError(
                acknowledgement.error_code or "object-transfer-retry-requested",
                "remote endpoint requested retransmission",
                action,
            )
        raise DirectTransportUnavailable(
            "object-transfer-retry-exhausted",
            f"{action} was not acknowledged after {self.max_attempts} attempts",
        ) from last_error

    async def _send_once(
        self,
        target: DirectPeerDescriptor,
        opening: PeerStreamOpen,
        shared_secret: bytes,
        sequence: int,
        action: str,
        message: Mapping[str, Any],
    ) -> ObjectTransferAck:
        plaintext = {
            "schema": DIRECT_OBJECT_TRANSFER_MESSAGE_SCHEMA,
            "protocol_version": DIRECT_OBJECT_TRANSFER_PROTOCOL_VERSION,
            "action": action,
            **dict(message),
        }
        frame = _encrypt(
            opening,
            "request",
            sequence,
            shared_secret,
            _canonical(plaintext),
            self.credentials.sign,
        )
        packet = {
            "schema": DIRECT_OBJECT_TRANSFER_PACKET_SCHEMA,
            "open": opening.to_dict(),
            "frame": frame.to_dict(),
        }
        self._check_packet_size(packet, "packet")
        response = await self.channel.request(
            target_peer_id=target.peer_id,
            target_multiaddr=target.multiaddr,
            payload=packet,
        )
        self._check_packet_size(response, "response")
        if (
            not isinstance(response, dict)
            or response.get("schema") != DIRECT_OBJECT_TRANSFER_PACKET_SCHEMA
        ):
            raise FederationValidationError(
                "invalid-object-transfer-response",
                "schema",
                "unexpected response packet",
            )
        response_frame = PeerStreamFrame.from_dict(response.get("frame"))
        if response_frame.sequence != sequence:
            raise FederationValidationError(
                "object-transfer-response-sequence-mismatch",
                "sequence",
                "ack does not match the current transfer attempt",
            )
        _verify_frame_signature(
            response_frame,
            target.node_id,
            self.resolve_public_key,
            self.verify_signature,
        )
        decrypted = _decrypt(
            opening,
            "response",
            response_frame,
            shared_secret,
        )
        try:
            return ObjectTransferAck.from_dict(json.loads(decrypted))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-object-transfer-response",
                "ciphertext",
                "decrypted acknowledgement is not JSON",
            ) from exc

    async def _abort(
        self,
        target: DirectPeerDescriptor,
        opening: PeerStreamOpen,
        shared_secret: bytes,
        sequence: int,
        manifest: ObjectTransferManifest,
    ) -> None:
        try:
            await self._send_once(
                target,
                opening,
                shared_secret,
                sequence,
                "abort",
                {
                    "transfer_id": manifest.transfer_id,
                    "object_id": manifest.object_id,
                },
            )
        except (
            OSError,
            TimeoutError,
            DirectTransportUnavailable,
            FederationOperationError,
            FederationValidationError,
        ):
            return

    @staticmethod
    def _check_packet_size(value: Any, field: str) -> None:
        try:
            size = len(_canonical(value))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-object-transfer-packet",
                field,
                "must be canonical JSON",
            ) from exc
        if size > MAX_DIRECT_OBJECT_TRANSFER_PACKET_BYTES:
            raise FederationValidationError(
                "object-transfer-packet-too-large",
                field,
                "encrypted transfer packet exceeds the protocol bound",
            )

    @staticmethod
    def _validate_ack(
        acknowledgement: ObjectTransferAck,
        manifest: ObjectTransferManifest,
        action: str,
        chunk_index: int | None,
    ) -> None:
        if (
            acknowledgement.transfer_id,
            acknowledgement.object_id,
            acknowledgement.action,
        ) != (
            manifest.transfer_id,
            manifest.object_id,
            action,
        ):
            raise FederationValidationError(
                "object-transfer-ack-binding-mismatch",
                "ack",
                "ack belongs to another transfer action",
            )
        if (
            action == "chunk"
            and acknowledgement.accepted
            and acknowledgement.chunk_index is None
        ):
            raise FederationValidationError(
                "object-transfer-ack-index-missing",
                "chunk_index",
                "accepted chunk ack requires an index",
            )
        if (
            action == "chunk"
            and acknowledgement.chunk_index is not None
            and acknowledgement.chunk_index != chunk_index
        ):
            raise FederationValidationError(
                "object-transfer-ack-index-mismatch",
                "chunk_index",
                "ack belongs to another chunk",
            )

    @staticmethod
    def _receipt(
        acknowledgement: ObjectTransferAck,
        manifest: ObjectTransferManifest,
    ) -> VerifiedObjectTransfer:
        value = acknowledgement.receipt
        if not isinstance(value, dict):
            raise FederationValidationError(
                "missing-object-transfer-receipt",
                "receipt",
                "completion ack requires a verified receipt",
            )
        receipt = VerifiedObjectTransfer(
            transfer_id=value.get("transfer_id"),
            object_id=value.get("object_id"),
            total_size=value.get("total_size"),
            object_hash=value.get("object_hash"),
            chunk_count=value.get("chunk_count"),
            protocol_version=value.get("protocol_version"),
        )
        if (
            receipt.transfer_id,
            receipt.object_id,
            receipt.total_size,
            receipt.object_hash,
            receipt.chunk_count,
        ) != (
            manifest.transfer_id,
            manifest.object_id,
            manifest.total_size,
            manifest.object_hash,
            manifest.chunk_count,
        ):
            raise FederationValidationError(
                "object-transfer-receipt-binding-mismatch",
                "receipt",
                "verified receipt does not match the sent manifest",
            )
        return receipt

    async def _handle_packet(self, source_peer_id: str, packet: JSON) -> JSON:
        self._check_packet_size(packet, "packet")
        if packet.get("schema") != DIRECT_OBJECT_TRANSFER_PACKET_SCHEMA:
            raise FederationValidationError(
                "invalid-object-transfer-packet",
                "schema",
                "unexpected transfer packet",
            )
        opening = PeerStreamOpen.from_dict(packet.get("open"))
        if opening.target_node_id != self.node_id:
            raise FederationValidationError(
                "object-transfer-target-mismatch",
                "target_node_id",
                "transfer is not addressed to the local node",
            )
        source = self._peers.get(opening.source_node_id)
        if source is None or source.peer_id != source_peer_id:
            raise FederationValidationError(
                "object-transfer-peer-identity-mismatch",
                "source_node_id",
                "libp2p identity is not bound to the enrolled node",
            )
        public_key = self.resolve_public_key(opening.source_node_id)
        if not isinstance(public_key, str) or not self.verify_signature(
            public_key,
            opening.signing_bytes(),
            opening.signature,
        ):
            raise FederationValidationError(
                "invalid-object-transfer-open-signature",
                "signature",
                "transfer open is not signed by the enrolled source node",
            )
        frame = PeerStreamFrame.from_dict(packet.get("frame"))
        _verify_frame_signature(
            frame,
            opening.source_node_id,
            self.resolve_public_key,
            self.verify_signature,
        )
        key = (opening.source_node_id, opening.stream_id)
        inbound = self._inbound.get(key)
        if inbound is None:
            self._reserve_capacity()
            inbound = _InboundTransfer(
                peer_id=source_peer_id,
                opening=opening,
                shared_secret=self._private_key.exchange(
                    _public_key(opening.ephemeral_public_key, "ephemeral_public_key")
                ),
            )
            self._inbound[key] = inbound
        elif inbound.peer_id != source_peer_id or inbound.opening != opening:
            raise FederationValidationError(
                "object-transfer-stream-rebinding",
                "open",
                "existing transfer stream cannot be rebound",
            )
        else:
            self._inbound.move_to_end(key)
        if frame.sequence <= inbound.last_sequence:
            raise FederationValidationError(
                "object-transfer-frame-replayed",
                "sequence",
                "frame sequence must increase for every attempt",
            )
        if frame.sequence - inbound.last_sequence > self.max_sequence_gap:
            raise FederationValidationError(
                "object-transfer-frame-gap-too-large",
                "sequence",
                "frame sequence gap exceeds the bounded retry window",
            )
        decrypted = _decrypt(
            opening,
            "request",
            frame,
            inbound.shared_secret,
        )
        inbound.last_sequence = frame.sequence
        message = self._message(decrypted)
        acknowledgement = self._apply(inbound, message)
        response_frame = _encrypt(
            opening,
            "response",
            frame.sequence,
            inbound.shared_secret,
            _canonical(acknowledgement.to_dict()),
            self.credentials.sign,
        )
        response = {
            "schema": DIRECT_OBJECT_TRANSFER_PACKET_SCHEMA,
            "frame": response_frame.to_dict(),
        }
        self._check_packet_size(response, "response")
        if message["action"] == "abort":
            self._inbound.pop(key, None)
        return response

    def _reserve_capacity(self) -> None:
        if len(self._inbound) < self.max_inbound_transfers:
            return
        for key, inbound in tuple(self._inbound.items()):
            if inbound.receipt is not None or inbound.receiver is None:
                self._inbound.pop(key, None)
                return
        raise FederationOperationError(
            "object-transfer-inbound-capacity-exceeded",
            "too many active inbound object transfers",
        )

    @staticmethod
    def _message(decrypted: bytes) -> JSON:
        try:
            value = json.loads(decrypted)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-object-transfer-message",
                "ciphertext",
                "decrypted transfer message is not JSON",
            ) from exc
        if (
            not isinstance(value, dict)
            or value.get("schema") != DIRECT_OBJECT_TRANSFER_MESSAGE_SCHEMA
        ):
            raise ProtocolCompatibilityError(
                "unsupported-object-transfer-message",
                "schema",
                "unexpected transfer message schema",
            )
        _protocol(value.get("protocol_version"))
        if value.get("action") not in _ACTIONS:
            raise FederationValidationError(
                "invalid-object-transfer-action",
                "action",
                "unsupported transfer action",
            )
        return value

    def _apply(
        self,
        inbound: _InboundTransfer,
        message: JSON,
    ) -> ObjectTransferAck:
        action = message["action"]
        if action == "manifest":
            return self._apply_manifest(inbound, message.get("manifest"))
        if action == "abort":
            return self._apply_abort(inbound, message)
        if inbound.manifest is None or inbound.receiver is None:
            raise FederationValidationError(
                "object-transfer-manifest-required",
                "action",
                "manifest must be accepted before transfer data",
            )
        if action == "chunk":
            return self._apply_chunk(inbound, message.get("chunk"))
        return self._apply_complete(inbound)

    def _apply_manifest(
        self,
        inbound: _InboundTransfer,
        value: Any,
    ) -> ObjectTransferAck:
        manifest = ObjectTransferManifest.from_dict(value)
        if inbound.opening.stream_id != f"object-{manifest.transfer_id}":
            raise FederationValidationError(
                "object-transfer-stream-id-mismatch",
                "stream_id",
                "manifest does not match the signed stream",
            )
        if inbound.manifest is not None:
            if inbound.manifest == manifest:
                return self._ack(
                    inbound,
                    "manifest",
                    accepted=True,
                    duplicate=True,
                )
            raise FederationValidationError(
                "object-transfer-conflicting-manifest",
                "manifest",
                "stream already has a different manifest",
            )
        inbound.manifest = manifest
        inbound.receiver = ObjectTransferReceiver(manifest, self.store)
        return self._ack(inbound, "manifest", accepted=True)

    def _apply_abort(
        self,
        inbound: _InboundTransfer,
        message: JSON,
    ) -> ObjectTransferAck:
        if inbound.receiver is not None:
            inbound.receiver.abort()
        transfer_id = (
            inbound.manifest.transfer_id
            if inbound.manifest is not None
            else _bounded(message.get("transfer_id"), "transfer_id")
        )
        object_id = (
            inbound.manifest.object_id
            if inbound.manifest is not None
            else _bounded(message.get("object_id"), "object_id")
        )
        return ObjectTransferAck(
            transfer_id=transfer_id,
            object_id=object_id,
            action="abort",
            accepted=True,
            retryable=False,
        )

    def _apply_chunk(
        self,
        inbound: _InboundTransfer,
        value: Any,
    ) -> ObjectTransferAck:
        assert inbound.receiver is not None
        try:
            chunk = ObjectTransferChunk.from_dict(value)
            accepted = inbound.receiver.accept(chunk)
        except (FederationOperationError, FederationValidationError) as exc:
            status = inbound.receiver.status()
            retryable = isinstance(exc, FederationValidationError) and exc.code in {
                "invalid-object-transfer-encoding",
                "object-transfer-chunk-length-mismatch",
                "object-transfer-chunk-hash-mismatch",
                "object-transfer-chunk-size-mismatch",
            }
            return self._ack(
                inbound,
                "chunk",
                accepted=False,
                retryable=retryable,
                error_code=exc.code,
                status=status,
            )
        return self._ack(
            inbound,
            "chunk",
            accepted=True,
            duplicate=not accepted,
            chunk_index=chunk.index,
        )

    def _apply_complete(self, inbound: _InboundTransfer) -> ObjectTransferAck:
        assert inbound.receiver is not None
        assert inbound.manifest is not None
        duplicate = inbound.receipt is not None
        if inbound.receipt is None:
            try:
                receipt = inbound.receiver.verify_complete()
                published_path = self.store.publish_verified(inbound.manifest, receipt)
            except (FederationOperationError, FederationValidationError) as exc:
                return self._ack(
                    inbound,
                    "complete",
                    accepted=False,
                    retryable=False,
                    error_code=exc.code,
                )
            inbound.receipt = receipt
            inbound.published_path = published_path
        return self._ack(
            inbound,
            "complete",
            accepted=True,
            duplicate=duplicate,
            receipt=inbound.receipt.to_dict(),
        )

    @staticmethod
    def _ack(
        inbound: _InboundTransfer,
        action: str,
        *,
        accepted: bool,
        retryable: bool = False,
        duplicate: bool = False,
        chunk_index: int | None = None,
        error_code: str | None = None,
        receipt: JSON | None = None,
        status: Any | None = None,
    ) -> ObjectTransferAck:
        assert inbound.manifest is not None
        assert inbound.receiver is not None
        current_status = status or inbound.receiver.status()
        return ObjectTransferAck(
            transfer_id=inbound.manifest.transfer_id,
            object_id=inbound.manifest.object_id,
            action=action,
            accepted=accepted,
            retryable=retryable,
            duplicate=duplicate,
            chunk_index=chunk_index,
            accepted_chunks=current_status.accepted_chunks,
            remaining_chunks=len(current_status.missing_chunks),
            next_missing_index=(
                current_status.missing_chunks[0]
                if current_status.missing_chunks
                else None
            ),
            error_code=error_code,
            receipt=receipt,
        )

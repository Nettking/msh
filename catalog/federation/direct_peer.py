"""Phase F6.2 directly reachable encrypted peer storage transport."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from .adaptive_transport import DirectTransportUnavailable
from .errors import FederationValidationError, ProtocolCompatibilityError
from .peer_stream import PeerStreamFrame, peer_stream_nonce, validate_peer_public_text
from .peer_stream_verifier import PeerStreamVerifier
from .storage_protocol import (
    STORAGE_PROTOCOL,
    StorageError,
    StorageErrorCode,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)

DIRECT_PEER_DESCRIPTOR_SCHEMA = "fcp.direct_peer.descriptor.v1"
DIRECT_PEER_OPEN_SCHEMA = "fcp.peer_stream.open.v1"
DIRECT_STORAGE_REQUEST_SCHEMA = "fcp.direct_storage.request.v1"
DIRECT_STORAGE_RESPONSE_SCHEMA = "fcp.direct_storage.response.v1"
DIRECT_PEER_PROTOCOL_VERSION = "1.0"
DIRECT_PEER_PROTOCOL_MAJOR = 1
X25519_PUBLIC_KEY_PREFIX = "x25519:"
MAX_DIRECT_PACKET_BYTES = 131_072

JSON = dict[str, Any]


class DirectPacketChannel(Protocol):
    """Process-neutral packet channel implemented by the libp2p sidecar client."""

    async def start(
        self,
        handler: Callable[[str, JSON], Awaitable[JSON]],
    ) -> ChannelReady: ...

    async def request(
        self,
        *,
        target_peer_id: str,
        target_multiaddr: str,
        payload: JSON,
    ) -> JSON: ...

    async def close(self) -> None: ...


class NodeSigningCredentials(Protocol):
    identity: Any

    def sign(self, message: bytes) -> str: ...


class AsyncStorageService(Protocol):
    async def dispatch(self, envelope: StorageRequestEnvelope) -> StorageResponseEnvelope: ...


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: Any, *, field: str, length: int) -> bytes:
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "invalid-direct-peer-encoding", field, "must be canonical base64url text"
        )
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-direct-peer-encoding", field, "must be canonical base64url text"
        ) from exc
    if len(decoded) != length or _b64encode(decoded) != value:
        raise FederationValidationError(
            "invalid-direct-peer-encoding",
            field,
            f"must encode exactly {length} bytes",
        )
    return decoded


def _public_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > 2048
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-direct-peer-field", field, "must be bounded non-empty text"
        )
    return value


def _protocol_version(value: Any) -> str:
    value = validate_peer_public_text(value, "protocol_version")
    try:
        major = int(value.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-direct-peer-protocol", "protocol_version", "must begin with a number"
        ) from exc
    if major != DIRECT_PEER_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-direct-peer-protocol",
            "protocol_version",
            f"supported major is {DIRECT_PEER_PROTOCOL_MAJOR}",
        )
    return value


def _x25519_public_text(public_key: X25519PublicKey) -> str:
    raw = public_key.public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return f"{X25519_PUBLIC_KEY_PREFIX}{_b64encode(raw)}"


def _x25519_public_key(value: Any, field: str) -> X25519PublicKey:
    if not isinstance(value, str) or not value.startswith(X25519_PUBLIC_KEY_PREFIX):
        raise FederationValidationError(
            "unsupported-direct-peer-key", field, "expected an X25519 public key"
        )
    raw = _b64decode(
        value.removeprefix(X25519_PUBLIC_KEY_PREFIX),
        field=field,
        length=32,
    )
    return X25519PublicKey.from_public_bytes(raw)


@dataclass(frozen=True)
class ChannelReady:
    peer_id: str
    listen_addrs: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", _public_text(self.peer_id, "peer_id"))
        if not self.listen_addrs:
            raise FederationValidationError(
                "missing-direct-listen-address", "listen_addrs", "at least one address is required"
            )
        object.__setattr__(
            self,
            "listen_addrs",
            tuple(_public_text(value, "listen_addrs") for value in self.listen_addrs),
        )


@dataclass(frozen=True)
class DirectPeerDescriptor:
    """Local route metadata; it must never be published in public status output."""

    node_id: str
    peer_id: str
    multiaddr: str
    encryption_public_key: str
    protocol_version: str = DIRECT_PEER_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", validate_peer_public_text(self.node_id, "node_id"))
        object.__setattr__(self, "peer_id", _public_text(self.peer_id, "peer_id"))
        object.__setattr__(self, "multiaddr", _public_text(self.multiaddr, "multiaddr"))
        _x25519_public_key(self.encryption_public_key, "encryption_public_key")
        object.__setattr__(self, "protocol_version", _protocol_version(self.protocol_version))

    @classmethod
    def from_dict(cls, value: Any) -> DirectPeerDescriptor:
        if not isinstance(value, dict) or value.get("schema") != DIRECT_PEER_DESCRIPTOR_SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-direct-peer-descriptor", "schema", "unexpected descriptor schema"
            )
        required = {
            "node_id",
            "peer_id",
            "multiaddr",
            "encryption_public_key",
            "protocol_version",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in required})

    def to_dict(self) -> JSON:
        return {
            "schema": DIRECT_PEER_DESCRIPTOR_SCHEMA,
            "protocol_version": self.protocol_version,
            "node_id": self.node_id,
            "peer_id": self.peer_id,
            "multiaddr": self.multiaddr,
            "encryption_public_key": self.encryption_public_key,
        }


@dataclass(frozen=True)
class PeerStreamOpen:
    session_id: str
    stream_id: str
    request_id: str
    source_node_id: str
    target_node_id: str
    key_id: str
    ephemeral_public_key: str
    signature: str
    protocol_version: str = DIRECT_PEER_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "stream_id",
            "request_id",
            "source_node_id",
            "target_node_id",
            "key_id",
        ):
            object.__setattr__(
                self,
                field,
                validate_peer_public_text(getattr(self, field), field),
            )
        if self.source_node_id == self.target_node_id:
            raise FederationValidationError(
                "direct-peer-loopback-identity",
                "target_node_id",
                "source and target identities must differ",
            )
        _x25519_public_key(self.ephemeral_public_key, "ephemeral_public_key")
        _b64decode(self.signature, field="signature", length=64)
        object.__setattr__(self, "protocol_version", _protocol_version(self.protocol_version))

    @classmethod
    def create(
        cls,
        *,
        session_id: str,
        stream_id: str,
        request_id: str,
        source_node_id: str,
        target_node_id: str,
        key_id: str,
        ephemeral_public_key: str,
        sign: Callable[[bytes], str],
    ) -> PeerStreamOpen:
        unsigned = cls._unsigned(
            session_id=session_id,
            stream_id=stream_id,
            request_id=request_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            key_id=key_id,
            ephemeral_public_key=ephemeral_public_key,
            protocol_version=DIRECT_PEER_PROTOCOL_VERSION,
        )
        return cls.from_dict({**unsigned, "signature": sign(_canonical(unsigned))})

    @classmethod
    def from_dict(cls, value: Any) -> PeerStreamOpen:
        if not isinstance(value, dict) or value.get("schema") != DIRECT_PEER_OPEN_SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-direct-peer-open", "schema", "unexpected stream-open schema"
            )
        required = {
            "protocol_version",
            "session_id",
            "stream_id",
            "request_id",
            "source_node_id",
            "target_node_id",
            "key_id",
            "ephemeral_public_key",
            "signature",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in required})

    @staticmethod
    def _unsigned(**value: Any) -> JSON:
        return {"schema": DIRECT_PEER_OPEN_SCHEMA, **value}

    def signing_bytes(self) -> bytes:
        return _canonical(
            self._unsigned(
                protocol_version=self.protocol_version,
                session_id=self.session_id,
                stream_id=self.stream_id,
                request_id=self.request_id,
                source_node_id=self.source_node_id,
                target_node_id=self.target_node_id,
                key_id=self.key_id,
                ephemeral_public_key=self.ephemeral_public_key,
            )
        )

    def to_dict(self) -> JSON:
        return {**json.loads(self.signing_bytes()), "signature": self.signature}


@dataclass(frozen=True)
class _CryptographicContext:
    session_id: str
    stream_id: str
    request_id: str
    source_node_id: str
    target_node_id: str
    key_id: str
    direction: str

    def value(self) -> JSON:
        return {
            "protocol": "fcp-direct-storage-v1",
            "session_id": self.session_id,
            "stream_id": self.stream_id,
            "request_id": self.request_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "key_id": self.key_id,
            "direction": self.direction,
        }

    def key(self, shared_secret: bytes) -> bytes:
        salt = hashlib.sha256(
            _canonical(
                {
                    "session_id": self.session_id,
                    "source_node_id": self.source_node_id,
                    "target_node_id": self.target_node_id,
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


def _frame_context(opening: PeerStreamOpen, *, direction: str) -> _CryptographicContext:
    if direction == "request":
        source = opening.source_node_id
        target = opening.target_node_id
    elif direction == "response":
        source = opening.target_node_id
        target = opening.source_node_id
    else:
        raise ValueError("direction must be request or response")
    return _CryptographicContext(
        session_id=opening.session_id,
        stream_id=opening.stream_id,
        request_id=opening.request_id,
        source_node_id=source,
        target_node_id=target,
        key_id=opening.key_id,
        direction=direction,
    )


def _encrypt_frame(
    *,
    opening: PeerStreamOpen,
    direction: str,
    shared_secret: bytes,
    plaintext: bytes,
    sign: Callable[[bytes], str],
) -> PeerStreamFrame:
    context = _frame_context(opening, direction=direction)
    nonce = peer_stream_nonce(0)
    ciphertext = ChaCha20Poly1305(context.key(shared_secret)).encrypt(
        nonce,
        plaintext,
        context.aad(),
    )
    return PeerStreamFrame.create(
        session_id=opening.session_id,
        stream_id=opening.stream_id,
        request_id=opening.request_id,
        source_node_id=context.source_node_id,
        target_node_id=context.target_node_id,
        sequence=0,
        key_id=opening.key_id,
        nonce=nonce,
        ciphertext=ciphertext,
        sign=sign,
    )


def _decrypt_frame(
    *,
    opening: PeerStreamOpen,
    frame: PeerStreamFrame,
    direction: str,
    shared_secret: bytes,
) -> bytes:
    context = _frame_context(opening, direction=direction)
    if (
        frame.session_id != opening.session_id
        or frame.stream_id != opening.stream_id
        or frame.request_id != opening.request_id
        or frame.key_id != opening.key_id
        or frame.source_node_id != context.source_node_id
        or frame.target_node_id != context.target_node_id
        or frame.sequence != 0
    ):
        raise FederationValidationError(
            "direct-peer-frame-mismatch", "frame", "frame does not match the signed stream open"
        )
    try:
        return ChaCha20Poly1305(context.key(shared_secret)).decrypt(
            frame.nonce,
            frame.ciphertext,
            context.aad(),
        )
    except InvalidTag as exc:
        raise FederationValidationError(
            "direct-peer-decryption-failed", "ciphertext", "ciphertext authentication failed"
        ) from exc


class DirectStorageEndpoint:
    """Directly reachable encrypted storage endpoint behind a packet sidecar."""

    def __init__(
        self,
        channel: DirectPacketChannel,
        credentials: NodeSigningCredentials,
        *,
        resolve_public_key: Callable[[str], str | None],
        verify_signature: Callable[[str, bytes, str], bool],
        services: Mapping[str, AsyncStorageService] | None = None,
        replay_window: int = 1024,
    ) -> None:
        self.channel = channel
        self.credentials = credentials
        self.resolve_public_key = resolve_public_key
        self.verify_signature = verify_signature
        if isinstance(replay_window, bool) or not isinstance(replay_window, int) or replay_window < 1:
            raise ValueError("replay_window must be a positive integer")
        self.services = dict(services or {})
        self._replay_window = replay_window
        self._accepted_keys: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._receiver_private_key = X25519PrivateKey.generate()
        self._descriptor: DirectPeerDescriptor | None = None
        self._peers: dict[str, DirectPeerDescriptor] = {}
        self._started = False

    @property
    def node_id(self) -> str:
        return self.credentials.identity.node_id

    @property
    def descriptor(self) -> DirectPeerDescriptor:
        if self._descriptor is None:
            raise RuntimeError("direct storage endpoint has not started")
        return self._descriptor

    def register_service(self, provider_id: str, service: AsyncStorageService) -> None:
        self.services[validate_peer_public_text(provider_id, "provider_id")] = service

    def register_peer(self, descriptor: DirectPeerDescriptor) -> None:
        if descriptor.node_id == self.node_id:
            raise FederationValidationError(
                "direct-peer-self-route", "node_id", "cannot register the local node as a peer"
            )
        self._peers[descriptor.node_id] = descriptor

    async def start(self) -> DirectPeerDescriptor:
        if not self._started:
            ready = await self.channel.start(self._handle_packet)
            self._descriptor = DirectPeerDescriptor(
                node_id=self.node_id,
                peer_id=ready.peer_id,
                multiaddr=ready.listen_addrs[0],
                encryption_public_key=_x25519_public_text(
                    self._receiver_private_key.public_key()
                ),
            )
            self._started = True
        return self.descriptor

    async def close(self) -> None:
        await self.channel.close()
        self._started = False

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        await self.start()
        target = self._peers.get(target_node_id)
        if target is None:
            raise DirectTransportUnavailable(
                "direct-peer-route-missing",
                f"no explicit direct route for {target_node_id}",
            )
        if envelope.actor_node_id != self.node_id:
            raise FederationValidationError(
                "direct-peer-actor-mismatch",
                "actor_node_id",
                "request actor must be the local node identity",
            )
        ephemeral = X25519PrivateKey.generate()
        stream_id = f"storage-{envelope.request_id}"
        key_id = f"key-{secrets.token_urlsafe(18)}"
        opening = PeerStreamOpen.create(
            session_id=envelope.session_id,
            stream_id=stream_id,
            request_id=envelope.request_id,
            source_node_id=self.node_id,
            target_node_id=target.node_id,
            key_id=key_id,
            ephemeral_public_key=_x25519_public_text(ephemeral.public_key()),
            sign=self.credentials.sign,
        )
        shared_secret = ephemeral.exchange(
            _x25519_public_key(target.encryption_public_key, "encryption_public_key")
        )
        request_frame = _encrypt_frame(
            opening=opening,
            direction="request",
            shared_secret=shared_secret,
            plaintext=_canonical(envelope.to_dict()),
            sign=self.credentials.sign,
        )
        packet = {
            "schema": DIRECT_STORAGE_REQUEST_SCHEMA,
            "open": opening.to_dict(),
            "frame": request_frame.to_dict(),
        }
        if len(_canonical(packet)) > MAX_DIRECT_PACKET_BYTES:
            raise DirectTransportUnavailable(
                "direct-peer-packet-too-large",
                "request exceeds the F6.2 direct frame bound",
            )
        try:
            response_packet = await self.channel.request(
                target_peer_id=target.peer_id,
                target_multiaddr=target.multiaddr,
                payload=packet,
            )
        except (OSError, TimeoutError) as exc:
            raise DirectTransportUnavailable(
                "direct-peer-unavailable", "direct peer request could not be completed"
            ) from exc
        if (
            not isinstance(response_packet, dict)
            or response_packet.get("schema") != DIRECT_STORAGE_RESPONSE_SCHEMA
        ):
            raise FederationValidationError(
                "invalid-direct-storage-response", "schema", "unexpected response packet"
            )
        response_frame = PeerStreamFrame.from_dict(response_packet.get("frame"))
        self._verify_frame(
            response_frame,
            session_id=opening.session_id,
            local_node_id=self.node_id,
            expected_source_node_id=target.node_id,
        )
        plaintext = _decrypt_frame(
            opening=opening,
            frame=response_frame,
            direction="response",
            shared_secret=shared_secret,
        )
        response = self._parse_response(plaintext)
        if response.request_id != envelope.request_id:
            raise FederationValidationError(
                "direct-peer-request-mismatch",
                "request_id",
                "response belongs to another storage request",
            )
        return response

    async def _handle_packet(self, source_peer_id: str, packet: JSON) -> JSON:
        if packet.get("schema") != DIRECT_STORAGE_REQUEST_SCHEMA:
            raise FederationValidationError(
                "invalid-direct-storage-request", "schema", "unexpected request packet"
            )
        if len(_canonical(packet)) > MAX_DIRECT_PACKET_BYTES:
            raise FederationValidationError(
                "direct-peer-packet-too-large", "packet", "packet exceeds the F6.2 bound"
            )
        opening = PeerStreamOpen.from_dict(packet.get("open"))
        if opening.target_node_id != self.node_id:
            raise FederationValidationError(
                "direct-peer-target-mismatch",
                "target_node_id",
                "stream is not addressed to the local node",
            )
        source = self._peers.get(opening.source_node_id)
        if source is None or source.peer_id != source_peer_id:
            raise FederationValidationError(
                "direct-peer-identity-mismatch",
                "source_node_id",
                "libp2p peer identity is not bound to the enrolled node",
            )
        public_key = self.resolve_public_key(opening.source_node_id)
        if not isinstance(public_key, str) or not self.verify_signature(
            public_key,
            opening.signing_bytes(),
            opening.signature,
        ):
            raise FederationValidationError(
                "invalid-direct-peer-open-signature",
                "signature",
                "stream open is not signed by the enrolled source node",
            )
        request_frame = PeerStreamFrame.from_dict(packet.get("frame"))
        self._verify_frame(
            request_frame,
            session_id=opening.session_id,
            local_node_id=self.node_id,
            expected_source_node_id=opening.source_node_id,
        )
        shared_secret = self._receiver_private_key.exchange(
            _x25519_public_key(opening.ephemeral_public_key, "ephemeral_public_key")
        )
        request_plaintext = _decrypt_frame(
            opening=opening,
            frame=request_frame,
            direction="request",
            shared_secret=shared_secret,
        )
        request = self._parse_request(request_plaintext)
        replay_key = (opening.source_node_id, opening.key_id)
        if replay_key in self._accepted_keys:
            raise FederationValidationError(
                "direct-peer-replayed",
                "key_id",
                "this authenticated direct stream was already accepted",
            )
        if (
            request.request_id != opening.request_id
            or request.session_id != opening.session_id
            or request.actor_node_id != opening.source_node_id
        ):
            raise FederationValidationError(
                "direct-peer-request-binding-mismatch",
                "request",
                "storage request does not match the signed direct stream",
            )
        self._accepted_keys[replay_key] = None
        self._accepted_keys.move_to_end(replay_key)
        while len(self._accepted_keys) > self._replay_window:
            self._accepted_keys.popitem(last=False)
        response = await self._dispatch(request)
        response_frame = _encrypt_frame(
            opening=opening,
            direction="response",
            shared_secret=shared_secret,
            plaintext=_canonical(response.to_dict()),
            sign=self.credentials.sign,
        )
        return {
            "schema": DIRECT_STORAGE_RESPONSE_SCHEMA,
            "frame": response_frame.to_dict(),
        }

    def _verify_frame(
        self,
        frame: PeerStreamFrame,
        *,
        session_id: str,
        local_node_id: str,
        expected_source_node_id: str,
    ) -> None:
        verifier = PeerStreamVerifier(
            session_id=session_id,
            local_node_id=local_node_id,
            resolve_public_key=self.resolve_public_key,
            verify_signature=self.verify_signature,
            max_streams=1,
        )
        verifier.accept(frame, expected_source_node_id=expected_source_node_id)

    @staticmethod
    def _parse_request(value: bytes) -> StorageRequestEnvelope:
        try:
            decoded = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-direct-storage-request", "ciphertext", "plaintext is not JSON"
            ) from exc
        return StorageRequestEnvelope.from_dict(decoded)

    @staticmethod
    def _parse_response(value: bytes) -> StorageResponseEnvelope:
        try:
            decoded = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-direct-storage-response", "ciphertext", "plaintext is not JSON"
            ) from exc
        return StorageResponseEnvelope.from_dict(decoded)

    async def _dispatch(self, request: StorageRequestEnvelope) -> StorageResponseEnvelope:
        provider_id = request.authorization_context.get("provider_id")
        service = self.services.get(provider_id) if isinstance(provider_id, str) else None
        if service is not None:
            return await service.dispatch(request)
        return StorageResponseEnvelope(
            request_id=request.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=request.protocol_version,
            ok=False,
            error=StorageError(
                code=StorageErrorCode.UNAUTHORIZED,
                message="target node does not host the requested storage provider",
                field="provider_id",
            ),
        )

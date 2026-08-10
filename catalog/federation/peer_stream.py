"""Signed ciphertext peer-stream contract for Phase F6.1 and later."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .errors import FederationValidationError, ProtocolCompatibilityError
from .redaction import redact_secrets

PEER_STREAM_SCHEMA = "fcp.peer_stream.frame.v1"
PEER_STREAM_PROTOCOL_VERSION = "1.0"
PEER_STREAM_PROTOCOL_MAJOR = 1
PEER_STREAM_CIPHER_SUITE = "x25519-hkdf-sha256+chacha20-poly1305"
MAX_PEER_STREAM_CIPHERTEXT_BYTES = 65_536
MAX_PEER_STREAM_ID_BYTES = 512
DEFAULT_PEER_STREAM_LIMIT = 1_024


def validate_peer_public_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > MAX_PEER_STREAM_ID_BYTES
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-peer-frame-field",
            field,
            "must be non-empty bounded public text without control characters",
        )
    if redact_secrets(value) != value:
        raise FederationValidationError(
            "nonpublic-peer-frame-field",
            field,
            "must not contain credentials, backend paths, or physical addresses",
        )
    return value


def _protocol_major(value: Any) -> int:
    value = validate_peer_public_text(value, "protocol_version")
    try:
        major = int(value.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-peer-protocol-version",
            "protocol_version",
            "must begin with a numeric major version",
        ) from exc
    if major != PEER_STREAM_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-peer-protocol-major",
            "protocol_version",
            f"supported major is {PEER_STREAM_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(
    value: Any,
    *,
    field: str,
    expected_length: int | None = None,
    maximum_length: int | None = None,
) -> bytes:
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "invalid-peer-frame-encoding", field, "must be canonical base64url text"
        )
    padded = value + ("=" * (-len(value) % 4))
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-peer-frame-encoding", field, "must be canonical base64url text"
        ) from exc
    if _base64url_encode(decoded) != value:
        raise FederationValidationError(
            "invalid-peer-frame-encoding", field, "must use canonical base64url"
        )
    if expected_length is not None and len(decoded) != expected_length:
        raise FederationValidationError(
            "invalid-peer-frame-length",
            field,
            f"must encode exactly {expected_length} bytes",
        )
    if maximum_length is not None and len(decoded) > maximum_length:
        raise FederationValidationError(
            "peer-frame-too-large",
            field,
            f"must not exceed {maximum_length} bytes",
        )
    return decoded


def _hash(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def peer_stream_nonce(sequence: int) -> bytes:
    """Derive the unique AEAD nonce for one per-stream key and sequence."""

    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or not 0 <= sequence < 2**64
    ):
        raise FederationValidationError(
            "invalid-peer-frame-sequence",
            "sequence",
            "must be an unsigned 64-bit integer",
        )
    return b"\0" * 4 + sequence.to_bytes(8, "big")


def _canonical(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


@dataclass(frozen=True)
class PeerStreamFrame:
    """One signed, ciphertext-only peer stream frame.

    Encryption and key agreement are deliberately external to this contract. The
    frame binds ciphertext to the logical session, enrolled node identities,
    stream, request, sequence, nonce, key identifier, protocol, and cipher suite.
    """

    session_id: str
    stream_id: str
    request_id: str
    source_node_id: str
    target_node_id: str
    sequence: int
    key_id: str
    nonce: bytes
    ciphertext: bytes
    signature: str
    protocol_version: str = PEER_STREAM_PROTOCOL_VERSION
    cipher_suite: str = PEER_STREAM_CIPHER_SUITE

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
                "peer-frame-loopback-identity",
                "target_node_id",
                "source and target node identities must differ",
            )
        expected_nonce = peer_stream_nonce(self.sequence)
        if not isinstance(self.nonce, bytes) or self.nonce != expected_nonce:
            raise FederationValidationError(
                "invalid-peer-frame-nonce",
                "nonce",
                "must be the canonical nonce derived from the frame sequence",
            )
        if (
            not isinstance(self.ciphertext, bytes)
            or not self.ciphertext
            or len(self.ciphertext) > MAX_PEER_STREAM_CIPHERTEXT_BYTES
        ):
            raise FederationValidationError(
                "invalid-peer-frame-ciphertext",
                "ciphertext",
                f"must contain 1-{MAX_PEER_STREAM_CIPHERTEXT_BYTES} bytes",
            )
        _base64url_decode(self.signature, field="signature", expected_length=64)
        _protocol_major(self.protocol_version)
        if self.cipher_suite != PEER_STREAM_CIPHER_SUITE:
            raise ProtocolCompatibilityError(
                "unsupported-peer-cipher-suite",
                "cipher_suite",
                f"expected {PEER_STREAM_CIPHER_SUITE}",
            )

    @classmethod
    def create(
        cls,
        *,
        session_id: str,
        stream_id: str,
        request_id: str,
        source_node_id: str,
        target_node_id: str,
        sequence: int,
        key_id: str,
        nonce: bytes,
        ciphertext: bytes,
        sign: Callable[[bytes], str],
        protocol_version: str = PEER_STREAM_PROTOCOL_VERSION,
        cipher_suite: str = PEER_STREAM_CIPHER_SUITE,
    ) -> PeerStreamFrame:
        unsigned = cls._unsigned_value(
            session_id=session_id,
            stream_id=stream_id,
            request_id=request_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            sequence=sequence,
            key_id=key_id,
            nonce=nonce,
            ciphertext=ciphertext,
            protocol_version=protocol_version,
            cipher_suite=cipher_suite,
        )
        signature = sign(_canonical(unsigned))
        return cls.from_dict({**unsigned, "signature": signature})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PeerStreamFrame:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-peer-frame", "frame", "must be a JSON object"
            )
        if value.get("schema") != PEER_STREAM_SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-peer-frame-schema",
                "schema",
                f"expected {PEER_STREAM_SCHEMA}",
            )
        required = {
            "protocol_version",
            "cipher_suite",
            "session_id",
            "stream_id",
            "request_id",
            "source_node_id",
            "target_node_id",
            "sequence",
            "key_id",
            "nonce",
            "ciphertext_length",
            "ciphertext_hash",
            "ciphertext",
            "signature",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError(
                "missing-peer-frame-field", missing[0], "is required"
            )
        nonce = _base64url_decode(value["nonce"], field="nonce", expected_length=12)
        ciphertext = _base64url_decode(
            value["ciphertext"],
            field="ciphertext",
            maximum_length=MAX_PEER_STREAM_CIPHERTEXT_BYTES,
        )
        declared_length = value["ciphertext_length"]
        if (
            isinstance(declared_length, bool)
            or not isinstance(declared_length, int)
            or declared_length != len(ciphertext)
        ):
            raise FederationValidationError(
                "peer-frame-length-mismatch",
                "ciphertext_length",
                "does not match the decoded ciphertext",
            )
        if value["ciphertext_hash"] != _hash(ciphertext):
            raise FederationValidationError(
                "peer-frame-hash-mismatch",
                "ciphertext_hash",
                "does not match the decoded ciphertext",
            )
        return cls(
            session_id=value["session_id"],
            stream_id=value["stream_id"],
            request_id=value["request_id"],
            source_node_id=value["source_node_id"],
            target_node_id=value["target_node_id"],
            sequence=value["sequence"],
            key_id=value["key_id"],
            nonce=nonce,
            ciphertext=ciphertext,
            signature=value["signature"],
            protocol_version=value["protocol_version"],
            cipher_suite=value["cipher_suite"],
        )

    @staticmethod
    def _unsigned_value(
        *,
        session_id: str,
        stream_id: str,
        request_id: str,
        source_node_id: str,
        target_node_id: str,
        sequence: int,
        key_id: str,
        nonce: bytes,
        ciphertext: bytes,
        protocol_version: str,
        cipher_suite: str,
    ) -> dict[str, Any]:
        # Construct once with a canonical dummy signature so all field validation
        # is shared with received frames before any signing callback is invoked.
        dummy = _base64url_encode(b"\0" * 64)
        candidate = PeerStreamFrame(
            session_id=session_id,
            stream_id=stream_id,
            request_id=request_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            sequence=sequence,
            key_id=key_id,
            nonce=nonce,
            ciphertext=ciphertext,
            signature=dummy,
            protocol_version=protocol_version,
            cipher_suite=cipher_suite,
        )
        return candidate.unsigned_dict()

    def unsigned_dict(self) -> dict[str, Any]:
        return {
            "schema": PEER_STREAM_SCHEMA,
            "protocol_version": self.protocol_version,
            "cipher_suite": self.cipher_suite,
            "session_id": self.session_id,
            "stream_id": self.stream_id,
            "request_id": self.request_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "sequence": self.sequence,
            "key_id": self.key_id,
            "nonce": _base64url_encode(self.nonce),
            "ciphertext_length": len(self.ciphertext),
            "ciphertext_hash": _hash(self.ciphertext),
            "ciphertext": _base64url_encode(self.ciphertext),
        }

    def signing_bytes(self) -> bytes:
        return _canonical(self.unsigned_dict())

    def to_dict(self) -> dict[str, Any]:
        return {**self.unsigned_dict(), "signature": self.signature}

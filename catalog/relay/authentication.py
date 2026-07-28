"""Application-level Ed25519 challenge authentication for relay connections."""

from __future__ import annotations

import base64
import json
import secrets
from typing import Final

from catalog.federation.errors import FederationValidationError
from catalog.federation.protocol import protocol_major
from catalog.node.identity import verify_signature

AUTHENTICATION_SCHEMA: Final = "msh.relay.authentication.v1"
NONCE_BYTES: Final = 32


def _bounded_text(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-authentication-field",
            field,
            "must be non-empty bounded text without control characters",
        )
    return value


def _validate_nonce(value: object) -> str:
    nonce = _bounded_text(value, field="nonce")
    padded = nonce + ("=" * (-len(nonce) % 4))
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-authentication-nonce",
            "nonce",
            "must be canonical base64url text",
        ) from exc
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if len(decoded) != NONCE_BYTES or canonical != nonce:
        raise FederationValidationError(
            "invalid-authentication-nonce",
            "nonce",
            f"must encode exactly {NONCE_BYTES} bytes",
        )
    return nonce


def create_authentication_nonce() -> str:
    """Return a fresh 256-bit base64url challenge nonce."""

    return base64.urlsafe_b64encode(secrets.token_bytes(NONCE_BYTES)).decode(
        "ascii"
    ).rstrip("=")


def authentication_message(
    *,
    challenge_id: str,
    nonce: str,
    node_id: str,
    protocol_version: str,
) -> bytes:
    """Return the exact canonical bytes a node signs for one relay challenge.

    Binding the signature to the challenge request ID, nonce, node identity, and
    protocol version prevents a proof captured on one connection from being
    substituted for another identity or protocol negotiation.
    """

    challenge_id = _bounded_text(challenge_id, field="challenge_id")
    nonce = _validate_nonce(nonce)
    node_id = _bounded_text(node_id, field="node_id")
    protocol_version = _bounded_text(
        protocol_version, field="protocol_version"
    )
    protocol_major(protocol_version)
    value = {
        "challenge_id": challenge_id,
        "node_id": node_id,
        "nonce": nonce,
        "protocol_version": protocol_version,
        "schema": AUTHENTICATION_SCHEMA,
    }
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")


def verify_authentication_signature(
    *,
    public_key: str,
    signature: str,
    challenge_id: str,
    nonce: str,
    node_id: str,
    protocol_version: str,
) -> bool:
    """Verify proof of possession for an enrolled Ed25519 public identity."""

    message = authentication_message(
        challenge_id=challenge_id,
        nonce=nonce,
        node_id=node_id,
        protocol_version=protocol_version,
    )
    return verify_signature(public_key, message, signature)

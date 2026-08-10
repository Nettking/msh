"""Bounded, versioned JSON envelopes for the relay-first Phase 2 transport."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar

from .errors import FederationValidationError, ProtocolCompatibilityError
from .redaction import redact_secrets

PROTOCOL_VERSION = "1.0"
PROTOCOL_MAJOR = 1
MAX_MESSAGE_BYTES = 65_536


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 512
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-envelope-field",
            field,
            "must be non-empty bounded text without control characters",
        )
    return value


def _public_text(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if redact_secrets(value) != value:
        raise FederationValidationError(
            "nonpublic-envelope-field",
            field,
            "must not contain credentials, backend paths, or physical addresses",
        )
    return value


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _public_text(value, field)


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FederationValidationError("invalid-object", field, "must be an object")
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain JSON-compatible values"
        ) from exc
    return value


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", "sent_at", "must be RFC 3339"
            ) from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp", "sent_at", "must be timezone-aware"
        )
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError(
            "invalid-timestamp", "sent_at", "must use UTC"
        )
    return value.astimezone(timezone.utc)


def protocol_major(value: Any) -> int:
    version = _required_text(value, "protocol_version")
    try:
        major = int(version.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-protocol-version",
            "protocol_version",
            "must begin with a numeric major version",
        ) from exc
    if major != PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "protocol_version",
            f"supported major is {PROTOCOL_MAJOR}, received {major}",
        )
    return major


@dataclass(frozen=True)
class RelayEnvelope:
    """One control or routed relay command.

    ``sent_at`` is diagnostics only. Authorization and ordering are derived from
    the authenticated connection, durable membership, and session revisions.
    """

    SCHEMA: ClassVar[str] = "fcp.relay.envelope.v1"

    request_id: str
    actor_node_id: str
    message_type: str
    payload: dict[str, Any]
    authorization_context: dict[str, Any]
    sent_at: datetime
    session_id: str | None = None
    target_node_id: str | None = None
    protocol_version: str = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in ("request_id", "actor_node_id", "message_type"):
            object.__setattr__(
                self,
                name,
                _public_text(getattr(self, name), name),
            )
        object.__setattr__(
            self, "session_id", _optional_text(self.session_id, "session_id")
        )
        object.__setattr__(
            self,
            "target_node_id",
            _optional_text(self.target_node_id, "target_node_id"),
        )
        protocol_major(self.protocol_version)
        object.__setattr__(self, "payload", _object(self.payload, "payload"))
        authorization_context = _object(
            self.authorization_context,
            "authorization_context",
        )
        if redact_secrets(authorization_context) != authorization_context:
            raise FederationValidationError(
                "nonpublic-authorization-context",
                "authorization_context",
                "must not contain credentials, backend paths, or physical addresses",
            )
        object.__setattr__(
            self,
            "authorization_context",
            authorization_context,
        )
        object.__setattr__(self, "sent_at", _timestamp(self.sent_at))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RelayEnvelope:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "envelope", "must be an object")
        if value.get("schema") != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema",
                "schema",
                f"expected {cls.SCHEMA}, received {value.get('schema')!r}",
            )
        required = {
            "request_id",
            "actor_node_id",
            "message_type",
            "payload",
            "authorization_context",
            "sent_at",
            "protocol_version",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            request_id=value["request_id"],
            actor_node_id=value["actor_node_id"],
            message_type=value["message_type"],
            payload=value["payload"],
            authorization_context=value["authorization_context"],
            sent_at=value["sent_at"],
            session_id=value.get("session_id"),
            target_node_id=value.get("target_node_id"),
            protocol_version=value["protocol_version"],
        )

    @classmethod
    def from_json(
        cls, value: str | bytes, *, max_bytes: int = MAX_MESSAGE_BYTES
    ) -> RelayEnvelope:
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        elif isinstance(value, bytes):
            encoded = value
        else:
            raise FederationValidationError(
                "malformed-message", "envelope", "must be a text or UTF-8 byte frame"
            )
        if len(encoded) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "envelope", f"exceeds {max_bytes} bytes"
            )
        try:
            decoded = encoded.decode("utf-8")
            parsed = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "malformed-message", "envelope", "must be valid UTF-8 JSON"
            ) from exc
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "actor_node_id": self.actor_node_id,
            "target_node_id": self.target_node_id,
            "message_type": self.message_type,
            "authorization_context": self.authorization_context,
            "payload": self.payload,
            "sent_at": self.sent_at.isoformat().replace("+00:00", "Z"),
        }

    def to_json(self, *, max_bytes: int = MAX_MESSAGE_BYTES) -> str:
        try:
            encoded = json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-json", "envelope", "contains non-JSON values"
            ) from exc
        if len(encoded.encode("utf-8")) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "envelope", f"exceeds {max_bytes} bytes"
            )
        return encoded

    def fingerprint(self) -> str:
        """Hash authority-relevant request content without persisting raw secrets."""

        value = self.to_dict()
        value.pop("sent_at", None)
        canonical = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def response_envelope(
    request: RelayEnvelope,
    *,
    actor_node_id: str,
    message_type: str,
    payload: dict[str, Any],
    authorization_context: dict[str, Any] | None = None,
) -> RelayEnvelope:
    return RelayEnvelope(
        request_id=request.request_id,
        session_id=request.session_id,
        actor_node_id=actor_node_id,
        target_node_id=request.actor_node_id,
        message_type=message_type,
        authorization_context=authorization_context or {"kind": "relay-response"},
        payload=payload,
        sent_at=utc_now(),
    )

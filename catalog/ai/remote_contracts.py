"""Versioned logical request/reply contracts for trusted remote AI invocation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

from .runtime_contracts import (
    AIProviderFailureClass,
    AIRuntimeRequest,
    MAX_AI_RESPONSE_BYTES,
    _canonical,
    _logical_id,
    _text,
    _utc,
)

REMOTE_AI_PROTOCOL = "msh-remote-language-model"
REMOTE_AI_PROTOCOL_VERSION = "1.0"
REMOTE_AI_PROTOCOL_MAJOR = 1
REMOTE_AI_REQUEST_SCHEMA = "msh.remote-ai-invocation-request.v1"
REMOTE_AI_RESPONSE_SCHEMA = "msh.remote-ai-invocation-response.v1"
MAX_REMOTE_AI_CONTROL_MESSAGE_BYTES = 5 * 1024 * 1024
MAX_REMOTE_AI_INVOCATION_TTL_SECONDS = 3_600


class RemoteAIInvocationOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be a positive integer",
        )
    return value


def _non_negative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual or ".v" not in expected:
        raise ProtocolCompatibilityError(
            "unsupported-schema",
            "schema",
            f"expected {expected}, received {actual!r}",
        )
    actual_name, actual_major = actual.rsplit(".v", 1)
    expected_name, expected_major = expected.rsplit(".v", 1)
    if actual_name != expected_name or not actual_major.isdigit():
        raise ProtocolCompatibilityError(
            "unsupported-schema",
            "schema",
            f"expected {expected}, received {actual!r}",
        )
    if int(actual_major) != int(expected_major):
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            "schema",
            f"supported major is {expected_major}, received {actual_major}",
        )


def validate_remote_ai_protocol(value: Any) -> int:
    version = _text(value, "remote_protocol_version", maximum=32)
    parts = version.split(".", 1)
    if not parts[0].isdigit() or (
        len(parts) == 2 and not parts[1].isdigit()
    ):
        raise ProtocolCompatibilityError(
            "invalid-remote-protocol-version",
            "remote_protocol_version",
            "must be a numeric major or major.minor version",
        )
    major = int(parts[0])
    if major != REMOTE_AI_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-remote-protocol-major",
            "remote_protocol_version",
            f"supported major is {REMOTE_AI_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def _decode_json(
    value: str | bytes,
    *,
    field: str,
    max_bytes: int = MAX_REMOTE_AI_CONTROL_MESSAGE_BYTES,
) -> dict[str, Any]:
    encoded = value.encode("utf-8") if isinstance(value, str) else value
    if not isinstance(encoded, bytes):
        raise FederationValidationError(
            "malformed-message",
            field,
            "must be text or UTF-8 bytes",
        )
    if len(encoded) > max_bytes:
        raise FederationValidationError(
            "message-too-large",
            field,
            f"exceeds {max_bytes} bytes",
        )
    try:
        decoded = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FederationValidationError(
            "malformed-message",
            field,
            "must be valid UTF-8 JSON",
        ) from exc
    if not isinstance(decoded, dict):
        raise FederationValidationError(
            "invalid-object",
            field,
            "must decode to an object",
        )
    return decoded


def _encode_json(
    value: dict[str, Any],
    *,
    field: str,
    max_bytes: int = MAX_REMOTE_AI_CONTROL_MESSAGE_BYTES,
) -> str:
    encoded = _canonical(value)
    if len(encoded.encode("utf-8")) > max_bytes:
        raise FederationValidationError(
            "message-too-large",
            field,
            f"exceeds {max_bytes} bytes",
        )
    return encoded


@dataclass(frozen=True)
class RemoteAIInvocationRequest:
    """One logical invocation routed to a specific healthy provider generation."""

    SCHEMA: ClassVar[str] = REMOTE_AI_REQUEST_SCHEMA

    invocation_id: str
    session_id: str
    requester_node_id: str
    provider_node_id: str
    capability_id: str
    provider_generation: int
    health_report_revision: int
    request: AIRuntimeRequest
    sent_at: datetime
    expires_at: datetime
    remote_protocol_version: str = REMOTE_AI_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in (
            "invocation_id",
            "session_id",
            "requester_node_id",
            "provider_node_id",
            "capability_id",
        ):
            object.__setattr__(self, name, _logical_id(getattr(self, name), name))
        object.__setattr__(
            self,
            "provider_generation",
            _positive(self.provider_generation, "provider_generation"),
        )
        object.__setattr__(
            self,
            "health_report_revision",
            _non_negative(
                self.health_report_revision,
                "health_report_revision",
            ),
        )
        if not isinstance(self.request, AIRuntimeRequest):
            raise FederationValidationError(
                "invalid-ai-request",
                "request",
                "must be an AIRuntimeRequest",
            )
        if self.request.session_id != self.session_id:
            raise FederationValidationError(
                "cross-session-remote-ai-request",
                "request.session_id",
                "must match the invocation session",
            )
        object.__setattr__(self, "sent_at", _utc(self.sent_at, "sent_at"))
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        if self.expires_at <= self.sent_at:
            raise FederationValidationError(
                "invalid-remote-ai-expiry",
                "expires_at",
                "must be later than sent_at",
            )
        if (
            self.expires_at - self.sent_at
        ).total_seconds() > MAX_REMOTE_AI_INVOCATION_TTL_SECONDS:
            raise FederationValidationError(
                "remote-ai-ttl-too-large",
                "expires_at",
                (
                    "must not exceed "
                    f"{MAX_REMOTE_AI_INVOCATION_TTL_SECONDS} seconds"
                ),
            )
        validate_remote_ai_protocol(self.remote_protocol_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "remote_protocol_version": self.remote_protocol_version,
            "invocation_id": self.invocation_id,
            "session_id": self.session_id,
            "requester_node_id": self.requester_node_id,
            "provider_node_id": self.provider_node_id,
            "capability_id": self.capability_id,
            "provider_generation": self.provider_generation,
            "health_report_revision": self.health_report_revision,
            "request": self.request.to_dict(),
            "sent_at": self.sent_at.isoformat().replace("+00:00", "Z"),
            "expires_at": self.expires_at.isoformat().replace("+00:00", "Z"),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "remote_ai_request",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "remote_protocol_version",
            "invocation_id",
            "session_id",
            "requester_node_id",
            "provider_node_id",
            "capability_id",
            "provider_generation",
            "health_report_revision",
            "request",
            "sent_at",
            "expires_at",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(
            remote_protocol_version=value["remote_protocol_version"],
            invocation_id=value["invocation_id"],
            session_id=value["session_id"],
            requester_node_id=value["requester_node_id"],
            provider_node_id=value["provider_node_id"],
            capability_id=value["capability_id"],
            provider_generation=value["provider_generation"],
            health_report_revision=value["health_report_revision"],
            request=AIRuntimeRequest.from_dict(value["request"]),
            sent_at=value["sent_at"],
            expires_at=value["expires_at"],
        )

    def to_json(self) -> str:
        return _encode_json(self.to_dict(), field="remote_ai_request")

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        return cls.from_dict(_decode_json(value, field="remote_ai_request"))


@dataclass(frozen=True)
class RemoteAIInvocationResponse:
    """Safe result frame returned by the authenticated provider node."""

    SCHEMA: ClassVar[str] = REMOTE_AI_RESPONSE_SCHEMA

    invocation_id: str
    session_id: str
    requester_node_id: str
    provider_node_id: str
    capability_id: str
    provider_generation: int
    request_id: str
    outcome: RemoteAIInvocationOutcome
    completed_at: datetime
    content: str | None = None
    error_code: str | None = None
    failure_class: AIProviderFailureClass | None = None
    retryable: bool = False
    remote_protocol_version: str = REMOTE_AI_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in (
            "invocation_id",
            "session_id",
            "requester_node_id",
            "provider_node_id",
            "capability_id",
            "request_id",
        ):
            object.__setattr__(self, name, _logical_id(getattr(self, name), name))
        object.__setattr__(
            self,
            "provider_generation",
            _positive(self.provider_generation, "provider_generation"),
        )
        try:
            object.__setattr__(
                self,
                "outcome",
                RemoteAIInvocationOutcome(self.outcome),
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-remote-ai-outcome",
                "outcome",
                "unknown invocation outcome",
            ) from exc
        object.__setattr__(
            self,
            "completed_at",
            _utc(self.completed_at, "completed_at"),
        )
        validate_remote_ai_protocol(self.remote_protocol_version)
        if not isinstance(self.retryable, bool):
            raise FederationValidationError(
                "invalid-retryable-flag",
                "retryable",
                "must be boolean",
            )
        if self.outcome is RemoteAIInvocationOutcome.SUCCEEDED:
            if self.content is None:
                raise FederationValidationError(
                    "missing-success-content",
                    "content",
                    "is required for a successful invocation",
                )
            object.__setattr__(
                self,
                "content",
                _text(self.content, "content", maximum=MAX_AI_RESPONSE_BYTES),
            )
            if self.error_code is not None or self.failure_class is not None:
                raise FederationValidationError(
                    "unexpected-success-error",
                    "response",
                    "successful responses must not contain failure fields",
                )
            if self.retryable:
                raise FederationValidationError(
                    "unexpected-success-retryable",
                    "retryable",
                    "successful responses cannot be retryable",
                )
        else:
            if self.content is not None:
                raise FederationValidationError(
                    "unexpected-failure-content",
                    "content",
                    "failed responses must not contain model output",
                )
            if self.error_code is None or self.failure_class is None:
                raise FederationValidationError(
                    "missing-failure-fields",
                    "response",
                    "failed responses require an error code and failure class",
                )
            object.__setattr__(
                self,
                "error_code",
                _logical_id(self.error_code, "error_code"),
            )
            try:
                object.__setattr__(
                    self,
                    "failure_class",
                    AIProviderFailureClass(self.failure_class),
                )
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "invalid-ai-failure-class",
                    "failure_class",
                    "unsupported failure class",
                ) from exc

    @classmethod
    def succeeded(
        cls,
        request: RemoteAIInvocationRequest,
        *,
        content: str,
        completed_at: datetime,
    ) -> Self:
        return cls(
            invocation_id=request.invocation_id,
            session_id=request.session_id,
            requester_node_id=request.requester_node_id,
            provider_node_id=request.provider_node_id,
            capability_id=request.capability_id,
            provider_generation=request.provider_generation,
            request_id=request.request.request_id,
            outcome=RemoteAIInvocationOutcome.SUCCEEDED,
            content=content,
            completed_at=completed_at,
        )

    @classmethod
    def failed(
        cls,
        request: RemoteAIInvocationRequest,
        *,
        error_code: str,
        failure_class: AIProviderFailureClass,
        retryable: bool,
        completed_at: datetime,
    ) -> Self:
        return cls(
            invocation_id=request.invocation_id,
            session_id=request.session_id,
            requester_node_id=request.requester_node_id,
            provider_node_id=request.provider_node_id,
            capability_id=request.capability_id,
            provider_generation=request.provider_generation,
            request_id=request.request.request_id,
            outcome=RemoteAIInvocationOutcome.FAILED,
            error_code=error_code,
            failure_class=failure_class,
            retryable=retryable,
            completed_at=completed_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "remote_protocol_version": self.remote_protocol_version,
            "invocation_id": self.invocation_id,
            "session_id": self.session_id,
            "requester_node_id": self.requester_node_id,
            "provider_node_id": self.provider_node_id,
            "capability_id": self.capability_id,
            "provider_generation": self.provider_generation,
            "request_id": self.request_id,
            "outcome": self.outcome.value,
            "content": self.content,
            "error_code": self.error_code,
            "failure_class": (
                None if self.failure_class is None else self.failure_class.value
            ),
            "retryable": self.retryable,
            "completed_at": self.completed_at.isoformat().replace("+00:00", "Z"),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "remote_ai_response",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "remote_protocol_version",
            "invocation_id",
            "session_id",
            "requester_node_id",
            "provider_node_id",
            "capability_id",
            "provider_generation",
            "request_id",
            "outcome",
            "content",
            "error_code",
            "failure_class",
            "retryable",
            "completed_at",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(**{name: value[name] for name in names})

    def to_json(self) -> str:
        return _encode_json(self.to_dict(), field="remote_ai_response")

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        return cls.from_dict(_decode_json(value, field="remote_ai_response"))

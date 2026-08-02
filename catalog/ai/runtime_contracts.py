"""Versioned contracts for the F7.7 logical language-model runtime."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import FederationValidationError, ProtocolCompatibilityError

AI_RUNTIME_PROTOCOL = "msh-language-model"
AI_RUNTIME_PROTOCOL_VERSION = "1.0"
AI_RUNTIME_PROTOCOL_MAJOR = 1
AI_REQUEST_SCHEMA = "msh.ai-request.v1"
AI_SELECTION_STATUS_SCHEMA = "msh.ai-selection-status.v1"
AI_ATTEMPT_SCHEMA = "msh.ai-provider-attempt.v1"
AI_RESULT_SCHEMA = "msh.ai-result.v1"
MAX_AI_CONTROL_MESSAGE_BYTES = 524_288
MAX_AI_PROMPT_BYTES = 262_144
MAX_AI_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_AI_TIMEOUT_SECONDS = 7 * 24 * 60 * 60
MAX_AI_ATTEMPTS = 32

_LOGICAL_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SCHEMA_RE = re.compile(r"^(?P<name>msh\.[a-z0-9.-]+)\.v(?P<major>[0-9]+)$")


class AIModality(str, Enum):
    TEXT = "text"
    VISION = "vision"
    EMBEDDINGS = "embeddings"


class AIProviderFailureClass(str, Enum):
    TRANSIENT = "transient"
    TIMEOUT = "timeout"
    OVERLOADED = "overloaded"
    CANCELLED = "cancelled"
    SECURITY = "security"
    AUTHORIZATION = "authorization"
    PROTOCOL = "protocol"
    INVALID_RESPONSE = "invalid-response"
    INTERNAL = "internal"


NON_FALLBACK_FAILURE_CLASSES = frozenset(
    {
        AIProviderFailureClass.SECURITY,
        AIProviderFailureClass.AUTHORIZATION,
        AIProviderFailureClass.PROTOCOL,
        AIProviderFailureClass.INVALID_RESPONSE,
        AIProviderFailureClass.CANCELLED,
    }
)


def _text(value: Any, field: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty text without surrounding whitespace",
        )
    if any(ord(character) < 32 and character not in "\n\r\t" for character in value):
        raise FederationValidationError(
            "invalid-text", field, "must not contain control characters"
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "text-too-large", field, f"must not exceed {maximum} UTF-8 bytes"
        )
    return value


def _logical_id(value: Any, field: str) -> str:
    value = _text(value, field, maximum=128)
    if _LOGICAL_ID_RE.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-logical-id",
            field,
            "must be a lowercase logical identifier without URLs, addresses, or paths",
        )
    return value


def _positive(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "integer-too-large", field, f"must not exceed {maximum}"
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", field, "must be RFC 3339"
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware UTC"
        )
    return value.astimezone(timezone.utc)


def _schema(value: Any, expected: str) -> None:
    actual_text = _text(value, "schema")
    actual = _SCHEMA_RE.fullmatch(actual_text)
    wanted = _SCHEMA_RE.fullmatch(expected)
    if actual is None or wanted is None or actual.group("name") != wanted.group("name"):
        raise ProtocolCompatibilityError(
            "unsupported-schema", "schema", f"expected {expected}, received {actual_text!r}"
        )
    if int(actual.group("major")) != int(wanted.group("major")):
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            "schema",
            f"supported major is {wanted.group('major')}, received {actual.group('major')}",
        )


def validate_ai_runtime_protocol(value: Any) -> int:
    version = _text(value, "protocol_version")
    parts = version.split(".", 1)
    if not parts[0].isdigit() or (len(parts) == 2 and not parts[1].isdigit()):
        raise ProtocolCompatibilityError(
            "invalid-protocol-version",
            "protocol_version",
            "must be a numeric major or major.minor version",
        )
    major = int(parts[0])
    if major != AI_RUNTIME_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "protocol_version",
            f"supported major is {AI_RUNTIME_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", "value", "must contain JSON-compatible finite values"
        ) from exc


@dataclass(frozen=True)
class AIRuntimeRequest:
    SCHEMA: ClassVar[str] = AI_REQUEST_SCHEMA

    request_id: str
    session_id: str
    idempotency_key: str
    model: str
    modality: AIModality
    prompt: str
    system_prompt: str
    timeout_seconds: int = 120
    protocol_version: str = AI_RUNTIME_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", _logical_id(self.request_id, "request_id"))
        object.__setattr__(self, "session_id", _logical_id(self.session_id, "session_id"))
        object.__setattr__(
            self, "idempotency_key", _logical_id(self.idempotency_key, "idempotency_key")
        )
        object.__setattr__(self, "model", _text(self.model, "model", maximum=256))
        try:
            object.__setattr__(self, "modality", AIModality(self.modality))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-ai-modality", "modality", "unsupported AI modality"
            ) from exc
        object.__setattr__(
            self, "prompt", _text(self.prompt, "prompt", maximum=MAX_AI_PROMPT_BYTES)
        )
        object.__setattr__(
            self,
            "system_prompt",
            _text(self.system_prompt, "system_prompt", maximum=MAX_AI_PROMPT_BYTES),
        )
        object.__setattr__(
            self,
            "timeout_seconds",
            _positive(
                self.timeout_seconds,
                "timeout_seconds",
                maximum=MAX_AI_TIMEOUT_SECONDS,
            ),
        )
        validate_ai_runtime_protocol(self.protocol_version)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "idempotency_key": self.idempotency_key,
            "model": self.model,
            "modality": self.modality.value,
            "prompt": self.prompt,
            "system_prompt": self.system_prompt,
            "timeout_seconds": self.timeout_seconds,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "ai_request", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        required = (
            "protocol_version",
            "request_id",
            "session_id",
            "idempotency_key",
            "model",
            "modality",
            "prompt",
            "system_prompt",
            "timeout_seconds",
        )
        missing = [name for name in required if name not in value]
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in required})


@dataclass(frozen=True)
class AIProviderAttempt:
    SCHEMA: ClassVar[str] = AI_ATTEMPT_SCHEMA

    capability_id: str
    provider_name: str
    outcome: str
    error_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "capability_id", _logical_id(self.capability_id, "capability_id")
        )
        object.__setattr__(
            self, "provider_name", _text(self.provider_name, "provider_name", maximum=80)
        )
        object.__setattr__(self, "outcome", _text(self.outcome, "outcome", maximum=64))
        if self.error_code is not None:
            object.__setattr__(
                self, "error_code", _logical_id(self.error_code, "error_code")
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "capability_id": self.capability_id,
            "provider_name": self.provider_name,
            "outcome": self.outcome,
            "error_code": self.error_code,
        }


@dataclass(frozen=True)
class AISelectionStatus:
    SCHEMA: ClassVar[str] = AI_SELECTION_STATUS_SCHEMA

    decision: str
    selected_capability_id: str | None
    selected_provider_name: str | None
    eligible_capability_ids: tuple[str, ...]
    rejected: dict[str, tuple[str, ...]]
    reason: str
    evaluated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "decision", _logical_id(self.decision, "decision"))
        if self.selected_capability_id is not None:
            object.__setattr__(
                self,
                "selected_capability_id",
                _logical_id(self.selected_capability_id, "selected_capability_id"),
            )
        if self.selected_provider_name is not None:
            object.__setattr__(
                self,
                "selected_provider_name",
                _text(self.selected_provider_name, "selected_provider_name", maximum=80),
            )
        eligible = tuple(
            _logical_id(item, f"eligible_capability_ids[{index}]")
            for index, item in enumerate(self.eligible_capability_ids)
        )
        object.__setattr__(self, "eligible_capability_ids", eligible)
        rejected: dict[str, tuple[str, ...]] = {}
        for key, reasons in sorted(self.rejected.items()):
            logical_key = _logical_id(key, "rejected.capability_id")
            rejected[logical_key] = tuple(
                _text(reason, f"rejected.{logical_key}[{index}]", maximum=256)
                for index, reason in enumerate(reasons)
            )
        object.__setattr__(self, "rejected", rejected)
        object.__setattr__(self, "reason", _logical_id(self.reason, "reason"))
        object.__setattr__(self, "evaluated_at", _utc(self.evaluated_at, "evaluated_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "decision": self.decision,
            "selected_capability_id": self.selected_capability_id,
            "selected_provider_name": self.selected_provider_name,
            "eligible_capability_ids": list(self.eligible_capability_ids),
            "rejected": {key: list(value) for key, value in self.rejected.items()},
            "reason": self.reason,
            "evaluated_at": self.evaluated_at.isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class AIRuntimeResult:
    SCHEMA: ClassVar[str] = AI_RESULT_SCHEMA

    request_id: str
    model: str
    modality: AIModality
    content: str
    provider_capability_id: str
    provider_name: str
    selection: AISelectionStatus
    attempts: tuple[AIProviderAttempt, ...]
    completed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", _logical_id(self.request_id, "request_id"))
        object.__setattr__(self, "model", _text(self.model, "model", maximum=256))
        try:
            object.__setattr__(self, "modality", AIModality(self.modality))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-ai-modality", "modality", "unsupported AI modality"
            ) from exc
        object.__setattr__(
            self, "content", _text(self.content, "content", maximum=MAX_AI_RESPONSE_BYTES)
        )
        object.__setattr__(
            self,
            "provider_capability_id",
            _logical_id(self.provider_capability_id, "provider_capability_id"),
        )
        object.__setattr__(
            self, "provider_name", _text(self.provider_name, "provider_name", maximum=80)
        )
        if not isinstance(self.selection, AISelectionStatus):
            raise FederationValidationError(
                "invalid-selection-status", "selection", "must be AISelectionStatus"
            )
        attempts = tuple(self.attempts)
        if not attempts or len(attempts) > MAX_AI_ATTEMPTS:
            raise FederationValidationError(
                "invalid-ai-attempt-count",
                "attempts",
                f"must contain between 1 and {MAX_AI_ATTEMPTS} attempts",
            )
        if any(not isinstance(item, AIProviderAttempt) for item in attempts):
            raise FederationValidationError(
                "invalid-ai-attempt", "attempts", "must contain AIProviderAttempt values"
            )
        object.__setattr__(self, "attempts", attempts)
        object.__setattr__(self, "completed_at", _utc(self.completed_at, "completed_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "request_id": self.request_id,
            "model": self.model,
            "modality": self.modality.value,
            "content": self.content,
            "provider_capability_id": self.provider_capability_id,
            "provider_name": self.provider_name,
            "selection": self.selection.to_dict(),
            "attempts": [item.to_dict() for item in self.attempts],
            "completed_at": self.completed_at.isoformat().replace("+00:00", "Z"),
        }


class AIRuntimeError(RuntimeError):
    """Structured fail-closed error returned by the logical AI runtime."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        failure_class: AIProviderFailureClass = AIProviderFailureClass.INTERNAL,
        retryable: bool = False,
        provider_capability_id: str | None = None,
        selection: AISelectionStatus | None = None,
        attempts: tuple[AIProviderAttempt, ...] = (),
    ) -> None:
        self.code = _logical_id(code, "code")
        self.message = _text(message, "message", maximum=2048)
        try:
            self.failure_class = AIProviderFailureClass(failure_class)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-ai-failure-class", "failure_class", "unsupported failure class"
            ) from exc
        self.retryable = bool(retryable)
        self.provider_capability_id = (
            None
            if provider_capability_id is None
            else _logical_id(provider_capability_id, "provider_capability_id")
        )
        self.selection = selection
        self.attempts = tuple(attempts)
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "failure_class": self.failure_class.value,
            "retryable": self.retryable,
            "provider_capability_id": self.provider_capability_id,
            "selection": None if self.selection is None else self.selection.to_dict(),
            "attempts": [item.to_dict() for item in self.attempts],
        }


def encode_ai_control_message(value: dict[str, Any]) -> str:
    encoded = _canonical(value)
    if len(encoded.encode("utf-8")) > MAX_AI_CONTROL_MESSAGE_BYTES:
        raise FederationValidationError(
            "message-too-large",
            "ai_control_message",
            f"exceeds {MAX_AI_CONTROL_MESSAGE_BYTES} bytes",
        )
    return encoded

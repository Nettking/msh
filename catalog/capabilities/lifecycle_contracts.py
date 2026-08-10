"""Versioned F7.5 cancellation, heartbeat, retry, and result contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

from .job_store import DurableJobSnapshot, _canonical, _text, _timestamp, _utc
from .jobs import ArtifactReference, protocol_major

LIFECYCLE_PROTOCOL = "fcp-capability-lifecycle"
LIFECYCLE_PROTOCOL_VERSION = "1.0"
LIFECYCLE_PROTOCOL_MAJOR = 1
CANCELLATION_REQUEST_SCHEMA = "fcp.capability-cancellation.v1"
CANCELLATION_RESPONSE_SCHEMA = "fcp.capability-cancellation-response.v1"
HEARTBEAT_SCHEMA = "fcp.capability-job-heartbeat.v1"
RETRY_STATE_SCHEMA = "fcp.capability-job-retry-state.v1"
RESULT_COMMIT_SCHEMA = "fcp.capability-result-commit.v1"
MAX_LIFECYCLE_MESSAGE_BYTES = 131_072
MAX_HEARTBEAT_TIMEOUT_SECONDS = 3_600


class CancellationState(str, Enum):
    CANCELLED = "cancelled"
    NOT_RUNNING = "not-running"
    REJECTED = "rejected"


class CompletionDisposition(str, Enum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    RETRY_SCHEDULED = "retry-scheduled"
    FINAL_FAILURE = "final-failure"
    STALE_IGNORED = "stale-ignored"
    CANCELLED_IGNORED = "cancelled-ignored"


class LifecycleAction(str, Enum):
    NONE = "none"
    CANCELLATION_FORCED = "cancellation-forced"
    RETRY_SCHEDULED = "retry-scheduled"
    FINAL_TIMEOUT = "final-timeout"
    FINAL_FAILURE = "final-failure"


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


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual:
        raise ProtocolCompatibilityError(
            "unsupported-schema", "schema", f"expected {expected}"
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


def _protocol(value: Any) -> str:
    version = _text(value, "protocol_version")
    major = protocol_major(version)
    if major != LIFECYCLE_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-lifecycle-protocol-major",
            "protocol_version",
            f"supported major is {LIFECYCLE_PROTOCOL_MAJOR}, received {major}",
        )
    return version


def _bounded(value: dict[str, Any]) -> None:
    if len(_canonical(value).encode("utf-8")) > MAX_LIFECYCLE_MESSAGE_BYTES:
        raise FederationValidationError(
            "lifecycle-message-too-large",
            "message",
            f"must not exceed {MAX_LIFECYCLE_MESSAGE_BYTES} bytes",
        )


@dataclass(frozen=True)
class CancellationRequest:
    cancellation_id: str
    session_id: str
    coordinator_node_id: str
    target_node_id: str
    provider_id: str
    job_id: str
    attempt_id: str
    lease_id: str
    lease_generation: int
    requested_at: datetime
    reason: str
    protocol_version: str = LIFECYCLE_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = CANCELLATION_REQUEST_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "cancellation_id",
            "session_id",
            "coordinator_node_id",
            "target_node_id",
            "provider_id",
            "job_id",
            "attempt_id",
            "lease_id",
            "reason",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "lease_generation",
            _positive(self.lease_generation, "lease_generation"),
        )
        object.__setattr__(
            self, "requested_at", _utc(self.requested_at, "requested_at")
        )
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    @classmethod
    def from_snapshot(
        cls,
        snapshot: DurableJobSnapshot,
        *,
        cancellation_id: str,
        target_node_id: str,
        requested_at: datetime,
        reason: str,
    ) -> Self:
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned",
                "job_id",
                "remote cancellation requires active ownership",
            )
        return cls(
            cancellation_id=cancellation_id,
            session_id=snapshot.job.session_id,
            coordinator_node_id=ownership.granted_by_coordinator_id,
            target_node_id=target_node_id,
            provider_id=ownership.owner_provider_id,
            job_id=snapshot.job.job_id,
            attempt_id=ownership.attempt_id,
            lease_id=ownership.lease_id,
            lease_generation=ownership.lease_generation,
            requested_at=requested_at,
            reason=reason,
        )

    def identity_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": LIFECYCLE_PROTOCOL,
            "protocol_version": self.protocol_version,
            "cancellation_id": self.cancellation_id,
            "session_id": self.session_id,
            "coordinator_node_id": self.coordinator_node_id,
            "target_node_id": self.target_node_id,
            "provider_id": self.provider_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "reason": self.reason,
        }

    def fingerprint(self) -> str:
        payload = _canonical(self.identity_dict()).encode("utf-8")
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.identity_dict(),
            "requested_at": _timestamp(self.requested_at),
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "cancellation", "must be an object"
            )
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != LIFECYCLE_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-lifecycle-protocol",
                "protocol",
                f"expected {LIFECYCLE_PROTOCOL}",
            )
        names = (
            "cancellation_id",
            "session_id",
            "coordinator_node_id",
            "target_node_id",
            "provider_id",
            "job_id",
            "attempt_id",
            "lease_id",
            "lease_generation",
            "requested_at",
            "reason",
            "protocol_version",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(**{name: value[name] for name in names})

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        try:
            return cls.from_dict(json.loads(value))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-json", "cancellation", "must be valid JSON"
            ) from exc


@dataclass(frozen=True)
class CancellationResponse:
    cancellation_id: str
    session_id: str
    provider_id: str
    job_id: str
    attempt_id: str
    state: CancellationState
    occurred_at: datetime
    reason_code: str | None = None
    protocol_version: str = LIFECYCLE_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = CANCELLATION_RESPONSE_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "cancellation_id",
            "session_id",
            "provider_id",
            "job_id",
            "attempt_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            object.__setattr__(self, "state", CancellationState(self.state))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum", "state", "unknown cancellation state"
            ) from exc
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        object.__setattr__(
            self, "reason_code", _optional_text(self.reason_code, "reason_code")
        )
        if self.state is CancellationState.REJECTED and self.reason_code is None:
            raise FederationValidationError(
                "missing-reason-code",
                "reason_code",
                "rejected cancellation requires a reason",
            )
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def validate_for(self, request: CancellationRequest) -> Self:
        if (
            self.cancellation_id,
            self.session_id,
            self.provider_id,
            self.job_id,
            self.attempt_id,
        ) != (
            request.cancellation_id,
            request.session_id,
            request.provider_id,
            request.job_id,
            request.attempt_id,
        ):
            raise FederationValidationError(
                "cancellation-response-mismatch",
                "response",
                "response identifies another cancellation",
            )
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": LIFECYCLE_PROTOCOL,
            "protocol_version": self.protocol_version,
            "cancellation_id": self.cancellation_id,
            "session_id": self.session_id,
            "provider_id": self.provider_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "state": self.state.value,
            "occurred_at": _timestamp(self.occurred_at),
            "reason_code": self.reason_code,
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "response", "must be an object"
            )
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != LIFECYCLE_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-lifecycle-protocol",
                "protocol",
                f"expected {LIFECYCLE_PROTOCOL}",
            )
        return cls(
            cancellation_id=value.get("cancellation_id"),
            session_id=value.get("session_id"),
            provider_id=value.get("provider_id"),
            job_id=value.get("job_id"),
            attempt_id=value.get("attempt_id"),
            state=value.get("state"),
            occurred_at=value.get("occurred_at"),
            reason_code=value.get("reason_code"),
            protocol_version=value.get("protocol_version"),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        try:
            return cls.from_dict(json.loads(value))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-json", "response", "must be valid JSON"
            ) from exc


@dataclass(frozen=True)
class JobHeartbeat:
    heartbeat_id: str
    session_id: str
    worker_node_id: str
    provider_id: str
    job_id: str
    attempt_id: str
    lease_id: str
    lease_generation: int
    sequence: int
    occurred_at: datetime
    protocol_version: str = LIFECYCLE_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = HEARTBEAT_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "heartbeat_id",
            "session_id",
            "worker_node_id",
            "provider_id",
            "job_id",
            "attempt_id",
            "lease_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "lease_generation",
            _positive(self.lease_generation, "lease_generation"),
        )
        object.__setattr__(self, "sequence", _positive(self.sequence, "sequence"))
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": LIFECYCLE_PROTOCOL,
            "protocol_version": self.protocol_version,
            "heartbeat_id": self.heartbeat_id,
            "session_id": self.session_id,
            "worker_node_id": self.worker_node_id,
            "provider_id": self.provider_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "sequence": self.sequence,
            "occurred_at": _timestamp(self.occurred_at),
        }

    def to_json(self) -> str:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "heartbeat", "must be an object"
            )
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != LIFECYCLE_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-lifecycle-protocol",
                "protocol",
                f"expected {LIFECYCLE_PROTOCOL}",
            )
        return cls(
            heartbeat_id=value.get("heartbeat_id"),
            session_id=value.get("session_id"),
            worker_node_id=value.get("worker_node_id"),
            provider_id=value.get("provider_id"),
            job_id=value.get("job_id"),
            attempt_id=value.get("attempt_id"),
            lease_id=value.get("lease_id"),
            lease_generation=value.get("lease_generation"),
            sequence=value.get("sequence"),
            occurred_at=value.get("occurred_at"),
            protocol_version=value.get("protocol_version"),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        try:
            return cls.from_dict(json.loads(value))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "invalid-json", "heartbeat", "must be valid JSON"
            ) from exc


@dataclass(frozen=True)
class RetryState:
    job_id: str
    source_attempt_id: str
    source_provider_id: str
    source_lease_id: str
    source_lease_generation: int
    error_code: str
    retry_not_before: datetime
    SCHEMA: ClassVar[str] = RETRY_STATE_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "job_id",
            "source_attempt_id",
            "source_provider_id",
            "source_lease_id",
            "error_code",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "source_lease_generation",
            _positive(self.source_lease_generation, "source_lease_generation"),
        )
        object.__setattr__(
            self, "retry_not_before", _utc(self.retry_not_before, "retry_not_before")
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "job_id": self.job_id,
            "source_attempt_id": self.source_attempt_id,
            "source_provider_id": self.source_provider_id,
            "source_lease_id": self.source_lease_id,
            "source_lease_generation": self.source_lease_generation,
            "error_code": self.error_code,
            "retry_not_before": _timestamp(self.retry_not_before),
        }


@dataclass(frozen=True)
class ResultCommit:
    job_id: str
    attempt_id: str
    provider_id: str
    lease_id: str
    lease_generation: int
    reference: ArtifactReference
    committed_at: datetime
    SCHEMA: ClassVar[str] = RESULT_COMMIT_SCHEMA

    def __post_init__(self) -> None:
        for field in ("job_id", "attempt_id", "provider_id", "lease_id"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "lease_generation",
            _positive(self.lease_generation, "lease_generation"),
        )
        if not isinstance(self.reference, ArtifactReference):
            object.__setattr__(
                self, "reference", ArtifactReference.from_dict(self.reference)
            )
        object.__setattr__(self, "committed_at", _utc(self.committed_at, "committed_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "provider_id": self.provider_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "reference": self.reference.to_dict(),
            "committed_at": _timestamp(self.committed_at),
        }

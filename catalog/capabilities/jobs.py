"""Versioned, transport-independent job contracts for distributed capabilities.

This module contains pure domain logic only. It does not dispatch work, claim
ownership, contact providers, or alter the existing MSH AI/runtime flow.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

JOBS_PROTOCOL = "msh-jobs"
JOBS_PROTOCOL_VERSION = "1.0"
JOBS_PROTOCOL_MAJOR = 1
JOB_SCHEMA = "msh.job.v1"
CAPABILITY_REQUIREMENT_SCHEMA = "msh.job-capability-requirement.v1"
ARTIFACT_REFERENCE_SCHEMA = "msh.job-artifact-reference.v1"
RETRY_POLICY_SCHEMA = "msh.job-retry-policy.v1"
TIMEOUT_POLICY_SCHEMA = "msh.job-timeout-policy.v1"
ATTEMPT_SCHEMA = "msh.job-attempt.v1"
MAX_JOB_MESSAGE_BYTES = 262_144
MAX_TEXT_BYTES = 512
MAX_REFERENCE_COUNT = 128
MAX_ATTEMPTS = 32
MAX_TIMEOUT_SECONDS = 7 * 24 * 60 * 60

_SCHEMA_PATTERN = re.compile(r"^(?P<name>msh\.[a-z0-9.-]+)\.v(?P<major>[0-9]+)$")
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class JobStatus(str, Enum):
    """Monotonic logical status for one job."""

    SUBMITTED = "submitted"
    QUEUED = "queued"
    ACTIVE = "active"
    RETRY_WAIT = "retry-wait"
    CANCELLATION_REQUESTED = "cancellation-requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed-out"


class AttemptStatus(str, Enum):
    """Monotonic status for one immutable attempt generation."""

    PENDING = "pending"
    ASSIGNED = "assigned"
    ACCEPTED = "accepted"
    RUNNING = "running"
    CANCELLATION_REQUESTED = "cancellation-requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed-out"
    LOST = "lost"


_TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.TIMED_OUT,
    }
)
_TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.LOST,
    }
)
_ACTIVE_ATTEMPT_STATUSES = frozenset(set(AttemptStatus) - _TERMINAL_ATTEMPT_STATUSES)

_ALLOWED_JOB_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.SUBMITTED: frozenset(
        {JobStatus.QUEUED, JobStatus.CANCELLATION_REQUESTED}
    ),
    JobStatus.QUEUED: frozenset(
        {
            JobStatus.ACTIVE,
            JobStatus.CANCELLATION_REQUESTED,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
        }
    ),
    JobStatus.ACTIVE: frozenset(
        {
            JobStatus.RETRY_WAIT,
            JobStatus.CANCELLATION_REQUESTED,
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
        }
    ),
    JobStatus.RETRY_WAIT: frozenset(
        {
            JobStatus.ACTIVE,
            JobStatus.CANCELLATION_REQUESTED,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
        }
    ),
    JobStatus.CANCELLATION_REQUESTED: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.TIMED_OUT,
        }
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
    JobStatus.TIMED_OUT: frozenset(),
}

_ALLOWED_ATTEMPT_TRANSITIONS: dict[AttemptStatus, frozenset[AttemptStatus]] = {
    AttemptStatus.PENDING: frozenset(
        {AttemptStatus.ASSIGNED, AttemptStatus.CANCELLATION_REQUESTED}
    ),
    AttemptStatus.ASSIGNED: frozenset(
        {
            AttemptStatus.ACCEPTED,
            AttemptStatus.CANCELLATION_REQUESTED,
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }
    ),
    AttemptStatus.ACCEPTED: frozenset(
        {
            AttemptStatus.RUNNING,
            AttemptStatus.CANCELLATION_REQUESTED,
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }
    ),
    AttemptStatus.RUNNING: frozenset(
        {
            AttemptStatus.CANCELLATION_REQUESTED,
            AttemptStatus.SUCCEEDED,
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }
    ),
    AttemptStatus.CANCELLATION_REQUESTED: frozenset(
        {
            AttemptStatus.SUCCEEDED,
            AttemptStatus.FAILED,
            AttemptStatus.CANCELLED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }
    ),
    AttemptStatus.SUCCEEDED: frozenset(),
    AttemptStatus.FAILED: frozenset(),
    AttemptStatus.CANCELLED: frozenset(),
    AttemptStatus.TIMED_OUT: frozenset(),
    AttemptStatus.LOST: frozenset(),
}


def _bounded_text(value: Any, field: str, *, max_bytes: int = MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise FederationValidationError(
            "invalid-text", field, "must be non-empty text without surrounding whitespace"
        )
    if any(ord(character) < 32 for character in value):
        raise FederationValidationError(
            "invalid-text", field, "must not contain control characters"
        )
    if len(value.encode("utf-8")) > max_bytes:
        raise FederationValidationError(
            "text-too-large", field, f"must not exceed {max_bytes} UTF-8 bytes"
        )
    return value


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, field)


def _positive_int(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "integer-too-large", field, f"must not exceed {maximum}"
        )
    return value


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer", field, "must be a non-negative integer"
        )
    return value


def _json_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise FederationValidationError(
            "invalid-object", field, "must be an object with string keys"
        )
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain JSON-compatible finite values"
        ) from exc
    return dict(value)


def _enum(value: Any, enum_type: type[Enum], field: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-enum", field, f"unknown {enum_type.__name__} value"
        ) from exc


def _schema_major(value: Any, expected_schema: str, field: str = "schema") -> int:
    schema = _bounded_text(value, field)
    match = _SCHEMA_PATTERN.fullmatch(schema)
    expected = _SCHEMA_PATTERN.fullmatch(expected_schema)
    if match is None or expected is None or match.group("name") != expected.group("name"):
        raise ProtocolCompatibilityError(
            "unsupported-schema", field, f"expected {expected_schema}, received {schema!r}"
        )
    major = int(match.group("major"))
    expected_major = int(expected.group("major"))
    if major != expected_major:
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            field,
            f"supported major is {expected_major}, received {major}",
        )
    return major


def protocol_major(value: Any, field: str = "protocol_version") -> int:
    version = _bounded_text(value, field)
    try:
        major_text, *minor = version.split(".", 1)
        major = int(major_text)
        if major < 0 or (minor and (not minor[0] or not minor[0].isdigit())):
            raise ValueError
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-protocol-version",
            field,
            "must be a numeric major or major.minor version",
        ) from exc
    return major


def validate_jobs_protocol(value: Any) -> int:
    major = protocol_major(value)
    if major != JOBS_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "protocol_version",
            f"supported major is {JOBS_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def _required_fields(value: dict[str, Any], required: tuple[str, ...]) -> None:
    missing = [field for field in required if field not in value]
    if missing:
        raise FederationValidationError("missing-field", missing[0], "is required")


def _sequence(value: Any, field: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise FederationValidationError("invalid-array", field, "must be an array")
    return tuple(value)


def validate_job_transition(current: JobStatus | str, target: JobStatus | str) -> JobStatus:
    current_status = _enum(current, JobStatus, "current_status")
    target_status = _enum(target, JobStatus, "target_status")
    if target_status == current_status:
        return target_status
    if target_status not in _ALLOWED_JOB_TRANSITIONS[current_status]:
        raise FederationValidationError(
            "invalid-job-transition",
            "status",
            f"cannot transition from {current_status.value} to {target_status.value}",
        )
    return target_status


def validate_attempt_transition(
    current: AttemptStatus | str, target: AttemptStatus | str
) -> AttemptStatus:
    current_status = _enum(current, AttemptStatus, "current_status")
    target_status = _enum(target, AttemptStatus, "target_status")
    if target_status == current_status:
        return target_status
    if target_status not in _ALLOWED_ATTEMPT_TRANSITIONS[current_status]:
        raise FederationValidationError(
            "invalid-attempt-transition",
            "status",
            f"cannot transition from {current_status.value} to {target_status.value}",
        )
    return target_status


@dataclass(frozen=True)
class CapabilityRequirement:
    SCHEMA: ClassVar[str] = CAPABILITY_REQUIREMENT_SCHEMA

    capability_type: str
    protocol: str
    protocol_version: str
    requirements: dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("capability_type", "protocol", "protocol_version"):
            object.__setattr__(self, name, _bounded_text(getattr(self, name), name))
        protocol_major(self.protocol_version, "protocol_version")
        object.__setattr__(
            self, "requirements", _json_object(self.requirements, "requirements")
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "capability", "must be an object"
            )
        _required_fields(
            value,
            (
                "schema",
                "capability_type",
                "protocol",
                "protocol_version",
                "requirements",
            ),
        )
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(
            capability_type=value["capability_type"],
            protocol=value["protocol"],
            protocol_version=value["protocol_version"],
            requirements=value["requirements"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "requirements": self.requirements,
        }


@dataclass(frozen=True)
class ArtifactReference:
    SCHEMA: ClassVar[str] = ARTIFACT_REFERENCE_SCHEMA

    reference_id: str
    session_id: str
    schema_name: str
    media_type: str
    content_hash: str | None = None
    size_bytes: int | None = None

    def __post_init__(self) -> None:
        for name in ("reference_id", "session_id", "schema_name", "media_type"):
            object.__setattr__(self, name, _bounded_text(getattr(self, name), name))
        content_hash = _optional_text(self.content_hash, "content_hash")
        if content_hash is not None and _HASH_PATTERN.fullmatch(content_hash) is None:
            raise FederationValidationError(
                "invalid-content-hash",
                "content_hash",
                "must be a lowercase sha256:<64 hex> digest",
            )
        object.__setattr__(self, "content_hash", content_hash)
        if self.size_bytes is not None:
            object.__setattr__(
                self, "size_bytes", _non_negative_int(self.size_bytes, "size_bytes")
            )
        if (self.content_hash is None) != (self.size_bytes is None):
            raise FederationValidationError(
                "incomplete-artifact-identity",
                "content_hash",
                "content_hash and size_bytes must be supplied together",
            )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "artifact_reference", "must be an object"
            )
        _required_fields(
            value,
            (
                "schema",
                "reference_id",
                "session_id",
                "schema_name",
                "media_type",
            ),
        )
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(
            reference_id=value["reference_id"],
            session_id=value["session_id"],
            schema_name=value["schema_name"],
            media_type=value["media_type"],
            content_hash=value.get("content_hash"),
            size_bytes=value.get("size_bytes"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "reference_id": self.reference_id,
            "session_id": self.session_id,
            "schema_name": self.schema_name,
            "media_type": self.media_type,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class RetryPolicy:
    SCHEMA: ClassVar[str] = RETRY_POLICY_SCHEMA

    max_attempts: int
    backoff_seconds: tuple[int, ...]
    retryable_error_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "max_attempts",
            _positive_int(self.max_attempts, "max_attempts", maximum=MAX_ATTEMPTS),
        )
        backoff = tuple(
            _non_negative_int(value, f"backoff_seconds[{index}]")
            for index, value in enumerate(_sequence(self.backoff_seconds, "backoff_seconds"))
        )
        if len(backoff) != self.max_attempts - 1:
            raise FederationValidationError(
                "invalid-backoff-count",
                "backoff_seconds",
                "must contain exactly one delay for each possible retry",
            )
        if any(value > MAX_TIMEOUT_SECONDS for value in backoff):
            raise FederationValidationError(
                "backoff-too-large",
                "backoff_seconds",
                f"each delay must not exceed {MAX_TIMEOUT_SECONDS}",
            )
        if tuple(sorted(backoff)) != backoff:
            raise FederationValidationError(
                "nonmonotonic-backoff",
                "backoff_seconds",
                "delays must be monotonically non-decreasing",
            )
        object.__setattr__(self, "backoff_seconds", backoff)

        error_codes = tuple(
            _bounded_text(code, f"retryable_error_codes[{index}]")
            for index, code in enumerate(
                _sequence(self.retryable_error_codes, "retryable_error_codes")
            )
        )
        if len(set(error_codes)) != len(error_codes):
            raise FederationValidationError(
                "duplicate-error-code",
                "retryable_error_codes",
                "must not contain duplicates",
            )
        object.__setattr__(self, "retryable_error_codes", error_codes)

    def allows_retry(self, *, attempt_number: int, error_code: str) -> bool:
        attempt = _positive_int(
            attempt_number, "attempt_number", maximum=MAX_ATTEMPTS
        )
        code = _bounded_text(error_code, "error_code")
        return attempt < self.max_attempts and code in self.retryable_error_codes

    def delay_after(self, attempt_number: int) -> int:
        attempt = _positive_int(
            attempt_number, "attempt_number", maximum=MAX_ATTEMPTS
        )
        if attempt >= self.max_attempts:
            raise FederationValidationError(
                "retry-exhausted",
                "attempt_number",
                "no retry delay exists after the final attempt",
            )
        return self.backoff_seconds[attempt - 1]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "retry_policy", "must be an object"
            )
        _required_fields(
            value,
            ("schema", "max_attempts", "backoff_seconds", "retryable_error_codes"),
        )
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(
            max_attempts=value["max_attempts"],
            backoff_seconds=_sequence(value["backoff_seconds"], "backoff_seconds"),
            retryable_error_codes=_sequence(
                value["retryable_error_codes"], "retryable_error_codes"
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "max_attempts": self.max_attempts,
            "backoff_seconds": list(self.backoff_seconds),
            "retryable_error_codes": list(self.retryable_error_codes),
        }


@dataclass(frozen=True)
class TimeoutPolicy:
    SCHEMA: ClassVar[str] = TIMEOUT_POLICY_SCHEMA

    overall_timeout_seconds: int
    queue_timeout_seconds: int
    start_timeout_seconds: int
    run_timeout_seconds: int
    cancellation_grace_seconds: int

    def __post_init__(self) -> None:
        for name in (
            "overall_timeout_seconds",
            "queue_timeout_seconds",
            "start_timeout_seconds",
            "run_timeout_seconds",
            "cancellation_grace_seconds",
        ):
            object.__setattr__(
                self,
                name,
                _positive_int(
                    getattr(self, name), name, maximum=MAX_TIMEOUT_SECONDS
                ),
            )
        if self.overall_timeout_seconds < max(
            self.queue_timeout_seconds,
            self.start_timeout_seconds,
            self.run_timeout_seconds,
            self.cancellation_grace_seconds,
        ):
            raise FederationValidationError(
                "invalid-overall-timeout",
                "overall_timeout_seconds",
                "must be at least each individual timeout",
            )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "timeout_policy", "must be an object"
            )
        required = (
            "schema",
            "overall_timeout_seconds",
            "queue_timeout_seconds",
            "start_timeout_seconds",
            "run_timeout_seconds",
            "cancellation_grace_seconds",
        )
        _required_fields(value, required)
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(**{name: value[name] for name in required if name != "schema"})

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "overall_timeout_seconds": self.overall_timeout_seconds,
            "queue_timeout_seconds": self.queue_timeout_seconds,
            "start_timeout_seconds": self.start_timeout_seconds,
            "run_timeout_seconds": self.run_timeout_seconds,
            "cancellation_grace_seconds": self.cancellation_grace_seconds,
        }


@dataclass(frozen=True)
class JobAttempt:
    SCHEMA: ClassVar[str] = ATTEMPT_SCHEMA

    attempt_id: str
    attempt_number: int
    status: AttemptStatus
    error_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attempt_id", _bounded_text(self.attempt_id, "attempt_id"))
        object.__setattr__(
            self,
            "attempt_number",
            _positive_int(
                self.attempt_number, "attempt_number", maximum=MAX_ATTEMPTS
            ),
        )
        object.__setattr__(self, "status", _enum(self.status, AttemptStatus, "status"))
        object.__setattr__(
            self, "error_code", _optional_text(self.error_code, "error_code")
        )
        if self.error_code is not None and self.status not in {
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }:
            raise FederationValidationError(
                "unexpected-error-code",
                "error_code",
                "is only valid for failed, timed-out, or lost attempts",
            )

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_ATTEMPT_STATUSES

    def transition_to(
        self, target: AttemptStatus | str, *, error_code: str | None = None
    ) -> Self:
        next_status = validate_attempt_transition(self.status, target)
        next_error = error_code
        if next_status not in {
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }:
            next_error = None
        return replace(self, status=next_status, error_code=next_error)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "attempt", "must be an object"
            )
        _required_fields(value, ("schema", "attempt_id", "attempt_number", "status"))
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(
            attempt_id=value["attempt_id"],
            attempt_number=value["attempt_number"],
            status=value["status"],
            error_code=value.get("error_code"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "status": self.status.value,
            "error_code": self.error_code,
        }


@dataclass(frozen=True)
class JobContract:
    """One validated job request and its deterministic logical state snapshot."""

    SCHEMA: ClassVar[str] = JOB_SCHEMA

    job_id: str
    session_id: str
    request_id: str
    idempotency_key: str
    capability: CapabilityRequirement
    inputs: tuple[ArtifactReference, ...]
    outputs: tuple[ArtifactReference, ...]
    retry_policy: RetryPolicy
    timeout_policy: TimeoutPolicy
    status: JobStatus = JobStatus.SUBMITTED
    attempts: tuple[JobAttempt, ...] = ()
    cancellation_reason: str | None = None
    protocol_version: str = JOBS_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in ("job_id", "session_id", "request_id", "idempotency_key"):
            object.__setattr__(self, name, _bounded_text(getattr(self, name), name))
        validate_jobs_protocol(self.protocol_version)
        if not isinstance(self.capability, CapabilityRequirement):
            object.__setattr__(
                self,
                "capability",
                CapabilityRequirement.from_dict(self.capability),
            )
        object.__setattr__(self, "status", _enum(self.status, JobStatus, "status"))
        object.__setattr__(
            self,
            "cancellation_reason",
            _optional_text(self.cancellation_reason, "cancellation_reason"),
        )

        inputs = tuple(
            item if isinstance(item, ArtifactReference) else ArtifactReference.from_dict(item)
            for item in _sequence(self.inputs, "inputs")
        )
        outputs = tuple(
            item if isinstance(item, ArtifactReference) else ArtifactReference.from_dict(item)
            for item in _sequence(self.outputs, "outputs")
        )
        if len(inputs) > MAX_REFERENCE_COUNT or len(outputs) > MAX_REFERENCE_COUNT:
            raise FederationValidationError(
                "too-many-references",
                "inputs",
                f"inputs and outputs are each limited to {MAX_REFERENCE_COUNT}",
            )
        for field, references in (("inputs", inputs), ("outputs", outputs)):
            ids = [reference.reference_id for reference in references]
            if len(ids) != len(set(ids)):
                raise FederationValidationError(
                    "duplicate-reference", field, "reference_id values must be unique"
                )
            for reference in references:
                if reference.session_id != self.session_id:
                    raise FederationValidationError(
                        "cross-session-reference",
                        field,
                        "artifact references must belong to the job session",
                    )
        if set(item.reference_id for item in inputs).intersection(
            item.reference_id for item in outputs
        ):
            raise FederationValidationError(
                "input-output-reference-conflict",
                "outputs",
                "an artifact reference cannot be both input and output",
            )
        object.__setattr__(self, "inputs", inputs)
        object.__setattr__(self, "outputs", outputs)

        if not isinstance(self.retry_policy, RetryPolicy):
            object.__setattr__(
                self, "retry_policy", RetryPolicy.from_dict(self.retry_policy)
            )
        if not isinstance(self.timeout_policy, TimeoutPolicy):
            object.__setattr__(
                self, "timeout_policy", TimeoutPolicy.from_dict(self.timeout_policy)
            )

        attempts = tuple(
            item if isinstance(item, JobAttempt) else JobAttempt.from_dict(item)
            for item in _sequence(self.attempts, "attempts")
        )
        expected_numbers = tuple(range(1, len(attempts) + 1))
        actual_numbers = tuple(item.attempt_number for item in attempts)
        if actual_numbers != expected_numbers:
            raise FederationValidationError(
                "nonmonotonic-attempt-generation",
                "attempts",
                "attempt numbers must be contiguous and start at 1",
            )
        if len(attempts) > self.retry_policy.max_attempts:
            raise FederationValidationError(
                "too-many-attempts",
                "attempts",
                "attempt count exceeds retry policy",
            )
        attempt_ids = [attempt.attempt_id for attempt in attempts]
        if len(attempt_ids) != len(set(attempt_ids)):
            raise FederationValidationError(
                "duplicate-attempt-id", "attempts", "attempt_id values must be unique"
            )
        active_attempts = [
            attempt for attempt in attempts if attempt.status in _ACTIVE_ATTEMPT_STATUSES
        ]
        if len(active_attempts) > 1:
            raise FederationValidationError(
                "multiple-active-attempts",
                "attempts",
                "at most one attempt may be active",
            )
        successful_attempts = [
            attempt for attempt in attempts if attempt.status == AttemptStatus.SUCCEEDED
        ]
        if len(successful_attempts) > 1:
            raise FederationValidationError(
                "multiple-successful-attempts",
                "attempts",
                "at most one attempt may succeed",
            )
        if self.status == JobStatus.SUBMITTED and attempts:
            raise FederationValidationError(
                "attempt-before-queue",
                "attempts",
                "submitted jobs cannot already contain attempts",
            )
        if self.status == JobStatus.ACTIVE and len(active_attempts) != 1:
            raise FederationValidationError(
                "active-job-attempt-mismatch",
                "attempts",
                "an active job must have exactly one active attempt",
            )
        if self.status == JobStatus.RETRY_WAIT:
            if active_attempts or not attempts:
                raise FederationValidationError(
                    "retry-wait-attempt-mismatch",
                    "attempts",
                    "retry-wait requires completed attempts and no active attempt",
                )
            if len(attempts) >= self.retry_policy.max_attempts:
                raise FederationValidationError(
                    "retry-exhausted",
                    "attempts",
                    "retry-wait is invalid after the final allowed attempt",
                )
        if self.status == JobStatus.SUCCEEDED and len(successful_attempts) != 1:
            raise FederationValidationError(
                "successful-job-attempt-mismatch",
                "attempts",
                "a succeeded job must contain exactly one succeeded attempt",
            )
        if self.status != JobStatus.SUCCEEDED and successful_attempts:
            raise FederationValidationError(
                "premature-successful-attempt",
                "attempts",
                "a successful attempt requires succeeded job status",
            )
        if self.status in _TERMINAL_JOB_STATUSES and active_attempts:
            raise FederationValidationError(
                "terminal-job-active-attempt",
                "attempts",
                "terminal jobs cannot contain an active attempt",
            )
        if self.cancellation_reason is not None and self.status not in {
            JobStatus.CANCELLATION_REQUESTED,
            JobStatus.CANCELLED,
        }:
            raise FederationValidationError(
                "unexpected-cancellation-reason",
                "cancellation_reason",
                "is only valid after cancellation has been requested",
            )
        object.__setattr__(self, "attempts", attempts)

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_JOB_STATUSES

    def transition_to(
        self, target: JobStatus | str, *, cancellation_reason: str | None = None
    ) -> Self:
        next_status = validate_job_transition(self.status, target)
        next_reason = self.cancellation_reason
        if next_status == JobStatus.CANCELLATION_REQUESTED:
            if cancellation_reason is None and self.status == JobStatus.CANCELLATION_REQUESTED:
                if next_reason is None:
                    raise FederationValidationError(
                        "missing-cancellation-reason",
                        "cancellation_reason",
                        "is required when cancellation is requested",
                    )
            else:
                next_reason = _bounded_text(cancellation_reason, "cancellation_reason")
        elif next_status not in {
            JobStatus.CANCELLATION_REQUESTED,
            JobStatus.CANCELLED,
        }:
            next_reason = None
        return replace(self, status=next_status, cancellation_reason=next_reason)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "job", "must be an object")
        required = (
            "schema",
            "protocol_version",
            "job_id",
            "session_id",
            "request_id",
            "idempotency_key",
            "capability",
            "inputs",
            "outputs",
            "retry_policy",
            "timeout_policy",
            "status",
            "attempts",
        )
        _required_fields(value, required)
        _schema_major(value["schema"], cls.SCHEMA)
        return cls(
            job_id=value["job_id"],
            session_id=value["session_id"],
            request_id=value["request_id"],
            idempotency_key=value["idempotency_key"],
            capability=CapabilityRequirement.from_dict(value["capability"]),
            inputs=tuple(
                ArtifactReference.from_dict(item)
                for item in _sequence(value["inputs"], "inputs")
            ),
            outputs=tuple(
                ArtifactReference.from_dict(item)
                for item in _sequence(value["outputs"], "outputs")
            ),
            retry_policy=RetryPolicy.from_dict(value["retry_policy"]),
            timeout_policy=TimeoutPolicy.from_dict(value["timeout_policy"]),
            status=value["status"],
            attempts=tuple(
                JobAttempt.from_dict(item)
                for item in _sequence(value["attempts"], "attempts")
            ),
            cancellation_reason=value.get("cancellation_reason"),
            protocol_version=value["protocol_version"],
        )

    @classmethod
    def from_json(
        cls, value: str | bytes, *, max_bytes: int = MAX_JOB_MESSAGE_BYTES
    ) -> Self:
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        elif isinstance(value, bytes):
            encoded = value
        else:
            raise FederationValidationError(
                "malformed-message", "job", "must be text or UTF-8 bytes"
            )
        if len(encoded) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "job", f"exceeds {max_bytes} bytes"
            )
        try:
            parsed = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "malformed-message", "job", "must be valid UTF-8 JSON"
            ) from exc
        return cls.from_dict(parsed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol_version": self.protocol_version,
            "job_id": self.job_id,
            "session_id": self.session_id,
            "request_id": self.request_id,
            "idempotency_key": self.idempotency_key,
            "capability": self.capability.to_dict(),
            "inputs": [item.to_dict() for item in self.inputs],
            "outputs": [item.to_dict() for item in self.outputs],
            "retry_policy": self.retry_policy.to_dict(),
            "timeout_policy": self.timeout_policy.to_dict(),
            "status": self.status.value,
            "attempts": [item.to_dict() for item in self.attempts],
            "cancellation_reason": self.cancellation_reason,
        }

    def to_json(self, *, max_bytes: int = MAX_JOB_MESSAGE_BYTES) -> str:
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
                "invalid-json", "job", "contains non-JSON values"
            ) from exc
        if len(encoded.encode("utf-8")) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "job", f"exceeds {max_bytes} bytes"
            )
        return encoded

"""F7.4 transport-neutral capability dispatch and worker execution.

Ownership is granted only by the F7.3 job store. This module carries that
existing attempt and lease to a registered worker, validates every binding,
and durably suppresses duplicate execution. It grants no storage authority.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Protocol, Self

from catalog.federation.errors import FederationValidationError, ProtocolCompatibilityError

from .job_store import DurableJobSnapshot, SQLiteJobStore
from .jobs import AttemptStatus, JobAttempt, JobContract, JobStatus, protocol_major

DISPATCH_PROTOCOL = "msh-capability-dispatch"
DISPATCH_PROTOCOL_VERSION = "1.0"
DISPATCH_PROTOCOL_MAJOR = 1
DISPATCH_REQUEST_SCHEMA = "msh.capability-dispatch.v1"
DISPATCH_EVENT_SCHEMA = "msh.capability-dispatch-event.v1"
DISPATCH_RESPONSE_SCHEMA = "msh.capability-dispatch-response.v1"
EXECUTION_RESULT_SCHEMA = "msh.capability-execution-result.v1"
WORKER_REGISTRATION_SCHEMA = "msh.capability-worker-registration.v1"
MAX_DISPATCH_BYTES = 524_288
MAX_RESULT_BYTES = 65_536


class DispatchState(str, Enum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    REJECTED = "rejected"


def _text(value: Any, field: str, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text", field, "must be non-empty trimmed text without controls"
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "text-too-large", field, f"must not exceed {maximum} UTF-8 bytes"
        )
    return value


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
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
            "invalid-timestamp", field, "must be a UTC timestamp"
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _json(value: Any) -> str:
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
            "invalid-json", "dispatch", "must contain finite JSON values"
        ) from exc


def _object(value: Any, field: str, maximum: int = MAX_RESULT_BYTES) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise FederationValidationError(
            "invalid-object", field, "must be an object with string keys"
        )
    if len(_json(value).encode("utf-8")) > maximum:
        raise FederationValidationError(
            "object-too-large", field, f"must not exceed {maximum} UTF-8 bytes"
        )
    return dict(value)


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    expected_name, expected_major = expected.rsplit(".v", 1)
    if ".v" not in actual:
        raise ProtocolCompatibilityError("unsupported-schema", "schema", f"expected {expected}")
    actual_name, actual_major = actual.rsplit(".v", 1)
    if actual_name != expected_name or not actual_major.isdigit():
        raise ProtocolCompatibilityError(
            "unsupported-schema", "schema", f"expected {expected}, received {actual!r}"
        )
    if int(actual_major) != int(expected_major):
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            "schema",
            f"supported major is {expected_major}, received {actual_major}",
        )


def _dispatch_protocol(value: Any) -> str:
    version = _text(value, "protocol_version")
    major = protocol_major(version)
    if major != DISPATCH_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-dispatch-protocol-major",
            "protocol_version",
            f"supported major is {DISPATCH_PROTOCOL_MAJOR}, received {major}",
        )
    return version


def _requirements_satisfied(required: Any, offered: Any) -> bool:
    if isinstance(required, dict):
        return isinstance(offered, dict) and all(
            key in offered and _requirements_satisfied(item, offered[key])
            for key, item in required.items()
        )
    if isinstance(required, list):
        return isinstance(offered, list) and all(item in offered for item in required)
    return required == offered


def _execution_job(snapshot: DurableJobSnapshot) -> JobContract:
    ownership = snapshot.ownership
    if ownership is None:
        raise FederationValidationError(
            "job-not-owned", "job_id", "dispatch requires active ownership"
        )
    attempts = tuple(
        replace(attempt, status=AttemptStatus.ASSIGNED, error_code=None)
        if attempt.attempt_id == ownership.attempt_id
        else attempt
        for attempt in snapshot.job.attempts
    )
    if not any(attempt.attempt_id == ownership.attempt_id for attempt in attempts):
        raise FederationValidationError(
            "attempt-not-found", "attempt_id", "ownership attempt is missing"
        )
    return replace(snapshot.job, status=JobStatus.ACTIVE, attempts=attempts)


@dataclass(frozen=True)
class WorkerRegistration:
    session_id: str
    node_id: str
    provider_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    attributes: dict[str, Any]
    SCHEMA: ClassVar[str] = WORKER_REGISTRATION_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "session_id",
            "node_id",
            "provider_id",
            "capability_type",
            "protocol",
            "protocol_version",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        protocol_major(self.protocol_version)
        object.__setattr__(self, "attributes", _object(self.attributes, "attributes"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "node_id": self.node_id,
            "provider_id": self.provider_id,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "attributes": self.attributes,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "registration", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "session_id",
            "node_id",
            "provider_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "attributes",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(**{name: value[name] for name in names})


@dataclass(frozen=True)
class DispatchRequest:
    dispatch_id: str
    session_id: str
    coordinator_node_id: str
    target_node_id: str
    provider_id: str
    job: JobContract
    attempt_id: str
    attempt_number: int
    lease_id: str
    lease_generation: int
    lease_expires_at: datetime
    sent_at: datetime
    protocol_version: str = DISPATCH_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = DISPATCH_REQUEST_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "dispatch_id",
            "session_id",
            "coordinator_node_id",
            "target_node_id",
            "provider_id",
            "attempt_id",
            "lease_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "protocol_version", _dispatch_protocol(self.protocol_version))
        if not isinstance(self.job, JobContract):
            object.__setattr__(self, "job", JobContract.from_dict(self.job))
        object.__setattr__(self, "attempt_number", _positive(self.attempt_number, "attempt_number"))
        object.__setattr__(
            self, "lease_generation", _positive(self.lease_generation, "lease_generation")
        )
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at, "lease_expires_at"))
        object.__setattr__(self, "sent_at", _utc(self.sent_at, "sent_at"))
        if self.job.session_id != self.session_id:
            raise FederationValidationError(
                "dispatch-session-mismatch", "session_id", "job belongs to another session"
            )
        if self.job.status != JobStatus.ACTIVE:
            raise FederationValidationError(
                "dispatch-job-not-active", "job.status", "job must be active"
            )
        active = tuple(attempt for attempt in self.job.attempts if not attempt.terminal)
        if (
            len(active) != 1
            or active[0].attempt_id != self.attempt_id
            or active[0].attempt_number != self.attempt_number
        ):
            raise FederationValidationError(
                "dispatch-attempt-mismatch",
                "attempt_id",
                "dispatch must identify the only active attempt",
            )
        if self.lease_generation != self.attempt_number:
            raise FederationValidationError(
                "dispatch-lease-generation-mismatch",
                "lease_generation",
                "lease and attempt generations must match",
            )
        if self.sent_at >= self.lease_expires_at:
            raise FederationValidationError(
                "dispatch-after-lease", "sent_at", "dispatch must precede lease expiry"
            )
        if len(self.to_json().encode("utf-8")) > MAX_DISPATCH_BYTES:
            raise FederationValidationError(
                "dispatch-too-large", "dispatch", f"must not exceed {MAX_DISPATCH_BYTES} bytes"
            )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: DurableJobSnapshot,
        *,
        dispatch_id: str,
        target_node_id: str,
        sent_at: datetime,
    ) -> Self:
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned", "job_id", "dispatch requires active ownership"
            )
        return cls(
            dispatch_id=dispatch_id,
            session_id=snapshot.job.session_id,
            coordinator_node_id=ownership.granted_by_coordinator_id,
            target_node_id=target_node_id,
            provider_id=ownership.owner_provider_id,
            job=_execution_job(snapshot),
            attempt_id=ownership.attempt_id,
            attempt_number=ownership.attempt_number,
            lease_id=ownership.lease_id,
            lease_generation=ownership.lease_generation,
            lease_expires_at=ownership.lease_expires_at,
            sent_at=sent_at,
        )

    def identity_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": DISPATCH_PROTOCOL,
            "protocol_version": self.protocol_version,
            "dispatch_id": self.dispatch_id,
            "session_id": self.session_id,
            "coordinator_node_id": self.coordinator_node_id,
            "target_node_id": self.target_node_id,
            "provider_id": self.provider_id,
            "job": self.job.to_dict(),
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
        }

    def fingerprint(self) -> str:
        return "sha256:" + hashlib.sha256(_json(self.identity_dict()).encode()).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.identity_dict(),
            "lease_expires_at": _stamp(self.lease_expires_at),
            "sent_at": _stamp(self.sent_at),
        }

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "dispatch", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != DISPATCH_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-dispatch-protocol", "protocol", f"expected {DISPATCH_PROTOCOL}"
            )
        names = (
            "dispatch_id",
            "session_id",
            "coordinator_node_id",
            "target_node_id",
            "provider_id",
            "job",
            "attempt_id",
            "attempt_number",
            "lease_id",
            "lease_generation",
            "lease_expires_at",
            "sent_at",
            "protocol_version",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        arguments = {name: value[name] for name in names}
        arguments["job"] = JobContract.from_dict(arguments["job"])
        return cls(**arguments)

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        try:
            return cls.from_dict(json.loads(value))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError("invalid-json", "dispatch", "must be valid JSON") from exc


@dataclass(frozen=True)
class DispatchEvent:
    sequence: int
    state: DispatchState
    occurred_at: datetime
    reason_code: str | None = None
    details: dict[str, Any] | None = None
    SCHEMA: ClassVar[str] = DISPATCH_EVENT_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "sequence", _positive(self.sequence, "sequence"))
        try:
            object.__setattr__(self, "state", DispatchState(self.state))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "state", "unknown dispatch state") from exc
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        if self.state in {DispatchState.FAILED, DispatchState.REJECTED}:
            object.__setattr__(self, "reason_code", _text(self.reason_code, "reason_code", 128))
        elif self.reason_code is not None:
            raise FederationValidationError(
                "unexpected-reason-code", "reason_code", "only failed or rejected events use it"
            )
        if self.details is not None:
            object.__setattr__(self, "details", _object(self.details, "details"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "sequence": self.sequence,
            "state": self.state.value,
            "occurred_at": _stamp(self.occurred_at),
            "reason_code": self.reason_code,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "event", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        return cls(
            sequence=value.get("sequence"),
            state=value.get("state"),
            occurred_at=value.get("occurred_at"),
            reason_code=value.get("reason_code"),
            details=value.get("details"),
        )


@dataclass(frozen=True)
class DispatchResponse:
    dispatch_id: str
    session_id: str
    job_id: str
    provider_id: str
    attempt_id: str
    events: tuple[DispatchEvent, ...]
    protocol_version: str = DISPATCH_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = DISPATCH_RESPONSE_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "dispatch_id",
            "session_id",
            "job_id",
            "provider_id",
            "attempt_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "protocol_version", _dispatch_protocol(self.protocol_version))
        events = tuple(
            event if isinstance(event, DispatchEvent) else DispatchEvent.from_dict(event)
            for event in self.events
        )
        if not events or len(events) > 8:
            raise FederationValidationError(
                "invalid-dispatch-event-count", "events", "must contain one to eight events"
            )
        if tuple(event.sequence for event in events) != tuple(range(1, len(events) + 1)):
            raise FederationValidationError(
                "dispatch-event-gap", "events", "sequences must be contiguous from one"
            )
        states = tuple(event.state for event in events)
        allowed = {
            (DispatchState.REJECTED,),
            (DispatchState.ACCEPTED,),
            (DispatchState.ACCEPTED, DispatchState.RUNNING),
            (DispatchState.ACCEPTED, DispatchState.RUNNING, DispatchState.SUCCEEDED),
            (DispatchState.ACCEPTED, DispatchState.RUNNING, DispatchState.FAILED),
        }
        if states not in allowed:
            raise FederationValidationError(
                "invalid-dispatch-event-order",
                "events",
                "must follow accepted, running, then one terminal event",
            )
        if tuple(sorted(event.occurred_at for event in events)) != tuple(
            event.occurred_at for event in events
        ):
            raise FederationValidationError(
                "nonmonotonic-dispatch-time", "events", "times must be monotonic"
            )
        object.__setattr__(self, "events", events)
        if len(self.to_json().encode("utf-8")) > MAX_DISPATCH_BYTES:
            raise FederationValidationError(
                "dispatch-response-too-large", "response", "response exceeds the bound"
            )

    @property
    def state(self) -> DispatchState:
        return self.events[-1].state

    @property
    def terminal(self) -> bool:
        return self.state in {
            DispatchState.SUCCEEDED,
            DispatchState.FAILED,
            DispatchState.REJECTED,
        }

    def validate_for(self, request: DispatchRequest) -> Self:
        if (
            self.dispatch_id,
            self.session_id,
            self.job_id,
            self.provider_id,
            self.attempt_id,
        ) != (
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
        ):
            raise FederationValidationError(
                "dispatch-response-mismatch", "response", "response identifies other work"
            )
        if any(
            event.state is not DispatchState.REJECTED
            and event.occurred_at > request.lease_expires_at
            for event in self.events
        ):
            raise FederationValidationError(
                "dispatch-event-after-lease", "events", "work occurred after lease expiry"
            )
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": DISPATCH_PROTOCOL,
            "protocol_version": self.protocol_version,
            "dispatch_id": self.dispatch_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "provider_id": self.provider_id,
            "attempt_id": self.attempt_id,
            "events": [event.to_dict() for event in self.events],
        }

    def to_json(self) -> str:
        return _json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "response", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != DISPATCH_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-dispatch-protocol", "protocol", f"expected {DISPATCH_PROTOCOL}"
            )
        events = value.get("events")
        if not isinstance(events, list):
            raise FederationValidationError("invalid-array", "events", "must be an array")
        return cls(
            dispatch_id=value.get("dispatch_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            provider_id=value.get("provider_id"),
            attempt_id=value.get("attempt_id"),
            events=tuple(DispatchEvent.from_dict(event) for event in events),
            protocol_version=value.get("protocol_version"),
        )

    @classmethod
    def from_json(cls, value: str | bytes) -> Self:
        try:
            return cls.from_dict(json.loads(value))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError("invalid-json", "response", "must be valid JSON") from exc


@dataclass(frozen=True)
class ExecutionResult:
    succeeded: bool
    details: dict[str, Any]
    reason_code: str | None = None
    SCHEMA: ClassVar[str] = EXECUTION_RESULT_SCHEMA

    def __post_init__(self) -> None:
        if not isinstance(self.succeeded, bool):
            raise FederationValidationError("invalid-boolean", "succeeded", "must be boolean")
        object.__setattr__(self, "details", _object(self.details, "details"))
        if self.succeeded and self.reason_code is not None:
            raise FederationValidationError(
                "unexpected-reason-code", "reason_code", "successful work cannot fail"
            )
        if not self.succeeded:
            object.__setattr__(self, "reason_code", _text(self.reason_code, "reason_code", 128))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "succeeded": self.succeeded,
            "details": self.details,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "result", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        return cls(value.get("succeeded"), value.get("details"), value.get("reason_code"))


class CapabilityHandler(Protocol):
    async def execute(self, job: JobContract) -> ExecutionResult: ...


class DispatchTransport(Protocol):
    async def request(
        self, *, target_node_id: str, request: DispatchRequest
    ) -> DispatchResponse: ...


class SQLiteDispatchInbox:
    """Durable duplicate suppression; a reserved dispatch never starts twice."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS capability_dispatch_receipts (
                    session_id TEXT NOT NULL,
                    dispatch_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    request_json TEXT NOT NULL CHECK(json_valid(request_json)),
                    response_json TEXT NOT NULL CHECK(json_valid(response_json)),
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, dispatch_id)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _decode(row: sqlite3.Row) -> DispatchResponse:
        request = DispatchRequest.from_json(row["request_json"])
        response = DispatchResponse.from_json(row["response_json"]).validate_for(request)
        if request.fingerprint() != row["fingerprint"]:
            raise FederationValidationError(
                "dispatch-receipt-row-mismatch", "request_json", "fingerprint differs"
            )
        if (
            response.session_id,
            response.dispatch_id,
            response.provider_id,
            response.job_id,
            response.attempt_id,
            response.state.value,
        ) != (
            row["session_id"],
            row["dispatch_id"],
            row["provider_id"],
            row["job_id"],
            row["attempt_id"],
            row["state"],
        ):
            raise FederationValidationError(
                "dispatch-receipt-row-mismatch", "response_json", "indexed metadata differs"
            )
        return response

    def reserve(
        self, request: DispatchRequest, response: DispatchResponse, *, now: datetime
    ) -> DispatchResponse | None:
        now = _utc(now, "now")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM capability_dispatch_receipts WHERE session_id=? AND dispatch_id=?",
                    (request.session_id, request.dispatch_id),
                ).fetchone()
                if row is not None:
                    if row["fingerprint"] != request.fingerprint():
                        raise FederationValidationError(
                            "dispatch-id-conflict", "dispatch_id", "ID identifies different work"
                        )
                    replay = self._decode(row)
                    connection.commit()
                    return replay
                connection.execute(
                    """INSERT INTO capability_dispatch_receipts
                       (session_id, dispatch_id, fingerprint, provider_id, job_id,
                        attempt_id, request_json, response_json, state, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        request.session_id,
                        request.dispatch_id,
                        request.fingerprint(),
                        request.provider_id,
                        request.job.job_id,
                        request.attempt_id,
                        request.to_json(),
                        response.to_json(),
                        response.state.value,
                        _stamp(now),
                        _stamp(now),
                    ),
                )
                connection.commit()
                return None
            except Exception:
                connection.rollback()
                raise

    def finish(
        self, request: DispatchRequest, response: DispatchResponse, *, now: datetime
    ) -> DispatchResponse:
        now = _utc(now, "now")
        response.validate_for(request)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT * FROM capability_dispatch_receipts WHERE session_id=? AND dispatch_id=?",
                    (request.session_id, request.dispatch_id),
                ).fetchone()
                if row is None or row["fingerprint"] != request.fingerprint():
                    raise FederationValidationError(
                        "dispatch-receipt-missing", "dispatch_id", "dispatch was not reserved"
                    )
                existing = self._decode(row)
                if existing.terminal:
                    connection.commit()
                    return existing
                connection.execute(
                    """UPDATE capability_dispatch_receipts
                       SET response_json=?, state=?, updated_at=?
                       WHERE session_id=? AND dispatch_id=? AND fingerprint=?""",
                    (
                        response.to_json(),
                        response.state.value,
                        _stamp(now),
                        request.session_id,
                        request.dispatch_id,
                        request.fingerprint(),
                    ),
                )
                connection.commit()
                return response
            except Exception:
                connection.rollback()
                raise

    def response(self, session_id: str, dispatch_id: str) -> DispatchResponse:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM capability_dispatch_receipts WHERE session_id=? AND dispatch_id=?",
                (_text(session_id, "session_id"), _text(dispatch_id, "dispatch_id")),
            ).fetchone()
        if row is None:
            raise FederationValidationError(
                "dispatch-not-found", "dispatch_id", "receipt does not exist"
            )
        return self._decode(row)


class CapabilityWorker:
    def __init__(
        self,
        registration: WorkerRegistration,
        handler: CapabilityHandler,
        inbox: SQLiteDispatchInbox,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self.registration = registration
        self.handler = handler
        self.inbox = inbox
        self.clock = clock

    def _reject(self, request: DispatchRequest, reason: str, now: datetime) -> DispatchResponse:
        return DispatchResponse(
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
            (DispatchEvent(1, DispatchState.REJECTED, now, reason_code=reason),),
        )

    def _reason(
        self,
        request: DispatchRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
        now: datetime,
    ) -> str | None:
        registration = self.registration
        checks = (
            (authenticated_session == request.session_id, "session-mismatch"),
            (authenticated_actor == request.coordinator_node_id, "coordinator-mismatch"),
            (registration.session_id == request.session_id, "worker-session-mismatch"),
            (registration.node_id == request.target_node_id, "target-node-mismatch"),
            (registration.provider_id == request.provider_id, "provider-mismatch"),
            (
                registration.capability_type == request.job.capability.capability_type,
                "capability-type-mismatch",
            ),
            (registration.protocol == request.job.capability.protocol, "capability-protocol-mismatch"),
            (
                protocol_major(registration.protocol_version)
                == protocol_major(request.job.capability.protocol_version),
                "capability-version-mismatch",
            ),
            (
                _requirements_satisfied(
                    request.job.capability.requirements, registration.attributes
                ),
                "capability-requirements-mismatch",
            ),
            (now < request.lease_expires_at, "lease-expired"),
        )
        return next((reason for valid, reason in checks if not valid), None)

    async def handle(
        self,
        request: DispatchRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> DispatchResponse:
        started_at = _utc(self.clock(), "now")
        reason = self._reason(
            request,
            authenticated_actor=authenticated_actor,
            authenticated_session=authenticated_session,
            now=started_at,
        )
        if reason is not None:
            return self._reject(request, reason, started_at)
        running = DispatchResponse(
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
            (
                DispatchEvent(1, DispatchState.ACCEPTED, started_at),
                DispatchEvent(2, DispatchState.RUNNING, started_at),
            ),
        )
        replay = self.inbox.reserve(request, running, now=started_at)
        if replay is not None:
            return replay.validate_for(request)
        try:
            result = await self.handler.execute(request.job)
            if not isinstance(result, ExecutionResult):
                result = ExecutionResult.from_dict(result)
        except Exception:  # noqa: BLE001
            result = ExecutionResult(False, {}, "worker-handler-failed")
        finished_at = max(started_at, _utc(self.clock(), "finished_at"))
        terminal = DispatchEvent(
            3,
            DispatchState.SUCCEEDED if result.succeeded else DispatchState.FAILED,
            finished_at,
            reason_code=result.reason_code,
            details=result.details,
        )
        response = DispatchResponse(
            request.dispatch_id,
            request.session_id,
            request.job.job_id,
            request.provider_id,
            request.attempt_id,
            (*running.events, terminal),
        ).validate_for(request)
        return self.inbox.finish(request, response, now=finished_at)


@dataclass(frozen=True)
class DispatchOutcome:
    response: DispatchResponse
    snapshot: DurableJobSnapshot


class DispatchCoordinator:
    def __init__(
        self,
        store: SQLiteJobStore,
        transport: DispatchTransport,
        *,
        coordinator_node_id: str,
    ) -> None:
        self.store = store
        self.transport = transport
        self.coordinator_node_id = _text(coordinator_node_id, "coordinator_node_id")

    @staticmethod
    def _attempt(snapshot: DurableJobSnapshot, attempt_id: str) -> JobAttempt:
        for attempt in snapshot.job.attempts:
            if attempt.attempt_id == attempt_id:
                return attempt
        raise FederationValidationError("attempt-not-found", "attempt_id", "attempt is missing")

    async def dispatch(
        self,
        job_id: str,
        *,
        dispatch_id: str,
        target_node_id: str,
        now: datetime,
    ) -> DispatchOutcome:
        current = self.store.snapshot(job_id)
        ownership = current.ownership
        now = _utc(now, "now")
        if ownership is None:
            raise FederationValidationError("job-not-owned", "job_id", "job has no owner")
        if ownership.granted_by_coordinator_id != self.coordinator_node_id:
            raise FederationValidationError(
                "dispatch-coordinator-mismatch",
                "coordinator_node_id",
                "only the granting coordinator may dispatch",
            )
        if now >= ownership.lease_expires_at:
            raise FederationValidationError(
                "ownership-lease-expired", "lease_expires_at", "ownership has expired"
            )
        request = DispatchRequest.from_snapshot(
            current,
            dispatch_id=dispatch_id,
            target_node_id=target_node_id,
            sent_at=now,
        )
        response = (
            await self.transport.request(target_node_id=target_node_id, request=request)
        ).validate_for(request)
        for event in response.events:
            attempt = self._attempt(current, request.attempt_id)
            if event.state is DispatchState.ACCEPTED:
                if attempt.status in {AttemptStatus.ACCEPTED, AttemptStatus.RUNNING} or attempt.terminal:
                    continue
                current = self.store.record_attempt_status(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{dispatch_id}:accepted",
                    expected_revision=current.revision,
                    target_status=AttemptStatus.ACCEPTED,
                    now=event.occurred_at,
                ).snapshot
            elif event.state is DispatchState.RUNNING:
                if attempt.status is AttemptStatus.RUNNING or attempt.terminal:
                    continue
                current = self.store.record_attempt_status(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{dispatch_id}:running",
                    expected_revision=current.revision,
                    target_status=AttemptStatus.RUNNING,
                    now=event.occurred_at,
                ).snapshot
            elif event.state is DispatchState.SUCCEEDED:
                current = self.store.complete(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{dispatch_id}:completed",
                    expected_revision=current.revision,
                    terminal_status=AttemptStatus.SUCCEEDED,
                    now=event.occurred_at,
                ).snapshot
            elif event.state in {DispatchState.FAILED, DispatchState.REJECTED}:
                if event.state is DispatchState.REJECTED and event.reason_code == "lease-expired":
                    continue
                current = self.store.complete(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{dispatch_id}:completed",
                    expected_revision=current.revision,
                    terminal_status=AttemptStatus.FAILED,
                    error_code=event.reason_code or "worker-rejected",
                    now=event.occurred_at,
                ).snapshot
        return DispatchOutcome(response, current)


class CallableDispatchTransport:
    """Adapter for any authenticated direct carrier using the same app contract."""

    def __init__(
        self,
        sender: Callable[[str, DispatchRequest], Awaitable[DispatchResponse]],
    ) -> None:
        self.sender = sender

    async def request(
        self, *, target_node_id: str, request: DispatchRequest
    ) -> DispatchResponse:
        return await self.sender(target_node_id, request)

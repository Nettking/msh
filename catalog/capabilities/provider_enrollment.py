"""Durable, explicit enrollment for session-announced capability providers.

Phase F8.1 bridges the existing coordinator capability registry to later F7
runtime activation. An announcement remains metadata only: this module does
not create resource reports, reserve capacity, dispatch jobs, invoke providers,
or grant storage, artifact, or execution authority.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Self

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
)

from .provider_reports import protocol_version_parts

PROVIDER_ENROLLMENT_PROTOCOL = "fcp-provider-enrollment"
PROVIDER_ENROLLMENT_PROTOCOL_VERSION = "1.0"
PROVIDER_ENROLLMENT_PROTOCOL_MAJOR = 1
PROVIDER_ENROLLMENT_SCHEMA = "fcp.provider-enrollment.v1"
PROVIDER_ENROLLMENT_AUDIT_SCHEMA = "fcp.provider-enrollment-audit.v1"
PROVIDER_ENROLLMENT_STORE_SCHEMA_VERSION = 1
MAX_ENROLLMENT_AUDIT_READ = 10_000
MAX_TEXT_BYTES = 512
MAX_REASON_BYTES = 256


class ProviderEnrollmentState(str, Enum):
    """Durable operator decision for one session-scoped capability."""

    PENDING = "pending"
    APPROVED = "approved"
    SUSPENDED = "suspended"
    REVOKED = "revoked"


def _text(value: Any, field: str, *, maximum: int = MAX_TEXT_BYTES) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "text-too-large",
            field,
            f"must not exceed {maximum} UTF-8 bytes",
        )
    return value


def _optional_text(
    value: Any,
    field: str,
    *,
    maximum: int = MAX_TEXT_BYTES,
) -> str | None:
    return None if value is None else _text(value, field, maximum=maximum)


def _reason(value: Any, field: str = "reason_code") -> str:
    reason = _text(value, field, maximum=MAX_REASON_BYTES)
    if contains_secret_material(reason):
        raise FederationValidationError(
            "secret-enrollment-reason",
            field,
            "must not contain credentials or secret material",
        )
    if contains_nonpublic_location(reason):
        raise FederationValidationError(
            "nonpublic-enrollment-reason",
            field,
            "must not expose endpoints, addresses, or backend paths",
        )
    return reason


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be a positive integer",
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                field,
                "must be RFC 3339",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be a UTC timestamp",
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


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
            "invalid-json",
            "value",
            "must contain JSON-compatible finite values",
        ) from exc


def _hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _command_key(value: Any) -> str:
    return _hash(_text(value, "command_id"))


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual:
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


def _validate_enrollment_protocol(value: Any) -> str:
    version = _text(value, "enrollment_protocol_version")
    major, _minor = protocol_version_parts(
        version,
        "enrollment_protocol_version",
    )
    if major != PROVIDER_ENROLLMENT_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-enrollment-protocol-major",
            "enrollment_protocol_version",
            (
                f"supported major is {PROVIDER_ENROLLMENT_PROTOCOL_MAJOR}, "
                f"received {major}"
            ),
        )
    return version


def _announcement_fingerprint(announcement: CapabilityAnnouncement) -> str:
    return _hash(_canonical(announcement.to_dict()))


@dataclass(frozen=True)
class ProviderEnrollmentRecord:
    """Durable approval state for one coordinator-announced capability."""

    SCHEMA: ClassVar[str] = PROVIDER_ENROLLMENT_SCHEMA

    enrollment_id: str
    session_id: str
    capability_id: str
    node_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    protocol_major: int
    state: ProviderEnrollmentState
    revision: int
    announcement_status: CapabilityStatus
    announcement_announced_at: datetime
    announcement_fingerprint: str
    requested_by_node_id: str
    approved_by_node_id: str | None
    reason_code: str | None
    created_at: datetime
    updated_at: datetime
    enrollment_protocol_version: str = PROVIDER_ENROLLMENT_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for field in (
            "enrollment_id",
            "session_id",
            "capability_id",
            "node_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "requested_by_node_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "enrollment_protocol_version",
            _validate_enrollment_protocol(self.enrollment_protocol_version),
        )
        major, _minor = protocol_version_parts(
            self.protocol_version,
            "protocol_version",
        )
        object.__setattr__(
            self,
            "protocol_major",
            _positive(self.protocol_major, "protocol_major"),
        )
        if major != self.protocol_major:
            raise FederationValidationError(
                "protocol-major-mismatch",
                "protocol_major",
                "must match protocol_version",
            )
        try:
            object.__setattr__(
                self,
                "state",
                ProviderEnrollmentState(self.state),
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-enrollment-state",
                "state",
                "unknown provider enrollment state",
            ) from exc
        object.__setattr__(self, "revision", _positive(self.revision, "revision"))
        try:
            object.__setattr__(
                self,
                "announcement_status",
                CapabilityStatus(self.announcement_status),
            )
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-capability-status",
                "announcement_status",
                "unknown capability status",
            ) from exc
        object.__setattr__(
            self,
            "announcement_announced_at",
            _utc(self.announcement_announced_at, "announcement_announced_at"),
        )
        fingerprint = _text(
            self.announcement_fingerprint,
            "announcement_fingerprint",
        )
        if (
            not fingerprint.startswith("sha256:")
            or len(fingerprint) != 71
            or any(
                character not in "0123456789abcdef"
                for character in fingerprint[7:]
            )
        ):
            raise FederationValidationError(
                "invalid-announcement-fingerprint",
                "announcement_fingerprint",
                "must be a lowercase SHA-256 identifier",
            )
        object.__setattr__(
            self,
            "approved_by_node_id",
            _optional_text(self.approved_by_node_id, "approved_by_node_id"),
        )
        object.__setattr__(
            self,
            "reason_code",
            None if self.reason_code is None else _reason(self.reason_code),
        )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))
        if self.updated_at < self.created_at:
            raise FederationValidationError(
                "invalid-update-time",
                "updated_at",
                "must not precede created_at",
            )
        if (
            self.state is ProviderEnrollmentState.APPROVED
            and self.approved_by_node_id is None
        ):
            raise FederationValidationError(
                "missing-approval-actor",
                "approved_by_node_id",
                "approved enrollment requires an approval actor",
            )

    @property
    def eligible_for_resource_binding(self) -> bool:
        """Whether F8.2 may accept a fresh live resource report for this record."""

        return (
            self.state is ProviderEnrollmentState.APPROVED
            and self.announcement_status is CapabilityStatus.READY
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "enrollment_protocol_version": self.enrollment_protocol_version,
            "enrollment_id": self.enrollment_id,
            "session_id": self.session_id,
            "capability_id": self.capability_id,
            "node_id": self.node_id,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "protocol_major": self.protocol_major,
            "state": self.state.value,
            "revision": self.revision,
            "announcement_status": self.announcement_status.value,
            "announcement_announced_at": _stamp(self.announcement_announced_at),
            "announcement_fingerprint": self.announcement_fingerprint,
            "requested_by_node_id": self.requested_by_node_id,
            "approved_by_node_id": self.approved_by_node_id,
            "reason_code": self.reason_code,
            "created_at": _stamp(self.created_at),
            "updated_at": _stamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "provider_enrollment",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "enrollment_protocol_version",
            "enrollment_id",
            "session_id",
            "capability_id",
            "node_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "protocol_major",
            "state",
            "revision",
            "announcement_status",
            "announcement_announced_at",
            "announcement_fingerprint",
            "requested_by_node_id",
            "approved_by_node_id",
            "reason_code",
            "created_at",
            "updated_at",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(**{name: value[name] for name in names})


@dataclass(frozen=True)
class ProviderEnrollmentAuditEvent:
    """Safe append-only operator evidence for enrollment mutations."""

    SCHEMA: ClassVar[str] = PROVIDER_ENROLLMENT_AUDIT_SCHEMA

    audit_id: int
    operation: str
    outcome: str
    reason_code: str
    actor_node_id: str
    session_id: str
    capability_id: str
    enrollment_id: str | None
    record_revision: int | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "audit_id", _positive(self.audit_id, "audit_id"))
        for field in (
            "operation",
            "outcome",
            "reason_code",
            "actor_node_id",
            "session_id",
            "capability_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "enrollment_id",
            _optional_text(self.enrollment_id, "enrollment_id"),
        )
        if self.record_revision is not None:
            object.__setattr__(
                self,
                "record_revision",
                _positive(self.record_revision, "record_revision"),
            )
        object.__setattr__(
            self,
            "occurred_at",
            _utc(self.occurred_at, "occurred_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "audit_id": self.audit_id,
            "operation": self.operation,
            "outcome": self.outcome,
            "reason_code": self.reason_code,
            "actor_node_id": self.actor_node_id,
            "session_id": self.session_id,
            "capability_id": self.capability_id,
            "enrollment_id": self.enrollment_id,
            "record_revision": self.record_revision,
            "occurred_at": _stamp(self.occurred_at),
        }


class SQLiteProviderEnrollmentStore:
    """Transactional, restart-safe provider approval authority."""

    def __init__(
        self,
        database: Path | str,
        *,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.database = str(database)
        self._id_factory = id_factory or (lambda: uuid.uuid4().hex)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        database = self._connect()
        try:
            database.execute("BEGIN IMMEDIATE")
            yield database
            database.commit()
        except BaseException:
            database.rollback()
            raise
        finally:
            database.close()

    def initialize(self) -> None:
        with self.transaction() as database:
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS provider_enrollment_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_enrollments(
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    enrollment_id TEXT NOT NULL UNIQUE,
                    node_id TEXT NOT NULL,
                    capability_type TEXT NOT NULL,
                    protocol TEXT NOT NULL,
                    protocol_version TEXT NOT NULL,
                    protocol_major INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    announcement_status TEXT NOT NULL,
                    announcement_announced_at TEXT NOT NULL,
                    announcement_fingerprint TEXT NOT NULL,
                    requested_by_node_id TEXT NOT NULL,
                    approved_by_node_id TEXT,
                    reason_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, capability_id)
                );

                CREATE TABLE IF NOT EXISTS provider_enrollment_commands(
                    actor_node_id TEXT NOT NULL,
                    command_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    command_fingerprint TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_node_id, command_key)
                );

                CREATE TABLE IF NOT EXISTS provider_enrollment_audit(
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    enrollment_id TEXT,
                    record_revision INTEGER,
                    occurred_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS provider_enrollments_listing
                ON provider_enrollments(
                    session_id, capability_type, state, node_id, capability_id
                );

                CREATE INDEX IF NOT EXISTS provider_enrollment_audit_listing
                ON provider_enrollment_audit(audit_id);
                """
            )
            row = database.execute(
                "SELECT value FROM provider_enrollment_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                database.execute(
                    """
                    INSERT INTO provider_enrollment_meta(key,value)
                    VALUES('schema_version',?)
                    """,
                    (str(PROVIDER_ENROLLMENT_STORE_SCHEMA_VERSION),),
                )
            elif row["value"] != str(PROVIDER_ENROLLMENT_STORE_SCHEMA_VERSION):
                raise FederationOperationError(
                    "unsupported-enrollment-store-schema",
                    "provider enrollment database uses an unsupported schema",
                )

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> ProviderEnrollmentRecord:
        return ProviderEnrollmentRecord(
            enrollment_id=row["enrollment_id"],
            session_id=row["session_id"],
            capability_id=row["capability_id"],
            node_id=row["node_id"],
            capability_type=row["capability_type"],
            protocol=row["protocol"],
            protocol_version=row["protocol_version"],
            protocol_major=row["protocol_major"],
            state=row["state"],
            revision=row["revision"],
            announcement_status=row["announcement_status"],
            announcement_announced_at=row["announcement_announced_at"],
            announcement_fingerprint=row["announcement_fingerprint"],
            requested_by_node_id=row["requested_by_node_id"],
            approved_by_node_id=row["approved_by_node_id"],
            reason_code=row["reason_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _write_record(
        database: sqlite3.Connection,
        record: ProviderEnrollmentRecord,
    ) -> None:
        database.execute(
            """
            INSERT INTO provider_enrollments(
                session_id,capability_id,enrollment_id,node_id,capability_type,
                protocol,protocol_version,protocol_major,state,revision,
                announcement_status,announcement_announced_at,
                announcement_fingerprint,requested_by_node_id,
                approved_by_node_id,reason_code,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(session_id,capability_id) DO UPDATE SET
                enrollment_id=excluded.enrollment_id,
                node_id=excluded.node_id,
                capability_type=excluded.capability_type,
                protocol=excluded.protocol,
                protocol_version=excluded.protocol_version,
                protocol_major=excluded.protocol_major,
                state=excluded.state,
                revision=excluded.revision,
                announcement_status=excluded.announcement_status,
                announcement_announced_at=excluded.announcement_announced_at,
                announcement_fingerprint=excluded.announcement_fingerprint,
                requested_by_node_id=excluded.requested_by_node_id,
                approved_by_node_id=excluded.approved_by_node_id,
                reason_code=excluded.reason_code,
                created_at=excluded.created_at,
                updated_at=excluded.updated_at
            """,
            (
                record.session_id,
                record.capability_id,
                record.enrollment_id,
                record.node_id,
                record.capability_type,
                record.protocol,
                record.protocol_version,
                record.protocol_major,
                record.state.value,
                record.revision,
                record.announcement_status.value,
                _stamp(record.announcement_announced_at),
                record.announcement_fingerprint,
                record.requested_by_node_id,
                record.approved_by_node_id,
                record.reason_code,
                _stamp(record.created_at),
                _stamp(record.updated_at),
            ),
        )

    @staticmethod
    def _audit(
        database: sqlite3.Connection,
        *,
        operation: str,
        outcome: str,
        reason_code: str,
        actor_node_id: str,
        session_id: str,
        capability_id: str,
        record: ProviderEnrollmentRecord | None,
        occurred_at: datetime,
    ) -> None:
        database.execute(
            """
            INSERT INTO provider_enrollment_audit(
                operation,outcome,reason_code,actor_node_id,session_id,
                capability_id,enrollment_id,record_revision,occurred_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                operation,
                outcome,
                reason_code,
                actor_node_id,
                session_id,
                capability_id,
                None if record is None else record.enrollment_id,
                None if record is None else record.revision,
                _stamp(occurred_at),
            ),
        )

    @staticmethod
    def _command_fingerprint(operation: str, payload: dict[str, Any]) -> str:
        return _hash(_canonical({"operation": operation, "payload": payload}))

    def _replay_command(
        self,
        database: sqlite3.Connection,
        *,
        actor_node_id: str,
        command_id: str,
        operation: str,
        fingerprint: str,
    ) -> ProviderEnrollmentRecord | None:
        row = database.execute(
            """
            SELECT * FROM provider_enrollment_commands
            WHERE actor_node_id=? AND command_key=?
            """,
            (actor_node_id, _command_key(command_id)),
        ).fetchone()
        if row is None:
            return None
        if (
            row["operation"] != operation
            or row["command_fingerprint"] != fingerprint
        ):
            raise FederationOperationError(
                "idempotency-conflict",
                "provider enrollment command ID was reused with different content",
                "command_id",
            )
        return ProviderEnrollmentRecord.from_dict(json.loads(row["result_json"]))

    @staticmethod
    def _record_command(
        database: sqlite3.Connection,
        *,
        actor_node_id: str,
        command_id: str,
        operation: str,
        fingerprint: str,
        record: ProviderEnrollmentRecord,
        now: datetime,
    ) -> None:
        database.execute(
            """
            INSERT INTO provider_enrollment_commands(
                actor_node_id,command_key,operation,command_fingerprint,
                session_id,capability_id,result_json,created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                actor_node_id,
                _command_key(command_id),
                operation,
                fingerprint,
                record.session_id,
                record.capability_id,
                _canonical(record.to_dict()),
                _stamp(now),
            ),
        )

    @staticmethod
    def _current(
        database: sqlite3.Connection,
        *,
        session_id: str,
        capability_id: str,
    ) -> ProviderEnrollmentRecord | None:
        row = database.execute(
            """
            SELECT * FROM provider_enrollments
            WHERE session_id=? AND capability_id=?
            """,
            (session_id, capability_id),
        ).fetchone()
        return (
            None
            if row is None
            else SQLiteProviderEnrollmentStore._record_from_row(row)
        )

    @staticmethod
    def _validated_announcement(
        announcement: Any,
    ) -> CapabilityAnnouncement:
        if not isinstance(announcement, CapabilityAnnouncement):
            raise FederationValidationError(
                "invalid-capability-announcement",
                "announcement",
                "must be a CapabilityAnnouncement",
            )
        return CapabilityAnnouncement.from_dict(announcement.to_dict())

    @staticmethod
    def _assert_identity(
        record: ProviderEnrollmentRecord,
        announcement: CapabilityAnnouncement,
    ) -> tuple[int, int]:
        if (
            record.session_id != announcement.session_id
            or record.capability_id != announcement.capability_id
            or record.node_id != announcement.node_id
            or record.capability_type != announcement.type
            or record.protocol != announcement.protocol
        ):
            raise FederationOperationError(
                "capability-identity-conflict",
                (
                    "enrolled capability identity cannot move between sessions, "
                    "nodes, types, or protocols"
                ),
                "capability_id",
            )
        offered_major, offered_minor = protocol_version_parts(
            announcement.protocol_version,
            "announcement.protocol_version",
        )
        if offered_major != record.protocol_major:
            raise ProtocolCompatibilityError(
                "incompatible-provider-protocol-major",
                "announcement.protocol_version",
                (
                    f"enrolled major is {record.protocol_major}, "
                    f"received {offered_major}"
                ),
            )
        return offered_major, offered_minor

    @staticmethod
    def _assert_expected_revision(
        record: ProviderEnrollmentRecord,
        expected_revision: int,
    ) -> None:
        if (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision <= 0
        ):
            raise FederationValidationError(
                "invalid-positive-integer",
                "expected_revision",
                "must be a positive integer",
            )
        if record.revision != expected_revision:
            raise FederationOperationError(
                "stale-enrollment-revision",
                (
                    f"expected revision {expected_revision}, "
                    f"current revision is {record.revision}"
                ),
                "expected_revision",
            )

    @staticmethod
    def _announcement_changes(
        record: ProviderEnrollmentRecord,
        announcement: CapabilityAnnouncement,
    ) -> dict[str, Any]:
        SQLiteProviderEnrollmentStore._assert_identity(record, announcement)
        fingerprint = _announcement_fingerprint(announcement)
        if announcement.announced_at < record.announcement_announced_at:
            raise FederationOperationError(
                "stale-capability-announcement",
                "announcement timestamp moved backward",
                "announced_at",
            )
        state = record.state
        reason_code = record.reason_code
        if announcement.status is CapabilityStatus.REVOKED:
            state = ProviderEnrollmentState.REVOKED
            reason_code = "capability-revoked"
        elif (
            announcement.status is CapabilityStatus.DISABLED
            and state is not ProviderEnrollmentState.REVOKED
        ):
            state = ProviderEnrollmentState.SUSPENDED
            reason_code = "capability-disabled"
        return {
            "protocol_version": announcement.protocol_version,
            "announcement_status": announcement.status,
            "announcement_announced_at": announcement.announced_at,
            "announcement_fingerprint": fingerprint,
            "state": state,
            "reason_code": reason_code,
        }

    def request(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        announcement = self._validated_announcement(announcement)
        actor_node_id = _text(actor_node_id, "actor_node_id")
        now = _utc(now, "now")
        protocol_major, _minor = protocol_version_parts(
            announcement.protocol_version,
            "announcement.protocol_version",
        )
        payload = {
            "announcement": announcement.to_dict(),
        }
        fingerprint = self._command_fingerprint("request", payload)
        with self.transaction() as database:
            replay = self._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="request",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            current = self._current(
                database,
                session_id=announcement.session_id,
                capability_id=announcement.capability_id,
            )
            if current is None:
                state = (
                    ProviderEnrollmentState.REVOKED
                    if announcement.status is CapabilityStatus.REVOKED
                    else ProviderEnrollmentState.PENDING
                )
                record = ProviderEnrollmentRecord(
                    enrollment_id=f"enrollment-{self._id_factory()}",
                    session_id=announcement.session_id,
                    capability_id=announcement.capability_id,
                    node_id=announcement.node_id,
                    capability_type=announcement.type,
                    protocol=announcement.protocol,
                    protocol_version=announcement.protocol_version,
                    protocol_major=protocol_major,
                    state=state,
                    revision=1,
                    announcement_status=announcement.status,
                    announcement_announced_at=announcement.announced_at,
                    announcement_fingerprint=_announcement_fingerprint(announcement),
                    requested_by_node_id=actor_node_id,
                    approved_by_node_id=None,
                    reason_code=(
                        "capability-revoked"
                        if state is ProviderEnrollmentState.REVOKED
                        else "approval-pending"
                    ),
                    created_at=now,
                    updated_at=now,
                )
                reason = "enrollment-requested"
            else:
                changes = self._announcement_changes(current, announcement)
                changed = any(
                    getattr(current, name) != value
                    for name, value in changes.items()
                )
                record = (
                    replace(
                        current,
                        **changes,
                        revision=current.revision + 1,
                        updated_at=now,
                    )
                    if changed
                    else current
                )
                reason = (
                    "announcement-reconciled"
                    if changed
                    else "existing-enrollment-returned"
                )
            self._write_record(database, record)
            self._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="request",
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self._audit(
                database,
                operation="request",
                outcome="accepted",
                reason_code=reason,
                actor_node_id=actor_node_id,
                session_id=record.session_id,
                capability_id=record.capability_id,
                record=record,
                occurred_at=now,
            )
            return record

    def approve(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        return self._decide(
            "approve",
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_state=ProviderEnrollmentState.APPROVED,
            reason_code="explicitly-approved",
            now=now,
        )

    def suspend(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        reason_code: str,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        return self._decide(
            "suspend",
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_state=ProviderEnrollmentState.SUSPENDED,
            reason_code=reason_code,
            now=now,
        )

    def revoke(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        reason_code: str,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        return self._decide(
            "revoke",
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_state=ProviderEnrollmentState.REVOKED,
            reason_code=reason_code,
            now=now,
        )

    def _decide(
        self,
        operation: str,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        target_state: ProviderEnrollmentState,
        reason_code: str,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        announcement = self._validated_announcement(announcement)
        actor_node_id = _text(actor_node_id, "actor_node_id")
        reason_code = _reason(reason_code)
        now = _utc(now, "now")
        payload = {
            "session_id": announcement.session_id,
            "capability_id": announcement.capability_id,
            "announcement": announcement.to_dict(),
            "expected_revision": expected_revision,
            "target_state": target_state.value,
            "reason_code": reason_code,
        }
        fingerprint = self._command_fingerprint(operation, payload)
        with self.transaction() as database:
            replay = self._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation=operation,
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            current = self._current(
                database,
                session_id=announcement.session_id,
                capability_id=announcement.capability_id,
            )
            if current is None:
                raise FederationOperationError(
                    "unknown-provider-enrollment",
                    "provider must be requested before a decision",
                    "capability_id",
                )
            self._assert_expected_revision(current, expected_revision)
            changes = self._announcement_changes(current, announcement)
            if (
                current.state is ProviderEnrollmentState.REVOKED
                and target_state is not ProviderEnrollmentState.REVOKED
            ):
                raise FederationOperationError(
                    "provider-enrollment-revoked",
                    "revoked provider enrollment cannot be reopened",
                    "capability_id",
                )
            if (
                target_state is ProviderEnrollmentState.APPROVED
                and announcement.status is not CapabilityStatus.READY
            ):
                raise FederationOperationError(
                    "provider-not-ready",
                    "only a ready capability announcement may be approved",
                    "announcement_status",
                )
            final_state = target_state
            final_reason = reason_code
            if announcement.status is CapabilityStatus.REVOKED:
                final_state = ProviderEnrollmentState.REVOKED
                final_reason = "capability-revoked"
            elif (
                announcement.status is CapabilityStatus.DISABLED
                and target_state is ProviderEnrollmentState.APPROVED
            ):
                raise FederationOperationError(
                    "provider-not-ready",
                    "disabled capability cannot be approved",
                    "announcement_status",
                )
            approved_by = current.approved_by_node_id
            if final_state is ProviderEnrollmentState.APPROVED:
                approved_by = actor_node_id
            changes.update(
                {
                    "state": final_state,
                    "approved_by_node_id": approved_by,
                    "reason_code": final_reason,
                    "revision": current.revision + 1,
                    "updated_at": now,
                }
            )
            record = replace(current, **changes)
            self._write_record(database, record)
            self._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation=operation,
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self._audit(
                database,
                operation=operation,
                outcome="accepted",
                reason_code=final_reason,
                actor_node_id=actor_node_id,
                session_id=record.session_id,
                capability_id=record.capability_id,
                record=record,
                occurred_at=now,
            )
            return record

    def reconcile(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> ProviderEnrollmentRecord:
        announcement = self._validated_announcement(announcement)
        actor_node_id = _text(actor_node_id, "actor_node_id")
        now = _utc(now, "now")
        payload = {
            "announcement": announcement.to_dict(),
            "expected_revision": expected_revision,
        }
        fingerprint = self._command_fingerprint("reconcile", payload)
        with self.transaction() as database:
            replay = self._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="reconcile",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            current = self._current(
                database,
                session_id=announcement.session_id,
                capability_id=announcement.capability_id,
            )
            if current is None:
                raise FederationOperationError(
                    "unknown-provider-enrollment",
                    "provider must be requested before reconciliation",
                    "capability_id",
                )
            self._assert_expected_revision(current, expected_revision)
            changes = self._announcement_changes(current, announcement)
            changed = any(
                getattr(current, name) != value
                for name, value in changes.items()
            )
            record = (
                replace(
                    current,
                    **changes,
                    revision=current.revision + 1,
                    updated_at=now,
                )
                if changed
                else current
            )
            self._write_record(database, record)
            self._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="reconcile",
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self._audit(
                database,
                operation="reconcile",
                outcome="accepted",
                reason_code=(
                    "announcement-reconciled"
                    if changed
                    else "announcement-unchanged"
                ),
                actor_node_id=actor_node_id,
                session_id=record.session_id,
                capability_id=record.capability_id,
                record=record,
                occurred_at=now,
            )
            return record

    def get(
        self,
        *,
        session_id: str,
        capability_id: str,
    ) -> ProviderEnrollmentRecord | None:
        session_id = _text(session_id, "session_id")
        capability_id = _text(capability_id, "capability_id")
        with self._connect() as database:
            return self._current(
                database,
                session_id=session_id,
                capability_id=capability_id,
            )

    def list_records(
        self,
        *,
        session_id: str,
        capability_type: str | None = None,
        state: ProviderEnrollmentState | str | None = None,
    ) -> tuple[ProviderEnrollmentRecord, ...]:
        session_id = _text(session_id, "session_id")
        clauses = ["session_id=?"]
        arguments: list[Any] = [session_id]
        if capability_type is not None:
            clauses.append("capability_type=?")
            arguments.append(_text(capability_type, "capability_type"))
        if state is not None:
            try:
                state_value = ProviderEnrollmentState(state).value
            except (TypeError, ValueError) as exc:
                raise FederationValidationError(
                    "invalid-enrollment-state",
                    "state",
                    "unknown provider enrollment state",
                ) from exc
            clauses.append("state=?")
            arguments.append(state_value)
        with self._connect() as database:
            rows = database.execute(
                f"""
                SELECT * FROM provider_enrollments
                WHERE {' AND '.join(clauses)}
                ORDER BY capability_type,node_id,capability_id
                """,
                arguments,
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def eligible_records(
        self,
        *,
        session_id: str,
        capability_type: str | None = None,
    ) -> tuple[ProviderEnrollmentRecord, ...]:
        return tuple(
            record
            for record in self.list_records(
                session_id=session_id,
                capability_type=capability_type,
                state=ProviderEnrollmentState.APPROVED,
            )
            if record.eligible_for_resource_binding
        )

    def audit_entries(
        self,
        *,
        limit: int = 1_000,
    ) -> tuple[ProviderEnrollmentAuditEvent, ...]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_ENROLLMENT_AUDIT_READ
        ):
            raise FederationValidationError(
                "invalid-audit-limit",
                "limit",
                f"must be between 1 and {MAX_ENROLLMENT_AUDIT_READ}",
            )
        with self._connect() as database:
            rows = database.execute(
                """
                SELECT * FROM provider_enrollment_audit
                ORDER BY audit_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(
            ProviderEnrollmentAuditEvent(
                audit_id=row["audit_id"],
                operation=row["operation"],
                outcome=row["outcome"],
                reason_code=row["reason_code"],
                actor_node_id=row["actor_node_id"],
                session_id=row["session_id"],
                capability_id=row["capability_id"],
                enrollment_id=row["enrollment_id"],
                record_revision=row["record_revision"],
                occurred_at=row["occurred_at"],
            )
            for row in reversed(rows)
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FederatedProviderEnrollmentService:
    """Authorized bridge from coordinator discovery to durable approval."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        store: SQLiteProviderEnrollmentStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.coordinator = coordinator
        self.store = store
        self._clock = clock

    def discover(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        capability_type: str | None = None,
    ) -> tuple[CapabilityAnnouncement, ...]:
        self.coordinator.store.require_membership(
            session_id=session_id,
            node_id=actor_node_id,
        )
        return self.coordinator.store.list_capabilities(
            session_id=session_id,
            capability_type=capability_type,
        )

    def _announcement(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
    ) -> CapabilityAnnouncement:
        capabilities = self.discover(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        for announcement in capabilities:
            if announcement.capability_id == capability_id:
                return announcement
        raise FederationOperationError(
            "unknown-session-capability",
            "capability is not announced in the target session",
            "capability_id",
        )

    def _require_session_owner(
        self,
        *,
        session_id: str,
        actor_node_id: str,
    ) -> None:
        self.coordinator.store.require_membership(
            session_id=session_id,
            node_id=actor_node_id,
        )
        session = self.coordinator.store.get_session(session_id)
        if session is None:
            raise AuthorizationError(
                "unknown-session",
                "target session does not exist",
                "session_id",
            )
        if session.created_by_node_id != actor_node_id:
            raise AuthorizationError(
                "provider-enrollment-not-authorized",
                "only the session creator may manage provider enrollment in F8.1",
                "actor_node_id",
            )

    def request(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
    ) -> ProviderEnrollmentRecord:
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        return self.store.request(
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            now=self._clock(),
        )

    def approve(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
    ) -> ProviderEnrollmentRecord:
        self._require_session_owner(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        return self.store.approve(
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            now=self._clock(),
        )

    def suspend(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        reason_code: str,
    ) -> ProviderEnrollmentRecord:
        self._require_session_owner(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        return self.store.suspend(
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            reason_code=reason_code,
            now=self._clock(),
        )

    def revoke(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
        reason_code: str,
    ) -> ProviderEnrollmentRecord:
        self._require_session_owner(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        return self.store.revoke(
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            reason_code=reason_code,
            now=self._clock(),
        )

    def reconcile(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
    ) -> ProviderEnrollmentRecord:
        self._require_session_owner(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        return self.store.reconcile(
            announcement,
            actor_node_id=actor_node_id,
            command_id=command_id,
            expected_revision=expected_revision,
            now=self._clock(),
        )

    def eligible_records(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        capability_type: str | None = None,
    ) -> tuple[ProviderEnrollmentRecord, ...]:
        # Discovery is deliberately repeated so removed or revoked callers
        # cannot read previously approved session state. Exact fingerprints
        # also prevent stale approval state from masquerading as current health.
        announcements = self.discover(
            session_id=session_id,
            actor_node_id=actor_node_id,
            capability_type=capability_type,
        )
        current = {
            announcement.capability_id: announcement
            for announcement in announcements
            if announcement.status is CapabilityStatus.READY
        }
        return tuple(
            record
            for record in self.store.eligible_records(
                session_id=session_id,
                capability_type=capability_type,
            )
            if record.capability_id in current
            and record.announcement_fingerprint
            == _announcement_fingerprint(current[record.capability_id])
        )

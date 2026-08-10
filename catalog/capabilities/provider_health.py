"""Authenticated, short-lived health synchronization for approved providers.

F8.2 binds an F8.1 enrollment to the existing F7 ProviderResourceReport. The
health store never grants approval, reserves capacity, selects a provider,
assigns ownership, dispatches work, or exposes a private provider endpoint.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

from .provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
)
from .provider_reports import (
    PROVIDER_REPORT_PROTOCOL_MAJOR,
    ProviderResourceReport,
    protocol_version_parts,
)

PROVIDER_HEALTH_PROTOCOL = "fcp-provider-health-sync"
PROVIDER_HEALTH_PROTOCOL_VERSION = "1.0"
PROVIDER_HEALTH_PROTOCOL_MAJOR = 1
PROVIDER_HEALTH_RECORD_SCHEMA = "fcp.provider-health-record.v1"
PROVIDER_HEALTH_OBSERVATION_SCHEMA = "fcp.provider-health-observation.v1"
PROVIDER_HEALTH_AUDIT_SCHEMA = "fcp.provider-health-audit.v1"
PROVIDER_HEALTH_STORE_SCHEMA_VERSION = 1
MAX_HEALTH_AUDIT_READ = 10_000
MAX_TEXT_BYTES = 512


class ProviderHealthState(str, Enum):
    """Safe evaluated state of one provider's latest live report."""

    ABSENT = "absent"
    CURRENT = "current"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"
    ENROLLMENT_INELIGIBLE = "enrollment-ineligible"
    ANNOUNCEMENT_INELIGIBLE = "announcement-ineligible"


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


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _non_negative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _positive(value: Any, field: str) -> int:
    value = _non_negative(value, field)
    if value == 0:
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


def _validate_health_protocol(value: Any) -> str:
    version = _text(value, "health_protocol_version")
    major, _minor = protocol_version_parts(version, "health_protocol_version")
    if major != PROVIDER_HEALTH_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-health-protocol-major",
            "health_protocol_version",
            (
                f"supported major is {PROVIDER_HEALTH_PROTOCOL_MAJOR}, "
                f"received {major}"
            ),
        )
    return version


def _report_fingerprint(report: ProviderResourceReport) -> str:
    return _hash(_canonical(report.to_dict()))


@dataclass(frozen=True)
class ProviderHealthRecord:
    """Latest accepted short-lived report for one approved enrollment."""

    SCHEMA: ClassVar[str] = PROVIDER_HEALTH_RECORD_SCHEMA

    enrollment_id: str
    enrollment_revision: int
    provider_generation: int
    report: ProviderResourceReport
    report_fingerprint: str
    received_at: datetime
    health_protocol_version: str = PROVIDER_HEALTH_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "enrollment_id", _text(self.enrollment_id, "enrollment_id"))
        object.__setattr__(
            self,
            "enrollment_revision",
            _positive(self.enrollment_revision, "enrollment_revision"),
        )
        object.__setattr__(
            self,
            "provider_generation",
            _positive(self.provider_generation, "provider_generation"),
        )
        if not isinstance(self.report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        object.__setattr__(
            self,
            "health_protocol_version",
            _validate_health_protocol(self.health_protocol_version),
        )
        fingerprint = _text(self.report_fingerprint, "report_fingerprint")
        if (
            not fingerprint.startswith("sha256:")
            or len(fingerprint) != 71
            or any(character not in "0123456789abcdef" for character in fingerprint[7:])
        ):
            raise FederationValidationError(
                "invalid-report-fingerprint",
                "report_fingerprint",
                "must be a lowercase SHA-256 identifier",
            )
        if fingerprint != _report_fingerprint(self.report):
            raise FederationValidationError(
                "report-fingerprint-mismatch",
                "report_fingerprint",
                "must match the canonical provider report",
            )
        object.__setattr__(self, "received_at", _utc(self.received_at, "received_at"))

    @property
    def session_id(self) -> str:
        return self.report.session_id

    @property
    def capability_id(self) -> str:
        return self.report.capability_id

    @property
    def node_id(self) -> str:
        return self.report.node_id

    @property
    def capability_type(self) -> str:
        return self.report.capability_type

    @property
    def report_revision(self) -> int:
        return self.report.report_revision

    def is_fresh_at(self, value: datetime) -> bool:
        return self.report.is_fresh_at(value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "health_protocol_version": self.health_protocol_version,
            "enrollment_id": self.enrollment_id,
            "enrollment_revision": self.enrollment_revision,
            "provider_generation": self.provider_generation,
            "report": self.report.to_dict(),
            "report_fingerprint": self.report_fingerprint,
            "received_at": _stamp(self.received_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                "provider_health_record",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "health_protocol_version",
            "enrollment_id",
            "enrollment_revision",
            "provider_generation",
            "report",
            "report_fingerprint",
            "received_at",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        return cls(
            health_protocol_version=value["health_protocol_version"],
            enrollment_id=value["enrollment_id"],
            enrollment_revision=value["enrollment_revision"],
            provider_generation=value["provider_generation"],
            report=ProviderResourceReport.from_dict(value["report"]),
            report_fingerprint=value["report_fingerprint"],
            received_at=value["received_at"],
        )


@dataclass(frozen=True)
class ProviderHealthObservation:
    """Safe evaluated health result for an authorized session reader."""

    SCHEMA: ClassVar[str] = PROVIDER_HEALTH_OBSERVATION_SCHEMA

    session_id: str
    capability_id: str
    state: ProviderHealthState
    reason_code: str
    evaluated_at: datetime
    enrollment_id: str | None = None
    enrollment_revision: int | None = None
    provider_generation: int | None = None
    report_revision: int | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "session_id", _text(self.session_id, "session_id"))
        object.__setattr__(self, "capability_id", _text(self.capability_id, "capability_id"))
        try:
            object.__setattr__(self, "state", ProviderHealthState(self.state))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-health-state",
                "state",
                "unknown provider health state",
            ) from exc
        object.__setattr__(self, "reason_code", _text(self.reason_code, "reason_code"))
        object.__setattr__(self, "evaluated_at", _utc(self.evaluated_at, "evaluated_at"))
        object.__setattr__(self, "enrollment_id", _optional_text(self.enrollment_id, "enrollment_id"))
        for name in ("enrollment_revision", "provider_generation"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _positive(value, name))
        if self.report_revision is not None:
            object.__setattr__(
                self,
                "report_revision",
                _non_negative(self.report_revision, "report_revision"),
            )
        if self.expires_at is not None:
            object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "capability_id": self.capability_id,
            "state": self.state.value,
            "reason_code": self.reason_code,
            "evaluated_at": _stamp(self.evaluated_at),
            "enrollment_id": self.enrollment_id,
            "enrollment_revision": self.enrollment_revision,
            "provider_generation": self.provider_generation,
            "report_revision": self.report_revision,
            "expires_at": None if self.expires_at is None else _stamp(self.expires_at),
        }


@dataclass(frozen=True)
class ProviderHealthAuditEvent:
    """Safe append-only evidence for accepted health mutations."""

    SCHEMA: ClassVar[str] = PROVIDER_HEALTH_AUDIT_SCHEMA

    audit_id: int
    operation: str
    outcome: str
    reason_code: str
    actor_node_id: str
    session_id: str
    capability_id: str
    enrollment_id: str | None
    enrollment_revision: int | None
    provider_generation: int | None
    report_revision: int | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "audit_id", _positive(self.audit_id, "audit_id"))
        for name in (
            "operation",
            "outcome",
            "reason_code",
            "actor_node_id",
            "session_id",
            "capability_id",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        object.__setattr__(self, "enrollment_id", _optional_text(self.enrollment_id, "enrollment_id"))
        for name in ("enrollment_revision", "provider_generation"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _positive(value, name))
        if self.report_revision is not None:
            object.__setattr__(
                self,
                "report_revision",
                _non_negative(self.report_revision, "report_revision"),
            )
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))

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
            "enrollment_revision": self.enrollment_revision,
            "provider_generation": self.provider_generation,
            "report_revision": self.report_revision,
            "occurred_at": _stamp(self.occurred_at),
        }


class SQLiteProviderHealthStore:
    """Transactional latest-report store with generation and revision fencing."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
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
                CREATE TABLE IF NOT EXISTS provider_health_meta(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_health_latest(
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    capability_type TEXT NOT NULL,
                    enrollment_id TEXT NOT NULL,
                    enrollment_revision INTEGER NOT NULL,
                    provider_generation INTEGER NOT NULL,
                    report_revision INTEGER NOT NULL,
                    report_json TEXT NOT NULL,
                    report_fingerprint TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, capability_id)
                );

                CREATE TABLE IF NOT EXISTS provider_health_commands(
                    actor_node_id TEXT NOT NULL,
                    command_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    command_fingerprint TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_node_id, command_key)
                );

                CREATE TABLE IF NOT EXISTS provider_health_audit(
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    capability_id TEXT NOT NULL,
                    enrollment_id TEXT,
                    enrollment_revision INTEGER,
                    provider_generation INTEGER,
                    report_revision INTEGER,
                    occurred_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS provider_health_listing
                ON provider_health_latest(session_id, capability_type, node_id, capability_id);

                CREATE INDEX IF NOT EXISTS provider_health_audit_listing
                ON provider_health_audit(audit_id);
                """
            )
            row = database.execute(
                "SELECT value FROM provider_health_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                database.execute(
                    """
                    INSERT INTO provider_health_meta(key,value)
                    VALUES('schema_version',?)
                    """,
                    (str(PROVIDER_HEALTH_STORE_SCHEMA_VERSION),),
                )
            elif row["value"] != str(PROVIDER_HEALTH_STORE_SCHEMA_VERSION):
                raise FederationOperationError(
                    "unsupported-health-store-schema",
                    "provider health database uses an unsupported schema",
                )

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> ProviderHealthRecord:
        return ProviderHealthRecord(
            enrollment_id=row["enrollment_id"],
            enrollment_revision=row["enrollment_revision"],
            provider_generation=row["provider_generation"],
            report=ProviderResourceReport.from_dict(json.loads(row["report_json"])),
            report_fingerprint=row["report_fingerprint"],
            received_at=row["received_at"],
        )

    @staticmethod
    def _current(
        database: sqlite3.Connection,
        *,
        session_id: str,
        capability_id: str,
    ) -> ProviderHealthRecord | None:
        row = database.execute(
            """
            SELECT * FROM provider_health_latest
            WHERE session_id=? AND capability_id=?
            """,
            (session_id, capability_id),
        ).fetchone()
        return None if row is None else SQLiteProviderHealthStore._record_from_row(row)

    @staticmethod
    def _write_record(database: sqlite3.Connection, record: ProviderHealthRecord) -> None:
        database.execute(
            """
            INSERT INTO provider_health_latest(
                session_id,capability_id,node_id,capability_type,
                enrollment_id,enrollment_revision,provider_generation,
                report_revision,report_json,report_fingerprint,received_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(session_id,capability_id) DO UPDATE SET
                node_id=excluded.node_id,
                capability_type=excluded.capability_type,
                enrollment_id=excluded.enrollment_id,
                enrollment_revision=excluded.enrollment_revision,
                provider_generation=excluded.provider_generation,
                report_revision=excluded.report_revision,
                report_json=excluded.report_json,
                report_fingerprint=excluded.report_fingerprint,
                received_at=excluded.received_at
            """,
            (
                record.session_id,
                record.capability_id,
                record.node_id,
                record.capability_type,
                record.enrollment_id,
                record.enrollment_revision,
                record.provider_generation,
                record.report_revision,
                _canonical(record.report.to_dict()),
                record.report_fingerprint,
                _stamp(record.received_at),
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
        record: ProviderHealthRecord,
        occurred_at: datetime,
    ) -> None:
        database.execute(
            """
            INSERT INTO provider_health_audit(
                operation,outcome,reason_code,actor_node_id,session_id,
                capability_id,enrollment_id,enrollment_revision,
                provider_generation,report_revision,occurred_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                operation,
                outcome,
                reason_code,
                actor_node_id,
                record.session_id,
                record.capability_id,
                record.enrollment_id,
                record.enrollment_revision,
                record.provider_generation,
                record.report_revision,
                _stamp(occurred_at),
            ),
        )

    @staticmethod
    def _command_fingerprint(payload: dict[str, Any]) -> str:
        return _hash(_canonical({"operation": "publish", "payload": payload}))

    def _replay_command(
        self,
        database: sqlite3.Connection,
        *,
        actor_node_id: str,
        command_id: str,
        fingerprint: str,
    ) -> ProviderHealthRecord | None:
        row = database.execute(
            """
            SELECT * FROM provider_health_commands
            WHERE actor_node_id=? AND command_key=?
            """,
            (actor_node_id, _command_key(command_id)),
        ).fetchone()
        if row is None:
            return None
        if row["operation"] != "publish" or row["command_fingerprint"] != fingerprint:
            raise FederationOperationError(
                "idempotency-conflict",
                "provider health command ID was reused with different content",
                "command_id",
            )
        return ProviderHealthRecord.from_dict(json.loads(row["result_json"]))

    @staticmethod
    def _record_command(
        database: sqlite3.Connection,
        *,
        actor_node_id: str,
        command_id: str,
        fingerprint: str,
        record: ProviderHealthRecord,
        now: datetime,
    ) -> None:
        database.execute(
            """
            INSERT INTO provider_health_commands(
                actor_node_id,command_key,operation,command_fingerprint,
                result_json,created_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                actor_node_id,
                _command_key(command_id),
                "publish",
                fingerprint,
                _canonical(record.to_dict()),
                _stamp(now),
            ),
        )

    @staticmethod
    def _assert_enrollment_binding(
        enrollment: ProviderEnrollmentRecord,
        report: ProviderResourceReport,
    ) -> None:
        if enrollment.state is not ProviderEnrollmentState.APPROVED:
            raise FederationOperationError(
                "provider-enrollment-ineligible",
                "provider enrollment is not approved",
                "enrollment_id",
            )
        if not enrollment.eligible_for_resource_binding:
            raise FederationOperationError(
                "provider-enrollment-ineligible",
                "provider enrollment is not ready for resource binding",
                "enrollment_id",
            )
        identity = (
            enrollment.session_id == report.session_id
            and enrollment.capability_id == report.capability_id
            and enrollment.node_id == report.node_id
            and enrollment.capability_type == report.capability_type
            and enrollment.protocol == report.protocol
            and enrollment.protocol_version == report.protocol_version
        )
        if not identity:
            raise FederationOperationError(
                "provider-health-identity-mismatch",
                "provider report does not match the approved enrollment identity",
                "report",
            )
        report_major, _minor = protocol_version_parts(
            report.protocol_version,
            "report.protocol_version",
        )
        if report_major != enrollment.protocol_major:
            raise ProtocolCompatibilityError(
                "incompatible-provider-protocol-major",
                "report.protocol_version",
                (
                    f"enrolled major is {enrollment.protocol_major}, "
                    f"received {report_major}"
                ),
            )
        report_protocol_major, _report_minor = protocol_version_parts(
            report.report_protocol_version,
            "report.report_protocol_version",
        )
        if report_protocol_major != PROVIDER_REPORT_PROTOCOL_MAJOR:
            raise ProtocolCompatibilityError(
                "unsupported-provider-report-major",
                "report.report_protocol_version",
                (
                    f"supported major is {PROVIDER_REPORT_PROTOCOL_MAJOR}, "
                    f"received {report_protocol_major}"
                ),
            )

    def publish(
        self,
        report: ProviderResourceReport,
        enrollment: ProviderEnrollmentRecord,
        *,
        actor_node_id: str,
        command_id: str,
        provider_generation: int,
        now: datetime,
    ) -> ProviderHealthRecord:
        if not isinstance(report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        if not isinstance(enrollment, ProviderEnrollmentRecord):
            raise FederationValidationError(
                "invalid-provider-enrollment",
                "enrollment",
                "must be a ProviderEnrollmentRecord",
            )
        actor_node_id = _text(actor_node_id, "actor_node_id")
        command_id = _text(command_id, "command_id")
        provider_generation = _positive(provider_generation, "provider_generation")
        now = _utc(now, "now")
        if actor_node_id != report.node_id:
            raise AuthorizationError(
                "provider-health-impersonation",
                "only the provider node may publish its resource report",
                "actor_node_id",
            )
        self._assert_enrollment_binding(enrollment, report)
        if report.reported_at > now:
            raise FederationOperationError(
                "provider-report-from-future",
                "provider report timestamp is later than receipt time",
                "reported_at",
            )
        if now >= report.expires_at:
            raise FederationOperationError(
                "provider-report-expired",
                "provider report is already expired at receipt time",
                "expires_at",
            )
        payload = {
            "enrollment_id": enrollment.enrollment_id,
            "enrollment_revision": enrollment.revision,
            "provider_generation": provider_generation,
            "report": report.to_dict(),
        }
        fingerprint = self._command_fingerprint(payload)
        report_fingerprint = _report_fingerprint(report)
        with self.transaction() as database:
            replay = self._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay
            current = self._current(
                database,
                session_id=report.session_id,
                capability_id=report.capability_id,
            )
            reason = "initial-report-accepted"
            if current is not None:
                if current.node_id != report.node_id:
                    raise FederationOperationError(
                        "provider-health-identity-conflict",
                        "live provider identity cannot move to another node",
                        "node_id",
                    )
                if provider_generation < current.provider_generation:
                    raise FederationOperationError(
                        "stale-provider-generation",
                        "provider generation is older than the accepted generation",
                        "provider_generation",
                    )
                if provider_generation == current.provider_generation:
                    if report.report_revision < current.report_revision:
                        raise FederationOperationError(
                            "stale-provider-report-revision",
                            "provider report revision moved backward",
                            "report_revision",
                        )
                    if report.report_revision == current.report_revision:
                        if (
                            report_fingerprint != current.report_fingerprint
                            or enrollment.enrollment_id != current.enrollment_id
                            or enrollment.revision != current.enrollment_revision
                        ):
                            raise FederationOperationError(
                                "provider-report-revision-conflict",
                                "provider generation and report revision were reused with different content",
                                "report_revision",
                            )
                        record = current
                        reason = "duplicate-report-replayed"
                    else:
                        if report.reported_at < current.report.reported_at:
                            raise FederationOperationError(
                                "stale-provider-report-time",
                                "provider report timestamp moved backward",
                                "reported_at",
                            )
                        record = ProviderHealthRecord(
                            enrollment_id=enrollment.enrollment_id,
                            enrollment_revision=enrollment.revision,
                            provider_generation=provider_generation,
                            report=report,
                            report_fingerprint=report_fingerprint,
                            received_at=now,
                        )
                        reason = "report-revision-advanced"
                else:
                    record = ProviderHealthRecord(
                        enrollment_id=enrollment.enrollment_id,
                        enrollment_revision=enrollment.revision,
                        provider_generation=provider_generation,
                        report=report,
                        report_fingerprint=report_fingerprint,
                        received_at=now,
                    )
                    reason = "provider-generation-superseded"
            else:
                record = ProviderHealthRecord(
                    enrollment_id=enrollment.enrollment_id,
                    enrollment_revision=enrollment.revision,
                    provider_generation=provider_generation,
                    report=report,
                    report_fingerprint=report_fingerprint,
                    received_at=now,
                )
            self._write_record(database, record)
            self._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self._audit(
                database,
                operation="publish",
                outcome="accepted",
                reason_code=reason,
                actor_node_id=actor_node_id,
                record=record,
                occurred_at=now,
            )
            return record

    def get(
        self,
        *,
        session_id: str,
        capability_id: str,
    ) -> ProviderHealthRecord | None:
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
    ) -> tuple[ProviderHealthRecord, ...]:
        session_id = _text(session_id, "session_id")
        clauses = ["session_id=?"]
        arguments: list[Any] = [session_id]
        if capability_type is not None:
            clauses.append("capability_type=?")
            arguments.append(_text(capability_type, "capability_type"))
        with self._connect() as database:
            rows = database.execute(
                f"""
                SELECT * FROM provider_health_latest
                WHERE {' AND '.join(clauses)}
                ORDER BY capability_type,node_id,capability_id
                """,
                arguments,
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def audit_entries(self, *, limit: int = 1_000) -> tuple[ProviderHealthAuditEvent, ...]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_HEALTH_AUDIT_READ
        ):
            raise FederationValidationError(
                "invalid-audit-limit",
                "limit",
                f"must be between 1 and {MAX_HEALTH_AUDIT_READ}",
            )
        with self._connect() as database:
            rows = database.execute(
                """
                SELECT * FROM provider_health_audit
                ORDER BY audit_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(
            ProviderHealthAuditEvent(
                audit_id=row["audit_id"],
                operation=row["operation"],
                outcome=row["outcome"],
                reason_code=row["reason_code"],
                actor_node_id=row["actor_node_id"],
                session_id=row["session_id"],
                capability_id=row["capability_id"],
                enrollment_id=row["enrollment_id"],
                enrollment_revision=row["enrollment_revision"],
                provider_generation=row["provider_generation"],
                report_revision=row["report_revision"],
                occurred_at=row["occurred_at"],
            )
            for row in reversed(rows)
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FederatedProviderHealthService:
    """Authorized bridge from F8.1 enrollment to fresh F7 provider reports."""

    def __init__(
        self,
        enrollments: FederatedProviderEnrollmentService,
        store: SQLiteProviderHealthStore,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(enrollments, FederatedProviderEnrollmentService):
            raise FederationValidationError(
                "invalid-enrollment-service",
                "enrollments",
                "must be a FederatedProviderEnrollmentService",
            )
        if not isinstance(store, SQLiteProviderHealthStore):
            raise FederationValidationError(
                "invalid-health-store",
                "store",
                "must be a SQLiteProviderHealthStore",
            )
        self.enrollments = enrollments
        self.store = store
        self._clock = clock

    def _eligible_enrollment(
        self,
        report: ProviderResourceReport,
        *,
        actor_node_id: str,
    ) -> ProviderEnrollmentRecord:
        if actor_node_id != report.node_id:
            raise AuthorizationError(
                "provider-health-impersonation",
                "only the provider node may publish its resource report",
                "actor_node_id",
            )
        self.enrollments.coordinator.require_active_node(actor_node_id)
        eligible = self.enrollments.eligible_records(
            session_id=report.session_id,
            actor_node_id=actor_node_id,
            capability_type=report.capability_type,
        )
        for enrollment in eligible:
            if enrollment.capability_id == report.capability_id:
                return enrollment
        raise FederationOperationError(
            "provider-enrollment-ineligible",
            "provider has no current approved enrollment for this report",
            "capability_id",
        )

    def publish(
        self,
        report: ProviderResourceReport,
        *,
        actor_node_id: str,
        command_id: str,
        provider_generation: int,
    ) -> ProviderHealthRecord:
        if not isinstance(report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        enrollment = self._eligible_enrollment(report, actor_node_id=actor_node_id)
        return self.store.publish(
            report,
            enrollment,
            actor_node_id=actor_node_id,
            command_id=command_id,
            provider_generation=provider_generation,
            now=self._clock(),
        )

    def fresh_reports(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        capability_type: str | None = None,
    ) -> tuple[ProviderResourceReport, ...]:
        now = _utc(self._clock(), "now")
        eligible = {
            record.capability_id: record
            for record in self.enrollments.eligible_records(
                session_id=session_id,
                actor_node_id=actor_node_id,
                capability_type=capability_type,
            )
        }
        reports: list[ProviderResourceReport] = []
        for health in self.store.list_records(
            session_id=session_id,
            capability_type=capability_type,
        ):
            enrollment = eligible.get(health.capability_id)
            if enrollment is None:
                continue
            if (
                health.enrollment_id != enrollment.enrollment_id
                or health.enrollment_revision != enrollment.revision
                or health.node_id != enrollment.node_id
                or not health.is_fresh_at(now)
            ):
                continue
            reports.append(health.report)
        return tuple(sorted(reports, key=lambda report: report.capability_id))

    def observe(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        provider_generation: int | None = None,
    ) -> ProviderHealthObservation:
        now = _utc(self._clock(), "now")
        self.enrollments.discover(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        health = self.store.get(session_id=session_id, capability_id=capability_id)
        if health is None:
            return ProviderHealthObservation(
                session_id=session_id,
                capability_id=capability_id,
                state=ProviderHealthState.ABSENT,
                reason_code="no-live-report",
                evaluated_at=now,
            )
        if provider_generation is not None:
            provider_generation = _positive(provider_generation, "provider_generation")
            if provider_generation < health.provider_generation:
                return self._observation(
                    health,
                    state=ProviderHealthState.SUPERSEDED,
                    reason_code="provider-generation-superseded",
                    now=now,
                )
        enrollment = self.enrollments.store.get(
            session_id=session_id,
            capability_id=capability_id,
        )
        if (
            enrollment is None
            or enrollment.state is not ProviderEnrollmentState.APPROVED
            or health.enrollment_id != enrollment.enrollment_id
            or health.enrollment_revision != enrollment.revision
        ):
            return self._observation(
                health,
                state=ProviderHealthState.ENROLLMENT_INELIGIBLE,
                reason_code="enrollment-not-current",
                now=now,
            )
        current_eligible = {
            record.capability_id: record
            for record in self.enrollments.eligible_records(
                session_id=session_id,
                actor_node_id=actor_node_id,
                capability_type=enrollment.capability_type,
            )
        }
        if capability_id not in current_eligible:
            return self._observation(
                health,
                state=ProviderHealthState.ANNOUNCEMENT_INELIGIBLE,
                reason_code="announcement-not-current",
                now=now,
            )
        if not health.is_fresh_at(now):
            return self._observation(
                health,
                state=ProviderHealthState.EXPIRED,
                reason_code="provider-report-expired",
                now=now,
            )
        return self._observation(
            health,
            state=ProviderHealthState.CURRENT,
            reason_code="provider-report-current",
            now=now,
        )

    @staticmethod
    def _observation(
        health: ProviderHealthRecord,
        *,
        state: ProviderHealthState,
        reason_code: str,
        now: datetime,
    ) -> ProviderHealthObservation:
        return ProviderHealthObservation(
            session_id=health.session_id,
            capability_id=health.capability_id,
            state=state,
            reason_code=reason_code,
            evaluated_at=now,
            enrollment_id=health.enrollment_id,
            enrollment_revision=health.enrollment_revision,
            provider_generation=health.provider_generation,
            report_revision=health.report_revision,
            expires_at=health.report.expires_at,
        )

    def observations(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        capability_type: str | None = None,
    ) -> tuple[ProviderHealthObservation, ...]:
        discovered = self.enrollments.discover(
            session_id=session_id,
            actor_node_id=actor_node_id,
            capability_type=capability_type,
        )
        capability_ids = {item.capability_id for item in discovered}
        capability_ids.update(
            record.capability_id
            for record in self.store.list_records(
                session_id=session_id,
                capability_type=capability_type,
            )
        )
        return tuple(
            self.observe(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=actor_node_id,
            )
            for capability_id in sorted(capability_ids)
        )

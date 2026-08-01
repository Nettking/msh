"""Transactional, restart-safe ownership for distributed capability jobs.

This module owns job scheduling state only. It deliberately does not grant
storage authority, dispatch work, interpret worker liveness, or reassign an
expired attempt. Every ownership mutation is performed by a coordinator and
is fenced by an expected durable revision plus an idempotent command ID.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar, Self

from catalog.federation.errors import FederationValidationError

from .jobs import AttemptStatus, JobAttempt, JobContract, JobStatus

JOB_STORE_SCHEMA_VERSION = 1
MAX_OWNERSHIP_LEASE_SECONDS = 60 * 60
_TERMINAL_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.TIMED_OUT,
        AttemptStatus.LOST,
    }
)


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty text without surrounding whitespace or controls",
        )
    if len(value.encode("utf-8")) > 512:
        raise FederationValidationError(
            "text-too-large", field, "must not exceed 512 UTF-8 bytes"
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
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
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware"
        )
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError(
            "invalid-timestamp", field, "must use UTC"
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
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
            "command",
            "must contain JSON-compatible finite values",
        ) from exc


def _fingerprint(operation: str, payload: dict[str, Any]) -> str:
    encoded = _canonical(
        {"operation": operation, "payload": payload}
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _lease_window(
    now: datetime,
    expires_at: datetime,
) -> tuple[datetime, datetime]:
    now = _utc(now, "now")
    expires_at = _utc(expires_at, "lease_expires_at")
    if expires_at <= now:
        raise FederationValidationError(
            "invalid-lease-expiry",
            "lease_expires_at",
            "must be later than now",
        )
    if expires_at - now > timedelta(seconds=MAX_OWNERSHIP_LEASE_SECONDS):
        raise FederationValidationError(
            "lease-too-long",
            "lease_expires_at",
            f"must not exceed {MAX_OWNERSHIP_LEASE_SECONDS} seconds",
        )
    return now, expires_at


@dataclass(frozen=True)
class OwnershipLease:
    SCHEMA: ClassVar[str] = "msh.job-ownership-lease.v1"
    job_id: str
    session_id: str
    attempt_id: str
    attempt_number: int
    owner_provider_id: str
    granted_by_coordinator_id: str
    lease_id: str
    lease_generation: int
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        for field in (
            "job_id",
            "session_id",
            "attempt_id",
            "owner_provider_id",
            "granted_by_coordinator_id",
            "lease_id",
        ):
            object.__setattr__(
                self,
                field,
                _text(getattr(self, field), field),
            )
        object.__setattr__(
            self,
            "attempt_number",
            _uint(self.attempt_number, "attempt_number"),
        )
        object.__setattr__(
            self,
            "lease_generation",
            _uint(self.lease_generation, "lease_generation"),
        )
        if self.attempt_number == 0 or self.lease_generation == 0:
            raise FederationValidationError(
                "invalid-generation",
                "attempt_number",
                "generations must start at one",
            )
        if self.owner_provider_id == self.granted_by_coordinator_id:
            raise FederationValidationError(
                "worker-self-assignment",
                "owner_provider_id",
                "the owner provider cannot grant its own ownership",
            )
        object.__setattr__(
            self,
            "lease_expires_at",
            _utc(self.lease_expires_at, "lease_expires_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "job_id": self.job_id,
            "session_id": self.session_id,
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "owner_provider_id": self.owner_provider_id,
            "granted_by_coordinator_id": self.granted_by_coordinator_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "lease_expires_at": _timestamp(self.lease_expires_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict) or value.get("schema") != cls.SCHEMA:
            raise FederationValidationError(
                "invalid-ownership-lease",
                "ownership",
                "invalid schema",
            )
        return cls(
            job_id=value["job_id"],
            session_id=value["session_id"],
            attempt_id=value["attempt_id"],
            attempt_number=value["attempt_number"],
            owner_provider_id=value["owner_provider_id"],
            granted_by_coordinator_id=value[
                "granted_by_coordinator_id"
            ],
            lease_id=value["lease_id"],
            lease_generation=value["lease_generation"],
            lease_expires_at=value["lease_expires_at"],
        )


@dataclass(frozen=True)
class DurableJobSnapshot:
    SCHEMA: ClassVar[str] = "msh.durable-job-snapshot.v1"
    job: JobContract
    revision: int
    attempt_generation: int
    ownership: OwnershipLease | None
    updated_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.job, JobContract):
            object.__setattr__(
                self,
                "job",
                JobContract.from_dict(self.job),
            )
        object.__setattr__(
            self,
            "revision",
            _uint(self.revision, "revision"),
        )
        object.__setattr__(
            self,
            "attempt_generation",
            _uint(self.attempt_generation, "attempt_generation"),
        )
        if self.attempt_generation != len(self.job.attempts):
            raise FederationValidationError(
                "attempt-generation-mismatch",
                "attempt_generation",
                "must equal the durable attempt count",
            )
        if self.ownership is not None and not isinstance(
            self.ownership,
            OwnershipLease,
        ):
            object.__setattr__(
                self,
                "ownership",
                OwnershipLease.from_dict(self.ownership),
            )
        if self.ownership is None:
            active = [
                attempt
                for attempt in self.job.attempts
                if not attempt.terminal
            ]
            if active:
                raise FederationValidationError(
                    "active-attempt-without-owner",
                    "ownership",
                    "active attempts require one durable owner",
                )
        else:
            if (
                self.ownership.job_id != self.job.job_id
                or self.ownership.session_id != self.job.session_id
            ):
                raise FederationValidationError(
                    "ownership-job-mismatch",
                    "ownership",
                    "ownership must identify the same job",
                )
            active = [
                attempt
                for attempt in self.job.attempts
                if not attempt.terminal
            ]
            if (
                len(active) != 1
                or active[0].attempt_id != self.ownership.attempt_id
            ):
                raise FederationValidationError(
                    "ownership-attempt-mismatch",
                    "ownership",
                    "ownership must identify the only active attempt",
                )
        object.__setattr__(
            self,
            "updated_at",
            _utc(self.updated_at, "updated_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "job": self.job.to_dict(),
            "revision": self.revision,
            "attempt_generation": self.attempt_generation,
            "ownership": (
                None
                if self.ownership is None
                else self.ownership.to_dict()
            ),
            "updated_at": _timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict) or value.get("schema") != cls.SCHEMA:
            raise FederationValidationError(
                "invalid-job-snapshot",
                "snapshot",
                "invalid schema",
            )
        return cls(
            job=JobContract.from_dict(value["job"]),
            revision=value["revision"],
            attempt_generation=value["attempt_generation"],
            ownership=(
                None
                if value.get("ownership") is None
                else OwnershipLease.from_dict(value["ownership"])
            ),
            updated_at=value["updated_at"],
        )


@dataclass(frozen=True)
class JobStoreResult:
    SCHEMA: ClassVar[str] = "msh.job-store-result.v1"
    snapshot: DurableJobSnapshot
    changed: bool

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, DurableJobSnapshot):
            object.__setattr__(
                self,
                "snapshot",
                DurableJobSnapshot.from_dict(self.snapshot),
            )
        if not isinstance(self.changed, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "changed",
                "must be boolean",
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "snapshot": self.snapshot.to_dict(),
            "changed": self.changed,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict) or value.get("schema") != cls.SCHEMA:
            raise FederationValidationError(
                "invalid-store-result",
                "result",
                "invalid schema",
            )
        return cls(
            snapshot=DurableJobSnapshot.from_dict(value["snapshot"]),
            changed=value["changed"],
        )


@dataclass(frozen=True)
class JobAuditEvent:
    job_id: str
    sequence: int
    event_type: str
    actor_id: str
    command_id: str
    attempt_id: str | None
    owner_provider_id: str | None
    event_at: datetime
    details: dict[str, Any]

    def __post_init__(self) -> None:
        for field in (
            "job_id",
            "event_type",
            "actor_id",
            "command_id",
        ):
            object.__setattr__(
                self,
                field,
                _text(getattr(self, field), field),
            )
        object.__setattr__(
            self,
            "sequence",
            _uint(self.sequence, "sequence"),
        )
        if self.sequence == 0:
            raise FederationValidationError(
                "invalid-sequence",
                "sequence",
                "must start at one",
            )
        if self.attempt_id is not None:
            object.__setattr__(
                self,
                "attempt_id",
                _text(self.attempt_id, "attempt_id"),
            )
        if self.owner_provider_id is not None:
            object.__setattr__(
                self,
                "owner_provider_id",
                _text(self.owner_provider_id, "owner_provider_id"),
            )
        object.__setattr__(
            self,
            "event_at",
            _utc(self.event_at, "event_at"),
        )
        _canonical(self.details)


class SQLiteJobStore:
    """Coordinator-owned SQLite state for jobs, attempts, leases, and audit."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS capability_job_store_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                INSERT OR IGNORE INTO capability_job_store_meta(key, value)
                    VALUES ('schema_version', '1');

                CREATE TABLE IF NOT EXISTS capability_jobs (
                    job_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    job_json TEXT NOT NULL CHECK(json_valid(job_json)),
                    revision INTEGER NOT NULL CHECK(revision >= 0),
                    attempt_generation INTEGER NOT NULL
                        CHECK(attempt_generation >= 0),
                    active_attempt_id TEXT,
                    active_owner_provider_id TEXT,
                    active_coordinator_id TEXT,
                    active_lease_id TEXT,
                    active_lease_generation INTEGER,
                    lease_expires_at TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE(session_id, request_id),
                    UNIQUE(session_id, idempotency_key),
                    CHECK (
                        (active_attempt_id IS NULL
                         AND active_owner_provider_id IS NULL
                         AND active_coordinator_id IS NULL
                         AND active_lease_id IS NULL
                         AND active_lease_generation IS NULL
                         AND lease_expires_at IS NULL)
                        OR
                        (active_attempt_id IS NOT NULL
                         AND active_owner_provider_id IS NOT NULL
                         AND active_coordinator_id IS NOT NULL
                         AND active_lease_id IS NOT NULL
                         AND active_lease_generation IS NOT NULL
                         AND lease_expires_at IS NOT NULL)
                    )
                );

                CREATE TABLE IF NOT EXISTS capability_job_attempts (
                    job_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL
                        CHECK(attempt_number > 0),
                    attempt_id TEXT NOT NULL,
                    owner_provider_id TEXT NOT NULL,
                    coordinator_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL UNIQUE,
                    lease_generation INTEGER NOT NULL
                        CHECK(lease_generation > 0),
                    status TEXT NOT NULL,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    terminal_at TEXT,
                    PRIMARY KEY(job_id, attempt_number),
                    UNIQUE(job_id, attempt_id),
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_commands (
                    session_id TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    result_json TEXT NOT NULL CHECK(json_valid(result_json)),
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, command_id),
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_audit (
                    job_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK(sequence > 0),
                    event_type TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    attempt_id TEXT,
                    owner_provider_id TEXT,
                    event_at TEXT NOT NULL,
                    details_json TEXT NOT NULL CHECK(json_valid(details_json)),
                    PRIMARY KEY(job_id, sequence),
                    UNIQUE(job_id, command_id),
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );
                CREATE INDEX IF NOT EXISTS capability_jobs_expired_owner
                    ON capability_jobs(lease_expires_at, job_id)
                    WHERE active_attempt_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS capability_job_audit_order
                    ON capability_job_audit(job_id, sequence);
                """
            )
            version = connection.execute(
                """SELECT value FROM capability_job_store_meta
                   WHERE key='schema_version'"""
            ).fetchone()
            if (
                version is None
                or int(version["value"]) != JOB_STORE_SCHEMA_VERSION
            ):
                raise FederationValidationError(
                    "unsupported-job-store-schema",
                    "schema_version",
                    f"expected {JOB_STORE_SCHEMA_VERSION}",
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _assert_expected_revision(
        row: sqlite3.Row,
        expected_revision: int,
    ) -> None:
        expected_revision = _uint(
            expected_revision,
            "expected_revision",
        )
        actual = int(row["revision"])
        if actual != expected_revision:
            raise FederationValidationError(
                "job-revision-conflict",
                "expected_revision",
                f"expected revision {expected_revision}, found {actual}",
            )

    @staticmethod
    def _command_replay(
        connection: sqlite3.Connection,
        *,
        session_id: str,
        command_id: str,
        job_id: str,
        operation: str,
        fingerprint: str,
    ) -> JobStoreResult | None:
        row = connection.execute(
            """SELECT * FROM capability_job_commands
               WHERE session_id=? AND command_id=?""",
            (session_id, command_id),
        ).fetchone()
        if row is None:
            return None
        if (
            row["job_id"] != job_id
            or row["operation"] != operation
            or row["fingerprint"] != fingerprint
        ):
            raise FederationValidationError(
                "command-id-conflict",
                "command_id",
                "the command ID was already used for different content",
            )
        return JobStoreResult.from_dict(
            json.loads(row["result_json"])
        )

    @staticmethod
    def _record_command(
        connection: sqlite3.Connection,
        *,
        session_id: str,
        command_id: str,
        job_id: str,
        operation: str,
        fingerprint: str,
        result: JobStoreResult,
        recorded_at: datetime,
    ) -> None:
        connection.execute(
            """INSERT INTO capability_job_commands
               (session_id, command_id, job_id, operation, fingerprint,
                result_json, recorded_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id,
                command_id,
                job_id,
                operation,
                fingerprint,
                _canonical(result.to_dict()),
                _timestamp(recorded_at),
            ),
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        *,
        job_id: str,
        event_type: str,
        actor_id: str,
        command_id: str,
        event_at: datetime,
        attempt_id: str | None = None,
        owner_provider_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        row = connection.execute(
            """SELECT COALESCE(MAX(sequence), 0) AS sequence
               FROM capability_job_audit WHERE job_id=?""",
            (job_id,),
        ).fetchone()
        sequence = int(row["sequence"]) + 1
        connection.execute(
            """INSERT INTO capability_job_audit
               (job_id, sequence, event_type, actor_id, command_id,
                attempt_id, owner_provider_id, event_at, details_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                sequence,
                event_type,
                actor_id,
                command_id,
                attempt_id,
                owner_provider_id,
                _timestamp(event_at),
                _canonical(details or {}),
            ),
        )

    @staticmethod
    def _ownership_from_row(
        row: sqlite3.Row,
    ) -> OwnershipLease | None:
        if row["active_attempt_id"] is None:
            return None
        return OwnershipLease(
            job_id=row["job_id"],
            session_id=row["session_id"],
            attempt_id=row["active_attempt_id"],
            attempt_number=int(row["active_lease_generation"]),
            owner_provider_id=row["active_owner_provider_id"],
            granted_by_coordinator_id=row["active_coordinator_id"],
            lease_id=row["active_lease_id"],
            lease_generation=int(row["active_lease_generation"]),
            lease_expires_at=row["lease_expires_at"],
        )

    def _snapshot_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> DurableJobSnapshot:
        try:
            job = JobContract.from_dict(json.loads(row["job_json"]))
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise FederationValidationError(
                "malformed-job-row",
                "job_json",
                "stored job JSON is invalid",
            ) from exc
        expected = {
            "job_id": job.job_id,
            "session_id": job.session_id,
            "request_id": job.request_id,
            "idempotency_key": job.idempotency_key,
        }
        for field, value in expected.items():
            if row[field] != value:
                raise FederationValidationError(
                    "job-row-mismatch",
                    field,
                    "indexed metadata differs from canonical job JSON",
                )
        attempt_rows = connection.execute(
            """SELECT * FROM capability_job_attempts
               WHERE job_id=? ORDER BY attempt_number""",
            (job.job_id,),
        ).fetchall()
        durable_attempts = tuple(
            JobAttempt(
                attempt_id=attempt_row["attempt_id"],
                attempt_number=int(attempt_row["attempt_number"]),
                status=attempt_row["status"],
                error_code=attempt_row["error_code"],
            )
            for attempt_row in attempt_rows
        )
        if durable_attempts != job.attempts:
            raise FederationValidationError(
                "attempt-row-mismatch",
                "attempts",
                "durable attempt rows differ from canonical job JSON",
            )
        generation = int(row["attempt_generation"])
        if generation != len(durable_attempts):
            raise FederationValidationError(
                "attempt-generation-mismatch",
                "attempt_generation",
                "attempt rows are not contiguous",
            )
        ownership = self._ownership_from_row(row)
        if (
            ownership is not None
            and ownership.attempt_number != generation
        ):
            raise FederationValidationError(
                "ownership-generation-mismatch",
                "active_lease_generation",
                "active ownership must identify the latest attempt generation",
            )
        return DurableJobSnapshot(
            job=job,
            revision=int(row["revision"]),
            attempt_generation=generation,
            ownership=ownership,
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row(
        connection: sqlite3.Connection,
        job_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM capability_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise FederationValidationError(
                "job-not-found",
                "job_id",
                "job does not exist",
            )
        return row

    def snapshot(self, job_id: str) -> DurableJobSnapshot:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            return self._snapshot_from_row(
                connection,
                self._row(connection, job_id),
            )

    def submit(
        self,
        job: JobContract,
        *,
        coordinator_id: str,
        now: datetime,
    ) -> JobStoreResult:
        if not isinstance(job, JobContract):
            raise FederationValidationError(
                "invalid-object",
                "job",
                "must be JobContract",
            )
        coordinator_id = _text(coordinator_id, "coordinator_id")
        now = _utc(now, "now")
        if job.status != JobStatus.SUBMITTED or job.attempts:
            raise FederationValidationError(
                "invalid-initial-job-state",
                "status",
                "new durable jobs must be submitted without attempts",
            )
        command_id = job.request_id
        operation = "submit"
        fingerprint = _fingerprint(
            operation,
            {
                "job": job.to_dict(),
                "coordinator_id": coordinator_id,
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                replay = self._command_replay(
                    connection,
                    session_id=job.session_id,
                    command_id=command_id,
                    job_id=job.job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                existing = connection.execute(
                    "SELECT * FROM capability_jobs WHERE job_id=?",
                    (job.job_id,),
                ).fetchone()
                if existing is not None:
                    snapshot = self._snapshot_from_row(
                        connection,
                        existing,
                    )
                    if snapshot.job.to_json() != job.to_json():
                        raise FederationValidationError(
                            "job-id-conflict",
                            "job_id",
                            "job ID identifies different content",
                        )
                    result = JobStoreResult(
                        snapshot=snapshot,
                        changed=False,
                    )
                else:
                    collision = connection.execute(
                        """SELECT job_id FROM capability_jobs
                           WHERE session_id=?
                             AND (request_id=? OR idempotency_key=?)""",
                        (
                            job.session_id,
                            job.request_id,
                            job.idempotency_key,
                        ),
                    ).fetchone()
                    if collision is not None:
                        raise FederationValidationError(
                            "job-idempotency-conflict",
                            "idempotency_key",
                            "request or idempotency identity belongs to another job",
                        )
                    connection.execute(
                        """INSERT INTO capability_jobs
                           (job_id, session_id, request_id, idempotency_key,
                            job_json, revision, attempt_generation, updated_at)
                           VALUES (?, ?, ?, ?, ?, 0, 0, ?)""",
                        (
                            job.job_id,
                            job.session_id,
                            job.request_id,
                            job.idempotency_key,
                            job.to_json(),
                            _timestamp(now),
                        ),
                    )
                    snapshot = self._snapshot_from_row(
                        connection,
                        self._row(connection, job.job_id),
                    )
                    result = JobStoreResult(
                        snapshot=snapshot,
                        changed=True,
                    )
                    self._audit(
                        connection,
                        job_id=job.job_id,
                        event_type="job-submitted",
                        actor_id=coordinator_id,
                        command_id=command_id,
                        event_at=now,
                        details={
                            "job_status": job.status.value,
                            "authority": "job-coordinator",
                        },
                    )
                self._record_command(
                    connection,
                    session_id=job.session_id,
                    command_id=command_id,
                    job_id=job.job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def queue(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        operation = "queue"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "expected_revision": expected_revision,
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, job_id)
                replay = self._command_replay(
                    connection,
                    session_id=row["session_id"],
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                if snapshot.ownership is not None:
                    raise FederationValidationError(
                        "active-job-owner",
                        "job_id",
                        "job already has active ownership",
                    )
                if snapshot.job.status == JobStatus.QUEUED:
                    result = JobStoreResult(
                        snapshot=snapshot,
                        changed=False,
                    )
                else:
                    if snapshot.job.status != JobStatus.SUBMITTED:
                        raise FederationValidationError(
                            "invalid-job-store-transition",
                            "status",
                            f"cannot queue from {snapshot.job.status.value}",
                        )
                    job = snapshot.job.transition_to(JobStatus.QUEUED)
                    revision = snapshot.revision + 1
                    connection.execute(
                        """UPDATE capability_jobs
                           SET job_json=?, revision=?, updated_at=?
                           WHERE job_id=? AND revision=?""",
                        (
                            job.to_json(),
                            revision,
                            _timestamp(now),
                            job_id,
                            snapshot.revision,
                        ),
                    )
                    updated = self._snapshot_from_row(
                        connection,
                        self._row(connection, job_id),
                    )
                    result = JobStoreResult(
                        snapshot=updated,
                        changed=True,
                    )
                    self._audit(
                        connection,
                        job_id=job_id,
                        event_type="job-queued",
                        actor_id=coordinator_id,
                        command_id=command_id,
                        event_at=now,
                        details={
                            "job_status": JobStatus.QUEUED.value,
                            "authority": "job-coordinator",
                        },
                    )
                self._record_command(
                    connection,
                    session_id=snapshot.job.session_id,
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def claim(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        lease_expires_at: datetime,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(
            owner_provider_id,
            "owner_provider_id",
        )
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        command_id = _text(command_id, "command_id")
        if coordinator_id == owner_provider_id:
            raise FederationValidationError(
                "worker-self-assignment",
                "owner_provider_id",
                "ownership must be granted by a distinct coordinator identity",
            )
        now, lease_expires_at = _lease_window(
            now,
            lease_expires_at,
        )
        operation = "claim"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "expected_revision": expected_revision,
                "lease_expires_at": _timestamp(lease_expires_at),
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, job_id)
                replay = self._command_replay(
                    connection,
                    session_id=row["session_id"],
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                if snapshot.ownership is not None:
                    raise FederationValidationError(
                        "active-job-owner",
                        "job_id",
                        "an active owner must be explicitly released or recovered first",
                    )
                if snapshot.job.status != JobStatus.QUEUED:
                    raise FederationValidationError(
                        "job-not-claimable",
                        "status",
                        "only queued jobs may be claimed in F7.3",
                    )
                generation = snapshot.attempt_generation + 1
                if generation > snapshot.job.retry_policy.max_attempts:
                    raise FederationValidationError(
                        "attempt-generation-exhausted",
                        "attempt_generation",
                        "retry policy has no remaining attempt generation",
                    )
                attempt = JobAttempt(
                    attempt_id=attempt_id,
                    attempt_number=generation,
                    status=AttemptStatus.ASSIGNED,
                )
                job = replace(
                    snapshot.job,
                    status=JobStatus.ACTIVE,
                    attempts=(*snapshot.job.attempts, attempt),
                )
                revision = snapshot.revision + 1
                connection.execute(
                    """INSERT INTO capability_job_attempts
                       (job_id, attempt_number, attempt_id, owner_provider_id,
                        coordinator_id, lease_id, lease_generation, status,
                        error_code, created_at, updated_at, terminal_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)""",
                    (
                        job_id,
                        generation,
                        attempt_id,
                        owner_provider_id,
                        coordinator_id,
                        lease_id,
                        generation,
                        attempt.status.value,
                        _timestamp(now),
                        _timestamp(now),
                    ),
                )
                updated_count = connection.execute(
                    """UPDATE capability_jobs SET
                           job_json=?, revision=?, attempt_generation=?,
                           active_attempt_id=?, active_owner_provider_id=?,
                           active_coordinator_id=?, active_lease_id=?,
                           active_lease_generation=?, lease_expires_at=?,
                           updated_at=?
                       WHERE job_id=? AND revision=?
                         AND active_attempt_id IS NULL""",
                    (
                        job.to_json(),
                        revision,
                        generation,
                        attempt_id,
                        owner_provider_id,
                        coordinator_id,
                        lease_id,
                        generation,
                        _timestamp(lease_expires_at),
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                    ),
                ).rowcount
                if updated_count != 1:
                    raise FederationValidationError(
                        "job-ownership-conflict",
                        "job_id",
                        "another owner won the claim",
                    )
                updated = self._snapshot_from_row(
                    connection,
                    self._row(connection, job_id),
                )
                result = JobStoreResult(
                    snapshot=updated,
                    changed=True,
                )
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type="ownership-granted",
                    actor_id=coordinator_id,
                    command_id=command_id,
                    attempt_id=attempt_id,
                    owner_provider_id=owner_provider_id,
                    event_at=now,
                    details={
                        "lease_id": lease_id,
                        "lease_generation": generation,
                        "lease_expires_at": _timestamp(lease_expires_at),
                        "authority": "job-coordinator",
                    },
                )
                self._record_command(
                    connection,
                    session_id=updated.job.session_id,
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise FederationValidationError(
                    "job-ownership-conflict",
                    "job_id",
                    "attempt or lease identity is already in use",
                ) from exc
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _require_active_lease(
        snapshot: DurableJobSnapshot,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        now: datetime,
    ) -> OwnershipLease:
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned",
                "job_id",
                "job has no active owner",
            )
        expected = (
            ownership.granted_by_coordinator_id,
            ownership.owner_provider_id,
            ownership.attempt_id,
            ownership.lease_id,
        )
        actual = (
            coordinator_id,
            owner_provider_id,
            attempt_id,
            lease_id,
        )
        if actual != expected:
            raise FederationValidationError(
                "job-ownership-conflict",
                "lease_id",
                "command does not match the active ownership identity",
            )
        if now >= ownership.lease_expires_at:
            raise FederationValidationError(
                "ownership-lease-expired",
                "lease_expires_at",
                "expired ownership must be recovered by the coordinator",
            )
        return ownership

    def renew(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        lease_expires_at: datetime,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(
            owner_provider_id,
            "owner_provider_id",
        )
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        command_id = _text(command_id, "command_id")
        now, lease_expires_at = _lease_window(
            now,
            lease_expires_at,
        )
        operation = "renew"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "expected_revision": expected_revision,
                "lease_expires_at": _timestamp(lease_expires_at),
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, job_id)
                replay = self._command_replay(
                    connection,
                    session_id=row["session_id"],
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                ownership = self._require_active_lease(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=owner_provider_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    now=now,
                )
                if lease_expires_at <= ownership.lease_expires_at:
                    raise FederationValidationError(
                        "nonmonotonic-lease-renewal",
                        "lease_expires_at",
                        "renewal must extend the active lease",
                    )
                revision = snapshot.revision + 1
                connection.execute(
                    """UPDATE capability_jobs
                       SET revision=?, lease_expires_at=?, updated_at=?
                       WHERE job_id=? AND revision=?
                         AND active_lease_id=?""",
                    (
                        revision,
                        _timestamp(lease_expires_at),
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                        lease_id,
                    ),
                )
                connection.execute(
                    """UPDATE capability_job_attempts SET updated_at=?
                       WHERE job_id=? AND attempt_id=?""",
                    (_timestamp(now), job_id, attempt_id),
                )
                updated = self._snapshot_from_row(
                    connection,
                    self._row(connection, job_id),
                )
                result = JobStoreResult(
                    snapshot=updated,
                    changed=True,
                )
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type="ownership-renewed",
                    actor_id=coordinator_id,
                    command_id=command_id,
                    attempt_id=attempt_id,
                    owner_provider_id=owner_provider_id,
                    event_at=now,
                    details={
                        "lease_id": lease_id,
                        "previous_expires_at": _timestamp(
                            ownership.lease_expires_at
                        ),
                        "lease_expires_at": _timestamp(lease_expires_at),
                    },
                )
                self._record_command(
                    connection,
                    session_id=updated.job.session_id,
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def record_attempt_status(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        target_status: AttemptStatus | str,
        now: datetime,
    ) -> JobStoreResult:
        try:
            target = AttemptStatus(target_status)
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "target_status",
                "unknown attempt status",
            ) from exc
        if target not in {
            AttemptStatus.ACCEPTED,
            AttemptStatus.RUNNING,
        }:
            raise FederationValidationError(
                "unsupported-f73-attempt-status",
                "target_status",
                "F7.3 only persists accepted and running progress; terminal states use complete",
            )
        return self._update_attempt(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_status=target,
            error_code=None,
            now=now,
            operation="record-attempt-status",
            event_type="attempt-status-recorded",
            terminal_job_status=None,
        )

    def complete(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        terminal_status: AttemptStatus | str,
        now: datetime,
        error_code: str | None = None,
    ) -> JobStoreResult:
        try:
            target = AttemptStatus(terminal_status)
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum",
                "terminal_status",
                "unknown attempt status",
            ) from exc
        mapping = {
            AttemptStatus.SUCCEEDED: JobStatus.SUCCEEDED,
            AttemptStatus.FAILED: JobStatus.FAILED,
            AttemptStatus.TIMED_OUT: JobStatus.TIMED_OUT,
            AttemptStatus.LOST: JobStatus.FAILED,
        }
        if target not in mapping:
            raise FederationValidationError(
                "unsupported-f73-completion",
                "terminal_status",
                "F7.3 completion supports succeeded, failed, timed-out, or lost",
            )
        return self._update_attempt(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_status=target,
            error_code=error_code,
            now=now,
            operation="complete",
            event_type="attempt-completed",
            terminal_job_status=mapping[target],
        )

    def release(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> JobStoreResult:
        return self._update_attempt(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            command_id=command_id,
            expected_revision=expected_revision,
            target_status=AttemptStatus.LOST,
            error_code="ownership-released",
            now=now,
            operation="release",
            event_type="ownership-released",
            terminal_job_status=JobStatus.FAILED,
        )

    def _update_attempt(
        self,
        *,
        job_id: str,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        command_id: str,
        expected_revision: int,
        target_status: AttemptStatus,
        error_code: str | None,
        now: datetime,
        operation: str,
        event_type: str,
        terminal_job_status: JobStatus | None,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(
            owner_provider_id,
            "owner_provider_id",
        )
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        operation = _text(operation, "operation")
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "expected_revision": expected_revision,
                "target_status": target_status.value,
                "error_code": error_code,
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, job_id)
                replay = self._command_replay(
                    connection,
                    session_id=row["session_id"],
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                self._require_active_lease(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=owner_provider_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    now=now,
                )
                index = next(
                    (
                        index
                        for index, attempt in enumerate(
                            snapshot.job.attempts
                        )
                        if attempt.attempt_id == attempt_id
                    ),
                    None,
                )
                if index is None:
                    raise FederationValidationError(
                        "attempt-not-found",
                        "attempt_id",
                        "active attempt is missing",
                    )
                current = snapshot.job.attempts[index]
                updated_attempt = current.transition_to(
                    target_status,
                    error_code=error_code,
                )
                if (
                    updated_attempt == current
                    and terminal_job_status is None
                ):
                    result = JobStoreResult(
                        snapshot=snapshot,
                        changed=False,
                    )
                else:
                    attempts = list(snapshot.job.attempts)
                    attempts[index] = updated_attempt
                    if terminal_job_status is None:
                        job = replace(
                            snapshot.job,
                            attempts=tuple(attempts),
                        )
                    else:
                        job = replace(
                            snapshot.job,
                            status=terminal_job_status,
                            attempts=tuple(attempts),
                        )
                    revision = snapshot.revision + 1
                    terminal = (
                        updated_attempt.status
                        in _TERMINAL_ATTEMPT_STATUSES
                    )
                    connection.execute(
                        """UPDATE capability_job_attempts SET
                               status=?, error_code=?, updated_at=?, terminal_at=?
                           WHERE job_id=? AND attempt_id=?""",
                        (
                            updated_attempt.status.value,
                            updated_attempt.error_code,
                            _timestamp(now),
                            _timestamp(now) if terminal else None,
                            job_id,
                            attempt_id,
                        ),
                    )
                    if terminal:
                        connection.execute(
                            """UPDATE capability_jobs SET
                                   job_json=?, revision=?, active_attempt_id=NULL,
                                   active_owner_provider_id=NULL,
                                   active_coordinator_id=NULL,
                                   active_lease_id=NULL,
                                   active_lease_generation=NULL,
                                   lease_expires_at=NULL, updated_at=?
                               WHERE job_id=? AND revision=?
                                 AND active_lease_id=?""",
                            (
                                job.to_json(),
                                revision,
                                _timestamp(now),
                                job_id,
                                snapshot.revision,
                                lease_id,
                            ),
                        )
                    else:
                        connection.execute(
                            """UPDATE capability_jobs
                               SET job_json=?, revision=?, updated_at=?
                               WHERE job_id=? AND revision=?
                                 AND active_lease_id=?""",
                            (
                                job.to_json(),
                                revision,
                                _timestamp(now),
                                job_id,
                                snapshot.revision,
                                lease_id,
                            ),
                        )
                    updated = self._snapshot_from_row(
                        connection,
                        self._row(connection, job_id),
                    )
                    result = JobStoreResult(
                        snapshot=updated,
                        changed=True,
                    )
                    self._audit(
                        connection,
                        job_id=job_id,
                        event_type=event_type,
                        actor_id=coordinator_id,
                        command_id=command_id,
                        attempt_id=attempt_id,
                        owner_provider_id=owner_provider_id,
                        event_at=now,
                        details={
                            "attempt_status": updated_attempt.status.value,
                            "job_status": updated.job.status.value,
                            "error_code": updated_attempt.error_code,
                        },
                    )
                self._record_command(
                    connection,
                    session_id=snapshot.job.session_id,
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def recover_expired(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        operation = "recover-expired"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "expected_revision": expected_revision,
                "now": _timestamp(now),
            },
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, job_id)
                replay = self._command_replay(
                    connection,
                    session_id=row["session_id"],
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                )
                if replay is not None:
                    connection.commit()
                    return replay
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                ownership = snapshot.ownership
                if ownership is None:
                    raise FederationValidationError(
                        "job-not-owned",
                        "job_id",
                        "job has no ownership to recover",
                    )
                if (
                    coordinator_id
                    != ownership.granted_by_coordinator_id
                ):
                    raise FederationValidationError(
                        "job-ownership-conflict",
                        "coordinator_id",
                        "only the granting coordinator can recover the ownership",
                    )
                if now < ownership.lease_expires_at:
                    raise FederationValidationError(
                        "ownership-lease-active",
                        "lease_expires_at",
                        "active ownership cannot be recovered as expired",
                    )
                index = next(
                    index
                    for index, attempt in enumerate(
                        snapshot.job.attempts
                    )
                    if attempt.attempt_id == ownership.attempt_id
                )
                attempt = snapshot.job.attempts[index].transition_to(
                    AttemptStatus.LOST,
                    error_code="lease-expired",
                )
                attempts = list(snapshot.job.attempts)
                attempts[index] = attempt
                job = replace(
                    snapshot.job,
                    status=JobStatus.FAILED,
                    attempts=tuple(attempts),
                )
                revision = snapshot.revision + 1
                connection.execute(
                    """UPDATE capability_job_attempts SET
                           status=?, error_code=?, updated_at=?, terminal_at=?
                       WHERE job_id=? AND attempt_id=?""",
                    (
                        attempt.status.value,
                        attempt.error_code,
                        _timestamp(now),
                        _timestamp(now),
                        job_id,
                        ownership.attempt_id,
                    ),
                )
                connection.execute(
                    """UPDATE capability_jobs SET
                           job_json=?, revision=?, active_attempt_id=NULL,
                           active_owner_provider_id=NULL,
                           active_coordinator_id=NULL,
                           active_lease_id=NULL,
                           active_lease_generation=NULL,
                           lease_expires_at=NULL, updated_at=?
                       WHERE job_id=? AND revision=?
                         AND active_lease_id=?""",
                    (
                        job.to_json(),
                        revision,
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                        ownership.lease_id,
                    ),
                )
                updated = self._snapshot_from_row(
                    connection,
                    self._row(connection, job_id),
                )
                result = JobStoreResult(
                    snapshot=updated,
                    changed=True,
                )
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type="ownership-expired",
                    actor_id=coordinator_id,
                    command_id=command_id,
                    attempt_id=ownership.attempt_id,
                    owner_provider_id=ownership.owner_provider_id,
                    event_at=now,
                    details={
                        "lease_id": ownership.lease_id,
                        "lease_expires_at": _timestamp(
                            ownership.lease_expires_at
                        ),
                        "reassignment": "deferred-to-f7.5",
                    },
                )
                self._record_command(
                    connection,
                    session_id=updated.job.session_id,
                    command_id=command_id,
                    job_id=job_id,
                    operation=operation,
                    fingerprint=fingerprint,
                    result=result,
                    recorded_at=now,
                )
                connection.commit()
                return result
            except Exception:
                connection.rollback()
                raise

    def expired(
        self,
        *,
        now: datetime,
    ) -> tuple[DurableJobSnapshot, ...]:
        now = _utc(now, "now")
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM capability_jobs
                   WHERE active_attempt_id IS NOT NULL
                     AND lease_expires_at <= ?
                   ORDER BY job_id""",
                (_timestamp(now),),
            ).fetchall()
            return tuple(
                self._snapshot_from_row(connection, row)
                for row in rows
            )

    def audit_trail(
        self,
        job_id: str,
    ) -> tuple[JobAuditEvent, ...]:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            self._row(connection, job_id)
            rows = connection.execute(
                """SELECT * FROM capability_job_audit
                   WHERE job_id=? ORDER BY sequence""",
                (job_id,),
            ).fetchall()
        return tuple(
            JobAuditEvent(
                job_id=row["job_id"],
                sequence=int(row["sequence"]),
                event_type=row["event_type"],
                actor_id=row["actor_id"],
                command_id=row["command_id"],
                attempt_id=row["attempt_id"],
                owner_provider_id=row["owner_provider_id"],
                event_at=row["event_at"],
                details=json.loads(row["details_json"]),
            )
            for row in rows
        )

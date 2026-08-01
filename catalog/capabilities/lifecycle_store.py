"""Transactional F7.5 lifecycle state layered on the F7.3 job store."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from catalog.federation.errors import FederationValidationError

from .job_store import (
    DurableJobSnapshot,
    JobStoreResult,
    OwnershipLease,
    SQLiteJobStore,
    _canonical,
    _fingerprint,
    _text,
    _timestamp,
    _utc,
)
from .jobs import ArtifactReference, AttemptStatus, JobStatus
from .lifecycle_contracts import JobHeartbeat, ResultCommit, RetryState, _positive


@dataclass(frozen=True)
class RetryMutation:
    snapshot: DurableJobSnapshot
    retry: RetryState | None


@dataclass(frozen=True)
class ResultMutation:
    snapshot: DurableJobSnapshot
    result: ResultCommit
    changed: bool


class SQLiteJobLifecycleStore(SQLiteJobStore):
    """Additive durable cancellation, retry, heartbeat, and result state."""

    def __init__(self, database: Path | str) -> None:
        super().__init__(database)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS capability_job_retry_state (
                    job_id TEXT PRIMARY KEY,
                    source_attempt_id TEXT NOT NULL,
                    source_provider_id TEXT NOT NULL,
                    source_lease_id TEXT NOT NULL,
                    source_lease_generation INTEGER NOT NULL
                        CHECK(source_lease_generation > 0),
                    error_code TEXT NOT NULL,
                    retry_not_before TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_heartbeats (
                    job_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL
                        CHECK(lease_generation > 0),
                    sequence INTEGER NOT NULL CHECK(sequence > 0),
                    occurred_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_heartbeat_commands (
                    session_id TEXT NOT NULL,
                    heartbeat_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, heartbeat_id),
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_cancellations (
                    job_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    cancellation_id TEXT NOT NULL,
                    attempt_id TEXT,
                    provider_id TEXT,
                    lease_id TEXT,
                    lease_generation INTEGER,
                    reason TEXT NOT NULL,
                    state TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    completed_at TEXT,
                    UNIQUE(session_id, cancellation_id),
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );

                CREATE TABLE IF NOT EXISTS capability_job_results (
                    job_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL
                        CHECK(lease_generation > 0),
                    reference_json TEXT NOT NULL CHECK(json_valid(reference_json)),
                    committed_at TEXT NOT NULL,
                    command_id TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES capability_jobs(job_id)
                );
                """
            )

    @staticmethod
    def _exact_ownership(
        snapshot: DurableJobSnapshot,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int | None = None,
    ) -> OwnershipLease:
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned", "job_id", "job has no active ownership"
            )
        actual = (
            coordinator_id,
            owner_provider_id,
            attempt_id,
            lease_id,
        )
        expected = (
            ownership.granted_by_coordinator_id,
            ownership.owner_provider_id,
            ownership.attempt_id,
            ownership.lease_id,
        )
        if actual != expected:
            raise FederationValidationError(
                "stale-job-attempt",
                "lease_id",
                "message does not match the active attempt and lease",
            )
        if (
            lease_generation is not None
            and lease_generation != ownership.lease_generation
        ):
            raise FederationValidationError(
                "stale-job-attempt",
                "lease_generation",
                "message uses an obsolete ownership generation",
            )
        return ownership

    @staticmethod
    def _attempt_index(snapshot: DurableJobSnapshot, attempt_id: str) -> int:
        for index, attempt in enumerate(snapshot.job.attempts):
            if attempt.attempt_id == attempt_id:
                return index
        raise FederationValidationError(
            "attempt-not-found", "attempt_id", "attempt is missing"
        )

    @staticmethod
    def _retry_from_row(row: sqlite3.Row | None) -> RetryState | None:
        if row is None:
            return None
        return RetryState(
            job_id=row["job_id"],
            source_attempt_id=row["source_attempt_id"],
            source_provider_id=row["source_provider_id"],
            source_lease_id=row["source_lease_id"],
            source_lease_generation=int(row["source_lease_generation"]),
            error_code=row["error_code"],
            retry_not_before=row["retry_not_before"],
        )

    @staticmethod
    def _result_from_row(row: sqlite3.Row) -> ResultCommit:
        return ResultCommit(
            job_id=row["job_id"],
            attempt_id=row["attempt_id"],
            provider_id=row["provider_id"],
            lease_id=row["lease_id"],
            lease_generation=int(row["lease_generation"]),
            reference=ArtifactReference.from_dict(json.loads(row["reference_json"])),
            committed_at=row["committed_at"],
        )

    def retry_state(self, job_id: str) -> RetryState | None:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM capability_job_retry_state WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return self._retry_from_row(row)

    def result_commit(self, job_id: str) -> ResultCommit | None:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM capability_job_results WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return None if row is None else self._result_from_row(row)

    def heartbeat_at(self, job_id: str) -> datetime | None:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT occurred_at FROM capability_job_heartbeats WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return None if row is None else _utc(row["occurred_at"], "occurred_at")

    def request_cancellation(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        cancellation_id: str,
        command_id: str,
        expected_revision: int,
        reason: str,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        cancellation_id = _text(cancellation_id, "cancellation_id")
        command_id = _text(command_id, "command_id")
        reason = _text(reason, "reason")
        now = _utc(now, "now")
        operation = "request-cancellation"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "cancellation_id": cancellation_id,
                "expected_revision": expected_revision,
                "reason": reason,
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
                if snapshot.job.terminal:
                    if snapshot.job.status is JobStatus.CANCELLED:
                        result = JobStoreResult(snapshot=snapshot, changed=False)
                    else:
                        raise FederationValidationError(
                            "job-terminal",
                            "status",
                            "completed jobs cannot be cancelled",
                        )
                elif snapshot.ownership is None:
                    if snapshot.job.status not in {
                        JobStatus.SUBMITTED,
                        JobStatus.QUEUED,
                        JobStatus.RETRY_WAIT,
                    }:
                        raise FederationValidationError(
                            "job-not-cancellable",
                            "status",
                            f"cannot cancel from {snapshot.job.status.value}",
                        )
                    job = replace(
                        snapshot.job,
                        status=JobStatus.CANCELLED,
                        cancellation_reason=reason,
                    )
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
                    connection.execute(
                        "DELETE FROM capability_job_retry_state WHERE job_id=?",
                        (job_id,),
                    )
                    updated = self._snapshot_from_row(
                        connection, self._row(connection, job_id)
                    )
                    result = JobStoreResult(snapshot=updated, changed=True)
                else:
                    ownership = snapshot.ownership
                    if ownership.granted_by_coordinator_id != coordinator_id:
                        raise FederationValidationError(
                            "job-ownership-conflict",
                            "coordinator_id",
                            "only the granting coordinator may cancel",
                        )
                    if snapshot.job.status is JobStatus.CANCELLATION_REQUESTED:
                        result = JobStoreResult(snapshot=snapshot, changed=False)
                    else:
                        index = self._attempt_index(snapshot, ownership.attempt_id)
                        attempts = list(snapshot.job.attempts)
                        attempts[index] = attempts[index].transition_to(
                            AttemptStatus.CANCELLATION_REQUESTED
                        )
                        job = replace(
                            snapshot.job,
                            status=JobStatus.CANCELLATION_REQUESTED,
                            attempts=tuple(attempts),
                            cancellation_reason=reason,
                        )
                        revision = snapshot.revision + 1
                        connection.execute(
                            """UPDATE capability_job_attempts
                               SET status=?, updated_at=?
                               WHERE job_id=? AND attempt_id=?""",
                            (
                                AttemptStatus.CANCELLATION_REQUESTED.value,
                                _timestamp(now),
                                job_id,
                                ownership.attempt_id,
                            ),
                        )
                        connection.execute(
                            """UPDATE capability_jobs
                               SET job_json=?, revision=?, updated_at=?
                               WHERE job_id=? AND revision=? AND active_lease_id=?""",
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
                            connection, self._row(connection, job_id)
                        )
                        result = JobStoreResult(snapshot=updated, changed=True)

                ownership = result.snapshot.ownership
                existing = connection.execute(
                    "SELECT * FROM capability_job_cancellations WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if existing is not None and (
                    existing["cancellation_id"] != cancellation_id
                    or existing["reason"] != reason
                ):
                    raise FederationValidationError(
                        "cancellation-conflict",
                        "cancellation_id",
                        "job already has a different cancellation request",
                    )
                connection.execute(
                    """INSERT INTO capability_job_cancellations
                       (job_id, session_id, cancellation_id, attempt_id,
                        provider_id, lease_id, lease_generation, reason, state,
                        requested_at, completed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(job_id) DO UPDATE SET
                         cancellation_id=excluded.cancellation_id,
                         reason=excluded.reason,
                         state=excluded.state,
                         completed_at=excluded.completed_at""",
                    (
                        job_id,
                        result.snapshot.job.session_id,
                        cancellation_id,
                        None if ownership is None else ownership.attempt_id,
                        None if ownership is None else ownership.owner_provider_id,
                        None if ownership is None else ownership.lease_id,
                        None if ownership is None else ownership.lease_generation,
                        reason,
                        (
                            "cancelled"
                            if result.snapshot.job.status is JobStatus.CANCELLED
                            else "requested"
                        ),
                        _timestamp(now),
                        (
                            _timestamp(now)
                            if result.snapshot.job.status is JobStatus.CANCELLED
                            else None
                        ),
                    ),
                )
                if result.changed:
                    self._audit(
                        connection,
                        job_id=job_id,
                        event_type=(
                            "job-cancelled"
                            if result.snapshot.job.status is JobStatus.CANCELLED
                            else "cancellation-requested"
                        ),
                        actor_id=coordinator_id,
                        command_id=command_id,
                        event_at=now,
                        attempt_id=(
                            None if ownership is None else ownership.attempt_id
                        ),
                        owner_provider_id=(
                            None if ownership is None else ownership.owner_provider_id
                        ),
                        details={
                            "cancellation_id": cancellation_id,
                            "reason": reason,
                        },
                    )
                self._record_command(
                    connection,
                    session_id=result.snapshot.job.session_id,
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

    def finalize_cancellation(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> JobStoreResult:
        return self._finish_cancellation(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            lease_generation=lease_generation,
            command_id=command_id,
            expected_revision=expected_revision,
            now=now,
            forced=False,
        )

    def force_cancellation(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        command_id: str,
        expected_revision: int,
        now: datetime,
    ) -> JobStoreResult:
        snapshot = self.snapshot(job_id)
        ownership = snapshot.ownership
        if ownership is None:
            if snapshot.job.status is JobStatus.CANCELLED:
                return JobStoreResult(snapshot=snapshot, changed=False)
            raise FederationValidationError(
                "job-not-owned",
                "job_id",
                "forced cancellation requires active ownership",
            )
        return self._finish_cancellation(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=ownership.owner_provider_id,
            attempt_id=ownership.attempt_id,
            lease_id=ownership.lease_id,
            lease_generation=ownership.lease_generation,
            command_id=command_id,
            expected_revision=expected_revision,
            now=now,
            forced=True,
        )

    def _finish_cancellation(
        self,
        *,
        job_id: str,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        now: datetime,
        forced: bool,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(owner_provider_id, "owner_provider_id")
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        lease_generation = _positive(lease_generation, "lease_generation")
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        operation = "force-cancellation" if forced else "finalize-cancellation"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "lease_generation": lease_generation,
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
                ownership = self._exact_ownership(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=owner_provider_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    lease_generation=lease_generation,
                )
                if snapshot.job.status is not JobStatus.CANCELLATION_REQUESTED:
                    raise FederationValidationError(
                        "cancellation-not-requested",
                        "status",
                        "job is not awaiting cancellation",
                    )
                index = self._attempt_index(snapshot, attempt_id)
                attempts = list(snapshot.job.attempts)
                attempts[index] = attempts[index].transition_to(
                    AttemptStatus.CANCELLED
                )
                job = replace(
                    snapshot.job,
                    status=JobStatus.CANCELLED,
                    attempts=tuple(attempts),
                )
                revision = snapshot.revision + 1
                connection.execute(
                    """UPDATE capability_job_attempts SET
                           status=?, error_code=NULL, updated_at=?, terminal_at=?
                       WHERE job_id=? AND attempt_id=?""",
                    (
                        AttemptStatus.CANCELLED.value,
                        _timestamp(now),
                        _timestamp(now),
                        job_id,
                        attempt_id,
                    ),
                )
                connection.execute(
                    """UPDATE capability_jobs SET
                           job_json=?, revision=?, active_attempt_id=NULL,
                           active_owner_provider_id=NULL,
                           active_coordinator_id=NULL, active_lease_id=NULL,
                           active_lease_generation=NULL, lease_expires_at=NULL,
                           updated_at=?
                       WHERE job_id=? AND revision=? AND active_lease_id=?""",
                    (
                        job.to_json(),
                        revision,
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                        ownership.lease_id,
                    ),
                )
                connection.execute(
                    "DELETE FROM capability_job_heartbeats WHERE job_id=?",
                    (job_id,),
                )
                connection.execute(
                    """UPDATE capability_job_cancellations
                       SET state='cancelled', completed_at=?
                       WHERE job_id=?""",
                    (_timestamp(now), job_id),
                )
                updated = self._snapshot_from_row(
                    connection, self._row(connection, job_id)
                )
                result = JobStoreResult(snapshot=updated, changed=True)
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type=(
                        "cancellation-forced"
                        if forced
                        else "cancellation-completed"
                    ),
                    actor_id=coordinator_id,
                    command_id=command_id,
                    attempt_id=attempt_id,
                    owner_provider_id=owner_provider_id,
                    event_at=now,
                    details={
                        "lease_generation": lease_generation,
                        "forced": forced,
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

    def schedule_retry(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        terminal_status: AttemptStatus | str,
        error_code: str,
        now: datetime,
    ) -> RetryMutation:
        try:
            target = AttemptStatus(terminal_status)
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-enum", "terminal_status", "unknown attempt status"
            ) from exc
        if target not in {
            AttemptStatus.FAILED,
            AttemptStatus.TIMED_OUT,
            AttemptStatus.LOST,
        }:
            raise FederationValidationError(
                "invalid-retry-terminal",
                "terminal_status",
                "must be failed, timed-out, or lost",
            )
        return self._terminalize_attempt(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            lease_generation=lease_generation,
            command_id=command_id,
            expected_revision=expected_revision,
            terminal_status=target,
            error_code=error_code,
            now=now,
            permit_retry=True,
            operation="schedule-retry",
        )

    def timeout_active_final(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        error_code: str,
        now: datetime,
    ) -> RetryMutation:
        return self._terminalize_attempt(
            job_id=job_id,
            coordinator_id=coordinator_id,
            owner_provider_id=owner_provider_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            lease_generation=lease_generation,
            command_id=command_id,
            expected_revision=expected_revision,
            terminal_status=AttemptStatus.TIMED_OUT,
            error_code=error_code,
            now=now,
            permit_retry=False,
            operation="timeout-active-final",
        )

    def _terminalize_attempt(
        self,
        *,
        job_id: str,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        terminal_status: AttemptStatus,
        error_code: str,
        now: datetime,
        permit_retry: bool,
        operation: str,
    ) -> RetryMutation:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(owner_provider_id, "owner_provider_id")
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        lease_generation = _positive(lease_generation, "lease_generation")
        command_id = _text(command_id, "command_id")
        error_code = _text(error_code, "error_code")
        now = _utc(now, "now")
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "lease_generation": lease_generation,
                "expected_revision": expected_revision,
                "terminal_status": terminal_status.value,
                "error_code": error_code,
                "permit_retry": permit_retry,
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
                    retry_row = connection.execute(
                        "SELECT * FROM capability_job_retry_state WHERE job_id=?",
                        (job_id,),
                    ).fetchone()
                    connection.commit()
                    return RetryMutation(replay.snapshot, self._retry_from_row(retry_row))
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                ownership = self._exact_ownership(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=owner_provider_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    lease_generation=lease_generation,
                )
                if snapshot.job.status is JobStatus.CANCELLATION_REQUESTED:
                    raise FederationValidationError(
                        "cancellation-in-progress",
                        "status",
                        "cancelled work cannot be retried",
                    )
                index = self._attempt_index(snapshot, attempt_id)
                attempts = list(snapshot.job.attempts)
                attempts[index] = attempts[index].transition_to(
                    terminal_status, error_code=error_code
                )
                retry_allowed = permit_retry and snapshot.job.retry_policy.allows_retry(
                    attempt_number=ownership.attempt_number,
                    error_code=error_code,
                )
                retry: RetryState | None = None
                if retry_allowed:
                    retry = RetryState(
                        job_id=job_id,
                        source_attempt_id=attempt_id,
                        source_provider_id=owner_provider_id,
                        source_lease_id=lease_id,
                        source_lease_generation=lease_generation,
                        error_code=error_code,
                        retry_not_before=now
                        + timedelta(
                            seconds=snapshot.job.retry_policy.delay_after(
                                ownership.attempt_number
                            )
                        ),
                    )
                    target_job_status = JobStatus.RETRY_WAIT
                else:
                    target_job_status = (
                        JobStatus.TIMED_OUT
                        if terminal_status is AttemptStatus.TIMED_OUT
                        else JobStatus.FAILED
                    )
                job = replace(
                    snapshot.job,
                    status=target_job_status,
                    attempts=tuple(attempts),
                )
                revision = snapshot.revision + 1
                connection.execute(
                    """UPDATE capability_job_attempts SET
                           status=?, error_code=?, updated_at=?, terminal_at=?
                       WHERE job_id=? AND attempt_id=?""",
                    (
                        terminal_status.value,
                        error_code,
                        _timestamp(now),
                        _timestamp(now),
                        job_id,
                        attempt_id,
                    ),
                )
                connection.execute(
                    """UPDATE capability_jobs SET
                           job_json=?, revision=?, active_attempt_id=NULL,
                           active_owner_provider_id=NULL,
                           active_coordinator_id=NULL, active_lease_id=NULL,
                           active_lease_generation=NULL, lease_expires_at=NULL,
                           updated_at=?
                       WHERE job_id=? AND revision=? AND active_lease_id=?""",
                    (
                        job.to_json(),
                        revision,
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                        ownership.lease_id,
                    ),
                )
                connection.execute(
                    "DELETE FROM capability_job_heartbeats WHERE job_id=?",
                    (job_id,),
                )
                connection.execute(
                    "DELETE FROM capability_job_retry_state WHERE job_id=?",
                    (job_id,),
                )
                if retry is not None:
                    connection.execute(
                        """INSERT INTO capability_job_retry_state
                           (job_id, source_attempt_id, source_provider_id,
                            source_lease_id, source_lease_generation, error_code,
                            retry_not_before, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            retry.job_id,
                            retry.source_attempt_id,
                            retry.source_provider_id,
                            retry.source_lease_id,
                            retry.source_lease_generation,
                            retry.error_code,
                            _timestamp(retry.retry_not_before),
                            _timestamp(now),
                        ),
                    )
                updated = self._snapshot_from_row(
                    connection, self._row(connection, job_id)
                )
                result = JobStoreResult(snapshot=updated, changed=True)
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type=(
                        "retry-scheduled" if retry is not None else "attempt-terminal"
                    ),
                    actor_id=coordinator_id,
                    command_id=command_id,
                    attempt_id=attempt_id,
                    owner_provider_id=owner_provider_id,
                    event_at=now,
                    details={
                        "error_code": error_code,
                        "attempt_status": terminal_status.value,
                        "job_status": target_job_status.value,
                        "retry_not_before": (
                            None
                            if retry is None
                            else _timestamp(retry.retry_not_before)
                        ),
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
                return RetryMutation(updated, retry)
            except Exception:
                connection.rollback()
                raise

    def activate_retry(
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
        operation = "activate-retry"
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
                retry_row = connection.execute(
                    "SELECT * FROM capability_job_retry_state WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if snapshot.job.status is not JobStatus.RETRY_WAIT or retry_row is None:
                    raise FederationValidationError(
                        "job-not-waiting-for-retry",
                        "status",
                        "job has no pending retry",
                    )
                if now < _utc(retry_row["retry_not_before"], "retry_not_before"):
                    raise FederationValidationError(
                        "retry-backoff-active",
                        "retry_not_before",
                        "retry backoff has not elapsed",
                    )
                job = replace(snapshot.job, status=JobStatus.QUEUED)
                revision = snapshot.revision + 1
                connection.execute(
                    """UPDATE capability_jobs
                       SET job_json=?, revision=?, updated_at=?
                       WHERE job_id=? AND revision=? AND active_attempt_id IS NULL""",
                    (
                        job.to_json(),
                        revision,
                        _timestamp(now),
                        job_id,
                        snapshot.revision,
                    ),
                )
                connection.execute(
                    "DELETE FROM capability_job_retry_state WHERE job_id=?",
                    (job_id,),
                )
                updated = self._snapshot_from_row(
                    connection, self._row(connection, job_id)
                )
                result = JobStoreResult(snapshot=updated, changed=True)
                self._audit(
                    connection,
                    job_id=job_id,
                    event_type="retry-backoff-elapsed",
                    actor_id=coordinator_id,
                    command_id=command_id,
                    event_at=now,
                    details={"job_status": JobStatus.QUEUED.value},
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

    def record_heartbeat(
        self,
        heartbeat: JobHeartbeat,
        *,
        coordinator_id: str,
        authenticated_worker_node_id: str,
        authenticated_session_id: str,
    ) -> bool:
        if not isinstance(heartbeat, JobHeartbeat):
            raise FederationValidationError(
                "invalid-heartbeat", "heartbeat", "must be JobHeartbeat"
            )
        coordinator_id = _text(coordinator_id, "coordinator_id")
        authenticated_worker_node_id = _text(
            authenticated_worker_node_id, "authenticated_worker_node_id"
        )
        authenticated_session_id = _text(
            authenticated_session_id, "authenticated_session_id"
        )
        if heartbeat.worker_node_id != authenticated_worker_node_id:
            raise FederationValidationError(
                "heartbeat-actor-mismatch",
                "worker_node_id",
                "heartbeat differs from the authenticated relay actor",
            )
        if heartbeat.session_id != authenticated_session_id:
            raise FederationValidationError(
                "heartbeat-session-mismatch",
                "session_id",
                "heartbeat differs from the authenticated relay session",
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._row(connection, heartbeat.job_id)
                snapshot = self._snapshot_from_row(connection, row)
                ownership = self._exact_ownership(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=heartbeat.provider_id,
                    attempt_id=heartbeat.attempt_id,
                    lease_id=heartbeat.lease_id,
                    lease_generation=heartbeat.lease_generation,
                )
                if heartbeat.session_id != snapshot.job.session_id:
                    raise FederationValidationError(
                        "heartbeat-session-mismatch",
                        "session_id",
                        "heartbeat belongs to another job session",
                    )
                if heartbeat.occurred_at >= ownership.lease_expires_at:
                    raise FederationValidationError(
                        "heartbeat-after-lease",
                        "occurred_at",
                        "heartbeat occurred after lease expiry",
                    )
                command = connection.execute(
                    """SELECT * FROM capability_job_heartbeat_commands
                       WHERE session_id=? AND heartbeat_id=?""",
                    (heartbeat.session_id, heartbeat.heartbeat_id),
                ).fetchone()
                if command is not None:
                    if (
                        command["job_id"] != heartbeat.job_id
                        or command["fingerprint"] != heartbeat.fingerprint()
                    ):
                        raise FederationValidationError(
                            "heartbeat-id-conflict",
                            "heartbeat_id",
                            "heartbeat ID identifies different content",
                        )
                    connection.commit()
                    return False
                current = connection.execute(
                    "SELECT * FROM capability_job_heartbeats WHERE job_id=?",
                    (heartbeat.job_id,),
                ).fetchone()
                if current is not None:
                    if (
                        current["attempt_id"],
                        current["provider_id"],
                        current["lease_id"],
                        int(current["lease_generation"]),
                    ) != (
                        heartbeat.attempt_id,
                        heartbeat.provider_id,
                        heartbeat.lease_id,
                        heartbeat.lease_generation,
                    ):
                        raise FederationValidationError(
                            "stale-heartbeat",
                            "attempt_id",
                            "heartbeat does not match the tracked attempt",
                        )
                    if heartbeat.sequence <= int(current["sequence"]):
                        raise FederationValidationError(
                            "nonmonotonic-heartbeat",
                            "sequence",
                            "heartbeat sequence must increase",
                        )
                    if heartbeat.occurred_at <= _utc(
                        current["occurred_at"], "occurred_at"
                    ):
                        raise FederationValidationError(
                            "nonmonotonic-heartbeat",
                            "occurred_at",
                            "heartbeat time must increase",
                        )
                connection.execute(
                    """INSERT INTO capability_job_heartbeats
                       (job_id, session_id, attempt_id, provider_id, lease_id,
                        lease_generation, sequence, occurred_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(job_id) DO UPDATE SET
                         session_id=excluded.session_id,
                         attempt_id=excluded.attempt_id,
                         provider_id=excluded.provider_id,
                         lease_id=excluded.lease_id,
                         lease_generation=excluded.lease_generation,
                         sequence=excluded.sequence,
                         occurred_at=excluded.occurred_at""",
                    (
                        heartbeat.job_id,
                        heartbeat.session_id,
                        heartbeat.attempt_id,
                        heartbeat.provider_id,
                        heartbeat.lease_id,
                        heartbeat.lease_generation,
                        heartbeat.sequence,
                        _timestamp(heartbeat.occurred_at),
                    ),
                )
                connection.execute(
                    """INSERT INTO capability_job_heartbeat_commands
                       (session_id, heartbeat_id, job_id, fingerprint, recorded_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        heartbeat.session_id,
                        heartbeat.heartbeat_id,
                        heartbeat.job_id,
                        heartbeat.fingerprint(),
                        _timestamp(heartbeat.occurred_at),
                    ),
                )
                connection.commit()
                return True
            except Exception:
                connection.rollback()
                raise

    def timeout_unowned(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        command_id: str,
        expected_revision: int,
        reason_code: str,
        now: datetime,
    ) -> JobStoreResult:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        command_id = _text(command_id, "command_id")
        reason_code = _text(reason_code, "reason_code")
        now = _utc(now, "now")
        operation = "timeout-unowned"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "expected_revision": expected_revision,
                "reason_code": reason_code,
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
                        "owned jobs require attempt timeout handling",
                    )
                if snapshot.job.terminal:
                    result = JobStoreResult(snapshot=snapshot, changed=False)
                else:
                    job = replace(snapshot.job, status=JobStatus.TIMED_OUT)
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
                    connection.execute(
                        "DELETE FROM capability_job_retry_state WHERE job_id=?",
                        (job_id,),
                    )
                    updated = self._snapshot_from_row(
                        connection, self._row(connection, job_id)
                    )
                    result = JobStoreResult(snapshot=updated, changed=True)
                    self._audit(
                        connection,
                        job_id=job_id,
                        event_type="job-timed-out",
                        actor_id=coordinator_id,
                        command_id=command_id,
                        event_at=now,
                        details={"reason_code": reason_code},
                    )
                self._record_command(
                    connection,
                    session_id=result.snapshot.job.session_id,
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

    def commit_result(
        self,
        job_id: str,
        *,
        coordinator_id: str,
        owner_provider_id: str,
        attempt_id: str,
        lease_id: str,
        lease_generation: int,
        command_id: str,
        expected_revision: int,
        reference: ArtifactReference,
        now: datetime,
    ) -> ResultMutation:
        job_id = _text(job_id, "job_id")
        coordinator_id = _text(coordinator_id, "coordinator_id")
        owner_provider_id = _text(owner_provider_id, "owner_provider_id")
        attempt_id = _text(attempt_id, "attempt_id")
        lease_id = _text(lease_id, "lease_id")
        lease_generation = _positive(lease_generation, "lease_generation")
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        if not isinstance(reference, ArtifactReference):
            reference = ArtifactReference.from_dict(reference)
        operation = "commit-result"
        fingerprint = _fingerprint(
            operation,
            {
                "job_id": job_id,
                "coordinator_id": coordinator_id,
                "owner_provider_id": owner_provider_id,
                "attempt_id": attempt_id,
                "lease_id": lease_id,
                "lease_generation": lease_generation,
                "expected_revision": expected_revision,
                "reference": reference.to_dict(),
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
                    result_row = connection.execute(
                        "SELECT * FROM capability_job_results WHERE job_id=?",
                        (job_id,),
                    ).fetchone()
                    if result_row is None:
                        raise FederationValidationError(
                            "result-row-missing",
                            "job_id",
                            "idempotent result command has no result row",
                        )
                    committed = self._result_from_row(result_row)
                    connection.commit()
                    return ResultMutation(replay.snapshot, committed, False)
                self._assert_expected_revision(row, expected_revision)
                snapshot = self._snapshot_from_row(connection, row)
                ownership = self._exact_ownership(
                    snapshot,
                    coordinator_id=coordinator_id,
                    owner_provider_id=owner_provider_id,
                    attempt_id=attempt_id,
                    lease_id=lease_id,
                    lease_generation=lease_generation,
                )
                if snapshot.job.status is JobStatus.CANCELLATION_REQUESTED:
                    raise FederationValidationError(
                        "result-after-cancellation",
                        "status",
                        "cancellation wins once durably requested",
                    )
                index = self._attempt_index(snapshot, attempt_id)
                attempt = snapshot.job.attempts[index]
                if attempt.status is not AttemptStatus.RUNNING:
                    raise FederationValidationError(
                        "attempt-not-running",
                        "status",
                        "only a running attempt may commit a result",
                    )
                if reference.session_id != snapshot.job.session_id:
                    raise FederationValidationError(
                        "result-session-mismatch",
                        "reference.session_id",
                        "result belongs to another session",
                    )
                if reference.reference_id not in {
                    item.reference_id for item in snapshot.job.outputs
                }:
                    raise FederationValidationError(
                        "undeclared-result-reference",
                        "reference.reference_id",
                        "result must use a declared output reference",
                    )
                existing = connection.execute(
                    "SELECT * FROM capability_job_results WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if existing is not None:
                    committed = self._result_from_row(existing)
                    if (
                        committed.attempt_id,
                        committed.provider_id,
                        committed.lease_id,
                        committed.lease_generation,
                        committed.reference,
                    ) != (
                        attempt_id,
                        owner_provider_id,
                        lease_id,
                        lease_generation,
                        reference,
                    ):
                        raise FederationValidationError(
                            "result-commit-conflict",
                            "reference",
                            "job already has a different committed result",
                        )
                    result = JobStoreResult(snapshot=snapshot, changed=False)
                else:
                    attempts = list(snapshot.job.attempts)
                    attempts[index] = attempt.transition_to(AttemptStatus.SUCCEEDED)
                    job = replace(
                        snapshot.job,
                        status=JobStatus.SUCCEEDED,
                        attempts=tuple(attempts),
                    )
                    revision = snapshot.revision + 1
                    connection.execute(
                        """INSERT INTO capability_job_results
                           (job_id, session_id, attempt_id, provider_id, lease_id,
                            lease_generation, reference_json, committed_at, command_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            job_id,
                            snapshot.job.session_id,
                            attempt_id,
                            owner_provider_id,
                            lease_id,
                            lease_generation,
                            _canonical(reference.to_dict()),
                            _timestamp(now),
                            command_id,
                        ),
                    )
                    connection.execute(
                        """UPDATE capability_job_attempts SET
                               status=?, error_code=NULL, updated_at=?, terminal_at=?
                           WHERE job_id=? AND attempt_id=?""",
                        (
                            AttemptStatus.SUCCEEDED.value,
                            _timestamp(now),
                            _timestamp(now),
                            job_id,
                            attempt_id,
                        ),
                    )
                    connection.execute(
                        """UPDATE capability_jobs SET
                               job_json=?, revision=?, active_attempt_id=NULL,
                               active_owner_provider_id=NULL,
                               active_coordinator_id=NULL, active_lease_id=NULL,
                               active_lease_generation=NULL, lease_expires_at=NULL,
                               updated_at=?
                           WHERE job_id=? AND revision=? AND active_lease_id=?""",
                        (
                            job.to_json(),
                            revision,
                            _timestamp(now),
                            job_id,
                            snapshot.revision,
                            ownership.lease_id,
                        ),
                    )
                    connection.execute(
                        "DELETE FROM capability_job_heartbeats WHERE job_id=?",
                        (job_id,),
                    )
                    updated = self._snapshot_from_row(
                        connection, self._row(connection, job_id)
                    )
                    result = JobStoreResult(snapshot=updated, changed=True)
                    committed = ResultCommit(
                        job_id=job_id,
                        attempt_id=attempt_id,
                        provider_id=owner_provider_id,
                        lease_id=lease_id,
                        lease_generation=lease_generation,
                        reference=reference,
                        committed_at=now,
                    )
                    self._audit(
                        connection,
                        job_id=job_id,
                        event_type="result-committed",
                        actor_id=coordinator_id,
                        command_id=command_id,
                        attempt_id=attempt_id,
                        owner_provider_id=owner_provider_id,
                        event_at=now,
                        details={
                            "reference_id": reference.reference_id,
                            "content_hash": reference.content_hash,
                            "lease_generation": lease_generation,
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
                return ResultMutation(result.snapshot, committed, result.changed)
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise FederationValidationError(
                    "result-commit-conflict",
                    "reference",
                    "job already has a committed result",
                ) from exc
            except Exception:
                connection.rollback()
                raise

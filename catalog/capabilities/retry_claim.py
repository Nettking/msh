"""Atomic, replay-safe retry ownership transition for F7.5."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime

from catalog.federation.errors import FederationValidationError

from .job_store import (
    JobStoreResult,
    SQLiteJobStore,
    _fingerprint,
    _lease_window,
    _text,
    _timestamp,
    _utc,
)
from .jobs import AttemptStatus, JobAttempt, JobStatus


def attempt_owner(
    store: SQLiteJobStore,
    job_id: str,
    attempt_number: int,
) -> str:
    job_id = _text(job_id, "job_id")
    if isinstance(attempt_number, bool) or not isinstance(attempt_number, int):
        raise FederationValidationError(
            "invalid-attempt-number",
            "attempt_number",
            "must be an integer",
        )
    with store._connect() as connection:
        row = connection.execute(
            """SELECT owner_provider_id FROM capability_job_attempts
               WHERE job_id=? AND attempt_number=?""",
            (job_id, attempt_number),
        ).fetchone()
    if row is None:
        raise FederationValidationError(
            "attempt-not-found",
            "attempt_number",
            "durable attempt owner is missing",
        )
    return str(row["owner_provider_id"])


def claim_retry(
    store: SQLiteJobStore,
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
    """Atomically consume backoff and create the next fenced ownership lease."""

    job_id = _text(job_id, "job_id")
    coordinator_id = _text(coordinator_id, "coordinator_id")
    owner_provider_id = _text(owner_provider_id, "owner_provider_id")
    attempt_id = _text(attempt_id, "attempt_id")
    lease_id = _text(lease_id, "lease_id")
    command_id = _text(command_id, "command_id")
    if coordinator_id == owner_provider_id:
        raise FederationValidationError(
            "worker-self-assignment",
            "owner_provider_id",
            "ownership must be granted by a distinct coordinator identity",
        )
    now, lease_expires_at = _lease_window(now, lease_expires_at)
    operation = "claim-retry"
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
    with store._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = store._row(connection, job_id)
            replay = store._command_replay(
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
            store._assert_expected_revision(row, expected_revision)
            snapshot = store._snapshot_from_row(connection, row)
            retry_row = connection.execute(
                "SELECT * FROM capability_job_retry_state WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if snapshot.ownership is not None:
                raise FederationValidationError(
                    "active-job-owner",
                    "job_id",
                    "job already has active ownership",
                )
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
                       active_lease_generation=?, lease_expires_at=?, updated_at=?
                   WHERE job_id=? AND revision=? AND active_attempt_id IS NULL""",
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
                    "another owner won the retry claim",
                )
            connection.execute(
                "DELETE FROM capability_job_retry_state WHERE job_id=?",
                (job_id,),
            )
            updated = store._snapshot_from_row(
                connection, store._row(connection, job_id)
            )
            result = JobStoreResult(snapshot=updated, changed=True)
            store._audit(
                connection,
                job_id=job_id,
                event_type="retry-ownership-granted",
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
            store._record_command(
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

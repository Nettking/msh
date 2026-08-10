from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
COORDINATOR = "coordinator-001"
WORKER = "provider-worker-001"


def _job(
    job_id: str = "job-001",
    session_id: str = "session-001",
) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{job_id}",
        idempotency_key=f"{session_id}:request-{job_id}",
        capability=CapabilityRequirement(
            capability_type="compute-worker",
            protocol="fcp-compute",
            protocol_version="1.0",
            requirements={"handler": "allowlisted-test-handler"},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"{job_id}-input",
                session_id=session_id,
                schema_name="fcp.compute-input.v1",
                media_type="application/json",
                content_hash="sha256:" + "b" * 64,
                size_bytes=64,
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=f"{job_id}-output",
                session_id=session_id,
                schema_name="fcp.compute-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=3,
            backoff_seconds=(1, 5),
            retryable_error_codes=("worker-lost",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=60,
            start_timeout_seconds=30,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    )


def _claim(
    store: SQLiteJobStore,
    *,
    job: JobContract | None = None,
    expires_at: datetime | None = None,
    lease_id: str = "lease-001",
):
    job = job or _job()
    submitted = store.submit(job, coordinator_id=COORDINATOR, now=NOW)
    queued = store.queue(
        job.job_id,
        coordinator_id=COORDINATOR,
        command_id=f"queue-{job.job_id}",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )
    return store.claim(
        job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id=f"attempt-{job.job_id}",
        lease_id=lease_id,
        command_id=f"claim-{job.job_id}",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=expires_at or NOW + timedelta(minutes=5),
        now=NOW + timedelta(seconds=2),
    )


def test_f73_024_state_and_active_lease_survive_restart(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    claimed = _claim(store)

    reopened = SQLiteJobStore(database)

    assert reopened.snapshot("job-001") == claimed.snapshot
    assert reopened.snapshot("job-001").ownership is not None


def test_f73_025_expired_query_is_deterministic_and_does_not_mutate(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(
        store,
        expires_at=NOW + timedelta(minutes=1),
    )

    assert store.expired(now=NOW + timedelta(seconds=59)) == ()
    expired = store.expired(now=NOW + timedelta(minutes=1))

    assert expired == (claimed.snapshot,)
    assert store.snapshot("job-001") == claimed.snapshot


def test_f73_026_active_lease_cannot_be_recovered_as_expired(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.recover_expired(
            "job-001",
            coordinator_id=COORDINATOR,
            command_id="recover-too-early",
            expected_revision=claimed.snapshot.revision,
            now=NOW + timedelta(minutes=1),
        )

    assert exc_info.value.code == "ownership-lease-active"


def test_f73_027_only_granting_coordinator_can_recover_expiry(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(
        store,
        expires_at=NOW + timedelta(minutes=1),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        store.recover_expired(
            "job-001",
            coordinator_id="coordinator-other",
            command_id="recover-wrong-coordinator",
            expected_revision=claimed.snapshot.revision,
            now=NOW + timedelta(minutes=2),
        )

    assert exc_info.value.code == "job-ownership-conflict"


def test_f73_028_explicit_recovery_marks_attempt_lost_without_reassignment(
    tmp_path,
) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(
        store,
        expires_at=NOW + timedelta(minutes=1),
    )

    recovered = store.recover_expired(
        "job-001",
        coordinator_id=COORDINATOR,
        command_id="recover-001",
        expected_revision=claimed.snapshot.revision,
        now=NOW + timedelta(minutes=2),
    )

    assert recovered.snapshot.job.status == JobStatus.FAILED
    assert recovered.snapshot.job.attempts[0].status == AttemptStatus.LOST
    assert recovered.snapshot.job.attempts[0].error_code == "lease-expired"
    assert recovered.snapshot.ownership is None
    assert recovered.snapshot.attempt_generation == 1
    event = store.audit_trail("job-001")[-1]
    assert event.event_type == "ownership-expired"
    assert event.details["reassignment"] == "deferred-to-f7.5"


def test_f73_029_expired_recovery_is_idempotent_across_restart(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    claimed = _claim(
        store,
        expires_at=NOW + timedelta(minutes=1),
    )
    kwargs = {
        "coordinator_id": COORDINATOR,
        "command_id": "recover-001",
        "expected_revision": claimed.snapshot.revision,
        "now": NOW + timedelta(minutes=2),
    }
    first = store.recover_expired("job-001", **kwargs)

    reopened = SQLiteJobStore(database)
    replay = reopened.recover_expired("job-001", **kwargs)

    assert replay == first
    assert [event.event_type for event in reopened.audit_trail("job-001")].count(
        "ownership-expired"
    ) == 1


def test_f73_030_expired_owner_cannot_renew_or_complete(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(
        store,
        expires_at=NOW + timedelta(minutes=1),
    )

    with pytest.raises(FederationValidationError) as renew_error:
        store.renew(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-job-001",
            lease_id="lease-001",
            command_id="renew-expired",
            expected_revision=claimed.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=3),
            now=NOW + timedelta(minutes=2),
        )
    assert renew_error.value.code == "ownership-lease-expired"

    with pytest.raises(FederationValidationError) as completion_error:
        store.complete(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-job-001",
            lease_id="lease-001",
            command_id="complete-expired",
            expected_revision=claimed.snapshot.revision,
            terminal_status=AttemptStatus.FAILED,
            error_code="late-message",
            now=NOW + timedelta(minutes=2),
        )
    assert completion_error.value.code == "ownership-lease-expired"


def test_f73_031_lease_identity_is_unique_across_jobs(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    _claim(store, job=_job("job-001"), lease_id="shared-lease")
    second = _job("job-002")
    submitted = store.submit(second, coordinator_id=COORDINATOR, now=NOW)
    queued = store.queue(
        second.job_id,
        coordinator_id=COORDINATOR,
        command_id="queue-job-002",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        store.claim(
            second.job_id,
            coordinator_id=COORDINATOR,
            owner_provider_id="provider-worker-002",
            attempt_id="attempt-job-002",
            lease_id="shared-lease",
            command_id="claim-job-002",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=2),
        )

    assert exc_info.value.code == "job-ownership-conflict"
    assert store.snapshot(second.job_id).ownership is None


def test_f73_032_audit_trail_is_append_only_and_monotonic(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(store)
    renewed = store.renew(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-job-001",
        lease_id="lease-001",
        command_id="renew-001",
        expected_revision=claimed.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=10),
        now=NOW + timedelta(minutes=1),
    )
    store.release(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-job-001",
        lease_id="lease-001",
        command_id="release-001",
        expected_revision=renewed.snapshot.revision,
        now=NOW + timedelta(minutes=2),
    )

    events = store.audit_trail("job-001")

    assert tuple(event.sequence for event in events) == tuple(
        range(1, len(events) + 1)
    )
    assert tuple(event.event_type for event in events) == (
        "job-submitted",
        "job-queued",
        "ownership-granted",
        "ownership-renewed",
        "ownership-released",
    )
    assert all(event.actor_id == COORDINATOR for event in events)


def test_f73_033_job_ownership_contains_no_storage_authority_semantics(
    tmp_path,
) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claim(store)
    ownership = claimed.snapshot.ownership
    assert ownership is not None

    encoded = json.dumps(ownership.to_dict(), sort_keys=True)

    assert "storage" not in encoded
    assert "primary" not in encoded
    assert "replica" not in encoded
    assert ownership.granted_by_coordinator_id == COORDINATOR


def test_f73_034_tampered_attempt_row_is_detected(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    _claim(store)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """UPDATE capability_job_attempts
               SET status='running' WHERE job_id='job-001'"""
        )

    with pytest.raises(FederationValidationError) as exc_info:
        store.snapshot("job-001")

    assert exc_info.value.code == "attempt-row-mismatch"


def test_f73_035_tampered_attempt_generation_is_detected(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    _claim(store)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """UPDATE capability_jobs
               SET attempt_generation=7 WHERE job_id='job-001'"""
        )

    with pytest.raises(FederationValidationError) as exc_info:
        store.snapshot("job-001")

    assert exc_info.value.code == "attempt-generation-mismatch"


def test_f73_036_tampered_indexed_job_identity_is_detected(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    submitted = store.submit(_job(), coordinator_id=COORDINATOR, now=NOW)
    value = submitted.snapshot.job.to_dict()
    value["job_id"] = "job-tampered"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE capability_jobs SET job_json=? WHERE job_id='job-001'",
            (json.dumps(value, sort_keys=True, separators=(",", ":")),),
        )

    with pytest.raises(FederationValidationError) as exc_info:
        store.snapshot("job-001")

    assert exc_info.value.code == "job-row-mismatch"


def test_f73_037_unknown_store_schema_version_fails_closed(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    SQLiteJobStore(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """UPDATE capability_job_store_meta SET value='2'
               WHERE key='schema_version'"""
        )

    with pytest.raises(FederationValidationError) as exc_info:
        SQLiteJobStore(database)

    assert exc_info.value.code == "unsupported-job-store-schema"


def test_f73_038_command_ids_are_scoped_to_session(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    first = _job("job-001", "session-001")
    second = _job("job-002", "session-002")
    first = JobContract.from_dict(
        {**first.to_dict(), "request_id": "request-shared"}
    )
    second = JobContract.from_dict(
        {**second.to_dict(), "request_id": "request-shared"}
    )

    first_result = store.submit(first, coordinator_id=COORDINATOR, now=NOW)
    second_result = store.submit(second, coordinator_id=COORDINATOR, now=NOW)

    assert first_result.snapshot.job.session_id == "session-001"
    assert second_result.snapshot.job.session_id == "session-002"

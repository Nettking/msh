from __future__ import annotations

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


def _sha256(character: str = "a") -> str:
    return "sha256:" + character * 64


def _job(
    job_id: str = "job-001",
    session_id: str = "session-001",
    request_id: str | None = None,
    idempotency_key: str | None = None,
    **overrides: object,
) -> JobContract:
    request_id = request_id or f"request-{job_id}"
    idempotency_key = idempotency_key or f"{session_id}:{request_id}"
    values: dict[str, object] = {
        "job_id": job_id,
        "session_id": session_id,
        "request_id": request_id,
        "idempotency_key": idempotency_key,
        "capability": CapabilityRequirement(
            capability_type="language-model",
            protocol="ollama-chat",
            protocol_version="1.0",
            requirements={
                "model": "qwen3-vl:4b",
                "modalities": ["text", "vision"],
            },
        ),
        "inputs": (
            ArtifactReference(
                reference_id=f"{job_id}-input",
                session_id=session_id,
                schema_name="fcp.prompt.v1",
                media_type="application/json",
                content_hash=_sha256("a"),
                size_bytes=128,
            ),
        ),
        "outputs": (
            ArtifactReference(
                reference_id=f"{job_id}-output",
                session_id=session_id,
                schema_name="fcp.ai-response.v1",
                media_type="application/json",
            ),
        ),
        "retry_policy": RetryPolicy(
            max_attempts=3,
            backoff_seconds=(1, 5),
            retryable_error_codes=("worker-lost", "provider-unavailable"),
        ),
        "timeout_policy": TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=60,
            start_timeout_seconds=30,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    }
    values.update(overrides)
    return JobContract(**values)  # type: ignore[arg-type]


def _submitted(store: SQLiteJobStore, job: JobContract | None = None):
    job = job or _job()
    return store.submit(job, coordinator_id=COORDINATOR, now=NOW)


def _queued(store: SQLiteJobStore, job: JobContract | None = None):
    submitted = _submitted(store, job)
    return store.queue(
        submitted.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        command_id=f"queue-{submitted.snapshot.job.job_id}",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )


def _claimed(
    store: SQLiteJobStore,
    job: JobContract | None = None,
    *,
    lease_id: str = "lease-001",
    attempt_id: str = "attempt-001",
):
    queued = _queued(store, job)
    return store.claim(
        queued.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id=attempt_id,
        lease_id=lease_id,
        command_id=f"claim-{queued.snapshot.job.job_id}",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=5),
        now=NOW + timedelta(seconds=2),
    )


def test_f73_001_submit_is_durable_and_canonical(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")

    result = _submitted(store)

    assert result.changed
    assert result.snapshot.revision == 0
    assert result.snapshot.attempt_generation == 0
    assert result.snapshot.ownership is None
    assert store.snapshot("job-001") == result.snapshot


def test_f73_002_duplicate_submit_replays_original_result(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    first = _submitted(store)

    second = _submitted(store)

    assert second == first
    assert len(store.audit_trail("job-001")) == 1


def test_f73_003_job_id_conflict_fails_closed(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    _submitted(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.submit(
            _job(request_id="request-conflict"),
            coordinator_id=COORDINATOR,
            now=NOW,
        )

    assert exc_info.value.code == "job-id-conflict"


def test_f73_004_idempotency_identity_cannot_move_to_another_job(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    original = _job()
    _submitted(store, original)

    with pytest.raises(FederationValidationError) as exc_info:
        store.submit(
            _job(
                job_id="job-002",
                request_id="request-job-002",
                idempotency_key=original.idempotency_key,
            ),
            coordinator_id=COORDINATOR,
            now=NOW,
        )

    assert exc_info.value.code == "job-idempotency-conflict"


def test_f73_005_only_fresh_submitted_jobs_can_enter_store(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")

    with pytest.raises(FederationValidationError) as exc_info:
        store.submit(
            _job(status=JobStatus.QUEUED),
            coordinator_id=COORDINATOR,
            now=NOW,
        )

    assert exc_info.value.code == "invalid-initial-job-state"


def test_f73_006_queue_is_revisioned_and_idempotent(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    submitted = _submitted(store)

    queued = store.queue(
        "job-001",
        coordinator_id=COORDINATOR,
        command_id="queue-001",
        expected_revision=0,
        now=NOW + timedelta(seconds=1),
    )
    replay = store.queue(
        "job-001",
        coordinator_id=COORDINATOR,
        command_id="queue-001",
        expected_revision=0,
        now=NOW + timedelta(seconds=1),
    )

    assert submitted.snapshot.job.status == JobStatus.SUBMITTED
    assert queued.snapshot.job.status == JobStatus.QUEUED
    assert queued.snapshot.revision == 1
    assert replay == queued
    assert [event.event_type for event in store.audit_trail("job-001")] == [
        "job-submitted",
        "job-queued",
    ]


def test_f73_007_command_id_reuse_with_other_content_is_rejected(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    _submitted(store)
    store.queue(
        "job-001",
        coordinator_id=COORDINATOR,
        command_id="queue-001",
        expected_revision=0,
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(FederationValidationError) as exc_info:
        store.queue(
            "job-001",
            coordinator_id=COORDINATOR,
            command_id="queue-001",
            expected_revision=1,
            now=NOW + timedelta(seconds=2),
        )

    assert exc_info.value.code == "command-id-conflict"


def test_f73_008_stale_revision_fails_before_mutation(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    queued = _queued(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.claim(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="claim-stale",
            expected_revision=queued.snapshot.revision - 1,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=2),
        )

    assert exc_info.value.code == "job-revision-conflict"
    assert store.snapshot("job-001") == queued.snapshot


def test_f73_009_worker_cannot_grant_itself_ownership(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    queued = _queued(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.claim(
            "job-001",
            coordinator_id=WORKER,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="claim-self",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=2),
        )

    assert exc_info.value.code == "worker-self-assignment"


def test_f73_010_claim_creates_one_durable_attempt_and_owner(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")

    claimed = _claimed(store)

    assert claimed.snapshot.job.status == JobStatus.ACTIVE
    assert claimed.snapshot.revision == 2
    assert claimed.snapshot.attempt_generation == 1
    assert len(claimed.snapshot.job.attempts) == 1
    assert claimed.snapshot.job.attempts[0].status == AttemptStatus.ASSIGNED
    assert claimed.snapshot.ownership is not None
    assert claimed.snapshot.ownership.owner_provider_id == WORKER
    assert claimed.snapshot.ownership.granted_by_coordinator_id == COORDINATOR


def test_f73_011_duplicate_claim_does_not_add_attempt_or_audit(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    queued = _queued(store)
    kwargs = {
        "coordinator_id": COORDINATOR,
        "owner_provider_id": WORKER,
        "attempt_id": "attempt-001",
        "lease_id": "lease-001",
        "command_id": "claim-001",
        "expected_revision": queued.snapshot.revision,
        "lease_expires_at": NOW + timedelta(minutes=5),
        "now": NOW + timedelta(seconds=2),
    }

    first = store.claim("job-001", **kwargs)
    second = store.claim("job-001", **kwargs)

    assert second == first
    assert second.snapshot.attempt_generation == 1
    assert [event.event_type for event in store.audit_trail("job-001")].count(
        "ownership-granted"
    ) == 1


def test_f73_012_competing_claim_cannot_create_second_owner(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    first_store = SQLiteJobStore(database)
    second_store = SQLiteJobStore(database)
    claimed = _claimed(first_store)

    with pytest.raises(FederationValidationError) as exc_info:
        second_store.claim(
            "job-001",
            coordinator_id="coordinator-002",
            owner_provider_id="provider-worker-002",
            attempt_id="attempt-002",
            lease_id="lease-002",
            command_id="claim-002",
            expected_revision=claimed.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=6),
            now=NOW + timedelta(seconds=3),
        )

    assert exc_info.value.code == "active-job-owner"
    snapshot = first_store.snapshot("job-001")
    assert snapshot.attempt_generation == 1
    assert snapshot.ownership is not None
    assert snapshot.ownership.owner_provider_id == WORKER


def test_f73_013_nonqueued_job_is_not_claimable(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    submitted = _submitted(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.claim(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="claim-001",
            expected_revision=submitted.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=1),
        )

    assert exc_info.value.code == "job-not-claimable"


def test_f73_014_leases_are_finite_and_bounded(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    queued = _queued(store)

    with pytest.raises(FederationValidationError) as expired:
        store.claim(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="claim-expired",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW,
            now=NOW,
        )
    assert expired.value.code == "invalid-lease-expiry"

    with pytest.raises(FederationValidationError) as excessive:
        store.claim(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="claim-long",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW + timedelta(hours=2),
            now=NOW,
        )
    assert excessive.value.code == "lease-too-long"


def test_f73_015_renewal_extends_lease_and_revision(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    renewed = store.renew(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="renew-001",
        expected_revision=claimed.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=10),
        now=NOW + timedelta(minutes=1),
    )

    assert renewed.snapshot.revision == 3
    assert renewed.snapshot.ownership is not None
    assert renewed.snapshot.ownership.lease_expires_at == NOW + timedelta(minutes=10)


def test_f73_016_renewal_must_be_monotonic(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.renew(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="renew-shorter",
            expected_revision=claimed.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=4),
            now=NOW + timedelta(minutes=1),
        )

    assert exc_info.value.code == "nonmonotonic-lease-renewal"


def test_f73_017_wrong_owner_or_lease_cannot_mutate_attempt(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.record_attempt_status(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id="provider-worker-other",
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="accepted-wrong-owner",
            expected_revision=claimed.snapshot.revision,
            target_status=AttemptStatus.ACCEPTED,
            now=NOW + timedelta(minutes=1),
        )

    assert exc_info.value.code == "job-ownership-conflict"


def test_f73_018_attempt_progress_is_durable_and_ordered(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    accepted = store.record_attempt_status(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="accepted-001",
        expected_revision=claimed.snapshot.revision,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(minutes=1),
    )
    running = store.record_attempt_status(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="running-001",
        expected_revision=accepted.snapshot.revision,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(minutes=2),
    )

    assert accepted.snapshot.job.attempts[0].status == AttemptStatus.ACCEPTED
    assert running.snapshot.job.attempts[0].status == AttemptStatus.RUNNING
    assert running.snapshot.revision == 4


def test_f73_019_attempt_cannot_skip_required_safety_states(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    with pytest.raises(FederationValidationError) as exc_info:
        store.complete(
            "job-001",
            coordinator_id=COORDINATOR,
            owner_provider_id=WORKER,
            attempt_id="attempt-001",
            lease_id="lease-001",
            command_id="complete-too-early",
            expected_revision=claimed.snapshot.revision,
            terminal_status=AttemptStatus.SUCCEEDED,
            now=NOW + timedelta(minutes=1),
        )

    assert exc_info.value.code == "invalid-attempt-transition"


def test_f73_020_successful_completion_clears_owner_atomically(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)
    accepted = store.record_attempt_status(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="accepted-001",
        expected_revision=claimed.snapshot.revision,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(minutes=1),
    )
    running = store.record_attempt_status(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="running-001",
        expected_revision=accepted.snapshot.revision,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(minutes=2),
    )

    completed = store.complete(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="complete-001",
        expected_revision=running.snapshot.revision,
        terminal_status=AttemptStatus.SUCCEEDED,
        now=NOW + timedelta(minutes=3),
    )

    assert completed.snapshot.job.status == JobStatus.SUCCEEDED
    assert completed.snapshot.job.attempts[0].status == AttemptStatus.SUCCEEDED
    assert completed.snapshot.ownership is None


def test_f73_021_failed_completion_can_finish_assigned_attempt(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)

    completed = store.complete(
        "job-001",
        coordinator_id=COORDINATOR,
        owner_provider_id=WORKER,
        attempt_id="attempt-001",
        lease_id="lease-001",
        command_id="failed-001",
        expected_revision=claimed.snapshot.revision,
        terminal_status=AttemptStatus.FAILED,
        error_code="provider-rejected",
        now=NOW + timedelta(minutes=1),
    )

    assert completed.snapshot.job.status == JobStatus.FAILED
    assert completed.snapshot.job.attempts[0].error_code == "provider-rejected"
    assert completed.snapshot.ownership is None


def test_f73_022_release_is_idempotent_and_terminal_for_f73(tmp_path) -> None:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite")
    claimed = _claimed(store)
    kwargs = {
        "coordinator_id": COORDINATOR,
        "owner_provider_id": WORKER,
        "attempt_id": "attempt-001",
        "lease_id": "lease-001",
        "command_id": "release-001",
        "expected_revision": claimed.snapshot.revision,
        "now": NOW + timedelta(minutes=1),
    }

    first = store.release("job-001", **kwargs)
    replay = store.release("job-001", **kwargs)

    assert replay == first
    assert first.snapshot.job.status == JobStatus.FAILED
    assert first.snapshot.job.attempts[0].status == AttemptStatus.LOST
    assert first.snapshot.job.attempts[0].error_code == "ownership-released"
    assert first.snapshot.ownership is None
    assert [event.event_type for event in store.audit_trail("job-001")].count(
        "ownership-released"
    ) == 1


def test_f73_023_duplicate_completion_after_restart_is_deduplicated(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite"
    store = SQLiteJobStore(database)
    claimed = _claimed(store)
    kwargs = {
        "coordinator_id": COORDINATOR,
        "owner_provider_id": WORKER,
        "attempt_id": "attempt-001",
        "lease_id": "lease-001",
        "command_id": "failed-001",
        "expected_revision": claimed.snapshot.revision,
        "terminal_status": AttemptStatus.FAILED,
        "error_code": "provider-rejected",
        "now": NOW + timedelta(minutes=1),
    }
    first = store.complete("job-001", **kwargs)

    reopened = SQLiteJobStore(database)
    replay = reopened.complete("job-001", **kwargs)

    assert replay == first
    assert [event.event_type for event in reopened.audit_trail("job-001")].count(
        "attempt-completed"
    ) == 1

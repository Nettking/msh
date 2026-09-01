from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.jobs import (
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.provider_selection import evaluate_provider_candidate
from catalog.capabilities.retry_claim import claim_retry
from catalog.capabilities.update_drain import (
    MAX_DRAIN_PROVIDER_IDS,
    NodeUpdateDrainState,
    NodeUpdateDrainTarget,
    SQLiteNodeUpdateDrainStore,
)
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
SESSION = "session-drain-1"
NODE = "node-a"
PROVIDER = "provider-a"
COORDINATOR = "coordinator"


def _job(job_id: str = "job-drain-1") -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=SESSION,
        request_id=f"request-{job_id}",
        idempotency_key=f"{SESSION}:request-{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={},
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(0,),
            retryable_error_codes=("retryable",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=120,
            start_timeout_seconds=60,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    )


def _report(
    *,
    provider_id: str = PROVIDER,
    node_id: str = NODE,
    status: ProviderStatus = ProviderStatus.READY,
    active_jobs: int = 1,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id=SESSION,
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        status=status,
        report_revision=7,
        max_concurrent_jobs=4,
        active_jobs=active_jobs,
        queue_depth=2,
        utilization_millis=500,
        attributes={"kind": "synthetic"},
        reported_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def _queued(store: SQLiteJobLifecycleStore, job_id: str = "job-drain-1"):
    job = _job(job_id)
    submitted = store.submit(job, coordinator_id=COORDINATOR, now=NOW)
    return store.queue(
        job.job_id,
        coordinator_id=COORDINATOR,
        command_id=f"queue-{job_id}",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )


def _claim(
    store: SQLiteJobLifecycleStore,
    provider_id: str = PROVIDER,
    job_id: str = "job-drain-1",
):
    queued = _queued(store, job_id)
    return store.claim(
        queued.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=provider_id,
        attempt_id=f"attempt-{job_id}-1",
        lease_id=f"lease-{job_id}-1",
        command_id=f"claim-{job_id}-1",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=5),
        now=NOW + timedelta(seconds=2),
    )


def _target(*provider_ids: str) -> NodeUpdateDrainTarget:
    return NodeUpdateDrainTarget(NODE, tuple(provider_ids or (PROVIDER,)))


def _request_drain(
    drains: SQLiteNodeUpdateDrainStore,
    *,
    command_id: str = "drain-1",
    target: NodeUpdateDrainTarget | None = None,
):
    return drains.request_drain(
        session_id=SESSION,
        target=target or _target(),
        command_id=command_id,
        now=NOW + timedelta(seconds=3),
    )


def test_drain_projection_preserves_capacity_and_rejects_new_selection(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    target = _target()
    ready = _report(active_jobs=1)

    _request_drain(drains, target=target)
    draining = drains.project_report(ready)

    assert draining.status is ProviderStatus.DRAINING
    assert draining.active_jobs == ready.active_jobs == 1
    assert draining.queue_depth == ready.queue_depth == 2
    assert draining.max_concurrent_jobs == ready.max_concurrent_jobs == 4
    assert draining.utilization_millis == ready.utilization_millis
    assert draining.attributes == ready.attributes
    assert draining.protocol == ready.protocol
    candidate = evaluate_provider_candidate(_job(), draining, evaluated_at=NOW)
    assert not candidate.eligible
    assert "provider-status-draining" in candidate.reasons


def test_drain_does_not_weaken_stronger_state_or_touch_other_identity(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    _request_drain(drains)

    unavailable = _report(status=ProviderStatus.UNAVAILABLE)
    other_node = _report(node_id="node-b")
    other_provider = _report(provider_id="provider-b")

    assert drains.project_report(unavailable) is unavailable
    assert drains.project_report(other_node) is other_node
    assert drains.project_report(other_provider) is other_provider


def test_drain_state_is_durable_idempotent_and_command_fenced(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite3"
    jobs = SQLiteJobLifecycleStore(database)
    drains = SQLiteNodeUpdateDrainStore(jobs)

    first = _request_drain(drains)
    replay = _request_drain(drains)

    assert first.record.state is NodeUpdateDrainState.DRAINING
    assert first.record.revision == 1
    assert replay.record == first.record
    assert replay.changed == first.changed

    reopened_jobs = SQLiteJobLifecycleStore(database)
    reopened = SQLiteNodeUpdateDrainStore(reopened_jobs)
    assert reopened.get(session_id=SESSION, node_id=NODE) == first.record

    with pytest.raises(FederationValidationError) as conflict:
        _request_drain(
            reopened,
            command_id="drain-1",
            target=_target(PROVIDER, "provider-b"),
        )
    assert conflict.value.code == "update-drain-command-conflict"


def test_drain_commit_before_claim_atomically_rejects_ownership(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite3"
    jobs = SQLiteJobLifecycleStore(database)
    queued = _queued(jobs)
    drains = SQLiteNodeUpdateDrainStore(jobs)
    _request_drain(drains)

    with pytest.raises(FederationValidationError):
        jobs.claim(
            queued.snapshot.job.job_id,
            coordinator_id=COORDINATOR,
            owner_provider_id=PROVIDER,
            attempt_id="attempt-after-drain",
            lease_id="lease-after-drain",
            command_id="claim-after-drain",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=4),
        )

    snapshot = jobs.snapshot(queued.snapshot.job.job_id)
    assert snapshot.job.status is JobStatus.QUEUED
    assert snapshot.attempt_generation == 0
    assert snapshot.ownership is None
    assert snapshot.job.attempts == ()


def test_stale_ready_selection_cannot_claim_after_concurrent_drain(tmp_path) -> None:
    """Force READY-read -> drain-commit -> claim on separate SQLite connections."""

    database = tmp_path / "jobs.sqlite3"
    setup = SQLiteJobLifecycleStore(database)
    queued = _queued(setup)
    SQLiteNodeUpdateDrainStore(setup)  # installs the durable claim trigger

    selected = threading.Event()
    drained = threading.Event()
    failures: list[Exception] = []

    def claimant() -> None:
        jobs = SQLiteJobLifecycleStore(database)
        candidate = evaluate_provider_candidate(_job(), _report(), evaluated_at=NOW)
        assert candidate.eligible
        selected.set()
        assert drained.wait(timeout=10)
        try:
            jobs.claim(
                queued.snapshot.job.job_id,
                coordinator_id=COORDINATOR,
                owner_provider_id=PROVIDER,
                attempt_id="attempt-stale-selection",
                lease_id="lease-stale-selection",
                command_id="claim-stale-selection",
                expected_revision=queued.snapshot.revision,
                lease_expires_at=NOW + timedelta(minutes=5),
                now=NOW + timedelta(seconds=5),
            )
        except Exception as exc:  # expected: durable drain wins the admission race
            failures.append(exc)

    def drainer() -> None:
        assert selected.wait(timeout=10)
        jobs = SQLiteJobLifecycleStore(database)
        drains = SQLiteNodeUpdateDrainStore(jobs)
        _request_drain(drains, command_id="drain-race")
        drained.set()

    claim_thread = threading.Thread(target=claimant)
    drain_thread = threading.Thread(target=drainer)
    claim_thread.start()
    drain_thread.start()
    claim_thread.join(timeout=15)
    drain_thread.join(timeout=15)

    assert not claim_thread.is_alive()
    assert not drain_thread.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], FederationValidationError)
    snapshot = setup.snapshot(queued.snapshot.job.job_id)
    assert snapshot.ownership is None
    assert snapshot.attempt_generation == 0


def test_claim_commit_before_drain_remains_owned_and_can_finish(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite3"
    jobs = SQLiteJobLifecycleStore(database)
    drains = SQLiteNodeUpdateDrainStore(jobs)
    claimed = _claim(jobs)
    ownership = claimed.snapshot.ownership
    assert ownership is not None

    drain = _request_drain(drains, command_id="drain-after-claim")
    evidence = drain.record.target.active_ownerships(jobs)

    assert len(evidence) == 1
    assert evidence[0].job_id == claimed.snapshot.job.job_id
    assert evidence[0].attempt_id == ownership.attempt_id
    assert evidence[0].provider_id == PROVIDER
    assert evidence[0].lease_id == ownership.lease_id
    assert evidence[0].lease_generation == ownership.lease_generation
    assert not drain.record.target.is_quiescent(jobs)

    renewed = jobs.renew(
        claimed.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=PROVIDER,
        attempt_id=ownership.attempt_id,
        lease_id=ownership.lease_id,
        command_id="renew-during-drain",
        expected_revision=claimed.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=6),
        now=NOW + timedelta(minutes=1),
    )
    accepted = jobs.record_attempt_status(
        claimed.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=PROVIDER,
        attempt_id=ownership.attempt_id,
        lease_id=ownership.lease_id,
        command_id="accepted-during-drain",
        expected_revision=renewed.snapshot.revision,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(minutes=2),
    )
    running = jobs.record_attempt_status(
        claimed.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=PROVIDER,
        attempt_id=ownership.attempt_id,
        lease_id=ownership.lease_id,
        command_id="running-during-drain",
        expected_revision=accepted.snapshot.revision,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(minutes=3),
    )
    completed = jobs.complete(
        claimed.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=PROVIDER,
        attempt_id=ownership.attempt_id,
        lease_id=ownership.lease_id,
        command_id="complete-during-drain",
        expected_revision=running.snapshot.revision,
        terminal_status=AttemptStatus.SUCCEEDED,
        now=NOW + timedelta(minutes=4),
    )

    assert completed.snapshot.job.status is JobStatus.SUCCEEDED
    assert completed.snapshot.ownership is None
    assert drain.record.target.is_quiescent(jobs)


def test_retry_claim_is_fenced_by_same_durable_drain(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    claimed = _claim(jobs)
    ownership = claimed.snapshot.ownership
    assert ownership is not None

    retry = jobs.schedule_retry(
        claimed.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=PROVIDER,
        attempt_id=ownership.attempt_id,
        lease_id=ownership.lease_id,
        lease_generation=ownership.lease_generation,
        command_id="schedule-retry-before-drain",
        expected_revision=claimed.snapshot.revision,
        terminal_status=AttemptStatus.FAILED,
        error_code="retryable",
        now=NOW + timedelta(seconds=10),
    )
    assert retry.retry is not None
    assert retry.snapshot.job.status is JobStatus.RETRY_WAIT

    _request_drain(drains, command_id="drain-before-retry-claim")
    with pytest.raises(FederationValidationError):
        claim_retry(
            jobs,
            retry.snapshot.job.job_id,
            coordinator_id=COORDINATOR,
            owner_provider_id=PROVIDER,
            attempt_id="attempt-retry-2",
            lease_id="lease-retry-2",
            command_id="claim-retry-during-drain",
            expected_revision=retry.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=11),
        )

    snapshot = jobs.snapshot(retry.snapshot.job.job_id)
    assert snapshot.job.status is JobStatus.RETRY_WAIT
    assert snapshot.attempt_generation == 1
    assert snapshot.ownership is None
    assert jobs.retry_state(snapshot.job.job_id) is not None


def test_restart_preserves_drain_and_still_blocks_claim(tmp_path) -> None:
    database = tmp_path / "jobs.sqlite3"
    jobs = SQLiteJobLifecycleStore(database)
    queued = _queued(jobs)
    drains = SQLiteNodeUpdateDrainStore(jobs)
    persisted = _request_drain(drains, command_id="drain-before-restart").record

    reopened_jobs = SQLiteJobLifecycleStore(database)
    reopened_drains = SQLiteNodeUpdateDrainStore(reopened_jobs)

    assert reopened_drains.get(session_id=SESSION, node_id=NODE) == persisted
    assert reopened_drains.project_report(_report()).status is ProviderStatus.DRAINING
    with pytest.raises(FederationValidationError):
        reopened_jobs.claim(
            queued.snapshot.job.job_id,
            coordinator_id=COORDINATOR,
            owner_provider_id=PROVIDER,
            attempt_id="attempt-after-restart",
            lease_id="lease-after-restart",
            command_id="claim-after-restart",
            expected_revision=queued.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=20),
        )


def test_quiescence_fails_closed_on_inconsistent_durable_attempt(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    target = _target()
    claimed = _claim(jobs)
    ownership = claimed.snapshot.ownership
    assert ownership is not None

    with jobs._connect() as connection:
        connection.execute(
            """UPDATE capability_job_attempts
               SET status=?
               WHERE job_id=? AND attempt_id=?""",
            (
                AttemptStatus.SUCCEEDED.value,
                claimed.snapshot.job.job_id,
                ownership.attempt_id,
            ),
        )
        connection.commit()

    with pytest.raises(FederationValidationError):
        target.active_ownerships(jobs)


def test_clear_is_revision_fenced_and_does_not_mutate_job_ownership(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    claimed = _claim(jobs)
    first = _request_drain(drains, command_id="drain-clear")

    with pytest.raises(FederationValidationError) as stale:
        drains.clear_drain(
            session_id=SESSION,
            node_id=NODE,
            expected_revision=first.record.revision + 1,
            now=NOW + timedelta(minutes=1),
        )
    assert stale.value.code == "update-drain-revision-conflict"
    assert jobs.snapshot(claimed.snapshot.job.job_id).ownership is not None

    cleared = drains.clear_drain(
        session_id=SESSION,
        node_id=NODE,
        expected_revision=first.record.revision,
        now=NOW + timedelta(minutes=1),
    )
    assert cleared.record.state is NodeUpdateDrainState.READY
    assert jobs.snapshot(claimed.snapshot.job.job_id).ownership is not None


def test_provider_identity_set_is_nonempty_bounded_and_unique() -> None:
    with pytest.raises(FederationValidationError) as empty:
        NodeUpdateDrainTarget(NODE, ())
    assert empty.value.code == "empty-drain-provider-set"

    with pytest.raises(FederationValidationError) as duplicate:
        NodeUpdateDrainTarget(NODE, (PROVIDER, PROVIDER))
    assert duplicate.value.code == "duplicate-drain-provider"

    with pytest.raises(FederationValidationError) as oversized:
        NodeUpdateDrainTarget(
            NODE,
            tuple(f"provider-{index}" for index in range(MAX_DRAIN_PROVIDER_IDS + 1)),
        )
    assert oversized.value.code == "too-many-drain-providers"

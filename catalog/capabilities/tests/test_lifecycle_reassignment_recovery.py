from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_coordinator import JobLifecycleCoordinator
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _job() -> JobContract:
    return JobContract(
        job_id="job-reassign-restart",
        session_id="session-reassign-restart",
        request_id="request-reassign-restart",
        idempotency_key="session-reassign-restart:request",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo"},
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(2,),
            retryable_error_codes=("worker-heartbeat-lost",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    )


def _report(provider_id: str, node_id: str) -> ProviderResourceReport:
    evaluated_at = NOW + timedelta(seconds=8)
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id="session-reassign-restart",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=1,
        max_concurrent_jobs=1,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=100,
        attributes={"operation": "echo"},
        reported_at=evaluated_at - timedelta(seconds=1),
        expires_at=evaluated_at + timedelta(seconds=30),
    )


def test_retry_reassignment_replays_after_coordinator_restart(
    tmp_path: Path,
) -> None:
    database = tmp_path / "jobs.sqlite3"
    store = SQLiteJobLifecycleStore(database)
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=NOW)
    store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id="queue",
        expected_revision=0,
        now=NOW,
    )
    store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-a",
        lease_id="lease-a",
        command_id="claim-a",
        expected_revision=1,
        lease_expires_at=NOW + timedelta(seconds=60),
        now=NOW,
    )
    coordinator = JobLifecycleCoordinator(
        store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=5,
    )
    coordinator.evaluate(
        job.job_id,
        now=NOW + timedelta(seconds=6),
        command_prefix="loss-a",
    )
    reports = (
        _report("provider-a", "node-a"),
        _report("provider-b", "node-b"),
    )
    first = coordinator.reassign(
        job.job_id,
        reports=reports,
        policy=None,
        attempt_id="attempt-b",
        lease_id="lease-b",
        lease_expires_at=NOW + timedelta(seconds=50),
        command_prefix="retry-b",
        now=NOW + timedelta(seconds=8),
    )

    restarted_store = SQLiteJobLifecycleStore(database)
    restarted = JobLifecycleCoordinator(
        restarted_store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=5,
    ).reassign(
        job.job_id,
        reports=reports,
        policy=None,
        attempt_id="attempt-b",
        lease_id="lease-b",
        lease_expires_at=NOW + timedelta(seconds=50),
        command_prefix="retry-b",
        now=NOW + timedelta(seconds=8),
    )

    assert restarted.snapshot == first.snapshot
    assert restarted.selection.selected_capability_id == "provider-b"
    assert restarted.target_node_id == "node-b"
    assert len(restarted.snapshot.job.attempts) == 2
    assert restarted.snapshot.ownership is not None
    assert restarted.snapshot.ownership.lease_generation == 2

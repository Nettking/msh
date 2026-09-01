from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.update_drain import NodeUpdateDrainTarget, SQLiteNodeUpdateDrainStore
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 9, 1, 13, 0, tzinfo=timezone.utc)
NODE = "node-a"
COORDINATOR = "coordinator"
SESSION_A = "session-a"
SESSION_B = "session-b"


def _job(*, session_id: str, job_id: str) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{session_id}-{job_id}",
        idempotency_key=f"{session_id}:request-{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={},
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_seconds=(),
            retryable_error_codes=(),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=120,
            start_timeout_seconds=60,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    )


def _queued(store: SQLiteJobLifecycleStore, *, session_id: str, job_id: str):
    job = _job(session_id=session_id, job_id=job_id)
    submitted = store.submit(job, coordinator_id=COORDINATOR, now=NOW)
    return store.queue(
        job.job_id,
        coordinator_id=COORDINATOR,
        command_id=f"queue-{session_id}-{job_id}",
        expected_revision=submitted.snapshot.revision,
        now=NOW + timedelta(seconds=1),
    )


def _claim(
    store: SQLiteJobLifecycleStore,
    *,
    session_id: str,
    job_id: str,
    provider_id: str,
):
    queued = _queued(store, session_id=session_id, job_id=job_id)
    return store.claim(
        queued.snapshot.job.job_id,
        coordinator_id=COORDINATOR,
        owner_provider_id=provider_id,
        attempt_id=f"attempt-{session_id}-{job_id}",
        lease_id=f"lease-{session_id}-{job_id}",
        command_id=f"claim-{session_id}-{job_id}",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=NOW + timedelta(minutes=5),
        now=NOW + timedelta(seconds=2),
    )


def _report(
    *,
    session_id: str,
    provider_id: str,
    node_id: str = NODE,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id=session_id,
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=1,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={},
        reported_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def _request(
    drains: SQLiteNodeUpdateDrainStore,
    *,
    session_id: str = SESSION_A,
    provider_ids: tuple[str, ...] = ("provider-a",),
):
    return drains.request_drain(
        session_id=session_id,
        target=NodeUpdateDrainTarget(NODE, provider_ids),
        command_id=f"drain-{session_id}-{'-'.join(provider_ids)}",
        now=NOW + timedelta(seconds=3),
    )


def test_claim_fence_is_scoped_to_the_drained_session(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    _request(drains, session_id=SESSION_A, provider_ids=("provider-a",))

    queued_a = _queued(jobs, session_id=SESSION_A, job_id="job-a")
    with pytest.raises(FederationValidationError):
        jobs.claim(
            queued_a.snapshot.job.job_id,
            coordinator_id=COORDINATOR,
            owner_provider_id="provider-a",
            attempt_id="attempt-a",
            lease_id="lease-a",
            command_id="claim-a",
            expected_revision=queued_a.snapshot.revision,
            lease_expires_at=NOW + timedelta(minutes=5),
            now=NOW + timedelta(seconds=4),
        )

    other_session = _claim(
        jobs,
        session_id=SESSION_B,
        job_id="job-b",
        provider_id="provider-a",
    )
    assert other_session.snapshot.ownership is not None
    assert other_session.snapshot.ownership.owner_provider_id == "provider-a"
    assert other_session.snapshot.job.session_id == SESSION_B


def test_health_projection_is_scoped_to_session_node_and_provider_set(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    _request(drains, session_id=SESSION_A, provider_ids=("provider-a", "provider-b"))

    assert (
        drains.project_report(_report(session_id=SESSION_A, provider_id="provider-a")).status
        is ProviderStatus.DRAINING
    )
    assert (
        drains.project_report(_report(session_id=SESSION_A, provider_id="provider-b")).status
        is ProviderStatus.DRAINING
    )
    assert (
        drains.project_report(_report(session_id=SESSION_A, provider_id="provider-c")).status
        is ProviderStatus.READY
    )
    assert (
        drains.project_report(_report(session_id=SESSION_B, provider_id="provider-a")).status
        is ProviderStatus.READY
    )
    assert (
        drains.project_report(
            _report(session_id=SESSION_A, provider_id="provider-a", node_id="node-b")
        ).status
        is ProviderStatus.READY
    )


def test_multi_provider_node_fences_every_target_but_not_unrelated_provider(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    _request(drains, provider_ids=("provider-a", "provider-b"))

    for suffix, provider_id in (("a", "provider-a"), ("b", "provider-b")):
        queued = _queued(jobs, session_id=SESSION_A, job_id=f"job-{suffix}")
        with pytest.raises(FederationValidationError):
            jobs.claim(
                queued.snapshot.job.job_id,
                coordinator_id=COORDINATOR,
                owner_provider_id=provider_id,
                attempt_id=f"attempt-{suffix}",
                lease_id=f"lease-{suffix}",
                command_id=f"claim-{suffix}",
                expected_revision=queued.snapshot.revision,
                lease_expires_at=NOW + timedelta(minutes=5),
                now=NOW + timedelta(seconds=4),
            )

    unrelated = _claim(
        jobs,
        session_id=SESSION_A,
        job_id="job-c",
        provider_id="provider-c",
    )
    assert unrelated.snapshot.ownership is not None
    assert unrelated.snapshot.ownership.owner_provider_id == "provider-c"


def test_clearing_one_session_does_not_change_another_session_identity(tmp_path) -> None:
    jobs = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    drains = SQLiteNodeUpdateDrainStore(jobs)
    first = _request(drains, session_id=SESSION_A, provider_ids=("provider-a",))

    cleared = drains.clear_drain(
        session_id=SESSION_A,
        node_id=NODE,
        expected_revision=first.record.revision,
        now=NOW + timedelta(seconds=5),
    )
    assert cleared.changed
    assert (
        drains.project_report(_report(session_id=SESSION_A, provider_id="provider-a")).status
        is ProviderStatus.READY
    )
    assert drains.get(session_id=SESSION_B, node_id=NODE) is None

    claimed = _claim(
        jobs,
        session_id=SESSION_A,
        job_id="job-after-clear",
        provider_id="provider-a",
    )
    assert claimed.snapshot.ownership is not None

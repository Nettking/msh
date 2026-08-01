from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.dispatch import (
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
)
from catalog.capabilities.jobs import (
    ArtifactReference,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_contracts import (
    CancellationResponse,
    CancellationState,
    CompletionDisposition,
    LifecycleAction,
)
from catalog.capabilities.lifecycle_coordinator import (
    JobLifecycleCoordinator,
    ResilientDispatchCoordinator,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _job(
    *,
    job_id: str,
    overall_timeout_seconds: int = 120,
    cancellation_grace_seconds: int = 5,
) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id="session-races",
        request_id=f"request-{job_id}",
        idempotency_key=f"session-races:{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="msh-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo"},
        ),
        inputs=(),
        outputs=(
            ArtifactReference(
                reference_id=f"output-{job_id}",
                session_id="session-races",
                schema_name="msh.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_seconds=(),
            retryable_error_codes=(),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=overall_timeout_seconds,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=cancellation_grace_seconds,
        ),
    )


def _owned(
    store: SQLiteJobLifecycleStore,
    job: JobContract,
    *,
    lease_expires_at: datetime,
) -> None:
    store.submit(job, coordinator_id="coordinator", now=NOW)
    store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id=f"queue-{job.job_id}",
        expected_revision=0,
        now=NOW,
    )
    store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-a",
        lease_id="lease-a",
        command_id=f"claim-{job.job_id}",
        expected_revision=1,
        lease_expires_at=lease_expires_at,
        now=NOW,
    )


class LostThenAcceptedCancellationTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def request(self, **_kwargs):
        raise AssertionError("dispatch is not used")

    async def cancel(self, *, request, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError("reply lost")
        return CancellationResponse(
            cancellation_id=request.cancellation_id,
            session_id=request.session_id,
            provider_id=request.provider_id,
            job_id=request.job_id,
            attempt_id=request.attempt_id,
            state=CancellationState.NOT_RUNNING,
            occurred_at=NOW + timedelta(seconds=3),
        )


def test_cancellation_replays_after_lost_reply_without_new_revision(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
        job = _job(job_id="job-cancel-replay")
        _owned(store, job, lease_expires_at=NOW + timedelta(seconds=60))
        transport = LostThenAcceptedCancellationTransport()
        coordinator = ResilientDispatchCoordinator(
            store,
            transport,
            coordinator_node_id="coordinator",
        )

        pending = await coordinator.cancel(
            job.job_id,
            cancellation_id="cancel-replay",
            target_node_id="node-a",
            reason="operator-request",
            now=NOW + timedelta(seconds=1),
        )
        completed = await coordinator.cancel(
            job.job_id,
            cancellation_id="cancel-replay",
            target_node_id="node-a",
            reason="operator-request",
            now=NOW + timedelta(seconds=2),
        )

        assert pending.job.status is JobStatus.CANCELLATION_REQUESTED
        assert completed.job.status is JobStatus.CANCELLED
        assert completed.job.cancellation_reason == "operator-request"
        assert transport.calls == 2

    asyncio.run(scenario())


def test_durable_cancellation_precedes_overall_timeout(
    tmp_path: Path,
) -> None:
    store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    job = _job(
        job_id="job-cancel-precedence",
        overall_timeout_seconds=5,
        cancellation_grace_seconds=10,
    )
    _owned(store, job, lease_expires_at=NOW + timedelta(seconds=60))
    requested = store.request_cancellation(
        job.job_id,
        coordinator_id="coordinator",
        cancellation_id="cancel-precedence",
        command_id="cancel-precedence:request",
        expected_revision=2,
        reason="operator-request",
        now=NOW + timedelta(seconds=1),
    ).snapshot
    coordinator = JobLifecycleCoordinator(
        store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=30,
    )

    while_grace_active = coordinator.evaluate(
        job.job_id,
        now=NOW + timedelta(seconds=6),
        command_prefix="cancel-race",
    )
    after_grace = coordinator.evaluate(
        job.job_id,
        now=NOW + timedelta(seconds=12),
        command_prefix="cancel-race",
    )

    assert requested.job.status is JobStatus.CANCELLATION_REQUESTED
    assert while_grace_active.action is LifecycleAction.NONE
    assert while_grace_active.snapshot.job.status is JobStatus.CANCELLATION_REQUESTED
    assert after_grace.action is LifecycleAction.CANCELLATION_FORCED
    assert after_grace.snapshot.job.status is JobStatus.CANCELLED


def test_terminal_worker_event_at_lease_expiry_is_stale(
    tmp_path: Path,
) -> None:
    store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
    job = _job(job_id="job-expired-result")
    lease_expires_at = NOW + timedelta(seconds=10)
    _owned(store, job, lease_expires_at=lease_expires_at)
    request = DispatchRequest.from_snapshot(
        store.snapshot(job.job_id),
        dispatch_id="dispatch-expired",
        target_node_id="node-a",
        sent_at=NOW,
    )
    result_reference = ArtifactReference(
        reference_id="output-job-expired-result",
        session_id="session-races",
        schema_name="msh.synthetic-output.v1",
        media_type="application/json",
        content_hash="sha256:" + "c" * 64,
        size_bytes=64,
    )
    response = DispatchResponse(
        dispatch_id=request.dispatch_id,
        session_id=request.session_id,
        job_id=request.job.job_id,
        provider_id=request.provider_id,
        attempt_id=request.attempt_id,
        events=(
            DispatchEvent(1, DispatchState.ACCEPTED, NOW + timedelta(seconds=1)),
            DispatchEvent(2, DispatchState.RUNNING, NOW + timedelta(seconds=2)),
            DispatchEvent(
                3,
                DispatchState.SUCCEEDED,
                lease_expires_at,
                details={"result_reference": result_reference.to_dict()},
            ),
        ),
    )

    outcome = ResilientDispatchCoordinator(
        store,
        LostThenAcceptedCancellationTransport(),
        coordinator_node_id="coordinator",
    ).apply_response(request, response)

    assert outcome.disposition is CompletionDisposition.STALE_IGNORED
    assert outcome.snapshot.job.status is JobStatus.ACTIVE
    assert store.result_commit(job.job_id) is None

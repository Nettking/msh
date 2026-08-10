from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.dispatch import (
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
    ExecutionResult,
    WorkerRegistration,
)
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_contracts import (
    CancellationRequest,
    CancellationState,
    CompletionDisposition,
    JobHeartbeat,
    LifecycleAction,
)
from catalog.capabilities.lifecycle_coordinator import (
    JobLifecycleCoordinator,
    ResilientDispatchCoordinator,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.lifecycle_worker import (
    CancellableCapabilityWorker,
    SQLiteLifecycleDispatchInbox,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderSelectionPolicy,
    ProviderStatus,
)
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def _job(**overrides: object) -> JobContract:
    values: dict[str, object] = {
        "job_id": "job-lifecycle-1",
        "session_id": "session-lifecycle-1",
        "request_id": "request-lifecycle-1",
        "idempotency_key": "session-lifecycle-1:request-lifecycle-1",
        "capability": CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        "inputs": (),
        "outputs": (
            ArtifactReference(
                reference_id="output-lifecycle-1",
                session_id="session-lifecycle-1",
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        "retry_policy": RetryPolicy(
            max_attempts=3,
            backoff_seconds=(2, 5),
            retryable_error_codes=(
                "worker-heartbeat-lost",
                "lease-expired",
                "start-timeout",
                "run-timeout",
                "transient-worker-error",
            ),
        ),
        "timeout_policy": TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    }
    values.update(overrides)
    return JobContract(**values)  # type: ignore[arg-type]


def _store(tmp_path: Path) -> SQLiteJobLifecycleStore:
    return SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")


def _claim(
    store: SQLiteJobLifecycleStore,
    *,
    provider_id: str = "provider-a",
    attempt_id: str = "attempt-1",
    lease_id: str = "lease-1",
    now: datetime = NOW,
) -> None:
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=now)
    store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id="queue-1",
        expected_revision=0,
        now=now,
    )
    store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id=provider_id,
        attempt_id=attempt_id,
        lease_id=lease_id,
        command_id="claim-1",
        expected_revision=1,
        lease_expires_at=now + timedelta(seconds=60),
        now=now,
    )


def _running(store: SQLiteJobLifecycleStore) -> None:
    store.record_attempt_status(
        "job-lifecycle-1",
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="accepted-1",
        expected_revision=2,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(seconds=1),
    )
    store.record_attempt_status(
        "job-lifecycle-1",
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="running-1",
        expected_revision=3,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(seconds=2),
    )


def _result_reference() -> ArtifactReference:
    return ArtifactReference(
        reference_id="output-lifecycle-1",
        session_id="session-lifecycle-1",
        schema_name="fcp.synthetic-output.v1",
        media_type="application/json",
        content_hash="sha256:" + "a" * 64,
        size_bytes=128,
    )


def _report(
    provider_id: str,
    node_id: str,
    *,
    now: datetime,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id="session-lifecycle-1",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=1,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=100,
        attributes={"operation": "echo", "modalities": ["json", "text"]},
        reported_at=now - timedelta(seconds=1),
        expires_at=now + timedelta(seconds=30),
    )


def test_f75_cancel_before_claim_is_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=NOW)

    first = store.request_cancellation(
        job.job_id,
        coordinator_id="coordinator",
        cancellation_id="cancel-1",
        command_id="cancel-1:request",
        expected_revision=0,
        reason="operator-request",
        now=NOW + timedelta(seconds=1),
    )
    replay = store.request_cancellation(
        job.job_id,
        coordinator_id="coordinator",
        cancellation_id="cancel-1",
        command_id="cancel-1:request",
        expected_revision=0,
        reason="operator-request",
        now=NOW + timedelta(seconds=1),
    )

    assert first.snapshot.job.status is JobStatus.CANCELLED
    assert first.snapshot.job.cancellation_reason == "operator-request"
    assert replay == first
    assert store.snapshot(job.job_id).ownership is None


def test_f75_active_cancellation_finalizes_and_fences_completion(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _claim(store)
    requested = store.request_cancellation(
        "job-lifecycle-1",
        coordinator_id="coordinator",
        cancellation_id="cancel-active",
        command_id="cancel-active:request",
        expected_revision=2,
        reason="operator-request",
        now=NOW + timedelta(seconds=2),
    ).snapshot

    assert requested.job.status is JobStatus.CANCELLATION_REQUESTED
    assert requested.job.attempts[-1].status is AttemptStatus.CANCELLATION_REQUESTED

    cancelled = store.finalize_cancellation(
        "job-lifecycle-1",
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="cancel-active:complete",
        expected_revision=3,
        now=NOW + timedelta(seconds=3),
    ).snapshot

    assert cancelled.job.status is JobStatus.CANCELLED
    assert cancelled.job.attempts[-1].status is AttemptStatus.CANCELLED
    assert cancelled.ownership is None

    with pytest.raises(FederationValidationError) as stale:
        store.commit_result(
            "job-lifecycle-1",
            coordinator_id="coordinator",
            owner_provider_id="provider-a",
            attempt_id="attempt-1",
            lease_id="lease-1",
            lease_generation=1,
            command_id="late-result",
            expected_revision=4,
            reference=_result_reference(),
            now=NOW + timedelta(seconds=4),
        )
    assert stale.value.code == "job-not-owned"


def test_f75_retry_backoff_and_reassignment_exclude_old_provider(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _claim(store)
    coordinator = JobLifecycleCoordinator(
        store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=5,
    )

    decision = coordinator.evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=6),
        command_prefix="loss-1",
    )

    assert decision.action is LifecycleAction.RETRY_SCHEDULED
    assert decision.reason_code == "worker-heartbeat-lost"
    assert decision.snapshot.job.status is JobStatus.RETRY_WAIT
    assert decision.retry is not None
    assert decision.retry.retry_not_before == NOW + timedelta(seconds=8)

    with pytest.raises(FederationValidationError) as early:
        coordinator.reassign(
            "job-lifecycle-1",
            reports=(
                _report("provider-a", "node-a", now=NOW + timedelta(seconds=7)),
                _report("provider-b", "node-b", now=NOW + timedelta(seconds=7)),
            ),
            policy=ProviderSelectionPolicy(),
            attempt_id="attempt-2",
            lease_id="lease-2",
            lease_expires_at=NOW + timedelta(seconds=50),
            command_prefix="retry-1",
            now=NOW + timedelta(seconds=7),
        )
    assert early.value.code == "retry-backoff-active"

    outcome = coordinator.reassign(
        "job-lifecycle-1",
        reports=(
            _report("provider-a", "node-a", now=NOW + timedelta(seconds=8)),
            _report("provider-b", "node-b", now=NOW + timedelta(seconds=8)),
        ),
        policy=ProviderSelectionPolicy(),
        attempt_id="attempt-2",
        lease_id="lease-2",
        lease_expires_at=NOW + timedelta(seconds=50),
        command_prefix="retry-1",
        now=NOW + timedelta(seconds=8),
    )

    assert outcome.selection.selected_capability_id == "provider-b"
    assert outcome.target_node_id == "node-b"
    assert outcome.snapshot.ownership is not None
    assert outcome.snapshot.ownership.lease_generation == 2
    assert outcome.snapshot.job.attempts[0].status is AttemptStatus.LOST
    assert outcome.snapshot.job.attempts[1].status is AttemptStatus.ASSIGNED


def test_f75_heartbeat_authentication_and_sequence_are_fail_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _claim(store)
    heartbeat = JobHeartbeat(
        heartbeat_id="heartbeat-1",
        session_id="session-lifecycle-1",
        worker_node_id="node-a",
        provider_id="provider-a",
        job_id="job-lifecycle-1",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        sequence=1,
        occurred_at=NOW + timedelta(seconds=1),
    )

    assert store.record_heartbeat(
        heartbeat,
        coordinator_id="coordinator",
        authenticated_worker_node_id="node-a",
        authenticated_session_id="session-lifecycle-1",
    )
    assert not store.record_heartbeat(
        heartbeat,
        coordinator_id="coordinator",
        authenticated_worker_node_id="node-a",
        authenticated_session_id="session-lifecycle-1",
    )

    with pytest.raises(FederationValidationError) as wrong_actor:
        store.record_heartbeat(
            replace_heartbeat(heartbeat, heartbeat_id="heartbeat-2"),
            coordinator_id="coordinator",
            authenticated_worker_node_id="node-b",
            authenticated_session_id="session-lifecycle-1",
        )
    assert wrong_actor.value.code == "heartbeat-actor-mismatch"

    with pytest.raises(FederationValidationError) as repeated_sequence:
        store.record_heartbeat(
            replace_heartbeat(heartbeat, heartbeat_id="heartbeat-3"),
            coordinator_id="coordinator",
            authenticated_worker_node_id="node-a",
            authenticated_session_id="session-lifecycle-1",
        )
    assert repeated_sequence.value.code == "nonmonotonic-heartbeat"


def replace_heartbeat(
    heartbeat: JobHeartbeat,
    **changes: object,
) -> JobHeartbeat:
    values = heartbeat.to_dict()
    values.pop("schema")
    values.pop("protocol")
    values.update(changes)
    return JobHeartbeat(**values)  # type: ignore[arg-type]


def test_f75_timeout_policy_covers_queue_start_run_and_overall(
    tmp_path: Path,
) -> None:
    queue_store = SQLiteJobLifecycleStore(tmp_path / "queue.sqlite3")
    queue_store.submit(_job(), coordinator_id="coordinator", now=NOW)
    queue_store.queue(
        "job-lifecycle-1",
        coordinator_id="coordinator",
        command_id="queue",
        expected_revision=0,
        now=NOW,
    )
    queue_decision = JobLifecycleCoordinator(
        queue_store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=100,
    ).evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=21),
        command_prefix="queue-timeout",
    )
    assert queue_decision.action is LifecycleAction.FINAL_TIMEOUT
    assert queue_decision.reason_code == "queue-timeout"

    start_store = SQLiteJobLifecycleStore(tmp_path / "start.sqlite3")
    _claim(start_store)
    start_decision = JobLifecycleCoordinator(
        start_store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=100,
    ).evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=11),
        command_prefix="start-timeout",
    )
    assert start_decision.reason_code == "start-timeout"
    assert start_decision.action is LifecycleAction.RETRY_SCHEDULED

    run_store = SQLiteJobLifecycleStore(tmp_path / "run.sqlite3")
    _claim(run_store)
    _running(run_store)
    run_decision = JobLifecycleCoordinator(
        run_store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=100,
    ).evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=33),
        command_prefix="run-timeout",
    )
    assert run_decision.reason_code == "run-timeout"
    assert run_decision.action is LifecycleAction.RETRY_SCHEDULED

    overall_store = SQLiteJobLifecycleStore(tmp_path / "overall.sqlite3")
    overall_store.submit(_job(), coordinator_id="coordinator", now=NOW)
    overall = JobLifecycleCoordinator(
        overall_store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=100,
    ).evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=121),
        command_prefix="overall-timeout",
    )
    assert overall.reason_code == "overall-timeout"
    assert overall.action is LifecycleAction.FINAL_TIMEOUT


def test_f75_one_result_commit_and_duplicate_or_late_completion(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _claim(store)
    request = DispatchRequest.from_snapshot(
        store.snapshot("job-lifecycle-1"),
        dispatch_id="dispatch-1",
        target_node_id="node-a",
        sent_at=NOW,
    )
    response = DispatchResponse(
        dispatch_id="dispatch-1",
        session_id="session-lifecycle-1",
        job_id="job-lifecycle-1",
        provider_id="provider-a",
        attempt_id="attempt-1",
        events=(
            DispatchEvent(1, DispatchState.ACCEPTED, NOW + timedelta(seconds=1)),
            DispatchEvent(2, DispatchState.RUNNING, NOW + timedelta(seconds=2)),
            DispatchEvent(
                3,
                DispatchState.SUCCEEDED,
                NOW + timedelta(seconds=3),
                details={"result_reference": _result_reference().to_dict()},
            ),
        ),
    )
    coordinator = ResilientDispatchCoordinator(
        store,
        NoopLifecycleTransport(),
        coordinator_node_id="coordinator",
    )

    first = coordinator.apply_response(request, response)
    duplicate = coordinator.apply_response(request, response)

    assert first.disposition is CompletionDisposition.APPLIED
    assert first.snapshot.job.status is JobStatus.SUCCEEDED
    assert first.result is not None
    assert duplicate.disposition is CompletionDisposition.DUPLICATE
    assert duplicate.result == first.result
    assert store.result_commit("job-lifecycle-1") == first.result


def test_f75_old_worker_completion_is_ignored_after_reassignment(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _claim(store)
    old_request = DispatchRequest.from_snapshot(
        store.snapshot("job-lifecycle-1"),
        dispatch_id="dispatch-old",
        target_node_id="node-a",
        sent_at=NOW,
    )
    lifecycle = JobLifecycleCoordinator(
        store,
        coordinator_node_id="coordinator",
        heartbeat_timeout_seconds=5,
    )
    lifecycle.evaluate(
        "job-lifecycle-1",
        now=NOW + timedelta(seconds=6),
        command_prefix="loss-old",
    )
    lifecycle.reassign(
        "job-lifecycle-1",
        reports=(
            _report("provider-a", "node-a", now=NOW + timedelta(seconds=8)),
            _report("provider-b", "node-b", now=NOW + timedelta(seconds=8)),
        ),
        policy=None,
        attempt_id="attempt-2",
        lease_id="lease-2",
        lease_expires_at=NOW + timedelta(seconds=50),
        command_prefix="reassign-old",
        now=NOW + timedelta(seconds=8),
    )
    old_response = DispatchResponse(
        dispatch_id="dispatch-old",
        session_id="session-lifecycle-1",
        job_id="job-lifecycle-1",
        provider_id="provider-a",
        attempt_id="attempt-1",
        events=(
            DispatchEvent(1, DispatchState.ACCEPTED, NOW + timedelta(seconds=1)),
            DispatchEvent(2, DispatchState.RUNNING, NOW + timedelta(seconds=2)),
            DispatchEvent(
                3,
                DispatchState.SUCCEEDED,
                NOW + timedelta(seconds=3),
                details={"result_reference": _result_reference().to_dict()},
            ),
        ),
    )

    outcome = ResilientDispatchCoordinator(
        store,
        NoopLifecycleTransport(),
        coordinator_node_id="coordinator",
    ).apply_response(old_request, old_response)

    assert outcome.disposition is CompletionDisposition.STALE_IGNORED
    assert outcome.snapshot.ownership is not None
    assert outcome.snapshot.ownership.owner_provider_id == "provider-b"
    assert store.result_commit("job-lifecycle-1") is None


class NoopLifecycleTransport:
    async def request(self, **_kwargs):
        raise AssertionError("transport should not be used")

    async def cancel(self, **_kwargs):
        raise AssertionError("transport should not be used")


class BlockingHandler:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()

    async def execute(self, _job: JobContract) -> ExecutionResult:
        self.calls += 1
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _worker(
    tmp_path: Path,
    handler: BlockingHandler,
) -> CancellableCapabilityWorker:
    return CancellableCapabilityWorker(
        WorkerRegistration(
            session_id="session-lifecycle-1",
            node_id="node-a",
            provider_id="provider-a",
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            attributes={"operation": "echo", "modalities": ["json"]},
        ),
        handler,
        SQLiteLifecycleDispatchInbox(tmp_path / "worker.sqlite3"),
        clock=lambda: NOW + timedelta(seconds=1),
    )


def test_f75_worker_cancellation_before_start_and_during_execution(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path / "store")
        _claim(store)
        snapshot = store.snapshot("job-lifecycle-1")
        dispatch = DispatchRequest.from_snapshot(
            snapshot,
            dispatch_id="dispatch-cancel",
            target_node_id="node-a",
            sent_at=NOW,
        )
        cancellation = CancellationRequest.from_snapshot(
            snapshot,
            cancellation_id="cancel-before",
            target_node_id="node-a",
            requested_at=NOW,
            reason="operator-request",
        )
        first_handler = BlockingHandler()
        first_worker = _worker(tmp_path / "before", first_handler)
        response = await first_worker.cancel(
            cancellation,
            authenticated_actor="coordinator",
            authenticated_session="session-lifecycle-1",
        )
        rejected = await first_worker.handle(
            dispatch,
            authenticated_actor="coordinator",
            authenticated_session="session-lifecycle-1",
        )
        assert response.state is CancellationState.NOT_RUNNING
        assert rejected.state is DispatchState.REJECTED
        assert rejected.events[-1].reason_code == "cancelled-before-start"
        assert first_handler.calls == 0

        second_handler = BlockingHandler()
        second_worker = _worker(tmp_path / "during", second_handler)
        task = asyncio.create_task(
            second_worker.handle(
                dispatch,
                authenticated_actor="coordinator",
                authenticated_session="session-lifecycle-1",
            )
        )
        await asyncio.wait_for(second_handler.started.wait(), timeout=2)
        active_cancel = CancellationRequest.from_snapshot(
            snapshot,
            cancellation_id="cancel-during",
            target_node_id="node-a",
            requested_at=NOW + timedelta(seconds=1),
            reason="operator-request",
        )
        cancelled = await second_worker.cancel(
            active_cancel,
            authenticated_actor="coordinator",
            authenticated_session="session-lifecycle-1",
        )
        completion = await asyncio.wait_for(task, timeout=2)
        replay = await second_worker.cancel(
            active_cancel,
            authenticated_actor="coordinator",
            authenticated_session="session-lifecycle-1",
        )
        assert cancelled.state is CancellationState.CANCELLED
        assert replay == cancelled
        assert completion.state is DispatchState.FAILED
        assert completion.events[-1].reason_code == "worker-cancelled"
        assert second_handler.calls == 1

    asyncio.run(scenario())

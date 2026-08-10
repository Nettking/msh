from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.dispatch import (
    CallableDispatchTransport,
    CapabilityWorker,
    DispatchCoordinator,
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
    ExecutionResult,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobAttempt,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.federation.errors import FederationValidationError, ProtocolCompatibilityError

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)


def _job(*, status: JobStatus = JobStatus.ACTIVE, job_id: str = "job-1") -> JobContract:
    attempts = (
        (JobAttempt("attempt-1", 1, AttemptStatus.ASSIGNED),)
        if status is JobStatus.ACTIVE
        else ()
    )
    return JobContract(
        job_id=job_id,
        session_id="session-1",
        request_id=f"request-{job_id}",
        idempotency_key=f"session-1:{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(
            ArtifactReference(
                reference_id="input-1",
                session_id="session-1",
                schema_name="fcp.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id="output-1",
                session_id="session-1",
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(1,),
            retryable_error_codes=("worker-lost",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=60,
            queue_timeout_seconds=30,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
        status=status,
        attempts=attempts,
    )


def _request(**overrides: object) -> DispatchRequest:
    values: dict[str, object] = {
        "dispatch_id": "dispatch-1",
        "session_id": "session-1",
        "coordinator_node_id": "node-coordinator",
        "target_node_id": "node-worker",
        "provider_id": "provider-worker",
        "job": _job(),
        "attempt_id": "attempt-1",
        "attempt_number": 1,
        "lease_id": "lease-1",
        "lease_generation": 1,
        "lease_expires_at": NOW + timedelta(minutes=5),
        "sent_at": NOW,
    }
    values.update(overrides)
    return DispatchRequest(**values)  # type: ignore[arg-type]


def _registration(**overrides: object) -> WorkerRegistration:
    values: dict[str, object] = {
        "session_id": "session-1",
        "node_id": "node-worker",
        "provider_id": "provider-worker",
        "capability_type": "synthetic-compute",
        "protocol": "fcp-synthetic",
        "protocol_version": "1.2",
        "attributes": {"operation": "echo", "modalities": ["json", "text"]},
    }
    values.update(overrides)
    return WorkerRegistration(**values)  # type: ignore[arg-type]


class CountingHandler:
    def __init__(self, *, fail: bool = False, raise_error: bool = False) -> None:
        self.calls = 0
        self.fail = fail
        self.raise_error = raise_error

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        if self.raise_error:
            raise RuntimeError("synthetic failure")
        if self.fail:
            return ExecutionResult(False, {"job_id": job.job_id}, "synthetic-failed")
        return ExecutionResult(True, {"job_id": job.job_id, "kind": "synthetic"})


def _worker(tmp_path: Path, handler: CountingHandler, **registration: object) -> CapabilityWorker:
    return CapabilityWorker(
        _registration(**registration),
        handler,
        SQLiteDispatchInbox(tmp_path / "dispatch.sqlite3"),
        clock=lambda: NOW + timedelta(seconds=1),
    )


def test_f74_001_request_round_trip_is_canonical() -> None:
    request = _request()
    assert DispatchRequest.from_json(request.to_json()) == request
    assert request.to_json() == DispatchRequest.from_dict(request.to_dict()).to_json()
    assert json.loads(request.to_json()) == request.to_dict()


def test_f74_002_response_round_trip_and_additive_fields() -> None:
    response = DispatchResponse(
        "dispatch-1",
        "session-1",
        "job-1",
        "provider-worker",
        "attempt-1",
        (
            DispatchEvent(1, DispatchState.ACCEPTED, NOW),
            DispatchEvent(2, DispatchState.RUNNING, NOW),
            DispatchEvent(3, DispatchState.SUCCEEDED, NOW, details={"ok": True}),
        ),
    )
    value = response.to_dict()
    value["future_field"] = {"safe": True}
    value["events"][0]["future_field"] = "ignored"
    restored = DispatchResponse.from_dict(value)
    assert restored == response
    assert restored.validate_for(_request()) == response


def test_f74_003_unknown_schema_and_protocol_major_fail_closed() -> None:
    value = _request().to_dict()
    value["schema"] = "fcp.capability-dispatch.v2"
    with pytest.raises(ProtocolCompatibilityError) as schema:
        DispatchRequest.from_dict(value)
    assert schema.value.code == "unsupported-schema-major"

    value = _request().to_dict()
    value["protocol_version"] = "2.0"
    with pytest.raises(ProtocolCompatibilityError) as protocol:
        DispatchRequest.from_dict(value)
    assert protocol.value.code == "unsupported-dispatch-protocol-major"


def test_f74_004_dispatch_requires_active_matching_attempt() -> None:
    with pytest.raises(FederationValidationError) as inactive:
        _request(job=_job(status=JobStatus.QUEUED))
    assert inactive.value.code == "dispatch-job-not-active"

    with pytest.raises(FederationValidationError) as attempt:
        _request(attempt_id="attempt-other")
    assert attempt.value.code == "dispatch-attempt-mismatch"


def test_f74_005_dispatch_must_precede_lease() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _request(sent_at=NOW + timedelta(minutes=5))
    assert exc_info.value.code == "dispatch-after-lease"


def test_f74_006_response_order_and_binding_fail_closed() -> None:
    with pytest.raises(FederationValidationError) as order:
        DispatchResponse(
            "dispatch-1",
            "session-1",
            "job-1",
            "provider-worker",
            "attempt-1",
            (DispatchEvent(1, DispatchState.RUNNING, NOW),),
        )
    assert order.value.code == "invalid-dispatch-event-order"

    response = DispatchResponse(
        "dispatch-1",
        "session-1",
        "job-other",
        "provider-worker",
        "attempt-1",
        (DispatchEvent(1, DispatchState.REJECTED, NOW, "bad-job"),),
    )
    with pytest.raises(FederationValidationError) as mismatch:
        response.validate_for(_request())
    assert mismatch.value.code == "dispatch-response-mismatch"


def test_f74_007_worker_executes_registered_handler(tmp_path: Path) -> None:
    handler = CountingHandler()
    worker = _worker(tmp_path, handler)
    response = asyncio.run(
        worker.handle(
            _request(),
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    assert handler.calls == 1
    assert [event.state for event in response.events] == [
        DispatchState.ACCEPTED,
        DispatchState.RUNNING,
        DispatchState.SUCCEEDED,
    ]
    assert response.events[-1].details == {"job_id": "job-1", "kind": "synthetic"}


def test_f74_008_duplicate_delivery_replays_without_duplicate_work(tmp_path: Path) -> None:
    handler = CountingHandler()
    worker = _worker(tmp_path, handler)
    request = _request()

    first = asyncio.run(
        worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    second = asyncio.run(
        worker.handle(
            replace(request, sent_at=NOW + timedelta(seconds=2)),
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )

    assert first == second
    assert handler.calls == 1


def test_f74_009_duplicate_after_worker_restart_replays(tmp_path: Path) -> None:
    handler = CountingHandler()
    request = _request()
    first_worker = _worker(tmp_path, handler)
    first = asyncio.run(
        first_worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    restarted = _worker(tmp_path, handler)
    second = asyncio.run(
        restarted.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    assert first == second
    assert handler.calls == 1


def test_f74_010_reserved_before_crash_never_restarts_work(tmp_path: Path) -> None:
    request = _request()
    inbox = SQLiteDispatchInbox(tmp_path / "dispatch.sqlite3")
    running = DispatchResponse(
        request.dispatch_id,
        request.session_id,
        request.job.job_id,
        request.provider_id,
        request.attempt_id,
        (
            DispatchEvent(1, DispatchState.ACCEPTED, NOW),
            DispatchEvent(2, DispatchState.RUNNING, NOW),
        ),
    )
    assert inbox.reserve(request, running, now=NOW) is None

    handler = CountingHandler()
    worker = CapabilityWorker(
        _registration(),
        handler,
        SQLiteDispatchInbox(tmp_path / "dispatch.sqlite3"),
        clock=lambda: NOW + timedelta(seconds=1),
    )
    replay = asyncio.run(
        worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    assert replay.state is DispatchState.RUNNING
    assert handler.calls == 0


def test_f74_011_conflicting_dispatch_id_fails_closed(tmp_path: Path) -> None:
    handler = CountingHandler()
    worker = _worker(tmp_path, handler)
    request = _request()
    asyncio.run(
        worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    conflicting = replace(request, job=_job(job_id="job-2"))
    with pytest.raises(FederationValidationError) as exc_info:
        asyncio.run(
            worker.handle(
                conflicting,
                authenticated_actor="node-coordinator",
                authenticated_session="session-1",
            )
        )
    assert exc_info.value.code == "dispatch-id-conflict"
    assert handler.calls == 1


@pytest.mark.parametrize(
    ("registration", "actor", "session", "reason"),
    [
        ({}, "node-other", "session-1", "coordinator-mismatch"),
        ({}, "node-coordinator", "session-other", "session-mismatch"),
        ({"session_id": "session-other"}, "node-coordinator", "session-1", "worker-session-mismatch"),
        ({"node_id": "node-other"}, "node-coordinator", "session-1", "target-node-mismatch"),
        ({"provider_id": "provider-other"}, "node-coordinator", "session-1", "provider-mismatch"),
        ({"capability_type": "other"}, "node-coordinator", "session-1", "capability-type-mismatch"),
        ({"protocol": "other"}, "node-coordinator", "session-1", "capability-protocol-mismatch"),
        ({"protocol_version": "2.0"}, "node-coordinator", "session-1", "capability-version-mismatch"),
        ({"attributes": {"operation": "other"}}, "node-coordinator", "session-1", "capability-requirements-mismatch"),
    ],
)
def test_f74_012_worker_rejects_binding_mismatch(
    tmp_path: Path,
    registration: dict[str, object],
    actor: str,
    session: str,
    reason: str,
) -> None:
    handler = CountingHandler()
    response = asyncio.run(
        _worker(tmp_path, handler, **registration).handle(
            _request(),
            authenticated_actor=actor,
            authenticated_session=session,
        )
    )
    assert response.state is DispatchState.REJECTED
    assert response.events[-1].reason_code == reason
    assert handler.calls == 0


def test_f74_013_expired_lease_rejected_without_work(tmp_path: Path) -> None:
    handler = CountingHandler()
    worker = CapabilityWorker(
        _registration(),
        handler,
        SQLiteDispatchInbox(tmp_path / "dispatch.sqlite3"),
        clock=lambda: NOW + timedelta(minutes=6),
    )
    response = asyncio.run(
        worker.handle(
            _request(),
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    assert response.state is DispatchState.REJECTED
    assert response.events[-1].reason_code == "lease-expired"
    assert handler.calls == 0


def test_f74_014_handler_failure_is_structured(tmp_path: Path) -> None:
    response = asyncio.run(
        _worker(tmp_path, CountingHandler(raise_error=True)).handle(
            _request(),
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    assert response.state is DispatchState.FAILED
    assert response.events[-1].reason_code == "worker-handler-failed"


def _claimed_store(tmp_path: Path) -> SQLiteJobStore:
    store = SQLiteJobStore(tmp_path / "jobs.sqlite3")
    submitted = _job(status=JobStatus.SUBMITTED)
    store.submit(submitted, coordinator_id="node-coordinator", now=NOW)
    store.queue(
        submitted.job_id,
        coordinator_id="node-coordinator",
        command_id="queue-1",
        expected_revision=0,
        now=NOW,
    )
    store.claim(
        submitted.job_id,
        coordinator_id="node-coordinator",
        owner_provider_id="provider-worker",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="claim-1",
        expected_revision=1,
        lease_expires_at=NOW + timedelta(minutes=5),
        now=NOW,
    )
    return store


def test_f74_015_coordinator_persists_progress_and_completion(tmp_path: Path) -> None:
    store = _claimed_store(tmp_path)
    handler = CountingHandler()
    worker = _worker(tmp_path, handler)

    async def send(target: str, request: DispatchRequest) -> DispatchResponse:
        assert target == "node-worker"
        return await worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )

    outcome = asyncio.run(
        DispatchCoordinator(
            store,
            CallableDispatchTransport(send),
            coordinator_node_id="node-coordinator",
        ).dispatch(
            "job-1",
            dispatch_id="dispatch-1",
            target_node_id="node-worker",
            now=NOW,
        )
    )
    assert outcome.response.state is DispatchState.SUCCEEDED
    assert outcome.snapshot.job.status is JobStatus.SUCCEEDED
    assert outcome.snapshot.job.attempts[-1].status is AttemptStatus.SUCCEEDED
    assert outcome.snapshot.ownership is None
    assert outcome.snapshot.revision == 5
    assert handler.calls == 1


def test_f74_016_coordinator_persists_structured_rejection(tmp_path: Path) -> None:
    store = _claimed_store(tmp_path)
    worker = _worker(tmp_path, CountingHandler(), protocol="other")

    async def send(_target: str, request: DispatchRequest) -> DispatchResponse:
        return await worker.handle(
            request,
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )

    outcome = asyncio.run(
        DispatchCoordinator(
            store,
            CallableDispatchTransport(send),
            coordinator_node_id="node-coordinator",
        ).dispatch(
            "job-1",
            dispatch_id="dispatch-1",
            target_node_id="node-worker",
            now=NOW,
        )
    )
    assert outcome.snapshot.job.status is JobStatus.FAILED
    assert outcome.snapshot.job.attempts[-1].error_code == "capability-protocol-mismatch"
    assert outcome.snapshot.ownership is None


def test_f74_017_callable_transport_preserves_application_semantics(tmp_path: Path) -> None:
    worker = _worker(tmp_path, CountingHandler())
    request = _request()

    async def direct(target: str, value: DispatchRequest) -> DispatchResponse:
        assert target == value.target_node_id
        restored = DispatchRequest.from_json(value.to_json())
        response = await worker.handle(
            restored,
            authenticated_actor=restored.coordinator_node_id,
            authenticated_session=restored.session_id,
        )
        return DispatchResponse.from_json(response.to_json())

    response = asyncio.run(
        CallableDispatchTransport(direct).request(
            target_node_id="node-worker",
            request=request,
        )
    )
    assert response.state is DispatchState.SUCCEEDED
    assert response.validate_for(request) == response


def test_f74_018_corrupt_receipt_metadata_fails_closed(tmp_path: Path) -> None:
    handler = CountingHandler()
    worker = _worker(tmp_path, handler)
    asyncio.run(
        worker.handle(
            _request(),
            authenticated_actor="node-coordinator",
            authenticated_session="session-1",
        )
    )
    with sqlite3.connect(tmp_path / "dispatch.sqlite3") as connection:
        connection.execute(
            "UPDATE capability_dispatch_receipts SET provider_id='tampered'"
        )
    with pytest.raises(FederationValidationError) as exc_info:
        SQLiteDispatchInbox(tmp_path / "dispatch.sqlite3").response(
            "session-1", "dispatch-1"
        )
    assert exc_info.value.code == "dispatch-receipt-row-mismatch"

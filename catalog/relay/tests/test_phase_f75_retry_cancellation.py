from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_contracts import (
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
    ProviderStatus,
)
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.federation.coordinator import SessionCoordinator
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


class ResultHandler:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.calls = 0

    async def execute(self, _job: JobContract) -> ExecutionResult:
        self.calls += 1
        reference = ArtifactReference(
            reference_id="output-relay-f75",
            session_id=self.session_id,
            schema_name="msh.synthetic-output.v1",
            media_type="application/json",
            content_hash="sha256:" + "b" * 64,
            size_bytes=256,
        )
        return ExecutionResult(
            True,
            {"result_reference": reference.to_dict()},
        )


class BlockingHandler:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()

    async def execute(self, _job: JobContract) -> ExecutionResult:
        self.calls += 1
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def _start_relay(database: Path) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: NOW),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=TIMEOUT,
        send_timeout_seconds=TIMEOUT,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def _client(path: Path, relay: RelayServer, name: str) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=path,
        relay_url=relay.url,
        display_name=name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=TIMEOUT,
        clock=lambda: NOW,
    )


async def _enroll(relay: RelayServer, client: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await client.connect(enrollment_token=token)


async def _join(
    owner: RelayNodeClient,
    member: RelayNodeClient,
    session_id: str,
) -> None:
    invitation = await owner.create_invitation(
        session_id,
        ttl_seconds=60,
        max_uses=1,
    )
    joined = await member.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)


def _job(session_id: str, job_id: str) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{job_id}",
        idempotency_key=f"{session_id}:request-{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="msh-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(),
        outputs=(
            ArtifactReference(
                reference_id=(
                    "output-relay-f75"
                    if job_id == "job-loss"
                    else "output-relay-cancel"
                ),
                session_id=session_id,
                schema_name="msh.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(2,),
            retryable_error_codes=(
                "worker-heartbeat-lost",
                "lease-expired",
            ),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=60,
            cancellation_grace_seconds=5,
        ),
    )


def _registration(
    session_id: str,
    node_id: str,
    provider_id: str,
) -> WorkerRegistration:
    return WorkerRegistration(
        session_id=session_id,
        node_id=node_id,
        provider_id=provider_id,
        capability_type="synthetic-compute",
        protocol="msh-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo", "modalities": ["json"]},
    )


def _report(
    session_id: str,
    provider_id: str,
    node_id: str,
    *,
    now: datetime,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=provider_id,
        node_id=node_id,
        session_id=session_id,
        capability_type="synthetic-compute",
        protocol="msh-synthetic",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=1,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=100,
        attributes={"operation": "echo", "modalities": ["json"]},
        reported_at=now - timedelta(seconds=1),
        expires_at=now + timedelta(seconds=30),
    )


def test_f75_worker_loss_reassigns_and_fences_old_completion(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        coordinator_node: RelayNodeClient | None = None
        worker_a_node: RelayNodeClient | None = None
        worker_b_node: RelayNodeClient | None = None
        endpoints: list[RelayLifecycleEndpoint] = []
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3")
            coordinator_node = _client(
                tmp_path / "coordinator", relay, "F7.5 coordinator"
            )
            worker_a_node = _client(tmp_path / "worker-a", relay, "Worker A")
            worker_b_node = _client(tmp_path / "worker-b", relay, "Worker B")
            await _enroll(relay, coordinator_node)
            await _enroll(relay, worker_a_node)
            await _enroll(relay, worker_b_node)
            session = await coordinator_node.create_session("F7.5 worker loss")
            session_id = str(session["session_id"])
            await _join(coordinator_node, worker_a_node, session_id)
            await _join(coordinator_node, worker_b_node, session_id)

            store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
            job = _job(session_id, "job-loss")
            store.submit(job, coordinator_id=coordinator_node.node_id, now=NOW)
            store.queue(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                command_id="queue-loss",
                expected_revision=0,
                now=NOW,
            )
            store.claim(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                owner_provider_id="provider-a",
                attempt_id="attempt-a",
                lease_id="lease-a",
                command_id="claim-a",
                expected_revision=1,
                lease_expires_at=NOW + timedelta(seconds=60),
                now=NOW,
            )
            old_request = DispatchRequest.from_snapshot(
                store.snapshot(job.job_id),
                dispatch_id="dispatch-a",
                target_node_id=worker_a_node.node_id,
                sent_at=NOW,
            )

            result_handler = ResultHandler(session_id)
            worker_b = CancellableCapabilityWorker(
                _registration(session_id, worker_b_node.node_id, "provider-b"),
                result_handler,
                SQLiteLifecycleDispatchInbox(tmp_path / "worker-b-inbox.sqlite3"),
                clock=lambda: NOW + timedelta(seconds=10),
            )
            coordinator_endpoint = RelayLifecycleEndpoint(
                coordinator_node, request_timeout=TIMEOUT
            )
            worker_a_endpoint = RelayLifecycleEndpoint(
                worker_a_node, request_timeout=TIMEOUT
            )
            worker_b_endpoint = RelayLifecycleEndpoint(
                worker_b_node, request_timeout=TIMEOUT
            )
            worker_b_endpoint.register_worker("provider-b", worker_b)
            endpoints.extend(
                (coordinator_endpoint, worker_a_endpoint, worker_b_endpoint)
            )
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            await worker_a_endpoint.send_heartbeat(
                target_node_id=coordinator_node.node_id,
                heartbeat=JobHeartbeat(
                    heartbeat_id="heartbeat-a-1",
                    session_id=session_id,
                    worker_node_id=worker_a_node.node_id,
                    provider_id="provider-a",
                    job_id=job.job_id,
                    attempt_id="attempt-a",
                    lease_id="lease-a",
                    lease_generation=1,
                    sequence=1,
                    occurred_at=NOW + timedelta(seconds=1),
                ),
            )
            authenticated = await coordinator_endpoint.receive_heartbeat(
                timeout=TIMEOUT
            )
            lifecycle = JobLifecycleCoordinator(
                store,
                coordinator_node_id=coordinator_node.node_id,
                heartbeat_timeout_seconds=5,
            )
            assert lifecycle.record_heartbeat(
                authenticated.heartbeat,
                authenticated_worker_node_id=authenticated.actor_node_id,
                authenticated_session_id=authenticated.session_id,
            )

            lost = lifecycle.evaluate(
                job.job_id,
                now=NOW + timedelta(seconds=7),
                command_prefix="loss-a",
            )
            assert lost.action is LifecycleAction.RETRY_SCHEDULED
            assert lost.reason_code == "worker-heartbeat-lost"
            assert lost.retry is not None

            reassigned = lifecycle.reassign(
                job.job_id,
                reports=(
                    _report(
                        session_id,
                        "provider-a",
                        worker_a_node.node_id,
                        now=NOW + timedelta(seconds=9),
                    ),
                    _report(
                        session_id,
                        "provider-b",
                        worker_b_node.node_id,
                        now=NOW + timedelta(seconds=9),
                    ),
                ),
                policy=None,
                attempt_id="attempt-b",
                lease_id="lease-b",
                lease_expires_at=NOW + timedelta(seconds=60),
                command_prefix="reassign-b",
                now=NOW + timedelta(seconds=9),
            )
            assert reassigned.selection.selected_capability_id == "provider-b"
            assert reassigned.snapshot.ownership is not None
            assert reassigned.snapshot.ownership.lease_generation == 2

            resilient = ResilientDispatchCoordinator(
                store,
                coordinator_endpoint,
                coordinator_node_id=coordinator_node.node_id,
            )
            completed = await resilient.dispatch(
                job.job_id,
                dispatch_id="dispatch-b",
                target_node_id=worker_b_node.node_id,
                now=NOW + timedelta(seconds=9),
            )
            assert completed.disposition is CompletionDisposition.APPLIED
            assert completed.snapshot.job.status is JobStatus.SUCCEEDED
            assert completed.result is not None
            assert result_handler.calls == 1

            old_response = DispatchResponse(
                dispatch_id="dispatch-a",
                session_id=session_id,
                job_id=job.job_id,
                provider_id="provider-a",
                attempt_id="attempt-a",
                events=(
                    DispatchEvent(
                        1, DispatchState.ACCEPTED, NOW + timedelta(seconds=2)
                    ),
                    DispatchEvent(
                        2, DispatchState.RUNNING, NOW + timedelta(seconds=3)
                    ),
                    DispatchEvent(
                        3,
                        DispatchState.SUCCEEDED,
                        NOW + timedelta(seconds=4),
                        details={
                            "result_reference": completed.result.reference.to_dict()
                        },
                    ),
                ),
            )
            stale = resilient.apply_response(old_request, old_response)
            assert stale.disposition is CompletionDisposition.STALE_IGNORED
            assert stale.result == completed.result

            replay_request = DispatchRequest.from_snapshot(
                reassigned.snapshot,
                dispatch_id="dispatch-b",
                target_node_id=worker_b_node.node_id,
                sent_at=NOW + timedelta(seconds=9),
            )
            duplicate_response = await coordinator_endpoint.request(
                target_node_id=worker_b_node.node_id,
                request=replay_request,
            )
            duplicate = resilient.apply_response(
                replay_request, duplicate_response
            )
            assert duplicate.disposition is CompletionDisposition.DUPLICATE
            assert result_handler.calls == 1
            assert store.result_commit(job.job_id) == completed.result
        finally:
            if endpoints:
                await asyncio.gather(
                    *(endpoint.close() for endpoint in endpoints),
                    return_exceptions=True,
                )
            clients = tuple(
                client
                for client in (coordinator_node, worker_a_node, worker_b_node)
                if client is not None
            )
            if clients:
                await asyncio.gather(
                    *(client.disconnect() for client in clients),
                    return_exceptions=True,
                )
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())


def test_f75_cancellation_during_execution_over_existing_relay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        coordinator_node: RelayNodeClient | None = None
        worker_node: RelayNodeClient | None = None
        endpoints: list[RelayLifecycleEndpoint] = []
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3")
            coordinator_node = _client(
                tmp_path / "coordinator", relay, "F7.5 coordinator"
            )
            worker_node = _client(tmp_path / "worker", relay, "Blocking worker")
            await _enroll(relay, coordinator_node)
            await _enroll(relay, worker_node)
            session = await coordinator_node.create_session("F7.5 cancellation")
            session_id = str(session["session_id"])
            await _join(coordinator_node, worker_node, session_id)

            store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
            job = _job(session_id, "job-cancel")
            store.submit(job, coordinator_id=coordinator_node.node_id, now=NOW)
            store.queue(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                command_id="queue-cancel",
                expected_revision=0,
                now=NOW,
            )
            store.claim(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                owner_provider_id="provider-cancel",
                attempt_id="attempt-cancel",
                lease_id="lease-cancel",
                command_id="claim-cancel",
                expected_revision=1,
                lease_expires_at=NOW + timedelta(seconds=60),
                now=NOW,
            )

            clock = [NOW + timedelta(seconds=1)]
            handler = BlockingHandler()
            worker = CancellableCapabilityWorker(
                _registration(
                    session_id, worker_node.node_id, "provider-cancel"
                ),
                handler,
                SQLiteLifecycleDispatchInbox(tmp_path / "worker-inbox.sqlite3"),
                clock=lambda: clock[0],
            )
            coordinator_endpoint = RelayLifecycleEndpoint(
                coordinator_node, request_timeout=TIMEOUT
            )
            worker_endpoint = RelayLifecycleEndpoint(
                worker_node, request_timeout=TIMEOUT
            )
            worker_endpoint.register_worker("provider-cancel", worker)
            endpoints.extend((coordinator_endpoint, worker_endpoint))
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            resilient = ResilientDispatchCoordinator(
                store,
                coordinator_endpoint,
                coordinator_node_id=coordinator_node.node_id,
            )
            dispatch_task = asyncio.create_task(
                resilient.dispatch(
                    job.job_id,
                    dispatch_id="dispatch-cancel",
                    target_node_id=worker_node.node_id,
                    now=NOW,
                )
            )
            await asyncio.wait_for(handler.started.wait(), timeout=TIMEOUT)
            clock[0] = NOW + timedelta(seconds=3)
            cancelled = await resilient.cancel(
                job.job_id,
                cancellation_id="cancel-relay",
                target_node_id=worker_node.node_id,
                reason="operator-request",
                now=NOW + timedelta(seconds=2),
            )
            dispatch_outcome = await asyncio.wait_for(
                dispatch_task, timeout=TIMEOUT
            )

            assert cancelled.job.status is JobStatus.CANCELLED
            assert cancelled.ownership is None
            assert dispatch_outcome.disposition is CompletionDisposition.CANCELLED_IGNORED
            assert handler.calls == 1
            assert store.result_commit(job.job_id) is None

            replay = await resilient.cancel(
                job.job_id,
                cancellation_id="cancel-relay",
                target_node_id=None,
                reason="operator-request",
                now=NOW + timedelta(seconds=2),
            )
            assert replay == cancelled
        finally:
            if endpoints:
                await asyncio.gather(
                    *(endpoint.close() for endpoint in endpoints),
                    return_exceptions=True,
                )
            clients = tuple(
                client
                for client in (coordinator_node, worker_node)
                if client is not None
            )
            if clients:
                await asyncio.gather(
                    *(client.disconnect() for client in clients),
                    return_exceptions=True,
                )
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.dispatch import (
    CapabilityWorker,
    DispatchCoordinator,
    DispatchRequest,
    DispatchState,
    ExecutionResult,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import (
    ArtifactReference,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.relay_dispatch import RelayDispatchEndpoint
from catalog.federation.coordinator import SessionCoordinator
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


class SyntheticHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(
            True,
            {
                "kind": "synthetic-non-ai",
                "job_id": job.job_id,
                "operation": job.capability.requirements["operation"],
            },
        )


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


def _job(session_id: str) -> JobContract:
    return JobContract(
        job_id="job-relay-1",
        session_id=session_id,
        request_id="request-relay-1",
        idempotency_key=f"{session_id}:request-relay-1",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="msh-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(
            ArtifactReference(
                reference_id="input-relay-1",
                session_id=session_id,
                schema_name="msh.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id="output-relay-1",
                session_id=session_id,
                schema_name="msh.synthetic-output.v1",
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
    )


def test_f74_two_nodes_complete_synthetic_job_over_existing_relay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        coordinator_node: RelayNodeClient | None = None
        worker_node: RelayNodeClient | None = None
        endpoints: list[RelayDispatchEndpoint] = []
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3")
            coordinator_node = _client(
                tmp_path / "coordinator-node",
                relay,
                "Job coordinator",
            )
            worker_node = _client(
                tmp_path / "worker-node",
                relay,
                "Synthetic worker",
            )
            await _enroll(relay, coordinator_node)
            await _enroll(relay, worker_node)
            session = await coordinator_node.create_session("F7.4 relay dispatch")
            session_id = str(session["session_id"])
            await _join(coordinator_node, worker_node, session_id)

            store = SQLiteJobStore(tmp_path / "jobs.sqlite3")
            job = _job(session_id)
            store.submit(job, coordinator_id=coordinator_node.node_id, now=NOW)
            store.queue(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                command_id="queue-relay-1",
                expected_revision=0,
                now=NOW,
            )
            store.claim(
                job.job_id,
                coordinator_id=coordinator_node.node_id,
                owner_provider_id="provider-synthetic",
                attempt_id="attempt-relay-1",
                lease_id="lease-relay-1",
                command_id="claim-relay-1",
                expected_revision=1,
                lease_expires_at=NOW + timedelta(minutes=5),
                now=NOW,
            )

            handler = SyntheticHandler()
            worker = CapabilityWorker(
                WorkerRegistration(
                    session_id=session_id,
                    node_id=worker_node.node_id,
                    provider_id="provider-synthetic",
                    capability_type="synthetic-compute",
                    protocol="msh-synthetic",
                    protocol_version="1.1",
                    attributes={
                        "operation": "echo",
                        "modalities": ["json", "text"],
                    },
                ),
                handler,
                SQLiteDispatchInbox(tmp_path / "worker-inbox.sqlite3"),
                clock=lambda: NOW + timedelta(seconds=1),
            )
            coordinator_endpoint = RelayDispatchEndpoint(
                coordinator_node,
                request_timeout=TIMEOUT,
            )
            worker_endpoint = RelayDispatchEndpoint(
                worker_node,
                request_timeout=TIMEOUT,
            )
            worker_endpoint.register_worker("provider-synthetic", worker)
            endpoints.extend((coordinator_endpoint, worker_endpoint))
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            replay_request = DispatchRequest.from_snapshot(
                store.snapshot(job.job_id),
                dispatch_id="dispatch-relay-1",
                target_node_id=worker_node.node_id,
                sent_at=NOW,
            )
            outcome = await DispatchCoordinator(
                store,
                coordinator_endpoint,
                coordinator_node_id=coordinator_node.node_id,
            ).dispatch(
                job.job_id,
                dispatch_id="dispatch-relay-1",
                target_node_id=worker_node.node_id,
                now=NOW,
            )

            assert outcome.response.state is DispatchState.SUCCEEDED
            assert outcome.snapshot.job.status is JobStatus.SUCCEEDED
            assert outcome.snapshot.ownership is None
            assert outcome.response.events[-1].details == {
                "kind": "synthetic-non-ai",
                "job_id": job.job_id,
                "operation": "echo",
            }
            assert handler.calls == 1

            duplicate = await coordinator_endpoint.request(
                target_node_id=worker_node.node_id,
                request=replay_request,
            )
            assert duplicate == outcome.response
            assert handler.calls == 1

            restarted_worker = CapabilityWorker(
                worker.registration,
                handler,
                SQLiteDispatchInbox(tmp_path / "worker-inbox.sqlite3"),
                clock=lambda: NOW + timedelta(seconds=2),
            )
            worker_endpoint.register_worker("provider-synthetic", restarted_worker)
            after_restart = await coordinator_endpoint.request(
                target_node_id=worker_node.node_id,
                request=replay_request,
            )
            assert after_restart == outcome.response
            assert handler.calls == 1
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

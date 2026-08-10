from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.dispatch import (
    DispatchRequest,
    DispatchState,
    ExecutionResult,
    SQLiteDispatchInbox,
)
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
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.relay_dispatch import RelayDispatchEndpoint
from catalog.capabilities.worker_activation import (
    ComputeHandlerActivationReference,
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 2, 16, tzinfo=timezone.utc)
TIMEOUT = 15.0
CAPABILITY_ID = "relay-compute"


class CountingHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(True, {"job_id": job.job_id, "route": "relay"})


async def start_relay(database: Path, current: list[datetime]) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: current[0]),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=TIMEOUT,
        send_timeout_seconds=TIMEOUT,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def client(
    path: Path,
    relay: RelayServer,
    name: str,
    current: list[datetime],
) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=path,
        relay_url=relay.url,
        display_name=name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=TIMEOUT,
        clock=lambda: current[0],
    )


async def enroll(relay: RelayServer, node: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    await node.connect(enrollment_token=token)


async def join(owner: RelayNodeClient, provider: RelayNodeClient, session_id: str) -> None:
    invitation = await owner.create_invitation(session_id, ttl_seconds=60, max_uses=1)
    joined = await provider.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)


def descriptor() -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id="relay-echo-handler",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo", "modalities": ["json"]},
    )


def report(
    *,
    session_id: str,
    provider_node_id: str,
    local_descriptor: LocalComputeHandlerDescriptor,
    revision: int,
    reported_at: datetime,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=provider_node_id,
        session_id=session_id,
        capability_type=local_descriptor.capability_type,
        protocol=local_descriptor.protocol,
        protocol_version=local_descriptor.protocol_version,
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            **local_descriptor.attributes,
            "activation": ComputeHandlerActivationReference(
                local_descriptor.handler_id,
                local_descriptor.descriptor_fingerprint,
            ).to_dict(),
        },
        reported_at=reported_at,
        expires_at=reported_at + timedelta(seconds=30),
    )


def active_job(session_id: str, job_id: str) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{job_id}",
        idempotency_key=f"{session_id}:{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"input-{job_id}",
                session_id=session_id,
                schema_name="fcp.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=f"output-{job_id}",
                session_id=session_id,
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(1, (), ()),
        timeout_policy=TimeoutPolicy(60, 30, 10, 30, 5),
        status=JobStatus.ACTIVE,
        attempts=(JobAttempt(f"attempt-{job_id}", 1, AttemptStatus.ASSIGNED),),
    )


def request(
    *,
    session_id: str,
    owner_node_id: str,
    provider_node_id: str,
    dispatch_id: str,
    job_id: str,
    sent_at: datetime,
) -> DispatchRequest:
    job = active_job(session_id, job_id)
    return DispatchRequest(
        dispatch_id=dispatch_id,
        session_id=session_id,
        coordinator_node_id=owner_node_id,
        target_node_id=provider_node_id,
        provider_id=CAPABILITY_ID,
        job=job,
        attempt_id=job.attempts[0].attempt_id,
        attempt_number=1,
        lease_id=f"lease-{job_id}",
        lease_generation=1,
        lease_expires_at=sent_at + timedelta(seconds=30),
        sent_at=sent_at,
    )


def test_f84_compute_worker_executes_once_and_fences_generation_over_relay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        current = [NOW]
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        provider_node: RelayNodeClient | None = None
        endpoints: list[RelayDispatchEndpoint] = []
        try:
            relay = await start_relay(tmp_path / "relay" / "control.sqlite3", current)
            owner = client(tmp_path / "owner", relay, "Owner", current)
            provider_node = client(tmp_path / "provider", relay, "Provider", current)
            await enroll(relay, owner)
            await enroll(relay, provider_node)
            session = await owner.create_session("F8.4 relay compute")
            session_id = str(session["session_id"])
            await join(owner, provider_node, session_id)

            local_descriptor = descriptor()
            relay.coordinator.announce_capability(
                CapabilityAnnouncement(
                    capability_id=CAPABILITY_ID,
                    node_id=provider_node.node_id,
                    session_id=session_id,
                    type=local_descriptor.capability_type,
                    protocol=local_descriptor.protocol,
                    protocol_version=local_descriptor.protocol_version,
                    status=CapabilityStatus.READY,
                    properties=local_descriptor.attributes,
                    announced_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                request_id="announce-f84-relay-compute",
            )
            enrollments = FederatedProviderEnrollmentService(
                relay.coordinator,
                SQLiteProviderEnrollmentStore(tmp_path / "provider-enrollment.sqlite3"),
                clock=lambda: current[0],
            )
            requested = enrollments.request(
                session_id=session_id,
                capability_id=CAPABILITY_ID,
                actor_node_id=provider_node.node_id,
                command_id="request-f84-relay-compute",
            )
            enrollments.approve(
                session_id=session_id,
                capability_id=CAPABILITY_ID,
                actor_node_id=owner.node_id,
                command_id="approve-f84-relay-compute",
                expected_revision=requested.revision,
            )
            health = FederatedProviderHealthService(
                enrollments,
                SQLiteProviderHealthStore(tmp_path / "provider-health.sqlite3"),
                clock=lambda: current[0],
            )
            health.publish(
                report(
                    session_id=session_id,
                    provider_node_id=provider_node.node_id,
                    local_descriptor=local_descriptor,
                    revision=0,
                    reported_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                command_id="publish-f84-generation-one",
                provider_generation=1,
            )

            handler = CountingHandler()
            inventory = LocalComputeHandlerInventory()
            inventory.register(local_descriptor, handler)
            authority = ComputeWorkerActivationAuthority(
                health,
                inventory,
                session_id=session_id,
                local_node_id=provider_node.node_id,
                clock=lambda: current[0],
            )
            activation = TrustedComputeWorkerBinder(
                authority,
                lambda provider_id: SQLiteDispatchInbox(
                    tmp_path / f"dispatch-{provider_id}.sqlite3"
                ),
                clock=lambda: current[0] + timedelta(seconds=1),
            )
            owner_endpoint = RelayDispatchEndpoint(owner, request_timeout=TIMEOUT)
            provider_endpoint = RelayDispatchEndpoint(provider_node, request_timeout=TIMEOUT)
            activation.activate_on(provider_endpoint, CAPABILITY_ID)
            endpoints.extend((owner_endpoint, provider_endpoint))
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            first_request = request(
                session_id=session_id,
                owner_node_id=owner.node_id,
                provider_node_id=provider_node.node_id,
                dispatch_id="dispatch-relay-one",
                job_id="job-relay-one",
                sent_at=current[0],
            )
            first = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=first_request,
            )
            duplicate = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=first_request,
            )
            assert first.state is DispatchState.SUCCEEDED
            assert duplicate == first
            assert handler.calls == 1

            current[0] += timedelta(seconds=2)
            health.publish(
                report(
                    session_id=session_id,
                    provider_node_id=provider_node.node_id,
                    local_descriptor=local_descriptor,
                    revision=0,
                    reported_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                command_id="publish-f84-generation-two",
                provider_generation=2,
            )
            stale = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=request(
                    session_id=session_id,
                    owner_node_id=owner.node_id,
                    provider_node_id=provider_node.node_id,
                    dispatch_id="dispatch-relay-stale",
                    job_id="job-relay-stale",
                    sent_at=current[0],
                ),
            )
            assert stale.state is DispatchState.FAILED
            assert stale.events[-1].reason_code == "compute-activation-not-current"
            assert handler.calls == 1
        finally:
            if endpoints:
                await asyncio.gather(
                    *(endpoint.close() for endpoint in endpoints),
                    return_exceptions=True,
                )
            if provider_node is not None:
                await provider_node.disconnect()
            if owner is not None:
                await owner.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())

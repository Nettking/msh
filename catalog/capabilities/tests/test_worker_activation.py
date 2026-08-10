from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

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
    ProviderEnrollmentState,
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
from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 15, tzinfo=timezone.utc)
SESSION_ID = "session-f84"


class CountingHandler:
    def __init__(self, label: str = "compute") -> None:
        self.label = label
        self.calls = 0

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(
            True,
            {"handler": self.label, "job_id": job.job_id},
        )


class StubRelayClient:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(node.identity, token=token)


def join(
    coordinator: SessionCoordinator,
    *,
    owner: NodeCredentials,
    node: NodeCredentials,
    suffix: str,
) -> None:
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id=f"invite-{suffix}",
    )
    coordinator.join_session(
        node_id=node.identity.node_id,
        token=invitation["token"],
        request_id=f"join-{suffix}",
        expected_session_id=SESSION_ID,
    )


def environment(
    tmp_path: Path,
) -> tuple[
    SessionCoordinator,
    FederatedProviderEnrollmentService,
    FederatedProviderHealthService,
    NodeCredentials,
    NodeCredentials,
    list[datetime],
]:
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = credentials(tmp_path, "Owner")
    provider = credentials(tmp_path, "Provider")
    for node in (owner, provider):
        enroll(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.4 compute activation",
        request_id="create-f84-session",
        session_id=SESSION_ID,
    )
    join(coordinator, owner=owner, node=provider, suffix="provider")
    enrollments = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: current[0],
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
        clock=lambda: current[0],
    )
    return coordinator, enrollments, health, owner, provider, current


def descriptor(handler_id: str, operation: str = "echo") -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id=handler_id,
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        attributes={"operation": operation, "modalities": ["json"]},
    )


def announce_and_approve(
    coordinator: SessionCoordinator,
    enrollments: FederatedProviderEnrollmentService,
    *,
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
) -> None:
    coordinator.announce_capability(
        CapabilityAnnouncement(
            capability_id=capability_id,
            node_id=provider.identity.node_id,
            session_id=SESSION_ID,
            type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            status=CapabilityStatus.READY,
            properties={"operation": "echo", "modalities": ["json"]},
            announced_at=NOW,
        ),
        actor_node_id=provider.identity.node_id,
        request_id=f"announce-{capability_id}",
    )
    requested = enrollments.request(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=provider.identity.node_id,
        command_id=f"request-{capability_id}",
    )
    enrollments.approve(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=owner.identity.node_id,
        command_id=f"approve-{capability_id}",
        expected_revision=requested.revision,
    )


def publish_health(
    health: FederatedProviderHealthService,
    *,
    provider: NodeCredentials,
    capability_id: str,
    handler: LocalComputeHandlerDescriptor,
    generation: int,
    revision: int = 0,
    reported_at: datetime = NOW,
    expires_at: datetime | None = None,
) -> ProviderResourceReport:
    report = ProviderResourceReport(
        capability_id=capability_id,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type=handler.capability_type,
        protocol=handler.protocol,
        protocol_version=handler.protocol_version,
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            **handler.attributes,
            "activation": ComputeHandlerActivationReference(
                handler.handler_id,
                handler.descriptor_fingerprint,
            ).to_dict(),
        },
        reported_at=reported_at,
        expires_at=expires_at or reported_at + timedelta(seconds=30),
    )
    health.publish(
        report,
        actor_node_id=provider.identity.node_id,
        command_id=f"health-{capability_id}-{generation}-{revision}",
        provider_generation=generation,
    )
    return report


def active_job(job_id: str = "job-f84") -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=SESSION_ID,
        request_id=f"request-{job_id}",
        idempotency_key=f"{SESSION_ID}:{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"input-{job_id}",
                session_id=SESSION_ID,
                schema_name="fcp.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=f"output-{job_id}",
                session_id=SESSION_ID,
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_seconds=(),
            retryable_error_codes=(),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=60,
            queue_timeout_seconds=30,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
        status=JobStatus.ACTIVE,
        attempts=(JobAttempt(f"attempt-{job_id}", 1, AttemptStatus.ASSIGNED),),
    )


def dispatch_request(
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
    *,
    dispatch_id: str = "dispatch-f84",
    job_id: str = "job-f84",
    sent_at: datetime = NOW,
) -> DispatchRequest:
    job = active_job(job_id)
    return DispatchRequest(
        dispatch_id=dispatch_id,
        session_id=SESSION_ID,
        coordinator_node_id=owner.identity.node_id,
        target_node_id=provider.identity.node_id,
        provider_id=capability_id,
        job=job,
        attempt_id=job.attempts[0].attempt_id,
        attempt_number=1,
        lease_id=f"lease-{job_id}",
        lease_generation=1,
        lease_expires_at=sent_at + timedelta(seconds=30),
        sent_at=sent_at,
    )


def binder(
    health: FederatedProviderHealthService,
    inventory: LocalComputeHandlerInventory,
    provider: NodeCredentials,
    current: list[datetime],
    tmp_path: Path,
) -> TrustedComputeWorkerBinder:
    authority = ComputeWorkerActivationAuthority(
        health,
        inventory,
        session_id=SESSION_ID,
        local_node_id=provider.identity.node_id,
        clock=lambda: current[0],
    )
    return TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteDispatchInbox(
            tmp_path / f"dispatch-{provider_id}.sqlite3"
        ),
        clock=lambda: current[0] + timedelta(seconds=1),
    )


def handle(worker, request: DispatchRequest):
    return asyncio.run(
        worker.handle(
            request,
            authenticated_actor=request.coordinator_node_id,
            authenticated_session=request.session_id,
        )
    )


def test_descriptor_roundtrip_and_rejects_executable_metadata() -> None:
    value = descriptor("echo-handler")
    assert LocalComputeHandlerDescriptor.from_dict(value.to_dict()) == value
    serialized = str(value.to_dict())
    for forbidden in ("module_path", "command", "endpoint", "credential", "token"):
        assert forbidden not in serialized

    with pytest.raises(FederationValidationError) as exc_info:
        LocalComputeHandlerDescriptor(
            handler_id="unsafe-handler",
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            attributes={"module_path": "plugins.unsafe:run"},
        )
    assert exc_info.value.code == "unsafe-compute-handler-metadata"


def test_activation_executes_and_duplicate_delivery_replays_once(tmp_path: Path) -> None:
    coordinator, enrollments, health, owner, provider, current = environment(tmp_path)
    local_descriptor = descriptor("echo-handler")
    handler = CountingHandler()
    inventory = LocalComputeHandlerInventory()
    inventory.register(local_descriptor, handler)
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=provider,
        capability_id="compute-one",
    )
    publish_health(
        health,
        provider=provider,
        capability_id="compute-one",
        handler=local_descriptor,
        generation=1,
    )
    worker = binder(health, inventory, provider, current, tmp_path).activate("compute-one")
    request = dispatch_request(owner, provider, "compute-one")

    first = handle(worker, request)
    duplicate = handle(worker, replace(request, sent_at=NOW + timedelta(seconds=2)))

    assert first == duplicate
    assert first.state is DispatchState.SUCCEEDED
    assert first.events[-1].details == {"handler": "compute", "job_id": "job-f84"}
    assert handler.calls == 1


def test_inventory_replacement_and_generation_advance_fence_old_workers(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, provider, current = environment(tmp_path)
    local_descriptor = descriptor("replaceable-handler")
    first_handler = CountingHandler("first")
    inventory = LocalComputeHandlerInventory()
    inventory.register(local_descriptor, first_handler)
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=provider,
        capability_id="compute-replaced",
    )
    publish_health(
        health,
        provider=provider,
        capability_id="compute-replaced",
        handler=local_descriptor,
        generation=1,
    )
    activation = binder(health, inventory, provider, current, tmp_path)
    old_worker = activation.activate("compute-replaced")

    assert inventory.remove(
        local_descriptor.handler_id,
        expected_fingerprint=local_descriptor.descriptor_fingerprint,
    )
    second_handler = CountingHandler("second")
    inventory.register(local_descriptor, second_handler)
    replaced = handle(
        old_worker,
        dispatch_request(
            owner,
            provider,
            "compute-replaced",
            dispatch_id="dispatch-replaced",
            job_id="job-replaced",
        ),
    )
    assert replaced.state is DispatchState.FAILED
    assert replaced.events[-1].reason_code == "compute-activation-not-current"
    assert first_handler.calls == 0
    assert second_handler.calls == 0

    fresh_worker = activation.activate("compute-replaced")
    current[0] += timedelta(seconds=1)
    publish_health(
        health,
        provider=provider,
        capability_id="compute-replaced",
        handler=local_descriptor,
        generation=2,
        reported_at=current[0],
    )
    stale_generation = handle(
        fresh_worker,
        dispatch_request(
            owner,
            provider,
            "compute-replaced",
            dispatch_id="dispatch-generation",
            job_id="job-generation",
            sent_at=current[0],
        ),
    )
    assert stale_generation.state is DispatchState.FAILED
    assert stale_generation.events[-1].reason_code == "compute-activation-not-current"
    assert second_handler.calls == 0


def test_expiry_and_suspension_prevent_new_execution_without_deleting_record(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, provider, current = environment(tmp_path)
    local_descriptor = descriptor("fenced-handler")
    handler = CountingHandler()
    inventory = LocalComputeHandlerInventory()
    inventory.register(local_descriptor, handler)
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=provider,
        capability_id="compute-fenced",
    )
    publish_health(
        health,
        provider=provider,
        capability_id="compute-fenced",
        handler=local_descriptor,
        generation=1,
        expires_at=NOW + timedelta(seconds=5),
    )
    activation = binder(health, inventory, provider, current, tmp_path)
    expired_worker = activation.activate("compute-fenced")
    current[0] = NOW + timedelta(seconds=6)
    expired = handle(
        expired_worker,
        dispatch_request(
            owner,
            provider,
            "compute-fenced",
            dispatch_id="dispatch-expired",
            job_id="job-expired",
            sent_at=current[0],
        ),
    )
    assert expired.state is DispatchState.FAILED
    assert expired.events[-1].reason_code == "compute-activation-not-current"
    assert handler.calls == 0

    publish_health(
        health,
        provider=provider,
        capability_id="compute-fenced",
        handler=local_descriptor,
        generation=2,
        reported_at=current[0],
    )
    approved = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id="compute-fenced",
    )
    assert approved is not None and approved.state is ProviderEnrollmentState.APPROVED
    current_worker = activation.activate("compute-fenced")
    enrollments.suspend(
        session_id=SESSION_ID,
        capability_id="compute-fenced",
        actor_node_id=owner.identity.node_id,
        command_id="suspend-compute-fenced",
        expected_revision=approved.revision,
        reason_code="operator-suspended",
    )
    suspended = handle(
        current_worker,
        dispatch_request(
            owner,
            provider,
            "compute-fenced",
            dispatch_id="dispatch-suspended",
            job_id="job-suspended",
            sent_at=current[0],
        ),
    )
    assert suspended.state is DispatchState.FAILED
    assert suspended.events[-1].reason_code == "compute-activation-not-current"
    assert handler.calls == 0
    stored = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id="compute-fenced",
    )
    assert stored is not None and stored.state is ProviderEnrollmentState.SUSPENDED


def test_two_compute_providers_activate_distinct_handlers_and_exact_removal(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, provider, current = environment(tmp_path)
    inventory = LocalComputeHandlerInventory()
    first_descriptor = descriptor("first-handler")
    second_descriptor = descriptor("second-handler")
    first_handler = CountingHandler("first")
    second_handler = CountingHandler("second")
    inventory.register(first_descriptor, first_handler)
    inventory.register(second_descriptor, second_handler)
    for capability_id, local_descriptor in (
        ("compute-first", first_descriptor),
        ("compute-second", second_descriptor),
    ):
        announce_and_approve(
            coordinator,
            enrollments,
            owner=owner,
            provider=provider,
            capability_id=capability_id,
        )
        publish_health(
            health,
            provider=provider,
            capability_id=capability_id,
            handler=local_descriptor,
            generation=1,
        )
    activation = binder(health, inventory, provider, current, tmp_path)
    workers = activation.activate_all()
    assert tuple(worker.snapshot.provider_id for worker in workers) == (
        "compute-first",
        "compute-second",
    )

    endpoint = RelayDispatchEndpoint(StubRelayClient(provider.identity.node_id))
    first_worker = activation.activate_on(endpoint, "compute-first")
    replacement = activation.activate_on(endpoint, "compute-first")
    with pytest.raises(FederationValidationError) as changed:
        activation.deactivate_from(endpoint, first_worker)
    assert changed.value.code == "worker-registration-changed"
    assert activation.deactivate_from(endpoint, replacement)
    assert "compute-first" not in endpoint.workers

"""F8.4 activation must serve both F7.4 and F7.5 worker contracts."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.dispatch import (
    CapabilityWorker,
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
from catalog.capabilities.lifecycle_contracts import (
    CancellationRequest,
    CancellationState,
)
from catalog.capabilities.lifecycle_worker import (
    CancellableCapabilityWorker,
    SQLiteLifecycleDispatchInbox,
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
from catalog.capabilities.worker_activation import (
    ComputeHandlerActivationReference,
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)
CAPABILITY_ID = "activation-test-provider"


class _Handler:
    def __init__(self) -> None:
        self.calls = 0
        self.release = asyncio.Event()

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(True, {"job_id": job.job_id})


def _descriptor() -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id="activation-test-handler",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo"},
    )


def _report(session_id, node_id, descriptor) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=node_id,
        session_id=session_id,
        capability_type=descriptor.capability_type,
        protocol=descriptor.protocol,
        protocol_version=descriptor.protocol_version,
        status=ProviderStatus.READY,
        report_revision=0,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            **descriptor.attributes,
            "activation": ComputeHandlerActivationReference(
                descriptor.handler_id, descriptor.descriptor_fingerprint
            ).to_dict(),
        },
        reported_at=NOW,
        expires_at=NOW + timedelta(seconds=60),
    )


def _authority(tmp_path, handler):
    """Build a real coordinator, enrollment, health and activation authority."""

    credentials = IdentityStore(
        tmp_path / "identity", display_name="Activation Test"
    ).load_or_create(now=NOW)
    node_id = credentials.identity.node_id
    coordinator = SessionCoordinator(tmp_path / "control.sqlite3", clock=lambda: NOW)
    token = coordinator.create_enrollment_token(ttl_seconds=600, max_uses=1)["token"]
    coordinator.enroll_node(credentials.identity, token=token)
    session = coordinator.create_session(
        actor_node_id=node_id,
        display_name="Activation Test Session",
        request_id="create-activation-session",
    )
    session_id = str(session.session_id)
    descriptor = _descriptor()
    coordinator.announce_capability(
        CapabilityAnnouncement(
            capability_id=CAPABILITY_ID,
            node_id=node_id,
            session_id=session_id,
            type=descriptor.capability_type,
            protocol=descriptor.protocol,
            protocol_version=descriptor.protocol_version,
            status=CapabilityStatus.READY,
            properties=descriptor.attributes,
            announced_at=NOW,
        ),
        actor_node_id=node_id,
        request_id="announce-activation",
    )
    enrollments = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: NOW,
    )
    requested = enrollments.request(
        session_id=session_id,
        capability_id=CAPABILITY_ID,
        actor_node_id=node_id,
        command_id="request-activation",
    )
    enrollments.approve(
        session_id=session_id,
        capability_id=CAPABILITY_ID,
        actor_node_id=node_id,
        command_id="approve-activation",
        expected_revision=requested.revision,
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
        clock=lambda: NOW,
    )
    health.publish(
        _report(session_id, node_id, descriptor),
        actor_node_id=node_id,
        command_id="publish-activation",
        provider_generation=1,
    )
    inventory = LocalComputeHandlerInventory()
    inventory.register(descriptor, handler)
    authority = ComputeWorkerActivationAuthority(
        health,
        inventory,
        session_id=session_id,
        local_node_id=node_id,
        clock=lambda: NOW,
    )
    return authority, session_id, node_id


def _job(session_id: str) -> JobContract:
    return JobContract(
        job_id="job-activation",
        session_id=session_id,
        request_id="request-activation-job",
        idempotency_key=f"{session_id}:job-activation",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo"},
        ),
        inputs=(
            ArtifactReference(
                reference_id="input-activation",
                session_id=session_id,
                schema_name="fcp.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id="output-activation",
                session_id=session_id,
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(1, (), ()),
        timeout_policy=TimeoutPolicy(600, 300, 120, 300, 30),
        status=JobStatus.ACTIVE,
        attempts=(JobAttempt("attempt-activation", 1, AttemptStatus.ASSIGNED),),
    )


def _request(session_id, node_id):
    from catalog.capabilities.dispatch import DispatchRequest

    job = _job(session_id)
    return DispatchRequest(
        dispatch_id="dispatch-activation",
        session_id=session_id,
        coordinator_node_id=f"{node_id}-coordinator",
        target_node_id=node_id,
        provider_id=CAPABILITY_ID,
        job=job,
        attempt_id=job.attempts[0].attempt_id,
        attempt_number=1,
        lease_id="lease-activation",
        lease_generation=1,
        lease_expires_at=NOW + timedelta(minutes=10),
        sent_at=NOW,
    )


def test_default_binder_still_produces_the_original_f74_worker(tmp_path) -> None:
    handler = _Handler()
    authority, session_id, node_id = _authority(tmp_path, handler)
    binder = TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteDispatchInbox(tmp_path / f"inbox-{provider_id}.sqlite3"),
        clock=lambda: NOW,
    )

    activated = binder.activate(CAPABILITY_ID)
    response = asyncio.run(
        activated.handle(
            _request(session_id, node_id),
            authenticated_actor=f"{node_id}-coordinator",
            authenticated_session=session_id,
        )
    )

    assert isinstance(activated._worker, CapabilityWorker)
    assert not isinstance(activated._worker, CancellableCapabilityWorker)
    assert activated.cancellable is False
    assert response.state is DispatchState.SUCCEEDED
    assert handler.calls == 1


def test_default_activated_worker_refuses_cancellation_cleanly(tmp_path) -> None:
    handler = _Handler()
    authority, session_id, node_id = _authority(tmp_path, handler)
    binder = TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteDispatchInbox(tmp_path / f"inbox-{provider_id}.sqlite3"),
        clock=lambda: NOW,
    )
    activated = binder.activate(CAPABILITY_ID)

    with pytest.raises(FederationOperationError) as error:
        asyncio.run(
            activated.cancel(
                object(),
                authenticated_actor=node_id,
                authenticated_session=session_id,
            )
        )
    assert error.value.code == "compute-worker-not-cancellable"


def test_lifecycle_factory_produces_a_cancellable_activated_worker(tmp_path) -> None:
    handler = _Handler()
    authority, session_id, node_id = _authority(tmp_path, handler)
    binder = TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteLifecycleDispatchInbox(
            tmp_path / f"lifecycle-inbox-{provider_id}.sqlite3"
        ),
        clock=lambda: NOW,
        worker_factory=CancellableCapabilityWorker,
    )

    activated = binder.activate(CAPABILITY_ID)

    assert isinstance(activated._worker, CancellableCapabilityWorker)
    assert activated.cancellable is True
    # Activation evidence is unchanged by the worker contract.
    assert activated.snapshot.provider_id == CAPABILITY_ID
    assert activated.registration.node_id == node_id


def test_cancellable_activated_worker_answers_cancellation(tmp_path) -> None:
    handler = _Handler()
    authority, session_id, node_id = _authority(tmp_path, handler)
    binder = TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteLifecycleDispatchInbox(
            tmp_path / f"lifecycle-inbox-{provider_id}.sqlite3"
        ),
        clock=lambda: NOW,
        worker_factory=CancellableCapabilityWorker,
    )
    activated = binder.activate(CAPABILITY_ID)
    job = _job(session_id)
    cancellation = CancellationRequest(
        cancellation_id="cancel-activation",
        session_id=session_id,
        coordinator_node_id=f"{node_id}-coordinator",
        target_node_id=node_id,
        provider_id=CAPABILITY_ID,
        job_id=job.job_id,
        attempt_id=job.attempts[0].attempt_id,
        lease_id="lease-activation",
        lease_generation=1,
        reason="operator-cancelled",
        requested_at=NOW,
    )

    response = asyncio.run(
        activated.cancel(
            cancellation,
            authenticated_actor=f"{node_id}-coordinator",
            authenticated_session=session_id,
        )
    )

    assert response.state in {
        CancellationState.CANCELLED,
        CancellationState.NOT_RUNNING,
    }
    # Cancelling before start durably fences the attempt, so a later dispatch of
    # the same attempt must not execute the handler.
    later = asyncio.run(
        activated.handle(
            _request(session_id, node_id),
            authenticated_actor=f"{node_id}-coordinator",
            authenticated_session=session_id,
        )
    )
    assert later.state in {DispatchState.REJECTED, DispatchState.FAILED}
    assert handler.calls == 0


def test_worker_factory_must_return_a_capability_worker(tmp_path) -> None:
    handler = _Handler()
    authority, _session_id, _node_id = _authority(tmp_path, handler)
    binder = TrustedComputeWorkerBinder(
        authority,
        lambda provider_id: SQLiteDispatchInbox(tmp_path / f"inbox-{provider_id}.sqlite3"),
        clock=lambda: NOW,
        worker_factory=lambda *args, **kwargs: object(),
    )

    with pytest.raises(Exception) as error:
        binder.activate(CAPABILITY_ID)
    assert getattr(error.value, "code", "") == "invalid-compute-worker"

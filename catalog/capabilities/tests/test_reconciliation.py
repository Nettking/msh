from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.ai.remote_contracts import (
    RemoteAIInvocationRequest,
    RemoteAIInvocationResponse,
)
from catalog.ai.remote_provider import (
    RemoteAIHealthAuthority,
    TrustedRemoteLanguageModelBinder,
)
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager
from catalog.capabilities.dispatch import ExecutionResult, SQLiteDispatchInbox
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
from catalog.capabilities.reconciliation import (
    ProviderReconciliationCheckpoint,
    ReconciledBindingKind,
    SQLiteProviderReconciliationStore,
    TrustedProviderRuntimeReconciler,
)
from catalog.capabilities.relay_dispatch import RelayDispatchEndpoint
from catalog.capabilities.worker_activation import (
    ComputeHandlerActivationReference,
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError, FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 16, tzinfo=timezone.utc)
SESSION_ID = "session-f86"
AI_CAPABILITY = "remote-restart-model"
COMPUTE_CAPABILITY = "compute-restart-worker"


class RecordingTransport:
    def invoke(
        self,
        request: RemoteAIInvocationRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event,
    ) -> RemoteAIInvocationResponse:
        del timeout_seconds, cancellation_event
        return RemoteAIInvocationResponse.succeeded(
            request,
            content="reconciled answer",
            completed_at=request.sent_at + timedelta(milliseconds=1),
        )


class CountingHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, job) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(True, {"job_id": job.job_id})


class StubRelayClient:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id


class FailingCheckpointStore(SQLiteProviderReconciliationStore):
    def save(self, checkpoint, *, expected_checkpoint_revision):
        del checkpoint, expected_checkpoint_revision
        raise FederationOperationError(
            "checkpoint-write-failed",
            "injected reconciliation checkpoint failure",
        )


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


def stores(
    tmp_path: Path,
    current: list[datetime],
) -> tuple[
    SessionCoordinator,
    FederatedProviderEnrollmentService,
    FederatedProviderHealthService,
    SQLiteProviderReconciliationStore,
]:
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
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
    checkpoints = SQLiteProviderReconciliationStore(
        tmp_path / "reconciliation.sqlite3"
    )
    return coordinator, enrollments, health, checkpoints


def environment(
    tmp_path: Path,
) -> tuple[
    SessionCoordinator,
    FederatedProviderEnrollmentService,
    FederatedProviderHealthService,
    SQLiteProviderReconciliationStore,
    NodeCredentials,
    NodeCredentials,
    NodeCredentials,
    list[datetime],
]:
    current = [NOW]
    coordinator, enrollments, health, checkpoints = stores(tmp_path, current)
    owner = credentials(tmp_path, "Owner")
    ai_provider = credentials(tmp_path, "AI Provider")
    local_actor = credentials(tmp_path, "Compute Actor")
    for node in (owner, ai_provider, local_actor):
        enroll(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.6 restart reconciliation",
        request_id="create-f86-session",
        session_id=SESSION_ID,
    )
    join(coordinator, owner=owner, node=ai_provider, suffix="ai")
    join(coordinator, owner=owner, node=local_actor, suffix="compute")
    return (
        coordinator,
        enrollments,
        health,
        checkpoints,
        owner,
        ai_provider,
        local_actor,
        current,
    )


def announce_and_approve(
    coordinator: SessionCoordinator,
    enrollments: FederatedProviderEnrollmentService,
    *,
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
    capability_type: str,
    protocol: str,
    properties: dict,
) -> None:
    coordinator.announce_capability(
        CapabilityAnnouncement(
            capability_id=capability_id,
            node_id=provider.identity.node_id,
            session_id=SESSION_ID,
            type=capability_type,
            protocol=protocol,
            protocol_version="1.0",
            status=CapabilityStatus.READY,
            properties=properties,
            announced_at=NOW,
        ),
        actor_node_id=provider.identity.node_id,
        request_id=f"announce-{capability_id}",
    )
    pending = enrollments.request(
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
        expected_revision=pending.revision,
    )


def compute_descriptor() -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id="restart-handler",
        capability_type="synthetic-compute",
        protocol="msh-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo", "modalities": ["json"]},
    )


def publish_ai(
    health: FederatedProviderHealthService,
    provider: NodeCredentials,
    current: datetime,
    *,
    generation: int,
    revision: int,
    ttl_seconds: int = 120,
) -> ProviderResourceReport:
    report = ProviderResourceReport(
        capability_id=AI_CAPABILITY,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type="language-model",
        protocol="msh-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            "label": "Restart model",
            "models": ["small"],
            "modalities": ["text"],
            "features": {"timeout": True, "cancellation": True},
        },
        reported_at=current,
        expires_at=current + timedelta(seconds=ttl_seconds),
    )
    health.publish(
        report,
        actor_node_id=provider.identity.node_id,
        command_id=f"health-ai-{generation}-{revision}",
        provider_generation=generation,
    )
    return report


def publish_compute(
    health: FederatedProviderHealthService,
    provider: NodeCredentials,
    descriptor: LocalComputeHandlerDescriptor,
    current: datetime,
    *,
    generation: int,
    revision: int,
    ttl_seconds: int = 120,
) -> ProviderResourceReport:
    report = ProviderResourceReport(
        capability_id=COMPUTE_CAPABILITY,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type=descriptor.capability_type,
        protocol=descriptor.protocol,
        protocol_version=descriptor.protocol_version,
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            **descriptor.attributes,
            "activation": ComputeHandlerActivationReference(
                descriptor.handler_id,
                descriptor.descriptor_fingerprint,
            ).to_dict(),
        },
        reported_at=current,
        expires_at=current + timedelta(seconds=ttl_seconds),
    )
    health.publish(
        report,
        actor_node_id=provider.identity.node_id,
        command_id=f"health-compute-{generation}-{revision}",
        provider_generation=generation,
    )
    return report


def prepare_authority(
    *,
    coordinator: SessionCoordinator,
    enrollments: FederatedProviderEnrollmentService,
    health: FederatedProviderHealthService,
    owner: NodeCredentials,
    ai_provider: NodeCredentials,
    local_actor: NodeCredentials,
    current: list[datetime],
) -> tuple[LocalComputeHandlerInventory, LocalComputeHandlerDescriptor]:
    descriptor = compute_descriptor()
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=ai_provider,
        capability_id=AI_CAPABILITY,
        capability_type="language-model",
        protocol="msh-language-model",
        properties={"models": ["small"], "modalities": ["text"]},
    )
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=local_actor,
        capability_id=COMPUTE_CAPABILITY,
        capability_type=descriptor.capability_type,
        protocol=descriptor.protocol,
        properties=descriptor.attributes,
    )
    publish_ai(
        health,
        ai_provider,
        current[0],
        generation=1,
        revision=0,
    )
    publish_compute(
        health,
        local_actor,
        descriptor,
        current[0],
        generation=1,
        revision=0,
    )
    inventory = LocalComputeHandlerInventory()
    inventory.register(descriptor, CountingHandler())
    return inventory, descriptor


def reconciler(
    tmp_path: Path,
    *,
    coordinator: SessionCoordinator,
    health: FederatedProviderHealthService,
    checkpoints: SQLiteProviderReconciliationStore,
    local_actor: NodeCredentials,
    current: list[datetime],
    inventory: LocalComputeHandlerInventory,
    manager: ConfiguredLanguageModelRuntimeManager | None = None,
    endpoint: RelayDispatchEndpoint | None = None,
    maximum_replay_events: int = 4096,
) -> tuple[
    TrustedProviderRuntimeReconciler,
    ConfiguredLanguageModelRuntimeManager,
    RelayDispatchEndpoint,
]:
    manager = manager or ConfiguredLanguageModelRuntimeManager(session_id=SESSION_ID)
    endpoint = endpoint or RelayDispatchEndpoint(
        StubRelayClient(local_actor.identity.node_id)
    )
    ai_authority = RemoteAIHealthAuthority(
        health,
        session_id=SESSION_ID,
        actor_node_id=local_actor.identity.node_id,
        clock=lambda: current[0],
    )
    ai_binder = TrustedRemoteLanguageModelBinder(
        ai_authority,
        lambda _node_id, _capability_id: RecordingTransport(),
    )
    compute_authority = ComputeWorkerActivationAuthority(
        health,
        inventory,
        session_id=SESSION_ID,
        local_node_id=local_actor.identity.node_id,
        clock=lambda: current[0],
    )
    compute_binder = TrustedComputeWorkerBinder(
        compute_authority,
        lambda provider_id: SQLiteDispatchInbox(
            tmp_path / f"dispatch-{provider_id}.sqlite3"
        ),
        clock=lambda: current[0],
    )
    return (
        TrustedProviderRuntimeReconciler(
            coordinator,
            checkpoints,
            session_id=SESSION_ID,
            actor_node_id=local_actor.identity.node_id,
            ai_binder=ai_binder,
            ai_runtime_manager=manager,
            compute_binder=compute_binder,
            compute_endpoint=endpoint,
            replay_page_size=2,
            maximum_replay_events=maximum_replay_events,
            clock=lambda: current[0],
        ),
        manager,
        endpoint,
    )


def test_restart_reopens_all_authorities_and_rebuilds_exact_runtime(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollments,
        health,
        checkpoints,
        owner,
        ai_provider,
        local_actor,
        current,
    ) = environment(tmp_path)
    inventory, _descriptor = prepare_authority(
        coordinator=coordinator,
        enrollments=enrollments,
        health=health,
        owner=owner,
        ai_provider=ai_provider,
        local_actor=local_actor,
        current=current,
    )
    first, manager, endpoint = reconciler(
        tmp_path,
        coordinator=coordinator,
        health=health,
        checkpoints=checkpoints,
        local_actor=local_actor,
        current=current,
        inventory=inventory,
    )

    initial = first.reconcile()

    assert initial.changed is True
    assert initial.ai_provider_ids == (AI_CAPABILITY,)
    assert initial.compute_provider_ids == (COMPUTE_CAPABILITY,)
    assert manager.additional_provider_ids() == (AI_CAPABILITY,)
    assert tuple(endpoint.workers) == (COMPUTE_CAPABILITY,)
    stored = checkpoints.get(
        session_id=SESSION_ID,
        actor_node_id=local_actor.identity.node_id,
    )
    assert stored is not None
    assert stored.last_applied_revision == initial.applied_event_revision
    assert tuple(item.kind for item in stored.bindings) == (
        ReconciledBindingKind.COMPUTE,
        ReconciledBindingKind.REMOTE_AI,
    )

    reopened_coordinator, _reopened_enrollments, reopened_health, reopened_store = (
        stores(tmp_path, current)
    )
    reopened_inventory = LocalComputeHandlerInventory()
    reopened_inventory.register(compute_descriptor(), CountingHandler())
    restarted, restarted_manager, restarted_endpoint = reconciler(
        tmp_path,
        coordinator=reopened_coordinator,
        health=reopened_health,
        checkpoints=reopened_store,
        local_actor=local_actor,
        current=current,
        inventory=reopened_inventory,
    )

    rebuilt = restarted.reconcile()
    unchanged = restarted.reconcile()

    assert rebuilt.changed is True
    assert rebuilt.reason_code == "runtime-authority-rebuilt"
    assert restarted_manager.additional_provider_ids() == (AI_CAPABILITY,)
    assert tuple(restarted_endpoint.workers) == (COMPUTE_CAPABILITY,)
    assert unchanged.changed is False
    assert unchanged.reason_code == "reconciliation-unchanged"
    assert unchanged.checkpoint_revision == rebuilt.checkpoint_revision

    reopened_checkpoint = reopened_store.get(
        session_id=SESSION_ID,
        actor_node_id=local_actor.identity.node_id,
    )
    assert reopened_checkpoint is not None
    serialized = str(reopened_checkpoint.to_dict())
    for forbidden in (
        "properties",
        "attributes",
        "prompt",
        "result",
        "endpoint",
        "base_url",
        "credential",
        "token",
        "module",
        "executable",
        "backend_path",
        "artifact",
        "storage",
    ):
        assert forbidden not in serialized.casefold()


def test_report_revision_generation_and_expiry_replace_then_remove_bindings(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollments,
        health,
        checkpoints,
        owner,
        ai_provider,
        local_actor,
        current,
    ) = environment(tmp_path)
    inventory, descriptor = prepare_authority(
        coordinator=coordinator,
        enrollments=enrollments,
        health=health,
        owner=owner,
        ai_provider=ai_provider,
        local_actor=local_actor,
        current=current,
    )
    service, manager, endpoint = reconciler(
        tmp_path,
        coordinator=coordinator,
        health=health,
        checkpoints=checkpoints,
        local_actor=local_actor,
        current=current,
        inventory=inventory,
    )
    service.reconcile()
    old_ai = manager.additional_providers()[0]
    old_worker = endpoint.workers[COMPUTE_CAPABILITY]

    current[0] += timedelta(seconds=1)
    publish_ai(
        health,
        ai_provider,
        current[0],
        generation=1,
        revision=1,
        ttl_seconds=5,
    )
    publish_compute(
        health,
        local_actor,
        descriptor,
        current[0],
        generation=2,
        revision=0,
        ttl_seconds=5,
    )
    advanced = service.reconcile()

    assert advanced.changed is True
    assert manager.additional_providers()[0] is not old_ai
    assert endpoint.workers[COMPUTE_CAPABILITY] is not old_worker
    assert old_ai.resource_report(current[0]) is None
    with pytest.raises(FederationOperationError) as old_generation:
        service.compute_binder.authority.current_snapshot(
            COMPUTE_CAPABILITY,
            provider_generation=old_worker.snapshot.provider_generation,
            report_revision=old_worker.snapshot.report_revision,
            expected_binding_id=old_worker.snapshot.binding_id,
        )
    assert old_generation.value.code in {
        "compute-provider-not-current",
        "compute-provider-generation-mismatch",
    }

    current[0] += timedelta(seconds=6)
    expired = service.reconcile()

    assert expired.changed is True
    assert manager.additional_provider_ids() == ()
    assert endpoint.workers == {}
    for capability_id in (AI_CAPABILITY, COMPUTE_CAPABILITY):
        approval = enrollments.store.get(
            session_id=SESSION_ID,
            capability_id=capability_id,
        )
        assert approval is not None
        assert approval.state is ProviderEnrollmentState.APPROVED


def test_suspension_and_member_removal_clear_only_reconciler_owned_runtime(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollments,
        health,
        checkpoints,
        owner,
        ai_provider,
        local_actor,
        current,
    ) = environment(tmp_path)
    inventory, _descriptor = prepare_authority(
        coordinator=coordinator,
        enrollments=enrollments,
        health=health,
        owner=owner,
        ai_provider=ai_provider,
        local_actor=local_actor,
        current=current,
    )
    service, manager, endpoint = reconciler(
        tmp_path,
        coordinator=coordinator,
        health=health,
        checkpoints=checkpoints,
        local_actor=local_actor,
        current=current,
        inventory=inventory,
    )
    service.reconcile()
    ai_record = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id=AI_CAPABILITY,
    )
    assert ai_record is not None
    enrollments.suspend(
        session_id=SESSION_ID,
        capability_id=AI_CAPABILITY,
        actor_node_id=owner.identity.node_id,
        command_id="suspend-ai-f86",
        expected_revision=ai_record.revision,
        reason_code="operator-maintenance",
    )

    suspended = service.reconcile()

    assert suspended.ai_provider_ids == ()
    assert suspended.compute_provider_ids == (COMPUTE_CAPABILITY,)
    assert manager.additional_provider_ids() == ()
    assert tuple(endpoint.workers) == (COMPUTE_CAPABILITY,)

    coordinator.remove_member(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        target_node_id=local_actor.identity.node_id,
        request_id="remove-compute-actor-f86",
        reason="operator-removed",
    )
    with pytest.raises(AuthorizationError):
        service.reconcile()
    assert manager.additional_provider_ids() == ()
    assert endpoint.workers == {}


def test_checkpoint_failure_rolls_back_runtime_and_replay_limit_fails_closed(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollments,
        health,
        _checkpoints,
        owner,
        ai_provider,
        local_actor,
        current,
    ) = environment(tmp_path)
    inventory, _descriptor = prepare_authority(
        coordinator=coordinator,
        enrollments=enrollments,
        health=health,
        owner=owner,
        ai_provider=ai_provider,
        local_actor=local_actor,
        current=current,
    )
    failing = FailingCheckpointStore(tmp_path / "failing-reconciliation.sqlite3")
    service, manager, endpoint = reconciler(
        tmp_path,
        coordinator=coordinator,
        health=health,
        checkpoints=failing,
        local_actor=local_actor,
        current=current,
        inventory=inventory,
    )

    with pytest.raises(FederationOperationError) as failed_save:
        service.reconcile()
    assert failed_save.value.code == "checkpoint-write-failed"
    assert manager.additional_provider_ids() == ()
    assert endpoint.workers == {}
    assert failing.get(
        session_id=SESSION_ID,
        actor_node_id=local_actor.identity.node_id,
    ) is None

    bounded_store = SQLiteProviderReconciliationStore(
        tmp_path / "bounded-reconciliation.sqlite3"
    )
    bounded, bounded_manager, bounded_endpoint = reconciler(
        tmp_path,
        coordinator=coordinator,
        health=health,
        checkpoints=bounded_store,
        local_actor=local_actor,
        current=current,
        inventory=inventory,
        maximum_replay_events=1,
    )
    with pytest.raises(FederationOperationError) as replay_limit:
        bounded.reconcile()
    assert replay_limit.value.code == "reconciliation-replay-too-large"
    assert bounded_manager.additional_provider_ids() == ()
    assert bounded_endpoint.workers == {}


def test_checkpoint_roundtrip_rejects_cross_identity_lookup(tmp_path: Path) -> None:
    store = SQLiteProviderReconciliationStore(tmp_path / "checkpoint.sqlite3")
    checkpoint = ProviderReconciliationCheckpoint(
        session_id=SESSION_ID,
        actor_node_id="node-safe-actor",
        checkpoint_revision=1,
        last_applied_revision=4,
        bindings=(),
        updated_at=NOW,
    )
    store.save(checkpoint, expected_checkpoint_revision=0)

    assert store.get(
        session_id=SESSION_ID,
        actor_node_id="node-safe-actor",
    ) == checkpoint
    assert store.get(
        session_id=SESSION_ID,
        actor_node_id="node-other-actor",
    ) is None

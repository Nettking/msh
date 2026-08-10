from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.ai.remote_provider import RemoteAIHealthAuthority
from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorAction,
    ProviderOperatorSurface,
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
from catalog.capabilities.worker_activation import (
    ComputeHandlerActivationReference,
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError, FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 16, tzinfo=timezone.utc)
SESSION_ID = "session-f85"


class NoopHandler:
    async def execute(self, job):
        raise AssertionError(f"operator status must not execute {job}")


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def enroll_node(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(node.identity, token=token)


def join_session(
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


def environment(tmp_path: Path):
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = credentials(tmp_path, "Owner")
    provider = credentials(tmp_path, "Provider")
    reader = credentials(tmp_path, "Reader")
    outsider = credentials(tmp_path, "Outsider")
    for node in (owner, provider, reader, outsider):
        enroll_node(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.5 operator surface",
        request_id="create-f85-session",
        session_id=SESSION_ID,
    )
    join_session(coordinator, owner=owner, node=provider, suffix="provider")
    join_session(coordinator, owner=owner, node=reader, suffix="reader")
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
    inventory = LocalComputeHandlerInventory()
    compute_descriptor = LocalComputeHandlerDescriptor(
        handler_id="echo-handler",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo", "modalities": ["json"]},
    )
    inventory.register(compute_descriptor, NoopHandler())
    return (
        coordinator,
        enrollments,
        health,
        inventory,
        compute_descriptor,
        owner,
        provider,
        reader,
        outsider,
        current,
    )


def announce(
    coordinator: SessionCoordinator,
    *,
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


def approve(
    enrollments: FederatedProviderEnrollmentService,
    *,
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
) -> None:
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


def publish_language_model(
    health: FederatedProviderHealthService,
    *,
    provider: NodeCredentials,
    current: datetime,
    generation: int = 1,
    revision: int = 0,
) -> None:
    health.publish(
        ProviderResourceReport(
            capability_id="remote-model",
            node_id=provider.identity.node_id,
            session_id=SESSION_ID,
            capability_type="language-model",
            protocol="fcp-language-model",
            protocol_version="1.0",
            status=ProviderStatus.READY,
            report_revision=revision,
            max_concurrent_jobs=3,
            active_jobs=1,
            queue_depth=2,
            utilization_millis=450,
            attributes={
                "label": "Remote model",
                "models": ["small"],
                "modalities": ["text"],
                "features": {"timeout": True, "cancellation": True},
            },
            reported_at=current,
            expires_at=current + timedelta(seconds=30),
        ),
        actor_node_id=provider.identity.node_id,
        command_id=f"health-model-{generation}-{revision}",
        provider_generation=generation,
    )


def publish_compute(
    health: FederatedProviderHealthService,
    *,
    provider: NodeCredentials,
    descriptor: LocalComputeHandlerDescriptor,
    current: datetime,
    generation: int = 1,
    revision: int = 0,
    ttl_seconds: int = 30,
) -> None:
    health.publish(
        ProviderResourceReport(
            capability_id="compute-provider",
            node_id=provider.identity.node_id,
            session_id=SESSION_ID,
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            status=ProviderStatus.READY,
            report_revision=revision,
            max_concurrent_jobs=2,
            active_jobs=0,
            queue_depth=0,
            utilization_millis=100,
            attributes={
                **descriptor.attributes,
                "activation": ComputeHandlerActivationReference(
                    descriptor.handler_id,
                    descriptor.descriptor_fingerprint,
                ).to_dict(),
            },
            reported_at=current,
            expires_at=current + timedelta(seconds=ttl_seconds),
        ),
        actor_node_id=provider.identity.node_id,
        command_id=f"health-compute-{generation}-{revision}",
        provider_generation=generation,
    )


def ready_environment(tmp_path: Path):
    values = environment(tmp_path)
    (
        coordinator,
        enrollments,
        health,
        inventory,
        descriptor,
        owner,
        provider,
        _reader,
        _outsider,
        current,
    ) = values
    announce(
        coordinator,
        provider=provider,
        capability_id="remote-model",
        capability_type="language-model",
        protocol="fcp-language-model",
        properties={"label": "Remote model", "models": ["small"]},
    )
    announce(
        coordinator,
        provider=provider,
        capability_id="compute-provider",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        properties={"operation": "echo"},
    )
    for capability_id in ("remote-model", "compute-provider"):
        approve(
            enrollments,
            owner=owner,
            provider=provider,
            capability_id=capability_id,
        )
    publish_language_model(
        health,
        provider=provider,
        current=current[0],
    )
    publish_compute(
        health,
        provider=provider,
        descriptor=descriptor,
        current=current[0],
    )
    ai_authority = RemoteAIHealthAuthority(
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        clock=lambda: current[0],
    )
    compute_authority = ComputeWorkerActivationAuthority(
        health,
        inventory,
        session_id=SESSION_ID,
        local_node_id=provider.identity.node_id,
        clock=lambda: current[0],
    )
    surface = ProviderOperatorSurface(
        enrollments,
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ai_authority=ai_authority,
        compute_authority=compute_authority,
        clock=lambda: current[0],
    )
    return values, surface


def test_owner_view_combines_ai_compute_without_private_metadata(tmp_path: Path) -> None:
    (_values, surface) = ready_environment(tmp_path)

    view = surface.view()

    assert view.is_owner
    assert [provider.capability_id for provider in view.providers] == [
        "compute-provider",
        "remote-model",
    ]
    compute, model = view.providers
    assert compute.activation_state is ProviderActivationState.ELIGIBLE
    assert compute.inventory_compatible is True
    assert compute.activation_reason_code == "compute-handler-compatible"
    assert model.activation_state is ProviderActivationState.ELIGIBLE
    assert model.activation_reason_code == "remote-ai-eligible"
    assert model.active_jobs == 1
    assert model.queue_depth == 2

    serialized = json.dumps(view.to_dict(), sort_keys=True)
    for forbidden in (
        "properties",
        "attributes",
        "handler_id",
        "descriptor_fingerprint",
        "report_fingerprint",
        "enrollment_id",
        "models",
        "modalities",
    ):
        assert forbidden not in serialized


def test_member_view_is_read_only_and_outsider_cannot_read(tmp_path: Path) -> None:
    values, owner_surface = ready_environment(tmp_path)
    (
        _coordinator,
        enrollments,
        health,
        _inventory,
        _descriptor,
        _owner,
        _provider,
        reader,
        outsider,
        current,
    ) = values
    reader_surface = ProviderOperatorSurface(
        enrollments,
        health,
        session_id=SESSION_ID,
        actor_node_id=reader.identity.node_id,
        clock=lambda: current[0],
    )
    reader_view = reader_surface.view()
    assert not reader_view.is_owner
    assert all(not provider.allowed_actions for provider in reader_view.providers)
    with pytest.raises(AuthorizationError) as exc_info:
        reader_surface.execute(
            ProviderOperatorAction.SUSPEND,
            capability_id="remote-model",
            expected_revision=2,
        )
    assert exc_info.value.code == "provider-operator-action-not-authorized"

    outsider_surface = ProviderOperatorSurface(
        enrollments,
        health,
        session_id=SESSION_ID,
        actor_node_id=outsider.identity.node_id,
        clock=lambda: current[0],
    )
    with pytest.raises(AuthorizationError):
        outsider_surface.view()

    assert owner_surface.view().is_owner


def test_owner_commands_preserve_revision_and_idempotency_fences(tmp_path: Path) -> None:
    (
        coordinator,
        enrollments,
        health,
        _inventory,
        _descriptor,
        owner,
        provider,
        _reader,
        _outsider,
        current,
    ) = environment(tmp_path)
    announce(
        coordinator,
        provider=provider,
        capability_id="pending-provider",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        properties={"operation": "pending"},
    )
    surface = ProviderOperatorSurface(
        enrollments,
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        clock=lambda: current[0],
    )

    initial = surface.get("pending-provider")
    assert initial.enrollment_state == "not-requested"
    assert initial.allowed_actions == (ProviderOperatorAction.REQUEST,)

    pending = surface.execute(
        ProviderOperatorAction.REQUEST,
        capability_id="pending-provider",
        command_id="operator-request-one",
    )
    assert pending.enrollment_state == ProviderEnrollmentState.PENDING.value
    approved = surface.execute(
        ProviderOperatorAction.APPROVE,
        capability_id="pending-provider",
        expected_revision=pending.enrollment_revision,
        command_id="operator-approve-one",
    )
    assert approved.enrollment_state == ProviderEnrollmentState.APPROVED.value

    with pytest.raises(FederationOperationError) as stale:
        surface.execute(
            ProviderOperatorAction.SUSPEND,
            capability_id="pending-provider",
            expected_revision=pending.enrollment_revision,
            command_id="operator-stale-suspend",
        )
    assert stale.value.code == "stale-enrollment-revision"

    suspended = surface.execute(
        ProviderOperatorAction.SUSPEND,
        capability_id="pending-provider",
        expected_revision=approved.enrollment_revision,
        reason_code="operator-maintenance",
        command_id="operator-shared-command",
    )
    assert suspended.enrollment_state == ProviderEnrollmentState.SUSPENDED.value
    with pytest.raises(FederationOperationError) as conflict:
        surface.execute(
            ProviderOperatorAction.APPROVE,
            capability_id="pending-provider",
            expected_revision=suspended.enrollment_revision,
            command_id="operator-shared-command",
        )
    assert conflict.value.code == "idempotency-conflict"


def test_expiry_and_inventory_removal_change_status_not_approval(tmp_path: Path) -> None:
    values, surface = ready_environment(tmp_path)
    (
        _coordinator,
        enrollments,
        health,
        inventory,
        descriptor,
        _owner,
        provider,
        _reader,
        _outsider,
        current,
    ) = values

    current[0] += timedelta(seconds=31)
    expired = surface.get("compute-provider")
    assert expired.enrollment_state == ProviderEnrollmentState.APPROVED.value
    assert expired.health_state != "current"
    assert expired.activation_state is ProviderActivationState.UNAVAILABLE

    publish_compute(
        health,
        provider=provider,
        descriptor=descriptor,
        current=current[0],
        generation=2,
    )
    assert inventory.remove(
        descriptor.handler_id,
        expected_fingerprint=descriptor.descriptor_fingerprint,
    )
    incompatible = surface.get("compute-provider")
    assert incompatible.enrollment_state == ProviderEnrollmentState.APPROVED.value
    assert incompatible.activation_state is ProviderActivationState.INCOMPATIBLE
    assert incompatible.activation_reason_code == "compute-handler-not-installed"
    assert incompatible.inventory_compatible is False

    durable = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id="compute-provider",
    )
    assert durable is not None
    assert durable.state is ProviderEnrollmentState.APPROVED

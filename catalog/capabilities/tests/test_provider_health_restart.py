from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 12, 30, tzinfo=timezone.utc)
SESSION_ID = "session-f82-restart"
CAPABILITY_ID = "provider-after-restart"


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(node.identity, token=token)


def test_fresh_report_survives_full_service_restart_then_expires(
    tmp_path: Path,
) -> None:
    current = [NOW]
    coordinator_path = tmp_path / "coordinator.sqlite3"
    enrollment_path = tmp_path / "enrollment.sqlite3"
    health_path = tmp_path / "health.sqlite3"

    coordinator = SessionCoordinator(
        coordinator_path,
        clock=lambda: current[0],
    )
    owner = credentials(tmp_path, "Owner")
    provider = credentials(tmp_path, "Provider")
    enroll(coordinator, owner)
    enroll(coordinator, provider)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.2 restart session",
        request_id="create-restart-session",
        session_id=SESSION_ID,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-restart-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-restart-provider",
        expected_session_id=SESSION_ID,
    )
    announcement = CapabilityAnnouncement(
        capability_id=CAPABILITY_ID,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=CapabilityStatus.READY,
        properties={
            "label": CAPABILITY_ID,
            "models": ["small"],
            "modalities": ["text"],
        },
        announced_at=current[0],
    )
    coordinator.announce_capability(
        announcement,
        actor_node_id=provider.identity.node_id,
        request_id="announce-restart-provider",
    )

    enrollment_store = SQLiteProviderEnrollmentStore(enrollment_path)
    enrollment_service = FederatedProviderEnrollmentService(
        coordinator,
        enrollment_store,
        clock=lambda: current[0],
    )
    requested = enrollment_service.request(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
        actor_node_id=provider.identity.node_id,
        command_id="request-restart-provider",
    )
    approved = enrollment_service.approve(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
        actor_node_id=owner.identity.node_id,
        command_id="approve-restart-provider",
        expected_revision=requested.revision,
    )

    report = ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=0,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=100,
        attributes={
            "models": ["small"],
            "modalities": ["text"],
        },
        reported_at=current[0],
        expires_at=current[0] + timedelta(seconds=30),
    )
    health_store = SQLiteProviderHealthStore(health_path)
    health_service = FederatedProviderHealthService(
        enrollment_service,
        health_store,
        clock=lambda: current[0],
    )
    accepted = health_service.publish(
        report,
        actor_node_id=provider.identity.node_id,
        command_id="publish-before-restart",
        provider_generation=1,
    )

    restarted_coordinator = SessionCoordinator(
        coordinator_path,
        clock=lambda: current[0],
    )
    restarted_enrollment_store = SQLiteProviderEnrollmentStore(enrollment_path)
    restarted_enrollment_service = FederatedProviderEnrollmentService(
        restarted_coordinator,
        restarted_enrollment_store,
        clock=lambda: current[0],
    )
    restarted_health_store = SQLiteProviderHealthStore(health_path)
    restarted_health_service = FederatedProviderHealthService(
        restarted_enrollment_service,
        restarted_health_store,
        clock=lambda: current[0],
    )

    assert restarted_health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="language-model",
    ) == (report,)
    assert restarted_health_store.get(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
    ) == accepted
    assert restarted_enrollment_store.get(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
    ) == approved

    current[0] += timedelta(seconds=30)
    assert restarted_health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == ()
    observation = restarted_health_service.observe(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
        actor_node_id=owner.identity.node_id,
    )
    assert observation.state is ProviderHealthState.EXPIRED
    assert restarted_enrollment_store.get(
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
    ).state is ProviderEnrollmentState.APPROVED

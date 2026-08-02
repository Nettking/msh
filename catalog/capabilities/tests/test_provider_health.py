from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthRecord,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
SESSION_ID = "session-f82"


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


def join(
    coordinator: SessionCoordinator,
    *,
    owner: NodeCredentials,
    node: NodeCredentials,
    request_id: str,
) -> None:
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id=f"invite-{request_id}",
    )
    coordinator.join_session(
        node_id=node.identity.node_id,
        token=invitation["token"],
        request_id=f"join-{request_id}",
        expected_session_id=SESSION_ID,
    )


def environment(
    tmp_path: Path,
) -> tuple[
    SessionCoordinator,
    SQLiteProviderEnrollmentStore,
    FederatedProviderEnrollmentService,
    SQLiteProviderHealthStore,
    FederatedProviderHealthService,
    NodeCredentials,
    NodeCredentials,
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
    second = credentials(tmp_path, "Second Provider")
    outsider = credentials(tmp_path, "Outsider")
    for node in (owner, provider, second, outsider):
        enroll(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.2 test session",
        request_id="create-f82-session",
        session_id=SESSION_ID,
    )
    join(coordinator, owner=owner, node=provider, request_id="provider")
    join(coordinator, owner=owner, node=second, request_id="second")
    enrollment_store = SQLiteProviderEnrollmentStore(
        tmp_path / "enrollment.sqlite3"
    )
    enrollment_service = FederatedProviderEnrollmentService(
        coordinator,
        enrollment_store,
        clock=lambda: current[0],
    )
    health_store = SQLiteProviderHealthStore(tmp_path / "health.sqlite3")
    health_service = FederatedProviderHealthService(
        enrollment_service,
        health_store,
        clock=lambda: current[0],
    )
    return (
        coordinator,
        enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        second,
        outsider,
        current,
    )


def announcement(
    node: NodeCredentials,
    capability_id: str,
    *,
    status: CapabilityStatus = CapabilityStatus.READY,
    announced_at: datetime = NOW,
    protocol_version: str = "1.0",
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id=capability_id,
        node_id=node.identity.node_id,
        session_id=SESSION_ID,
        type="language-model",
        protocol="msh-language-model",
        protocol_version=protocol_version,
        status=status,
        properties={
            "label": capability_id,
            "models": ["small", "vision"],
            "modalities": ["text", "image"],
        },
        announced_at=announced_at,
    )


def approve(
    coordinator: SessionCoordinator,
    service: FederatedProviderEnrollmentService,
    *,
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
    request_suffix: str,
) -> ProviderEnrollmentRecord:
    value = announcement(provider, capability_id)
    coordinator.announce_capability(
        value,
        actor_node_id=provider.identity.node_id,
        request_id=f"announce-{request_suffix}",
    )
    requested = service.request(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=provider.identity.node_id,
        command_id=f"request-{request_suffix}",
    )
    return service.approve(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=owner.identity.node_id,
        command_id=f"approve-{request_suffix}",
        expected_revision=requested.revision,
    )


def report(
    provider: NodeCredentials,
    capability_id: str,
    *,
    revision: int = 0,
    reported_at: datetime = NOW,
    expires_at: datetime | None = None,
    status: ProviderStatus = ProviderStatus.READY,
    protocol_version: str = "1.0",
    queue_depth: int = 0,
    attributes: dict[str, object] | None = None,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type="language-model",
        protocol="msh-language-model",
        protocol_version=protocol_version,
        status=status,
        report_revision=revision,
        max_concurrent_jobs=4,
        active_jobs=1,
        queue_depth=queue_depth,
        utilization_millis=250,
        attributes=attributes
        or {
            "models": ["small", "vision"],
            "modalities": ["text", "image"],
        },
        reported_at=reported_at,
        expires_at=expires_at or reported_at + timedelta(seconds=30),
    )


def test_only_authenticated_provider_can_publish_and_readers_need_membership(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        _health_store,
        health_service,
        owner,
        provider,
        _second,
        outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-health",
        request_suffix="provider-health",
    )
    value = report(provider, "provider-health")

    with pytest.raises(AuthorizationError) as impersonation:
        health_service.publish(
            value,
            actor_node_id=owner.identity.node_id,
            command_id="impersonated-health",
            provider_generation=1,
        )
    assert impersonation.value.code == "provider-health-impersonation"

    accepted = health_service.publish(
        value,
        actor_node_id=provider.identity.node_id,
        command_id="provider-health-1",
        provider_generation=1,
    )
    assert accepted.report == value
    assert health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == (value,)

    with pytest.raises(AuthorizationError) as outsider_rejected:
        health_service.fresh_reports(
            session_id=SESSION_ID,
            actor_node_id=outsider.identity.node_id,
        )
    assert outsider_rejected.value.code == "not-session-member"


def test_multiple_reports_survive_restart_then_expire_without_revoking_approval(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        second,
        _outsider,
        current,
    ) = environment(tmp_path)
    first_enrollment = approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-a",
        request_suffix="provider-a",
    )
    second_enrollment = approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=second,
        capability_id="provider-b",
        request_suffix="provider-b",
    )
    first = report(provider, "provider-a")
    second_report = report(second, "provider-b")
    health_service.publish(
        first,
        actor_node_id=provider.identity.node_id,
        command_id="health-a",
        provider_generation=1,
    )
    health_service.publish(
        second_report,
        actor_node_id=second.identity.node_id,
        command_id="health-b",
        provider_generation=1,
    )

    restarted_store = SQLiteProviderHealthStore(health_store.database)
    restarted_service = FederatedProviderHealthService(
        enrollment_service,
        restarted_store,
        clock=lambda: current[0],
    )
    assert restarted_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="language-model",
    ) == (first, second_report)

    current[0] += timedelta(seconds=30)
    assert restarted_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == ()
    expired = restarted_service.observe(
        session_id=SESSION_ID,
        capability_id="provider-a",
        actor_node_id=owner.identity.node_id,
    )
    assert expired.state is ProviderHealthState.EXPIRED
    assert enrollment_store.get(
        session_id=SESSION_ID,
        capability_id="provider-a",
    ) == first_enrollment
    assert enrollment_store.get(
        session_id=SESSION_ID,
        capability_id="provider-b",
    ) == second_enrollment
    assert all(
        value.state is ProviderEnrollmentState.APPROVED
        for value in (first_enrollment, second_enrollment)
    )


def test_generation_revision_and_command_fences_are_monotonic(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-fenced",
        request_suffix="provider-fenced",
    )
    initial = report(provider, "provider-fenced", revision=0)
    accepted = health_service.publish(
        initial,
        actor_node_id=provider.identity.node_id,
        command_id="stable-health",
        provider_generation=1,
    )
    replay = health_service.publish(
        initial,
        actor_node_id=provider.identity.node_id,
        command_id="stable-health",
        provider_generation=1,
    )
    duplicate = health_service.publish(
        initial,
        actor_node_id=provider.identity.node_id,
        command_id="duplicate-health",
        provider_generation=1,
    )
    assert replay == accepted
    assert duplicate == accepted

    conflict = replace(initial, queue_depth=1)
    with pytest.raises(FederationOperationError) as reused_revision:
        health_service.publish(
            conflict,
            actor_node_id=provider.identity.node_id,
            command_id="conflicting-health",
            provider_generation=1,
        )
    assert reused_revision.value.code == "provider-report-revision-conflict"

    current[0] += timedelta(seconds=1)
    advanced = report(
        provider,
        "provider-fenced",
        revision=1,
        reported_at=current[0],
    )
    advanced_record = health_service.publish(
        advanced,
        actor_node_id=provider.identity.node_id,
        command_id="advanced-health",
        provider_generation=1,
    )
    assert advanced_record.report_revision == 1

    with pytest.raises(FederationOperationError) as stale_revision:
        health_service.publish(
            initial,
            actor_node_id=provider.identity.node_id,
            command_id="stale-revision",
            provider_generation=1,
        )
    assert stale_revision.value.code == "stale-provider-report-revision"

    current[0] += timedelta(seconds=1)
    restarted_report = report(
        provider,
        "provider-fenced",
        revision=0,
        reported_at=current[0],
    )
    restarted_record = health_service.publish(
        restarted_report,
        actor_node_id=provider.identity.node_id,
        command_id="new-generation",
        provider_generation=2,
    )
    assert restarted_record.provider_generation == 2
    assert restarted_record.report_revision == 0
    superseded = health_service.observe(
        session_id=SESSION_ID,
        capability_id="provider-fenced",
        actor_node_id=owner.identity.node_id,
        provider_generation=1,
    )
    assert superseded.state is ProviderHealthState.SUPERSEDED

    with pytest.raises(FederationOperationError) as stale_generation:
        health_service.publish(
            replace(advanced, report_revision=2),
            actor_node_id=provider.identity.node_id,
            command_id="old-generation",
            provider_generation=1,
        )
    assert stale_generation.value.code == "stale-provider-generation"
    assert {event.reason_code for event in health_store.audit_entries()} >= {
        "initial-report-accepted",
        "duplicate-report-replayed",
        "report-revision-advanced",
        "provider-generation-superseded",
    }


def test_future_expired_unsafe_and_incompatible_reports_fail_closed(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        current,
    ) = environment(tmp_path)
    enrollment = approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-invalid",
        request_suffix="provider-invalid",
    )

    future = report(
        provider,
        "provider-invalid",
        reported_at=current[0] + timedelta(seconds=1),
    )
    with pytest.raises(FederationOperationError) as future_rejected:
        health_service.publish(
            future,
            actor_node_id=provider.identity.node_id,
            command_id="future-health",
            provider_generation=1,
        )
    assert future_rejected.value.code == "provider-report-from-future"

    expired = report(
        provider,
        "provider-invalid",
        reported_at=current[0] - timedelta(seconds=31),
        expires_at=current[0],
    )
    with pytest.raises(FederationOperationError) as expired_rejected:
        health_service.publish(
            expired,
            actor_node_id=provider.identity.node_id,
            command_id="expired-health",
            provider_generation=1,
        )
    assert expired_rejected.value.code == "provider-report-expired"

    incompatible = report(
        provider,
        "provider-invalid",
        protocol_version="2.0",
    )
    with pytest.raises(FederationOperationError) as incompatible_rejected:
        health_store.publish(
            incompatible,
            enrollment,
            actor_node_id=provider.identity.node_id,
            command_id="incompatible-health",
            provider_generation=1,
            now=current[0],
        )
    assert incompatible_rejected.value.code == "provider-health-identity-mismatch"

    with pytest.raises(FederationValidationError) as unsafe_attributes:
        report(
            provider,
            "provider-invalid",
            attributes={"endpoint": "http://10.0.0.2:11434"},
        )
    assert unsafe_attributes.value.code == "nonpublic-provider-attribute"
    assert health_store.get(
        session_id=SESSION_ID,
        capability_id="provider-invalid",
    ) is None


def test_enrollment_and_announcement_changes_remove_live_capacity(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        enrollment_store,
        enrollment_service,
        _health_store,
        health_service,
        owner,
        provider,
        second,
        _outsider,
        current,
    ) = environment(tmp_path)
    first = approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-suspended",
        request_suffix="provider-suspended",
    )
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=second,
        capability_id="provider-disabled",
        request_suffix="provider-disabled",
    )
    health_service.publish(
        report(provider, "provider-suspended"),
        actor_node_id=provider.identity.node_id,
        command_id="health-suspended",
        provider_generation=1,
    )
    health_service.publish(
        report(second, "provider-disabled"),
        actor_node_id=second.identity.node_id,
        command_id="health-disabled",
        provider_generation=1,
    )

    current[0] += timedelta(seconds=1)
    enrollment_service.suspend(
        session_id=SESSION_ID,
        capability_id="provider-suspended",
        actor_node_id=owner.identity.node_id,
        command_id="suspend-provider",
        expected_revision=first.revision,
        reason_code="operator-maintenance",
    )
    assert health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == (report(second, "provider-disabled"),)
    suspended = health_service.observe(
        session_id=SESSION_ID,
        capability_id="provider-suspended",
        actor_node_id=owner.identity.node_id,
    )
    assert suspended.state is ProviderHealthState.ENROLLMENT_INELIGIBLE

    disabled_announcement = announcement(
        second,
        "provider-disabled",
        status=CapabilityStatus.DISABLED,
        announced_at=current[0],
    )
    coordinator.announce_capability(
        disabled_announcement,
        actor_node_id=second.identity.node_id,
        request_id="disable-announcement",
    )
    assert health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == ()
    disabled = health_service.observe(
        session_id=SESSION_ID,
        capability_id="provider-disabled",
        actor_node_id=owner.identity.node_id,
    )
    assert disabled.state is ProviderHealthState.ANNOUNCEMENT_INELIGIBLE
    assert enrollment_store.get(
        session_id=SESSION_ID,
        capability_id="provider-disabled",
    ).state is ProviderEnrollmentState.APPROVED


def test_removed_or_revoked_provider_cannot_republish_and_is_not_fresh(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        _health_store,
        health_service,
        owner,
        provider,
        _second,
        _outsider,
        current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-removed",
        request_suffix="provider-removed",
    )
    initial = report(provider, "provider-removed")
    health_service.publish(
        initial,
        actor_node_id=provider.identity.node_id,
        command_id="health-before-removal",
        provider_generation=1,
    )

    current[0] += timedelta(seconds=1)
    coordinator.remove_member(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        target_node_id=provider.identity.node_id,
        request_id="remove-health-provider",
        reason="provider removed",
    )
    assert health_service.fresh_reports(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
    ) == ()
    with pytest.raises(AuthorizationError) as removed_rejected:
        health_service.publish(
            replace(
                initial,
                report_revision=1,
                reported_at=current[0],
                expires_at=current[0] + timedelta(seconds=30),
            ),
            actor_node_id=provider.identity.node_id,
            command_id="health-after-removal",
            provider_generation=1,
        )
    assert removed_rejected.value.code in {
        "not-session-member",
        "node-revoked",
    }


def test_observations_are_safe_versioned_and_deterministic(tmp_path: Path) -> None:
    (
        coordinator,
        _enrollment_store,
        enrollment_service,
        health_store,
        health_service,
        owner,
        provider,
        second,
        _outsider,
        _current,
    ) = environment(tmp_path)
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=provider,
        capability_id="provider-present",
        request_suffix="provider-present",
    )
    approve(
        coordinator,
        enrollment_service,
        owner=owner,
        provider=second,
        capability_id="provider-absent",
        request_suffix="provider-absent",
    )
    accepted = health_service.publish(
        report(provider, "provider-present"),
        actor_node_id=provider.identity.node_id,
        command_id="health-present",
        provider_generation=7,
    )

    observations = health_service.observations(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        capability_type="language-model",
    )
    assert [item.capability_id for item in observations] == [
        "provider-absent",
        "provider-present",
    ]
    assert [item.state for item in observations] == [
        ProviderHealthState.ABSENT,
        ProviderHealthState.CURRENT,
    ]
    public = observations[1].to_dict()
    assert public["provider_generation"] == 7
    assert "report" not in public
    assert "attributes" not in public
    assert "endpoint" not in public
    assert "credentials" not in public
    assert "authority" not in public
    assert ProviderHealthRecord.from_dict(accepted.to_dict()) == accepted

    incompatible = accepted.to_dict()
    incompatible["schema"] = "msh.provider-health-record.v2"
    with pytest.raises(ProtocolCompatibilityError) as schema_rejected:
        ProviderHealthRecord.from_dict(incompatible)
    assert schema_rejected.value.code == "unsupported-schema-major"
    assert all(
        "endpoint" not in event.to_dict()
        and "credentials" not in event.to_dict()
        and "authority" not in event.to_dict()
        for event in health_store.audit_entries()
    )

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
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 11, tzinfo=timezone.utc)


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def enroll(
    coordinator: SessionCoordinator,
    node: NodeCredentials,
) -> None:
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(node.identity, token=token)


def session_fixture(
    tmp_path: Path,
) -> tuple[
    SessionCoordinator,
    SQLiteProviderEnrollmentStore,
    FederatedProviderEnrollmentService,
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
    outsider = credentials(tmp_path, "Outsider")
    for node in (owner, provider, outsider):
        enroll(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8 test session",
        request_id="create-f8-session",
        session_id="session-f8",
    )
    invitation = coordinator.create_invitation(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-provider",
        expected_session_id="session-f8",
    )
    store = SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3")
    service = FederatedProviderEnrollmentService(
        coordinator,
        store,
        clock=lambda: current[0],
    )
    return coordinator, store, service, owner, provider, outsider, current


def announcement(
    *,
    node_id: str,
    capability_id: str,
    status: CapabilityStatus = CapabilityStatus.READY,
    announced_at: datetime = NOW,
    protocol_version: str = "1.0",
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id=capability_id,
        node_id=node_id,
        session_id="session-f8",
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


def announce(
    coordinator: SessionCoordinator,
    value: CapabilityAnnouncement,
    *,
    request_id: str,
) -> None:
    coordinator.announce_capability(
        value,
        actor_node_id=value.node_id,
        request_id=request_id,
    )


def test_discovery_requires_active_session_membership_and_is_deterministic(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        _store,
        service,
        owner,
        provider,
        outsider,
        current,
    ) = session_fixture(tmp_path)
    values = (
        announcement(
            node_id=provider.identity.node_id,
            capability_id="provider-z",
        ),
        announcement(
            node_id=owner.identity.node_id,
            capability_id="provider-a",
        ),
    )
    announce(coordinator, values[0], request_id="announce-z")
    announce(coordinator, values[1], request_id="announce-a")

    discovered = service.discover(
        session_id="session-f8",
        actor_node_id=provider.identity.node_id,
        capability_type="language-model",
    )

    assert [item.capability_id for item in discovered] == [
        "provider-a",
        "provider-z",
    ]
    with pytest.raises(AuthorizationError) as outsider_rejected:
        service.discover(
            session_id="session-f8",
            actor_node_id=outsider.identity.node_id,
        )
    assert outsider_rejected.value.code == "not-session-member"

    current[0] += timedelta(seconds=1)
    coordinator.remove_member(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
        target_node_id=provider.identity.node_id,
        request_id="remove-provider",
        reason="provider removed",
    )
    with pytest.raises(AuthorizationError) as removed_rejected:
        service.discover(
            session_id="session-f8",
            actor_node_id=provider.identity.node_id,
        )
    assert removed_rejected.value.code == "not-session-member"


def test_approval_is_durable_and_supports_multiple_same_type_providers(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        store,
        service,
        owner,
        provider,
        _outsider,
        current,
    ) = session_fixture(tmp_path)
    first = announcement(
        node_id=owner.identity.node_id,
        capability_id="language-owner",
    )
    second = announcement(
        node_id=provider.identity.node_id,
        capability_id="language-provider",
    )
    announce(coordinator, first, request_id="announce-owner")
    announce(coordinator, second, request_id="announce-provider")

    owner_request = service.request(
        session_id="session-f8",
        capability_id=first.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="request-owner",
    )
    provider_request = service.request(
        session_id="session-f8",
        capability_id=second.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="request-provider",
    )
    current[0] += timedelta(seconds=1)
    approved_owner = service.approve(
        session_id="session-f8",
        capability_id=first.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-owner",
        expected_revision=owner_request.revision,
    )
    approved_provider = service.approve(
        session_id="session-f8",
        capability_id=second.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-provider",
        expected_revision=provider_request.revision,
    )

    assert approved_owner.state is ProviderEnrollmentState.APPROVED
    assert approved_provider.state is ProviderEnrollmentState.APPROVED
    assert approved_owner.eligible_for_resource_binding
    assert approved_provider.eligible_for_resource_binding

    restarted = SQLiteProviderEnrollmentStore(store.database)
    restarted_service = FederatedProviderEnrollmentService(
        coordinator,
        restarted,
        clock=lambda: current[0],
    )
    eligible = restarted_service.eligible_records(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
        capability_type="language-model",
    )
    assert [record.capability_id for record in eligible] == [
        "language-owner",
        "language-provider",
    ]
    assert all(record.revision == 2 for record in eligible)

    public_record = eligible[0].to_dict()
    assert "properties" not in public_record
    assert "endpoint" not in public_record
    assert "credentials" not in public_record
    assert "authority" not in public_record
    assert "command" not in public_record
    assert "handler" not in public_record
    assert {event.operation for event in restarted.audit_entries()} >= {
        "request",
        "approve",
    }


def test_commands_are_idempotent_conflict_safe_and_revision_fenced(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        store,
        service,
        owner,
        provider,
        _outsider,
        current,
    ) = session_fixture(tmp_path)
    value = announcement(
        node_id=provider.identity.node_id,
        capability_id="language-provider",
    )
    announce(coordinator, value, request_id="announce-provider")

    requested = service.request(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="stable-request",
    )
    replayed = service.request(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="stable-request",
    )
    assert replayed == requested
    assert store.get(
        session_id="session-f8",
        capability_id=value.capability_id,
    ).revision == 1

    with pytest.raises(FederationOperationError) as request_conflict:
        store.request(
            replace(
                value,
                status=CapabilityStatus.DRAINING,
                announced_at=NOW + timedelta(seconds=1),
            ),
            actor_node_id=provider.identity.node_id,
            command_id="stable-request",
            now=NOW + timedelta(seconds=1),
        )
    assert request_conflict.value.code == "idempotency-conflict"

    current[0] += timedelta(seconds=2)
    approved = service.approve(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="stable-approval",
        expected_revision=requested.revision,
    )
    approval_replay = service.approve(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="stable-approval",
        expected_revision=requested.revision,
    )
    assert approval_replay == approved

    with pytest.raises(FederationOperationError) as stale:
        service.suspend(
            session_id="session-f8",
            capability_id=value.capability_id,
            actor_node_id=owner.identity.node_id,
            command_id="stale-suspend",
            expected_revision=1,
            reason_code="operator-maintenance",
        )
    assert stale.value.code == "stale-enrollment-revision"
    assert store.get(
        session_id="session-f8",
        capability_id=value.capability_id,
    ) == approved


def test_only_session_owner_may_approve_and_identity_changes_fail_closed(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        store,
        service,
        owner,
        provider,
        _outsider,
        current,
    ) = session_fixture(tmp_path)
    value = announcement(
        node_id=provider.identity.node_id,
        capability_id="language-provider",
    )
    announce(coordinator, value, request_id="announce-provider")
    requested = service.request(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="request-provider",
    )

    with pytest.raises(AuthorizationError) as not_owner:
        service.approve(
            session_id="session-f8",
            capability_id=value.capability_id,
            actor_node_id=provider.identity.node_id,
            command_id="provider-self-approval",
            expected_revision=requested.revision,
        )
    assert not_owner.value.code == "provider-enrollment-not-authorized"

    with pytest.raises(FederationOperationError) as changed_node:
        store.reconcile(
            replace(value, node_id=owner.identity.node_id),
            actor_node_id=owner.identity.node_id,
            command_id="changed-node",
            expected_revision=requested.revision,
            now=current[0],
        )
    assert changed_node.value.code == "capability-identity-conflict"

    with pytest.raises(ProtocolCompatibilityError) as changed_major:
        store.reconcile(
            replace(value, protocol_version="2.0"),
            actor_node_id=owner.identity.node_id,
            command_id="changed-major",
            expected_revision=requested.revision,
            now=current[0],
        )
    assert changed_major.value.code == "incompatible-provider-protocol-major"

    with pytest.raises(FederationOperationError) as stale_announcement:
        store.reconcile(
            replace(value, announced_at=NOW - timedelta(seconds=1)),
            actor_node_id=owner.identity.node_id,
            command_id="stale-announcement",
            expected_revision=requested.revision,
            now=current[0],
        )
    assert stale_announcement.value.code == "stale-capability-announcement"


def test_unavailable_or_revoked_announcement_cannot_remain_eligible(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        store,
        service,
        owner,
        provider,
        _outsider,
        current,
    ) = session_fixture(tmp_path)
    value = announcement(
        node_id=provider.identity.node_id,
        capability_id="language-provider",
    )
    announce(coordinator, value, request_id="announce-ready")
    requested = service.request(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="request-provider",
    )
    current[0] += timedelta(seconds=1)
    approved = service.approve(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-provider",
        expected_revision=requested.revision,
    )
    assert service.eligible_records(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
    ) == (approved,)

    current[0] += timedelta(seconds=1)
    unavailable = replace(
        value,
        status=CapabilityStatus.UNAVAILABLE,
        announced_at=current[0],
    )
    announce(coordinator, unavailable, request_id="announce-unavailable")

    # The exact announcement fingerprint is checked before manual reconciliation.
    assert service.eligible_records(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
    ) == ()

    reconciled = service.reconcile(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="reconcile-unavailable",
        expected_revision=approved.revision,
    )
    assert reconciled.state is ProviderEnrollmentState.APPROVED
    assert reconciled.announcement_status is CapabilityStatus.UNAVAILABLE
    assert not reconciled.eligible_for_resource_binding

    current[0] += timedelta(seconds=1)
    ready_again = replace(
        value,
        announced_at=current[0],
    )
    announce(coordinator, ready_again, request_id="announce-ready-again")
    ready_record = service.reconcile(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="reconcile-ready",
        expected_revision=reconciled.revision,
    )
    assert ready_record.eligible_for_resource_binding

    current[0] += timedelta(seconds=1)
    coordinator.remove_member(
        session_id="session-f8",
        actor_node_id=owner.identity.node_id,
        target_node_id=provider.identity.node_id,
        request_id="remove-provider",
        reason="provider revoked from session",
    )
    revoked = service.reconcile(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="reconcile-revoked",
        expected_revision=ready_record.revision,
    )
    assert revoked.state is ProviderEnrollmentState.REVOKED
    assert revoked.announcement_status is CapabilityStatus.REVOKED
    assert not revoked.eligible_for_resource_binding
    assert SQLiteProviderEnrollmentStore(store.database).get(
        session_id="session-f8",
        capability_id=value.capability_id,
    ) == revoked

    with pytest.raises(FederationOperationError) as cannot_reopen:
        store.approve(
            replace(
                ready_again,
                announced_at=current[0] + timedelta(seconds=1),
            ),
            actor_node_id=owner.identity.node_id,
            command_id="reopen-revoked",
            expected_revision=revoked.revision,
            now=current[0] + timedelta(seconds=1),
        )
    assert cannot_reopen.value.code == "provider-enrollment-revoked"


def test_contract_versions_and_reason_redaction_fail_closed(
    tmp_path: Path,
) -> None:
    (
        coordinator,
        store,
        service,
        owner,
        provider,
        _outsider,
        current,
    ) = session_fixture(tmp_path)
    value = announcement(
        node_id=provider.identity.node_id,
        capability_id="language-provider",
    )
    announce(coordinator, value, request_id="announce-provider")
    requested = service.request(
        session_id="session-f8",
        capability_id=value.capability_id,
        actor_node_id=provider.identity.node_id,
        command_id="request-provider",
    )

    unsupported = requested.to_dict()
    unsupported["schema"] = "msh.provider-enrollment.v2"
    with pytest.raises(ProtocolCompatibilityError) as schema:
        ProviderEnrollmentRecord.from_dict(unsupported)
    assert schema.value.code == "unsupported-schema-major"

    unsupported_protocol = requested.to_dict()
    unsupported_protocol["enrollment_protocol_version"] = "2.0"
    with pytest.raises(ProtocolCompatibilityError) as protocol:
        ProviderEnrollmentRecord.from_dict(unsupported_protocol)
    assert protocol.value.code == "unsupported-enrollment-protocol-major"

    with pytest.raises(FederationValidationError) as endpoint_reason:
        store.suspend(
            value,
            actor_node_id=owner.identity.node_id,
            command_id="unsafe-reason",
            expected_revision=requested.revision,
            reason_code="see http://10.0.0.5:11434/private",
            now=current[0],
        )
    assert endpoint_reason.value.code == "nonpublic-enrollment-reason"

    with pytest.raises(FederationValidationError) as secret_property:
        CapabilityAnnouncement(
            capability_id="unsafe-provider",
            node_id=provider.identity.node_id,
            session_id="session-f8",
            type="language-model",
            protocol="msh-language-model",
            protocol_version="1.0",
            status=CapabilityStatus.READY,
            properties={"api_token": "super-secret-value"},
            announced_at=NOW,
        )
    assert secret_property.value.code == "secret-property"

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.capabilities.provider_enrollment import (
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.flask_app.services.pending_contribution_approval import (
    CapabilityFirstProviderEnrollmentService,
    CapabilityFirstProviderOperatorSurface,
    get_local_provider_operator_surface,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 11, 9, tzinfo=timezone.utc)
SESSION = "session-pending-contributions"


def _credentials(tmp_path: Path, name: str):
    return IdentityStore(
        tmp_path / name.casefold(),
        display_name=name,
    ).create(now=NOW)


def _environment(tmp_path: Path):
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = _credentials(tmp_path, "Owner")
    member = _credentials(tmp_path, "Member")
    for credentials in (owner, member):
        token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)[
            "token"
        ]
        coordinator.enroll_node(credentials.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Pending contribution test",
        request_id="create-pending-session",
        session_id=SESSION,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-member",
    )
    coordinator.join_session(
        node_id=member.identity.node_id,
        token=invitation["token"],
        request_id="join-member",
        expected_session_id=SESSION,
    )
    service = CapabilityFirstProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: current[0],
    )
    return coordinator, service, owner, member, current


def _announcement(node_id: str, *, status: CapabilityStatus, when: datetime):
    return CapabilityAnnouncement(
        capability_id="candidate-compute-one",
        node_id=node_id,
        session_id=SESSION,
        type="compute",
        protocol="fcp-compute-handler",
        protocol_version="1.0",
        status=status,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": "candidate-compute-one",
            "label": "Compute fcp-system-summary",
        },
        announced_at=when,
    )


def _request_registering(
    coordinator: SessionCoordinator,
    service: CapabilityFirstProviderEnrollmentService,
    member,
    current: list[datetime],
):
    registering = _announcement(
        member.identity.node_id,
        status=CapabilityStatus.REGISTERING,
        when=current[0],
    )
    coordinator.announce_capability(
        registering,
        actor_node_id=member.identity.node_id,
        request_id="announce-registering",
    )
    requested = service.request(
        session_id=SESSION,
        capability_id=registering.capability_id,
        actor_node_id=member.identity.node_id,
        command_id="request-registering",
    )
    return registering, requested


def test_registering_candidate_can_be_approved_without_becoming_eligible(
    tmp_path: Path,
) -> None:
    coordinator, service, owner, member, current = _environment(tmp_path)
    registering, requested = _request_registering(
        coordinator,
        service,
        member,
        current,
    )

    current[0] += timedelta(seconds=1)
    approved = service.approve(
        session_id=SESSION,
        capability_id=registering.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-registering",
        expected_revision=requested.revision,
    )

    assert approved.state is ProviderEnrollmentState.APPROVED
    assert approved.announcement_status is CapabilityStatus.REGISTERING
    assert approved.approved_by_node_id == owner.identity.node_id
    assert not approved.eligible_for_resource_binding
    assert service.eligible_records(
        session_id=SESSION,
        actor_node_id=owner.identity.node_id,
    ) == ()

    restarted = CapabilityFirstProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(service.store.database),
        clock=lambda: current[0],
    )
    persisted = restarted.store.get(
        session_id=SESSION,
        capability_id=registering.capability_id,
    )
    assert persisted == approved


def test_non_owner_cannot_approve_registering_candidate(tmp_path: Path) -> None:
    coordinator, service, _owner, member, current = _environment(tmp_path)
    registering, requested = _request_registering(
        coordinator,
        service,
        member,
        current,
    )

    with pytest.raises(AuthorizationError) as rejected:
        service.approve(
            session_id=SESSION,
            capability_id=registering.capability_id,
            actor_node_id=member.identity.node_id,
            command_id="member-self-approval",
            expected_revision=requested.revision,
        )

    assert rejected.value.code == "provider-enrollment-not-authorized"
    persisted = service.store.get(
        session_id=SESSION,
        capability_id=registering.capability_id,
    )
    assert persisted == requested


def test_approved_registering_candidate_becomes_eligible_only_after_ready_reconcile(
    tmp_path: Path,
) -> None:
    coordinator, service, owner, member, current = _environment(tmp_path)
    registering, requested = _request_registering(
        coordinator,
        service,
        member,
        current,
    )
    approved = service.approve(
        session_id=SESSION,
        capability_id=registering.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-registering",
        expected_revision=requested.revision,
    )

    current[0] += timedelta(seconds=2)
    ready = _announcement(
        member.identity.node_id,
        status=CapabilityStatus.READY,
        when=current[0],
    )
    coordinator.announce_capability(
        ready,
        actor_node_id=member.identity.node_id,
        request_id="announce-ready",
    )
    reconciled = service.reconcile(
        session_id=SESSION,
        capability_id=ready.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="reconcile-ready",
        expected_revision=approved.revision,
    )

    assert reconciled.state is ProviderEnrollmentState.APPROVED
    assert reconciled.announcement_status is CapabilityStatus.READY
    assert reconciled.eligible_for_resource_binding
    assert service.eligible_records(
        session_id=SESSION,
        actor_node_id=owner.identity.node_id,
    ) == (reconciled,)


def test_local_surface_factory_binds_only_the_real_session_creator(
    tmp_path: Path,
) -> None:
    coordinator, _service, owner, _member, current = _environment(tmp_path)

    class Onboarding:
        _clock = staticmethod(lambda: current[0])

        def authorized_context(self):
            return SimpleNamespace(
                credentials=owner,
                binding=SimpleNamespace(internal_session_id=SESSION),
                coordinator=coordinator,
            )

    app = Flask(__name__)
    app.config.update(
        CAPABILITY_ONBOARDING_SERVICE=Onboarding(),
        CAPABILITY_ONBOARDING_STATE_DATABASE=(
            tmp_path / "onboarding" / "onboarding.sqlite3"
        ),
    )

    with app.app_context():
        first = get_local_provider_operator_surface()
        second = get_local_provider_operator_surface()

    assert isinstance(first, CapabilityFirstProviderOperatorSurface)
    assert second is first
    assert first.view().is_owner is True
    assert first.view().actor_node_id == owner.identity.node_id


def test_remote_read_only_context_does_not_gain_leader_approval_authority(
    tmp_path: Path,
) -> None:
    _coordinator, _service, owner, _member, current = _environment(tmp_path)

    class Onboarding:
        _clock = staticmethod(lambda: current[0])

        def authorized_context(self):
            return SimpleNamespace(
                credentials=owner,
                binding=SimpleNamespace(internal_session_id=SESSION),
                coordinator=SimpleNamespace(store=SimpleNamespace()),
            )

    app = Flask(__name__)
    app.config.update(
        CAPABILITY_ONBOARDING_SERVICE=Onboarding(),
        CAPABILITY_ONBOARDING_STATE_DATABASE=(
            tmp_path / "onboarding" / "onboarding.sqlite3"
        ),
    )

    with app.app_context():
        assert get_local_provider_operator_surface() is None

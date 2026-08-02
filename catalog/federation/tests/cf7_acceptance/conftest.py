"""Reusable CF7-A fixtures built on existing federation authorities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
    FederationOnboardingDiscoveryService,
    SessionOnboardingAuthority,
)
from catalog.federation.onboarding_models import FederationSessionBinding
from catalog.node.identity import NodeCredentials

from .harness import DeviceState, MultiDeviceState, reconnect_after_restart

NOW = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class AuthorityFixture:
    state: MultiDeviceState
    database: Path
    coordinator: SessionCoordinator
    owner_device: DeviceState
    member_device: DeviceState
    observer_device: DeviceState
    owner: NodeCredentials
    member: NodeCredentials
    observer: NodeCredentials

    @staticmethod
    def clock() -> datetime:
        return NOW


@dataclass(frozen=True)
class CandidateFixture:
    session_id: str
    candidate: FederationDiscoveryCandidate


@dataclass(frozen=True)
class JoinedFixture:
    authority: AuthorityFixture
    coordinator: SessionCoordinator
    candidate: FederationDiscoveryCandidate
    binding: FederationSessionBinding
    member: NodeCredentials


@dataclass(frozen=True)
class RevokedFixture:
    joined: JoinedFixture
    removed: bool


def _enroll(coordinator: SessionCoordinator, credentials: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(credentials.identity, token=token)


def _candidate(
    fixture: AuthorityFixture,
    coordinator: SessionCoordinator,
    *,
    session_id: str,
    label: str,
    verification_code: str,
    prior_trust: bool = False,
) -> FederationDiscoveryCandidate:
    coordinator.create_session(
        actor_node_id=fixture.owner.identity.node_id,
        display_name=label,
        request_id=f"create-{session_id}",
        session_id=session_id,
    )
    invitation = coordinator.create_invitation(
        session_id=session_id,
        actor_node_id=fixture.owner.identity.node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id=f"invite-{session_id}",
    )
    return FederationDiscoveryCandidate(
        federation_label=label,
        internal_session_id=session_id,
        federation_fingerprint=f"fingerprint-{session_id}",
        transport_kind="configured",
        verification_code=verification_code,
        discovered_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        prior_trust=prior_trust,
        invitation_token=invitation["token"],
    )


@pytest.fixture
def authority_fixture(tmp_path: Path) -> AuthorityFixture:
    state = MultiDeviceState.create(
        tmp_path / "devices",
        ("Owner device", "Member device", "Observer device"),
    )
    owner_device, member_device, observer_device = state.devices
    owner = owner_device.create_identity(now=NOW)
    member = member_device.create_identity(now=NOW)
    observer = observer_device.create_identity(now=NOW)
    database = tmp_path / "authority" / "coordinator.sqlite3"
    database.parent.mkdir(parents=True)
    coordinator = SessionCoordinator(database, clock=AuthorityFixture.clock)
    for credentials in (owner, member, observer):
        _enroll(coordinator, credentials)
    return AuthorityFixture(
        state=state,
        database=database,
        coordinator=coordinator,
        owner_device=owner_device,
        member_device=member_device,
        observer_device=observer_device,
        owner=owner,
        member=member,
        observer=observer,
    )


@pytest.fixture
def multiple_federation_candidates(
    authority_fixture: AuthorityFixture,
) -> tuple[CandidateFixture, CandidateFixture]:
    first = _candidate(
        authority_fixture,
        authority_fixture.coordinator,
        session_id="session-cf7a-alpha",
        label="Alpha federation",
        verification_code="ALPHA-7031",
    )
    second = _candidate(
        authority_fixture,
        authority_fixture.coordinator,
        session_id="session-cf7a-beta",
        label="Beta federation",
        verification_code="BETA-1942",
    )
    return (
        CandidateFixture(first.internal_session_id, first),
        CandidateFixture(second.internal_session_id, second),
    )


@pytest.fixture
def joined_membership(authority_fixture: AuthorityFixture) -> JoinedFixture:
    candidate = _candidate(
        authority_fixture,
        authority_fixture.coordinator,
        session_id="session-cf7a-join",
        label="Join fixture federation",
        verification_code="JOIN-4826",
    )
    service = FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(
            authority_fixture.coordinator,
            clock=AuthorityFixture.clock,
        ),
        (ConfiguredFederationDiscoveryAdapter((candidate,)),),
        clock=AuthorityFixture.clock,
    )
    result = service.discover()[0]
    binding = service.connect(
        authority_fixture.member.identity,
        request_id="join-member-cf7a",
        discovery_id=result.discovery_id,
        verification_code=result.verification_code,
    )
    return JoinedFixture(
        authority=authority_fixture,
        coordinator=authority_fixture.coordinator,
        candidate=candidate,
        binding=binding,
        member=authority_fixture.member,
    )


@pytest.fixture
def reconnected_membership(joined_membership: JoinedFixture) -> JoinedFixture:
    coordinator, member, binding = reconnect_after_restart(
        database=joined_membership.authority.database,
        device=joined_membership.authority.member_device,
        binding=joined_membership.binding,
        clock=AuthorityFixture.clock,
    )
    return JoinedFixture(
        authority=joined_membership.authority,
        coordinator=coordinator,
        candidate=joined_membership.candidate,
        binding=binding,
        member=member,
    )


@pytest.fixture
def revoked_membership(reconnected_membership: JoinedFixture) -> RevokedFixture:
    removed = reconnected_membership.coordinator.remove_member(
        session_id=reconnected_membership.binding.internal_session_id,
        actor_node_id=reconnected_membership.authority.owner.identity.node_id,
        target_node_id=reconnected_membership.member.identity.node_id,
        request_id="remove-member-cf7a",
        reason="CF7-A controlled membership revocation fixture",
    )
    return RevokedFixture(joined=reconnected_membership, removed=removed)


@pytest.fixture
def controlled_rejoin(revoked_membership: RevokedFixture) -> JoinedFixture:
    joined = revoked_membership.joined
    invitation = joined.coordinator.create_invitation(
        session_id=joined.binding.internal_session_id,
        actor_node_id=joined.authority.owner.identity.node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id="rejoin-invitation-cf7a",
    )
    candidate = FederationDiscoveryCandidate(
        federation_label="Join fixture federation",
        internal_session_id=joined.binding.internal_session_id,
        federation_fingerprint=(
            f"fingerprint-{joined.binding.internal_session_id}"
        ),
        transport_kind="configured",
        verification_code="REJOIN-6913",
        discovered_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        prior_trust=True,
        invitation_token=invitation["token"],
    )
    service = FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(
            joined.coordinator,
            clock=AuthorityFixture.clock,
        ),
        (ConfiguredFederationDiscoveryAdapter((candidate,)),),
        clock=AuthorityFixture.clock,
    )
    result = service.discover()[0]
    binding = service.connect(
        joined.member.identity,
        request_id="controlled-rejoin-cf7a",
        discovery_id=result.discovery_id,
    )
    return JoinedFixture(
        authority=joined.authority,
        coordinator=joined.coordinator,
        candidate=candidate,
        binding=binding,
        member=joined.member,
    )

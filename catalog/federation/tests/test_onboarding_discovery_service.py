from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationDiscoveryCandidate,
    FederationOnboardingDiscoveryService,
    RelayFederationDiscoveryAdapter,
    SessionOnboardingAuthority,
)
from catalog.federation.onboarding_models import FederationConnectionState
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 20, tzinfo=timezone.utc)


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    state = tmp_path / f"identity-{name.casefold().replace(' ', '-')}"
    return IdentityStore(state, display_name=name).create(now=NOW)


def enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(node.identity, token=token)


def create_session(
    coordinator: SessionCoordinator,
    owner: NodeCredentials,
    *,
    session_id: str,
) -> None:
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name=f"Federation {session_id}",
        request_id=f"create-{session_id}",
        session_id=session_id,
    )


def candidate(
    coordinator: SessionCoordinator,
    owner: NodeCredentials,
    *,
    session_id: str,
    label: str,
    fingerprint: str,
    code: str,
    enrollment_token: str | None,
    prior_trust: bool = False,
    auto_accept_authorized: bool = False,
) -> FederationDiscoveryCandidate:
    invitation_token = None
    if not prior_trust:
        invitation_token = coordinator.create_invitation(
            session_id=session_id,
            actor_node_id=owner.identity.node_id,
            ttl_seconds=300,
            max_uses=1,
            request_id=f"invite-{session_id}",
        )["token"]
    return FederationDiscoveryCandidate(
        federation_label=label,
        internal_session_id=session_id,
        federation_fingerprint=fingerprint,
        transport_kind="configured-relay",
        verification_code=code,
        prior_trust=prior_trust,
        auto_accept_authorized=auto_accept_authorized,
        discovered_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
        enrollment_token=enrollment_token,
        invitation_token=invitation_token,
    )


def service_for(
    coordinator: SessionCoordinator,
    *candidates: FederationDiscoveryCandidate,
    allow_authenticated_auto_accept: bool = False,
) -> FederationOnboardingDiscoveryService:
    return FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(coordinator, clock=lambda: NOW),
        [ConfiguredFederationDiscoveryAdapter(candidates)],
        clock=lambda: NOW,
        allow_authenticated_auto_accept=allow_authenticated_auto_accept,
    )


def remote_fixture(tmp_path: Path):
    database = tmp_path / "coordinator.sqlite3"
    coordinator = SessionCoordinator(database, clock=lambda: NOW)
    owner = credentials(tmp_path, "Owner")
    joining = credentials(tmp_path, "Joining device")
    enroll(coordinator, owner)
    create_session(coordinator, owner, session_id="session-alpha")
    enrollment_token = coordinator.create_enrollment_token(
        ttl_seconds=300,
        max_uses=1,
    )["token"]
    discovered = candidate(
        coordinator,
        owner,
        session_id="session-alpha",
        label="Alpha federation",
        fingerprint="fingerprint-alpha",
        code="ALPHA-42",
        enrollment_token=enrollment_token,
    )
    return database, coordinator, owner, joining, enrollment_token, discovered


def test_discovery_is_safe_and_grants_no_authority(tmp_path: Path) -> None:
    (
        _database,
        coordinator,
        _owner,
        joining,
        enrollment_token,
        discovered,
    ) = remote_fixture(tmp_path)
    discovery = service_for(coordinator, discovered)

    results = discovery.discover()

    assert len(results) == 1
    assert results[0].state is FederationConnectionState.VERIFICATION_REQUIRED
    assert results[0].verification_code == "ALPHA-42"
    payload = json.dumps(results[0].to_dict(), sort_keys=True)
    assert enrollment_token not in payload
    assert discovered.invitation_token not in payload
    assert enrollment_token not in repr(discovered)
    assert discovered.invitation_token not in repr(discovered)
    assert coordinator.store.get_node(joining.identity.node_id) is None
    assert coordinator.session_ids_for_node(joining.identity.node_id) == ()


def test_wrong_verification_code_has_no_enrollment_or_membership_effect(
    tmp_path: Path,
) -> None:
    (
        _database,
        coordinator,
        _owner,
        joining,
        _enrollment_token,
        discovered,
    ) = remote_fixture(tmp_path)
    discovery = service_for(coordinator, discovered)
    discovery.discover()

    with pytest.raises(AuthenticationError) as raised:
        discovery.connect(
            joining.identity,
            request_id="join-alpha",
            verification_code="WRONG-CODE",
        )

    assert raised.value.code == "onboarding-verification-failed"
    assert coordinator.store.get_node(joining.identity.node_id) is None
    assert coordinator.session_ids_for_node(joining.identity.node_id) == ()


def test_verified_join_reuses_enrollment_invitation_and_session_authority(
    tmp_path: Path,
) -> None:
    (
        database,
        coordinator,
        _owner,
        joining,
        _enrollment_token,
        discovered,
    ) = remote_fixture(tmp_path)
    discovery = service_for(coordinator, discovered)
    discovery.discover()

    first = discovery.connect(
        joining.identity,
        request_id="stable-join-alpha",
        verification_code="ALPHA-42",
    )
    revision_after_first = coordinator.store.get_session("session-alpha").revision
    second = discovery.connect(
        joining.identity,
        request_id="stable-join-alpha",
        verification_code="ALPHA-42",
    )

    assert first == second
    assert first.internal_session_id == "session-alpha"
    assert first.device_id == joining.identity.node_id
    assert first.state is FederationConnectionState.CONNECTED
    assert first.trusted is True
    coordinator.store.require_membership(
        session_id="session-alpha",
        node_id=joining.identity.node_id,
    )
    assert coordinator.store.get_session("session-alpha").revision == revision_after_first
    with sqlite3.connect(database) as connection:
        invitation_use_count = connection.execute(
            "SELECT use_count FROM session_invitations"
        ).fetchone()[0]
        join_request_count = connection.execute(
            "SELECT COUNT(*) FROM session_join_requests"
        ).fetchone()[0]
    assert invitation_use_count == 1
    assert join_request_count == 1


def test_several_candidates_require_selection_and_only_selected_session_joins(
    tmp_path: Path,
) -> None:
    coordinator = SessionCoordinator(tmp_path / "several.sqlite3", clock=lambda: NOW)
    owner = credentials(tmp_path, "Owner")
    joining = credentials(tmp_path, "Joining")
    enroll(coordinator, owner)
    create_session(coordinator, owner, session_id="session-a")
    create_session(coordinator, owner, session_id="session-b")
    first = candidate(
        coordinator,
        owner,
        session_id="session-a",
        label="Federation A",
        fingerprint="fingerprint-a",
        code="CODE-A",
        enrollment_token=coordinator.create_enrollment_token(
            ttl_seconds=300,
            max_uses=1,
        )["token"],
    )
    second = candidate(
        coordinator,
        owner,
        session_id="session-b",
        label="Federation B",
        fingerprint="fingerprint-b",
        code="CODE-B",
        enrollment_token=coordinator.create_enrollment_token(
            ttl_seconds=300,
            max_uses=1,
        )["token"],
    )
    discovery = service_for(coordinator, first, second)
    results = discovery.discover()

    with pytest.raises(FederationOperationError) as required:
        discovery.connect(
            joining.identity,
            request_id="join-unspecified",
            verification_code="CODE-B",
        )
    assert required.value.code == "candidate-selection-required"

    selected = next(result for result in results if result.federation_label == "Federation B")
    binding = discovery.connect(
        joining.identity,
        request_id="join-b",
        discovery_id=selected.discovery_id,
        verification_code="CODE-B",
    )

    assert binding.internal_session_id == "session-b"
    assert coordinator.session_ids_for_node(joining.identity.node_id) == ("session-b",)


def test_trusted_reconnect_mints_no_new_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        _database,
        coordinator,
        owner,
        joining,
        _enrollment_token,
        discovered,
    ) = remote_fixture(tmp_path)
    discovery = service_for(coordinator, discovered)
    discovery.discover()
    binding = discovery.connect(
        joining.identity,
        request_id="join-alpha",
        verification_code="ALPHA-42",
    )
    revision = coordinator.store.get_session("session-alpha").revision

    def forbidden(*_args, **_kwargs):
        raise AssertionError("reconnect attempted to mint authority")

    monkeypatch.setattr(coordinator, "create_enrollment_token", forbidden)
    monkeypatch.setattr(coordinator, "create_invitation", forbidden)
    monkeypatch.setattr(coordinator, "join_session", forbidden)

    reconnected = discovery.reconnect(joining.identity, binding)

    assert reconnected == binding
    assert coordinator.store.get_session("session-alpha").revision == revision

    coordinator.remove_member(
        session_id="session-alpha",
        actor_node_id=owner.identity.node_id,
        target_node_id=joining.identity.node_id,
        request_id="remove-joining",
        reason="test-removal",
    )
    with pytest.raises(AuthorizationError):
        discovery.reconnect(joining.identity, binding)


def test_no_candidate_creates_local_federation_through_existing_authority(
    tmp_path: Path,
) -> None:
    coordinator = SessionCoordinator(tmp_path / "local.sqlite3", clock=lambda: NOW)
    device = credentials(tmp_path, "Local device")
    discovery = FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(coordinator, clock=lambda: NOW),
        [],
        clock=lambda: NOW,
    )

    assert discovery.discover() == ()
    binding = discovery.connect(
        device.identity,
        request_id="create-local",
        local_display_name="Workshop federation",
        local_session_id="session-local",
    )

    assert binding.internal_session_id == "session-local"
    assert binding.state is FederationConnectionState.CONNECTED
    assert coordinator.public_identity(device.identity.node_id) == device.identity
    coordinator.store.require_membership(
        session_id="session-local",
        node_id=device.identity.node_id,
    )


def test_discovery_failure_is_safe_and_blocks_accidental_local_creation(
    tmp_path: Path,
) -> None:
    coordinator = SessionCoordinator(tmp_path / "failure.sqlite3", clock=lambda: NOW)
    device = credentials(tmp_path, "Device")

    def broken_resolver(_deadline):
        raise RuntimeError("https://10.0.0.5/join?token=private-secret")

    discovery = FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(coordinator, clock=lambda: NOW),
        [RelayFederationDiscoveryAdapter(broken_resolver)],
        clock=lambda: NOW,
    )

    results = discovery.discover()

    assert len(results) == 1
    assert results[0].state is FederationConnectionState.UNAVAILABLE
    safe = json.dumps(results[0].to_dict(), sort_keys=True)
    assert "10.0.0.5" not in safe
    assert "private-secret" not in safe
    with pytest.raises(FederationOperationError) as blocked:
        discovery.connect(device.identity, request_id="must-not-create")
    assert blocked.value.code == "onboarding-discovery-unavailable"
    assert coordinator.store.get_node(device.identity.node_id) is None


def test_auto_accept_requires_both_local_and_candidate_policy(tmp_path: Path) -> None:
    (
        _database,
        coordinator,
        _owner,
        joining,
        _enrollment_token,
        base_candidate,
    ) = remote_fixture(tmp_path)
    approved_candidate = FederationDiscoveryCandidate(
        federation_label=base_candidate.federation_label,
        internal_session_id=base_candidate.internal_session_id,
        federation_fingerprint=base_candidate.federation_fingerprint,
        transport_kind=base_candidate.transport_kind,
        verification_code=base_candidate.verification_code,
        discovered_at=base_candidate.discovered_at,
        expires_at=base_candidate.expires_at,
        auto_accept_authorized=True,
        enrollment_token=base_candidate.enrollment_token,
        invitation_token=base_candidate.invitation_token,
    )
    local_policy_off = service_for(coordinator, approved_candidate)
    local_policy_off.discover()

    with pytest.raises(AuthenticationError):
        local_policy_off.connect(joining.identity, request_id="policy-off")

    local_policy_on = service_for(
        coordinator,
        approved_candidate,
        allow_authenticated_auto_accept=True,
    )
    local_policy_on.discover()
    binding = local_policy_on.connect(joining.identity, request_id="policy-on")

    assert binding.state is FederationConnectionState.CONNECTED
    assert binding.trusted is True


def test_prior_trust_allows_one_action_join_without_new_tokens(tmp_path: Path) -> None:
    coordinator = SessionCoordinator(tmp_path / "trusted.sqlite3", clock=lambda: NOW)
    owner = credentials(tmp_path, "Owner")
    trusted = credentials(tmp_path, "Trusted")
    for node in (owner, trusted):
        enroll(coordinator, node)
    create_session(coordinator, owner, session_id="session-trusted")
    invitation = coordinator.create_invitation(
        session_id="session-trusted",
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
    )
    coordinator.join_session(
        node_id=trusted.identity.node_id,
        token=invitation["token"],
        request_id="initial-trust",
        expected_session_id="session-trusted",
    )
    trusted_candidate = candidate(
        coordinator,
        owner,
        session_id="session-trusted",
        label="Trusted federation",
        fingerprint="fingerprint-trusted",
        code="TRUSTED",
        enrollment_token=None,
        prior_trust=True,
    )
    discovery = service_for(coordinator, trusted_candidate)

    results = discovery.discover()
    binding = discovery.connect(trusted.identity, request_id="trusted-reconnect")

    assert results[0].state is FederationConnectionState.DISCOVERED
    assert binding.state is FederationConnectionState.CONNECTED
    assert binding.trusted is True


def test_configured_source_enforces_its_candidate_bound(tmp_path: Path) -> None:
    coordinator = SessionCoordinator(tmp_path / "bound.sqlite3", clock=lambda: NOW)
    owner = credentials(tmp_path, "Owner")
    enroll(coordinator, owner)
    create_session(coordinator, owner, session_id="session-bound")
    one = FederationDiscoveryCandidate(
        federation_label="Bounded federation",
        internal_session_id="session-bound",
        federation_fingerprint="fingerprint-bound",
        transport_kind="configured-relay",
        verification_code="BOUND",
        discovered_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )

    with pytest.raises(FederationValidationError) as raised:
        ConfiguredFederationDiscoveryAdapter([one] * 33)

    assert raised.value.code == "too-many-discovery-candidates"

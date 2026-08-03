"""Executable CF7-A foundation scenarios.

These tests prove harness behavior and existing isolated contracts only. They do
not claim that the capability-first product flow is end-to-end accepted.
"""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
)
from catalog.federation.onboarding_discovery import (
    ConfiguredFederationDiscoveryAdapter,
    FederationOnboardingDiscoveryService,
    SessionOnboardingAuthority,
)
from catalog.federation.onboarding_models import FederationConnectionState
from catalog.node.identity import IdentityStore, NodeIdentityStateError

from .conftest import (
    AuthorityFixture,
    CandidateFixture,
    JoinedFixture,
    RevokedFixture,
)
from .harness import (
    DeviceState,
    assert_fenced,
    assert_member,
    assert_no_private_data,
    assert_not_member,
    assert_route_authorized,
    assert_same_identity,
    load_scenario_manifest,
    python_marker_process,
    reserved_tcp_port,
    wait_until,
    write_corrupted_bytes,
    write_partial_json,
)


@pytest.fixture
def partial_identity_state(tmp_path: Path) -> IdentityStore:
    device = DeviceState("Partial state device", tmp_path / "partial-device")
    store = device.identity_store()
    store.create(now=AuthorityFixture.clock())
    write_partial_json(store.public_identity_path)
    return store


@pytest.fixture
def corrupted_identity_state(tmp_path: Path) -> IdentityStore:
    device = DeviceState("Corrupted state device", tmp_path / "corrupt-device")
    store = device.identity_store()
    store.create(now=AuthorityFixture.clock())
    write_corrupted_bytes(store.private_key_path)
    return store


def test_device_state_directories_and_identities_are_isolated(
    authority_fixture: AuthorityFixture,
) -> None:
    roots = {device.root for device in authority_fixture.state.devices}
    identity_directories = {
        device.identity_directory for device in authority_fixture.state.devices
    }

    assert len(roots) == 3
    assert len(identity_directories) == 3
    assert all(path.is_dir() for path in identity_directories)
    assert len(
        {
            authority_fixture.owner.identity.node_id,
            authority_fixture.member.identity.node_id,
            authority_fixture.observer.identity.node_id,
        }
    ) == 3

    assert_same_identity(
        authority_fixture.owner,
        authority_fixture.owner_device.reopen_identity(),
    )
    assert_same_identity(
        authority_fixture.member,
        authority_fixture.member_device.reopen_identity(),
    )


def test_cross_platform_port_and_process_restart_helpers(tmp_path: Path) -> None:
    with reserved_tcp_port() as reserved:
        assert reserved.port > 0
        with (
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) as contender,
            pytest.raises(OSError),
        ):
            contender.bind((reserved.host, reserved.port))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as released:
        released.bind((reserved.host, reserved.port))

    marker = tmp_path / "child.pid"
    process = python_marker_process(marker).start()
    try:
        wait_until(marker.exists)
        first_pid = int(marker.read_text(encoding="utf-8"))
        assert first_pid == process.process.pid

        process = process.restart()
        wait_until(
            lambda: marker.exists()
            and int(marker.read_text(encoding="utf-8")) != first_pid
        )
        second_pid = int(marker.read_text(encoding="utf-8"))
        assert second_pid == process.process.pid
        assert second_pid != first_pid
    finally:
        process.stop()


def test_multiple_candidates_require_explicit_selection_and_hide_join_material(
    authority_fixture: AuthorityFixture,
    multiple_federation_candidates: tuple[CandidateFixture, CandidateFixture],
) -> None:
    candidates = tuple(item.candidate for item in multiple_federation_candidates)
    service = FederationOnboardingDiscoveryService(
        SessionOnboardingAuthority(
            authority_fixture.coordinator,
            clock=AuthorityFixture.clock,
        ),
        (ConfiguredFederationDiscoveryAdapter(candidates),),
        clock=AuthorityFixture.clock,
    )

    results = service.discover()
    assert len(results) == 2
    assert all(
        result.state is FederationConnectionState.VERIFICATION_REQUIRED
        for result in results
    )
    secrets = tuple(
        candidate.invitation_token
        for candidate in candidates
        if candidate.invitation_token is not None
    )
    assert_no_private_data(
        [result.to_dict() for result in results],
        secrets=secrets,
    )
    assert_no_private_data([repr(candidate) for candidate in candidates], secrets=secrets)

    with pytest.raises(FederationOperationError) as selection_required:
        service.connect(
            authority_fixture.member.identity,
            request_id="ambiguous-join-cf7a",
        )
    assert selection_required.value.code == "candidate-selection-required"
    for item in multiple_federation_candidates:
        assert_not_member(
            authority_fixture.coordinator,
            session_id=item.session_id,
            node_id=authority_fixture.member.identity.node_id,
        )

    selected_candidate = candidates[0]
    selected_result = next(
        result
        for result in results
        if result.discovery_id == selected_candidate.discovery_id
    )
    binding = service.connect(
        authority_fixture.member.identity,
        request_id="selected-join-cf7a",
        discovery_id=selected_result.discovery_id,
        verification_code=selected_result.verification_code,
    )
    assert_member(
        authority_fixture.coordinator,
        session_id=binding.internal_session_id,
        node_id=authority_fixture.member.identity.node_id,
    )
    assert_route_authorized(
        authority_fixture.coordinator,
        session_id=binding.internal_session_id,
        actor_node_id=authority_fixture.owner.identity.node_id,
        target_node_id=authority_fixture.member.identity.node_id,
    )
    assert_no_private_data(binding.to_dict(), secrets=secrets)


def test_joined_device_reopens_identity_and_reconnects_without_new_authority(
    reconnected_membership: JoinedFixture,
) -> None:
    joined = reconnected_membership
    assert_same_identity(joined.authority.member, joined.member)
    assert joined.binding.state is FederationConnectionState.CONNECTED
    assert joined.binding.trusted is True
    assert_member(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        node_id=joined.member.identity.node_id,
    )
    route = assert_route_authorized(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        actor_node_id=joined.authority.owner.identity.node_id,
        target_node_id=joined.member.identity.node_id,
    )
    assert_no_private_data(route)
    assert_no_private_data(
        joined.binding.to_dict(),
        secrets=(joined.candidate.invitation_token or "",),
    )


def test_removed_membership_is_fenced_and_saved_binding_cannot_reconnect(
    revoked_membership: RevokedFixture,
) -> None:
    joined = revoked_membership.joined
    assert revoked_membership.removed is True
    assert_not_member(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        node_id=joined.member.identity.node_id,
    )
    assert_fenced(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        actor_node_id=joined.authority.owner.identity.node_id,
        target_node_id=joined.member.identity.node_id,
    )

    authority = SessionOnboardingAuthority(
        joined.coordinator,
        clock=AuthorityFixture.clock,
    )
    with pytest.raises((AuthenticationError, AuthorizationError)):
        authority.reconnect(joined.member.identity, joined.binding)


def test_controlled_rejoin_restores_membership_without_granting_observer_authority(
    controlled_rejoin: JoinedFixture,
) -> None:
    joined = controlled_rejoin
    assert joined.binding.state is FederationConnectionState.CONNECTED
    assert_member(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        node_id=joined.member.identity.node_id,
    )
    assert_route_authorized(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        actor_node_id=joined.authority.owner.identity.node_id,
        target_node_id=joined.member.identity.node_id,
    )
    assert_not_member(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        node_id=joined.authority.observer.identity.node_id,
    )
    assert_fenced(
        joined.coordinator,
        session_id=joined.binding.internal_session_id,
        actor_node_id=joined.authority.owner.identity.node_id,
        target_node_id=joined.authority.observer.identity.node_id,
    )


def test_partial_and_corrupted_identity_state_fail_closed(
    partial_identity_state: IdentityStore,
    corrupted_identity_state: IdentityStore,
) -> None:
    with pytest.raises(NodeIdentityStateError) as partial:
        partial_identity_state.load()
    assert partial.value.code == "malformed-public-identity"

    with pytest.raises(NodeIdentityStateError) as corrupt:
        corrupted_identity_state.load()
    assert corrupt.value.code == "malformed-private-key"


def test_scenario_manifest_separates_executable_blocked_and_manual_status() -> None:
    manifest = load_scenario_manifest(Path(__file__).with_name("scenarios.json"))

    assert manifest["schema"] == "msh.cf7.acceptance-manifest.v1"
    assert manifest["acceptance_claim"] == "foundation-only"
    assert manifest["federation_v1_end_to_end_accepted"] is False
    assert manifest["capability_first_onboarding_end_to_end_accepted"] is False

    expected_status = {
        "executable_now": "executable",
        "blocked_by_cfi": "blocked",
        "manual_physical_verification": "manual",
    }
    scenario_ids: list[str] = []
    for section, status in expected_status.items():
        scenarios = manifest[section]
        assert scenarios
        assert {scenario["status"] for scenario in scenarios} == {status}
        scenario_ids.extend(scenario["id"] for scenario in scenarios)

    assert len(scenario_ids) == len(set(scenario_ids))
    assert any(
        scenario["blocked_by"].startswith("CFI-1")
        for scenario in manifest["blocked_by_cfi"]
    )
    assert any(
        scenario["blocked_by"].startswith("CFI-3")
        for scenario in manifest["blocked_by_cfi"]
    )
    assert "accepted" not in {
        scenario["status"]
        for section in expected_status
        for scenario in manifest[section]
    }

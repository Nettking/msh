from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.storage_authority_enrollment import STORAGE_AUTHORITY_SCHEMA
from catalog.capabilities.storage_authority_lease import (
    RenewingTrustedGreenStorageAuthority,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_protocol import STORAGE_PROTOCOL, STORAGE_PROTOCOL_VERSION
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
SESSION_ID = "session-storage-lease"
GROUP_ID = "fcp-local-storage"
PROVIDER_ID = "storage-provider-primary"


def _credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name}",
        display_name=name,
    ).create(now=NOW)


def _enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(node.identity, token=token)


def _fixture(tmp_path: Path):
    clock = [NOW]
    database = tmp_path / "coordinator.sqlite3"
    coordinator = SessionCoordinator(database, clock=lambda: clock[0])
    owner = _credentials(tmp_path, "owner")
    provider = _credentials(tmp_path, "provider")
    _enroll(coordinator, owner)
    _enroll(coordinator, provider)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Storage lease renewal",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-provider",
        expected_session_id=SESSION_ID,
    )
    coordinator.connected(
        node_id=owner.identity.node_id,
        connection_id="owner-live",
    )
    coordinator.connected(
        node_id=provider.identity.node_id,
        connection_id="provider-live",
    )
    control = PhaseDControlPlane(database)
    policy = RenewingTrustedGreenStorageAuthority(
        coordinator,
        control,
        clock=lambda: clock[0],
        lease_seconds=300,
        renew_before_seconds=60,
    )
    announcement = CapabilityAnnouncement(
        capability_id="storage-capability",
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        type="storage",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        status=CapabilityStatus.READY,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": "fcp-local-data-storage",
            "storage_authority": {
                "schema": STORAGE_AUTHORITY_SCHEMA,
                "eligible": True,
                "benchmark_state": "green",
                "provider_id": PROVIDER_ID,
                "group_id": GROUP_ID,
                "role": "primary",
                "inspection_revision": 1,
                "decision_revision": 1,
                "benchmark_run_ids": ["storage-green-1"],
                "desired_state": "enabled",
                "policy_state": "allowed",
            },
        },
        announced_at=NOW,
    )
    coordinator.announce_capability(
        announcement,
        actor_node_id=provider.identity.node_id,
        request_id="announce-storage",
    )
    return clock, coordinator, provider, control, policy, announcement


def test_current_green_primary_lease_renews_before_expiry(tmp_path: Path) -> None:
    clock, _coordinator, provider, control, policy, announcement = _fixture(tmp_path)

    initial = policy.reconcile(announcement)
    assert initial.active is True
    first = control.snapshot(SESSION_ID).leader_grants[GROUP_ID]

    # With more than the 60-second renewal window left, repeated capability
    # traffic is a true no-op and does not churn terms or fencing tokens.
    clock[0] = NOW + timedelta(seconds=200)
    settled = policy.reconcile_current_node(
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert len(settled) == 1
    assert settled[0].changed is False
    assert control.snapshot(SESSION_ID).leader_grants[GROUP_ID] == first

    # The supervised logical-storage authority announces every scan. Once that
    # accepted traffic arrives inside the renewal window, the relay-side policy
    # revalidates the persisted GREEN storage capability and rotates authority.
    clock[0] = NOW + timedelta(seconds=241)
    renewed = policy.reconcile_current_node(
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )
    assert len(renewed) == 1
    assert renewed[0].active is True
    assert renewed[0].changed is True

    second = control.snapshot(SESSION_ID).leader_grants[GROUP_ID]
    assert second["provider_id"] == PROVIDER_ID
    assert int(second["term"]) > int(first["term"])
    assert int(second["fencing_token"]) > int(first["fencing_token"])
    assert datetime.fromisoformat(str(second["lease_expires_at"]).replace("Z", "+00:00")) > datetime.fromisoformat(
        str(first["lease_expires_at"]).replace("Z", "+00:00")
    )


def test_expired_same_primary_lease_is_recovered_without_reassignment(
    tmp_path: Path,
) -> None:
    clock, _coordinator, provider, control, policy, announcement = _fixture(tmp_path)
    policy.reconcile(announcement)
    first = control.snapshot(SESSION_ID).leader_grants[GROUP_ID]

    clock[0] = NOW + timedelta(seconds=301)
    decisions = policy.reconcile_current_node(
        session_id=SESSION_ID,
        node_id=provider.identity.node_id,
    )

    assert len(decisions) == 1
    assert decisions[0].active is True
    assert decisions[0].role == "primary"
    assert decisions[0].changed is True
    snapshot = control.snapshot(SESSION_ID)
    assert snapshot.groups[GROUP_ID].primary_provider_id == PROVIDER_ID
    second = snapshot.leader_grants[GROUP_ID]
    assert int(second["term"]) > int(first["term"])
    assert int(second["fencing_token"]) > int(first["fencing_token"])

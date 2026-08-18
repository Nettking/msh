from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from catalog.capabilities.storage_authority_enrollment import (
    STORAGE_AUTHORITY_SCHEMA,
    TrustedGreenStorageAuthority,
    parse_storage_authority_evidence,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 17, 1, 0, tzinfo=timezone.utc)
SESSION_ID = "session-storage"
GROUP_ID = "group-recordings"


def _credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold()}",
        display_name=name,
    ).create(now=NOW)


def _enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(node.identity, token=token)


class _Fixture:
    def __init__(self, tmp_path: Path) -> None:
        self.clock = [NOW]
        self.coordinator = SessionCoordinator(
            tmp_path / "coordinator.sqlite3",
            clock=lambda: self.clock[0],
        )
        self.owner = _credentials(tmp_path, "owner")
        self.first = _credentials(tmp_path, "first")
        self.second = _credentials(tmp_path, "second")
        for node in (self.owner, self.first, self.second):
            _enroll(self.coordinator, node)
        self.coordinator.create_session(
            actor_node_id=self.owner.identity.node_id,
            display_name="Storage authority",
            request_id="create-session",
            session_id=SESSION_ID,
        )
        for index, node in enumerate((self.first, self.second)):
            invitation = self.coordinator.create_invitation(
                session_id=SESSION_ID,
                actor_node_id=self.owner.identity.node_id,
                ttl_seconds=60,
                max_uses=1,
                request_id=f"invite-{index}",
            )
            self.coordinator.join_session(
                node_id=node.identity.node_id,
                token=invitation["token"],
                request_id=f"join-{index}",
                expected_session_id=SESSION_ID,
            )
        for name, node in (
            ("owner", self.owner),
            ("first", self.first),
            ("second", self.second),
        ):
            self.coordinator.connected(
                node_id=node.identity.node_id,
                connection_id=f"{name}-live",
            )
        self.control_plane = PhaseDControlPlane(tmp_path / "coordinator.sqlite3")
        self.policy = TrustedGreenStorageAuthority(
            self.coordinator,
            self.control_plane,
            clock=lambda: self.clock[0],
        )

    def snapshot(self):
        return self.control_plane.snapshot(SESSION_ID)

    def assignment(self):
        return self.snapshot().groups[GROUP_ID]

    def grant(self):
        return self.snapshot().leader_grants.get(GROUP_ID)


def _attestation(
    *,
    provider_id: str,
    eligible: bool = True,
    benchmark_state: str = "green",
    role: str = "primary",
    benchmark_run_ids: list[str] | None = None,
    inspection_revision: int = 3,
    decision_revision: int = 4,
) -> dict[str, object]:
    return {
        "schema": STORAGE_AUTHORITY_SCHEMA,
        "eligible": eligible,
        "benchmark_state": benchmark_state,
        "provider_id": provider_id,
        "group_id": GROUP_ID,
        "role": role,
        "inspection_revision": inspection_revision,
        "decision_revision": decision_revision,
        "benchmark_run_ids": (
            ["benchmark-storage-green-1"]
            if benchmark_run_ids is None
            else benchmark_run_ids
        ),
        "desired_state": "enabled",
        "policy_state": "allowed",
    }


def _announcement(
    node: NodeCredentials,
    *,
    provider_id: str,
    capability_id: str | None = None,
    status: CapabilityStatus = CapabilityStatus.READY,
    attestation: dict[str, object] | None = None,
    announced_at: datetime = NOW,
    capability_type: str = "storage",
    protocol_version: str = STORAGE_PROTOCOL_VERSION,
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id=capability_id or provider_id,
        node_id=node.identity.node_id,
        session_id=SESSION_ID,
        type=capability_type,
        protocol=STORAGE_PROTOCOL,
        protocol_version=protocol_version,
        status=status,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": provider_id,
            "storage_authority": (
                _attestation(provider_id=provider_id)
                if attestation is None
                else attestation
            ),
        },
        announced_at=announced_at,
    )


def _publish(
    fixture: _Fixture,
    announcement: CapabilityAnnouncement,
    request_id: str,
) -> CapabilityAnnouncement:
    fixture.coordinator.announce_capability(
        announcement,
        actor_node_id=announcement.node_id,
        request_id=request_id,
    )
    return announcement


# --- A: trusted + current valid capability + GREEN -> automatic authority ----


def test_trusted_green_storage_activates_without_manual_approval(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )

    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "active"
    assert decision.reason_code == "trusted-green-storage-authority"
    assert decision.role == "primary"
    assert decision.changed is True
    assignment = fixture.assignment()
    assert assignment.primary_provider_id == "provider-first"
    grant = fixture.grant()
    assert grant is not None
    assert grant["provider_id"] == "provider-first"
    assert grant["node_id"] == fixture.first.identity.node_id
    assert int(grant["term"]) >= 1
    assert int(grant["fencing_token"]) >= 1


# --- B: untrusted + GREEN -> never eligible ---------------------------------


def test_untrusted_node_never_becomes_storage_authority(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    fixture.coordinator.remove_member(
        session_id=SESSION_ID,
        actor_node_id=fixture.owner.identity.node_id,
        target_node_id=fixture.first.identity.node_id,
        request_id="remove-first",
        reason="trust-revoked-in-test",
    )

    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "inactive"
    assert decision.reason_code == "federation-membership-revoked"
    assert GROUP_ID not in fixture.snapshot().groups


# --- C/D/E: missing, failed, and stale benchmark evidence -------------------


def test_missing_benchmark_evidence_is_not_eligible(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    attestation = _attestation(provider_id="provider-first")
    attestation.pop("benchmark_run_ids")
    announcement = _announcement(
        fixture.first,
        provider_id="provider-first",
        attestation=attestation,
    )

    assert parse_storage_authority_evidence(announcement) is None

    _publish(fixture, announcement, "announce-first")
    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "inactive"
    assert decision.reason_code == "storage-evidence-not-current-green"
    assert GROUP_ID not in fixture.snapshot().groups


def test_failed_benchmark_evidence_is_not_eligible(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _announcement(
        fixture.first,
        provider_id="provider-first",
        attestation=_attestation(
            provider_id="provider-first",
            benchmark_state="failed",
            eligible=False,
        ),
    )

    assert parse_storage_authority_evidence(announcement) is None

    _publish(fixture, announcement, "announce-first")
    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "inactive"
    assert GROUP_ID not in fixture.snapshot().groups


def test_capability_that_is_not_ready_is_not_eligible(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _announcement(
        fixture.first,
        provider_id="provider-first",
        status=CapabilityStatus.REGISTERING,
    )

    assert parse_storage_authority_evidence(announcement) is None

    _publish(fixture, announcement, "announce-first")
    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "inactive"
    assert GROUP_ID not in fixture.snapshot().groups


def test_incompatible_protocol_major_is_not_eligible(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _announcement(
        fixture.first,
        provider_id="provider-first",
        protocol_version="2.0",
    )

    assert parse_storage_authority_evidence(announcement) is None


# --- F/G/H: active provider loses eligibility -------------------------------


def test_unhealthy_capability_deactivates_active_authority(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    assert fixture.policy.reconcile(announcement).outcome == "active"

    unhealthy = _publish(
        fixture,
        _announcement(
            fixture.first,
            provider_id="provider-first",
            status=CapabilityStatus.UNAVAILABLE,
        ),
        "announce-first-unhealthy",
    )
    decision = fixture.policy.reconcile(unhealthy)

    assert decision.outcome == "inactive"
    assert fixture.assignment().primary_provider_id is None
    assert fixture.grant() is None
    assert fixture.snapshot().providers["provider-first"].assignable is False


def test_trust_revocation_deactivates_active_authority(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    assert fixture.policy.reconcile(announcement).outcome == "active"

    fixture.coordinator.remove_member(
        session_id=SESSION_ID,
        actor_node_id=fixture.owner.identity.node_id,
        target_node_id=fixture.first.identity.node_id,
        request_id="remove-first",
        reason="trust-revoked-in-test",
    )
    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "inactive"
    assert decision.reason_code == "federation-membership-revoked"
    assert fixture.assignment().primary_provider_id is None
    assert fixture.grant() is None


# --- I/J: revision fencing ---------------------------------------------------


def test_new_unvalidated_revision_cannot_ride_on_old_green_evidence(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    green = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-green",
    )
    assert fixture.policy.reconcile(green).outcome == "active"

    ungreen = _publish(
        fixture,
        _announcement(
            fixture.first,
            provider_id="provider-first",
            attestation=_attestation(
                provider_id="provider-first",
                inspection_revision=4,
                decision_revision=5,
                benchmark_state="pending",
                eligible=False,
            ),
        ),
        "announce-new-revision",
    )
    decision = fixture.policy.reconcile(ungreen)

    assert decision.outcome == "inactive"
    assert fixture.assignment().primary_provider_id is None


def test_late_stale_revision_cannot_reactivate_newer_state(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    green = _announcement(fixture.first, provider_id="provider-first")
    _publish(fixture, green, "announce-green")
    assert fixture.policy.reconcile(green).outcome == "active"

    superseding = _publish(
        fixture,
        _announcement(
            fixture.first,
            provider_id="provider-first",
            attestation=_attestation(
                provider_id="provider-first",
                inspection_revision=4,
                decision_revision=5,
                benchmark_state="pending",
                eligible=False,
            ),
        ),
        "announce-new-revision",
    )
    fixture.policy.reconcile(superseding)
    assert fixture.assignment().primary_provider_id is None

    # The superseded GREEN announcement arrives late and must change nothing.
    decision = fixture.policy.reconcile(green)

    assert decision.outcome == "ignored"
    assert decision.reason_code == "superseded-announcement"
    assert fixture.assignment().primary_provider_id is None
    assert fixture.grant() is None


# --- K: idempotency ----------------------------------------------------------


def test_repeated_identical_reconciliation_is_a_no_op(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )

    first = fixture.policy.reconcile(announcement)
    assert first.changed is True
    settled = fixture.snapshot()

    for _ in range(3):
        repeat = fixture.policy.reconcile(announcement)
        assert repeat.outcome == "active"
        assert repeat.changed is False

    latest = fixture.snapshot()
    assert latest.revision == settled.revision
    assert latest.groups == settled.groups
    assert latest.leader_grants == settled.leader_grants


# --- L: deterministic failover ----------------------------------------------


def test_second_trusted_provider_joins_as_replica_and_takes_over(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    first = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    second = _publish(
        fixture,
        _announcement(fixture.second, provider_id="provider-second"),
        "announce-second",
    )

    assert fixture.policy.reconcile(first).role == "primary"
    second_decision = fixture.policy.reconcile(second)

    # A healthy primary is never displaced by a newly eligible provider.
    assert second_decision.outcome == "active"
    assert second_decision.role == "replica"
    assignment = fixture.assignment()
    assert assignment.primary_provider_id == "provider-first"
    assert assignment.replica_provider_ids == ("provider-second",)
    first_grant = fixture.grant()
    assert first_grant is not None

    # The active authority fails; the eligible replica must take over.
    unhealthy = _publish(
        fixture,
        _announcement(
            fixture.first,
            provider_id="provider-first",
            status=CapabilityStatus.UNAVAILABLE,
        ),
        "announce-first-unhealthy",
    )
    failover = fixture.policy.reconcile(unhealthy)

    assert failover.outcome == "inactive"
    assignment = fixture.assignment()
    assert assignment.primary_provider_id == "provider-second"
    assert assignment.replica_provider_ids == ()

    # The new primary is granted leadership with strictly newer fencing state.
    promoted = fixture.policy.reconcile(second)
    assert promoted.role == "primary"
    new_grant = fixture.grant()
    assert new_grant is not None
    assert new_grant["provider_id"] == "provider-second"
    assert int(new_grant["term"]) > int(first_grant["term"])
    assert int(new_grant["fencing_token"]) > int(first_grant["fencing_token"])


def test_stale_authority_cannot_overwrite_the_new_authority(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    first = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    second = _publish(
        fixture,
        _announcement(fixture.second, provider_id="provider-second"),
        "announce-second",
    )
    fixture.policy.reconcile(first)
    fixture.policy.reconcile(second)
    _publish(
        fixture,
        _announcement(
            fixture.first,
            provider_id="provider-first",
            status=CapabilityStatus.UNAVAILABLE,
        ),
        "announce-first-unhealthy",
    )
    fixture.policy.reconcile(
        _announcement(
            fixture.first,
            provider_id="provider-first",
            status=CapabilityStatus.UNAVAILABLE,
        )
    )
    fixture.policy.reconcile(second)
    assert fixture.assignment().primary_provider_id == "provider-second"
    grant = fixture.grant()
    assert grant is not None

    # The deposed authority replays its old GREEN announcement.
    replay = fixture.policy.reconcile(first)

    assert replay.outcome == "ignored"
    assert replay.reason_code == "superseded-announcement"
    assert fixture.assignment().primary_provider_id == "provider-second"
    assert fixture.grant() == grant


def test_provider_id_cannot_be_hijacked_by_another_device(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    first = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-shared"),
        "announce-first",
    )
    assert fixture.policy.reconcile(first).outcome == "active"

    impostor = _publish(
        fixture,
        _announcement(
            fixture.second,
            provider_id="provider-shared",
            capability_id="provider-shared-second",
        ),
        "announce-second",
    )
    decision = fixture.policy.reconcile(impostor)

    assert decision.outcome == "ignored"
    assert decision.reason_code == "storage-provider-owned-by-another-device"
    assert (
        fixture.snapshot().providers["provider-shared"].node_id
        == fixture.first.identity.node_id
    )


def test_no_connected_leader_leaves_control_plane_untouched(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    announcement = _publish(
        fixture,
        _announcement(fixture.first, provider_id="provider-first"),
        "announce-first",
    )
    fixture.coordinator.disconnected(node_id=fixture.owner.identity.node_id)

    decision = fixture.policy.reconcile(announcement)

    assert decision.outcome == "ignored"
    assert decision.reason_code == "no-connected-session-leader"
    assert fixture.snapshot().revision == 0

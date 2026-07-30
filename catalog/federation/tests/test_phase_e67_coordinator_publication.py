from __future__ import annotations

from pathlib import Path

from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_control_plane import (
    STORAGE_ASSIGNMENT_CHANGED,
    STORAGE_LEADER_GRANTED,
    STORAGE_LEADER_REVOKED,
)

from .test_phase_e63_provider_fencing import NOW
from .test_phase_e66_recovery import _base, _record_grant, _recover


def _event_count(
    control: PhaseDControlPlane,
    event_type: str,
) -> int:
    return sum(
        event.event_type == event_type
        for event in control.store.events("session-1")
    )


def test_e67_completed_recovery_publishes_coordinator_authority(
    tmp_path: Path,
) -> None:
    control, old_store, new_store, _ = _base(tmp_path)

    result = _recover(control, old_store, new_store)

    assert result.status == "completed"
    assert "published-coordinator-authority" in result.actions
    snapshot = control.snapshot("session-1")
    assignment = snapshot.groups["storage-main"]
    grant = snapshot.leader_grants["storage-main"]
    transaction = control.promotion_transaction(
        "session-1",
        "storage-main",
        "promotion-1",
    )
    assert assignment.primary_provider_id == "provider-new"
    assert assignment.replica_provider_ids == ("provider-old",)
    assert grant["provider_id"] == "provider-new"
    assert grant["grant_id"] == transaction.grant_id
    assert grant["term"] == transaction.reserved_term == 6
    assert grant["fencing_token"] == 10
    assert control.storage_degraded_state("session-1", "storage-main") is None
    assert _event_count(control, STORAGE_LEADER_REVOKED) == 1
    assert _event_count(control, STORAGE_ASSIGNMENT_CHANGED) == 2
    assert _event_count(control, STORAGE_LEADER_GRANTED) == 2


def test_e67_duplicate_and_restart_do_not_republish_control_events(
    tmp_path: Path,
) -> None:
    control, old_store, new_store, coordinator_path = _base(tmp_path)
    first = _recover(control, old_store, new_store)
    first_snapshot = control.snapshot("session-1")
    first_events = control.store.events("session-1")

    restarted = PhaseDControlPlane(coordinator_path)
    second = _recover(restarted, old_store, new_store)
    second_snapshot = restarted.snapshot("session-1")
    second_events = restarted.store.events("session-1")

    assert first.status == second.status == "completed"
    assert "published-coordinator-authority" in first.actions
    assert "published-coordinator-authority" not in second.actions
    assert second_snapshot.revision == first_snapshot.revision
    assert second_snapshot.groups == first_snapshot.groups
    assert second_snapshot.leader_grants == first_snapshot.leader_grants
    assert second_events == first_events


def test_e67_restart_after_finalization_publishes_before_degraded_clear(
    tmp_path: Path,
) -> None:
    control, old_store, new_store, coordinator_path = _base(tmp_path)
    _record_grant(control, old_store, new_store)
    control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )

    before = control.snapshot("session-1")
    assert before.groups["storage-main"].primary_provider_id == "provider-old"
    assert control.storage_degraded_state("session-1", "storage-main") is not None

    result = _recover(
        PhaseDControlPlane(coordinator_path),
        old_store,
        new_store,
    )

    assert result.status == "completed"
    assert result.actions == (
        "published-coordinator-authority",
        "cleared-matching-degraded-state",
    )
    after = PhaseDControlPlane(coordinator_path).snapshot("session-1")
    assert after.groups["storage-main"].primary_provider_id == "provider-new"
    assert after.leader_grants["storage-main"]["term"] == 6


def test_e67_recovery_repairs_legacy_clear_without_publication(
    tmp_path: Path,
) -> None:
    control, old_store, new_store, coordinator_path = _base(tmp_path)
    _record_grant(control, old_store, new_store)
    completed = control.complete_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )
    assert completed.degraded_cleared
    assert (
        control.snapshot("session-1")
        .groups["storage-main"]
        .primary_provider_id
        == "provider-old"
    )

    result = _recover(
        PhaseDControlPlane(coordinator_path),
        old_store,
        new_store,
    )

    assert result.status == "completed"
    assert result.actions == ("published-coordinator-authority",)
    snapshot = PhaseDControlPlane(coordinator_path).snapshot("session-1")
    assert snapshot.groups["storage-main"].primary_provider_id == "provider-new"
    assert snapshot.leader_grants["storage-main"]["fencing_token"] == 10


def test_e67_mismatched_control_state_requires_operator_attention(
    tmp_path: Path,
) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    _record_grant(control, old_store, new_store)
    control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )
    control.change_assignment(
        "session-1",
        "coordinator",
        "storage-main",
        None,
        ("provider-old", "provider-new"),
    )

    result = _recover(control, old_store, new_store)

    assert result.status == "operator-attention"
    assert result.code == "promotion-control-state-mismatch"
    assert control.storage_degraded_state("session-1", "storage-main") is not None
    snapshot = control.snapshot("session-1")
    assert snapshot.groups["storage-main"].primary_provider_id is None
    assert "storage-main" not in snapshot.leader_grants

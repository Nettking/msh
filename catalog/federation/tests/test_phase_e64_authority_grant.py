from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.provider_fencing import (
    PROVIDER_GRANT_SCHEMA,
    ProviderFenceStore,
    StorageAuthorityGrantAcknowledgement,
)

from .test_phase_e6_promotion_transaction import _selection
from .test_phase_e63_provider_fencing import NOW, _store, _validated


def _fenced(
    tmp_path: Path,
) -> tuple[PhaseDControlPlane, ProviderFenceStore, Path, Path]:
    coordinator_path = tmp_path / "coordinator.sqlite3"
    old_provider_path = tmp_path / "provider-old.sqlite3"
    new_provider_path = tmp_path / "provider-new.sqlite3"
    control = _validated(coordinator_path)
    old_store = _store(old_provider_path)
    acknowledgement = old_store.apply(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-1"),
        now=NOW,
    )
    control.acknowledge_promotion_fencing(
        acknowledgement,
        actor_node_id="node-old",
        provider_fence_store=old_store,
    )
    return control, old_store, coordinator_path, new_provider_path


def _new_store(path: Path) -> ProviderFenceStore:
    return ProviderFenceStore(path, session_id="session-1", provider_id="provider-new")


def _reserved(
    tmp_path: Path,
) -> tuple[PhaseDControlPlane, ProviderFenceStore, ProviderFenceStore, Path]:
    control, old_store, coordinator_path, new_provider_path = _fenced(tmp_path)
    record = control.reserve_promotion_term(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        now=NOW,
    )
    assert record.state == "allocated-term"
    return control, old_store, _new_store(new_provider_path), coordinator_path


def _apply_grant(
    control: PhaseDControlPlane,
    old_store: ProviderFenceStore,
    new_store: ProviderFenceStore,
):
    command = control.promotion_grant_command(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
    )
    acknowledgement = new_store.apply_grant(
        command,
        expected_promotion_id="promotion-1",
        expected_group_id="storage-main",
        expected_manifest_revision=command.manifest_revision,
        expected_manifest_hash=command.manifest_hash,
        expected_report_revision=command.report_revision,
        expected_report_hash=command.report_hash,
        now=NOW,
    )
    return command, acknowledgement


def test_e64_reservation_is_strictly_above_previous_fence_and_observed_terms(tmp_path: Path) -> None:
    control, old_store, _, _ = _fenced(tmp_path)
    with control._connect() as connection:
        connection.execute(
            """UPDATE storage_fencing_counters SET last_term=12
               WHERE session_id='session-1' AND group_id='storage-main'"""
        )
    record = control.reserve_promotion_term(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        now=NOW,
    )
    assert record.previous_term == 5
    assert old_store.high_water_term("storage-main") == 5
    assert record.reserved_term == 13


def test_e64_duplicate_reservation_and_restart_reuse_exact_term(tmp_path: Path) -> None:
    control, old_store, coordinator_path, _ = _fenced(tmp_path)
    first = control.reserve_promotion_term(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store, now=NOW
    )
    second = control.reserve_promotion_term(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store, now=NOW
    )
    restarted = PhaseDControlPlane(coordinator_path).reserve_promotion_term(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=_store(tmp_path / "provider-old.sqlite3"),
        now=NOW + timedelta(seconds=5),
    )
    assert first == second == restarted
    assert first.reserved_term == 6


@pytest.mark.parametrize("corruption", ("missing", "forged"))
def test_e64_reservation_revalidates_exact_durable_fence(tmp_path: Path, corruption: str) -> None:
    control, old_store, _, _ = _fenced(tmp_path)
    with old_store._connect() as connection:
        if corruption == "missing":
            connection.execute("DELETE FROM provider_storage_fences")
        else:
            connection.execute("UPDATE provider_storage_fences SET acknowledgement_id='forged'")
    with pytest.raises(FederationValidationError):
        control.reserve_promotion_term(
            "session-1",
            "storage-main",
            "promotion-1",
            provider_fence_store=old_store,
            now=NOW,
        )
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").reserved_term is None


def test_e64_terms_are_not_reused_across_promotions(tmp_path: Path) -> None:
    control, old_store, _, _ = _fenced(tmp_path)
    first = control.reserve_promotion_term(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store, now=NOW
    )
    control.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-old",
        "observed-grant",
        7,
        10,
        occurred_at=NOW + timedelta(seconds=1),
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    record = control.validate_promotion_transaction_inputs(
        "session-1",
        "storage-main",
        _selection("provider-new"),
        promotion_id="promotion-2",
        reported_at=NOW + timedelta(seconds=2),
    )
    control.create_promotion_transaction(record)
    fence = old_store.apply(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-2"),
        now=NOW + timedelta(seconds=2),
    )
    control.acknowledge_promotion_fencing(
        fence,
        actor_node_id="node-old",
        provider_fence_store=old_store,
    )
    second = control.reserve_promotion_term(
        "session-1",
        "storage-main",
        "promotion-2",
        provider_fence_store=old_store,
        now=NOW + timedelta(seconds=3),
    )
    assert first.reserved_term == 6
    assert second.reserved_term == 8


def test_e64_provider_grant_is_durable_duplicate_and_lost_response_safe(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    command, lost = _apply_grant(control, old_store, new_store)
    retried = _new_store(tmp_path / "provider-new.sqlite3").apply_grant(
        command,
        expected_promotion_id="promotion-1",
        expected_group_id="storage-main",
        expected_manifest_revision=command.manifest_revision,
        expected_manifest_hash=command.manifest_hash,
        expected_report_revision=command.report_revision,
        expected_report_hash=command.report_hash,
        now=NOW + timedelta(seconds=5),
    )
    assert retried == lost
    assert retried.schema == PROVIDER_GRANT_SCHEMA
    assert new_store.has_valid_grant("storage-main", "promotion-1", command.term)


def test_e64_restart_before_delivery_and_after_provider_persistence(tmp_path: Path) -> None:
    _control, old_store, new_store, coordinator_path = _reserved(tmp_path)
    restarted = PhaseDControlPlane(coordinator_path)
    command = restarted.promotion_grant_command(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
    )
    acknowledgement = new_store.apply_grant(
        command,
        expected_promotion_id="promotion-1",
        expected_group_id="storage-main",
        expected_manifest_revision=command.manifest_revision,
        expected_manifest_hash=command.manifest_hash,
        expected_report_revision=command.report_revision,
        expected_report_hash=command.report_hash,
        now=NOW,
    )
    assert restarted.promotion_transaction("session-1", "storage-main", "promotion-1").state == "allocated-term"
    granted = PhaseDControlPlane(coordinator_path).acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )
    assert granted.state == "granted-new-authority"


def test_e64_restart_after_coordinator_records_grant_is_idempotent(tmp_path: Path) -> None:
    control, old_store, new_store, coordinator_path = _reserved(tmp_path)
    _, acknowledgement = _apply_grant(control, old_store, new_store)
    first = control.acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )
    second = PhaseDControlPlane(coordinator_path).acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )
    assert first == second
    assert second.state == "granted-new-authority"


def test_e64_provider_rejects_fenced_stale_and_conflicting_grants(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    command, _ = _apply_grant(control, old_store, new_store)
    old_store.validate_grant_term("storage-main", command.term)
    fenced_selected_store = ProviderFenceStore(
        tmp_path / "provider-selected-fenced.sqlite3",
        session_id="session-1",
        provider_id="provider-new",
    )
    fence_command = replace(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-1"),
        provider_id="provider-new",
        fencing_high_water_term=command.term,
    )
    fenced_selected_store.apply(fence_command, now=NOW)
    with pytest.raises(FederationValidationError) as stale:
        fenced_selected_store.apply_grant(
            command,
            expected_promotion_id="promotion-1",
            expected_group_id="storage-main",
            expected_manifest_revision=command.manifest_revision,
            expected_manifest_hash=command.manifest_hash,
            expected_report_revision=command.report_revision,
            expected_report_hash=command.report_hash,
            now=NOW,
        )
    assert stale.value.code == "stale-term"
    with pytest.raises(FederationValidationError) as conflict:
        new_store.apply_grant(
            replace(command, term=command.term + 1),
            expected_promotion_id="promotion-1",
            expected_group_id="storage-main",
            expected_manifest_revision=command.manifest_revision,
            expected_manifest_hash=command.manifest_hash,
            expected_report_revision=command.report_revision,
            expected_report_hash=command.report_hash,
            now=NOW,
        )
    assert conflict.value.code == "conflicting-authority-grant"
    with pytest.raises(FederationValidationError) as older:
        new_store.apply_grant(
            replace(
                command,
                promotion_id="promotion-other",
                term=command.term - 1,
                grant_id="grant-other",
                idempotency_key="idempotency-other",
            ),
            expected_promotion_id="promotion-other",
            expected_group_id="storage-main",
            expected_manifest_revision=command.manifest_revision,
            expected_manifest_hash=command.manifest_hash,
            expected_report_revision=command.report_revision,
            expected_report_hash=command.report_hash,
            now=NOW,
        )
    assert older.value.code == "stale-term"


@pytest.mark.parametrize(
    "changes",
    (
        {"promotion_id": "promotion-other"},
        {"provider_id": "provider-other"},
        {"group_id": "storage-other"},
        {"session_id": "session-other"},
        {"report_hash": "sha256:" + "f" * 64},
        {"manifest_hash": "sha256:" + "e" * 64},
    ),
)
def test_e64_provider_rejects_mismatched_grant_binding(tmp_path: Path, changes: dict[str, object]) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    command = control.promotion_grant_command(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store
    )
    invalid = replace(command, **changes)
    with pytest.raises(FederationValidationError):
        new_store.apply_grant(
            invalid,
            expected_promotion_id="promotion-1",
            expected_group_id="storage-main",
            expected_manifest_revision=command.manifest_revision,
            expected_manifest_hash=command.manifest_hash,
            expected_report_revision=command.report_revision,
            expected_report_hash=command.report_hash,
            now=NOW,
        )


def test_e64_coordinator_only_false_success_cannot_advance(tmp_path: Path) -> None:
    control, old_store, _, _ = _reserved(tmp_path)
    command = control.promotion_grant_command(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store
    )
    fabricated = StorageAuthorityGrantAcknowledgement(
        schema=PROVIDER_GRANT_SCHEMA,
        acknowledgement_id=ProviderFenceStore.grant_acknowledgement_identity(command),
        persisted_at=NOW,
        **command.binding(),
    )
    with pytest.raises(FederationValidationError) as error:
        control.acknowledge_promotion_grant(
            fabricated,
            actor_node_id="node-new",
            provider_fence_store=old_store,
            selected_provider_store=_new_store(tmp_path / "empty-new.sqlite3"),
        )
    assert error.value.code == "missing-durable-grant-evidence"


@pytest.mark.parametrize(
    "changes",
    (
        {"promotion_id": "promotion-other"},
        {"provider_id": "provider-other"},
        {"group_id": "storage-other"},
        {"session_id": "session-other"},
        {"report_hash": "sha256:" + "f" * 64},
        {"manifest_hash": "sha256:" + "e" * 64},
    ),
)
def test_e64_coordinator_rejects_mismatched_grant_ack(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    _, acknowledgement = _apply_grant(control, old_store, new_store)
    with pytest.raises(FederationValidationError):
        control.acknowledge_promotion_grant(
            replace(acknowledgement, **changes),
            actor_node_id="node-new",
            provider_fence_store=old_store,
            selected_provider_store=new_store,
        )
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "allocated-term"


def test_e64_coordinator_rejects_unauthenticated_grant_ack(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    _, acknowledgement = _apply_grant(control, old_store, new_store)
    with pytest.raises(FederationValidationError) as error:
        control.acknowledge_promotion_grant(
            acknowledgement,
            actor_node_id="node-old",
            provider_fence_store=old_store,
            selected_provider_store=new_store,
        )
    assert error.value.code == "unauthenticated-grant-ack"


def test_e64_failed_delivery_preserves_reservation_and_degraded_state(tmp_path: Path) -> None:
    control, old_store, _, _ = _reserved(tmp_path)
    manifest = control.manifest("session-1", "storage-main")
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="promotion-in-progress",
        reason_detail="new grant delivery pending",
        missing_ranges=(),
        obligations={"repair": ["provider-old"]},
        now=NOW,
    )
    failed = control.record_promotion_grant_delivery_failure(
        "session-1", "storage-main", "promotion-1", reason="unavailable", now=NOW
    )
    retried = control.reserve_promotion_term(
        "session-1", "storage-main", "promotion-1", provider_fence_store=old_store, now=NOW
    )
    assert failed.reserved_term == retried.reserved_term
    assert failed.state == "allocated-term"
    assert control.storage_degraded_state("session-1", "storage-main") is not None


def test_e64_corrupt_reservation_and_provider_grant_fail_closed(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _reserved(tmp_path)
    with control._connect() as connection:
        connection.execute(
            """UPDATE storage_promotion_transactions SET reserved_term=1
               WHERE promotion_id='promotion-1'"""
        )
    with pytest.raises(FederationValidationError) as reservation:
        control.reserve_promotion_term(
            "session-1", "storage-main", "promotion-1", provider_fence_store=old_store, now=NOW
        )
    assert reservation.value.code == "corrupt-term-reservation"

    with control._connect() as connection:
        connection.execute(
            """UPDATE storage_promotion_transactions SET reserved_term=6
               WHERE promotion_id='promotion-1'"""
        )
    _, acknowledgement = _apply_grant(control, old_store, new_store)
    with sqlite3.connect(tmp_path / "provider-new.sqlite3") as connection:
        connection.execute(
            "UPDATE provider_storage_authority_grants SET acknowledgement_id='forged'"
        )
    with pytest.raises(FederationValidationError) as grant:
        new_store.grant_acknowledgement("storage-main", acknowledgement.promotion_id)
    assert grant.value.code == "corrupt-provider-grant-state"


def test_e64_no_grant_before_durable_fence_and_old_authority_remains_invalid(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    with pytest.raises(FederationValidationError):
        control.promotion_grant_command(
            "session-1",
            "storage-main",
            "promotion-1",
            provider_fence_store=_store(tmp_path / "provider-old.sqlite3"),
        )

    control, old_store, new_store, _ = _reserved(tmp_path / "complete")
    manifest = control.manifest("session-1", "storage-main")
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="promotion-in-progress",
        reason_detail="grant pending",
        missing_ranges=(),
        obligations={"repair": ["provider-old"]},
        now=NOW,
    )
    command, acknowledgement = _apply_grant(control, old_store, new_store)
    control.acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )
    assert new_store.has_valid_grant("storage-main", "promotion-1", command.term)
    with pytest.raises(FederationValidationError):
        old_store.validate_grant_term("storage-main", 5)
    assert control.storage_degraded_state("session-1", "storage-main")["obligations"] == {
        "repair": ["provider-old"]
    }

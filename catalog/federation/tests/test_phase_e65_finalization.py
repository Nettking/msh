from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.provider_fencing import ProviderFenceStore

from .test_phase_e63_provider_fencing import NOW, _store
from .test_phase_e64_authority_grant import _apply_grant, _reserved


def _granted(
    tmp_path: Path,
) -> tuple[PhaseDControlPlane, ProviderFenceStore, ProviderFenceStore, Path]:
    control, old_store, new_store, coordinator_path = _reserved(tmp_path)
    manifest = control.manifest("session-1", "storage-main")
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="promotion-in-progress",
        reason_detail="durable grant awaiting finalization",
        missing_ranges=(
            {
                "dataset_id": "primary",
                "first_sequence": 20,
                "last_sequence": 25,
            },
        ),
        obligations={
            "catch_up": ["provider-old"],
            "replicate": ["item-pending"],
        },
        now=NOW,
    )
    _, acknowledgement = _apply_grant(control, old_store, new_store)
    control.acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )
    return control, old_store, new_store, coordinator_path


def _complete(
    control: PhaseDControlPlane,
    old_store: ProviderFenceStore,
    new_store: ProviderFenceStore,
):
    return control.complete_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )


def test_e65_durable_finalization_precedes_degraded_state_clear(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    manifest_before = control.manifest("session-1", "storage-main")

    persisted = control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )

    assert persisted.degraded_cleared is False
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "finalized"
    assert control.storage_degraded_state("session-1", "storage-main") is not None
    assert control.manifest("session-1", "storage-main") == manifest_before


def test_e65_successful_finalization_clears_only_after_durable_proof(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    completed = _complete(control, old_store, new_store)

    assert completed.degraded_cleared is True
    assert completed.selected_provider_id == "provider-new"
    assert completed.new_authority_term == 6
    assert completed.final_authority_identity.startswith("sha256:")
    assert control.storage_degraded_state("session-1", "storage-main") is None
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "finalized"


def test_e65_duplicate_and_restart_after_complete_return_same_result(tmp_path: Path) -> None:
    control, old_store, new_store, coordinator_path = _granted(tmp_path)
    first = _complete(control, old_store, new_store)
    second = _complete(control, old_store, new_store)
    restarted = PhaseDControlPlane(coordinator_path).complete_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=_store(tmp_path / "provider-old.sqlite3"),
        selected_provider_store=ProviderFenceStore(
            tmp_path / "provider-new.sqlite3",
            session_id="session-1",
            provider_id="provider-new",
        ),
        now=NOW,
    )
    assert first == second == restarted


def test_e65_restart_before_finalization_leaves_group_degraded(tmp_path: Path) -> None:
    _, old_store, new_store, coordinator_path = _granted(tmp_path)
    restarted = PhaseDControlPlane(coordinator_path)
    assert restarted.promotion_finalization("session-1", "storage-main", "promotion-1") is None
    assert restarted.storage_degraded_state("session-1", "storage-main") is not None
    assert restarted.promotion_transaction("session-1", "storage-main", "promotion-1").state == "granted-new-authority"
    assert old_store.high_water_term("storage-main") == 5
    assert new_store.has_valid_grant("storage-main", "promotion-1", 6)


def test_e65_restart_after_persistence_clears_matching_degraded_without_reissuing(tmp_path: Path) -> None:
    control, old_store, new_store, coordinator_path = _granted(tmp_path)
    before = control.promotion_transaction("session-1", "storage-main", "promotion-1")
    grant_before = new_store.grant_acknowledgement("storage-main", "promotion-1")
    control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )

    completed = PhaseDControlPlane(coordinator_path).complete_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )
    after = PhaseDControlPlane(coordinator_path).promotion_transaction(
        "session-1", "storage-main", "promotion-1"
    )
    assert completed.degraded_cleared is True
    assert after.reserved_term == before.reserved_term
    assert after.grant_id == before.grant_id
    assert new_store.grant_acknowledgement("storage-main", "promotion-1") == grant_before


def test_e65_missing_grant_evidence_fails_and_preserves_degraded_state(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    with new_store._connect() as connection:
        connection.execute("DELETE FROM provider_storage_authority_grants")
    with pytest.raises(FederationValidationError):
        _complete(control, old_store, new_store)
    assert control.promotion_finalization("session-1", "storage-main", "promotion-1") is None
    assert control.storage_degraded_state("session-1", "storage-main") is not None


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("selected_provider_id", "provider-other"),
        ("group_id", "storage-other"),
        ("session_id", "session-other"),
        ("reserved_term", 99),
        ("selected_manifest_hash", "sha256:" + "e" * 64),
        ("selected_report_hash", "sha256:" + "f" * 64),
    ),
)
def test_e65_mismatched_transaction_binding_fails_closed(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    with control._connect() as connection:
        connection.execute(
            f"UPDATE storage_promotion_transactions SET {column}=? WHERE promotion_id='promotion-1'",
            (value,),
        )
    with pytest.raises(FederationValidationError):
        _complete(control, old_store, new_store)
    assert control.storage_degraded_state("session-1", "storage-main") is not None


@pytest.mark.parametrize(
    ("table", "column"),
    (
        ("provider_storage_fences", "acknowledgement_id"),
        ("provider_storage_authority_grants", "acknowledgement_id"),
    ),
)
def test_e65_corrupt_fence_or_grant_ack_fails_closed(
    tmp_path: Path,
    table: str,
    column: str,
) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    target = old_store if table == "provider_storage_fences" else new_store
    with target._connect() as connection:
        connection.execute(f"UPDATE {table} SET {column}='forged'")
    with pytest.raises(FederationValidationError):
        _complete(control, old_store, new_store)
    assert control.storage_degraded_state("session-1", "storage-main") is not None


def test_e65_newer_or_unrelated_degraded_state_is_never_cleared(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    persisted = control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )
    manifest = control.manifest("session-1", "storage-main")
    newer = control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="newer-repair-required",
        reason_detail="unrelated recovery condition",
        missing_ranges=(),
        obligations={"repair": ["provider-new"]},
        now=NOW.replace(microsecond=1),
    )
    with pytest.raises(FederationValidationError) as error:
        _complete(control, old_store, new_store)
    assert error.value.code == "degraded-state-mismatch"
    assert control.storage_degraded_state("session-1", "storage-main") == newer
    assert persisted.degraded_cleared is False


def test_e65_authorities_manifest_term_and_grant_remain_stable(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    manifest_before = control.manifest("session-1", "storage-main")
    transaction_before = control.promotion_transaction("session-1", "storage-main", "promotion-1")
    grant_before = new_store.grant_acknowledgement("storage-main", "promotion-1")

    completed = _complete(control, old_store, new_store)

    with pytest.raises(FederationValidationError):
        old_store.validate_grant_term("storage-main", 5)
    assert new_store.has_valid_grant("storage-main", "promotion-1", 6)
    assert control.manifest("session-1", "storage-main") == manifest_before
    transaction_after = control.promotion_transaction("session-1", "storage-main", "promotion-1")
    assert transaction_after.reserved_term == transaction_before.reserved_term
    assert transaction_after.grant_id == transaction_before.grant_id
    assert new_store.grant_acknowledgement("storage-main", "promotion-1") == grant_before
    assert completed.new_authority_term == transaction_before.reserved_term


def test_e65_e7_recovery_obligations_remain_visible_after_degraded_exit(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    _complete(control, old_store, new_store)
    recovery = control.promotion_recovery_obligations("session-1", "storage-main", "promotion-1")
    assert recovery == {
        "missing_ranges": [
            {
                "dataset_id": "primary",
                "first_sequence": 20,
                "last_sequence": 25,
            }
        ],
        "obligations": {
            "catch_up": ["provider-old"],
            "replicate": ["item-pending"],
        },
    }


def test_e65_corrupt_finalization_state_fails_closed(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _granted(tmp_path)
    control.persist_promotion_finalization(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
    )
    with sqlite3.connect(control.database) as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE storage_promotion_finalizations SET recovery_obligations_json='not-json'"
        )
    with pytest.raises(FederationValidationError) as error:
        control.promotion_finalization("session-1", "storage-main", "promotion-1")
    assert error.value.code == "corrupt-promotion-finalization"
    assert control.storage_degraded_state("session-1", "storage-main") is not None

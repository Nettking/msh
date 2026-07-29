from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.provider_fencing import ProviderFenceStore

from .test_phase_e63_provider_fencing import NOW, _store, _validated
from .test_phase_e64_authority_grant import _apply_grant, _new_store


def _base(
    tmp_path: Path,
) -> tuple[PhaseDControlPlane, ProviderFenceStore, ProviderFenceStore, Path]:
    coordinator_path = tmp_path / "coordinator.sqlite3"
    control = _validated(coordinator_path)
    manifest = control.manifest("session-1", "storage-main")
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="promotion-recovery",
        reason_detail="promotion requires restart-safe recovery",
        missing_ranges=(
            {
                "dataset_id": "primary",
                "first_sequence": 50,
                "last_sequence": 55,
            },
        ),
        obligations={
            "catch_up": ["provider-old"],
            "replicate": ["item-e7"],
        },
        now=NOW,
    )
    return (
        control,
        _store(tmp_path / "provider-old.sqlite3"),
        _new_store(tmp_path / "provider-new.sqlite3"),
        coordinator_path,
    )


def _persist_fence(control: PhaseDControlPlane, old_store: ProviderFenceStore):
    return old_store.apply(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-1"),
        now=NOW,
    )


def _record_fence(control: PhaseDControlPlane, old_store: ProviderFenceStore):
    acknowledgement = old_store.acknowledgement("storage-main") or _persist_fence(control, old_store)
    return control.acknowledge_promotion_fencing(
        acknowledgement,
        actor_node_id="node-old",
        provider_fence_store=old_store,
    )


def _reserve(control: PhaseDControlPlane, old_store: ProviderFenceStore):
    if control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "validated":
        _record_fence(control, old_store)
    return control.reserve_promotion_term(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        now=NOW,
    )


def _persist_grant(
    control: PhaseDControlPlane,
    old_store: ProviderFenceStore,
    new_store: ProviderFenceStore,
):
    _reserve(control, old_store)
    return _apply_grant(control, old_store, new_store)[1]


def _record_grant(
    control: PhaseDControlPlane,
    old_store: ProviderFenceStore,
    new_store: ProviderFenceStore,
):
    acknowledgement = (
        new_store.grant_acknowledgement("storage-main", "promotion-1")
        or _persist_grant(control, old_store, new_store)
    )
    return control.acknowledge_promotion_grant(
        acknowledgement,
        actor_node_id="node-new",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
    )


def _recover(
    control: PhaseDControlPlane,
    old_store: ProviderFenceStore,
    new_store: ProviderFenceStore,
    **options,
):
    return control.recover_promotion(
        "session-1",
        "storage-main",
        "promotion-1",
        provider_fence_store=old_store,
        selected_provider_store=new_store,
        now=NOW,
        **options,
    )


@pytest.mark.parametrize(
    "boundary",
    (
        "before-provider-fencing",
        "after-provider-fencing-persistence",
        "after-fencing-acknowledgement",
        "before-term-reservation",
        "after-term-reservation",
        "after-provider-grant-persistence",
        "after-grant-acknowledgement",
        "before-finalization-persistence",
        "after-finalization-persistence",
        "before-degraded-state-clearing",
        "after-complete-finalization",
    ),
)
def test_e66_recovers_every_crash_boundary(tmp_path: Path, boundary: str) -> None:
    control, old_store, new_store, coordinator_path = _base(tmp_path)
    if boundary == "after-provider-fencing-persistence":
        _persist_fence(control, old_store)
    elif boundary in ("after-fencing-acknowledgement", "before-term-reservation"):
        _record_fence(control, old_store)
    elif boundary == "after-term-reservation":
        _reserve(control, old_store)
    elif boundary == "after-provider-grant-persistence":
        _persist_grant(control, old_store, new_store)
    elif boundary in ("after-grant-acknowledgement", "before-finalization-persistence"):
        _record_grant(control, old_store, new_store)
    elif boundary in ("after-finalization-persistence", "before-degraded-state-clearing"):
        _record_grant(control, old_store, new_store)
        control.persist_promotion_finalization(
            "session-1",
            "storage-main",
            "promotion-1",
            provider_fence_store=old_store,
            selected_provider_store=new_store,
            now=NOW,
        )
    elif boundary == "after-complete-finalization":
        _record_grant(control, old_store, new_store)
        control.complete_promotion_finalization(
            "session-1",
            "storage-main",
            "promotion-1",
            provider_fence_store=old_store,
            selected_provider_store=new_store,
            now=NOW,
        )

    result = _recover(PhaseDControlPlane(coordinator_path), old_store, new_store)
    assert result.status == "completed"
    assert result.stage == "finalized"
    assert result.finalization is not None and result.finalization.degraded_cleared


def test_e66_lost_fencing_response_reconciles_provider_evidence(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    fence = _persist_fence(control, old_store)
    result = _recover(
        control,
        old_store,
        new_store,
        fencing_delivery_available=False,
    )
    assert result.status == "completed"
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").fencing_ack_identity == fence.acknowledgement_id


def test_e66_lost_grant_response_reconciles_provider_evidence(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    grant = _persist_grant(control, old_store, new_store)
    result = _recover(
        control,
        old_store,
        new_store,
        grant_delivery_available=False,
    )
    assert result.status == "completed"
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").grant_ack_identity == grant.acknowledgement_id


@pytest.mark.parametrize("delivery", ("fencing", "grant"))
def test_e66_temporary_provider_unavailability_is_retryable_not_failed(
    tmp_path: Path,
    delivery: str,
) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    if delivery == "grant":
        _reserve(control, old_store)
    result = _recover(
        control,
        old_store,
        new_store,
        fencing_delivery_available=delivery != "fencing",
        grant_delivery_available=delivery != "grant",
    )
    assert result.status == "retryable"
    assert result.transaction is not None
    assert result.transaction.state in ("validated", "allocated-term")
    assert control.storage_degraded_state("session-1", "storage-main") is not None

    completed = _recover(control, old_store, new_store)
    assert completed.status == "completed"


def test_e66_duplicate_and_completed_result_replay_do_not_repeat_side_effects(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    first = _recover(control, old_store, new_store)
    transaction = control.promotion_transaction("session-1", "storage-main", "promotion-1")
    fence = old_store.acknowledgement("storage-main")
    grant = new_store.grant_acknowledgement("storage-main", "promotion-1")

    second = _recover(control, old_store, new_store)

    assert first.status == second.status == "completed"
    assert second.finalization == first.finalization
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1") == transaction
    assert old_store.acknowledgement("storage-main") == fence
    assert new_store.grant_acknowledgement("storage-main", "promotion-1") == grant


def test_e66_concurrent_recovery_converges_on_one_result(tmp_path: Path) -> None:
    _control, old_store, new_store, coordinator_path = _base(tmp_path)

    def recover() -> object:
        return _recover(PhaseDControlPlane(coordinator_path), old_store, new_store)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = tuple(executor.map(lambda _: recover(), range(4)))

    assert {result.status for result in results} == {"completed"}
    assert len({result.finalization.final_authority_identity for result in results}) == 1
    with sqlite3.connect(coordinator_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM storage_promotion_finalizations"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(DISTINCT reserved_term) FROM storage_promotion_transactions"
        ).fetchone()[0] == 1
    with new_store._connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM provider_storage_authority_grants"
        ).fetchone()[0] == 1


@pytest.mark.parametrize(
    ("corruption", "expected_code"),
    (
        ("transaction-row", "invalid-content-hash"),
        ("fencing-state", "corrupt-provider-fencing-state"),
        ("reservation-state", "impossible-promotion-stage"),
        ("grant-state", "corrupt-provider-grant-state"),
        ("finalization-state", "corrupt-promotion-finalization"),
        ("impossible-order", "impossible-promotion-stage"),
        ("coordinator-ahead-no-fence", "unverifiable-provider-fence"),
        ("coordinator-ahead-no-grant", "unverifiable-provider-grant"),
    ),
)
def test_e66_corrupt_or_contradictory_state_requires_operator_attention(
    tmp_path: Path,
    corruption: str,
    expected_code: str,
) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    if corruption == "transaction-row":
        with control._connect() as connection:
            connection.execute(
                "UPDATE storage_promotion_transactions SET selected_report_hash='invalid'"
            )
    elif corruption == "fencing-state":
        _persist_fence(control, old_store)
        with old_store._connect() as connection:
            connection.execute("UPDATE provider_storage_fences SET acknowledgement_id='forged'")
    elif corruption == "reservation-state":
        _record_fence(control, old_store)
        with control._connect() as connection:
            connection.execute(
                """UPDATE storage_promotion_transactions
                   SET state='allocated-term', reserved_term=NULL, grant_id=NULL"""
            )
    elif corruption == "grant-state":
        _persist_grant(control, old_store, new_store)
        with new_store._connect() as connection:
            connection.execute(
                "UPDATE provider_storage_authority_grants SET acknowledgement_id='forged'"
            )
    elif corruption == "finalization-state":
        _record_grant(control, old_store, new_store)
        control.persist_promotion_finalization(
            "session-1",
            "storage-main",
            "promotion-1",
            provider_fence_store=old_store,
            selected_provider_store=new_store,
            now=NOW,
        )
        with control._connect() as connection:
            connection.execute("PRAGMA ignore_check_constraints=ON")
            connection.execute(
                "UPDATE storage_promotion_finalizations SET missing_ranges_json='invalid'"
            )
    elif corruption == "impossible-order":
        with control._connect() as connection:
            connection.execute(
                """UPDATE storage_promotion_transactions
                   SET state='granted-new-authority', grant_acknowledged=0,
                       grant_ack_identity=NULL"""
            )
    elif corruption == "coordinator-ahead-no-fence":
        _record_fence(control, old_store)
        with old_store._connect() as connection:
            connection.execute("DELETE FROM provider_storage_fences")
    elif corruption == "coordinator-ahead-no-grant":
        _record_grant(control, old_store, new_store)
        with new_store._connect() as connection:
            connection.execute("DELETE FROM provider_storage_authority_grants")

    result = _recover(control, old_store, new_store)
    assert result.status == "operator-attention"
    assert result.code == expected_code
    assert control.storage_degraded_state("session-1", "storage-main") is not None


def test_e66_mismatched_provider_evidence_fails_closed(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    fence = _persist_fence(control, old_store)
    with old_store._connect() as connection:
        connection.execute(
            "UPDATE provider_storage_fences SET promotion_id='promotion-other'"
        )
        connection.execute(
            "UPDATE provider_storage_fences SET acknowledgement_id=?",
            ("sha256:" + "a" * 64,),
        )
    result = _recover(control, old_store, new_store)
    assert result.status == "operator-attention"
    assert result.code == "corrupt-provider-fencing-state"
    assert fence.promotion_id == "promotion-1"


def test_e66_durable_failed_transaction_replays_diagnostics_and_preserves_state(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    _reserve(control, old_store)
    failed = control.fail_promotion_transaction(
        "session-1",
        "storage-main",
        "promotion-1",
        failure_code="operator-aborted-promotion",
        failure_reason="provider identity was administratively revoked",
        now=NOW,
    )
    result = _recover(control, old_store, new_store)
    assert result.status == "failed"
    assert result.code == failed.failure_code
    assert result.reason == failed.failure_reason
    assert result.transaction.reserved_term == failed.reserved_term
    assert old_store.acknowledgement("storage-main") is not None
    assert control.storage_degraded_state("session-1", "storage-main") is not None


def test_e66_manifest_obligations_and_single_authority_are_preserved(tmp_path: Path) -> None:
    control, old_store, new_store, _ = _base(tmp_path)
    manifest = control.manifest("session-1", "storage-main")
    result = _recover(control, old_store, new_store)
    assert result.status == "completed"
    assert control.manifest("session-1", "storage-main") == manifest
    assert control.promotion_recovery_obligations("session-1", "storage-main", "promotion-1") == {
        "missing_ranges": [
            {
                "dataset_id": "primary",
                "first_sequence": 50,
                "last_sequence": 55,
            }
        ],
        "obligations": {
            "catch_up": ["provider-old"],
            "replicate": ["item-e7"],
        },
    }
    with pytest.raises(FederationValidationError):
        old_store.validate_grant_term("storage-main", 5)
    assert new_store.has_valid_grant("storage-main", "promotion-1", 6)

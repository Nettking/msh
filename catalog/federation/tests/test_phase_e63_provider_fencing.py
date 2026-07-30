from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.provider_fencing import (
    PROVIDER_FENCE_SCHEMA,
    ProviderFenceStore,
    StorageFenceAcknowledgement,
)
from catalog.federation.selection import StoragePromotionSelection
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

from .test_phase_e6_promotion_transaction import _report

NOW = datetime(2026, 7, 29, 19, 0, tzinfo=timezone.utc)


def _provider(provider_id: str, node_id: str) -> StorageProviderRegistration:
    return StorageProviderRegistration(
        session_id="session-1",
        provider_id=provider_id,
        node_id=node_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True,
        status="ready",
    )


def _validated(database: Path) -> PhaseDControlPlane:
    control = PhaseDControlPlane(database)
    control.create_group("session-1", "coordinator", "storage-main")
    control.register_provider("session-1", "coordinator", _provider("provider-old", "node-old"))
    control.register_provider("session-1", "coordinator", _provider("provider-new", "node-new"))
    control.change_assignment(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-old",
        ("provider-new",),
    )
    control.grant_leader(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-old",
        "grant-old",
        5,
        9,
        occurred_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    manifest = control.manifest("session-1", "storage-main")
    control.submit_storage_replica_report(
        _report(manifest, provider_id="provider-new"),
        actor_node_id="node-new",
    )
    selection = StoragePromotionSelection(
        decision="select-candidate",
        selected_provider_id="provider-new",
        selected_role="replica",
        candidate_provider_ids=("provider-new",),
        rejected={},
        reason="highest-ranked-complete-candidate",
        preferred_current_primary=None,
    )
    record = control.validate_promotion_transaction_inputs(
        "session-1",
        "storage-main",
        selection,
        promotion_id="promotion-1",
        reported_at=NOW,
    )
    control.create_promotion_transaction(record)
    return control


def _store(path: Path) -> ProviderFenceStore:
    return ProviderFenceStore(path, session_id="session-1", provider_id="provider-old")


def test_e63_provider_persists_fence_before_ack_and_rejects_old_and_lower_terms(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    provider_path = tmp_path / "provider.sqlite3"
    store = _store(provider_path)
    command = control.promotion_fencing_command("session-1", "storage-main", "promotion-1")

    acknowledgement = store.apply(command, now=NOW)
    restarted = _store(provider_path)

    assert restarted.high_water_term("storage-main") == 5
    for term in (0, 4, 5):
        with pytest.raises(FederationValidationError) as error:
            restarted.validate_grant_term("storage-main", term)
        assert error.value.code == "stale-term"
    restarted.validate_grant_term("storage-main", 6)
    assert acknowledgement.schema == PROVIDER_FENCE_SCHEMA


def test_e63_duplicate_and_lost_response_retry_return_same_durable_ack(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    store = _store(tmp_path / "provider.sqlite3")
    command = control.promotion_fencing_command("session-1", "storage-main", "promotion-1")

    lost = store.apply(command, now=NOW)
    retried = _store(tmp_path / "provider.sqlite3").apply(command, now=NOW + timedelta(seconds=10))

    assert retried == lost


def test_e63_restart_before_send_and_after_provider_persistence_are_recoverable(tmp_path: Path) -> None:
    coordinator_path = tmp_path / "coordinator.sqlite3"
    provider_path = tmp_path / "provider.sqlite3"
    _validated(coordinator_path)
    restarted = PhaseDControlPlane(coordinator_path)
    command = restarted.promotion_fencing_command("session-1", "storage-main", "promotion-1")
    acknowledgement = _store(provider_path).apply(command, now=NOW)

    assert restarted.promotion_transaction("session-1", "storage-main", "promotion-1").state == "validated"
    fenced = PhaseDControlPlane(coordinator_path).acknowledge_promotion_fencing(
        acknowledgement,
        actor_node_id="node-old",
        provider_fence_store=_store(provider_path),
    )
    assert fenced.state == "fenced-old-authority"


def test_e63_restart_after_coordinator_records_fence_is_idempotent(tmp_path: Path) -> None:
    coordinator_path = tmp_path / "coordinator.sqlite3"
    provider_path = tmp_path / "provider.sqlite3"
    control = _validated(coordinator_path)
    store = _store(provider_path)
    acknowledgement = store.apply(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-1"),
        now=NOW,
    )
    first = control.acknowledge_promotion_fencing(
        acknowledgement,
        actor_node_id="node-old",
        provider_fence_store=store,
    )
    second = PhaseDControlPlane(coordinator_path).acknowledge_promotion_fencing(
        acknowledgement,
        actor_node_id="node-old",
        provider_fence_store=_store(provider_path),
    )
    assert second == first


@pytest.mark.parametrize(
    ("changes", "actor_node_id"),
    (
        ({"promotion_id": "promotion-other"}, "node-old"),
        ({"provider_id": "provider-new"}, "node-old"),
        ({"group_id": "storage-other"}, "node-old"),
        ({"fencing_high_water_term": 4}, "node-old"),
        ({}, "node-new"),
    ),
)
def test_e63_mismatched_stale_or_unauthenticated_ack_fails_closed(
    tmp_path: Path,
    changes: dict[str, object],
    actor_node_id: str,
) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    store = _store(tmp_path / "provider.sqlite3")
    acknowledgement = store.apply(
        control.promotion_fencing_command("session-1", "storage-main", "promotion-1"),
        now=NOW,
    )
    invalid = replace(acknowledgement, **changes)
    with pytest.raises(FederationValidationError):
        control.acknowledge_promotion_fencing(
            invalid,
            actor_node_id=actor_node_id,
            provider_fence_store=store,
        )
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "validated"


def test_e63_coordinator_only_false_success_cannot_advance_transaction(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    command = control.promotion_fencing_command("session-1", "storage-main", "promotion-1")
    fabricated = StorageFenceAcknowledgement(
        schema=PROVIDER_FENCE_SCHEMA,
        acknowledgement_id=ProviderFenceStore.acknowledgement_identity(command),
        persisted_at=NOW,
        **command.binding(),
    )
    with pytest.raises(FederationValidationError) as error:
        control.acknowledge_promotion_fencing(
            fabricated,
            actor_node_id="node-old",
            provider_fence_store=_store(tmp_path / "empty-provider.sqlite3"),
        )
    assert error.value.code == "missing-durable-fencing-evidence"
    assert control.promotion_transaction("session-1", "storage-main", "promotion-1").state == "validated"


def test_e63_failed_delivery_is_retryable_and_preserves_degraded_state(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="promotion-required",
        reason_detail="old authority must be fenced",
        missing_ranges=(),
        obligations={"replicate": ["item-a"]},
        now=NOW,
    )
    failed = control.record_promotion_fencing_delivery_failure(
        "session-1",
        "storage-main",
        "promotion-1",
        reason="provider unavailable",
        now=NOW,
    )
    assert failed.state == "validated"
    assert failed.fencing_status == "delivery-failed-retryable"
    assert failed.reserved_term is None
    assert control.storage_degraded_state("session-1", "storage-main")["obligations"] == {
        "replicate": ["item-a"]
    }


def test_e63_corrupt_provider_fence_fails_closed(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    provider_path = tmp_path / "provider.sqlite3"
    store = _store(provider_path)
    store.apply(control.promotion_fencing_command("session-1", "storage-main", "promotion-1"), now=NOW)
    with sqlite3.connect(provider_path) as connection:
        connection.execute(
            "UPDATE provider_storage_fences SET acknowledgement_id='forged' WHERE group_id='storage-main'"
        )
    with pytest.raises(FederationValidationError) as error:
        _store(provider_path).acknowledgement("storage-main")
    assert error.value.code == "corrupt-provider-fencing-state"


def test_e63_command_emission_alone_never_advances_or_allocates_authority(tmp_path: Path) -> None:
    control = _validated(tmp_path / "coordinator.sqlite3")
    command = control.promotion_fencing_command("session-1", "storage-main", "promotion-1")
    record = PhaseDControlPlane(tmp_path / "coordinator.sqlite3").promotion_transaction(
        "session-1", "storage-main", "promotion-1"
    )
    assert command.fencing_high_water_term == record.previous_term == 5
    assert record.state == "validated"
    assert record.fencing_acknowledged is False
    assert record.reserved_term is None
    assert record.grant_id is None

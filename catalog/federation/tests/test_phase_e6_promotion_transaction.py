from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation import (
    CommitState,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
    StorageReplicaReport,
)
from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import AuthoritativeStorageManifest
from catalog.federation.manifest_store import ManifestItemProposal
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.promotion_transaction import (
    PROMOTION_TRANSACTION_SCHEMA,
    PromotionTransactionRecord,
)
from catalog.federation.selection import StoragePromotionSelection
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

NOW = datetime(2026, 7, 29, 18, 0, tzinfo=timezone.utc)


def _control(database: Path) -> PhaseDControlPlane:
    control = PhaseDControlPlane(database)
    control.create_group("session-1", "coordinator", "storage-main")
    control.register_provider(
        "session-1",
        "coordinator",
        StorageProviderRegistration(
            session_id="session-1",
            provider_id="provider-a",
            node_id="node-a",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    control.change_assignment("session-1", "coordinator", "storage-main", "provider-a", ())
    return control


def _manifest() -> AuthoritativeStorageManifest:
    return AuthoritativeStorageManifest.build(
        session_id="session-1",
        group_id="storage-main",
        revision=0,
        term=0,
        previous_manifest_hash=None,
        datasets=(
            DatasetManifest(
                dataset_id="primary",
                schema_name="records",
                schema_version=1,
                required=True,
                source_id="source-a",
                last_contiguous_sequence=2,
                missing_ranges=(),
            ),
        ),
        items=(
            ManifestItem(
                item_id="item-a",
                kind=ManifestItemKind.BATCH,
                dataset_id="primary",
                idempotency_key="idem-a",
                content_hash="sha256:" + "a" * 64,
                size_bytes=1,
                schema_name="records",
                schema_version=1,
                source_id="source-a",
                first_sequence=1,
                last_sequence=2,
                commit_state=CommitState.COMMITTED,
                acknowledged_provider_ids=("provider-a",),
                committed_at=NOW,
            ),
        ),
        updated_at=NOW,
    )


def _report(manifest: AuthoritativeStorageManifest, *, provider_id: str = "provider-a") -> StorageReplicaReport:
    return StorageReplicaReport(
        schema="fcp.storage_replica_report.v1",
        protocol_version="1.0",
        session_id=manifest.session_id,
        group_id=manifest.group_id,
        provider_id=provider_id,
        report_revision=1,
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
        committed_item_hashes=tuple(item.content_hash for item in manifest.items),
        datasets=manifest.datasets,
        integrity_verified=True,
        synchronization_state="synchronized",
        eligible=True,
        eligibility_reasons=("eligible",),
        reported_at=NOW,
    )


def _advance_manifest(control: PhaseDControlPlane) -> None:
    control.manifests.commit(
        ManifestItemProposal(
            session_id="session-1",
            group_id="storage-main",
            item_id="item-b",
            kind=ManifestItemKind.BATCH,
            dataset_id="primary",
            idempotency_key="idem-b",
            content_hash="sha256:" + "b" * 64,
            size_bytes=1,
            schema_name="records",
            schema_version=1,
            source_id=None,
            first_sequence=None,
            last_sequence=None,
            dataset_required=True,
            acknowledged_provider_ids=("provider-a",),
            term=0,
            expected_control_revision=control.snapshot("session-1").revision,
            committed_at=NOW,
        )
    )


def _selection(provider_id: str = "provider-a") -> StoragePromotionSelection:
    return StoragePromotionSelection(
        decision="select-candidate",
        selected_provider_id=provider_id,
        selected_role="primary",
        candidate_provider_ids=(provider_id,),
        rejected={},
        reason="highest-ranked-complete-candidate",
        preferred_current_primary=None,
    )


def _record(*, state: str = "pending", failure_code: str | None = None, failure_reason: str | None = None, updated_at: datetime = NOW) -> PromotionTransactionRecord:
    return PromotionTransactionRecord(
        schema=PROMOTION_TRANSACTION_SCHEMA,
        promotion_id="promotion-1",
        session_id="session-1",
        group_id="storage-main",
        selected_provider_id="provider-a",
        selected_report_hash="sha256:" + "a" * 64,
        selected_report_revision=9,
        selected_manifest_revision=7,
        selected_manifest_hash="sha256:" + "b" * 64,
        previous_provider_id="provider-b",
        previous_term=12,
        reserved_term=13,
        fencing_status="not-started",
        fencing_acknowledged=False,
        fencing_ack_identity=None,
        grant_id=None,
        grant_status="not-started",
        grant_acknowledged=False,
        grant_ack_identity=None,
        state=state,
        failure_code=failure_code,
        failure_reason=failure_reason,
        updated_at=updated_at,
    )


def test_e61_promotion_record_round_trip_and_restart_safe(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    record = _record()
    saved = control.upsert_promotion_transaction(record)

    restored = PhaseDControlPlane(tmp_path / "promotion.sqlite3").promotion_transaction(
        "session-1",
        "storage-main",
        "promotion-1",
    )

    assert saved == record
    assert restored == record
    assert restored is not None
    assert restored.schema == PROMOTION_TRANSACTION_SCHEMA
    assert restored.state == "pending"


def test_e61_duplicate_upsert_is_idempotent_and_preserves_binding(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    first = control.upsert_promotion_transaction(_record())
    second = control.upsert_promotion_transaction(_record(updated_at=NOW))

    assert first == second
    restored = control.promotion_transaction("session-1", "storage-main", "promotion-1")
    assert restored == first


def test_e61_corrupt_persisted_state_fails_closed(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    control.upsert_promotion_transaction(_record())
    with control._connect() as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE storage_promotion_transactions SET selected_report_hash='not-a-hash' WHERE session_id=? AND group_id=? AND promotion_id=?",
            ("session-1", "storage-main", "promotion-1"),
        )
        connection.commit()
    with pytest.raises(FederationValidationError) as error:
        control.promotion_transaction("session-1", "storage-main", "promotion-1")
    assert error.value.code == "invalid-content-hash"


def test_e61_schema_is_additive_and_restart_recovers_existing_rows(tmp_path: Path) -> None:
    database = tmp_path / "promotion.sqlite3"
    control = _control(database)
    control.upsert_promotion_transaction(_record(state="validated"))

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='storage_promotion_transactions'"
            )
        }
    assert tables == {"storage_promotion_transactions"}
    assert PhaseDControlPlane(database).promotion_transaction("session-1", "storage-main", "promotion-1").state == "validated"


def test_e61_failed_state_persists_failure_reason(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    saved = control.upsert_promotion_transaction(
        _record(
            state="failed",
            failure_code="failed-fencing",
            failure_reason="provider rejected fence",
        )
    )
    assert saved.state == "failed"
    assert saved.failure_code == "failed-fencing"
    assert saved.failure_reason == "provider rejected fence"


def test_e62_validation_binds_report_and_manifest_and_is_idempotent(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    report = _report(manifest)
    control.submit_storage_replica_report(report, actor_node_id="node-a")

    record = control.validate_promotion_transaction_inputs(
        "session-1",
        "storage-main",
        _selection(),
        promotion_id="promotion-1",
        reported_at=NOW,
    )
    saved = control.create_promotion_transaction(record)
    repeated = control.create_promotion_transaction(record)

    assert saved == record
    assert repeated == record
    assert saved.state == "validated"
    assert saved.selected_report_hash == report.report_hash()
    assert saved.selected_manifest_hash == manifest.manifest_hash


def test_e62_conflicting_promotion_id_fails_closed(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    control.submit_storage_replica_report(_report(manifest), actor_node_id="node-a")
    record = control.validate_promotion_transaction_inputs(
        "session-1",
        "storage-main",
        _selection(),
        promotion_id="promotion-1",
        reported_at=NOW,
    )
    control.create_promotion_transaction(record)
    conflicting = PromotionTransactionRecord(
        **{**record.to_dict(), "selected_report_hash": "sha256:" + "b" * 64, "updated_at": NOW}
    )
    with pytest.raises(FederationValidationError) as error:
        control.create_promotion_transaction(conflicting)
    assert error.value.code == "conflicting-promotion-command"


def test_e62_rejects_stale_report_and_changed_manifest(tmp_path: Path) -> None:
    control = _control(tmp_path / "promotion.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    report = _report(manifest)
    control.submit_storage_replica_report(report, actor_node_id="node-a")
    _advance_manifest(control)
    with pytest.raises(FederationValidationError):
        control.validate_promotion_transaction_inputs(
            "session-1",
            "storage-main",
            _selection(),
            promotion_id="promotion-2",
            reported_at=NOW,
        )

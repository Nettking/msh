from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.promotion_transaction import (
    PROMOTION_TRANSACTION_SCHEMA,
    PromotionTransactionRecord,
)
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
    return control


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

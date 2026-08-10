from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation import (
    CommitState,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
    StorageReplicaReport,
    assess_storage_replica_report,
    select_storage_promotion_candidate,
)
from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import AuthoritativeStorageManifest
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

NOW = datetime(2026, 7, 29, 17, 0, tzinfo=timezone.utc)


def _manifest(*, revision: int = 0, term: int = 7) -> AuthoritativeStorageManifest:
    datasets = (
        DatasetManifest(
            dataset_id="primary",
            schema_name="records",
            schema_version=1,
            required=True,
            source_id="source-a",
            last_contiguous_sequence=2,
            missing_ranges=(),
        ),
    )
    items = (
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
    )
    return AuthoritativeStorageManifest.build(
        session_id="session-1",
        group_id="storage-main",
        revision=revision,
        term=term,
        previous_manifest_hash=None,
        datasets=datasets,
        items=items,
        updated_at=NOW,
    )


def _report(manifest: AuthoritativeStorageManifest, *, provider_id: str, integrity_verified: bool = True, manifest_hash: str | None = None, report_revision: int = 1) -> StorageReplicaReport:
    return StorageReplicaReport(
        schema="fcp.storage_replica_report.v1",
        protocol_version="1.0",
        session_id=manifest.session_id,
        group_id=manifest.group_id,
        provider_id=provider_id,
        report_revision=report_revision,
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash if manifest_hash is None else manifest_hash,
        committed_item_hashes=tuple(item.content_hash for item in manifest.items),
        datasets=manifest.datasets,
        integrity_verified=integrity_verified,
        synchronization_state="synchronized",
        eligible=True,
        eligibility_reasons=("eligible",),
        reported_at=NOW,
    )


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


def test_e5_enters_and_persists_storage_degraded_state(tmp_path: Path) -> None:
    control = _control(tmp_path / "e5.sqlite3")
    manifest = _manifest()
    state = control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="required-committed-data-unavailable",
        reason_detail="no complete eligible candidate",
        missing_ranges=(),
        obligations={"replication": ["provider-a"]},
        now=NOW,
    )

    restored = PhaseDControlPlane(tmp_path / "e5.sqlite3")
    assert restored.storage_degraded_state("session-1", "storage-main") == state


def test_e5_idempotent_and_preserves_manifest_identity(tmp_path: Path) -> None:
    control = _control(tmp_path / "e5.sqlite3")
    manifest = _manifest()
    first = control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="required-committed-data-unavailable",
        reason_detail="no complete eligible candidate",
        missing_ranges=({"start": 3, "end": 4},),
        obligations={"replication": ["provider-a"], "recovery": ["provider-b"]},
        now=NOW,
    )
    second = control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="required-committed-data-unavailable",
        reason_detail="no complete eligible candidate",
        missing_ranges=({"start": 3, "end": 4},),
        obligations={"recovery": ["provider-b"], "replication": ["provider-a"]},
        now=NOW,
    )

    assert first == second
    assert first["manifest_revision"] == manifest.revision
    assert first["manifest_hash"] == manifest.manifest_hash


def test_e5_recovery_requires_complete_candidate_and_is_deterministic() -> None:
    manifest = _manifest()
    stale_report = _report(manifest, provider_id="provider-a", manifest_hash="sha256:" + "f" * 64)
    stale = assess_storage_replica_report(
        stale_report,
        manifest,
        assigned_provider_ids=("provider-a",),
        expected_session_id="session-1",
        expected_group_id="storage-main",
    )
    healthy = assess_storage_replica_report(
        _report(manifest, provider_id="provider-b"),
        manifest,
        assigned_provider_ids=("provider-b",),
        expected_session_id="session-1",
        expected_group_id="storage-main",
    )

    degraded = select_storage_promotion_candidate(manifest, (stale,))
    recovered = select_storage_promotion_candidate(manifest, (stale, healthy))
    assert degraded.decision == "storage-degraded"
    assert recovered.decision == "select-candidate"
    assert recovered.selected_provider_id == "provider-b"


def test_e5_corrupt_persisted_state_fails_closed(tmp_path: Path) -> None:
    control = _control(tmp_path / "e5.sqlite3")
    manifest = _manifest()
    control.set_storage_degraded_state(
        "session-1",
        "storage-main",
        manifest=manifest,
        reason_code="required-committed-data-unavailable",
        reason_detail="no complete eligible candidate",
        missing_ranges=(),
        obligations={"replication": ["provider-a"]},
        now=NOW,
    )
    with control._connect() as connection:
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute(
            "UPDATE storage_degraded_states SET missing_ranges_json='not-json' WHERE session_id=? AND group_id=?",
            ("session-1", "storage-main"),
        )
        connection.commit()
    with pytest.raises(FederationValidationError) as error:
        control.storage_degraded_state("session-1", "storage-main")
    assert error.value.code == "corrupt-storage-degraded-state"

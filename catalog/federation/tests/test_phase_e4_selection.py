from __future__ import annotations

from datetime import datetime, timezone

from catalog.federation import (
    CommitState,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
    StorageReplicaAssessment,
    StorageReplicaReport,
    assess_storage_replica_report,
    select_storage_promotion_candidate,
)
from catalog.federation.manifest import AuthoritativeStorageManifest

NOW = datetime(2026, 7, 29, 16, 0, tzinfo=timezone.utc)


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
        DatasetManifest(
            dataset_id="secondary",
            schema_name="records",
            schema_version=1,
            required=True,
            source_id="source-b",
            last_contiguous_sequence=4,
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
        ManifestItem(
            item_id="item-b",
            kind=ManifestItemKind.BATCH,
            dataset_id="secondary",
            idempotency_key="idem-b",
            content_hash="sha256:" + "b" * 64,
            size_bytes=1,
            schema_name="records",
            schema_version=1,
            source_id="source-b",
            first_sequence=3,
            last_sequence=4,
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


def _report(manifest: AuthoritativeStorageManifest, *, provider_id: str, datasets: tuple[DatasetManifest, ...] | None = None, committed_hashes: tuple[str, ...] | None = None, manifest_hash: str | None = None, revision: int = 1, manifest_revision: int | None = None, integrity_verified: bool = True, synchronization_state: str = "synchronized", eligible: bool = True, reasons: tuple[str, ...] = ("eligible",)) -> StorageReplicaReport:
    return StorageReplicaReport(
        schema="fcp.storage_replica_report.v1",
        protocol_version="1.0",
        session_id=manifest.session_id,
        group_id=manifest.group_id,
        provider_id=provider_id,
        report_revision=revision,
        manifest_revision=manifest.revision if manifest_revision is None else manifest_revision,
        manifest_hash=manifest.manifest_hash if manifest_hash is None else manifest_hash,
        committed_item_hashes=tuple(item.content_hash for item in manifest.items) if committed_hashes is None else committed_hashes,
        datasets=manifest.datasets if datasets is None else datasets,
        integrity_verified=integrity_verified,
        synchronization_state=synchronization_state,
        eligible=eligible,
        eligibility_reasons=reasons,
        reported_at=NOW,
    )


def _assessment(report: StorageReplicaReport, manifest: AuthoritativeStorageManifest, *, eligible: bool = True, reason: str = "eligible") -> StorageReplicaAssessment:
    return assess_storage_replica_report(
        report,
        manifest,
        assigned_provider_ids=(report.provider_id,),
        expected_session_id=manifest.session_id,
        expected_group_id=manifest.group_id,
    ) if eligible else StorageReplicaAssessment(
        session_id=report.session_id,
        group_id=report.group_id,
        provider_id=report.provider_id,
        report_revision=report.report_revision,
        report_hash=report.report_hash(),
        accepted=True,
        eligibility=False,
        eligibility_reason=reason,
        assessed_at=report.reported_at,
        report=report,
    )


def test_e4_prefers_healthy_current_primary_when_equivalent() -> None:
    manifest = _manifest()
    current = _assessment(_report(manifest, provider_id="provider-b"), manifest)
    other = _assessment(_report(manifest, provider_id="provider-a"), manifest)

    result = select_storage_promotion_candidate(
        manifest,
        (other, current),
        current_primary_provider_id="provider-b",
    )

    assert result.decision == "retain-current"
    assert result.selected_provider_id == "provider-b"
    assert result.candidate_provider_ids == ("provider-b", "provider-a")
    assert result.reason == "current-primary-is-healthy-complete-and-not-authoritatively-behind"


def test_e4_rejects_incomplete_and_corrupted_candidates() -> None:
    manifest = _manifest()
    healthy = _assessment(_report(manifest, provider_id="provider-a"), manifest)
    incomplete_report = _report(
        manifest,
        provider_id="provider-b",
        datasets=(manifest.datasets[0],),
        committed_hashes=(manifest.items[0].content_hash,),
    )
    incomplete = _assessment(incomplete_report, manifest, eligible=False, reason="missing-ranges")
    corrupted_report = _report(
        manifest,
        provider_id="provider-c",
        manifest_hash="sha256:" + "f" * 64,
    )
    corrupted = _assessment(corrupted_report, manifest, eligible=False, reason="manifest-hash-mismatch")

    result = select_storage_promotion_candidate(
        manifest,
        (healthy, incomplete, corrupted),
        current_primary_provider_id=None,
    )

    assert result.decision == "select-candidate"
    assert result.selected_provider_id == "provider-a"
    assert result.rejected["provider-b"] == ("missing-ranges", "committed-hash-conflict")
    assert result.rejected["provider-c"] == ("manifest-hash-mismatch",)


def test_e4_complementary_partials_do_not_produce_false_complete_winner() -> None:
    manifest = _manifest()
    partial_a = _assessment(
        _report(
            manifest,
            provider_id="provider-a",
            datasets=(manifest.datasets[0],),
            committed_hashes=(manifest.items[0].content_hash,),
        ),
        manifest,
        eligible=False,
        reason="missing-ranges",
    )
    partial_b = _assessment(
        _report(
            manifest,
            provider_id="provider-b",
            datasets=(manifest.datasets[1],),
            committed_hashes=(manifest.items[1].content_hash,),
        ),
        manifest,
        eligible=False,
        reason="missing-ranges",
    )

    result = select_storage_promotion_candidate(manifest, (partial_a, partial_b))

    assert result.decision == "storage-degraded"
    assert result.selected_provider_id is None
    assert result.reason == "required-committed-data-unavailable"
    assert result.rejected == {
        "provider-a": ("missing-ranges", "committed-hash-conflict"),
        "provider-b": ("missing-ranges", "committed-hash-conflict"),
    }


def test_e4_selection_is_deterministic_for_ties() -> None:
    manifest = _manifest()
    first = _assessment(_report(manifest, provider_id="provider-a"), manifest)
    second = _assessment(_report(manifest, provider_id="provider-b"), manifest)

    a = select_storage_promotion_candidate(manifest, (first, second), current_primary_provider_id=None)
    b = select_storage_promotion_candidate(manifest, (second, first), current_primary_provider_id=None)

    assert a == b
    assert a.selected_provider_id == "provider-a"

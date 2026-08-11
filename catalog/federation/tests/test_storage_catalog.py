from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
)
from catalog.federation.models import CommitState
from catalog.federation.storage_catalog import (
    MAX_BATCH_PAGE_SIZE,
    CommittedBatchDeltaPage,
    CommittedBatchPage,
    CommittedBatchReference,
    ManifestBatchCatalog,
)

NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
SESSION_ID = "session-catalog"
GROUP_ID = "storage-main"
CURSOR_SECRET = b"catalog-test-secret-is-at-least-32-bytes"


def _item(
    index: int,
    *,
    dataset_id: str = "dataset-a",
    kind: ManifestItemKind = ManifestItemKind.BATCH,
) -> ManifestItem:
    return ManifestItem(
        item_id=f"item-{index:03d}",
        kind=kind,
        dataset_id=dataset_id,
        idempotency_key=f"recorder:{index}" if kind is ManifestItemKind.BATCH else None,
        content_hash=f"sha256:{index + 1:064x}",
        size_bytes=index + 1,
        schema_name="fcp.test",
        schema_version=1,
        source_id=None,
        first_sequence=None,
        last_sequence=None,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider-b", "provider-a"),
        committed_at=NOW + timedelta(seconds=index),
    )


def _manifest(
    items: tuple[ManifestItem, ...],
    *,
    revision: int = 0,
    previous_manifest_hash: str | None = None,
) -> AuthoritativeStorageManifest:
    dataset_ids = sorted({item.dataset_id for item in items})
    return AuthoritativeStorageManifest.build(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        revision=revision,
        term=1,
        previous_manifest_hash=previous_manifest_hash,
        datasets=tuple(
            DatasetManifest(
                dataset_id=dataset_id,
                schema_name="fcp.test",
                schema_version=1,
                required=True,
                source_id=None,
                last_contiguous_sequence=None,
                missing_ranges=(),
            )
            for dataset_id in dataset_ids
        ),
        items=items,
        updated_at=NOW + timedelta(minutes=revision),
    )


class _ManifestStore:
    def __init__(self, *manifests: AuthoritativeStorageManifest) -> None:
        self.by_revision = {manifest.revision: manifest for manifest in manifests}
        self.head_revision = max(self.by_revision)

    def at_revision(
        self,
        session_id: str,
        group_id: str,
        revision: int,
    ) -> AuthoritativeStorageManifest:
        manifest = self.by_revision[revision]
        assert manifest.session_id == session_id
        assert manifest.group_id == group_id
        return manifest


class _ControlPlane:
    def __init__(self, *manifests: AuthoritativeStorageManifest) -> None:
        self.manifests = _ManifestStore(*manifests)

    def manifest(
        self,
        session_id: str,
        group_id: str,
    ) -> AuthoritativeStorageManifest:
        manifest = self.manifests.by_revision[self.manifests.head_revision]
        assert manifest.session_id == session_id
        assert manifest.group_id == group_id
        return manifest


def _catalog(
    *manifests: AuthoritativeStorageManifest,
) -> tuple[ManifestBatchCatalog, _ControlPlane]:
    control = _ControlPlane(*manifests)
    return (
        ManifestBatchCatalog(
            control,
            SESSION_ID,
            cursor_secret=CURSOR_SECRET,
        ),
        control,
    )


def test_reference_and_page_serialization_are_strict_and_canonical() -> None:
    manifest = _manifest((_item(1),))
    reference = CommittedBatchReference.from_manifest_item(
        manifest,
        manifest.items[0],
    )
    page = CommittedBatchPage(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        dataset_id="dataset-a",
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
        batches=(reference,),
        next_cursor=None,
    )

    assert CommittedBatchReference.from_dict(reference.to_dict()) == reference
    assert CommittedBatchPage.from_dict(page.to_dict()) == page
    assert reference.acknowledged_provider_ids == ("provider-a", "provider-b")

    with_extra = reference.to_dict() | {"provider_path": "private/file.json"}
    with pytest.raises(FederationValidationError) as unknown:
        CommittedBatchReference.from_dict(with_extra)
    assert unknown.value.code == "unknown-field"

    missing = page.to_dict()
    del missing["manifest_hash"]
    with pytest.raises(FederationValidationError) as required:
        CommittedBatchPage.from_dict(missing)
    assert required.value.code == "missing-field"

    wrong_container = reference.to_dict()
    wrong_container["acknowledged_provider_ids"] = ("provider-a",)
    with pytest.raises(FederationValidationError) as array:
        CommittedBatchReference.from_dict(wrong_container)
    assert array.value.code == "invalid-array"


def test_pagination_is_bounded_filtered_and_pinned_to_manifest_revision() -> None:
    original_items = tuple(_item(index) for index in range(55)) + (
        _item(100, dataset_id="dataset-b"),
        _item(101, kind=ManifestItemKind.OBJECT),
    )
    revision_zero = _manifest(original_items)
    revision_one = _manifest(
        original_items + (_item(99),),
        revision=1,
        previous_manifest_hash=revision_zero.manifest_hash,
    )
    catalog, control = _catalog(revision_zero, revision_one)
    control.manifests.head_revision = 0

    first = catalog.list_batches(
        GROUP_ID,
        dataset_id="dataset-a",
        limit=500,
    )
    assert len(first.batches) == MAX_BATCH_PAGE_SIZE
    assert first.manifest_revision == 0
    assert first.next_cursor is not None
    assert all(batch.dataset_id == "dataset-a" for batch in first.batches)

    # A new head appears before the next request.  The signed cursor must keep
    # the listing on revision zero, excluding item-099 until a fresh listing.
    control.manifests.head_revision = 1
    second = catalog.list_batches(
        GROUP_ID,
        dataset_id="dataset-a",
        limit=50,
        cursor=first.next_cursor,
    )
    combined = (*first.batches, *second.batches)
    assert second.manifest_revision == 0
    assert len(combined) == 55
    assert len({batch.batch_id for batch in combined}) == 55
    assert "item-099" not in {batch.batch_id for batch in combined}
    assert second.next_cursor is None

    fresh = catalog.list_batches(GROUP_ID, dataset_id="dataset-a")
    assert fresh.manifest_revision == 1
    assert fresh.next_cursor is not None
    fresh_tail = catalog.list_batches(
        GROUP_ID,
        dataset_id="dataset-a",
        cursor=fresh.next_cursor,
    )
    assert "item-099" in {
        batch.batch_id for batch in (*fresh.batches, *fresh_tail.batches)
    }


def test_cursor_rejects_tampering_and_scope_reuse() -> None:
    manifest = _manifest(tuple(_item(index) for index in range(3)))
    catalog, _ = _catalog(manifest)
    first = catalog.list_batches(GROUP_ID, dataset_id="dataset-a", limit=1)
    assert first.next_cursor is not None

    replacement = "A" if first.next_cursor[-1] != "A" else "B"
    tampered = first.next_cursor[:-1] + replacement
    with pytest.raises(FederationValidationError) as signature:
        catalog.list_batches(
            GROUP_ID,
            dataset_id="dataset-a",
            cursor=tampered,
        )
    assert signature.value.code == "invalid-batch-cursor"

    with pytest.raises(FederationValidationError) as dataset_scope:
        catalog.list_batches(
            GROUP_ID,
            dataset_id="dataset-b",
            cursor=first.next_cursor,
        )
    assert dataset_scope.value.code == "invalid-batch-cursor"

    other_secret = ManifestBatchCatalog(
        catalog.control_plane,
        SESSION_ID,
        cursor_secret=b"another-catalog-secret-with-32-bytes",
    )
    with pytest.raises(FederationValidationError) as issuer:
        other_secret.list_batches(
            GROUP_ID,
            dataset_id="dataset-a",
            cursor=first.next_cursor,
        )
    assert issuer.value.code == "invalid-batch-cursor"


def test_resolve_revalidates_pinned_commit_evidence_without_provider_reads() -> None:
    revision_zero = _manifest((_item(1), _item(2, kind=ManifestItemKind.OBJECT)))
    revision_one = _manifest(
        (*revision_zero.items, _item(3)),
        revision=1,
        previous_manifest_hash=revision_zero.manifest_hash,
    )
    catalog, _ = _catalog(revision_zero, revision_one)

    reference = catalog.resolve_batch(GROUP_ID, "item-001")
    assert reference.manifest_revision == 1
    assert catalog.resolve_reference(reference.to_dict()) == reference

    pinned = catalog.resolve_batch(
        GROUP_ID,
        "item-001",
        manifest_revision=0,
        manifest_hash=revision_zero.manifest_hash,
    )
    assert pinned.manifest_revision == 0
    assert catalog.resolve_reference(pinned) == pinned

    forged = replace(reference, content_hash=f"sha256:{999:064x}")
    with pytest.raises(FederationValidationError) as mismatch:
        catalog.resolve_reference(forged)
    assert mismatch.value.code == "batch-reference-mismatch"

    with pytest.raises(FederationValidationError) as object_item:
        catalog.resolve_batch(GROUP_ID, "item-002")
    assert object_item.value.code == "batch-not-found"

    with pytest.raises(FederationValidationError) as missing:
        catalog.resolve_batch(GROUP_ID, "not-committed")
    assert missing.value.code == "batch-not-found"


def test_invalid_limits_are_rejected_before_listing() -> None:
    catalog, _ = _catalog(_manifest((_item(1),)))

    for value in (0, -1, True, 1.5, "2"):
        with pytest.raises(FederationValidationError) as invalid:
            catalog.list_batches(GROUP_ID, limit=value)  # type: ignore[arg-type]
        assert invalid.value.code == "invalid-page-limit"


def test_delta_lists_only_batches_absent_from_exact_baseline() -> None:
    revision_zero = _manifest((_item(1), _item(2, kind=ManifestItemKind.OBJECT)))
    revision_one = _manifest(
        (*revision_zero.items, _item(3), _item(4, dataset_id="dataset-b")),
        revision=1,
        previous_manifest_hash=revision_zero.manifest_hash,
    )
    catalog, _ = _catalog(revision_zero, revision_one)

    page = catalog.list_batch_delta(
        GROUP_ID,
        baseline_manifest_revision=revision_zero.revision,
        baseline_manifest_hash=revision_zero.manifest_hash,
    )

    assert isinstance(page, CommittedBatchDeltaPage)
    assert page.baseline_manifest_revision == 0
    assert page.baseline_manifest_hash == revision_zero.manifest_hash
    assert page.manifest_revision == 1
    assert page.manifest_hash == revision_one.manifest_hash
    assert [batch.batch_id for batch in page.batches] == ["item-003", "item-004"]
    assert CommittedBatchDeltaPage.from_dict(page.to_dict()) == page

    filtered = catalog.list_batch_delta(
        GROUP_ID,
        baseline_manifest_revision=0,
        baseline_manifest_hash=revision_zero.manifest_hash,
        dataset_id="dataset-a",
    )
    assert [batch.batch_id for batch in filtered.batches] == ["item-003"]


def test_delta_short_circuits_an_unchanged_head() -> None:
    manifest = _manifest((_item(1),))
    catalog, _ = _catalog(manifest)

    page = catalog.list_batch_delta(
        GROUP_ID,
        baseline_manifest_revision=manifest.revision,
        baseline_manifest_hash=manifest.manifest_hash,
    )

    assert page.batches == ()
    assert page.next_cursor is None
    assert page.manifest_revision == page.baseline_manifest_revision
    assert page.manifest_hash == page.baseline_manifest_hash


def test_delta_pagination_is_bound_to_baseline_and_snapshotted_head() -> None:
    revision_zero = _manifest((_item(0),))
    revision_one = _manifest(
        (*revision_zero.items, *(_item(index) for index in range(1, 5))),
        revision=1,
        previous_manifest_hash=revision_zero.manifest_hash,
    )
    revision_two = _manifest(
        (*revision_one.items, _item(5)),
        revision=2,
        previous_manifest_hash=revision_one.manifest_hash,
    )
    catalog, control = _catalog(revision_zero, revision_one, revision_two)
    control.manifests.head_revision = 1

    first = catalog.list_batch_delta(
        GROUP_ID,
        baseline_manifest_revision=0,
        baseline_manifest_hash=revision_zero.manifest_hash,
        limit=2,
    )
    assert first.next_cursor is not None
    control.manifests.head_revision = 2

    second = catalog.list_batch_delta(
        GROUP_ID,
        baseline_manifest_revision=0,
        baseline_manifest_hash=revision_zero.manifest_hash,
        limit=2,
        cursor=first.next_cursor,
    )
    assert second.manifest_revision == 1
    assert [batch.batch_id for batch in (*first.batches, *second.batches)] == [
        "item-001",
        "item-002",
        "item-003",
        "item-004",
    ]
    assert second.next_cursor is None

    with pytest.raises(FederationValidationError) as changed_baseline:
        catalog.list_batch_delta(
            GROUP_ID,
            baseline_manifest_revision=1,
            baseline_manifest_hash=revision_one.manifest_hash,
            cursor=first.next_cursor,
        )
    assert changed_baseline.value.code == "invalid-batch-cursor"


def test_delta_rejects_missing_or_mismatched_baseline() -> None:
    revision_zero = _manifest((_item(1),))
    revision_one = _manifest(
        (*revision_zero.items, _item(2)),
        revision=1,
        previous_manifest_hash=revision_zero.manifest_hash,
    )
    catalog, _ = _catalog(revision_zero, revision_one)

    with pytest.raises(FederationValidationError) as missing:
        catalog.list_batch_delta(
            GROUP_ID,
            baseline_manifest_revision=99,
            baseline_manifest_hash=revision_zero.manifest_hash,
        )
    assert missing.value.code == "invalid-manifest-baseline"

    with pytest.raises(FederationValidationError) as mismatch:
        catalog.list_batch_delta(
            GROUP_ID,
            baseline_manifest_revision=0,
            baseline_manifest_hash=f"sha256:{999:064x}",
        )
    assert mismatch.value.code == "manifest-baseline-mismatch"

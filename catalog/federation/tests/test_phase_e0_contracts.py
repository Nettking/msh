from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
    SequenceRange,
)
from catalog.federation.models import CommitState

NOW = datetime(2026, 7, 29, 9, 0, tzinfo=timezone.utc)
GENESIS_HASH = "sha256:" + "0" * 64


def dataset(
    dataset_id: str = "telemetry",
    *,
    source_id: str | None = "recorder-a",
    watermark: int | None = 9,
    missing_ranges: tuple[SequenceRange, ...] = (),
) -> DatasetManifest:
    return DatasetManifest(
        dataset_id=dataset_id,
        schema_name="fcp.telemetry",
        schema_version=1,
        required=True,
        source_id=source_id,
        last_contiguous_sequence=watermark,
        missing_ranges=missing_ranges,
    )


def item(
    item_id: str = "batch-1",
    *,
    dataset_id: str = "telemetry",
    idempotency_key: str | None = "recorder-a:0-9",
    content_hash: str = "sha256:" + "1" * 64,
    commit_state: CommitState = CommitState.COMMITTED,
) -> ManifestItem:
    return ManifestItem(
        item_id=item_id,
        kind=ManifestItemKind.BATCH,
        dataset_id=dataset_id,
        idempotency_key=idempotency_key,
        content_hash=content_hash,
        size_bytes=128,
        schema_name="fcp.telemetry",
        schema_version=1,
        source_id="recorder-a",
        first_sequence=0,
        last_sequence=9,
        commit_state=commit_state,
        acknowledged_provider_ids=("provider-b", "provider-a"),
        committed_at=NOW,
    )


def manifest(
    *,
    datasets: tuple[DatasetManifest, ...] | None = None,
    items: tuple[ManifestItem, ...] | None = None,
) -> AuthoritativeStorageManifest:
    return AuthoritativeStorageManifest.build(
        session_id="session-1",
        group_id="storage-main",
        revision=1,
        term=1,
        previous_manifest_hash=GENESIS_HASH,
        datasets=datasets or (dataset(),),
        items=items or (item(),),
        updated_at=NOW,
    )


def test_e0_manifest_round_trip_is_canonical_and_additive() -> None:
    # F0-017/F0-018: JSON round trip and additive same-schema fields.
    audio = DatasetManifest(
        dataset_id="audio",
        schema_name="fcp.audio",
        schema_version=2,
        required=False,
        source_id=None,
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    value = manifest(datasets=(dataset(), audio))
    encoded = value.to_dict()
    encoded["future_optional_field"] = {"safe": True}

    reconstructed = AuthoritativeStorageManifest.from_dict(encoded)

    assert reconstructed == value
    assert [entry.dataset_id for entry in reconstructed.datasets] == [
        "audio",
        "telemetry",
    ]
    assert reconstructed.manifest_hash == reconstructed.calculated_hash()
    assert reconstructed.dataset_hash("telemetry").startswith("sha256:")


def test_e0_manifest_hash_ignores_wall_clock_but_not_authority() -> None:
    # F0-020: wall clock does not create storage authority.
    first = manifest()
    later = replace(first, updated_at=NOW.replace(hour=10))
    assert later.manifest_hash == first.manifest_hash

    next_revision = AuthoritativeStorageManifest.build(
        session_id=first.session_id,
        group_id=first.group_id,
        revision=2,
        term=first.term,
        previous_manifest_hash=first.manifest_hash,
        datasets=first.datasets,
        items=first.items,
        updated_at=NOW,
    )
    assert next_revision.manifest_hash != first.manifest_hash


def test_e0_manifest_detects_corruption_and_unknown_schema_major() -> None:
    # F0-019/F4-003: schema majors and authoritative hashes are verified.
    encoded = manifest().to_dict()
    encoded["manifest_hash"] = "sha256:" + "f" * 64
    with pytest.raises(FederationValidationError) as corrupted:
        AuthoritativeStorageManifest.from_dict(encoded)
    assert corrupted.value.code == "manifest-hash-mismatch"

    encoded = manifest().to_dict()
    encoded["schema"] = "fcp.storage_manifest.v2"
    with pytest.raises(ProtocolCompatibilityError) as incompatible:
        AuthoritativeStorageManifest.from_dict(encoded)
    assert incompatible.value.code == "unsupported-schema"


def test_e0_authoritative_manifest_contains_only_committed_items() -> None:
    # F4-005: primary-local or under-replicated data cannot claim authority.
    provisional = item(commit_state=CommitState.PENDING_REPLICATION)
    with pytest.raises(FederationValidationError) as error:
        manifest(items=(provisional,))
    assert error.value.code == "item-not-committed"


@pytest.mark.parametrize(
    ("items", "expected_code"),
    [
        ((item(), item(content_hash="sha256:" + "2" * 64)), "duplicate-item"),
        (
            (item(), item("batch-2", content_hash="sha256:" + "2" * 64)),
            "duplicate-idempotency-key",
        ),
    ],
)
def test_e0_manifest_rejects_ambiguous_committed_identity(
    items: tuple[ManifestItem, ...],
    expected_code: str,
) -> None:
    # F3-002: batch and idempotency identities cannot silently conflict.
    with pytest.raises(FederationValidationError) as error:
        manifest(items=items)
    assert error.value.code == expected_code


def test_e0_items_are_scoped_to_declared_dataset_schema_and_source() -> None:
    wrong_dataset = replace(item(), dataset_id="undeclared")
    with pytest.raises(FederationValidationError) as error:
        manifest(items=(wrong_dataset,))
    assert error.value.code == "unknown-dataset"

    wrong_schema = replace(item(), schema_version=2)
    with pytest.raises(FederationValidationError) as error:
        manifest(items=(wrong_schema,))
    assert error.value.code == "item-schema-mismatch"

    wrong_source = replace(item(), source_id="recorder-b")
    with pytest.raises(FederationValidationError) as error:
        manifest(items=(wrong_source,))
    assert error.value.code == "item-source-mismatch"


def test_e0_inclusive_ranges_and_empty_dataset_are_unambiguous() -> None:
    # E2 contract: endpoints are inclusive and empty means no watermark/ranges.
    assert SequenceRange(5, 5).to_dict() == {"start": 5, "end": 5}
    empty = DatasetManifest(
        dataset_id="knowledge",
        schema_name="fcp.knowledge",
        schema_version=1,
        required=True,
        source_id=None,
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    assert empty.last_contiguous_sequence is None
    assert empty.missing_ranges == ()

    with pytest.raises(FederationValidationError) as adjacent:
        dataset(
            watermark=9,
            missing_ranges=(SequenceRange(10, 12), SequenceRange(13, 14)),
        )
    assert adjacent.value.code == "non-normalized-range"

    with pytest.raises(FederationValidationError) as before_watermark:
        dataset(watermark=9, missing_ranges=(SequenceRange(9, 10),))
    assert before_watermark.value.code == "range-before-watermark"


def test_e0_manifest_chain_requires_exact_predecessor_shape() -> None:
    with pytest.raises(FederationValidationError) as missing_predecessor:
        AuthoritativeStorageManifest.build(
            session_id="session-1",
            group_id="storage-main",
            revision=1,
            term=1,
            previous_manifest_hash=None,
            datasets=(dataset(),),
            items=(item(),),
            updated_at=NOW,
        )
    assert missing_predecessor.value.code == "invalid-manifest-chain"

    genesis = AuthoritativeStorageManifest.build(
        session_id="session-1",
        group_id="storage-main",
        revision=0,
        term=0,
        previous_manifest_hash=None,
        datasets=(),
        items=(),
        updated_at=NOW,
    )
    assert genesis.revision == 0
    assert genesis.previous_manifest_hash is None

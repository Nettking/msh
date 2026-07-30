from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import (
    DatasetManifest,
    SequenceRange,
    normalize_sequence_ranges,
    subtract_sequence_ranges,
)
from catalog.federation.manifest_store import (
    AuthoritativeManifestStore,
    DatasetCoverageUpdate,
)
from catalog.federation.storage_control_plane import StorageControlPlaneStore

NOW = datetime(2026, 7, 29, 13, 0, tzinfo=timezone.utc)


def _create_group(database: Path, group_id: str = "storage-main") -> int:
    control = StorageControlPlaneStore(database)
    return control.create_group("session-1", "coordinator", group_id).revision


def _seed_manifest(
    database: Path,
    *,
    group_id: str = "storage-main",
    dataset_id: str = "telemetry",
    source_id: str = "source-a",
) -> AuthoritativeManifestStore:
    revision = _create_group(database, group_id)
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        "session-1",
        group_id,
        term=0,
        expected_control_revision=revision,
        now=NOW,
    )
    store.update_dataset_coverage(
        DatasetCoverageUpdate(
            session_id="session-1",
            group_id=group_id,
            dataset_id=dataset_id,
            schema_name="msh.telemetry",
            schema_version=1,
            required=True,
            source_id=source_id,
            committed_ranges=(),
            expected_control_revision=revision,
            updated_at=NOW,
        )
    )
    return store


def test_e2_normalizes_merges_and_splits_ranges() -> None:
    assert normalize_sequence_ranges(
        (
            SequenceRange(10, 12),
            SequenceRange(1, 3),
            SequenceRange(4, 4),
            SequenceRange(7, 9),
            SequenceRange(8, 15),
        )
    ) == (SequenceRange(1, 4), SequenceRange(7, 15))
    assert subtract_sequence_ranges(
        (SequenceRange(1, 4), SequenceRange(7, 15)),
        (SequenceRange(3, 8),),
    ) == (SequenceRange(1, 2), SequenceRange(9, 15))


def test_e2_empty_dataset_has_no_watermark_or_missing_ranges() -> None:
    empty = DatasetManifest(
        dataset_id="telemetry",
        schema_name="msh.telemetry",
        schema_version=1,
        required=True,
        source_id=None,
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    assert empty.last_contiguous_sequence is None
    assert empty.missing_ranges == ()


def test_e2_first_committed_range_and_contiguous_append() -> None:
    dataset = DatasetManifest(
        dataset_id="telemetry",
        schema_name="msh.telemetry",
        schema_version=1,
        required=True,
        source_id="source-a",
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    first = dataset.with_committed_sequences((SequenceRange(0, 9),))
    assert first.last_contiguous_sequence == 9
    assert first.missing_ranges == ()
    appended = first.with_committed_sequences((SequenceRange(0, 14),))
    assert appended.last_contiguous_sequence == 14
    assert appended.missing_ranges == ()


def test_e2_gap_creation_and_gap_filling_is_deterministic() -> None:
    dataset = DatasetManifest(
        dataset_id="telemetry",
        schema_name="msh.telemetry",
        schema_version=1,
        required=True,
        source_id="source-a",
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    gapped = dataset.with_committed_sequences(
        (SequenceRange(0, 9), SequenceRange(20, 29))
    )
    assert gapped.last_contiguous_sequence == 9
    assert gapped.missing_ranges == (SequenceRange(10, 19),)
    filled = gapped.fill_missing_ranges((SequenceRange(10, 14),))
    assert filled.last_contiguous_sequence == 9
    assert filled.missing_ranges == (SequenceRange(15, 19),)
    completed = filled.fill_missing_ranges((SequenceRange(15, 19),))
    assert completed.missing_ranges == ()


def test_e2_overlapping_identical_evidence_merges_and_conflicting_ranges_fail_closed() -> None:
    dataset = DatasetManifest(
        dataset_id="telemetry",
        schema_name="msh.telemetry",
        schema_version=1,
        required=True,
        source_id="source-a",
        last_contiguous_sequence=None,
        missing_ranges=(),
    )
    merged = dataset.with_committed_sequences(
        (SequenceRange(0, 4), SequenceRange(2, 7))
    )
    assert merged.last_contiguous_sequence == 7
    assert merged.missing_ranges == ()

    with pytest.raises(FederationValidationError) as error:
        SequenceRange(5, 4)
    assert error.value.code == "invalid-range"


def test_e2_restart_reconstructs_the_same_coverage_state(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coverage.sqlite3"
    store = _seed_manifest(database)
    manifest = store.update_dataset_coverage(
        DatasetCoverageUpdate(
            session_id="session-1",
            group_id="storage-main",
            dataset_id="telemetry",
            schema_name="msh.telemetry",
            schema_version=1,
            required=True,
            source_id="source-a",
            committed_ranges=(
                (0, 9),
                (20, 29),
            ),
            expected_control_revision=1,
            updated_at=NOW,
        )
    ).manifest

    restarted = AuthoritativeManifestStore(database)
    assert restarted.head("session-1", "storage-main") == manifest
    assert restarted.history("session-1", "storage-main")[-1] == manifest


def test_e2_multiple_datasets_sources_and_groups_are_isolated(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coverage.sqlite3"
    store = _seed_manifest(database, group_id="storage-a", source_id="source-a")
    _seed_manifest(database, group_id="storage-b", source_id="source-b")

    result_a = store.update_dataset_coverage(
        DatasetCoverageUpdate(
            session_id="session-1",
            group_id="storage-a",
            dataset_id="telemetry",
            schema_name="msh.telemetry",
            schema_version=1,
            required=True,
            source_id="source-a",
            committed_ranges=((0, 4),),
            expected_control_revision=2,
            updated_at=NOW,
        )
    )
    result_b = AuthoritativeManifestStore(database).update_dataset_coverage(
        DatasetCoverageUpdate(
            session_id="session-1",
            group_id="storage-b",
            dataset_id="audio",
            schema_name="msh.audio",
            schema_version=1,
            required=False,
            source_id="source-b",
            committed_ranges=((10, 19),),
            expected_control_revision=2,
            updated_at=NOW,
        )
    )

    assert result_a.manifest.group_id == "storage-a"
    assert result_b.manifest.group_id == "storage-b"
    assert result_a.manifest.datasets[0].last_contiguous_sequence == 4
    assert result_b.manifest.datasets[0].last_contiguous_sequence == 19


def test_e2_rejects_malformed_negative_and_conflicting_coverage_updates(
    tmp_path: Path,
) -> None:
    database = tmp_path / "coverage.sqlite3"
    store = _seed_manifest(database)

    with pytest.raises(FederationValidationError) as negative:
        DatasetManifest(
            dataset_id="telemetry",
            schema_name="msh.telemetry",
            schema_version=1,
            required=True,
            source_id="source-a",
            last_contiguous_sequence=-1,
            missing_ranges=(),
        )
    assert negative.value.code == "invalid-non-negative-integer"

    with pytest.raises(FederationValidationError) as conflict:
        store.update_dataset_coverage(
            DatasetCoverageUpdate(
                session_id="session-1",
                group_id="storage-main",
                dataset_id="telemetry",
                schema_name="msh.telemetry",
                schema_version=2,
                required=True,
                source_id="source-a",
                committed_ranges=((0, 9),),
                expected_control_revision=1,
                updated_at=NOW,
            )
        )
    assert conflict.value.code == "manifest-dataset-conflict"

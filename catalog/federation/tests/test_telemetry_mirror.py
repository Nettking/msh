from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from catalog.common.artifact_registry import scan_artifacts
from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CommitState
from catalog.federation.storage_catalog import CommittedBatchReference
from catalog.federation.storage_protocol import BatchIngestRequest
from catalog.federation.telemetry_mirror import (
    FederatedTelemetryMirror,
    TelemetryMirrorIngestState,
)


def _observation(sequence: int, value: float) -> dict[str, object]:
    return {
        "schema": "fcp.mtconnect.observation.v2",
        "source": "mtconnect_recorder",
        "source_name": "Mazak Cell 1",
        "source_record_id": f"77:{sequence}:xp:0",
        "machine": "Mazak",
        "machine_name": "Mazak",
        "machine_id": "MAZAK-001",
        "agent_instance_id": 77,
        "sequence": sequence,
        "timestamp": f"2026-08-09T03:00:{sequence:02d}Z",
        "received_at": "2026-08-09T03:01:00Z",
        "data_item_id": "xp",
        "name": "Xabs",
        "category": "SAMPLE",
        "observation_type": "Position",
        "value": value,
        "native_value": str(value),
    }


def _content(
    *sequences: int,
    raw_digest: str = "a" * 64,
    values: dict[int, float] | None = None,
    instance_id: int = 77,
) -> dict[str, object]:
    observations = [
        _observation(sequence, (values or {}).get(sequence, float(sequence)))
        for sequence in sequences
    ]
    for observation in observations:
        observation["agent_instance_id"] = instance_id
        observation["source_record_id"] = (
            f"{instance_id}:{observation['sequence']}:xp:0"
        )
    return {
        "schema": "fcp.mtconnect.observations.v1",
        "source_name": "Mazak Cell 1",
        "machine_id": "MAZAK-001",
        "agent_instance_id": instance_id,
        "first_sequence": sequences[0],
        "last_sequence": sequences[-1],
        "raw_batch_first_sequence": sequences[0],
        "raw_batch_last_sequence": sequences[-1],
        "raw_sha256": f"sha256:{raw_digest}",
        "probe_sha256": f"sha256:{'f' * 64}",
        "observation_count": len(observations),
        "observations": observations,
    }


def _reference(
    content: dict[str, object],
    *,
    content_hash: str | None = None,
    size_bytes: int | None = None,
    legacy_coverage: bool = False,
) -> CommittedBatchReference:
    dataset_id = "mtconnect:recorder-node-1:Mazak-Cell-1"
    raw_hash = str(content["raw_sha256"])[7:]
    instance_id = int(content["agent_instance_id"])
    first = int(content["first_sequence"])
    last = int(content["last_sequence"])
    batch_id = f"Mazak-Cell-1:{instance_id}:{first}:{last}:{raw_hash}"
    encoded = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return CommittedBatchReference(
        session_id="session:oslo",
        group_id="telemetry:primary",
        dataset_id=dataset_id,
        batch_id=batch_id,
        idempotency_key=f"session:oslo:{dataset_id}:{batch_id}",
        content_hash=(
            content_hash or BatchIngestRequest.calculate_content_hash(content)
        ),
        size_bytes=len(encoded) if size_bytes is None else size_bytes,
        schema_name="fcp.mtconnect.observations",
        schema_version=1,
        source_id=None if legacy_coverage else dataset_id,
        first_sequence=None if legacy_coverage else first,
        last_sequence=None if legacy_coverage else last,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider-1",),
        committed_at=datetime(2026, 8, 9, 3, 2, tzinfo=timezone.utc),
        manifest_revision=4,
        manifest_hash=f"sha256:{'b' * 64}",
    )


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_ingest_uses_hashed_paths_and_exposes_only_verified_snapshots(tmp_path):
    data_root = tmp_path / "data"
    mirror = FederatedTelemetryMirror(data_root / "federation" / "shared")
    content = _content(10, 11)
    reference = _reference(content)

    result = mirror.ingest(reference, content)

    assert result.state is TelemetryMirrorIngestState.STORED
    assert result.inserted_observations == 2
    relative = result.telemetry_path.relative_to(mirror.root)
    assert relative.parts[0] == "telemetry"
    assert relative.suffix == ".jsonl"
    assert all(":" not in part for part in relative.parts)
    assert reference.dataset_id not in relative.as_posix()
    assert reference.batch_id not in relative.as_posix()

    snapshots = _read_jsonl(result.telemetry_path)
    assert [row["sequence"] for row in snapshots] == [10, 11]
    assert snapshots[0]["schema"] == "fcp.mtconnect.snapshot.v2"
    assert snapshots[0]["Xabs"] == 10.0
    assert snapshots[1]["Xabs"] == 11.0

    for scan_root in (data_root, data_root / "federation", mirror.root):
        artifacts, warnings = scan_artifacts([str(scan_root)])
        assert warnings == []
        assert [artifact["path"] for artifact in artifacts] == [
            str(result.telemetry_path)
        ]

    source_config = data_root / "source_config"
    source_config.mkdir()
    (source_config / "credentials.json").write_text(
        '{"secret":"must-not-be-indexed"}',
        encoding="utf-8",
    )
    artifacts, warnings = scan_artifacts([str(source_config)])
    assert warnings == []
    assert artifacts == []


def test_ingest_is_idempotent_and_deduplicates_exact_sequence_overlap(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    first_content = _content(10, 11)
    first_reference = _reference(first_content)

    first = mirror.ingest(first_reference, first_content)
    again = mirror.ingest(first_reference, first_content)
    overlap_content = _content(11, 12, raw_digest="c" * 64)
    overlap = mirror.ingest(_reference(overlap_content), overlap_content)

    assert first.state is TelemetryMirrorIngestState.STORED
    assert again.state is TelemetryMirrorIngestState.ALREADY_STORED
    assert again.inserted_observations == 0
    assert again.deduplicated_observations == 2
    assert overlap.inserted_observations == 1
    assert overlap.deduplicated_observations == 1
    assert [row["sequence"] for row in _read_jsonl(overlap.telemetry_path)] == [
        10,
        11,
        12,
    ]


def test_contains_requires_the_full_immutable_identity(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    content = _content(10, 11)
    reference = _reference(content)

    assert mirror.contains(reference) is False
    mirror.ingest(reference, content)
    assert mirror.contains(reference) is True
    assert mirror.contains(_reference(content, size_bytes=reference.size_bytes + 1)) is False
    assert mirror.contains(_reference(content, content_hash=f"sha256:{'0' * 64}")) is False


def test_legacy_manifest_without_coverage_is_validated_and_derived_locally(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    content = _content(10, 11)
    legacy_reference = _reference(content, legacy_coverage=True)

    result = mirror.ingest(legacy_reference, content)

    assert result.state is TelemetryMirrorIngestState.STORED
    assert mirror.contains(legacy_reference) is True
    assert [row["sequence"] for row in _read_jsonl(result.telemetry_path)] == [10, 11]


def test_immutable_batch_conflict_is_rejected_and_quarantined(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    original = _content(10, 11)
    mirror.ingest(_reference(original), original)
    changed = _content(10, 11, values={11: 999.0})

    with pytest.raises(FederationValidationError) as caught:
        mirror.ingest(_reference(changed), changed)

    assert caught.value.code == "telemetry-mirror-batch-conflict"
    assert mirror.quarantine_count() == 1
    assert [row["Xabs"] for row in _read_jsonl(mirror.telemetry_root.glob("*.jsonl").__next__())] == [
        10.0,
        11.0,
    ]


def test_rebuild_recovers_index_and_materialization_from_verified_blobs(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    content = _content(10, 11)
    result = mirror.ingest(_reference(content), content)
    result.telemetry_path.unlink()
    with sqlite3.connect(mirror.database_path) as connection:
        connection.execute("DELETE FROM observations")

    rebuilt = mirror.rebuild()

    assert rebuilt.batches == 1
    assert rebuilt.observations == 2
    assert rebuilt.materializations == 1
    assert [row["sequence"] for row in _read_jsonl(result.telemetry_path)] == [10, 11]


def test_rejects_content_that_does_not_match_manifest_size_or_sequence(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    content = _content(10, 11)

    with pytest.raises(FederationValidationError) as size_error:
        mirror.ingest(_reference(content, size_bytes=1), content)
    assert size_error.value.code == "telemetry-size-mismatch"

    content["last_sequence"] = 12
    content["raw_batch_last_sequence"] = 12
    with pytest.raises(FederationValidationError) as sequence_error:
        mirror.ingest(_reference(content), content)
    assert sequence_error.value.code == "invalid-telemetry-sequence-range"


def test_enforces_batch_and_record_bounds_before_writing(tmp_path):
    content = _content(10, 11)
    mirror = FederatedTelemetryMirror(
        tmp_path / "shared",
        max_batch_bytes=10,
        max_records_per_batch=1,
    )

    with pytest.raises(FederationValidationError) as caught:
        mirror.ingest(_reference(content), content)

    assert caught.value.code == "telemetry-batch-too-large"
    assert list(mirror.batch_root.rglob("*.json")) == []


def test_materialization_quota_is_total_across_source_instances(tmp_path):
    mirror = FederatedTelemetryMirror(tmp_path / "shared")
    first_content = _content(10)
    first = mirror.ingest(_reference(first_content), first_content)
    mirror.max_materialization_bytes = first.telemetry_path.stat().st_size + 1
    second_content = _content(10, raw_digest="d" * 64, instance_id=78)

    with pytest.raises(FederationValidationError) as caught:
        mirror.ingest(_reference(second_content), second_content)

    assert caught.value.code == "telemetry-materialization-quota-exceeded"
    assert len(list(mirror.telemetry_root.glob("*.jsonl"))) == 1

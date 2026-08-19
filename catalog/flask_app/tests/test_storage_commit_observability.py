from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from catalog.federation.manifest import (
    AuthoritativeStorageManifest,
    DatasetManifest,
    ManifestItem,
    ManifestItemKind,
)
from catalog.federation.models import CommitState
from catalog.flask_app.services.storage_commit_observability import (
    build_storage_commit_view,
)

NOW = datetime(2026, 8, 19, 18, 0, tzinfo=timezone.utc)
SESSION_ID = "session-a"
GROUP_ID = "fcp-local-storage"
RECORDER_DATASET = "mtconnect:recorder-node:mazak"


def _batch(
    index: int,
    *,
    dataset_id: str = RECORDER_DATASET,
    schema_name: str = "fcp.mtconnect.observations",
    source_id: str | None = "mazak",
    first_sequence: int | None = None,
    last_sequence: int | None = None,
) -> ManifestItem:
    first = first_sequence if first_sequence is not None else index * 100 + 1
    last = last_sequence if last_sequence is not None else index * 100 + 100
    return ManifestItem(
        item_id=f"batch-{index}",
        kind=ManifestItemKind.BATCH,
        dataset_id=dataset_id,
        idempotency_key=f"idem-{index}",
        content_hash=f"sha256:{index + 1:064x}",
        size_bytes=1000 + index,
        schema_name=schema_name,
        schema_version=1,
        source_id=source_id,
        first_sequence=first if source_id is not None else None,
        last_sequence=last if source_id is not None else None,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider-a",),
        committed_at=NOW + timedelta(seconds=index),
    )


def _manifest(items: tuple[ManifestItem, ...]) -> AuthoritativeStorageManifest:
    datasets = []
    seen: set[str] = set()
    for item in items:
        if item.dataset_id in seen:
            continue
        seen.add(item.dataset_id)
        datasets.append(
            DatasetManifest(
                dataset_id=item.dataset_id,
                schema_name=item.schema_name,
                schema_version=item.schema_version,
                required=True,
                source_id=item.source_id,
                last_contiguous_sequence=item.last_sequence,
                missing_ranges=(),
            )
        )
    return AuthoritativeStorageManifest.build(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        revision=0,
        term=1,
        previous_manifest_hash=None,
        datasets=tuple(datasets),
        items=items,
        updated_at=NOW + timedelta(minutes=1),
    )


class _Control:
    def __init__(self, manifest: AuthoritativeStorageManifest) -> None:
        self._manifest = manifest

    def snapshot(self, session_id: str):
        assert session_id == SESSION_ID
        return SimpleNamespace(groups={GROUP_ID: object()})

    def manifest(self, session_id: str, group_id: str):
        assert session_id == SESSION_ID
        assert group_id == GROUP_ID
        return self._manifest


def test_storage_view_exposes_authoritative_recorder_commit_evidence() -> None:
    manifest = _manifest((_batch(0), _batch(1), _batch(2)))

    view = build_storage_commit_view(_Control(manifest), session_id=SESSION_ID)

    assert view["state"] == "committed"
    assert view["committed_batches"] == 3
    assert view["recorder_batches"] == 3
    assert view["dataset_count"] == 1
    assert view["latest_committed_at"] == "2026-08-19T18:00:02Z"

    dataset = view["datasets"][0]
    assert dataset["dataset_id"] == RECORDER_DATASET
    assert dataset["schema_name"] == "fcp.mtconnect.observations"
    assert dataset["batch_count"] == 3
    assert dataset["sequence_range"] == "1\u2013300"
    assert dataset["latest_sequence"] == 300
    assert dataset["latest_batch_id"] == "batch-2"
    assert dataset["latest_content_hash"] == f"sha256:{3:064x}"

    latest = view["recent_batches"][0]
    assert latest["batch_id"] == "batch-2"
    assert latest["sequence_range"] == "201\u2013300"
    assert latest["acknowledgement_count"] == 1
    assert latest["manifest_revision"] == 0


def test_storage_view_keeps_non_recorder_data_distinct() -> None:
    generic = _batch(
        8,
        dataset_id="uploaded-jsonl",
        schema_name="fcp.jsonl.file",
        source_id=None,
    )
    view = build_storage_commit_view(
        _Control(_manifest((_batch(0), generic))),
        session_id=SESSION_ID,
    )

    assert view["committed_batches"] == 2
    assert view["recorder_batches"] == 1
    assert {item["dataset_id"] for item in view["datasets"]} == {
        RECORDER_DATASET,
        "uploaded-jsonl",
    }
    generic_view = next(
        item for item in view["datasets"] if item["dataset_id"] == "uploaded-jsonl"
    )
    assert generic_view["is_recorder"] is False
    assert generic_view["sequence_range"] is None


def test_storage_view_reports_no_commits_without_fabricating_delivery() -> None:
    view = build_storage_commit_view(
        _Control(_manifest(())),
        session_id=SESSION_ID,
    )

    assert view["state"] == "empty"
    assert view["committed_batches"] == 0
    assert view["recorder_batches"] == 0
    assert view["datasets"] == []
    assert view["recent_batches"] == []
    assert "Recorder-local capture does not count" in view["message"]


def test_storage_view_is_bounded_to_twenty_recent_batches_by_default() -> None:
    items = tuple(_batch(index) for index in range(30))
    view = build_storage_commit_view(_Control(_manifest(items)), session_id=SESSION_ID)

    assert view["committed_batches"] == 30
    assert len(view["recent_batches"]) == 20
    assert view["recent_batches"][0]["batch_id"] == "batch-29"
    assert view["recent_batches"][-1]["batch_id"] == "batch-10"


def test_storage_view_does_not_expose_backend_or_idempotency_fields() -> None:
    view = build_storage_commit_view(
        _Control(_manifest((_batch(0),))),
        session_id=SESSION_ID,
    )
    rendered = repr(view).lower()

    assert "idempotency" not in rendered
    assert "relative_path" not in rendered
    assert "storage-index.sqlite3" not in rendered
    assert "/app/" not in rendered

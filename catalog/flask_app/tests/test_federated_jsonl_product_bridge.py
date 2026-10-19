from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.manifest import ManifestItemKind
from catalog.federation.manifest_store import (
    AuthoritativeManifestStore,
    ManifestItemProposal,
)
from catalog.federation.models import CommitState
from catalog.federation.shared_file_storage import (
    FEDERATED_JSONL_CHUNK_BYTES,
    FEDERATED_JSONL_CONTENT_SCHEMA,
    FEDERATED_JSONL_DATASET_SCHEMA_NAME,
    FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
    federated_jsonl_batch_id,
    federated_jsonl_dataset_id,
    federated_jsonl_idempotency_key,
    validate_federated_jsonl_ingest,
)
from catalog.federation.storage_catalog import CommittedBatchReference
from catalog.federation.storage_control_plane import StorageControlPlaneStore
from catalog.federation.storage_protocol import BatchIngestRequest
from catalog.flask_app.services.federated_jsonl_product_bridge import (
    FederatedJsonlProductBridge,
    FederatedJsonlPublishResult,
)

_MANIFEST_HASH = "sha256:" + "a" * 64


def _bridge(
    tmp_path: Path,
    name: str,
    *,
    relay_runtime: object | None = None,
) -> FederatedJsonlProductBridge:
    data_root = tmp_path / name / "data"
    app = Flask(name)
    bridge = FederatedJsonlProductBridge(
        app,
        SimpleNamespace(relay_runtime=relay_runtime),
        data_root=data_root,
        database=tmp_path / name / "state.sqlite3",
        cache_root=tmp_path / name / "cache",
        mirror_root=data_root / "federation" / "shared" / "jsonl-files",
    )
    bridge._ensure_initialized()
    return bridge


class _PublishingRuntime:
    def __init__(self, *, committed: bool = True) -> None:
        self.calls: list[dict[str, object]] = []
        self.committed = committed

    def publish_federated_batches(
        self,
        runtime_state: object,
        *,
        session_id: str,
        authority_node_id: str,
        batches: list[dict[str, object]],
    ) -> tuple[object, ...]:
        entries = tuple(batches)
        self.calls.append(
            {
                "runtime_state": runtime_state,
                "session_id": session_id,
                "authority_node_id": authority_node_id,
                "batches": entries,
            }
        )
        return tuple(
            SimpleNamespace(committed=self.committed) for _entry in entries
        )


def _trusted_scope(
    *,
    session_id: str,
    node_id: str,
) -> tuple[object, object]:
    runtime_state = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id=session_id)
    )
    context = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id=session_id),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=node_id)),
    )
    return runtime_state, context


def _reference(batch: dict[str, object], *, session_id: str) -> CommittedBatchReference:
    content = batch["content"]
    assert isinstance(content, dict)
    encoded = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return CommittedBatchReference(
        session_id=session_id,
        group_id=str(batch["group_id"]),
        dataset_id=str(batch["dataset_id"]),
        batch_id=str(batch["batch_id"]),
        idempotency_key=str(batch["idempotency_key"]),
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        size_bytes=len(encoded),
        schema_name=str(batch["dataset_schema_name"]),
        schema_version=int(batch["dataset_schema_version"]),
        source_id=None,
        first_sequence=None,
        last_sequence=None,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("storage-a",),
        committed_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        manifest_revision=1,
        manifest_hash=_MANIFEST_HASH,
    )


def test_publisher_only_pass_commits_normal_jsonl_with_existing_contract(
    tmp_path: Path,
):
    runtime = _PublishingRuntime()
    bridge = _bridge(tmp_path, "headless", relay_runtime=runtime)
    session_id = "session-headless-jsonl"
    node_id = "node-headless"
    authority_node_id = "node-storage"
    group_id = "storage-1"
    relative_path = Path("sources") / "observer_phoenix" / "jsonl" / "day.jsonl"
    source = bridge.data_root / relative_path
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('{"machine_id":"observer-a","value":1}\n', encoding="utf-8")
    runtime_state, context = _trusted_scope(
        session_id=session_id,
        node_id=node_id,
    )

    result = bridge.publish_local_once(
        runtime_state,
        context,
        authority_node_id=authority_node_id,
        group_id=group_id,
    )

    assert result == FederatedJsonlPublishResult(
        session_id=session_id,
        node_id=node_id,
        authority_node_id=authority_node_id,
        group_id=group_id,
        published_chunks=1,
    )
    assert len(runtime.calls) == 1
    call = runtime.calls[0]
    assert call["runtime_state"] is runtime_state
    assert call["session_id"] == session_id
    assert call["authority_node_id"] == authority_node_id
    batches = call["batches"]
    assert isinstance(batches, tuple) and len(batches) == 1
    batch = batches[0]
    assert isinstance(batch, dict)
    content = batch["content"]
    assert isinstance(content, dict)
    expected_dataset_id = federated_jsonl_dataset_id(
        node_id,
        relative_path.as_posix(),
    )
    assert batch["group_id"] == group_id
    assert batch["dataset_id"] == expected_dataset_id
    assert batch["dataset_schema_name"] == FEDERATED_JSONL_DATASET_SCHEMA_NAME
    assert batch["dataset_schema_version"] == FEDERATED_JSONL_DATASET_SCHEMA_VERSION
    assert batch["idempotency_key"] == federated_jsonl_idempotency_key(
        session_id,
        expected_dataset_id,
        str(batch["batch_id"]),
    )
    assert content["schema"] == FEDERATED_JSONL_CONTENT_SCHEMA
    assert content["producer_node_id"] == node_id
    assert content["relative_path"] == relative_path.as_posix()

    repeated = bridge.publish_local_once(
        runtime_state,
        context,
        authority_node_id=authority_node_id,
        group_id=group_id,
    )

    assert repeated.published_chunks == 0
    assert len(runtime.calls) == 1


def test_identical_files_in_distinct_datasets_commit_and_advance_each_source(
    tmp_path: Path,
) -> None:
    session_id = "session-identical-files"
    node_id = "node-identical-files"
    group_id = "storage-identical-files"
    database = tmp_path / "authority.sqlite3"
    control = StorageControlPlaneStore(database)
    revision = control.create_group(session_id, "coordinator", group_id).revision
    store = AuthoritativeManifestStore(database)
    store.ensure_genesis(
        session_id,
        group_id,
        term=0,
        expected_control_revision=revision,
        now=datetime(2026, 8, 12, tzinfo=timezone.utc),
    )

    class ManifestRuntime:
        def publish_federated_batches(self, _state, **kwargs):
            outcomes = []
            for batch in kwargs["batches"]:
                content = batch["content"]
                validate_federated_jsonl_ingest(
                    actor_node_id=node_id,
                    session_id=session_id,
                    dataset_id=batch["dataset_id"],
                    batch_id=batch["batch_id"],
                    idempotency_key=batch["idempotency_key"],
                    schema_name=batch["dataset_schema_name"],
                    schema_version=batch["dataset_schema_version"],
                    content=content,
                )
                committed = store.commit(
                    ManifestItemProposal(
                        session_id=session_id,
                        group_id=group_id,
                        item_id=batch["batch_id"],
                        kind=ManifestItemKind.BATCH,
                        dataset_id=batch["dataset_id"],
                        idempotency_key=batch["idempotency_key"],
                        content_hash=BatchIngestRequest.calculate_content_hash(content),
                        size_bytes=len(json.dumps(content).encode()),
                        schema_name=batch["dataset_schema_name"],
                        schema_version=batch["dataset_schema_version"],
                        source_id=None,
                        first_sequence=None,
                        last_sequence=None,
                        dataset_required=True,
                        acknowledged_provider_ids=("storage-a",),
                        term=1,
                        expected_control_revision=revision,
                        committed_at=batch["created_at"],
                    )
                )
                assert any(
                    item.dataset_id == batch["dataset_id"]
                    and item.item_id == batch["batch_id"]
                    for item in committed.manifest.items
                )
                outcomes.append(SimpleNamespace(committed=True))
            return tuple(outcomes)

    bridge = _bridge(tmp_path, "identical-files", relay_runtime=ManifestRuntime())
    for name in ("first", "second"):
        source = bridge.data_root / "uploads" / name / "input.jsonl"
        source.parent.mkdir(parents=True)
        source.write_text('{"machine_id":"same","value":1}\n', encoding="utf-8")
    state, context = _trusted_scope(session_id=session_id, node_id=node_id)

    result = bridge.publish_local_once(
        state, context, authority_node_id="node-storage", group_id=group_id
    )

    assert result.published_chunks == 2
    assert len(store.head(session_id, group_id).items) == 2
    with bridge._connect() as connection:
        rows = connection.execute(
            "SELECT next_chunk,chunk_count,published_at FROM local_files"
        ).fetchall()
    assert len(rows) == 2
    assert all(row["next_chunk"] == row["chunk_count"] == 1 for row in rows)
    assert all(row["published_at"] is not None for row in rows)
    assert bridge.publish_local_once(
        state, context, authority_node_id="node-storage", group_id=group_id
    ).published_chunks == 0


def test_publisher_only_pass_excludes_recorder_and_federation_jsonl(tmp_path: Path):
    runtime = _PublishingRuntime()
    bridge = _bridge(tmp_path, "headless-exclusions", relay_runtime=runtime)
    recorder = (
        bridge.data_root
        / "sources"
        / "mtconnect_recorder"
        / "jsonl"
        / "machine-a"
        / "recorder.jsonl"
    )
    mirrored = (
        bridge.data_root
        / "federation"
        / "shared"
        / "jsonl-files"
        / "node-peer"
        / "mirrored.jsonl"
    )
    for path in (recorder, mirrored):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"value":1}\n', encoding="utf-8")
    runtime_state, context = _trusted_scope(
        session_id="session-headless-exclusions",
        node_id="node-headless",
    )

    result = bridge.publish_local_once(
        runtime_state,
        context,
        authority_node_id="node-storage",
        group_id="storage-1",
    )

    assert result.published_chunks == 0
    assert runtime.calls == []


def test_publisher_only_pass_fails_closed_when_storage_does_not_commit(
    tmp_path: Path,
) -> None:
    runtime = _PublishingRuntime(committed=False)
    bridge = _bridge(tmp_path, "headless-pending", relay_runtime=runtime)
    source = bridge.data_root / "uploads" / "pending.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('{"value":1}\n', encoding="utf-8")
    runtime_state, context = _trusted_scope(
        session_id="session-headless-pending",
        node_id="node-headless",
    )

    with pytest.raises(FederationOperationError) as excinfo:
        bridge.publish_local_once(
            runtime_state,
            context,
            authority_node_id="node-storage",
            group_id="storage-1",
        )

    assert excinfo.value.code == "federated-jsonl-publication-pending"


def test_remote_upload_materializes_into_unchanged_legacy_jsonl_scanner(tmp_path: Path):
    source = _bridge(tmp_path, "source")
    target = _bridge(tmp_path, "target")
    session_id = "session-federated-jsonl-test"
    source_node = "node-source"
    target_node = "node-target"

    upload = source.data_root / "uploads" / "upload-1" / "measurements.jsonl"
    upload.parent.mkdir(parents=True, exist_ok=True)
    original = (
        b'{"timestamp":"2026-08-12T00:00:00Z","machine_id":"A","sequence":1}\n'
        b'{"timestamp":"2026-08-12T00:00:01Z","machine_id":"A","sequence":2}\n'
    )
    upload.write_bytes(original)

    recorder = (
        source.data_root
        / "sources"
        / "mtconnect_recorder"
        / "jsonl"
        / "machine"
        / "1"
        / "2026-08-12"
        / "recorder.jsonl"
    )
    recorder.parent.mkdir(parents=True, exist_ok=True)
    recorder.write_text('{"timestamp":"2026-08-12T00:00:00Z"}\n', encoding="utf-8")

    candidates = {relative for relative, _path in source._local_candidates()}
    assert "uploads/upload-1/measurements.jsonl" in candidates
    assert not any(value.startswith("sources/mtconnect_recorder/jsonl/") for value in candidates)

    source._prepare_local_rows(source_node)
    entries = source._pending_publish_entries(
        session_id=session_id,
        node_id=source_node,
        group_id="storage-1",
        maximum=128,
    )
    assert entries

    materialized = False
    for batch, _relative_path, _index in entries:
        reference = _reference(batch, session_id=session_id)
        materialized = (
            target._ingest_remote(
                reference,
                batch["content"],
                local_node_id=target_node,
            )
            or materialized
        )

    assert materialized is True
    mirrored = list(target.mirror_root.rglob("*.jsonl"))
    assert len(mirrored) == 1
    assert mirrored[0].read_bytes() == original

    discovered = list(iter_jsonl_files(target.data_root, recursive=True))
    assert mirrored[0] in discovered
    records = list(iter_jsonl_records(mirrored[0]))
    assert [record["sequence"] for record in records] == [1, 2]


@pytest.mark.parametrize("defer_materialization", [False, True])
def test_legacy_and_scoped_chunk_aliases_materialize_one_complete_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, defer_materialization: bool
) -> None:
    runtime = _PublishingRuntime()
    source = _bridge(tmp_path, "alias-source", relay_runtime=runtime)
    target = _bridge(tmp_path, "alias-target")
    session_id = "session-chunk-aliases"
    source_node = "node-alias-source"
    original = (
        json.dumps({
            "value": random.Random(2009).randbytes(FEDERATED_JSONL_CHUNK_BYTES * 2).hex()
        }) + "\n"
    ).encode()
    upload = source.data_root / "uploads" / "aliases.jsonl"
    upload.parent.mkdir(parents=True)
    upload.write_bytes(original)
    state, context = _trusted_scope(session_id=session_id, node_id=source_node)
    for _ in range(128):
        if source.publish_local_once(
            state, context, authority_node_id="node-storage", group_id="storage-1"
        ).published_chunks == 0:
            break
    batches = [batch for call in runtime.calls for batch in call["batches"]]
    assert len(batches) >= 2
    aliases = []
    for batch in batches:
        content = batch["content"]
        legacy_id = federated_jsonl_batch_id(
            str(content["file_sha256"]),
            int(content["chunk_index"]),
            str(content["chunk_sha256"]),
        )
        assert legacy_id != batch["batch_id"]
        legacy = dict(batch, batch_id=legacy_id)
        legacy["idempotency_key"] = federated_jsonl_idempotency_key(
            session_id, str(batch["dataset_id"]), legacy_id
        )
        aliases.extend((legacy, batch))

    materialized = 0
    with monkeypatch.context() as delayed:
        if defer_materialization:
            delayed.setattr(target, "_try_materialize", lambda **_kwargs: False)
        for batch in aliases:
            materialized += target._ingest_remote(
                _reference(batch, session_id=session_id),
                batch["content"],
                local_node_id="node-alias-target",
            )
            if int(batch["content"]["chunk_index"]) == 0:
                assert list(target.mirror_root.rglob("*.jsonl")) == []
    if defer_materialization:
        materialized += target._retry_staged_materializations(session_id=session_id)

    assert materialized == 1
    mirrored = list(target.mirror_root.rglob("*.jsonl"))
    assert len(mirrored) == 1
    assert mirrored[0].read_bytes() == original
    with target._connect() as connection:
        rows = connection.execute("SELECT chunk_path FROM seen_batches").fetchall()
    assert len(rows) == 2 * len(batches)
    assert all(row["chunk_path"] is None for row in rows)


@pytest.mark.parametrize("conflict_field", ["group_id", "source_mtime_ns"])
def test_chunk_aliases_reject_contradictory_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conflict_field: str
) -> None:
    runtime = _PublishingRuntime()
    source = _bridge(tmp_path, "conflict-source", relay_runtime=runtime)
    target = _bridge(tmp_path, "conflict-target")
    session_id = "session-alias-conflict"
    state, context = _trusted_scope(session_id=session_id, node_id="node-source")
    source_file = source.data_root / "uploads" / "input.jsonl"
    source_file.parent.mkdir(parents=True)
    source_file.write_text('{"value":1}\n', encoding="utf-8")
    assert source.publish_local_once(
        state, context, authority_node_id="node-storage", group_id="storage-1"
    ).published_chunks == 1
    batch = runtime.calls[0]["batches"][0]
    content = batch["content"]
    legacy_id = federated_jsonl_batch_id(
        str(content["file_sha256"]), 0, str(content["chunk_sha256"])
    )
    legacy = dict(batch, batch_id=legacy_id)
    legacy["idempotency_key"] = federated_jsonl_idempotency_key(
        session_id, str(batch["dataset_id"]), legacy_id
    )
    with monkeypatch.context() as delayed:
        delayed.setattr(target, "_try_materialize", lambda **_kwargs: False)
        assert not target._ingest_remote(
            _reference(legacy, session_id=session_id),
            content,
            local_node_id="node-target",
        )
    conflicting = dict(batch, content=dict(content))
    if conflict_field == "group_id":
        conflicting["group_id"] = "another-group"
    else:
        conflicting["content"]["source_mtime_ns"] += 1
    with pytest.raises(FederationValidationError) as invalid:
        target._ingest_remote(
            _reference(conflicting, session_id=session_id),
            conflicting["content"],
            local_node_id="node-target",
        )
    assert invalid.value.code == "federated-jsonl-version-conflict"
    assert list(target.mirror_root.rglob("*.jsonl")) == []


def test_own_published_file_is_not_mirrored_back_as_a_duplicate(tmp_path: Path):
    bridge = _bridge(tmp_path, "self")
    session_id = "session-self-jsonl-test"
    node_id = "node-self"
    source = bridge.data_root / "uploads" / "self.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('{"timestamp":"2026-08-12T00:00:00Z"}\n', encoding="utf-8")

    bridge._prepare_local_rows(node_id)
    entries = bridge._pending_publish_entries(
        session_id=session_id,
        node_id=node_id,
        group_id="storage-1",
        maximum=128,
    )
    for batch, _relative_path, _index in entries:
        assert (
            bridge._ingest_remote(
                _reference(batch, session_id=session_id),
                batch["content"],
                local_node_id=node_id,
            )
            is False
        )

    assert list(bridge.mirror_root.rglob("*.jsonl")) == []


def test_same_relative_path_from_two_producers_is_materialized_without_collision(
    tmp_path: Path,
):
    first = _bridge(tmp_path, "first")
    second = _bridge(tmp_path, "second")
    target = _bridge(tmp_path, "target")
    session_id = "session-two-jsonl-producers"
    relative_path = Path("exports") / "shared.jsonl"
    payloads = {
        "node-first": b'{"machine_id":"first","sequence":1}\n',
        "node-second": b'{"machine_id":"second","sequence":1}\n',
    }

    for bridge, (node_id, payload) in zip(
        (first, second), payloads.items(), strict=True
    ):
        source = bridge.data_root / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(payload)
        bridge._prepare_local_rows(node_id)
        entries = bridge._pending_publish_entries(
            session_id=session_id,
            node_id=node_id,
            group_id="storage-1",
            maximum=128,
        )
        assert len(entries) == 1
        batch, published_path, _index = entries[0]
        assert published_path == relative_path.as_posix()
        assert target._ingest_remote(
            _reference(batch, session_id=session_id),
            batch["content"],
            local_node_id="node-target",
        )

    mirrored = sorted(target.mirror_root.rglob("shared.jsonl"))
    assert len(mirrored) == 2
    assert {path.read_bytes() for path in mirrored} == set(payloads.values())
    producer_directories = {
        path.relative_to(target.mirror_root).parts[0] for path in mirrored
    }
    assert len(producer_directories) == 2
    assert all(
        path.relative_to(target.mirror_root).parts[1:] == relative_path.parts
        for path in mirrored
    )

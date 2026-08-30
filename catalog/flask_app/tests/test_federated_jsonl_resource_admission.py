from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest
from flask import Flask

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.host_resources import HostResourceRefused
from catalog.federation.models import CommitState
from catalog.federation.storage_catalog import CommittedBatchReference
from catalog.federation.storage_protocol import BatchIngestRequest
from catalog.flask_app.services.federated_jsonl_product_bridge import (
    FederatedJsonlProductBridge,
)

_MANIFEST_HASH = "sha256:" + "a" * 64


class _RecordingAdmission:
    def __init__(
        self,
        *,
        fail_call: int | None = None,
        changed_resource_assessment: int | None = None,
    ) -> None:
        self.fail_call = fail_call
        self.changed_resource_assessment = changed_resource_assessment
        self.calls: list[tuple[Path, int, int]] = []
        self.many_calls: list[tuple[tuple[Path, int, int], ...]] = []
        self.assessment_calls = 0
        self.active = 0
        self.max_active = 0

    @contextmanager
    def reserve(
        self,
        path: Path,
        *,
        bytes_required: int,
        inodes_required: int = 0,
    ) -> Iterator[object]:
        self.calls.append((Path(path), bytes_required, inodes_required))
        if self.fail_call == len(self.calls):
            raise HostResourceRefused("resource_pressure", SimpleNamespace())
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            yield SimpleNamespace(resource_id="test-resource")
        finally:
            self.active -= 1

    @contextmanager
    def reserve_many(
        self, requirements: tuple[tuple[Path, int, int], ...]
    ) -> Iterator[tuple[object, ...]]:
        requested = tuple(
            (Path(path), bytes_required, inodes_required)
            for path, bytes_required, inodes_required in requirements
        )
        self.many_calls.append(requested)
        first_call = len(self.calls) + 1
        self.calls.extend(requested)
        last_call = len(self.calls)
        if (
            self.fail_call is not None
            and first_call <= self.fail_call <= last_call
        ):
            raise HostResourceRefused("resource_pressure", SimpleNamespace())
        self.active += len(requested)
        self.max_active = max(self.max_active, self.active)
        try:
            # The fake models the common same-filesystem case, so atomic
            # admission coalesces both requirements into one resource.
            yield (SimpleNamespace(resource_id="test-resource"),)
        finally:
            self.active -= len(requested)

    def assessment(self, path: Path) -> object:
        del path
        self.assessment_calls += 1
        resource_id = (
            "changed-resource"
            if self.changed_resource_assessment == self.assessment_calls
            else "test-resource"
        )
        return SimpleNamespace(resource_id=resource_id)


class _PublishingRuntime:
    def __init__(self) -> None:
        self.batches: list[dict[str, object]] = []

    def publish_federated_batches(
        self,
        runtime_state: object,
        *,
        session_id: str,
        authority_node_id: str,
        batches: list[dict[str, object]],
    ) -> tuple[object, ...]:
        del runtime_state, session_id, authority_node_id
        self.batches.extend(batches)
        return tuple(SimpleNamespace(committed=True) for _ in batches)


def _bridge(
    tmp_path: Path,
    name: str,
    *,
    admission: _RecordingAdmission,
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
        resource_admission=admission,  # type: ignore[arg-type]
    )
    pending_failure = admission.fail_call
    admission.fail_call = None
    bridge._ensure_initialized()
    admission.calls.clear()
    admission.many_calls.clear()
    admission.assessment_calls = 0
    admission.active = 0
    admission.max_active = 0
    admission.fail_call = pending_failure
    return bridge


def _trusted_scope(session_id: str, node_id: str) -> tuple[object, object]:
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
        committed_at=datetime(2026, 8, 30, tzinfo=timezone.utc),
        manifest_revision=1,
        manifest_hash=_MANIFEST_HASH,
    )


def _published_batch(
    tmp_path: Path,
    *,
    payload: bytes = b'{"machine_id":"m","value":1}\n',
) -> tuple[dict[str, object], str]:
    runtime = _PublishingRuntime()
    admission = _RecordingAdmission()
    bridge = _bridge(
        tmp_path,
        "producer",
        admission=admission,
        relay_runtime=runtime,
    )
    source = bridge.data_root / "sources" / "demo" / "day.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(payload)
    session_id = "session-jsonl-resource"
    runtime_state, context = _trusted_scope(session_id, "node-producer")
    result = bridge.publish_local_once(
        runtime_state,
        context,
        authority_node_id="node-storage",
        group_id="storage-1",
    )
    assert result.published_chunks == 1
    assert len(runtime.batches) == 1
    return runtime.batches[0], session_id


def test_local_gzip_cache_refuses_before_creating_temp_output(tmp_path: Path) -> None:
    admission = _RecordingAdmission(fail_call=1)
    runtime = _PublishingRuntime()
    bridge = _bridge(
        tmp_path,
        "local-refusal",
        admission=admission,
        relay_runtime=runtime,
    )
    source = bridge.data_root / "sources" / "demo" / "day.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('{"value":1}\n', encoding="utf-8")
    runtime_state, context = _trusted_scope("session-local", "node-local")

    with pytest.raises(FederationOperationError) as captured:
        bridge.publish_local_once(
            runtime_state,
            context,
            authority_node_id="node-storage",
            group_id="storage-1",
        )

    assert captured.value.code == "federated-jsonl-resource-pressure"
    assert admission.calls[0][0] == bridge.cache_root
    assert list(bridge.cache_root.iterdir()) == []
    assert runtime.batches == []


def test_remote_chunk_staging_refuses_before_temp_write(tmp_path: Path) -> None:
    admission = _RecordingAdmission(fail_call=1)
    bridge = _bridge(tmp_path, "chunk-refusal", admission=admission)
    target = bridge.cache_root / "remote" / "hash" / "00000000.chunk"
    target.parent.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FederationOperationError) as captured:
        bridge._write_chunk(target, b"bounded chunk")

    assert captured.value.code == "federated-jsonl-resource-pressure"
    assert admission.calls == [(target.parent, len(b"bounded chunk"), 2)]
    assert list(target.parent.iterdir()) == []


def test_remote_chunk_refuses_if_backing_resource_changes_after_mkdir(
    tmp_path: Path,
) -> None:
    admission = _RecordingAdmission(changed_resource_assessment=1)
    bridge = _bridge(tmp_path, "chunk-resource-change", admission=admission)
    target = bridge.cache_root / "remote" / "hash" / "00000000.chunk"

    with pytest.raises(FederationOperationError) as captured:
        bridge._write_chunk(target, b"bounded chunk")

    assert captured.value.code == "federated-jsonl-resource-changed"
    assert target.parent.is_dir()
    assert list(target.parent.iterdir()) == []
    assert admission.active == 0


def test_materialization_reserves_encoded_and_raw_peaks_together(tmp_path: Path) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission()
    consumer = _bridge(tmp_path, "consumer", admission=admission)

    materialized = consumer._ingest_remote(
        _reference(batch, session_id=session_id),
        content,
        local_node_id="node-consumer",
    )

    assert materialized is True
    payload_calls = [call for call in admission.calls if call[0] != consumer.database.parent]
    assert len(payload_calls) == 3
    assert len(admission.many_calls) == 1
    assert len(admission.many_calls[0]) == 2
    chunk_call, encoded_call, raw_call = payload_calls
    assert chunk_call[1] <= int(content["encoded_size"])
    assert encoded_call[0] == consumer.cache_root
    assert encoded_call[1] == int(content["encoded_size"])
    assert raw_call[1] == int(content["file_size"])
    # encoded + raw remain admitted while SQLite bookkeeping is admitted.
    assert admission.max_active == 3
    targets = list(consumer.mirror_root.rglob("*.jsonl"))
    assert len(targets) == 1
    assert targets[0].read_bytes() == b'{"machine_id":"m","value":1}\n'


def test_materialization_revalidates_target_after_directory_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission()
    consumer = _bridge(tmp_path, "target-revalidation", admission=admission)
    original_target_path = consumer._target_path
    calls = 0

    def guarded_target_path(producer: str, relative_path: str) -> Path:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise FederationValidationError(
                "unsafe-federated-jsonl-target",
                "content.relative_path",
                "simulated target escape after directory creation",
            )
        return original_target_path(producer, relative_path)

    monkeypatch.setattr(consumer, "_target_path", guarded_target_path)

    with pytest.raises(FederationValidationError) as captured:
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )

    assert captured.value.code == "unsafe-federated-jsonl-target"
    assert calls == 2
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []
    assert admission.active == 0


def test_materialization_refuses_if_backing_resource_changes_after_mkdir(
    tmp_path: Path,
) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    # Assessment 1 validates remote chunk staging. Assessment 2 occurs after the
    # mirror parent is created but before reconstruction starts.
    admission = _RecordingAdmission(changed_resource_assessment=2)
    consumer = _bridge(tmp_path, "materialization-resource-change", admission=admission)

    with pytest.raises(FederationOperationError) as captured:
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )

    assert captured.value.code == "federated-jsonl-resource-changed"
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []
    assert admission.active == 0


def test_mirror_quota_refuses_before_reconstruction_temp_files(tmp_path: Path) -> None:
    payload = b'{"machine_id":"m","value":"' + (b"x" * 2048) + b'"}\n'
    batch, session_id = _published_batch(tmp_path, payload=payload)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission()
    consumer = _bridge(tmp_path, "quota", admission=admission)
    consumer.app.config["FEDERATED_JSONL_MAX_MIRROR_BYTES"] = len(payload) - 1

    materialized = consumer._ingest_remote(
        _reference(batch, session_id=session_id),
        content,
        local_node_id="node-consumer",
    )

    assert materialized is False
    # The one admitted write is the retained remote chunk. The quota check runs
    # before either reconstruction temp file is created, so there are no second
    # and third reservations for encoded/raw materialization.
    payload_calls = [call for call in admission.calls if call[0] != consumer.database.parent]
    assert len(payload_calls) == 1
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []
    snapshot = consumer.snapshot()
    assert snapshot["mirror_quota_reached"] is True


def test_materialization_atomic_admission_refuses_without_partial_reservation(
    tmp_path: Path,
) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    # Call 1 is chunk staging. Calls 2 and 3 are submitted in one reserve_many
    # transaction, so refusing the raw requirement must publish neither one.
    admission = _RecordingAdmission(fail_call=4)
    consumer = _bridge(tmp_path, "unwind", admission=admission)

    with pytest.raises(FederationOperationError) as captured:
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )

    assert captured.value.code == "federated-jsonl-resource-pressure"
    assert admission.active == 0
    assert list(consumer.cache_root.glob("tmp*")) == []
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []


def test_staged_materialization_retries_without_rediscovering_seen_batch(
    tmp_path: Path,
) -> None:
    batch, session_id = _published_batch(tmp_path)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission(fail_call=4)
    consumer = _bridge(tmp_path, "retry-staged", admission=admission)

    with pytest.raises(FederationOperationError) as captured:
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )
    assert captured.value.code == "federated-jsonl-resource-pressure"
    assert consumer._seen(_reference(batch, session_id=session_id)) is True
    assert list(consumer.mirror_root.rglob("*.jsonl")) == []

    admission.fail_call = None
    assert consumer._retry_staged_materializations(session_id=session_id) == 1
    targets = list(consumer.mirror_root.rglob("*.jsonl"))
    assert len(targets) == 1
    assert targets[0].read_bytes() == b'{"machine_id":"m","value":1}\n'
    with consumer._connect() as connection:
        staged = connection.execute(
            "SELECT COUNT(*) AS n FROM seen_batches WHERE chunk_path IS NOT NULL"
        ).fetchone()
    assert int(staged["n"]) == 0


def test_retry_reconciles_valid_replaced_target_without_reconstruction(
    tmp_path: Path,
) -> None:
    payload = b'{"machine_id":"m","value":1}\n'
    batch, session_id = _published_batch(tmp_path, payload=payload)
    content = batch["content"]
    assert isinstance(content, dict)
    admission = _RecordingAdmission(fail_call=4)
    consumer = _bridge(tmp_path, "reconcile-target", admission=admission)

    with pytest.raises(FederationOperationError):
        consumer._ingest_remote(
            _reference(batch, session_id=session_id),
            content,
            local_node_id="node-consumer",
        )
    producer = str(content["producer_node_id"])
    relative_path = str(content["relative_path"])
    target = consumer._target_path(producer, relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    many_calls_before = len(admission.many_calls)

    admission.fail_call = None
    assert consumer._retry_staged_materializations(session_id=session_id) == 1
    assert len(admission.many_calls) == many_calls_before
    with consumer._connect() as connection:
        row = connection.execute(
            "SELECT file_sha256,target_path,size_bytes FROM materialized_files "
            "WHERE session_id=? AND dataset_id=?",
            (session_id, str(batch["dataset_id"])),
        ).fetchone()
        staged = connection.execute(
            "SELECT COUNT(*) AS n FROM seen_batches WHERE chunk_path IS NOT NULL"
        ).fetchone()
    assert row is not None
    assert str(row["file_sha256"]) == str(content["file_sha256"])
    assert Path(str(row["target_path"])) == target
    assert int(row["size_bytes"]) == len(payload)
    assert int(staged["n"]) == 0


def test_retry_ignores_incomplete_staged_versions(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    consumer = _bridge(tmp_path, "retry-incomplete", admission=admission)
    with consumer._connect() as connection:
        connection.execute(
            """
            INSERT INTO seen_batches(
                session_id,group_id,dataset_id,batch_id,producer_node_id,
                relative_path,file_sha256,encoded_sha256,file_size,encoded_size,
                source_mtime_ns,chunk_index,chunk_count,chunk_sha256,chunk_path,
                committed_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "session-incomplete","storage-1","dataset-incomplete","batch-0",
                "node-remote","sources/demo/day.jsonl","sha256:" + "1" * 64,
                "sha256:" + "2" * 64,10,10,1,0,2,"sha256:" + "3" * 64,
                str(consumer.cache_root / "missing.chunk"),
                "2026-08-30T00:00:00+00:00",
            ),
        )
    assert consumer._retry_staged_materializations(session_id="session-incomplete") == 0
    assert admission.many_calls == []


def test_bootstrap_refuses_before_creating_bridge_directories(tmp_path: Path) -> None:
    admission = _RecordingAdmission(fail_call=1)
    root = tmp_path / "bootstrap-refusal"
    app = Flask("bootstrap-refusal")
    bridge = FederatedJsonlProductBridge(
        app, SimpleNamespace(relay_runtime=None), data_root=root / "data",
        database=root / "state" / "sync.sqlite3", cache_root=root / "cache",
        mirror_root=root / "mirror", resource_admission=admission,
    )
    with pytest.raises(FederationOperationError) as captured:
        bridge._ensure_initialized()
    assert captured.value.code == "federated-jsonl-resource-pressure"
    assert not root.exists()


def test_sqlite_state_mutation_uses_resource_admission(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    bridge = _bridge(tmp_path, "sqlite-write", admission=admission)
    with bridge._write_connection() as connection:
        connection.execute(
            "INSERT INTO local_files(relative_path,size_bytes,mtime_ns,file_sha256,encoded_sha256,encoded_size,dataset_id,chunk_count,next_chunk,cache_path,published_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            ("x.jsonl",1,1,"sha256:" + "1"*64,"sha256:" + "2"*64,1,"d",1,0,"x",None),
        )
    assert len(admission.calls) == 1
    assert admission.calls[0][0] == bridge.database.parent
    assert admission.calls[0][1] > 0
    assert admission.calls[0][2] >= 3


def test_staged_cache_quota_counts_orphaned_chunk_bytes(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    bridge = _bridge(tmp_path, "staged-quota", admission=admission)
    bridge.app.config["FEDERATED_JSONL_MAX_STAGED_BYTES"] = 8
    orphan = bridge.cache_root / "remote" / "orphan" / "00000000.chunk"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"12345678")
    target = bridge.cache_root / "remote" / "new" / "00000000.chunk"
    with pytest.raises(FederationOperationError) as captured:
        bridge._write_chunk(target, b"x")
    assert captured.value.code == "federated-jsonl-staged-cache-full"
    assert not target.exists()


def test_orphan_cleanup_removes_unreferenced_staged_chunks(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    bridge = _bridge(tmp_path, "orphan-cleanup", admission=admission)
    orphan = bridge.cache_root / "remote" / "orphan" / "00000000.chunk"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"orphan")
    assert bridge._cleanup_orphaned_staged_chunks() == 1
    assert not orphan.exists()


def test_staged_cache_file_quota_bounds_tiny_chunks(tmp_path: Path) -> None:
    admission = _RecordingAdmission()
    bridge = _bridge(tmp_path, "staged-files", admission=admission)
    bridge.app.config["FEDERATED_JSONL_MAX_STAGED_FILES"] = 2
    remote = bridge.cache_root / "remote"
    for index in range(2):
        staged = remote / f"existing-{index}" / "00000000.chunk"
        staged.parent.mkdir(parents=True, exist_ok=True)
        staged.write_bytes(b"x")
    target = remote / "new" / "00000000.chunk"
    with pytest.raises(FederationOperationError) as captured:
        bridge._write_chunk(target, b"x")
    assert captured.value.code == "federated-jsonl-staged-cache-full"
    assert not target.exists()

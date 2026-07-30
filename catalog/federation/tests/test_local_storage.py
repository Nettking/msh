from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from catalog.federation.local_storage import FilesystemBatchStorageProvider, LocalStorageService
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestState,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)


def _request(content: object, *, batch_id: str = "batch-1", key: str = "idem-1") -> BatchIngestRequest:
    now = datetime(2026, 7, 29, tzinfo=timezone.utc)
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1",
            group_id="storage-main",
            actor_node_id="node-a",
            grant_id="grant-1",
            term=1,
            fencing_token=10,
            lease_expires_at=now + timedelta(minutes=5),
        ),
        dataset_id="telemetry",
        batch_id=batch_id,
        idempotency_key=key,
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=now,
    )


def _envelope(request: BatchIngestRequest, operation: StorageOperation = StorageOperation.BATCH_INGEST, payload=None):
    return StorageRequestEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=operation,
        session_id="session-1",
        actor_node_id="node-a",
        authorization_context={"scopes": ["storage:write"]},
        payload=request.to_dict() if payload is None else payload,
    )


def test_filesystem_provider_stores_reads_and_survives_restart(tmp_path):
    request = _request({"sequence": 1, "value": 42})
    provider = FilesystemBatchStorageProvider(tmp_path)

    result = provider.ingest(request)
    assert result.state is BatchIngestState.STORED
    assert provider.exists(session_id="session-1", group_id="storage-main", batch_id="batch-1")
    identity = provider.committed_identity(
        session_id="session-1",
        group_id="storage-main",
        batch_id="batch-1",
    )
    assert identity is not None
    assert identity.dataset_id == request.dataset_id
    assert identity.dataset_schema_name == request.dataset_schema_name
    assert identity.dataset_schema_version == request.dataset_schema_version
    assert identity.idempotency_key == request.idempotency_key
    assert identity.content_hash == request.content_hash
    assert provider.read(session_id="session-1", group_id="storage-main", batch_id="batch-1") == request.content

    restarted = FilesystemBatchStorageProvider(tmp_path)
    assert restarted.read(session_id="session-1", group_id="storage-main", batch_id="batch-1") == request.content


def test_filesystem_provider_migrates_legacy_index_without_losing_identity(
    tmp_path,
):
    request = _request({"sequence": 1, "value": 42})
    relative_path = "session-1/storage-main/telemetry/batch-1.json"
    final_path = tmp_path / "batches" / relative_path
    final_path.parent.mkdir(parents=True)
    final_path.write_text(
        json.dumps(request.to_dict(), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    database = tmp_path / "storage-index.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE committed_batches (
                session_id TEXT NOT NULL,
                group_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (session_id, group_id, batch_id),
                UNIQUE (session_id, group_id, idempotency_key)
            );
            """
        )
        connection.execute(
            """INSERT INTO committed_batches
               (session_id, group_id, dataset_id, batch_id, idempotency_key,
                content_hash, relative_path, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request.authority.session_id,
                request.authority.group_id,
                request.dataset_id,
                request.batch_id,
                request.idempotency_key,
                request.content_hash,
                relative_path,
                request.created_at.isoformat(),
            ),
        )

    provider = FilesystemBatchStorageProvider(tmp_path)
    identity = provider.committed_identity(
        session_id=request.authority.session_id,
        group_id=request.authority.group_id,
        batch_id=request.batch_id,
    )

    assert identity is not None
    assert identity.dataset_id == request.dataset_id
    assert identity.dataset_schema_name == "msh.storage.dataset.opaque"
    assert identity.dataset_schema_version == 1
    assert provider.read(
        session_id=request.authority.session_id,
        group_id=request.authority.group_id,
        batch_id=request.batch_id,
    ) == request.content
    assert provider.ingest(request).state is BatchIngestState.ALREADY_STORED


def test_identical_retry_is_idempotent(tmp_path):
    request = _request({"value": 1})
    provider = FilesystemBatchStorageProvider(tmp_path)
    assert provider.ingest(request).state is BatchIngestState.STORED
    assert provider.ingest(request).state is BatchIngestState.ALREADY_STORED


def test_reused_idempotency_key_with_different_content_is_rejected(tmp_path):
    provider = FilesystemBatchStorageProvider(tmp_path)
    provider.ingest(_request({"value": 1}))
    service = LocalStorageService(provider)

    response = service.dispatch(_envelope(_request({"value": 2})))
    assert not response.ok
    assert response.error.code is StorageErrorCode.IDEMPOTENCY_CONFLICT


def test_same_batch_with_another_key_is_rejected(tmp_path):
    provider = FilesystemBatchStorageProvider(tmp_path)
    provider.ingest(_request({"value": 1}))
    service = LocalStorageService(provider)

    response = service.dispatch(_envelope(_request({"value": 1}, key="idem-2")))
    assert not response.ok
    assert response.error.code is StorageErrorCode.IDEMPOTENCY_CONFLICT


def test_idempotency_identity_cannot_move_between_datasets(tmp_path):
    provider = FilesystemBatchStorageProvider(tmp_path)
    original = _request({"value": 1})
    provider.ingest(original)
    moved = BatchIngestRequest(
        authority=original.authority,
        dataset_id="another-dataset",
        batch_id=original.batch_id,
        idempotency_key=original.idempotency_key,
        content_hash=original.content_hash,
        content=original.content,
        created_at=original.created_at,
    )
    response = LocalStorageService(provider).dispatch(_envelope(moved))

    assert not response.ok
    assert response.error.code is StorageErrorCode.IDEMPOTENCY_CONFLICT
    assert response.error.field == "dataset_id"


def test_idempotency_identity_cannot_change_dataset_schema(tmp_path):
    provider = FilesystemBatchStorageProvider(tmp_path)
    original = _request({"value": 1})
    provider.ingest(original)
    changed = BatchIngestRequest(
        authority=original.authority,
        dataset_id=original.dataset_id,
        batch_id=original.batch_id,
        idempotency_key=original.idempotency_key,
        content_hash=original.content_hash,
        content=original.content,
        created_at=original.created_at,
        dataset_schema_name="msh.telemetry.observations",
        dataset_schema_version=2,
    )
    response = LocalStorageService(provider).dispatch(_envelope(changed))

    assert not response.ok
    assert response.error.code is StorageErrorCode.IDEMPOTENCY_CONFLICT
    assert response.error.field == "dataset_schema_name"


def test_dispatcher_describe_health_exists_and_read(tmp_path):
    request = _request({"value": 7})
    service = LocalStorageService(FilesystemBatchStorageProvider(tmp_path))
    assert service.dispatch(_envelope(request)).ok

    describe = service.dispatch(_envelope(request, StorageOperation.DESCRIBE, {}))
    assert describe.ok and describe.result["backend"] == "filesystem"

    health = service.dispatch(_envelope(request, StorageOperation.HEALTH, {}))
    assert health.ok and health.result["status"] == "ready"

    coordinates = {"group_id": "storage-main", "batch_id": "batch-1"}
    exists = service.dispatch(_envelope(request, StorageOperation.BATCH_EXISTS, coordinates))
    assert exists.ok and exists.result == {"exists": True}

    read = service.dispatch(_envelope(request, StorageOperation.BATCH_READ, coordinates))
    assert read.ok and read.result == {"content": {"value": 7}}


def test_dispatcher_rejects_envelope_authority_mismatch(tmp_path):
    request = _request({"value": 1})
    envelope = StorageRequestEnvelope(
        request_id="request-2",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.BATCH_INGEST,
        session_id="another-session",
        actor_node_id="node-a",
        authorization_context={},
        payload=request.to_dict(),
    )
    response = LocalStorageService(FilesystemBatchStorageProvider(tmp_path)).dispatch(envelope)
    assert not response.ok
    assert response.error.code is StorageErrorCode.INVALID_REQUEST


def test_invalid_content_hash_never_becomes_visible(tmp_path):
    request = _request({"value": 1})
    bad = BatchIngestRequest(
        authority=request.authority,
        dataset_id=request.dataset_id,
        batch_id=request.batch_id,
        idempotency_key=request.idempotency_key,
        content_hash="sha256:" + "0" * 64,
        content=request.content,
        created_at=request.created_at,
    )
    provider = FilesystemBatchStorageProvider(tmp_path)
    response = LocalStorageService(provider).dispatch(_envelope(bad))
    assert not response.ok
    assert response.error.code is StorageErrorCode.CONTENT_HASH_MISMATCH
    assert not provider.exists(session_id="session-1", group_id="storage-main", batch_id="batch-1")

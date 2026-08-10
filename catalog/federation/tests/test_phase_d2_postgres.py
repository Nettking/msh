from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from catalog.federation.errors import FederationValidationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.postgres_storage import PostgreSQLBatchStorageProvider
from catalog.federation.storage_protocol import (
    BatchIngestRequest,
    BatchIngestState,
    StorageErrorCode,
    WriteAuthority,
)

POSTGRES_DSN = os.environ.get(
    "FCP_TEST_POSTGRES_DSN",
    "postgresql://fcp:fcp@127.0.0.1:5432/fcp_test",
)


def _request(
    content: object,
    *,
    session_id: str,
    batch_id: str = "batch-1",
    key: str = "idem-1",
) -> BatchIngestRequest:
    now = datetime(2026, 7, 29, tzinfo=timezone.utc)
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id=session_id,
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


def _postgres() -> PostgreSQLBatchStorageProvider:
    try:
        return PostgreSQLBatchStorageProvider(POSTGRES_DSN)
    except psycopg.Error as exc:  # pragma: no cover - local developers may not run PostgreSQL
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")


@pytest.fixture(params=["filesystem", "postgresql"])
def provider(request, tmp_path: Path):
    if request.param == "filesystem":
        return FilesystemBatchStorageProvider(tmp_path / "storage")
    return _postgres()


def test_shared_provider_conformance(provider) -> None:
    session_id = f"session-{uuid4()}"
    content = {"observations": [{"sequence": 1, "value": 12.5}]}
    request = _request(content, session_id=session_id)

    first = provider.ingest(request)
    assert first.state is BatchIngestState.STORED
    assert provider.exists(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-1",
    )
    identity = provider.committed_identity(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-1",
    )
    assert identity is not None
    assert identity.dataset_id == request.dataset_id
    assert identity.dataset_schema_name == request.dataset_schema_name
    assert identity.dataset_schema_version == request.dataset_schema_version
    assert identity.idempotency_key == request.idempotency_key
    assert identity.content_hash == request.content_hash
    assert provider.read(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-1",
    ) == content

    retry = provider.ingest(request)
    assert retry.state is BatchIngestState.ALREADY_STORED

    description = provider.describe()
    assert description["protocol"] == "fcp-storage-v1"
    assert description["backend"] in {"filesystem", "postgresql"}
    assert provider.health()["status"] == "ready"


def test_shared_provider_rejects_idempotency_conflict(provider) -> None:
    session_id = f"session-{uuid4()}"
    provider.ingest(_request({"value": 1}, session_id=session_id))

    with pytest.raises(FederationValidationError) as error:
        provider.ingest(
            _request(
                {"value": 2},
                session_id=session_id,
                batch_id="batch-2",
                key="idem-1",
            )
        )

    assert error.value.code == StorageErrorCode.IDEMPOTENCY_CONFLICT.value
    assert not provider.exists(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-2",
    )


def test_shared_provider_rejects_dataset_identity_conflict(provider) -> None:
    session_id = f"session-{uuid4()}"
    original = _request({"value": 1}, session_id=session_id)
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

    with pytest.raises(FederationValidationError) as error:
        provider.ingest(moved)

    assert error.value.code == StorageErrorCode.IDEMPOTENCY_CONFLICT.value
    assert error.value.field == "dataset_id"


def test_shared_provider_rejects_dataset_schema_conflict(provider) -> None:
    session_id = f"session-{uuid4()}"
    original = _request({"value": 1}, session_id=session_id)
    provider.ingest(original)
    changed = BatchIngestRequest(
        authority=original.authority,
        dataset_id=original.dataset_id,
        batch_id=original.batch_id,
        idempotency_key=original.idempotency_key,
        content_hash=original.content_hash,
        content=original.content,
        created_at=original.created_at,
        dataset_schema_name="fcp.telemetry.observations",
        dataset_schema_version=2,
    )

    with pytest.raises(FederationValidationError) as error:
        provider.ingest(changed)

    assert error.value.code == StorageErrorCode.IDEMPOTENCY_CONFLICT.value
    assert error.value.field == "dataset_schema_name"


def test_postgresql_provider_survives_reconstruction() -> None:
    session_id = f"session-{uuid4()}"
    first = _postgres()
    first.ingest(_request({"value": "durable"}, session_id=session_id))

    reconstructed = _postgres()
    assert reconstructed.read(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-1",
    ) == {"value": "durable"}


def test_postgresql_provider_migrates_legacy_schema() -> None:
    schema_name = f"phase_e1_{uuid4().hex}"
    try:
        admin = psycopg.connect(POSTGRES_DSN, autocommit=True)
    except psycopg.Error as exc:  # pragma: no cover - local developers may not run PostgreSQL
        pytest.skip(f"PostgreSQL integration service unavailable: {exc}")
    with admin:
        admin.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
    isolated_dsn = make_conninfo(
        POSTGRES_DSN,
        options=f"-c search_path={schema_name}",
    )
    session_id = f"session-{uuid4()}"
    request = _request({"value": "legacy"}, session_id=session_id)
    try:
        with psycopg.connect(isolated_dsn) as connection:
            connection.execute(
                """
                CREATE TABLE fcp_storage_batches (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    content JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (session_id, group_id, batch_id),
                    UNIQUE (session_id, group_id, idempotency_key)
                )
                """
            )
            connection.execute(
                """INSERT INTO fcp_storage_batches
                   (session_id, group_id, dataset_id, batch_id,
                    idempotency_key, content_hash, content, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    session_id,
                    request.authority.group_id,
                    request.dataset_id,
                    request.batch_id,
                    request.idempotency_key,
                    request.content_hash,
                    json.dumps(request.content),
                    request.created_at,
                ),
            )

        provider = PostgreSQLBatchStorageProvider(isolated_dsn)
        identity = provider.committed_identity(
            session_id=session_id,
            group_id=request.authority.group_id,
            batch_id=request.batch_id,
        )

        assert identity is not None
        assert identity.dataset_schema_name == "fcp.storage.dataset.opaque"
        assert identity.dataset_schema_version == 1
        assert provider.ingest(request).state is BatchIngestState.ALREADY_STORED
    finally:
        with psycopg.connect(POSTGRES_DSN, autocommit=True) as cleanup:
            cleanup.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )


def test_postgresql_invalid_hash_is_not_committed() -> None:
    session_id = f"session-{uuid4()}"
    provider = _postgres()
    request = _request({"value": 1}, session_id=session_id)
    invalid = BatchIngestRequest(
        authority=request.authority,
        dataset_id=request.dataset_id,
        batch_id=request.batch_id,
        idempotency_key=request.idempotency_key,
        content_hash="sha256:" + "0" * 64,
        content=request.content,
        created_at=request.created_at,
    )

    with pytest.raises(FederationValidationError) as error:
        provider.ingest(invalid)

    assert error.value.code == StorageErrorCode.CONTENT_HASH_MISMATCH.value
    assert not provider.exists(
        session_id=session_id,
        group_id="storage-main",
        batch_id="batch-1",
    )

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.logical_storage import LogicalStorageClient, StorageRouteResolver
from catalog.federation.storage_control_plane import (
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    BatchIngestState,
    StorageResponseEnvelope,
    WriteAuthority,
)

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


class FakeTransport:
    def __init__(self) -> None:
        self.calls = []

    async def request(self, *, target_node_id, envelope):
        self.calls.append((target_node_id, envelope))
        if envelope.operation.value == "batch.ingest":
            request = BatchIngestRequest.from_dict(envelope.payload)
            result = BatchIngestResult(
                request.batch_id,
                request.idempotency_key,
                request.content_hash,
                BatchIngestState.STORED,
            ).to_dict()
        elif envelope.operation.value == "batch.exists":
            result = {"exists": True}
        else:
            result = {"content": {"value": 42}}
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=result,
        )


def _store(path: Path) -> StorageControlPlaneStore:
    store = StorageControlPlaneStore(path)
    store.create_group("session-1", "coordinator", "storage-main")
    store.register_provider(
        "session-1",
        "coordinator",
        StorageProviderRegistration(
            session_id="session-1",
            provider_id="provider-a",
            node_id="node-a",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    store.change_assignment(
        "session-1", "coordinator", "storage-main", "provider-a"
    )
    return store


def _request() -> BatchIngestRequest:
    content = {"value": 42}
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1",
            group_id="storage-main",
            actor_node_id="node-client",
            grant_id="grant-1",
            term=1,
            fencing_token=1,
            lease_expires_at=NOW + timedelta(minutes=5),
        ),
        dataset_id="telemetry",
        batch_id="batch-1",
        idempotency_key="idem-1",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=NOW,
    )


@pytest.mark.asyncio
async def test_logical_client_routes_ingest_to_assigned_primary(tmp_path: Path) -> None:
    transport = FakeTransport()
    client = LogicalStorageClient(
        session_id="session-1",
        actor_node_id="node-client",
        resolver=StorageRouteResolver(_store(tmp_path / "control.sqlite3")),
        transport=transport,
    )

    result = await client.ingest(_request())

    assert result.state is BatchIngestState.STORED
    assert transport.calls[0][0] == "node-a"
    envelope = transport.calls[0][1]
    assert envelope.authorization_context["provider_id"] == "provider-a"
    assert envelope.authorization_context["group_id"] == "storage-main"


@pytest.mark.asyncio
async def test_logical_client_exposes_provider_agnostic_reads(tmp_path: Path) -> None:
    transport = FakeTransport()
    client = LogicalStorageClient(
        session_id="session-1",
        actor_node_id="node-client",
        resolver=StorageRouteResolver(_store(tmp_path / "control.sqlite3")),
        transport=transport,
    )

    assert await client.exists(group_id="storage-main", batch_id="batch-1")
    assert await client.read(group_id="storage-main", batch_id="batch-1") == {"value": 42}
    assert [call[0] for call in transport.calls] == ["node-a", "node-a"]


@pytest.mark.asyncio
async def test_no_primary_fails_without_transport_access(tmp_path: Path) -> None:
    store = StorageControlPlaneStore(tmp_path / "control.sqlite3")
    store.create_group("session-1", "coordinator", "storage-main")
    transport = FakeTransport()
    client = LogicalStorageClient(
        session_id="session-1",
        actor_node_id="node-client",
        resolver=StorageRouteResolver(store),
        transport=transport,
    )

    with pytest.raises(FederationValidationError) as error:
        await client.exists(group_id="storage-main", batch_id="batch-1")

    assert error.value.code == "not-primary"
    assert transport.calls == []


@pytest.mark.asyncio
async def test_client_does_not_fail_over_to_replica(tmp_path: Path) -> None:
    store = _store(tmp_path / "control.sqlite3")
    store.register_provider(
        "session-1",
        "coordinator",
        StorageProviderRegistration(
            session_id="session-1",
            provider_id="provider-b",
            node_id="node-b",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    store.change_assignment(
        "session-1", "coordinator", "storage-main", None, ("provider-b",)
    )
    transport = FakeTransport()
    client = LogicalStorageClient(
        session_id="session-1",
        actor_node_id="node-client",
        resolver=StorageRouteResolver(store),
        transport=transport,
    )

    with pytest.raises(FederationValidationError) as error:
        await client.read(group_id="storage-main", batch_id="batch-1")

    assert error.value.code == "not-primary"
    assert transport.calls == []

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.outbox import OutboxState, SQLiteOutbox
from catalog.federation.replication import (
    PHASE_D_SERVICE_REPLICATION_OWNER,
    REPLICATION_SCHEMA,
    DurableReplicationOutbox,
    ReplicationPlanner,
    ReplicationRunResult,
    ReplicationWorker,
)
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
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = []

    async def request(self, *, target_node_id, envelope):
        self.calls.append((target_node_id, envelope))
        if self.fail:
            raise RuntimeError("replica unavailable")
        request = BatchIngestRequest.from_dict(envelope.payload)
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=True,
            result=BatchIngestResult(
                batch_id=request.batch_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                state=BatchIngestState.STORED,
            ).to_dict(),
        )


def _control_plane(path: Path) -> StorageControlPlaneStore:
    store = StorageControlPlaneStore(path)
    store.create_group("session-1", "coordinator", "storage-main")
    for provider_id, node_id in (
        ("provider-primary", "node-primary"),
        ("provider-replica-a", "node-a"),
        ("provider-replica-b", "node-b"),
    ):
        store.register_provider(
            "session-1",
            "coordinator",
            StorageProviderRegistration(
                session_id="session-1",
                provider_id=provider_id,
                node_id=node_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                authorized=True,
                status="ready",
            ),
        )
    store.change_assignment(
        "session-1",
        "coordinator",
        "storage-main",
        "provider-primary",
        ("provider-replica-a", "provider-replica-b"),
    )
    return store


def _request() -> BatchIngestRequest:
    content = {"value": 42}
    return BatchIngestRequest(
        authority=WriteAuthority(
            session_id="session-1",
            group_id="storage-main",
            actor_node_id="node-primary",
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


def test_replication_intent_is_durable_and_idempotent(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    replication = DurableReplicationOutbox(
        outbox,
        ReplicationPlanner(_control_plane(tmp_path / "control.sqlite3")),
    )

    first = replication.enqueue_ingest(_request(), now=NOW)
    second = replication.enqueue_ingest(_request(), now=NOW)

    assert len(first) == 2
    assert [entry.outbox_id for entry in first] == [entry.outbox_id for entry in second]
    assert {entry.destination_id for entry in first} == {
        "provider-replica-a",
        "provider-replica-b",
    }
    assert all(entry.state is OutboxState.PENDING for entry in outbox.pending())


def test_worker_delivers_and_acknowledges_each_replica(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    replication = DurableReplicationOutbox(
        outbox,
        ReplicationPlanner(_control_plane(tmp_path / "control.sqlite3")),
    )
    replication.enqueue_ingest(_request(), now=NOW)
    transport = FakeTransport()
    worker = ReplicationWorker(
        outbox=outbox,
        transport=transport,
        actor_node_id="node-primary",
        clock=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.attempted == 2
    assert result.completed == 2
    assert result.failed == 0
    assert {call[0] for call in transport.calls} == {"node-a", "node-b"}
    assert outbox.pending() == ()


def test_failed_delivery_remains_pending_with_backoff(tmp_path: Path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    replication = DurableReplicationOutbox(
        outbox,
        ReplicationPlanner(_control_plane(tmp_path / "control.sqlite3")),
    )
    replication.enqueue_ingest(_request(), now=NOW)
    worker = ReplicationWorker(
        outbox=outbox,
        transport=FakeTransport(fail=True),
        actor_node_id="node-primary",
        clock=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())
    pending = outbox.pending()

    assert result.failed == 2
    assert len(pending) == 2
    assert all(entry.attempt_count == 1 for entry in pending)
    assert all(entry.next_attempt_at == NOW + timedelta(seconds=1) for entry in pending)
    assert all(entry.last_error == "replica unavailable" for entry in pending)


def test_restart_recovers_pending_replication(tmp_path: Path) -> None:
    database = tmp_path / "outbox.sqlite3"
    first_outbox = SQLiteOutbox(database)
    replication = DurableReplicationOutbox(
        first_outbox,
        ReplicationPlanner(_control_plane(tmp_path / "control.sqlite3")),
    )
    replication.enqueue_ingest(_request(), now=NOW)

    recovered_outbox = SQLiteOutbox(database)
    worker = ReplicationWorker(
        outbox=recovered_outbox,
        transport=FakeTransport(),
        actor_node_id="node-primary",
        clock=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.completed == 2
    assert recovered_outbox.pending() == ()


def test_generic_worker_does_not_consume_service_owned_ack_entries(
    tmp_path: Path,
) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    request = _request()
    entry, _created = outbox.enqueue(
        session_id=request.authority.session_id,
        destination_id="provider-replica-a",
        schema_id=REPLICATION_SCHEMA,
        payload={
            "delivery_owner": PHASE_D_SERVICE_REPLICATION_OWNER,
            "request": request.to_dict(),
        },
        idempotency_key="service-owned",
        content_hash=request.content_hash,
        now=NOW,
    )
    worker = ReplicationWorker(
        outbox=outbox,
        transport=FakeTransport(),
        actor_node_id="node-primary",
        clock=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result == ReplicationRunResult(0, 0, 0)
    assert outbox.get(entry.outbox_id) == entry

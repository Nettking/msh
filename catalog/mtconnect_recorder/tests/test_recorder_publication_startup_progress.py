from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from contextlib import suppress
from datetime import datetime, timezone
from types import SimpleNamespace

from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    DurableRecorderDeliveryQueue,
)
from catalog.mtconnect_recorder import federation_node as federation_node_module
from catalog.mtconnect_recorder.federation_node import (
    RecorderFederationNode,
    RecorderFederationSnapshot,
)


def _status() -> dict[str, object]:
    return {
        "sessions": [
            {"session_id": "session-1", "created_by_node_id": "node-owner"}
        ],
        "capabilities": [
            {
                "capability_id": "logical-storage-authority",
                "node_id": "node-owner",
                "session_id": "session-1",
                "type": "storage-control",
                "protocol": "fcp.storage-control",
                "protocol_version": "1",
                "status": "ready",
                "properties": {
                    "kind": "recorder-logical-storage-authority",
                    "group_ids": ["telemetry"],
                },
            }
        ],
    }


def test_startup_readiness_is_published_at_first_commit(tmp_path, monkeypatch) -> None:
    class _StatusClient:
        async def coordinator_status(self):
            return _status()

    class _Runtime:
        def __init__(self) -> None:
            self.client = _StatusClient()

        async def _ensure_connected(self, _state) -> None:
            return None

        def _connected_client(self):
            return self.client

    class _StorageClient:
        def __init__(self) -> None:
            self.batch_ids: list[str] = []
            self.first_commit = asyncio.Event()
            self.all_commits = asyncio.Event()
            self.closed = False

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

        async def ingest_batch(self, **kwargs):
            await asyncio.sleep(0.15)
            self.batch_ids.append(str(kwargs["batch_id"]))
            if len(self.batch_ids) == 1:
                self.first_commit.set()
            if len(self.batch_ids) == 8:
                self.all_commits.set()
            return PhaseDIngestOutcome(committed=True)

    storage_client = _StorageClient()
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    seed_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=storage_client,
        session_id="session-1",
        destination_id="telemetry",
    )
    created_at = datetime(2026, 8, 20, 17, 21, tzinfo=timezone.utc)
    for index in range(8):
        seed_queue.enqueue(
            session_id="session-1",
            group_id="telemetry",
            dataset_id=f"dataset-{index}",
            batch_id=f"batch-{index}",
            idempotency_key=f"dataset-{index}:{index}",
            content={"dataset": index},
            created_at=created_at,
        )
    # Model a recovered startup backlog: every row carries a historical
    # failure/backoff marker, but the fresh queue is allowed to probe each
    # deferred head immediately. The early snapshot must reflect the current
    # successful route rather than stale error text on untouched rows.
    for entry in outbox.pending():
        outbox.record_failure(
            entry.outbox_id,
            error="previous federation outage",
            now=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(
        federation_node_module,
        "SQLiteOutbox",
        lambda _database: outbox,
    )
    monkeypatch.setattr(
        federation_node_module,
        "RelayRecorderStorageClient",
        lambda *_args, **_kwargs: storage_client,
    )

    node = RecorderFederationNode.__new__(RecorderFederationNode)
    node.data_directory = tmp_path
    node.source_names = tuple(f"dataset-{index}" for index in range(8))
    node.requested_storage_group = None
    node.request_timeout = 1.0
    node.publication_poll_seconds = 60.0
    node.runtime = _Runtime()
    node._lock = threading.RLock()
    node._stop = threading.Event()
    node._publication_future = Future()
    node._snapshot = RecorderFederationSnapshot(
        status="connected",
        node_id="node-recorder",
        federation_id="federation-1",
        session_id="session-1",
        storage_state="discovering",
        jsonl_state="discovering",
    )

    async def announce_connected(_state) -> None:
        return None

    async def publish_jsonl(
        _state,
        *,
        authority_node_id: str,
        group_id: str,
    ):
        assert authority_node_id == "node-owner"
        assert group_id == "telemetry"
        return SimpleNamespace(published_chunks=1)

    node._announce_connected = announce_connected
    node._publish_jsonl_once = publish_jsonl
    state = SimpleNamespace(
        binding=SimpleNamespace(
            internal_session_id="session-1",
            device_id="node-recorder",
        )
    )
    async def scenario() -> None:
        runner = asyncio.create_task(node._publication_loop(state))
        try:
            await asyncio.wait_for(storage_client.first_commit.wait(), timeout=2.0)
            snapshot = await asyncio.wait_for(
                asyncio.to_thread(
                    node.wait_until_sharing_ready,
                    timeout_seconds=0.6,
                ),
                timeout=1.0,
            )
            assert snapshot.last_committed_count == 1
            assert snapshot.jsonl_state == "ready"
            assert len(storage_client.batch_ids) < 8

            await asyncio.wait_for(storage_client.all_commits.wait(), timeout=4.0)
            for _ in range(20):
                if not outbox.pending():
                    break
                await asyncio.sleep(0.02)
            assert outbox.pending() == ()
        finally:
            node._stop.set()
            runner.cancel()
            with suppress(asyncio.CancelledError):
                await runner
            node._publication_future.cancel()

        assert storage_client.closed is True

    asyncio.run(scenario())

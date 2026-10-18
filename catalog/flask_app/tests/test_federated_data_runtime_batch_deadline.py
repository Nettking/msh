from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from catalog.flask_app.services import federated_data_runtime as data_runtime


def test_serial_generic_batches_complete_with_each_request_within_its_bound(
    tmp_path: Path, monkeypatch,
) -> None:
    committed: list[str] = []
    request_bounds: list[float] = []

    class Storage:
        def __init__(self, client, *, session_id, authority_node_id, request_timeout):
            self.request_timeout = request_timeout

        async def ingest_batch(self, **batch):
            request_bounds.append(self.request_timeout)
            await asyncio.wait_for(asyncio.sleep(1.2), timeout=self.request_timeout)
            committed.append(batch["batch_id"])
            return SimpleNamespace(committed=True)

        async def close(self):
            pass

    class Runtime(data_runtime.FederatedDataPairingRelayRuntime):
        async def _ensure_connected(self, state):
            pass

        def _connected_client(self):
            return object()

    monkeypatch.setattr(data_runtime, "RelayRecorderStorageClient", Storage)
    state = SimpleNamespace(binding=SimpleNamespace(internal_session_id="session"))
    runtime = Runtime(
        state_directory=tmp_path / "unused-identity",
        display_name="No-network batch deadline regression",
        timeout_seconds=2,
    )
    batches = [
        {
            "group_id": "group",
            "dataset_id": f"dataset-{index}",
            "batch_id": f"batch-{index}",
            "idempotency_key": f"key-{index}",
            "content": {},
            "created_at": datetime.now(timezone.utc),
            "dataset_schema_name": "fcp.federation.jsonl-file-chunk",
            "dataset_schema_version": 1,
        }
        for index in range(2)
    ]
    try:
        outcomes = runtime.publish_federated_batches(
            state,
            session_id="session",
            authority_node_id="authority",
            batches=batches,
        )
        assert [outcome.committed for outcome in outcomes] == [True, True]
        assert committed == ["batch-0", "batch-1"]
        assert request_bounds == [2, 2]
        assert not (tmp_path / "unused-identity").exists()
    finally:
        assert runtime.close()

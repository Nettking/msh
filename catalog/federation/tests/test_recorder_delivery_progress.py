from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
    RecorderDeliveryProgress,
)


class _CommitClient:
    async def ingest_batch(self, **_kwargs):
        return PhaseDIngestOutcome(committed=True)


def _enqueue(outbox: SQLiteOutbox, dataset_id: str, batch_id: str) -> None:
    outbox.enqueue(
        session_id="session-1",
        destination_id="telemetry",
        schema_id=RECORDER_STORAGE_SCHEMA,
        payload={
            "group_id": "telemetry",
            "dataset_id": dataset_id,
            "batch_id": batch_id,
            "idempotency_key": batch_id,
            "content": {"dataset": dataset_id},
            "created_at": "2026-08-20T17:21:00Z",
        },
        idempotency_key=batch_id,
        content_hash="a" * 64,
        now=datetime(2026, 8, 20, 17, 21, tzinfo=UTC),
    )


def test_progress_observer_runs_after_each_durable_commit(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue(outbox, "dataset-a", "batch-a")
    _enqueue(outbox, "dataset-b", "batch-b")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )
    observed: list[RecorderDeliveryProgress] = []
    durable_states: list[tuple[str, ...]] = []

    async def observe(progress: RecorderDeliveryProgress) -> None:
        observed.append(progress)
        durable_states.append(
            tuple(entry.payload["batch_id"] for entry in outbox.pending())
        )

    result = asyncio.run(queue.run_once(progress_observer=observe))

    assert result.committed == 2
    assert [item.committed for item in observed] == [1, 2]
    assert [item.dataset_id for item in observed] == ["dataset-a", "dataset-b"]
    assert durable_states == [("batch-b",), ()]
    assert outbox.pending() == ()

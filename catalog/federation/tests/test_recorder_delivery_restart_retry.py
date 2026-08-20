from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue

NOW = datetime(2026, 8, 20, 13, 0, tzinfo=timezone.utc)


class _FailingClient:
    async def ingest_batch(self, **_kwargs):
        return PhaseDIngestOutcome(
            committed=False,
            retryable=True,
            error_code="test-offline",
            message="test storage unavailable",
        )


class _CommittingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def ingest_batch(self, **_kwargs):
        self.calls += 1
        return PhaseDIngestOutcome(committed=True)


def _queue(outbox, client):
    return DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-a",
        destination_id="fcp-local-storage",
        clock=lambda: NOW,
    )


def test_new_queue_retries_one_deferred_head_without_rewriting_backoff(
    tmp_path,
) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    failing = _queue(outbox, _FailingClient())
    failing.enqueue(
        session_id="session-a",
        group_id="fcp-local-storage",
        dataset_id="dataset-a",
        batch_id="batch-1",
        idempotency_key="dataset-a:batch-1",
        content={"value": 1},
        created_at=NOW,
    )

    first = asyncio.run(failing.run_once())
    assert first.attempted == 1
    assert first.pending == 1
    deferred = outbox.pending()[0]
    assert deferred.next_attempt_at > NOW
    original_next_attempt = deferred.next_attempt_at
    original_attempt_count = deferred.attempt_count

    # The same runtime obeys the durable delay instead of hot-looping.
    same_runtime = asyncio.run(failing.run_once())
    assert same_runtime.attempted == 0
    still_deferred = outbox.pending()[0]
    assert still_deferred.next_attempt_at == original_next_attempt
    assert still_deferred.attempt_count == original_attempt_count

    # A fresh process/runtime receives exactly one immediate proof attempt. This
    # models an upgraded or repaired authority without asking the operator to
    # edit SQLite or wait out an hour-old failure timer.
    committing_client = _CommittingClient()
    restarted = _queue(outbox, committing_client)
    recovered = asyncio.run(restarted.run_once())

    assert recovered.attempted == 1
    assert recovered.committed == 1
    assert committing_client.calls == 1
    assert outbox.pending() == ()


def test_restart_retry_remains_one_head_per_dataset(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    failing = _queue(outbox, _FailingClient())
    for dataset_id in ("dataset-a", "dataset-b"):
        for index in (1, 2):
            failing.enqueue(
                session_id="session-a",
                group_id="fcp-local-storage",
                dataset_id=dataset_id,
                batch_id=f"{dataset_id}-batch-{index}",
                idempotency_key=f"{dataset_id}:{index}",
                content={"dataset": dataset_id, "index": index},
                created_at=NOW,
            )

    # First pass fails each dataset head and fences each newer row.
    first = asyncio.run(failing.run_once())
    assert first.attempted == 2
    assert first.pending == 2

    restarted = _queue(outbox, _FailingClient())
    second = asyncio.run(restarted.run_once())

    # Restart bypasses the delay only for the two oldest dataset heads. Their
    # failure fences the second row in each dataset; no backlog flood occurs.
    assert second.attempted == 2
    assert second.pending == 2
    pending = outbox.pending()
    attempts = {entry.payload["batch_id"]: entry.attempt_count for entry in pending}
    assert attempts["dataset-a-batch-1"] == 2
    assert attempts["dataset-b-batch-1"] == 2
    assert attempts["dataset-a-batch-2"] == 0
    assert attempts["dataset-b-batch-2"] == 0

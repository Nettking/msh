from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from catalog.federation.outbox import RetiredSummary, SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import (
    RECORDER_STORAGE_SCHEMA,
    DurableRecorderDeliveryQueue,
    RecorderDeliveryRunResult,
)
from catalog.federation.recorder_publication import (
    RecorderFederationDeliveryWorker,
    RecorderReconcileResult,
)


class _BacklogOutbox:
    def __init__(self, entries: list[object]) -> None:
        self.entries = entries
        self.pending_thread_ids: list[int] = []

    def pending(self, *, now=None):
        # The worker now asks only for backlog that is due, so a row waiting
        # out its retry backoff cannot defer reconciliation. Every entry this
        # stub holds models a recovered backlog, which is due immediately.
        self.pending_thread_ids.append(threading.get_ident())
        return tuple(self.entries)

    def retired_summary(self, **_kwargs):
        # This stub models a recorder with no permanently withdrawn evidence,
        # so restart ordering is exercised without degraded health in the way.
        return RetiredSummary(total=0, datasets=(), truncated=False)


class _Queue:
    session_id = "session-a"
    destination_id = "fcp-local-storage"

    def __init__(self, outbox: _BacklogOutbox) -> None:
        self.outbox = outbox
        self.calls = 0
        self._startup_probe_available = True

    @property
    def startup_probe_pending(self) -> bool:
        return self._startup_probe_available

    @staticmethod
    def clock() -> datetime:
        return datetime.now(timezone.utc)

    async def run_once(self, *, limit: int = 100) -> RecorderDeliveryRunResult:
        self.calls += 1
        self._startup_probe_available = False
        if self.outbox.entries:
            self.outbox.entries.clear()
            return RecorderDeliveryRunResult(attempted=1, committed=1, pending=0)
        return RecorderDeliveryRunResult(attempted=0, committed=0, pending=0)


class _Reconciler:
    def __init__(self, checkpoint_file: Path) -> None:
        self.checkpoint_file = checkpoint_file
        self.calls = 0
        self.thread_ids: list[int] = []

    def reconcile(self) -> RecorderReconcileResult:
        self.calls += 1
        self.thread_ids.append(threading.get_ident())
        return RecorderReconcileResult(
            scanned_batches=0,
            eligible_batches=0,
            publication_chunks=0,
            enqueued=0,
            already_enqueued=0,
        )


def _entry() -> object:
    return SimpleNamespace(
        schema_id=RECORDER_STORAGE_SCHEMA,
        session_id="session-a",
        destination_id="fcp-local-storage",
    )


def test_restart_drains_existing_backlog_before_full_archive_reconcile(tmp_path) -> None:
    checkpoint = tmp_path / "mtconnect_recorder_state.json"
    checkpoint.write_text("{}", encoding="utf-8")
    outbox = _BacklogOutbox([_entry()])
    queue = _Queue(outbox)
    reconciler = _Reconciler(checkpoint)
    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler,  # type: ignore[arg-type]
        queue=queue,  # type: ignore[arg-type]
    )

    first = asyncio.run(worker.run_cycle())

    assert first.checkpoint_changed is True
    assert first.reconcile is None
    assert first.delivery.committed == 1
    assert reconciler.calls == 0

    second = asyncio.run(worker.run_cycle())

    assert second.checkpoint_changed is True
    assert second.reconcile is not None
    assert reconciler.calls == 1


def test_archive_reconcile_runs_off_the_relay_event_loop_thread(tmp_path) -> None:
    checkpoint = tmp_path / "mtconnect_recorder_state.json"
    checkpoint.write_text("{}", encoding="utf-8")
    outbox = _BacklogOutbox([])
    queue = _Queue(outbox)
    reconciler = _Reconciler(checkpoint)
    worker = RecorderFederationDeliveryWorker(
        reconciler=reconciler,  # type: ignore[arg-type]
        queue=queue,  # type: ignore[arg-type]
    )
    caller_thread = threading.get_ident()

    result = asyncio.run(worker.run_cycle())

    assert result.reconcile is not None
    assert reconciler.thread_ids
    assert reconciler.thread_ids[0] != caller_thread


class _EmptyClient:
    async def ingest_batch(self, **_kwargs):  # pragma: no cover - no rows to deliver
        raise AssertionError("no delivery should be attempted")


class _ThreadRecordingOutbox:
    def __init__(self) -> None:
        self.thread_id: int | None = None

    def pending(self):
        self.thread_id = threading.get_ident()
        return ()


def test_large_outbox_snapshot_is_read_off_the_relay_event_loop_thread() -> None:
    outbox = _ThreadRecordingOutbox()
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,  # type: ignore[arg-type]
        client=_EmptyClient(),
        session_id="session-a",
        destination_id="fcp-local-storage",
    )
    caller_thread = threading.get_ident()

    result = asyncio.run(queue.run_once())

    assert result == RecorderDeliveryRunResult(attempted=0, committed=0, pending=0)
    assert outbox.thread_id is not None
    assert outbox.thread_id != caller_thread


def _enqueue_delivery_row(
    outbox: SQLiteOutbox,
    *,
    dataset_id: str,
    index: int,
    destination_id: str = "fcp-local-storage",
) -> None:
    outbox.enqueue(
        session_id="session-a",
        destination_id=destination_id,
        schema_id=RECORDER_STORAGE_SCHEMA,
        payload={
            "group_id": destination_id,
            "dataset_id": dataset_id,
            "batch_id": f"{dataset_id}-{index}",
            "idempotency_key": f"{dataset_id}-{index}",
            "content": {"sequence": index},
            "created_at": "2026-08-09T03:00:00+00:00",
        },
        idempotency_key=f"{dataset_id}-{index}",
        content_hash=f"sha256:{dataset_id}-{index}",
        now=datetime(2026, 8, 9, 3, 0, tzinfo=timezone.utc),
    )


def test_delivery_window_is_bounded_and_fair_across_datasets(tmp_path) -> None:
    """A large offline dataset cannot hide another dataset's delivery head."""

    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    for index in range(500):
        _enqueue_delivery_row(outbox, dataset_id="dataset-a", index=index)
    for index in range(3):
        _enqueue_delivery_row(outbox, dataset_id="dataset-b", index=index)

    window = outbox.pending_for_delivery(
        session_id="session-a",
        destination_id="fcp-local-storage",
        schema_id=RECORDER_STORAGE_SCHEMA,
        limit=4,
    )

    assert len(window) == 4
    assert [entry.payload["dataset_id"] for entry in window] == [
        "dataset-a",
        "dataset-a",
        "dataset-b",
        "dataset-b",
    ]
    assert [entry.payload["batch_id"] for entry in window] == [
        "dataset-a-0",
        "dataset-a-1",
        "dataset-b-0",
        "dataset-b-1",
    ]


def test_delivery_window_keeps_destinations_in_separate_ordering_groups(
    tmp_path,
) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue_delivery_row(
        outbox,
        dataset_id="dataset-a",
        index=0,
        destination_id="storage-a",
    )
    _enqueue_delivery_row(
        outbox,
        dataset_id="dataset-a",
        index=1,
        destination_id="storage-a",
    )
    _enqueue_delivery_row(
        outbox,
        dataset_id="dataset-a",
        index=0,
        destination_id="storage-b",
    )
    _enqueue_delivery_row(
        outbox,
        dataset_id="dataset-a",
        index=1,
        destination_id="storage-b",
    )

    window = outbox.pending_for_delivery(
        session_id="session-a",
        destination_id=None,
        schema_id=RECORDER_STORAGE_SCHEMA,
        limit=2,
    )

    assert [(entry.destination_id, entry.payload["batch_id"]) for entry in window] == [
        ("storage-a", "dataset-a-0"),
        ("storage-b", "dataset-a-0"),
    ]


class _CommitClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        return PhaseDIngestOutcome(committed=True)


def test_restarted_queue_makes_monotonic_bounded_backlog_progress(tmp_path) -> None:
    """Restarted delivery drains a durable backlog without a full snapshot."""

    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    for index in range(12):
        _enqueue_delivery_row(outbox, dataset_id="dataset-a", index=index)
        _enqueue_delivery_row(outbox, dataset_id="dataset-b", index=index)

    client = _CommitClient()
    first_queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=client,
        session_id="session-a",
        destination_id="fcp-local-storage",
    )
    first = asyncio.run(first_queue.run_once(limit=4))
    assert first.attempted == 2
    assert first.committed == 2

    # A new queue object models a process restart. The startup route probe is
    # intentionally bounded to one head per dataset, then ordinary cycles use
    # the configured window. The durable pending count must decrease after
    # every successful cycle; no in-memory backlog is carried across restart.
    restarted_queue = DurableRecorderDeliveryQueue(
        outbox=SQLiteOutbox(tmp_path / "outbox.sqlite3"),
        client=client,
        session_id="session-a",
        destination_id="fcp-local-storage",
    )
    remaining = len(restarted_queue.outbox.pending())
    assert remaining == 22
    while remaining:
        result = asyncio.run(restarted_queue.run_once(limit=4))
        assert result.committed > 0
        updated = len(restarted_queue.outbox.pending())
        assert updated < remaining
        remaining = updated

    assert len(client.calls) == 24


class _BoundedWindowOutbox:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def pending_for_delivery(self, **kwargs):
        self.kwargs = dict(kwargs)
        return ()

    def pending(self, **_kwargs):
        raise AssertionError("the delivery queue requested an unbounded snapshot")


def test_delivery_queue_uses_the_bounded_production_window() -> None:
    outbox = _BoundedWindowOutbox()
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,  # type: ignore[arg-type]
        client=_EmptyClient(),
        session_id="session-a",
        destination_id="fcp-local-storage",
        clock=lambda: datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc),
    )

    result = asyncio.run(queue.run_once(limit=7))

    assert result == RecorderDeliveryRunResult(attempted=0, committed=0, pending=0)
    assert outbox.kwargs == {
        "session_id": "session-a",
        "destination_id": "fcp-local-storage",
        "schema_id": RECORDER_STORAGE_SCHEMA,
        "limit": 7,
        "now": datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc),
    }

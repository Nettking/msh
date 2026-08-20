from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

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

    def pending(self):
        self.pending_thread_ids.append(threading.get_ident())
        return tuple(self.entries)


class _Queue:
    session_id = "session-a"
    destination_id = "fcp-local-storage"

    def __init__(self, outbox: _BacklogOutbox) -> None:
        self.outbox = outbox
        self.calls = 0

    async def run_once(self, *, limit: int = 100) -> RecorderDeliveryRunResult:
        self.calls += 1
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

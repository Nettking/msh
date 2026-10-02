from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from catalog.federation.errors import FederationValidationError
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


class _TimeoutClient:
    async def ingest_batch(self, **_kwargs):
        raise TimeoutError()


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
    assert [item.pending for item in observed] == [0, 0]
    assert [item.dataset_id for item in observed] == ["dataset-a", "dataset-b"]
    assert durable_states == [("batch-b",), ()]
    assert outbox.pending() == ()


def test_timeout_remains_pending_with_durable_error_type(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue(outbox, "dataset-a", "batch-timeout")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_TimeoutClient(),
        session_id="session-1",
        destination_id="telemetry",
    )

    result = asyncio.run(queue.run_once())

    assert result.attempted == 1
    assert result.committed == 0
    assert result.pending == 1
    entry = outbox.pending()[0]
    assert entry.state == "pending"
    assert entry.attempt_count == 1
    assert entry.last_error == "TimeoutError"


def test_sync_observer_failure_after_commit_does_not_reclassify_delivery(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue(outbox, "dataset-a", "batch-sync-observer-failure")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )

    def fail_after_commit(_progress: RecorderDeliveryProgress) -> None:
        raise RuntimeError("progress sink unavailable")

    result = asyncio.run(queue.run_once(progress_observer=fail_after_commit))

    assert result.attempted == 1
    assert result.committed == 1
    assert result.pending == 0
    assert result.blocked_datasets == ()
    assert outbox.pending() == ()
    completed = outbox.get(1)
    assert completed is not None
    assert completed.state == "completed"
    assert completed.attempt_count == 0
    assert completed.last_error is None


def test_async_observer_failure_after_commit_does_not_reclassify_delivery(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue(outbox, "dataset-a", "batch-async-observer-failure")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )

    async def fail_after_commit(_progress: RecorderDeliveryProgress) -> None:
        raise RuntimeError("progress sink unavailable")

    result = asyncio.run(queue.run_once(progress_observer=fail_after_commit))

    assert result.attempted == 1
    assert result.committed == 1
    assert result.pending == 0
    assert result.blocked_datasets == ()
    assert outbox.pending() == ()
    completed = outbox.get(1)
    assert completed is not None
    assert completed.state == "completed"
    assert completed.attempt_count == 0
    assert completed.last_error is None


def test_observer_failure_after_commit_does_not_mark_completed_row_pending(tmp_path) -> None:
    outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
    _enqueue(outbox, "dataset-a", "batch-observer-failure")
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )

    async def fail_after_commit(_progress: RecorderDeliveryProgress) -> None:
        raise RuntimeError("progress sink unavailable")

    result = asyncio.run(queue.run_once(progress_observer=fail_after_commit))

    assert result.attempted == 1
    assert result.committed == 1
    assert result.pending == 0
    assert result.blocked_datasets == ()
    assert outbox.pending() == ()
    completed = outbox.get(1)
    assert completed is not None
    assert completed.state == "completed"
    assert completed.attempt_count == 0
    assert completed.last_error is None


@pytest.mark.parametrize("commit_before_error", [False, True])
def test_ambiguous_sqlite_ack_error_is_reconciled_from_durable_row(
    tmp_path, caplog, commit_before_error: bool
) -> None:
    outbox = SQLiteOutbox(tmp_path / "ack-failure.sqlite3")
    _enqueue(outbox, "dataset-ack", "batch-ack-error")
    original_acknowledge = outbox.acknowledge

    def acknowledge_then_error(outbox_id: int, *, now: datetime) -> None:
        if commit_before_error:
            original_acknowledge(outbox_id, now=now)
        raise sqlite3.OperationalError("database is locked")

    outbox.acknowledge = acknowledge_then_error
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )
    caplog.set_level(logging.INFO, logger="catalog.federation.recorder_delivery")

    result = asyncio.run(queue.run_once())
    entry = outbox.get(1)
    assert entry is not None
    if commit_before_error:
        assert result.committed == 1
        assert result.pending == 0
        assert entry.state == "completed"
    else:
        assert result.committed == 0
        assert result.pending == 1
        assert entry.state == "pending"
        assert entry.attempt_count == 1
    records = [
        record
        for record in caplog.records
        if getattr(record, "storage_batch_id", None) == "batch-ack-error"
    ]
    assert len(records) == 1
    assert entry.content_hash == "a" * 64
    if commit_before_error:
        assert records[0].storage_stage == "delivery_ack_error_but_completed"
        assert records[0].storage_exception_type == "OperationalError"
        assert records[0].storage_error_code == "OperationalError"
        assert records[0].storage_outbox_id == 1
        assert records[0].storage_content_sha256 == "a" * 64
        assert records[0].storage_session_id == "session-1"
        assert records[0].storage_destination_id == "telemetry"
        assert records[0].storage_batch_id == "batch-ack-error"
    else:
        assert records[0].storage_stage == "delivery_ack_failed"
        assert records[0].storage_exception_type == "OperationalError"
        assert records[0].storage_content_sha256 == "a" * 64
        assert records[0].storage_retry_state_persisted is True
        assert entry.attempt_count == 1
        assert entry.last_error == "sqlite3.OperationalError"
    assert "database is locked" not in caplog.text



@pytest.mark.parametrize(
    "identity_field",
    ["session_id", "destination_id", "schema_id", "idempotency_key", "content_hash"],
)
def test_ack_error_with_different_durable_identity_refuses(
    tmp_path, identity_field: str
) -> None:
    outbox = SQLiteOutbox(tmp_path / "ack-identity.sqlite3")
    _enqueue(outbox, "dataset-ack", "batch-ack-error")
    original_get = outbox.get

    def acknowledge_error(_outbox_id: int, *, now: datetime) -> None:
        raise sqlite3.OperationalError("database is locked")

    def unrelated_identity(outbox_id: int):
        entry = original_get(outbox_id)
        assert entry is not None
        value = getattr(entry, identity_field)
        replacement = "b" * 64 if identity_field == "content_hash" else f"other-{value}"
        return replace(entry, **{identity_field: replacement})

    outbox.acknowledge = acknowledge_error
    outbox.get = unrelated_identity
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )

    with pytest.raises(FederationValidationError) as refused:
        asyncio.run(queue.run_once())

    assert refused.value.code == "outbox-acknowledgement-state-unknown"
    outbox.get = original_get
    entry = outbox.get(1)
    assert entry is not None
    assert entry.state == "pending"
    assert entry.content_hash == "a" * 64
    assert entry.attempt_count == 0

def test_ack_readback_error_refuses_unverified_completion_and_preserves_row(
    tmp_path, caplog
) -> None:
    outbox = SQLiteOutbox(tmp_path / "ack-readback.sqlite3")
    _enqueue(outbox, "dataset-ack", "batch-ack-readback")
    original_get = outbox.get

    def acknowledge_error(_outbox_id: int, *, now: datetime) -> None:
        raise sqlite3.OperationalError("ack write failed")

    def readback_error(_outbox_id: int):
        raise sqlite3.OperationalError("verification read failed")

    outbox.acknowledge = acknowledge_error
    outbox.get = readback_error
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )
    caplog.set_level(logging.INFO, logger="catalog.federation.recorder_delivery")

    with pytest.raises(sqlite3.OperationalError, match="ack write failed") as raised:
        asyncio.run(queue.run_once())

    assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
    event = next(
        record
        for record in caplog.records
        if getattr(record, "storage_stage", None)
        == "delivery_ack_state_unavailable"
    )
    assert event.storage_ack_exception_type == "OperationalError"
    assert event.storage_state_exception_type == "OperationalError"
    assert event.storage_content_sha256 == "a" * 64
    assert event.storage_batch_id == "batch-ack-readback"
    assert "verification read failed" not in caplog.text

    outbox.get = original_get
    entry = outbox.get(1)
    assert entry is not None
    assert entry.state == "pending"
    assert entry.attempt_count == 0
    assert entry.content_hash == "a" * 64
def test_ack_and_retry_state_write_failures_leave_row_pending(tmp_path, caplog) -> None:
    outbox = SQLiteOutbox(tmp_path / "ack-retry-write-failure.sqlite3")
    _enqueue(outbox, "dataset-ack", "batch-ack-retry-write-failure")
    original_acknowledge = outbox.acknowledge
    original_get = outbox.get
    original_record_failure = outbox.record_failure

    def acknowledge_error(_outbox_id: int, *, now: datetime) -> None:
        raise sqlite3.OperationalError("ack write failed")

    def retry_write_error(
        _outbox_id: int,
        *,
        error: str,
        now: datetime,
        base_delay_seconds: int = 1,
        max_delay_seconds: int = 3600,
    ):
        raise sqlite3.OperationalError("retry state write failed")

    outbox.acknowledge = acknowledge_error
    outbox.record_failure = retry_write_error
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=_CommitClient(),
        session_id="session-1",
        destination_id="telemetry",
    )
    caplog.set_level(logging.INFO, logger="catalog.federation.recorder_delivery")

    with pytest.raises(sqlite3.OperationalError, match="retry state write failed"):
        asyncio.run(queue.run_once())

    event = next(
        record
        for record in caplog.records
        if getattr(record, "storage_stage", None)
        == "delivery_ack_failure_not_persisted"
    )
    assert event.storage_ack_exception_type == "OperationalError"
    assert event.storage_persist_exception_type == "OperationalError"
    assert event.storage_batch_id == "batch-ack-retry-write-failure"
    assert event.storage_content_sha256 == "a" * 64
    assert "ack write failed" not in caplog.text
    assert "retry state write failed" not in caplog.text

    outbox.acknowledge = original_acknowledge
    outbox.get = original_get
    outbox.record_failure = original_record_failure
    entry = outbox.get(1)
    assert entry is not None
    assert entry.state == "pending"
    assert entry.attempt_count == 0
    assert entry.content_hash == "a" * 64


def test_pending_ack_failure_completes_on_due_normal_retry(tmp_path) -> None:
    now = [datetime(2026, 10, 2, 7, 0, tzinfo=UTC)]
    outbox = SQLiteOutbox(tmp_path / "ack-normal-retry.sqlite3")
    _enqueue(outbox, "dataset-ack", "batch-ack-retry")
    original_acknowledge = outbox.acknowledge
    acknowledge_calls = 0
    requests: list[dict[str, object]] = []

    class RecordingCommitClient:
        async def ingest_batch(self, **kwargs):
            requests.append(kwargs)
            return PhaseDIngestOutcome(committed=True)

    def fail_first_ack(outbox_id: int, *, now: datetime):
        nonlocal acknowledge_calls
        acknowledge_calls += 1
        if acknowledge_calls == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_acknowledge(outbox_id, now=now)

    outbox.acknowledge = fail_first_ack
    queue = DurableRecorderDeliveryQueue(
        outbox=outbox,
        client=RecordingCommitClient(),
        session_id="session-1",
        destination_id="telemetry",
        clock=lambda: now[0],
    )

    first = asyncio.run(queue.run_once())
    pending = outbox.get(1)
    assert pending is not None
    assert first.committed == 0
    assert first.pending == 1
    assert pending.state == "pending"
    assert pending.attempt_count == 1
    assert pending.last_error == "sqlite3.OperationalError"
    original_hash = pending.content_hash

    now[0] = pending.next_attempt_at
    second = asyncio.run(queue.run_once())
    completed = outbox.get(1)
    assert completed is not None
    assert second.committed == 1
    assert second.pending == 0
    assert completed.state == "completed"
    assert completed.content_hash == original_hash
    assert acknowledge_calls == 2
    assert len(requests) == 2
    assert requests[0]["idempotency_key"] == requests[1]["idempotency_key"]
    assert requests[0]["content"] == requests[1]["content"]

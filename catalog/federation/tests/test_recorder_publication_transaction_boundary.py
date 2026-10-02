from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue
from catalog.federation.recorder_storage_relay import (
    RECORDER_LOGICAL_STORAGE_KIND,
    RecorderLogicalStorageAuthority,
    RelayRecorderStorageClient,
)

SESSION = "session-1"
RECORDER = "node-recorder"
AUTHORITY = "node-authority"
GROUP = "telemetry"
DATASET = "mtconnect:node-recorder:Mazak"
RAW_HASH = "sha256:" + "a" * 64
BATCH = f"Mazak:1:1:2:{RAW_HASH.removeprefix('sha256:')}"
IDEMPOTENCY = f"{SESSION}:{DATASET}:{BATCH}"
CONTENT = {
    "schema": "fcp.mtconnect.observations.v1",
    "source_name": "Mazak",
    "machine_id": "machine-1",
    "agent_instance_id": 1,
    "first_sequence": 1,
    "last_sequence": 2,
    "raw_batch_first_sequence": 1,
    "raw_batch_last_sequence": 2,
    "raw_sha256": RAW_HASH,
    "probe_sha256": None,
    "observation_count": 2,
    "observations": [
        {"sequence": 1, "data_item_id": "execution", "value": "ACTIVE"},
        {"sequence": 2, "data_item_id": "execution", "value": "READY"},
    ],
}
CREATED_AT = datetime(2026, 10, 1, 18, 0, tzinfo=UTC)


class _IdempotentProvider:
    """Small durable-commit double with a one-time post-commit ingest timeout."""

    def __init__(self, *, timeout_after_commit: bool) -> None:
        self.rows: dict[str, str] = {}
        self.calls: list[dict[str, object]] = []
        self.timeout_after_commit = timeout_after_commit

    async def ingest_batch(self, **values):
        self.calls.append(dict(values))
        key = values["idempotency_key"]
        digest = hashlib.sha256(
            json.dumps(values["content"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        old = self.rows.get(key)
        if old is not None and old != digest:
            raise AssertionError("stable idempotency key was retried with changed content")
        if old is None:
            self.rows[key] = digest
            if self.timeout_after_commit:
                self.timeout_after_commit = False
                raise TimeoutError("provider committed before its ingest response timed out")
        return PhaseDIngestOutcome(committed=True)


class _LoopbackNetwork:
    def __init__(self, provider: _IdempotentProvider, *, lose_first_authority_response: bool):
        self.provider = provider
        self.lose_first_authority_response = lose_first_authority_response
        self.sender_queue: asyncio.Queue[object] = asyncio.Queue()
        self.authority: RecorderLogicalStorageAuthority | None = None
        self.handler_tasks: list[asyncio.Task[None]] = []
        self.authority_requests: list[dict[str, object]] = []
        self.authority_response_attempts = 0

    async def send(self, sender: str, *, session_id: str, target_node_id: str,
                   payload: dict[str, object], request_id: str | None = None):
        if sender == RECORDER:
            assert target_node_id == AUTHORITY
            assert self.authority is not None
            assert payload.get("kind") == RECORDER_LOGICAL_STORAGE_KIND
            self.authority_requests.append(dict(payload))
            task = asyncio.create_task(
                self.authority.handle_request(RECORDER, session_id, dict(payload))
            )
            self.handler_tasks.append(task)
            return {"delivered": True}

        assert sender == AUTHORITY and target_node_id == RECORDER
        self.authority_response_attempts += 1
        if self.lose_first_authority_response and self.authority_response_attempts == 1:
            return {"delivered": False}
        await self.sender_queue.put(
            SimpleNamespace(
                actor_node_id=AUTHORITY,
                session_id=session_id,
                request_id=request_id,
                payload=dict(payload),
            )
        )
        return {"delivered": True}

    async def drain_handlers(self) -> list[BaseException | None]:
        if not self.handler_tasks:
            return []
        results = await asyncio.gather(*self.handler_tasks, return_exceptions=True)
        self.handler_tasks.clear()
        return [item if isinstance(item, BaseException) else None for item in results]


class _RelayClient:
    def __init__(self, node_id: str, network: _LoopbackNetwork):
        self.node_id = node_id
        self.network = network

    async def send_message(self, **kwargs):
        return await self.network.send(self.node_id, **kwargs)

    async def receive_message(self, *, timeout: float | None = None):
        if timeout is None:
            return await self.network.sender_queue.get()
        return await asyncio.wait_for(self.network.sender_queue.get(), timeout)


class _Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 2, 0, 0, tzinfo=UTC)

    def __call__(self):
        return self.value


@pytest.mark.parametrize(
    ("failure_stage", "expected_first_stages"),
    [
        (
            "provider-ingest-timeout-after-commit",
            [
                "authority_request_received",
                "authority_ingest_started",
                "authority_ingest_wait_timeout",
            ],
        ),
        (
            "authority-response-delivery-failure-after-commit",
            [
                "authority_request_received",
                "authority_ingest_started",
                "authority_ingest_completed",
                "authority_response_delivery_started",
                "authority_response_delivery_failed",
            ],
        ),
    ],
)
def test_transaction_boundary_distinguishes_ingest_and_ack_failures_then_retries_idempotently(
    tmp_path, caplog, failure_stage, expected_first_stages
):
    async def run():
        provider = _IdempotentProvider(
            timeout_after_commit=failure_stage == "provider-ingest-timeout-after-commit"
        )
        network = _LoopbackNetwork(
            provider,
            lose_first_authority_response=(
                failure_stage == "authority-response-delivery-failure-after-commit"
            ),
        )
        authority_relay = _RelayClient(AUTHORITY, network)
        recorder_relay = _RelayClient(RECORDER, network)
        authority = RecorderLogicalStorageAuthority(
            client=authority_relay,
            logical_client=provider,
            session_id=SESSION,
        )
        network.authority = authority
        client = RelayRecorderStorageClient(
            recorder_relay,
            session_id=SESSION,
            authority_node_id=AUTHORITY,
            request_timeout=0.03,
        )
        outbox = SQLiteOutbox(tmp_path / "outbox.sqlite3")
        clock = _Clock()
        queue = DurableRecorderDeliveryQueue(
            outbox=outbox,
            client=client,
            session_id=SESSION,
            destination_id=GROUP,
            clock=clock,
        )
        entry, created = queue.enqueue(
            session_id=SESSION,
            group_id=GROUP,
            dataset_id=DATASET,
            batch_id=BATCH,
            idempotency_key=IDEMPOTENCY,
            content=CONTENT,
            created_at=CREATED_AT,
            dataset_schema_name="fcp.mtconnect.observations",
            dataset_schema_version=1,
        )
        assert created is True
        initial_hash = entry.content_hash

        caplog.set_level(logging.INFO, logger="catalog.federation.recorder_storage_relay")
        try:
            first = await queue.run_once()
            await network.drain_handlers()
            first_stages = [
                record.storage_stage for record in caplog.records
                if record.name == "catalog.federation.recorder_storage_relay"
                and hasattr(record, "storage_stage")
            ]
            assert first.attempted == 1 and first.committed == 0 and first.pending == 1
            pending = outbox.get(entry.outbox_id)
            assert pending is not None and pending.state == "pending"
            assert pending.attempt_count == 1
            assert pending.content_hash == initial_hash
            assert provider.rows[IDEMPOTENCY]
            assert network.authority_response_attempts == (
                0 if failure_stage == "provider-ingest-timeout-after-commit" else 1
            )
            assert first_stages == first_stages_expected

            caplog.clear()
            clock.value = pending.next_attempt_at
            second = await queue.run_once()
            await network.drain_handlers()
            second_stages = [
                record.storage_stage for record in caplog.records
                if record.name == "catalog.federation.recorder_storage_relay"
                and hasattr(record, "storage_stage")
            ]
            assert second.attempted == 1 and second.committed == 1 and second.pending == 0
            completed = outbox.get(entry.outbox_id)
            assert completed is not None and completed.state == "completed"
            assert completed.content_hash == initial_hash
            assert provider.rows[IDEMPOTENCY] and len(provider.rows) == 1
            assert len(provider.calls) == 2
            assert len(network.authority_requests) == 2
            requests = network.authority_requests
            assert [r["batch_id"] for r in requests] == [BATCH, BATCH]
            assert [r["idempotency_key"] for r in requests] == [IDEMPOTENCY, IDEMPOTENCY]
            assert requests[0]["correlation_id"] != requests[1]["correlation_id"]
            assert requests[0]["content"] == requests[1]["content"] == CONTENT
            assert second_stages == [
                "authority_request_received",
                "authority_ingest_started",
                "authority_ingest_completed",
                "authority_response_delivery_started",
                "authority_response_delivery_completed",
            ]
        finally:
            await client.close()

    first_stages_expected = expected_first_stages
    asyncio.run(run())

class _BlockingProvider:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.rows: dict[str, str] = {}
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **values):
        self.calls.append(dict(values))
        key = values["idempotency_key"]
        digest = hashlib.sha256(
            json.dumps(values["content"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        old = self.rows.get(key)
        if old is not None and old != digest:
            raise AssertionError("retry changed the committed idempotent payload")
        if old is None:
            self.rows[key] = digest
            self.entered.set()
            await self.release.wait()
        return PhaseDIngestOutcome(committed=True)


def test_cancelled_sender_keeps_outbox_pending_and_late_ack_cannot_complete_retry(tmp_path):
    async def run():
        provider = _BlockingProvider()
        network = _LoopbackNetwork(provider, lose_first_authority_response=False)
        authority_relay = _RelayClient(AUTHORITY, network)
        recorder_relay = _RelayClient(RECORDER, network)
        authority = RecorderLogicalStorageAuthority(
            client=authority_relay,
            logical_client=provider,
            session_id=SESSION,
        )
        network.authority = authority
        client = RelayRecorderStorageClient(
            recorder_relay,
            session_id=SESSION,
            authority_node_id=AUTHORITY,
            request_timeout=2.0,
        )
        outbox = SQLiteOutbox(tmp_path / "cancel-outbox.sqlite3")
        clock = _Clock()
        queue = DurableRecorderDeliveryQueue(
            outbox=outbox,
            client=client,
            session_id=SESSION,
            destination_id=GROUP,
            clock=clock,
        )
        entry, created = queue.enqueue(
            session_id=SESSION,
            group_id=GROUP,
            dataset_id=DATASET,
            batch_id=BATCH,
            idempotency_key=IDEMPOTENCY,
            content=CONTENT,
            created_at=CREATED_AT,
            dataset_schema_name="fcp.mtconnect.observations",
            dataset_schema_version=1,
        )
        assert created is True
        original_hash = entry.content_hash
        try:
            attempt = asyncio.create_task(queue.run_once())
            await asyncio.wait_for(provider.entered.wait(), timeout=1)
            attempt.cancel()
            with pytest.raises(asyncio.CancelledError):
                await attempt

            pending = outbox.get(entry.outbox_id)
            assert pending is not None and pending.state == "pending"
            assert pending.attempt_count == 0
            assert pending.content_hash == original_hash
            assert client._pending == {}

            provider.release.set()
            handler_results = await network.drain_handlers()
            assert handler_results == [None]
            assert len(provider.rows) == 1
            assert network.authority_response_attempts == 1
            assert len(network.authority_requests) == 1

            # The first acknowledgement was emitted only after sender cancellation.
            # It has the old correlation ID; it cannot satisfy the new request.
            retry = await queue.run_once()
            await network.drain_handlers()
            assert retry.attempted == 1 and retry.committed == 1 and retry.pending == 0
            completed = outbox.get(entry.outbox_id)
            assert completed is not None and completed.state == "completed"
            assert completed.content_hash == original_hash
            assert len(provider.rows) == 1
            assert len(provider.calls) == 2
            assert len(network.authority_requests) == 2
            first, second = network.authority_requests
            assert first["batch_id"] == second["batch_id"] == BATCH
            assert first["idempotency_key"] == second["idempotency_key"] == IDEMPOTENCY
            assert first["content"] == second["content"] == CONTENT
            assert first["correlation_id"] != second["correlation_id"]
            assert network.authority_response_attempts == 2
        finally:
            provider.release.set()
            await client.close()

    asyncio.run(run())

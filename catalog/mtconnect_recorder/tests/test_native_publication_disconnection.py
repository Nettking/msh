"""Local checkpoint reconciliation must survive native control-plane outage.

Software regression only: the existing native-loop transport fixture surrounds
real archive, checkpoint, incremental frontier, outbox and storage-provider work.
No real network, physical Recorder or acceptance duration is represented here.
"""
from __future__ import annotations

import asyncio
import json
import sys
from contextlib import suppress

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import OutboxState
from catalog.federation.storage_protocol import BatchIngestState
from catalog.federation.tests.test_b03_incremental_publication_frontier import (
    _mark_stored_pending,
)
from catalog.federation.tests.test_recorder_publication import (
    SAMPLE_XML,
    _second_sample,
    _store_sample,
    _write_checkpoint,
)
from catalog.mtconnect_recorder import DurableRecorderStore
from catalog.mtconnect_recorder.publication_frontier import RecorderPublicationFrontier
from catalog.mtconnect_recorder.tests.test_recorder_publication_store_failure import (
    GROUP,
    SESSION,
    _FaultyOutbox,
    _node,
    _ProviderBackedStorageClient,
    _quiesce,
    _state,
    _status,
)


class _TransportCycles:
    def __init__(self, fault_site: str) -> None:
        self.fault_site = fault_site
        self.entered: asyncio.Queue[int] = asyncio.Queue()
        self.permit: asyncio.Queue[bool] = asyncio.Queue()
        self.online = True
        self.attempts = 0
        self.offline_failures = 0
        self.successful_status_reads = 0

    def _offline(self) -> None:
        self.offline_failures += 1
        raise FederationOperationError(
            "pairing-relay-disconnected", "controlled native-loop transport outage",
        )

    async def _ensure_connected(self, _state) -> None:
        self.attempts += 1
        self.entered.put_nowait(self.attempts)
        self.online = await self.permit.get()
        if not self.online and self.fault_site == "ensure-connected":
            self._offline()

    def _connected_client(self):
        return self

    async def coordinator_status(self):
        if not self.online:
            assert self.fault_site == "coordinator-status"
            self._offline()
        self.successful_status_reads += 1
        return _status()


class _AuthorityGuardClient(_ProviderBackedStorageClient):
    def __init__(self, provider, transport: _TransportCycles) -> None:
        super().__init__(provider)
        self.transport = transport
        self.offline_offers = 0

    async def ingest_batch(self, **kwargs):
        if not self.transport.online:
            self.offline_offers += 1
            raise FederationOperationError(
                "pairing-relay-disconnected", "no current control-plane route",
            )
        return await super().ingest_batch(**kwargs)


async def _next_transport_boundary(transport: _TransportCycles, runner) -> int:
    next_attempt = asyncio.create_task(transport.entered.get())
    try:
        done, _pending = await asyncio.wait(
            {next_attempt, runner}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED,
        )
        if runner in done:
            await runner
            pytest.fail("native publication loop exited before the next cycle")
        assert next_attempt in done, "native publication cycle boundary was not reached"
        return next_attempt.result()
    finally:
        next_attempt.cancel()
        with suppress(asyncio.CancelledError):
            await next_attempt


@pytest.mark.parametrize("fault_site", ["ensure-connected", "coordinator-status"])
def test_native_loop_reconciles_new_checkpoint_during_control_plane_outage(
    tmp_path, monkeypatch, fault_site: str,
) -> None:
    async def scenario() -> None:
        transport = _TransportCycles(fault_site)
        provider = FilesystemBatchStorageProvider(tmp_path / "storage")
        storage_client = _AuthorityGuardClient(provider, transport)
        outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
        node = _node(tmp_path, outbox, storage_client, monkeypatch)
        node.runtime = transport
        node.source_names = ("Mazak",)
        store = DurableRecorderStore(tmp_path)
        frontier = RecorderPublicationFrontier(store)
        checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
        probe, _first, _stored = _store_sample(store, SAMPLE_XML)
        _write_checkpoint(checkpoint, probe_sha256=probe.sha256, next_sequence=13)

        runner = asyncio.create_task(node._publication_loop(_state()))
        try:
            assert await _next_transport_boundary(transport, runner) == 1
            transport.permit.put_nowait(True)
            # This boundary follows an actual complete healthy outer-loop cycle,
            # including the real installed incremental reconciler and local ACK.
            assert await _next_transport_boundary(transport, runner) == 2
            assert node.snapshot().storage_state == "up-to-date"
            assert storage_client.states == [BatchIngestState.STORED]
            assert len(outbox.acknowledged) == 1
            assert outbox.pending() == ()
            first_ack = outbox.acknowledged[0]
            first_batch_id = storage_client.batch_ids[0]
            assert frontier.pending(source_name="Mazak", instance_id=77) == ()

            # The physical transport condition changes independently of the next
            # connection attempt, so even premature cached-route delivery is refused.
            transport.online = False
            # Use the existing B03 producer helpers for an actual newer archive,
            # checkpoint and durable discovery record; never enqueue it ourselves.
            _probe, second, stored = _store_sample(store, _second_sample())
            _mark_stored_pending(frontier, batch=second, stored=stored)
            _write_checkpoint(checkpoint, probe_sha256=probe.sha256, next_sequence=16)
            assert len(frontier.pending(source_name="Mazak", instance_id=77)) == 1
            transport.permit.put_nowait(False)
            # The next entry proves that the classified failure cycle finished.
            # No sleep duration is used to infer that work did or did not happen.
            assert await _next_transport_boundary(transport, runner) == 3
            assert transport.offline_failures == 1
            assert transport.successful_status_reads == 1
            assert node.snapshot().status == "retrying"
            assert node.snapshot().last_error_code == "pairing-relay-disconnected"
            assert storage_client.offline_offers == 0, "stale authority was used for delivery"
            assert storage_client.states == [BatchIngestState.STORED]
            assert storage_client.batch_ids == [first_batch_id]
            assert outbox.acknowledged == [first_ack]
            pending = outbox.pending()
            diagnostic_evidence = {
                "diagnostic": "native-publication-disconnection",
                "stage": "completed-offline-cycle",
                "fault_site": fault_site,
                "healthy_cycles_completed": 1,
                "classified_offline_failures": transport.offline_failures,
                "next_transport_boundary": transport.attempts,
                "checkpoint_next_sequence": 16,
                "remote_stored_before": 1,
                "remote_stored_after": storage_client.states.count(BatchIngestState.STORED),
                "remote_offline_offers": storage_client.offline_offers,
                "local_ack_count": len(outbox.acknowledged),
                "new_local_pending_count": len(pending),
            }
            print(json.dumps(diagnostic_evidence, sort_keys=True), file=sys.stderr, flush=True)
            assert len(pending) == 1, (
                "native control-plane outage prevented local reconciliation of the new checkpoint"
            )
            assert pending[0].session_id == SESSION
            assert pending[0].destination_id == GROUP
            assert pending[0].payload["batch_id"] != first_batch_id
            assert frontier.pending(source_name="Mazak", instance_id=77) == ()
            second_id = pending[0].outbox_id
            second_batch_id = pending[0].payload["batch_id"]

            transport.permit.put_nowait(True)
            assert await _next_transport_boundary(transport, runner) == 4
            assert transport.successful_status_reads == 2
            assert node.snapshot().storage_state == "up-to-date"
            assert outbox.pending() == ()
            assert outbox.get(second_id).state is OutboxState.COMPLETED
            assert outbox.acknowledged == [first_ack, second_id]
            assert storage_client.batch_ids == [first_batch_id, second_batch_id]
            assert storage_client.states == [BatchIngestState.STORED] * 2
            assert len(tuple(provider.batch_root.rglob("*.json"))) == 2
            assert provider.read(
                session_id=SESSION, group_id=GROUP, batch_id=second_batch_id,
            ) is not None
            # One further genuine unchanged-checkpoint cycle must not redeliver.
            transport.permit.put_nowait(True)
            assert await _next_transport_boundary(transport, runner) == 5
            assert storage_client.batch_ids == [first_batch_id, second_batch_id]
            assert outbox.acknowledged == [first_ack, second_id]
        finally:
            _quiesce(node, runner)
            with suppress(asyncio.CancelledError):
                await asyncio.wait_for(runner, timeout=5.0)
            assert runner.done()
            assert storage_client.closed

    asyncio.run(scenario())

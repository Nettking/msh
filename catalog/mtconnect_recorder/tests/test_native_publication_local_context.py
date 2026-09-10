"""Local enqueue context must not become cached remote delivery authority."""
from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager, closing, suppress
from types import SimpleNamespace

import pytest

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.storage_protocol import BatchIngestState
from catalog.federation.tests.test_b03_incremental_publication_frontier import (
    _mark_stored_pending,
)
from catalog.federation.tests.test_recorder_publication import (
    SAMPLE_XML,
    _store_sample,
    _write_checkpoint,
)
from catalog.mtconnect_recorder import DurableRecorderStore
from catalog.mtconnect_recorder import federation_node as node_module
from catalog.mtconnect_recorder.publication_frontier import RecorderPublicationFrontier
from catalog.mtconnect_recorder.tests.test_native_publication_disconnection import (
    _AuthorityGuardClient,
    _next_transport_boundary,
    _TransportCycles,
)
from catalog.mtconnect_recorder.tests.test_recorder_publication_store_failure import (
    GROUP,
    SESSION,
    _FaultyOutbox,
    _node,
    _quiesce,
    _state,
    _status,
)


class _StatusView:
    def __init__(self, transport):
        self.transport = transport

    async def coordinator_status(self):
        return await self.transport.coordinator_status()


class _RoutedTransport(_TransportCycles):
    def __init__(self):
        super().__init__("coordinator-status")
        self.current_client = self
        self.status_value = _status()

    def _connected_client(self):
        return self.current_client

    async def coordinator_status(self):
        await super().coordinator_status()
        return self.status_value


class _MalformedSelection(dict):
    """A classified decoder fault occurs during actual selection, not the RPC."""

    def get(self, _key, _default=None):
        raise FederationValidationError(
            "malformed-local-context-status", "sessions", "controlled decoded-status fault",
        )


class _FailedRouteStart:
    def __init__(self):
        self.started = 0
        self.closed = False

    async def start(self):
        self.started += 1
        raise FederationOperationError("local-context-route-start", "controlled route failure")

    async def close(self):
        self.closed = True


def _produce(context, first_sequence: int) -> None:
    value = SAMPLE_XML
    replacements = (
        ('firstSequence="10"', f'firstSequence="{first_sequence}"'),
        ('lastSequence="12"', f'lastSequence="{first_sequence + 2}"'),
        ('nextSequence="13"', f'nextSequence="{first_sequence + 3}"'),
        ('sequence="10"', f'sequence="{first_sequence}"'),
        ('sequence="11"', f'sequence="{first_sequence + 1}"'),
        ('sequence="12"', f'sequence="{first_sequence + 2}"'),
    )
    for before, after in replacements:
        value = value.replace(before, after)
    _probe, batch, stored = _store_sample(context.store, value)
    _mark_stored_pending(context.frontier, batch=batch, stored=stored)
    _write_checkpoint(
        context.checkpoint,
        probe_sha256=context.probe.sha256,
        next_sequence=first_sequence + 3,
    )


@asynccontextmanager
async def _healthy_context(tmp_path, monkeypatch):
    transport = _RoutedTransport()
    provider = FilesystemBatchStorageProvider(tmp_path / "storage")
    client = _AuthorityGuardClient(provider, transport)
    outbox = _FaultyOutbox(tmp_path / "outbox.sqlite3")
    node = _node(tmp_path, outbox, client, monkeypatch)
    node.runtime = transport
    node.source_names = ("Mazak",)
    store = DurableRecorderStore(tmp_path)
    frontier = RecorderPublicationFrontier(store)
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    probe, _batch, _stored = _store_sample(store, SAMPLE_XML)
    _write_checkpoint(checkpoint, probe_sha256=probe.sha256, next_sequence=13)
    context = SimpleNamespace(
        transport=transport, provider=provider, client=client, outbox=outbox,
        node=node, store=store, frontier=frontier, checkpoint=checkpoint, probe=probe,
    )
    runner = asyncio.create_task(node._publication_loop(_state()))
    context.runner = runner
    try:
        assert await _next_transport_boundary(transport, runner) == 1
        transport.permit.put_nowait(True)
        assert await _next_transport_boundary(transport, runner) == 2
        assert node.snapshot().storage_state == "up-to-date"
        assert client.states == [BatchIngestState.STORED]
        assert len(outbox.acknowledged) == 1
        assert outbox.pending() == ()
        assert frontier.pending(source_name="Mazak", instance_id=77) == ()
        context.original_ack = outbox.acknowledged[0]
        context.original_batch = client.batch_ids[0]
        yield context
    finally:
        _quiesce(node, runner)
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(runner, timeout=5.0)
        assert runner.done()
        assert client.closed


def _assert_no_new_remote_work(context) -> None:
    assert context.client.offline_offers == 0
    assert context.client.states == [BatchIngestState.STORED]
    assert context.client.batch_ids == [context.original_batch]
    assert context.outbox.acknowledged == [context.original_ack]


def test_replacement_announcement_failure_keeps_local_enqueue_and_pending_scope(
    tmp_path, monkeypatch,
) -> None:
    async def scenario():
        async with _healthy_context(tmp_path, monkeypatch) as context:
            context.transport.online = False
            context.transport.current_client = _StatusView(context.transport)
            announce_calls = []

            async def fail_announcement(state):
                announce_calls.append(state.binding.internal_session_id)
                raise FederationOperationError(
                    "local-context-announcement", "controlled replacement announcement failure",
                )

            context.node._announce_connected = fail_announcement
            _produce(context, 13)
            context.transport.permit.put_nowait(False)
            assert await _next_transport_boundary(context.transport, context.runner) == 3
            assert announce_calls == [SESSION]
            assert context.client.closed
            assert context.transport.successful_status_reads == 1
            pending = context.outbox.pending()
            assert len(pending) == 1
            assert pending[0].session_id == SESSION
            assert pending[0].destination_id == GROUP
            assert context.frontier.pending(source_name="Mazak", instance_id=77) == ()
            snapshot = context.node.snapshot()
            assert snapshot.status == "retrying"
            assert snapshot.last_error_code == "local-context-announcement"
            assert snapshot.pending_batches == 1
            _assert_no_new_remote_work(context)

    asyncio.run(scenario())


@pytest.mark.parametrize("selection", ["new-group", "unavailable", "malformed"])
def test_fresh_selection_invalidates_old_local_group_before_a_later_outage(
    tmp_path, monkeypatch, selection,
) -> None:
    async def scenario():
        async with _healthy_context(tmp_path, monkeypatch) as context:
            route = _FailedRouteStart()
            if selection == "new-group":
                status = _status()
                status["capabilities"][0]["properties"]["group_ids"] = ["archive"]
                monkeypatch.setattr(
                    node_module, "RelayRecorderStorageClient", lambda *_a, **_kw: route,
                )
            elif selection == "unavailable":
                status = _status()
                status["capabilities"] = []
            else:
                status = _MalformedSelection()
            context.transport.status_value = status
            _produce(context, 13)
            context.transport.permit.put_nowait(True)
            assert await _next_transport_boundary(context.transport, context.runner) == 3
            assert context.transport.successful_status_reads == 2
            if selection == "new-group":
                assert route.started == 1
                assert route.closed
                assert context.node.snapshot().last_error_code == "local-context-route-start"
            elif selection == "unavailable":
                assert context.node.snapshot().storage_state == "authority-unavailable"
            else:
                assert context.node.snapshot().last_error_code == "malformed-local-context-status"
            assert context.outbox.pending() == ()
            assert len(context.frontier.pending(source_name="Mazak", instance_id=77)) == 1

            context.transport.online = False
            context.transport.permit.put_nowait(False)
            assert await _next_transport_boundary(context.transport, context.runner) == 4
            assert context.transport.offline_failures == 1
            assert context.node.snapshot().last_error_code == "pairing-relay-disconnected"
            assert context.outbox.pending() == ()
            assert len(context.frontier.pending(source_name="Mazak", instance_id=77)) == 1
            _assert_no_new_remote_work(context)

    asyncio.run(scenario())


def test_local_enqueue_sql_failure_is_visible_and_keeps_last_proved_pending_count(
    tmp_path, monkeypatch,
) -> None:
    async def scenario():
        async with _healthy_context(tmp_path, monkeypatch) as context:
            context.transport.online = False
            _produce(context, 13)
            context.transport.permit.put_nowait(False)
            assert await _next_transport_boundary(context.transport, context.runner) == 3
            assert len(context.outbox.pending()) == 1
            assert context.node.snapshot().pending_batches == 1
            # A real SQLite trigger aborts only the next local INSERT immediately;
            # production connection flags and busy/durability policies stay intact.
            with closing(sqlite3.connect(context.outbox.database)) as database:
                database.execute(
                    "CREATE TRIGGER local_context_reject_enqueue BEFORE INSERT ON outbox "
                    "BEGIN SELECT RAISE(ABORT, 'controlled local enqueue failure'); END"
                )
                database.commit()
            _produce(context, 16)
            # The existing durable-store fixture also makes the inventory unreadable:
            # the already proved count must survive rather than becoming a false zero.
            context.outbox.read_fault = sqlite3.OperationalError("controlled inventory read fault")
            context.transport.permit.put_nowait(False)
            assert await _next_transport_boundary(context.transport, context.runner) == 4
            snapshot = context.node.snapshot()
            assert snapshot.status == "retrying"
            assert snapshot.last_error_code == "IntegrityError"
            assert snapshot.pending_batches == 1
            assert context.transport.offline_failures == 2
            assert len(context.frontier.pending(source_name="Mazak", instance_id=77)) == 1
            with closing(sqlite3.connect(context.outbox.database)) as database:
                assert database.execute(
                    "SELECT COUNT(*) FROM outbox WHERE state='pending'"
                ).fetchone()[0] == 1
            _assert_no_new_remote_work(context)

    asyncio.run(scenario())

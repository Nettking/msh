"""Slow atomic phases keep loop progress without abandoning durable writers."""
from __future__ import annotations

import asyncio
import contextvars
import json
import threading
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

from catalog.federation import storage_async
from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.tests.test_phase_e1_service_manifest import (
    GROUP_ID,
    NOW,
    PRIMARY_ID,
    SESSION_ID,
    _envelope,
    _request,
    _runtime,
)


def wait_event(event):
    return asyncio.wait_for(asyncio.to_thread(event.wait, 3), 4)


@pytest.mark.parametrize("failure", [False, True])
def test_repeated_cancellation_drains_owned_atomic_phase_and_preserves_error(failure):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    owner = SimpleNamespace()

    def operation():
        entered.set()
        try:
            assert release.wait(3)
            if failure:
                raise ValueError("original worker failure")
            return "durable"
        finally:
            finished.set()

    async def scenario():
        task = asyncio.create_task(storage_async.owned_storage_call(owner, operation))
        try:
            assert await wait_event(entered)
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), .5)
            task.cancel()
            await asyncio.sleep(.01)
            task.cancel()
            await asyncio.sleep(.01)
            assert not task.done()
            assert owner._storage_async_work.lock.locked()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError) as cancellation:
            await task
        assert finished.is_set()
        assert not owner._storage_async_work.lock.locked()
        if failure:
            assert isinstance(cancellation.value.__cause__, ValueError)

    asyncio.run(scenario())


def test_waiting_phase_cancelled_before_admission_never_starts_writer():
    entered, release = threading.Event(), threading.Event()
    owner, writes = SimpleNamespace(), []

    def held():
        entered.set()
        assert release.wait(3)

    async def scenario():
        first = asyncio.create_task(storage_async.owned_storage_call(owner, held))
        try:
            assert await wait_event(entered)
            second = asyncio.create_task(storage_async.owned_storage_call(owner, writes.append, "unexpected"))
            await asyncio.sleep(.01)
            second.cancel()
            with pytest.raises(asyncio.CancelledError):
                await second
            assert writes == []
            assert owner._storage_async_work.waiting == 0
        finally:
            release.set()
            await first

    asyncio.run(scenario())


def test_shared_control_serializes_and_bounds_waiters(monkeypatch):
    monkeypatch.setattr(storage_async, "_MAX_WAITING_STORAGE_PHASES", 1)
    owner, entered, release, order = SimpleNamespace(), threading.Event(), threading.Event(), []

    def held():
        order.append("first")
        entered.set()
        assert release.wait(3)

    async def scenario():
        first = asyncio.create_task(storage_async.owned_storage_call(owner, held))
        try:
            assert await wait_event(entered)
            second = asyncio.create_task(storage_async.owned_storage_call(owner, order.append, "second"))
            await asyncio.sleep(.01)
            with pytest.raises(RuntimeError, match="capacity"):
                await storage_async.owned_storage_call(owner, order.append, "third")
            assert order == ["first"]
        finally:
            release.set()
            await first
        await second
        assert order == ["first", "second"]

    asyncio.run(scenario())


@pytest.mark.parametrize("after_provider", [False, True])
def test_cancelled_real_service_preserves_pending_and_replays_normally(tmp_path, monkeypatch, after_provider):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    entered, release = threading.Event(), threading.Event()
    target = runtime.service if after_provider else runtime.control
    name = "_commit_manifest" if after_provider else "prepare_batch_manifest"
    original = getattr(target, name)

    def held(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(target, name, held)

    async def scenario():
        task = asyncio.create_task(runtime.service.dispatch(_envelope(_request(), request_id="cancelled")))
        try:
            assert await wait_event(entered)
            task.cancel()
            await asyncio.sleep(.01)
            assert not task.done()  # Never report cancellation before durable drain.
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), .5)
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        status = runtime.acknowledgements.status(SESSION_ID, GROUP_ID, _request().batch_id)
        assert status.primary_committed is after_provider
        intent = runtime.control.manifests.intent(SESSION_ID, GROUP_ID, _request().batch_id)
        assert intent.committed_revision == (1 if after_provider else None)
        assert runtime.control.manifests.head(SESSION_ID, GROUP_ID).revision == int(after_provider)
        monkeypatch.setattr(target, name, original)
        response = await runtime.service.dispatch(_envelope(_request(), request_id="replay"))
        assert response.ok is True
        assert response.result["commit_state"] == "committed"
        assert runtime.control.manifests.head(SESSION_ID, GROUP_ID).revision == 1

    asyncio.run(scenario())


def test_control_revision_change_during_slow_prepare_refuses_provider_write(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    entered, release, writes = threading.Event(), threading.Event(), []
    original = runtime.control.prepare_batch_manifest

    def held(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(runtime.control, "prepare_batch_manifest", held)
    monkeypatch.setattr(runtime.provider, "ingest", lambda request: writes.append(request))

    async def scenario():
        task = asyncio.create_task(runtime.service.dispatch(_envelope(_request(), request_id="changed")))
        try:
            assert await wait_event(entered)
            runtime.control.create_group(SESSION_ID, "coordinator", "unrelated-new-group")
        finally:
            release.set()
        response = await task
        assert response.ok is False
        assert response.error.retryable is True
        assert writes == []

    asyncio.run(scenario())


def test_client_cancelled_during_prepare_never_sends_network_request(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    entered, release, network = threading.Event(), threading.Event(), []
    original = runtime.control.prepare_batch_manifest

    def held(*args, **kwargs):
        intent = original(*args, **kwargs)
        entered.set()
        assert release.wait(3)
        return intent

    class Transport:
        async def request(self, **kwargs):
            network.append(kwargs)
            return await runtime.service.dispatch(kwargs["envelope"])

    monkeypatch.setattr(runtime.control, "prepare_batch_manifest", held)
    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID, actor_node_id="sender", control_plane=runtime.control,
        transport=Transport(), acknowledgements=runtime.acknowledgements, clock=lambda: NOW,
    )

    async def scenario():
        task = asyncio.create_task(client.ingest(_request()))
        try:
            assert await wait_event(entered)
            task.cancel()
            await asyncio.sleep(.01)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert network == []
        assert runtime.control.manifests.intent(SESSION_ID, GROUP_ID, _request().batch_id).committed_revision is None
        monkeypatch.setattr(runtime.control, "prepare_batch_manifest", original)
        assert (await client.ingest(_request())).committed is True
        assert len(network) == 1

    asyncio.run(scenario())


def test_endpoint_repeated_close_waits_for_owned_writer_and_sends_no_success(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    entered, release, sent = threading.Event(), threading.Event(), []
    original = runtime.service._commit_manifest

    def held(*args):
        result = original(*args)
        entered.set()
        assert release.wait(3)
        return result

    class RelayClient:
        node_id = "provider-node"

        async def send_message(self, **kwargs):
            sent.append(kwargs)

    monkeypatch.setattr(runtime.service, "_commit_manifest", held)
    endpoint = RelayStorageEndpoint(RelayClient(), {runtime.service.provider_id: runtime.service})
    envelope = _envelope(_request(), request_id="endpoint-close")
    message = SimpleNamespace(session_id=SESSION_ID, actor_node_id=envelope.actor_node_id)
    payload = {"provider_id": runtime.service.provider_id, "frame": json.dumps(envelope.to_dict())}

    async def scenario():
        handler = asyncio.create_task(endpoint._handle_request(message, payload))
        endpoint._handler_tasks.add(handler)
        handler.add_done_callback(endpoint._finish_handler)
        try:
            assert await wait_event(entered)
            first_close = asyncio.create_task(endpoint.close())
            await asyncio.sleep(.01)
            second_close = asyncio.create_task(endpoint.close())
            await asyncio.sleep(.01)
            assert not first_close.done()
            assert not second_close.done()
            assert runtime.control._storage_async_work.lock.locked()
        finally:
            release.set()
        await asyncio.gather(first_close, second_close)
        assert handler.cancelled()
        assert sent == []
        assert not endpoint._handler_tasks
        assert not runtime.control._storage_async_work.lock.locked()
        assert runtime.acknowledgements.status(SESSION_ID, GROUP_ID, _request().batch_id).primary_committed

    asyncio.run(scenario())


def test_provider_write_keeps_original_owner_loop_exclusion(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    original, observations = runtime.provider.ingest, []
    owner_thread = threading.get_ident()

    async def scenario():
        loop = asyncio.get_running_loop()

        def rotate():
            observations.append("rotation")
            runtime.control.grant_leader(
                SESSION_ID, "coordinator", GROUP_ID, PRIMARY_ID, "grant-2", 2, 11,
                lease_expires_at=NOW + timedelta(minutes=20), occurred_at=NOW,
            )

        def ingest(request):
            assert threading.get_ident() == owner_thread
            loop.call_soon_threadsafe(rotate)
            result = original(request)
            assert runtime.control.snapshot(SESSION_ID).leader_grants[GROUP_ID]["grant_id"] == "grant-1"
            observations.append("provider-committed")
            return result

        monkeypatch.setattr(runtime.provider, "ingest", ingest)
        response = await runtime.service.dispatch(_envelope(_request(), request_id="write-exclusion"))
        assert observations == ["provider-committed", "rotation"]
        assert runtime.acknowledgements.status(SESSION_ID, GROUP_ID, _request().batch_id).primary_committed
        if not response.ok:
            # A control change racing the finalizer's transaction must refuse,
            # while retaining actual durable provider/ACK evidence for replay.
            assert response.error.retryable is True
        replay = replace(_request(), authority=replace(
            _request().authority, grant_id="grant-2", term=2, fencing_token=11,
            lease_expires_at=NOW + timedelta(minutes=20),
        ))
        monkeypatch.setattr(runtime.provider, "ingest", original)
        assert (await runtime.service.dispatch(_envelope(replay, request_id="after-rotation"))).ok is True

    asyncio.run(scenario())


def test_client_route_rotation_before_prepare_rejects_old_intent(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    sent = []

    class Transport:
        async def request(self, **kwargs):
            sent.append(kwargs)

    client = PhaseDLogicalStorageClient(
        session_id=SESSION_ID, actor_node_id="sender", control_plane=runtime.control,
        transport=Transport(), acknowledgements=runtime.acknowledgements, clock=lambda: NOW,
    )
    original = client._route

    async def scenario():
        loop = asyncio.get_running_loop()

        def rotate():
            runtime.control.grant_leader(
                SESSION_ID, "coordinator", GROUP_ID, PRIMARY_ID, "grant-2", 2, 11,
                lease_expires_at=NOW + timedelta(minutes=20), occurred_at=NOW,
            )

        def old_route(group_id):
            route = original(group_id)
            loop.call_soon_threadsafe(rotate)
            return route

        monkeypatch.setattr(client, "_route", old_route)
        with pytest.raises(FederationValidationError) as failure:
            await client.ingest(_request())
        assert failure.value.code == "manifest-authority-mismatch"
        assert sent == []
        assert not runtime.acknowledgements.status(SESSION_ID, GROUP_ID, _request().batch_id).primary_committed

    asyncio.run(scenario())


def test_offloop_operation_preserves_callers_context():
    variable = contextvars.ContextVar("storage-test-context", default="missing")
    owner = SimpleNamespace()

    async def scenario():
        token = variable.set("original-request")
        try:
            assert await storage_async.owned_storage_call(owner, variable.get) == "original-request"
            await storage_async.owned_storage_call(owner, variable.set, "worker-only")
            assert variable.get() == "original-request"
        finally:
            variable.reset(token)

    asyncio.run(scenario())


def test_grant_rotation_during_slow_prepare_refuses_old_provider_and_ack(tmp_path, monkeypatch):
    runtime = _runtime(tmp_path, acknowledgement_mode=AcknowledgementMode.PRIMARY)
    original, entered, release, writes = runtime.control.prepare_batch_manifest, threading.Event(), threading.Event(), []

    def held(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(runtime.control, "prepare_batch_manifest", held)
    monkeypatch.setattr(runtime.provider, "ingest", writes.append)

    async def scenario():
        task = asyncio.create_task(runtime.service.dispatch(_envelope(_request(), request_id="rotation")))
        try:
            assert await wait_event(entered)
            runtime.control.grant_leader(
                SESSION_ID, "coordinator", GROUP_ID, PRIMARY_ID, "grant-2", 2, 11,
                lease_expires_at=NOW + timedelta(minutes=20), occurred_at=NOW,
            )
        finally:
            release.set()
        response = await task
        assert response.ok is False
        assert writes == []
        assert not runtime.acknowledgements.status(SESSION_ID, GROUP_ID, _request().batch_id).primary_committed

    asyncio.run(scenario())


def test_wait_timeout_does_not_report_before_atomic_drain_or_advance_phase():
    owner, entered, release, advanced = SimpleNamespace(), threading.Event(), threading.Event(), []

    def operation():
        entered.set()
        assert release.wait(3)
        return "durable"

    async def request():
        await storage_async.owned_storage_call(owner, operation)
        advanced.append("false-completion")

    async def scenario():
        task = asyncio.create_task(asyncio.wait_for(request(), .02))
        try:
            assert await wait_event(entered)
            await asyncio.sleep(.04)
            assert not task.done()  # Timeout requested; owned durable work is still active.
            assert advanced == []
        finally:
            release.set()
        with pytest.raises(TimeoutError):
            await task
        assert advanced == []
        assert not owner._storage_async_work.lock.locked()

    asyncio.run(scenario())


def test_foreign_live_loop_cannot_launch_another_writer():
    owner, writes = SimpleNamespace(), []
    foreign = asyncio.new_event_loop()
    try:
        work = storage_async._StorageWork()
        work.loop, work.lock = foreign, asyncio.Lock()
        owner._storage_async_work = work

        async def scenario():
            with pytest.raises(RuntimeError, match="another live loop"):
                await storage_async.owned_storage_call(owner, writes.append, "unexpected")

        asyncio.run(scenario())
        assert writes == []
    finally:
        foreign.close()

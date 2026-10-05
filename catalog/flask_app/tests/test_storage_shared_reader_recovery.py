"""Isolated shared-reader recovery; no production data or physical acceptance."""
from __future__ import annotations

import asyncio
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from catalog.ai.relay_remote import RelayRemoteAIEndpoint
from catalog.ai.remote_contracts import RemoteAIInvocationRequest
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.federation import recorder_storage_relay, relay_storage
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationOperationError
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.recorder_storage_relay import RecorderLogicalStorageAuthority
from catalog.federation.relay_storage import RELAY_STORAGE_KIND, RelayStorageEndpoint
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    StorageOperation,
    StorageRequestEnvelope,
)
from catalog.federation.tests.test_recorder_storage_relay import _request_payload
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime
from catalog.flask_app.services.federation_storage_authority_install import (
    FederationStorageAuthorityMonitor,
    _SharedRelayContext,
    _StorageAwareRelayView,
)
from catalog.flask_app.tests.test_storage_analysis_reader_composition import _monitor
from catalog.flask_app.tests.test_storage_authority_local_provider_runtime import (
    _LoopbackRelayClient,
    _register_primary,
    _settings,
)
from catalog.flask_app.tests.test_trusted_storage_authority_runtime import (
    _compose,
    _CreatorClient,
)
from catalog.flask_app.tests.test_trusted_storage_authority_runtime import (
    _settings as _runtime_settings,
)


class _Bus(_LoopbackRelayClient):
    def __init__(self):
        super().__init__("node-creator")
        self.hold_response = False
        self.provider_stored = asyncio.Event()
        self.release_response = asyncio.Event()
        self.responses = []
        self.raw_reader_tasks = set()

    async def receive_message(self, *, timeout=None):
        self.raw_reader_tasks.add(asyncio.current_task())
        value = await super().receive_message(timeout=timeout)
        if isinstance(value, BaseException):
            raise value
        return value

    async def send_message(self, **kwargs):
        payload = kwargs["payload"]
        if payload.get("kind") == RELAY_STORAGE_KIND and payload.get("message") == "response":
            self.provider_stored.set()
            if self.hold_response:
                await self.release_response.wait()
        if kwargs["target_node_id"] == "node-recorder":
            self.responses.append(payload)
            return {"delivered": True}
        return await super().send_message(**kwargs)


class _Source:
    def __init__(self, ai):
        self.ai = ai

    async def receive_other(self):
        value = await self.ai.receive_other()
        if getattr(value, "payload", {}).get("kind") == "fixture-read-error":
            raise OSError("fixture upstream failure")
        return value


class _Channel(runtime.SharedRecorderAwareStorageControlRelayChannel):
    def __init__(self, *args):
        super().__init__(*args)
        self.finished = asyncio.Queue()

    def _recorder_task_done(self, task, **kwargs):
        error = None if task.cancelled() else task.exception()
        super()._recorder_task_done(task, **kwargs)
        self.finished.put_nowait(error)


async def _pipeline(client, ai, control, settings):
    endpoint = RelayStorageEndpoint(client, message_source=_Source(ai), request_timeout=.5)
    service = runtime._ensure_builtin_local_storage_service(
        endpoint=endpoint, control=control, client=client, settings=settings)
    assert service is not None
    logical = PhaseDLogicalStorageClient(
        session_id=settings.session_id, actor_node_id=client.node_id,
        control_plane=control, transport=endpoint,
        acknowledgements=DurableAcknowledgementStore(settings.acknowledgements_database))
    channel = _Channel(client, endpoint)
    authority = RecorderLogicalStorageAuthority(
        client=client, logical_client=logical, session_id=settings.session_id)
    channel.set_recorder_ingest_handler(authority.handle_request)
    lifecycle = RelayLifecycleEndpoint(
        client, message_source=_StorageAwareRelayView(ai, channel))
    await endpoint.start()
    await channel.start()
    await lifecycle.start()
    return endpoint, channel, logical, service, lifecycle


async def _close(stages):
    await stages[4].close()
    await stages[1].close()
    await stages[0].close()


def _control(tmp_path, client):
    settings = _settings(tmp_path)
    control = PhaseDControlPlane(settings.storage_control_database)
    _register_primary(control, session_id=settings.session_id,
        provider_id=runtime._builtin_local_provider_id(client.node_id),
        node_id=client.node_id, now=datetime.now(timezone.utc))
    return control, settings


async def _request(client, correlation):
    request = _request_payload()
    request.update(group_id="fcp-local-storage", correlation_id=correlation,
        idempotency_key="session-a:" + request["dataset_id"] + ":" + request["batch_id"])
    await client._messages.put(SimpleNamespace(
        session_id="session-a", actor_node_id="node-recorder", payload=request))
    return request


def test_shared_reader_failure_preserves_provider_commit_and_normal_same_identity_replay(tmp_path):
    async def scenario():
        client = _Bus()
        ai = RelayRemoteAIEndpoint(client)
        await ai.start()
        control, settings = _control(tmp_path, client)
        stages = await _pipeline(client, ai, control, settings)
        try:
            client.hold_response = True
            request = await _request(client, "before-read-error")
            await asyncio.wait_for(client.provider_stored.wait(), 2)
            await client._messages.put(SimpleNamespace(payload={"kind": "fixture-read-error"}))
            with pytest.raises(OSError, match="fixture upstream"):
                await asyncio.wait_for(asyncio.shield(stages[0]._reader_task), 1)
            assert isinstance(await asyncio.wait_for(stages[1].finished.get(), 1), OSError)
            assert not client.responses
            assert stages[3].provider.read(session_id="session-a", group_id="fcp-local-storage",
                batch_id=request["batch_id"]) == request["content"]
            status = stages[2].acknowledgements.status("session-a", "fcp-local-storage", request["batch_id"])
            assert status is not None and not status.primary_committed and not status.committed
            reader = stages[0]._reader_task
            with pytest.raises(FederationOperationError, match="ended") as error:
                await stages[0].start()
            assert isinstance(error.value.__cause__, OSError)
            assert stages[0]._reader_task is reader
            with pytest.raises(FederationOperationError):
                runtime._check_authority_readers(stages[0], stages[1], ai)
            await _close(stages)
            client.hold_response = False
            client.release_response.set()
            stages = await _pipeline(client, ai, control, settings)
            replay = await _request(client, "normal-replay")
            assert replay["batch_id"] == request["batch_id"] and replay["content"] == request["content"]
            assert await asyncio.wait_for(stages[1].finished.get(), 2) is None
            assert client.responses[-1]["outcome"]["committed"] is True
            status = stages[2].acknowledgements.status("session-a", "fcp-local-storage", request["batch_id"])
            assert status is not None and status.committed
            other = SimpleNamespace(payload={"kind": "fixture-analysis"})
            await client._messages.put(other)
            assert await stages[4].receive_other(timeout=1) is other
            assert client.raw_reader_tasks == {ai._reader_task}
        finally:
            await _close(stages)
            await ai.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("end", ["exception", "cancel", "return"])
def test_authority_supervisor_observes_failed_reader_and_closes_before_next_attempt(tmp_path, monkeypatch, end):
    async def scenario():
        client = _CreatorClient()
        endpoints = _compose(monkeypatch, client)
        async def ending(_self):
            if end == "exception":
                raise OSError("reader fixture failure")
            if end == "cancel":
                raise asyncio.CancelledError()
        monkeypatch.setattr(RelayStorageEndpoint, "_reader_loop", ending)
        with pytest.raises(FederationOperationError) as error:
            await asyncio.wait_for(runtime.run_trusted_storage_authority(_runtime_settings(tmp_path)), 2)
        assert error.value.code == "storage-relay-reader-stopped"
        assert len(endpoints) == 1 and endpoints[0]._closed and endpoints[0]._reader_task is None
        assert client.disconnected
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["storage-control", "shared-upstream"])
def test_ended_other_shared_stage_is_not_reported_as_healthy(stage):
    async def scenario():
        client = _Bus()
        endpoint = RelayStorageEndpoint(client)
        await endpoint.start()
        task = asyncio.create_task(asyncio.sleep(0))
        await task
        channel = SimpleNamespace(_receiver_task=task if stage == "storage-control" else None)
        source = SimpleNamespace(_reader_task=task if stage == "shared-upstream" else None)
        try:
            with pytest.raises(FederationOperationError) as error:
                runtime._check_authority_readers(endpoint, channel, source)
            assert error.value.code == "storage-authority-reader-stopped"
            assert stage in error.value.message
        finally:
            await endpoint.close()
    asyncio.run(scenario())


def test_ai_explicit_restart_preserves_queue_and_single_reader_and_refuses_closed_stage():
    async def scenario():
        client = _Bus()
        ai = RelayRemoteAIEndpoint(client)
        await ai.start()
        first = ai._reader_task
        queued = SimpleNamespace(payload={"kind": "fixture-downstream"})
        await client._messages.put(queued)
        await client._messages.put(OSError("raw source failed"))
        with pytest.raises(OSError):
            await asyncio.wait_for(asyncio.shield(first), 1)
        assert await ai.receive_other(timeout=1) is queued
        await ai.start()
        second = ai._reader_task
        assert second is not first and not second.done()
        await ai.start()
        assert ai._reader_task is second
        later = SimpleNamespace(payload={"kind": "fixture-after-reconnect"})
        await client._messages.put(later)
        assert await ai.receive_other(timeout=1) is later
        await ai.close()
        assert ai._reader_task is None
        with pytest.raises(RuntimeError, match="closed"):
            await ai.start()
    asyncio.run(scenario())


@pytest.mark.parametrize("end", ["exception", "cancel", "close"])
def test_inflight_requests_keep_their_failure_and_are_not_completed_by_reader_recovery(end):
    async def scenario():
        class Relay:
            node_id = "fixture-actor"
            def __init__(self):
                self.queue = asyncio.Queue()
                self.sent = []
                self.two_sent = asyncio.Event()
            async def receive_message(self):
                value = await self.queue.get()
                if isinstance(value, BaseException):
                    raise value
                return value
            async def send_message(self, **kwargs):
                self.sent.append(kwargs)
                if len(self.sent) == 2:
                    self.two_sent.set()
                return {"delivered": True}
        relay = Relay()
        endpoint = RelayStorageEndpoint(relay, request_timeout=1)
        pending = []
        for request_id in ("first-original-request", "second-original-request"):
            request = StorageRequestEnvelope(
                request_id=request_id, protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.HEALTH, session_id="session-fixture",
                actor_node_id=relay.node_id, authorization_context={"provider_id":"provider-fixture"}, payload={})
            pending.append(asyncio.create_task(endpoint.request(target_node_id="provider-owner", envelope=request)))
        await asyncio.wait_for(relay.two_sent.wait(), 1)
        reader = endpoint._reader_task
        if end == "exception":
            await relay.queue.put(OSError("retained original read failure"))
        elif end == "cancel":
            reader.cancel()
        else:
            await endpoint.close()
        results = await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), 1)
        expected = OSError if end == "exception" else FederationOperationError if end == "cancel" else RuntimeError
        assert all(isinstance(result, expected) for result in results)
        assert not endpoint._pending
        assert {item["request_id"] for item in relay.sent} == {
            "relay-first-original-request", "relay-second-original-request"}
        if end != "close":
            with pytest.raises(FederationOperationError):
                endpoint.check_reader()
            await endpoint.close()
        with pytest.raises(RuntimeError, match="closed"):
            await endpoint.start()
        assert all(task.done() for task in pending)
    asyncio.run(scenario())


@pytest.mark.parametrize("end", ["exception", "cancel", "close"])
def test_ai_reader_failure_fails_pending_invocations_before_explicit_restart(end):
    async def scenario():
        class Relay:
            node_id = "node-" + "a" * 32
            def __init__(self):
                self.queue = asyncio.Queue()
                self.sent = asyncio.Event()
            async def receive_message(self):
                value = await self.queue.get()
                if isinstance(value, BaseException):
                    raise value
                return value
            async def send_message(self, **_kwargs):
                self.sent.set()
                return {"delivered":True}
        relay = Relay()
        ai = RelayRemoteAIEndpoint(relay, request_timeout=1)
        now = datetime.now(timezone.utc)
        invocation = RemoteAIInvocationRequest(
            invocation_id="fixture-invocation", session_id="session-fixture",
            requester_node_id=relay.node_id, provider_node_id="node-" + "b" * 32,
            capability_id="fixture-model", provider_generation=1, health_report_revision=0,
            request=AIRuntimeRequest(request_id="fixture-request", session_id="session-fixture",
                idempotency_key="fixture-key", model="fixture", modality=AIModality.TEXT,
                prompt="fixture", system_prompt="fixture", timeout_seconds=1),
            sent_at=now, expires_at=now + timedelta(seconds=1))
        pending=asyncio.create_task(ai.request(target_node_id=invocation.provider_node_id,request=invocation))
        await asyncio.wait_for(relay.sent.wait(), 1)
        first=ai._reader_task
        if end == "exception":
            await relay.queue.put(OSError("retained private source failure"))
        elif end == "cancel":
            first.cancel()
        else:
            await ai.close()
        expected = OSError if end == "exception" else FederationOperationError if end == "cancel" else RuntimeError
        with pytest.raises(expected):
            await asyncio.wait_for(pending, 1)
        assert not ai._pending
        if end != "close":
            await ai.start()
            assert ai._reader_task is not first and not ai._reader_task.done()
            second = ai._reader_task
            await ai.start()
            assert ai._reader_task is second
            await ai.close()
        with pytest.raises(RuntimeError, match="closed"):
            await ai.start()
    asyncio.run(scenario())


@pytest.mark.parametrize("module", [relay_storage, recorder_storage_relay])
def test_default_message_only_error_logging_preserves_redacted_stage_correlation(module):
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger = module._LOGGER
    previous = logger.level
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    fields = {"storage_stage":"fixture_failure", "storage_request_id":"sha256:"+"a"*64,
              "storage_correlation_id":"sha256:"+"b"*64, "storage_exception_type":"TimeoutError"}
    try:
        module._log_failure("fixture failure",extra=fields)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)
    message = output.getvalue().strip()
    assert message.startswith("fixture failure ")
    assert json.loads(message.removeprefix("fixture failure ")) == fields
    assert len(message.encode()) < 1024


def test_actual_authority_failure_restore_and_next_attempt_rebind_analysis_to_current_channel(tmp_path, monkeypatch):
    async def scenario():
        client = _Bus()
        client.credentials = SimpleNamespace(identity=SimpleNamespace(node_id=client.node_id))
        client.connected_event = asyncio.Event()
        client.connected_event.set()
        client.state = SimpleNamespace(joined_sessions=lambda: (SimpleNamespace(session_id="session-a"),))
        async def announce(_value):
            return {"delivered":True}
        client.announce_capability = announce
        ai = RelayRemoteAIEndpoint(client)
        await ai.start()
        pairing_monitor, identity, consumers = _monitor(monkeypatch, client, ai)
        endpoints = _compose(monkeypatch, client)
        class Failover:
            def __init__(self, **kwargs): self.channel = kwargs["channel"]
            async def start(self): await self.channel.start()
            async def scan_once(self): return ()
            async def close(self): await self.channel.close()
        monkeypatch.setattr(runtime, "StorageFailoverCoordinator", Failover)
        authority_monitor = FederationStorageAuthorityMonitor(pairing_monitor.app, object())
        shared = _SharedRelayContext(client, asyncio.get_running_loop(), ai, pairing_monitor.ai_bridge)
        views=[]
        def exposed(_announcement):
            views.append(pairing_monitor.ai_bridge._endpoint)
            assert isinstance(views[-1], _StorageAwareRelayView)
            consumer = pairing_monitor.analysis_authority(identity, tmp_path, lambda: datetime.now(timezone.utc))
            assert consumer.message_source is views[-1]
            if len(views)==1:
                client._messages.put_nowait(OSError("fixture disconnected upstream reader"))
            else:
                authority_monitor._async_stop.set()
        monkeypatch.setattr(authority_monitor, "_on_announced", exposed)
        try:
            with pytest.raises(FederationOperationError) as error:
                await asyncio.wait_for(authority_monitor._run_authority(_runtime_settings(tmp_path),shared=shared), 2)
            assert error.value.code == "storage-authority-reader-stopped"
            assert isinstance(error.value.__cause__, OSError)
            assert pairing_monitor.ai_bridge._endpoint is ai
            first_reader=ai._reader_task
            assert first_reader.done()
            await asyncio.wait_for(authority_monitor._run_authority(_runtime_settings(tmp_path),shared=shared), 2)
            assert ai._reader_task is not first_reader and not ai._reader_task.done()
            assert len(views)==2 and views[0] is not views[1]
            assert len(consumers)==2 and consumers[0].message_source is views[0] and consumers[1].message_source is views[1]
            assert all(endpoint._closed and endpoint._reader_task is None for endpoint in endpoints)
            assert pairing_monitor.ai_bridge._endpoint is ai
            assert client.connected_event.is_set()
        finally:
            for consumer in consumers: await consumer.close()
            await ai.close()
    asyncio.run(scenario())

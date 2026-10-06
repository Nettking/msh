"""Focused regression coverage for retired analysis reader ownership.

All endpoints and databases are temporary, with no operational clients.
"""
from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.capabilities.analysis.artifact_carrier import RelayAnalysisArtifactEndpoint
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.orchestrator import analysis_federation as federation_module
from catalog.orchestrator import analysis_runtime as runtime_module

Transport = federation_module.ThreadsafeRelayLifecycleTransport


@pytest.fixture
def relay_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="isolated-artifact-loop", daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        async def cleanup():
            current = asyncio.current_task()
            tasks = [task for task in asyncio.all_tasks() if task is not current]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        asyncio.run_coroutine_threadsafe(cleanup(), loop).result(timeout=3)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=3)
        assert not thread.is_alive()
        loop.close()


def submit(loop, coroutine):
    return asyncio.run_coroutine_threadsafe(coroutine, loop).result(timeout=3)


def build_runtime(root, loop):
    async def endpoints():
        incoming = asyncio.Queue()
        async def receive_message(*, timeout=None):
            return await incoming.get()
        disconnects = []
        async def disconnect():
            disconnects.append(True)
        client = SimpleNamespace(node_id="isolated-node", receive_message=receive_message,
            disconnect=disconnect, disconnects=disconnects)
        lifecycle = RelayLifecycleEndpoint(client)
        await lifecycle.start()
        return client, lifecycle
    client, lifecycle = submit(loop, endpoints())
    clock = lambda: datetime.now(timezone.utc)
    federation = federation_module.DeviceFederationAuthority.without_authority(
        capability_root=root / "results" / "capabilities", node_id=client.node_id, clock=clock)
    federation.relay_client = client
    federation._lifecycle_transport = Transport(lifecycle, loop, close_timeout_seconds=1)
    def artifact_factory(gateway):
        artifact = RelayAnalysisArtifactEndpoint(client, gateway, clock=clock, message_source=lifecycle)
        submit(loop, artifact.start())
        return artifact
    federation._artifact_factory = artifact_factory
    runtime = runtime_module.AnalysisRuntime(root=root,
        identity=runtime_module.AnalysisIdentity("isolated-session", client.node_id, "isolated-provider", False),
        federation=federation, enable_local_provider=False)
    return runtime, lifecycle, client


def pending(loop, artifact):
    async def create():
        future = asyncio.get_running_loop().create_future()
        artifact._pending["isolated-exchange"] = future
        return future
    return submit(loop, create())


def test_runtime_stop_closes_actual_artifact_and_fails_own_pending(tmp_path, relay_loop):
    runtime, lifecycle, client = build_runtime(tmp_path / "old", relay_loop)
    artifact = runtime.artifact_carrier
    reader = artifact._reader_task
    future = pending(relay_loop, artifact)
    runtime.stop()
    assert artifact._closed and artifact._reader_task is None and reader.done()
    assert lifecycle._closed and lifecycle._reader_task is None
    assert isinstance(future.exception(), RuntimeError)
    assert not artifact._pending
    assert client.disconnects == []


def test_normal_completed_artifact_result_is_preserved_during_stop(tmp_path, relay_loop):
    runtime, lifecycle, _ = build_runtime(tmp_path / "normal", relay_loop)
    artifact = runtime.artifact_carrier
    future = pending(relay_loop, artifact)
    async def complete():
        artifact._accept_response({"exchange_id": "isolated-exchange", "result": "original-result"})
    submit(relay_loop, complete())
    runtime.stop()
    assert future.result() == {"exchange_id": "isolated-exchange", "result": "original-result"}
    assert artifact._closed and lifecycle._closed


def test_repeated_stop_is_idempotent_and_keeps_original_pending_failure(tmp_path, relay_loop):
    runtime, _, _ = build_runtime(tmp_path / "idempotent", relay_loop)
    future = pending(relay_loop, runtime.artifact_carrier)
    runtime.stop()
    error = future.exception()
    runtime.stop()
    assert future.exception() is error
    assert runtime.artifact_carrier._closed


def test_stop_of_replaced_runtime_preserves_current_reader_and_its_pending(tmp_path, relay_loop):
    old, old_lifecycle, _ = build_runtime(tmp_path / "old", relay_loop)
    new, new_lifecycle, new_client = build_runtime(tmp_path / "new", relay_loop)
    old_future = pending(relay_loop, old.artifact_carrier)
    new_future = pending(relay_loop, new.artifact_carrier)
    old_reader = old.artifact_carrier._reader_task
    new_reader = new.artifact_carrier._reader_task
    old.stop()
    assert old_reader.done() and old_lifecycle._closed
    assert isinstance(old_future.exception(), RuntimeError)
    assert not new_reader.done() and not new_lifecycle._closed
    assert not new_future.done() and new_client.disconnects == []
    new.stop()
    assert isinstance(new_future.exception(), RuntimeError)


def test_actual_reader_failure_stays_original_error_and_stop_drains_reader(tmp_path, relay_loop):
    runtime, lifecycle, _ = build_runtime(tmp_path / "failed", relay_loop)
    artifact = runtime.artifact_carrier
    reader = artifact._reader_task
    future = pending(relay_loop, artifact)
    original = OSError("isolated-upstream-reader-failure")
    async def fail():
        raise original
    async def trigger():
        artifact._receive = fail
        lifecycle._other.put_nowait(SimpleNamespace(payload={"kind": "other"}))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    submit(relay_loop, trigger())
    assert reader.done() and future.exception() is original
    runtime.stop()
    assert artifact._closed and artifact._reader_task is None and lifecycle._closed
    assert future.exception() is original


def recording_transport(loop, *, close_timeout=1):
    client = SimpleNamespace(node_id="isolated-recording-node")
    endpoint = SimpleNamespace(relay_client=client, closed=threading.Event(), calls=[])
    async def close():
        endpoint.calls.append(asyncio.get_running_loop())
        endpoint.closed.set()
    endpoint.close = close
    return Transport(endpoint, loop, close_timeout_seconds=close_timeout), endpoint, client


def test_artifact_close_failure_still_closes_only_its_lifecycle(relay_loop):
    transport, endpoint, client = recording_transport(relay_loop)
    artifact = SimpleNamespace(relay_client=client, message_source=endpoint)
    async def fail():
        raise OSError("isolated-artifact-close-failure")
    artifact.close = fail
    with pytest.raises(OSError, match="isolated-artifact-close-failure"):
        transport.close(artifact_carrier=artifact)
    assert endpoint.closed.is_set() and endpoint.calls == [relay_loop]


def test_one_close_timeout_cancels_artifact_then_finally_closes_lifecycle(relay_loop):
    transport, endpoint, client = recording_transport(relay_loop, close_timeout=0.05)
    artifact = SimpleNamespace(relay_client=client, message_source=endpoint, cancelled=threading.Event())
    async def hang():
        try:
            await asyncio.Event().wait()
        finally:
            artifact.cancelled.set()
    artifact.close = hang
    started = time.monotonic()
    transport.close(artifact_carrier=artifact)
    assert time.monotonic() - started < 0.5
    assert artifact.cancelled.wait(timeout=1) and endpoint.closed.wait(timeout=1)
    assert endpoint.calls == [relay_loop]


@pytest.mark.parametrize("foreign_field", ["relay_client", "message_source"])
def test_foreign_endpoint_is_not_closed_or_cancelled(relay_loop, foreign_field):
    transport, endpoint, client = recording_transport(relay_loop)
    artifact = SimpleNamespace(relay_client=client, message_source=endpoint, closed=False)
    setattr(artifact, foreign_field, object())
    async def close():
        artifact.closed = True
    artifact.close = close
    with pytest.raises(ValueError, match="another relay transport"):
        transport.close(artifact_carrier=artifact)
    assert not artifact.closed and endpoint.closed.is_set()


def test_legacy_transport_only_close_remains_supported(relay_loop):
    transport, endpoint, _ = recording_transport(relay_loop)
    transport.close()
    assert endpoint.closed.is_set()


def test_stopped_or_closed_loop_is_not_restarted():
    loop = asyncio.new_event_loop()
    transport, endpoint, client = recording_transport(loop)
    artifact = SimpleNamespace(relay_client=client, message_source=endpoint)
    transport.close(artifact_carrier=artifact)
    loop.close()
    transport.close(artifact_carrier=artifact)
    assert not endpoint.closed.is_set()


def test_failed_submission_closes_created_coroutine_without_foreign_action(relay_loop, monkeypatch):
    transport, endpoint, _ = recording_transport(relay_loop)
    captured = []
    def reject(coroutine, loop):
        captured.append(coroutine)
        raise RuntimeError("isolated-loop-closed-before-submit")
    monkeypatch.setattr(federation_module.asyncio, "run_coroutine_threadsafe", reject)
    transport.close()
    assert len(captured) == 1 and captured[0].cr_frame is None
    assert not endpoint.closed.is_set()
    monkeypatch.undo()

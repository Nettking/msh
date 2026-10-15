from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from catalog.capabilities.analysis.contracts import analysis_timeout_policy
from catalog.capabilities.dispatch import DispatchState, ExecutionResult
from catalog.capabilities.lifecycle_worker import (
    CancellableCapabilityWorker,
    SQLiteLifecycleDispatchInbox,
)
from catalog.capabilities.tests.test_dispatch import _job, _registration, _request
from catalog.orchestrator.analysis_federation import DeviceFederationAuthority

NOW = datetime(2026, 10, 15, 21, 59, tzinfo=timezone.utc)


@pytest.fixture
def relay_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        async def drain():
            pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        asyncio.run_coroutine_threadsafe(drain(), loop).result(timeout=3)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=3)
        assert not thread.is_alive()
        loop.close()


class LoopbackClient:
    node_id = "node-coordinator"

    def __init__(self):
        self.incoming = asyncio.Queue()

    async def receive_message(self, *, timeout=None):
        return await self.incoming.get()

    async def send_message(self, *, session_id, target_node_id, payload, request_id=None):
        assert target_node_id == self.node_id
        await self.incoming.put(SimpleNamespace(
            actor_node_id=self.node_id, session_id=session_id, payload=payload,
        ))
        return {"delivered": True}


def authority(tmp_path, relay_loop, current):
    client = LoopbackClient()
    runtime = SimpleNamespace(
        ensure_connected=lambda _state: None,
        _connected_client=lambda: client,
    )
    return DeviceFederationAuthority.from_authenticated_relay(
        capability_root=tmp_path,
        session_id="session-1",
        node_id=client.node_id,
        clock=lambda: current[0],
        runtime=runtime,
        runtime_state=object(),
        event_loop=relay_loop,
        upstream_message_source=SimpleNamespace(receive_other=client.receive_message),
        local_leader=True,
    )


@pytest.mark.parametrize("remaining_lease", [3300, 20])
def test_analysis_dispatch_waits_for_worker_but_not_past_actual_lease(
    tmp_path, relay_loop, monkeypatch, remaining_lease,
):
    current = [NOW + timedelta(seconds=3300 - remaining_lease)]
    owner = authority(tmp_path, relay_loop, current)
    transport = owner.lifecycle_transport()
    started = threading.Event()
    release = asyncio.Event()

    class Handler:
        async def execute(self, job):
            started.set()
            await release.wait()
            return ExecutionResult(True, {"job_id": job.job_id})

    worker = CancellableCapabilityWorker(
        _registration(node_id="node-coordinator"),
        Handler(),
        SQLiteLifecycleDispatchInbox(tmp_path / "worker.sqlite3"),
        clock=lambda: current[0],
    )
    transport.register_worker("provider-worker", worker)
    request = _request(
        target_node_id="node-coordinator",
        job=replace(_job(), timeout_policy=analysis_timeout_policy()),
        lease_expires_at=NOW + timedelta(seconds=3300),
        sent_at=NOW,
    )
    real_wait_for = asyncio.wait_for
    response_budgets = []

    async def elapsed_worker_time(awaitable, timeout):
        # Execute the actual endpoint and worker without sleeping 110 seconds.
        # Only its terminal-response future uses this deterministic time seam.
        if isinstance(awaitable, asyncio.Future):
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0)
            assert started.is_set(), "the genuine worker must accept the dispatch"
            response_budgets.append(timeout)
            if timeout < 110:
                awaitable.cancel()
                raise TimeoutError("worker completion exceeds response window")
            current[0] += timedelta(seconds=110)
            release.set()
            return await real_wait_for(awaitable, timeout=1)
        return await real_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(asyncio, "wait_for", elapsed_worker_time)
    try:
        if remaining_lease == 3300:
            response = asyncio.run(transport.request(target_node_id="node-coordinator", request=request))
            assert response.validate_for(request).events[-1].state is DispatchState.SUCCEEDED
            assert response.events[-1].occurred_at == NOW + timedelta(seconds=110)
            assert response_budgets == [3300]
        else:
            with pytest.raises(TimeoutError, match="response window"):
                asyncio.run(transport.request(target_node_id="node-coordinator", request=request))
            assert response_budgets == [20]
        assert not transport.endpoint._dispatches
        assert transport.endpoint.request_timeout == 60
    finally:
        relay_loop.call_soon_threadsafe(release.set)
        transport.close()


def test_analysis_control_and_artifact_response_windows_stay_short(tmp_path, relay_loop):
    owner = authority(tmp_path, relay_loop, [NOW])
    transport = owner.lifecycle_transport()
    artifact = owner.artifact_carrier(SimpleNamespace())
    try:
        # cancel() uses request_timeout, independently of the dispatch-only bound.
        assert transport.endpoint.request_timeout == 60
        assert artifact.request_timeout == 60
    finally:
        transport.close(artifact_carrier=artifact)

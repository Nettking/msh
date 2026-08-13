"""Releasing a superseded relay lifecycle reader on runtime replacement.

``AnalysisRuntime.stop`` releases the transport it is replacing with a plain
``getattr(transport, "close", None)``. The federated transport is a wrapper
around an endpoint whose ``close`` is a coroutine owned by the relay event loop,
so the wrapper has to expose a working synchronous path or the superseded
reader stays parked.
"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from catalog.orchestrator.analysis_federation import ThreadsafeRelayLifecycleTransport


class _RecordingEndpoint:
    """Stands in for RelayLifecycleEndpoint: async close, loop-affine."""

    def __init__(self, *, hang: bool = False) -> None:
        self.relay_client = SimpleNamespace(node_id="node-under-test")
        self.closed = threading.Event()
        self.closed_on_loop: asyncio.AbstractEventLoop | None = None
        self._hang = hang

    async def close(self) -> None:
        if self._hang:
            await asyncio.Event().wait()
        self.closed_on_loop = asyncio.get_running_loop()
        self.closed.set()


@pytest.fixture
def relay_loop():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="relay-loop", daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


def test_close_is_discoverable_the_way_the_runtime_looks_for_it():
    # AnalysisRuntime.stop does exactly this lookup; a missing attribute made
    # the release a silent no-op for every federated runtime.
    assert callable(getattr(ThreadsafeRelayLifecycleTransport, "close", None))


def test_close_runs_the_endpoint_shutdown_on_the_relay_loop(relay_loop):
    endpoint = _RecordingEndpoint()
    transport = ThreadsafeRelayLifecycleTransport(endpoint, relay_loop)

    transport.close()

    assert endpoint.closed.is_set()
    assert endpoint.closed_on_loop is relay_loop


def test_close_is_bounded_when_the_endpoint_will_not_finish(relay_loop):
    endpoint = _RecordingEndpoint(hang=True)
    transport = ThreadsafeRelayLifecycleTransport(
        endpoint, relay_loop, close_timeout_seconds=0.1
    )

    # Replacement must stay fail-safe: an endpoint that never settles is
    # abandoned instead of blocking the thread performing the replacement.
    transport.close()

    assert not endpoint.closed.is_set()


def test_close_on_an_already_stopped_loop_does_nothing(relay_loop):
    endpoint = _RecordingEndpoint()
    transport = ThreadsafeRelayLifecycleTransport(endpoint, relay_loop)
    relay_loop.call_soon_threadsafe(relay_loop.stop)
    for _ in range(50):
        if not relay_loop.is_running():
            break
        threading.Event().wait(0.02)

    transport.close()

    assert not endpoint.closed.is_set()

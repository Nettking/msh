from __future__ import annotations

import asyncio
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.flask_app.services import federation_pairing_service as pairing
from catalog.flask_app.tests.test_pairing_connection_ownership import (
    RUNTIMES,
    _state,
    _transport,
)


@pytest.mark.parametrize("runtime_type", RUNTIMES)
def test_close_cancels_unpublished_connection_and_releases_private_loop(
    tmp_path: Path, monkeypatch, runtime_type,
) -> None:
    state = _state()
    _entered, _release, clients, live, _peak = _transport(monkeypatch, state)
    connected = threading.Event()
    original_client = pairing.PairingRelayNodeClient

    class Client(original_client):
        async def connect(self, **kwargs):
            connected.set()
            await super().connect(**kwargs)

    monkeypatch.setattr(pairing, "PairingRelayNodeClient", Client)
    from catalog.flask_app.services import resilient_pairing_runtime as resilient
    monkeypatch.setattr(resilient, "PairingRelayNodeClient", Client)
    runtime = runtime_type(state_directory=tmp_path, display_name="Member")
    with ThreadPoolExecutor(max_workers=1) as executor:
        attempt = executor.submit(runtime.ensure_connected, state)
        try:
            assert connected.wait(2)
            thread = runtime._thread
            loop = runtime._loop
            assert runtime.close(timeout=2)
            with pytest.raises(CancelledError):
                attempt.result(timeout=2)
            assert len(clients) == 1
            assert not live
            assert thread is not None and not thread.is_alive()
            assert loop is not None and loop.is_closed()
            assert runtime.close(timeout=0)
            with pytest.raises(FederationOperationError) as error:
                runtime.ensure_connected(state)
            assert error.value.code == "pairing-runtime-closed"
            assert runtime._thread is thread
        finally:
            runtime.close(timeout=2)


def test_close_before_loop_start_finishes_does_not_leave_a_thread(
    tmp_path: Path, monkeypatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    new_event_loop = asyncio.new_event_loop

    def delayed_loop():
        entered.set()
        assert release.wait(2)
        return new_event_loop()

    monkeypatch.setattr(asyncio, "new_event_loop", delayed_loop)
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")
    with ThreadPoolExecutor(max_workers=1) as executor:
        startup = executor.submit(runtime._start_loop)
        try:
            assert entered.wait(2)
            assert runtime.close(timeout=0) is False
            release.set()
            with pytest.raises(FederationOperationError) as error:
                startup.result(timeout=2)
            assert error.value.code == "pairing-runtime-closed"
            assert runtime.close(timeout=2)
            assert runtime._loop is not None and runtime._loop.is_closed()
        finally:
            release.set()
            runtime.close(timeout=2)


def test_close_timeout_keeps_the_actual_owner_and_does_not_restart_it(tmp_path: Path) -> None:
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")
    loop = runtime._start_loop()
    entered = threading.Event()
    cancelled = threading.Event()
    release = asyncio.Event()

    async def slow_cleanup():
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    future = asyncio.run_coroutine_threadsafe(slow_cleanup(), loop)
    try:
        assert entered.wait(2)
        owner = runtime._thread
        assert runtime.close(timeout=0.05) is False
        assert cancelled.wait(2)
        assert runtime._thread is owner and owner is not None and owner.is_alive()
        with pytest.raises(FederationOperationError):
            runtime._start_loop()
        loop.call_soon_threadsafe(release.set)
        assert runtime.close(timeout=2)
        assert future.result(timeout=2) is None
        assert loop.is_closed()
    finally:
        if not loop.is_closed():
            loop.call_soon_threadsafe(release.set)
        runtime.close(timeout=2)


def test_closing_unused_runtime_does_not_start_a_thread(tmp_path: Path) -> None:
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")
    assert runtime.close(timeout=0)
    assert runtime._thread is None
    assert runtime._loop is None


def test_close_waits_for_actual_executor_work_after_awaiter_is_cancelled(tmp_path: Path) -> None:
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")
    loop = runtime._start_loop()
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def archive_work():
        entered.set()
        release.wait(5)
        finished.set()

    future = asyncio.run_coroutine_threadsafe(asyncio.to_thread(archive_work), loop)
    try:
        assert entered.wait(2)
        assert runtime.close(timeout=0.05) is False
        assert future.cancelled()
        assert not finished.is_set()
        assert runtime._thread is not None and runtime._thread.is_alive()
        assert not loop.is_closed()
        release.set()
        assert runtime.close(timeout=2)
        assert finished.is_set()
        assert loop.is_closed()
    finally:
        release.set()
        runtime.close(timeout=2)


def test_close_failure_remains_visible_without_retaining_exception_details(tmp_path: Path) -> None:
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")

    async def disconnect():
        raise RuntimeError("private transport details must not be retained")

    async def install_client():
        runtime._client = SimpleNamespace(disconnect=disconnect)

    runtime._submit(install_client())
    assert runtime.close(timeout=2) is False
    assert runtime._thread is not None and not runtime._thread.is_alive()
    assert runtime._loop is not None and runtime._loop.is_closed()
    assert runtime._shutdown_error == "RuntimeError"
    assert runtime.close(timeout=0) is False


def test_saved_reconnect_has_aggregate_budget_without_raising_rpc_budget(
    tmp_path: Path, monkeypatch,
) -> None:
    state = _state()
    runtime = pairing.PairingRelayRuntime(
        state_directory=tmp_path,
        display_name="Member",
        timeout_seconds=0.04,
    )
    phases = threading.Event()
    disconnected = threading.Event()

    class SlowInitialSyncClient:
        def __init__(self, **kwargs):
            del kwargs
            self.node_id = state.binding.device_id
            self.connected_event = asyncio.Event()
            self.state = SimpleNamespace(
                joined_sessions=lambda: [
                    SimpleNamespace(session_id=state.binding.internal_session_id)
                ]
            )

        async def connect(self, **kwargs):
            del kwargs
            # Six individually bounded capability sync steps take longer than
            # one ordinary request budget, but less than the reconnect budget.
            for _ in range(6):
                await asyncio.sleep(0.012)
            self.connected_event.set()
            phases.set()

        async def disconnect(self, **kwargs):
            del kwargs
            self.connected_event.clear()
            disconnected.set()

    monkeypatch.setattr(pairing, "PairingRelayNodeClient", SlowInitialSyncClient)
    try:
        # This is the pre-fix path: it incorrectly gives the multi-step
        # connection the one-RPC deadline, so the six successful substeps are
        # cancelled as a group.
        with pytest.raises(FederationOperationError) as old_path:
            runtime._submit(runtime._ensure_connected(state))
        assert old_path.value.code == "pairing-relay-timeout"
        assert disconnected.wait(1)
        disconnected.clear()

        runtime.ensure_connected(state)
        assert phases.is_set()
        assert runtime._client is not None
        # Ordinary operations keep the original short deadline.
        with pytest.raises(FederationOperationError) as error:
            runtime._submit(asyncio.sleep(0.08))
        assert error.value.code == "pairing-relay-timeout"
    finally:
        runtime.close(timeout=2)


def test_saved_reconnect_respects_its_own_bounded_deadline(
    tmp_path: Path, monkeypatch,
) -> None:
    state = _state()
    runtime = pairing.PairingRelayRuntime(
        state_directory=tmp_path,
        display_name="Member",
        timeout_seconds=0.04,
        connect_timeout_seconds=0.07,
    )
    cancelled = threading.Event()
    disconnected = threading.Event()

    class StalledInitialSyncClient:
        def __init__(self, **kwargs):
            del kwargs
            self.node_id = state.binding.device_id
            self.connected_event = asyncio.Event()
            self.state = SimpleNamespace(
                joined_sessions=lambda: [
                    SimpleNamespace(session_id=state.binding.internal_session_id)
                ]
            )

        async def connect(self, **kwargs):
            del kwargs
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def disconnect(self, **kwargs):
            del kwargs
            self.connected_event.clear()
            disconnected.set()

    monkeypatch.setattr(pairing, "PairingRelayNodeClient", StalledInitialSyncClient)
    try:
        with pytest.raises(FederationOperationError) as error:
            runtime.ensure_connected(state)
        assert error.value.code == "pairing-relay-timeout"
        # The timed-out connection remains owned until its cancellation cleanup
        # closes the unpublished client; it is never published as connected.
        assert cancelled.wait(1)
        assert disconnected.wait(1)
        assert runtime._client is None
    finally:
        runtime.close(timeout=2)


def test_inner_relay_request_timeout_is_not_mislabeled_as_aggregate_deadline(
    tmp_path: Path,
) -> None:
    runtime = pairing.PairingRelayRuntime(state_directory=tmp_path, display_name="Member")

    async def inner_timeout():
        raise TimeoutError("per-request timeout")

    try:
        with pytest.raises(FederationOperationError) as error:
            runtime._submit(inner_timeout(), operation="saved reconnect")
        assert error.value.code == "pairing-relay-request-timeout"
        assert "individual relay request timeout" in error.value.message
        assert isinstance(error.value.__cause__, TimeoutError)
    finally:
        runtime.close(timeout=2)

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.relay import provider_service, service
from catalog.relay.service import (
    RelayRuntimeError,
    RelayServer,
    _wait_for_relay_shutdown,
)


def test_stale_sweep_failure_reaches_the_relay_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "coordinator.sqlite3")
        relay = RelayServer(
            coordinator,
            host="127.0.0.1",
            port=0,
            sweep_interval_seconds=0.01,
        )

        def fail_sweep(*, heartbeat_timeout_seconds: float) -> None:
            del heartbeat_timeout_seconds
            raise sqlite3.OperationalError("injected coordinator failure")

        monkeypatch.setattr(coordinator, "sweep_stale", fail_sweep)
        await relay.start()
        try:
            with pytest.raises(
                RelayRuntimeError,
                match="required relay background task failed",
            ) as raised:
                await asyncio.wait_for(relay.wait_stopped(), timeout=2)
            assert raised.value.code == "relay-background-task-failed"
            assert isinstance(raised.value.__cause__, sqlite3.OperationalError)
        finally:
            await relay.stop()

    asyncio.run(scenario())


def test_shutdown_wait_propagates_relay_failure_even_with_signal_support() -> None:
    class FailedRelay:
        async def wait_stopped(self) -> None:
            raise RelayRuntimeError(
                "relay-background-task-failed",
                "required relay background task failed",
            )

    async def scenario() -> None:
        with pytest.raises(RelayRuntimeError):
            await asyncio.wait_for(
                _wait_for_relay_shutdown(FailedRelay(), asyncio.Event()),
                timeout=2,
            )

    asyncio.run(scenario())


def test_shutdown_wait_keeps_operator_stop_clean_and_cancels_relay_waiter() -> None:
    waiter_cancelled = asyncio.Event()

    class RunningRelay:
        async def wait_stopped(self) -> None:
            try:
                await asyncio.Event().wait()
            finally:
                waiter_cancelled.set()

    async def scenario() -> None:
        stop_requested = asyncio.Event()
        stop_requested.set()
        await asyncio.wait_for(
            _wait_for_relay_shutdown(RunningRelay(), stop_requested),
            timeout=2,
        )
        assert waiter_cancelled.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize("entrypoint", [service, provider_service])
def test_relay_entrypoints_return_nonzero_after_required_loop_failure(
    entrypoint: object,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stopped = []

    class FailedRelay:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def start(self) -> None:
            return None

        async def wait_stopped(self) -> None:
            raise RelayRuntimeError(
                "relay-background-task-failed",
                "required relay background task failed",
            )

        async def stop(self) -> None:
            stopped.append(True)

    monkeypatch.setattr(entrypoint, "SessionCoordinator", lambda path: path)
    relay_class = (
        "ProviderAuthorityRelayServer"
        if entrypoint is provider_service
        else "RelayServer"
    )
    monkeypatch.setattr(entrypoint, relay_class, FailedRelay)

    assert entrypoint.main(["serve", "--database", "unused.sqlite3"]) == 2
    assert stopped == [True]
    assert "relay command failed (relay-background-task-failed)" in capsys.readouterr().err

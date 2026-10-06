from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.flask_app.services import federation_pairing_service as pairing
from catalog.flask_app.services import resilient_pairing_runtime as resilient
from catalog.flask_app.services.federated_data_runtime import (
    FederatedDataPairingRelayRuntime,
)

RUNTIMES = (
    pairing.PairingRelayRuntime,
    resilient.ResilientPairingRelayRuntime,
    FederatedDataPairingRelayRuntime,
)
NOW = datetime(2026, 9, 10, tzinfo=timezone.utc)


def _state() -> pairing.RemotePairingState:
    return pairing.RemotePairingState(
        "ws://127.0.0.1:8765",
        FederationSessionBinding(
            federation_id="federation-connection-ownership",
            internal_session_id="session-connection-ownership",
            device_id="node-connection-ownership",
            state=FederationConnectionState.CONNECTED,
            revision=1,
            trusted=True,
            created_at=NOW,
            last_verified_at=NOW,
        ),
    )


def _transport(monkeypatch, state):
    entered = asyncio.Event()
    release = asyncio.Event()
    clients = []
    live = set()
    peak = []

    class Client:
        def __init__(self, **kwargs):
            self.node_id = state.binding.device_id
            self.connected_event = asyncio.Event()
            self.state = SimpleNamespace(
                joined_sessions=lambda: [
                    SimpleNamespace(session_id=state.binding.internal_session_id)
                ]
            )
            clients.append(self)

        async def connect(self, **kwargs):
            # Authentication has opened a connection; initial replay is still
            # pending. A second caller must not create a competing connection.
            live.add(self)
            peak.append(len(live))
            self.connected_event.set()
            entered.set()
            await release.wait()

        async def disconnect(self, **kwargs):
            live.discard(self)
            self.connected_event.clear()

        async def join_session(self, token):
            return {
                "session_id": state.binding.internal_session_id,
                "revision": 1,
                "created_at": NOW.isoformat(),
            }

    monkeypatch.setattr(pairing, "PairingRelayNodeClient", Client)
    monkeypatch.setattr(resilient, "PairingRelayNodeClient", Client)
    return entered, release, clients, live, peak


@pytest.mark.parametrize("runtime_type", RUNTIMES)
def test_overlapping_reconnects_share_one_owned_connection(
    tmp_path: Path, monkeypatch, runtime_type,
) -> None:
    async def scenario():
        state = _state()
        entered, release, clients, live, peak = _transport(monkeypatch, state)
        runtime = runtime_type(state_directory=tmp_path, display_name="Member")
        first = asyncio.create_task(runtime._ensure_connected(state))
        second = None
        try:
            await asyncio.wait_for(entered.wait(), 2)
            second = asyncio.create_task(runtime._ensure_connected(state))
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(asyncio.gather(first, second), 2)
            assert len(clients) == 1
            assert max(peak) == 1
            await runtime._disconnect_current()
            assert not live
        finally:
            release.set()
            await asyncio.gather(
                *(task for task in (first, second) if task is not None),
                return_exceptions=True,
            )
            for client in clients:
                await client.disconnect()

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_type", RUNTIMES)
def test_cancelled_reconnect_closes_the_unpublished_client(
    tmp_path: Path, monkeypatch, runtime_type,
) -> None:
    async def scenario():
        state = _state()
        entered, release, clients, live, _peak = _transport(monkeypatch, state)
        runtime = runtime_type(state_directory=tmp_path, display_name="Member")
        task = asyncio.create_task(runtime._ensure_connected(state))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not live
            release.set()
            await runtime._ensure_connected(state)
            assert len(live) == 1
            await runtime._disconnect_current()
            assert not live
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            for client in clients:
                await client.disconnect()

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_type", RUNTIMES)
def test_pairing_redemption_waits_for_pending_reconnect(
    tmp_path: Path, monkeypatch, runtime_type,
) -> None:
    async def scenario():
        state = _state()
        entered, release, clients, live, peak = _transport(monkeypatch, state)
        runtime = runtime_type(state_directory=tmp_path, display_name="Member")
        offer = SimpleNamespace(
            relay_url=state.relay_url,
            federation_id=state.binding.federation_id,
            internal_session_id=state.binding.internal_session_id,
            enrollment_token="test-enrollment",
            invitation_token="test-invitation",
        )
        first = asyncio.create_task(runtime._ensure_connected(state))
        second = None
        try:
            await asyncio.wait_for(entered.wait(), 2)
            second = asyncio.create_task(runtime._redeem(offer))
            await asyncio.sleep(0)
            release.set()
            await asyncio.wait_for(asyncio.gather(first, second), 2)
            assert max(peak) == 1
            assert len(live) == 1
            await runtime._disconnect_current()
            assert not live
        finally:
            release.set()
            await asyncio.gather(
                *(task for task in (first, second) if task is not None),
                return_exceptions=True,
            )
            for client in clients:
                await client.disconnect()

    asyncio.run(scenario())


@pytest.mark.parametrize("runtime_type", RUNTIMES)
def test_reconnect_timeout_retains_type_and_stage_without_exception_text(
    tmp_path: Path,
    monkeypatch,
    caplog: pytest.LogCaptureFixture,
    runtime_type,
) -> None:
    async def scenario() -> None:
        state = _state()
        disconnect_codes: list[str | None] = []

        class Client:
            def __init__(self, **kwargs):
                self.node_id = state.binding.device_id
                self.connected_event = asyncio.Event()
                self.state = SimpleNamespace(joined_sessions=list)

            async def connect(self, **kwargs):
                raise TimeoutError("private transport detail")

            async def disconnect(self, *, error_code=None):
                disconnect_codes.append(error_code)

        monkeypatch.setattr(pairing, "PairingRelayNodeClient", Client)
        monkeypatch.setattr(resilient, "PairingRelayNodeClient", Client)
        runtime = runtime_type(state_directory=tmp_path, display_name="Member")
        with pytest.raises(TimeoutError):
            await runtime._ensure_connected(state)
        assert disconnect_codes == ["pairing-connect-timeout"]

    caplog.set_level(logging.WARNING, logger=pairing.__name__)
    asyncio.run(scenario())
    record = caplog.records[-1]
    assert "stage=relay-connect-and-initial-sync" in record.message
    assert "exception_type=TimeoutError" in record.message
    assert "error_code=pairing-connect-timeout" in record.message
    assert "private transport detail" not in record.message

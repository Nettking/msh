from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.flask_app.services import federation_pairing_install
from catalog.flask_app.services.federation_pairing_install import (
    SavedFederationReconnectMonitor,
)
from catalog.flask_app.services.federation_pairing_service import PairingCodeCodec
from catalog.flask_app.services.resilient_pairing_runtime import (
    ResilientPairingRelayRuntime,
)
from catalog.node.identity import IdentityStore
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 6, 16, 30, tzinfo=timezone.utc)


def test_existing_enrollment_and_membership_recover_remote_binding(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(
            tmp_path / "relay" / "control.sqlite3",
            clock=lambda: NOW,
        )
        relay = RelayServer(
            coordinator,
            host="127.0.0.1",
            port=0,
            auth_timeout_seconds=5,
            send_timeout_seconds=5,
            heartbeat_timeout_seconds=300,
            sweep_interval_seconds=300,
        )
        await relay.start()
        runtime = ResilientPairingRelayRuntime(
            state_directory=tmp_path / "joiner",
            display_name="Joining device",
            clock=lambda: NOW,
            timeout_seconds=5,
        )
        try:
            host = IdentityStore(
                tmp_path / "host",
                display_name="Host device",
            ).load_or_create(now=NOW)
            host_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            coordinator.enroll_node(
                host.identity,
                token=str(host_enrollment["token"]),
            )
            session = coordinator.create_session(
                actor_node_id=host.identity.node_id,
                display_name="Shared Federation",
                request_id="auto-reconnect-session",
            )

            joiner = IdentityStore(
                tmp_path / "joiner",
                display_name="Joining device",
            ).load_or_create(now=NOW)
            initial_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            coordinator.enroll_node(
                joiner.identity,
                token=str(initial_enrollment["token"]),
            )
            initial_invitation = coordinator.create_invitation(
                session_id=session.session_id,
                actor_node_id=host.identity.node_id,
                ttl_seconds=300,
                max_uses=1,
                request_id="auto-reconnect-initial-invite",
            )
            coordinator.join_session(
                node_id=joiner.identity.node_id,
                token=str(initial_invitation["token"]),
                request_id="auto-reconnect-initial-join",
                expected_session_id=session.session_id,
            )

            replacement_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            replacement_invitation = coordinator.create_invitation(
                session_id=session.session_id,
                actor_node_id=host.identity.node_id,
                ttl_seconds=300,
                max_uses=1,
                request_id="auto-reconnect-recovery-invite",
            )
            code = PairingCodeCodec(clock=lambda: NOW).encode(
                credentials=host,
                relay_url=relay.url,
                federation_id=federation_id_from_session_id(session.session_id),
                internal_session_id=session.session_id,
                enrollment_token=str(replacement_enrollment["token"]),
                invitation_token=str(replacement_invitation["token"]),
            )

            binding = await runtime._redeem(
                PairingCodeCodec(clock=lambda: NOW).decode(code)
            )

            assert binding.device_id == joiner.identity.node_id
            assert binding.internal_session_id == session.session_id
            assert binding.trusted is True
            assert coordinator.session_ids_for_node(joiner.identity.node_id) == (
                session.session_id,
            )
        finally:
            if runtime._client is not None and runtime._client.connected_event.is_set():
                await runtime._client.disconnect()
            await relay.stop()

    asyncio.run(scenario())


def test_saved_membership_monitor_retries_until_relay_is_available(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        federation_pairing_install,
        "_CONNECTED_CHECK_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        federation_pairing_install,
        "_MAX_RETRY_SECONDS",
        0.01,
    )

    class RemoteStore:
        def load(self) -> object:
            return object()

    class Service:
        def __init__(self) -> None:
            self.remote_store = RemoteStore()
            self.calls = 0

        def authorized_context(self) -> object:
            self.calls += 1
            if self.calls < 3:
                raise OSError("relay is still starting")
            return SimpleNamespace(binding=object())

    app = Flask(__name__)
    service = Service()
    monitor = SavedFederationReconnectMonitor(app, service)  # type: ignore[arg-type]

    monitor.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if monitor.snapshot()["status"] == "connected":
            break
        time.sleep(0.01)
    monitor.stop()

    assert service.calls >= 3
    assert monitor.snapshot() == {
        "status": "connected",
        "attempts": 0,
        "last_error_code": None,
    }

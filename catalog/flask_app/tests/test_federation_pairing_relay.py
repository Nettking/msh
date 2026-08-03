from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.flask_app.services.federation_pairing_service import (
    PairingCodeCodec,
    PairingRelayRuntime,
)
from catalog.node.identity import IdentityStore
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 3, 18, 30, tzinfo=timezone.utc)


def test_signed_pairing_code_joins_the_existing_relay_session(
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
        runtime = PairingRelayRuntime(
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
            enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            coordinator.enroll_node(
                host.identity,
                token=str(enrollment["token"]),
            )
            session = coordinator.create_session(
                actor_node_id=host.identity.node_id,
                display_name="Shared physical Federation",
                request_id="pairing-session-create",
            )
            joiner_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            invitation = coordinator.create_invitation(
                session_id=session.session_id,
                actor_node_id=host.identity.node_id,
                ttl_seconds=300,
                max_uses=1,
                request_id="pairing-session-invite",
            )
            code = PairingCodeCodec(clock=lambda: NOW).encode(
                credentials=host,
                relay_url=relay.url,
                federation_id=federation_id_from_session_id(session.session_id),
                internal_session_id=session.session_id,
                enrollment_token=str(joiner_enrollment["token"]),
                invitation_token=str(invitation["token"]),
            )
            offer = PairingCodeCodec(clock=lambda: NOW).decode(code)

            binding = await runtime._redeem(offer)

            assert binding.device_id != host.identity.node_id
            assert binding.internal_session_id == session.session_id
            assert binding.trusted is True
            assert coordinator.session_ids_for_node(binding.device_id) == (
                session.session_id,
            )
            status = await runtime._client.coordinator_status()  # type: ignore[union-attr]
            node_ids = {
                item["node_id"]
                for item in status["nodes"]
                if isinstance(item, dict)
            }
            assert {host.identity.node_id, binding.device_id} <= node_ids
        finally:
            if runtime._client is not None and runtime._client.connected_event.is_set():
                await runtime._client.disconnect()
            await relay.stop()

    asyncio.run(scenario())

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.projections import FederationAuthorityAdapter
from catalog.flask_app.services.federation_pairing_service import (
    PairingCodeCodec,
    RemoteCoordinatorFacade,
    RemotePairingState,
)
from catalog.flask_app.services.resilient_pairing_runtime import (
    ResilientPairingRelayRuntime,
)
from catalog.node.identity import IdentityStore
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 3, 18, 30, tzinfo=timezone.utc)


def test_signed_pairing_code_projects_same_members_from_both_viewpoints(
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
            enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            coordinator.enroll_node(
                host.identity,
                token=str(enrollment["token"]),
            )
            outsider = IdentityStore(
                tmp_path / "outsider",
                display_name="Enrolled outsider",
            ).load_or_create(now=NOW)
            outsider_enrollment = coordinator.create_enrollment_token(
                ttl_seconds=300,
                max_uses=1,
            )
            coordinator.enroll_node(
                outsider.identity,
                token=str(outsider_enrollment["token"]),
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

            binding = await asyncio.to_thread(runtime.redeem, offer)

            assert binding.device_id != host.identity.node_id
            assert binding.internal_session_id == session.session_id
            assert binding.trusted is True
            assert coordinator.session_ids_for_node(binding.device_id) == (
                session.session_id,
            )
            assert coordinator.session_ids_for_node(outsider.identity.node_id) == ()
            expected_members = {host.identity.node_id, binding.device_id}
            status = await asyncio.to_thread(runtime.coordinator_status)
            node_ids = {
                item["node_id"]
                for item in status["nodes"]
                if isinstance(item, dict)
            }
            assert node_ids == expected_members

            owner_snapshot = FederationAuthorityAdapter(
                coordinator,
                actor_node_id=host.identity.node_id,
                internal_session_id=session.session_id,
            ).snapshot()
            remote_state = RemotePairingState(relay.url, binding)
            remote_snapshot = await asyncio.to_thread(
                FederationAuthorityAdapter(
                    RemoteCoordinatorFacade(runtime, remote_state),
                    actor_node_id=binding.device_id,
                    internal_session_id=session.session_id,
                ).snapshot
            )

            assert owner_snapshot.available is True
            assert remote_snapshot.available is True
            assert {item.node_id for item in owner_snapshot.devices} == expected_members
            assert {item.node_id for item in remote_snapshot.devices} == expected_members
            assert outsider.identity.node_id not in {
                item.node_id
                for snapshot in (owner_snapshot, remote_snapshot)
                for item in snapshot.devices
            }
        finally:
            if runtime._loop is not None:
                await asyncio.to_thread(
                    lambda: runtime._submit(runtime._disconnect_current())
                )
                runtime._loop.call_soon_threadsafe(runtime._loop.stop)
                if runtime._thread is not None:
                    runtime._thread.join(timeout=5)
            await relay.stop()

    asyncio.run(scenario())

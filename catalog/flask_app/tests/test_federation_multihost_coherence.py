from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.federation.projections import FederationAuthorityAdapter
from catalog.flask_app.services.federation_pairing_service import RemotePairingState
from catalog.flask_app.services.resilient_pairing_runtime import (
    ResilientPairingRelayRuntime,
)
from catalog.node.identity import IdentityStore
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 8, 18, 45, tzinfo=timezone.utc)


def _binding(session: object, node_id: str) -> FederationSessionBinding:
    return FederationSessionBinding(
        federation_id=federation_id_from_session_id(session.session_id),
        internal_session_id=session.session_id,
        device_id=node_id,
        state=FederationConnectionState.CONNECTED,
        revision=session.revision,
        trusted=True,
        created_at=session.created_at,
        last_verified_at=NOW,
    )


class _SnapshotCoordinator:
    def __init__(
        self,
        coordinator: SessionCoordinator,
        status: dict[str, object],
    ) -> None:
        self._coordinator = coordinator
        self._status = status
        self.store = coordinator.store

    def status(self, *, actor_node_id: str, cursor: str | None = None):
        assert cursor is None
        assert actor_node_id in {
            item["node_id"]
            for item in self._status["nodes"]
            if isinstance(item, dict)
        }
        return self._status

    def replay_page(self, **kwargs):
        return self._coordinator.replay_page(**kwargs)


def test_two_authenticated_members_see_same_online_devices_and_services(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(
            tmp_path / "relay" / "control.sqlite3",
            clock=lambda: NOW,
        )
        host_credentials = IdentityStore(
            tmp_path / "host",
            display_name="This FCP device",
        ).load_or_create(now=NOW)
        peer_credentials = IdentityStore(
            tmp_path / "peer",
            display_name="This FCP device",
        ).load_or_create(now=NOW)

        for credentials in (host_credentials, peer_credentials):
            token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)
            coordinator.enroll_node(credentials.identity, token=str(token["token"]))

        created = coordinator.create_session(
            actor_node_id=host_credentials.identity.node_id,
            display_name="Shared Federation",
            request_id="coherence-session",
        )
        invitation = coordinator.create_invitation(
            session_id=created.session_id,
            actor_node_id=host_credentials.identity.node_id,
            ttl_seconds=300,
            max_uses=1,
            request_id="coherence-invite",
        )
        coordinator.join_session(
            node_id=peer_credentials.identity.node_id,
            token=str(invitation["token"]),
            request_id="coherence-join",
            expected_session_id=created.session_id,
        )
        session = coordinator.store.get_session(created.session_id)
        assert session is not None

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
        host_runtime = ResilientPairingRelayRuntime(
            state_directory=tmp_path / "host",
            display_name="This FCP device",
            clock=lambda: NOW,
            timeout_seconds=5,
        )
        peer_runtime = ResilientPairingRelayRuntime(
            state_directory=tmp_path / "peer",
            display_name="This FCP device",
            clock=lambda: NOW,
            timeout_seconds=5,
        )
        host_state = RemotePairingState(
            relay.url,
            _binding(session, host_credentials.identity.node_id),
        )
        peer_state = RemotePairingState(
            relay.url,
            _binding(session, peer_credentials.identity.node_id),
        )

        try:
            await host_runtime._ensure_connected(host_state)
            await peer_runtime._ensure_connected(peer_state)

            for runtime, state, node_id, prefix in (
                (host_runtime, host_state, host_credentials.identity.node_id, "host"),
                (peer_runtime, peer_state, peer_credentials.identity.node_id, "peer"),
            ):
                for index, capability_type in enumerate(
                    ("language-model", "compute", "storage"),
                    start=1,
                ):
                    await runtime._announce_capability(
                        state,
                        CapabilityAnnouncement(
                            capability_id=f"candidate-{prefix}-{index}",
                            node_id=node_id,
                            session_id=session.session_id,
                            type=capability_type,
                            protocol=f"fcp-{capability_type}",
                            protocol_version="1.0",
                            status=(
                                CapabilityStatus.READY
                                if capability_type == "language-model"
                                else CapabilityStatus.DISABLED
                            ),
                            properties={"kind": "capability-first-candidate"},
                            announced_at=NOW,
                        ),
                        request_id=f"coherence-{prefix}-{index}",
                    )

            host_client = host_runtime._connected_client()
            peer_client = peer_runtime._connected_client()
            host_status = await host_client.coordinator_status()
            peer_status = await peer_client.coordinator_status()

            def normalized(status: dict[str, object]) -> tuple[tuple[object, ...], ...]:
                nodes = tuple(
                    sorted(
                        (
                            item["node_id"],
                            item["connection_state"],
                        )
                        for item in status["nodes"]
                        if isinstance(item, dict)
                    )
                )
                capabilities = tuple(
                    sorted(
                        (
                            item["node_id"],
                            item["capability_id"],
                            item["status"],
                        )
                        for item in status["capabilities"]
                        if isinstance(item, dict)
                    )
                )
                return nodes, capabilities

            assert normalized(host_status) == normalized(peer_status)

            host_projection = FederationAuthorityAdapter(
                _SnapshotCoordinator(coordinator, host_status),
                actor_node_id=host_credentials.identity.node_id,
                internal_session_id=session.session_id,
            ).snapshot()
            peer_projection = FederationAuthorityAdapter(
                _SnapshotCoordinator(coordinator, peer_status),
                actor_node_id=peer_credentials.identity.node_id,
                internal_session_id=session.session_id,
            ).snapshot()

            for projection in (host_projection, peer_projection):
                assert projection.available is True
                assert len(projection.devices) == 2
                assert {device.state for device in projection.devices} == {"connected"}
                assert {device.capability_count for device in projection.devices} == {3}
                assert len(projection.capabilities) == 6
                assert sum(
                    capability.status == "ready"
                    for capability in projection.capabilities
                ) == 2
        finally:
            for runtime in (host_runtime, peer_runtime):
                client = runtime._client
                if client is not None and client.connected_event.is_set():
                    await client.disconnect()
            await relay.stop()

    asyncio.run(scenario())

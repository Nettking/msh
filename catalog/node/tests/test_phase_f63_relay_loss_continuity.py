from __future__ import annotations

import asyncio

import pytest

from catalog.federation.adaptive_transport import (
    DIRECT_CONTINUITY_STATUS_SCHEMA,
    AdaptiveStorageTransport,
    ControlPlaneState,
    DirectTransportState,
    DirectTransportUnavailable,
    TransportKind,
    TransportPathUnavailable,
)
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    StorageError,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)


class _Transport:
    def __init__(
        self,
        *,
        label: str,
        error: Exception | None = None,
        response: StorageResponseEnvelope | None = None,
    ) -> None:
        self.label = label
        self.error = error
        self.response = response
        self.calls: list[tuple[str, StorageRequestEnvelope]] = []

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        self.calls.append((target_node_id, envelope))
        if self.error is not None:
            raise self.error
        if self.response is not None:
            return self.response
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=envelope.protocol_version,
            ok=True,
            result={"transport": self.label},
        )


def _request(request_id: str = "request-f63") -> StorageRequestEnvelope:
    return StorageRequestEnvelope(
        request_id=request_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.HEALTH,
        session_id="session-f63",
        actor_node_id="node-a",
        authorization_context={"provider_id": "storage-b"},
        payload={},
    )


def test_f63_established_direct_path_survives_relay_interruption() -> None:
    async def scenario() -> None:
        relay = _Transport(label="relay", error=RuntimeError("relay offline"))
        direct = _Transport(label="direct")
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=direct,
            direct_state=DirectTransportState.READY,
            control_plane_state=ControlPlaneState.INTERRUPTED,
        )

        response = await transport.request(
            target_node_id="node-b",
            envelope=_request(),
        )

        assert response.result == {"transport": "direct"}
        assert len(direct.calls) == 1
        assert relay.calls == []
        assert transport.status().last_reason == "direct-ready-control-interrupted-f63"
        continuity = transport.continuity_status().to_dict()
        assert continuity == {
            "schema": DIRECT_CONTINUITY_STATUS_SCHEMA,
            "control_plane_state": "interrupted",
            "direct_state": "ready",
            "direct_data_plane_available": True,
            "relay_fallback_available": False,
            "authority_refresh_available": False,
            "membership_updates_available": False,
            "coordinated_failover_available": False,
            "limitations": [
                "authority-refresh-unavailable",
                "membership-updates-unavailable",
                "route-updates-unavailable",
                "coordinated-failover-unavailable",
                "relay-fallback-unavailable",
            ],
        }

    asyncio.run(scenario())


def test_f63_recovering_control_plane_does_not_interrupt_direct_data() -> None:
    async def scenario() -> None:
        relay = _Transport(label="relay", error=RuntimeError("relay reconnecting"))
        direct = _Transport(label="direct")
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=direct,
            direct_state=DirectTransportState.READY,
            control_plane_state=ControlPlaneState.RECOVERING,
        )

        response = await transport.request(
            target_node_id="node-b",
            envelope=_request(),
        )

        assert response.ok is True
        assert relay.calls == []
        assert transport.status().last_reason == "direct-ready-control-recovering-f63"
        assert transport.continuity_status().relay_fallback_available is False

    asyncio.run(scenario())


def test_f63_direct_failure_does_not_attempt_known_unavailable_relay() -> None:
    async def scenario() -> None:
        relay = _Transport(label="relay")
        direct = _Transport(
            label="direct",
            error=DirectTransportUnavailable("direct-lost", "direct peer lost"),
        )
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=direct,
            direct_state=DirectTransportState.READY,
            control_plane_state=ControlPlaneState.INTERRUPTED,
        )

        with pytest.raises(TransportPathUnavailable) as raised:
            await transport.request(
                target_node_id="node-b",
                envelope=_request(),
            )

        assert raised.value.code == "direct-and-relay-unavailable"
        assert relay.calls == []
        assert transport.status().selected_transport is TransportKind.NONE
        assert transport.status().last_reason == "direct-failed-relay-unavailable-f63"
        assert transport.status().last_outcome == "failed"

    asyncio.run(scenario())


def test_f63_no_direct_path_fails_without_touching_interrupted_relay() -> None:
    async def scenario() -> None:
        relay = _Transport(label="relay")
        transport = AdaptiveStorageTransport(
            relay,
            control_plane_state=ControlPlaneState.INTERRUPTED,
        )

        with pytest.raises(TransportPathUnavailable) as raised:
            await transport.request(
                target_node_id="node-b",
                envelope=_request(),
            )

        assert raised.value.code == "control-plane-interrupted-no-direct-path"
        assert relay.calls == []
        assert transport.decision().selected is TransportKind.NONE

    asyncio.run(scenario())


def test_f63_control_recovery_restores_bounded_relay_fallback() -> None:
    async def scenario() -> None:
        relay = _Transport(label="relay")
        direct = _Transport(
            label="direct",
            error=DirectTransportUnavailable("direct-lost", "direct peer lost"),
        )
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=direct,
            direct_state=DirectTransportState.READY,
            control_plane_state=ControlPlaneState.INTERRUPTED,
        )

        with pytest.raises(TransportPathUnavailable):
            await transport.request(
                target_node_id="node-b",
                envelope=_request("request-before-recovery"),
            )
        transport.report_control_plane_state(ControlPlaneState.AVAILABLE)
        response = await transport.request(
            target_node_id="node-b",
            envelope=_request("request-after-recovery"),
        )

        assert response.result == {"transport": "relay"}
        assert len(relay.calls) == 1
        assert transport.status().last_reason == "direct-failed-relay-fallback"
        continuity = transport.continuity_status()
        assert continuity.relay_fallback_available is True
        assert continuity.authority_refresh_available is True
        assert continuity.limitations == ()

    asyncio.run(scenario())


def test_f63_direct_continuity_does_not_extend_expired_authority() -> None:
    async def scenario() -> None:
        request = _request()
        rejection = StorageResponseEnvelope(
            request_id=request.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=request.protocol_version,
            ok=False,
            error=StorageError(
                code=StorageErrorCode.LEASE_EXPIRED,
                message="existing authority lease expired",
                field="write_authority.lease_expires_at",
            ),
        )
        relay = _Transport(label="relay")
        direct = _Transport(label="direct", response=rejection)
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=direct,
            direct_state=DirectTransportState.READY,
            control_plane_state=ControlPlaneState.INTERRUPTED,
        )

        response = await transport.request(
            target_node_id="node-b",
            envelope=request,
        )

        assert response is rejection
        assert response.ok is False
        assert response.error is not None
        assert response.error.code is StorageErrorCode.LEASE_EXPIRED
        assert relay.calls == []
        assert transport.continuity_status().authority_refresh_available is False

    asyncio.run(scenario())

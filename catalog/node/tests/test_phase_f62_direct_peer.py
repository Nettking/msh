from __future__ import annotations

import asyncio
import base64
import copy
from collections.abc import Callable
from types import SimpleNamespace
from typing import ClassVar

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from catalog.federation.adaptive_transport import (
    AdaptiveStorageTransport,
    DirectTransportState,
    TransportKind,
)
from catalog.federation.direct_peer import ChannelReady, DirectStorageEndpoint
from catalog.federation.errors import FederationValidationError
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


class _Credentials:
    def __init__(self, node_id: str) -> None:
        self.private_key = Ed25519PrivateKey.generate()
        raw = self.private_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        self.identity = SimpleNamespace(
            node_id=node_id,
            public_key=f"ed25519:{_b64(raw)}",
        )

    def sign(self, value: bytes) -> str:
        return _b64(self.private_key.sign(value))


def _verify(public_key: str, value: bytes, signature: str) -> bool:
    encoded_key = public_key.removeprefix("ed25519:")
    raw_key = base64.urlsafe_b64decode(
        encoded_key + "=" * (-len(encoded_key) % 4)
    )
    raw_signature = base64.urlsafe_b64decode(
        signature + "=" * (-len(signature) % 4)
    )
    try:
        Ed25519PublicKey.from_public_bytes(raw_key).verify(raw_signature, value)
    except InvalidSignature:
        return False
    return True


class _MemoryChannel:
    registry: ClassVar[dict[str, _MemoryChannel]] = {}

    def __init__(self, peer_id: str) -> None:
        self.peer_id = peer_id
        self.handler = None
        self.available = True
        self.source_peer_override: str | None = None
        self.interceptor: Callable[[dict], dict] | None = None
        self.last_payload: dict | None = None

    async def start(self, handler) -> ChannelReady:
        self.handler = handler
        self.registry[self.peer_id] = self
        return ChannelReady(self.peer_id, (f"/memory/{self.peer_id}",))

    async def request(
        self,
        *,
        target_peer_id: str,
        target_multiaddr: str,
        payload: dict,
    ) -> dict:
        del target_multiaddr
        if not self.available:
            raise OSError("direct peer offline")
        target = self.registry[target_peer_id]
        value = copy.deepcopy(payload)
        self.last_payload = copy.deepcopy(value)
        if self.interceptor is not None:
            value = self.interceptor(value)
        source_peer_id = self.source_peer_override or self.peer_id
        return await target.handler(source_peer_id, value)

    async def close(self) -> None:
        self.registry.pop(self.peer_id, None)


class _Service:
    def __init__(self) -> None:
        self.requests: list[StorageRequestEnvelope] = []

    async def dispatch(self, request: StorageRequestEnvelope) -> StorageResponseEnvelope:
        self.requests.append(request)
        return StorageResponseEnvelope(
            request_id=request.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=request.protocol_version,
            ok=True,
            result={"transport": "direct", "operation": request.operation.value},
        )


class _Relay:
    def __init__(self) -> None:
        self.calls: list[tuple[str, StorageRequestEnvelope]] = []

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        self.calls.append((target_node_id, envelope))
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=envelope.protocol_version,
            ok=True,
            result={"transport": "relay"},
        )


def _request(actor_node_id: str, request_id: str = "request-f62") -> StorageRequestEnvelope:
    return StorageRequestEnvelope(
        request_id=request_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.HEALTH,
        session_id="session-f62",
        actor_node_id=actor_node_id,
        authorization_context={"provider_id": "storage-b"},
        payload={},
    )


async def _pair():
    _MemoryChannel.registry.clear()
    credentials_a = _Credentials("node-a")
    credentials_b = _Credentials("node-b")
    public_keys = {
        "node-a": credentials_a.identity.public_key,
        "node-b": credentials_b.identity.public_key,
    }
    channel_a = _MemoryChannel("peer-a")
    channel_b = _MemoryChannel("peer-b")
    service = _Service()
    endpoint_a = DirectStorageEndpoint(
        channel_a,
        credentials_a,
        resolve_public_key=public_keys.get,
        verify_signature=_verify,
    )
    endpoint_b = DirectStorageEndpoint(
        channel_b,
        credentials_b,
        resolve_public_key=public_keys.get,
        verify_signature=_verify,
        services={"storage-b": service},
    )
    descriptor_a = await endpoint_a.start()
    descriptor_b = await endpoint_b.start()
    endpoint_a.register_peer(descriptor_b)
    endpoint_b.register_peer(descriptor_a)
    return endpoint_a, endpoint_b, channel_a, channel_b, service


def test_f62_direct_encrypted_storage_round_trip() -> None:
    async def scenario() -> None:
        endpoint_a, _, _, _, service = await _pair()
        response = await endpoint_a.request(
            target_node_id="node-b",
            envelope=_request("node-a"),
        )
        assert response.ok is True
        assert response.result == {
            "transport": "direct",
            "operation": "storage.health",
        }
        assert len(service.requests) == 1
        captured = _MemoryChannel.registry["peer-a"].last_payload
        assert captured is not None
        assert "authorization_context" not in str(captured)
        assert "storage-b" not in str(captured)
        assert "ciphertext" in captured["frame"]

    asyncio.run(scenario())


def test_f62_adaptive_transport_prefers_ready_direct_path() -> None:
    async def scenario() -> None:
        endpoint_a, _, _, _, _ = await _pair()
        relay = _Relay()
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=endpoint_a,
            direct_state=DirectTransportState.READY,
        )
        response = await transport.request(
            target_node_id="node-b",
            envelope=_request("node-a"),
        )
        assert response.result["transport"] == "direct"
        assert relay.calls == []
        status = transport.status().to_dict()
        assert status["selected_transport"] == "direct"
        assert status["direct_transport_enabled"] is True
        assert status["direct_request_count"] == 1
        assert status["relay_request_count"] == 0
        assert status["last_reason"] == "direct-ready-f62"
        assert transport.decision().selected is TransportKind.DIRECT

    asyncio.run(scenario())


def test_f62_expected_direct_failure_falls_back_to_relay() -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, _, _ = await _pair()
        channel_a.available = False
        relay = _Relay()
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=endpoint_a,
            direct_state=DirectTransportState.READY,
        )
        response = await transport.request(
            target_node_id="node-b",
            envelope=_request("node-a"),
        )
        assert response.result == {"transport": "relay"}
        assert len(relay.calls) == 1
        status = transport.status().to_dict()
        assert status["direct_request_count"] == 1
        assert status["relay_request_count"] == 1
        assert status["fallback_count"] == 1
        assert status["last_reason"] == "direct-failed-relay-fallback"

    asyncio.run(scenario())


def test_f62_tampering_fails_closed_without_relay_downgrade() -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, _, _ = await _pair()

        def tamper(packet: dict) -> dict:
            packet["frame"]["ciphertext_hash"] = "sha256:" + "0" * 64
            return packet

        channel_a.interceptor = tamper
        relay = _Relay()
        transport = AdaptiveStorageTransport(
            relay,
            direct_transport=endpoint_a,
            direct_state=DirectTransportState.READY,
        )
        with pytest.raises(FederationValidationError):
            await transport.request(
                target_node_id="node-b",
                envelope=_request("node-a"),
            )
        assert relay.calls == []

    asyncio.run(scenario())


def test_f62_libp2p_peer_identity_must_match_enrolled_node() -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, _, _ = await _pair()
        channel_a.source_peer_override = "peer-attacker"
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a.request(
                target_node_id="node-b",
                envelope=_request("node-a"),
            )
        assert raised.value.code == "direct-peer-identity-mismatch"

    asyncio.run(scenario())


def test_f62_replayed_stream_key_is_rejected_before_second_dispatch() -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, channel_b, service = await _pair()
        await endpoint_a.request(
            target_node_id="node-b",
            envelope=_request("node-a"),
        )
        assert channel_a.last_payload is not None
        with pytest.raises(FederationValidationError) as raised:
            await channel_b.handler("peer-a", copy.deepcopy(channel_a.last_payload))
        assert raised.value.code == "direct-peer-replayed"
        assert len(service.requests) == 1

    asyncio.run(scenario())


def test_f62_wrong_actor_fails_before_direct_delivery() -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, _, _ = await _pair()
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a.request(
                target_node_id="node-b",
                envelope=_request("node-other"),
            )
        assert raised.value.code == "direct-peer-actor-mismatch"
        assert channel_a.last_payload is None

    asyncio.run(scenario())


def test_f62_unconfigured_ready_state_preserves_relay_only_behavior() -> None:
    async def scenario() -> None:
        relay = _Relay()
        transport = AdaptiveStorageTransport(
            relay,
            direct_state=DirectTransportState.READY,
        )
        response = await transport.request(
            target_node_id="node-b",
            envelope=_request("node-a"),
        )
        assert response.result == {"transport": "relay"}
        assert transport.status().last_reason == "direct-not-enabled-until-f62"

    asyncio.run(scenario())

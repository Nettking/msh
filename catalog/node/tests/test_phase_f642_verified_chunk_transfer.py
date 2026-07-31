from __future__ import annotations

import asyncio
import base64
import copy
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from catalog.federation.adaptive_transport import DirectTransportUnavailable
from catalog.federation.direct_peer import ChannelReady
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.verified_chunk_transfer import VerifiedChunkTransferEndpoint


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
        self.requests: list[dict] = []
        self.drop_before: set[int] = set()
        self.drop_after: set[int] = set()
        self.corrupt: set[int] = set()
        self.source_peer_override: str | None = None
        self.before_delivery: Callable[[int], None] | None = None

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
        request_number = len(self.requests)
        self.requests.append(copy.deepcopy(payload))
        if request_number in self.drop_before:
            self.drop_before.remove(request_number)
            raise TimeoutError("packet dropped before delivery")
        value = copy.deepcopy(payload)
        corrupted = request_number in self.corrupt
        if corrupted:
            self.corrupt.remove(request_number)
            value["frame"]["ciphertext_hash"] = "sha256:" + "0" * 64
        if self.before_delivery is not None:
            self.before_delivery(request_number)
        target = self.registry[target_peer_id]
        try:
            response = await target.handler(
                self.source_peer_override or self.peer_id,
                value,
            )
        except FederationValidationError as exc:
            if not corrupted:
                raise
            raise OSError("corrupted transport delivery") from exc
        if request_number in self.drop_after:
            self.drop_after.remove(request_number)
            raise TimeoutError("acknowledgement dropped after acceptance")
        return response

    async def close(self) -> None:
        self.registry.pop(self.peer_id, None)


async def _pair(
    root: Path,
    *,
    max_attempts: int = 4,
) -> tuple[
    VerifiedChunkTransferEndpoint,
    VerifiedChunkTransferEndpoint,
    _MemoryChannel,
    _MemoryChannel,
]:
    _MemoryChannel.registry.clear()
    credentials_a = _Credentials("node-a")
    credentials_b = _Credentials("node-b")
    public_keys = {
        "node-a": credentials_a.identity.public_key,
        "node-b": credentials_b.identity.public_key,
    }
    channel_a = _MemoryChannel("peer-a")
    channel_b = _MemoryChannel("peer-b")
    endpoint_a = VerifiedChunkTransferEndpoint(
        channel_a,
        credentials_a,
        resolve_public_key=public_keys.get,
        verify_signature=_verify,
        publication_root=root / "published-a",
        staging_parent=root / "staging-a",
        max_attempts=max_attempts,
    )
    endpoint_b = VerifiedChunkTransferEndpoint(
        channel_b,
        credentials_b,
        resolve_public_key=public_keys.get,
        verify_signature=_verify,
        publication_root=root / "published-b",
        staging_parent=root / "staging-b",
        max_attempts=max_attempts,
    )
    descriptor_a = await endpoint_a.start()
    descriptor_b = await endpoint_b.start()
    endpoint_a.register_peer(descriptor_b)
    endpoint_b.register_peer(descriptor_a)
    return endpoint_a, endpoint_b, channel_a, channel_b


def test_f642_encrypted_multi_chunk_round_trip(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        content = (b"phase-f642-payload-" * 8_000) + b"end"
        receipt = await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-round-trip",
            object_id="dataset/f642/object",
            content=content,
            chunk_size=32_768,
        )
        published = endpoint_b.store.published_path("dataset/f642/object")
        assert published is not None
        assert published.read_bytes() == content
        assert receipt.total_size == len(content)
        assert receipt.object_hash.startswith("sha256:")
        assert endpoint_b.store.stored_bytes == 0
        wire = str(channel_a.requests)
        assert "phase-f642-payload" not in wire
        assert "dataset/f642/object" not in wire
        assert "ciphertext" in wire
        await endpoint_a.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f642_empty_object_is_verified_and_published(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        receipt = await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-empty",
            object_id="empty-object",
            content=b"",
        )
        published = endpoint_b.store.published_path("empty-object")
        assert published is not None
        assert published.read_bytes() == b""
        assert receipt.chunk_count == 0
        assert len(channel_a.requests) == 2

    asyncio.run(scenario())


def test_f642_drop_before_delivery_uses_new_sequence(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        channel_a.drop_before.add(0)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-drop-before",
            object_id="drop-before",
            content=b"x" * 40_000,
        )
        assert channel_a.requests[0]["frame"]["sequence"] == 0
        assert channel_a.requests[1]["frame"]["sequence"] == 1
        assert endpoint_b.store.published_path("drop-before") is not None

    asyncio.run(scenario())


def test_f642_lost_chunk_ack_redelivers_logical_chunk(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        channel_a.drop_after.add(1)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-lost-chunk-ack",
            object_id="lost-chunk-ack",
            content=b"x" * 1_000,
            chunk_size=1_000,
        )
        assert len(channel_a.requests) == 4
        assert channel_a.requests[1]["frame"]["sequence"] == 1
        assert channel_a.requests[2]["frame"]["sequence"] == 2
        published = endpoint_b.store.published_path("lost-chunk-ack")
        assert published is not None
        assert published.read_bytes() == b"x" * 1_000

    asyncio.run(scenario())


def test_f642_lost_completion_ack_is_idempotent(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        channel_a.drop_after.add(2)
        receipt = await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-lost-complete-ack",
            object_id="lost-complete-ack",
            content=b"complete",
            chunk_size=8,
        )
        assert receipt.object_id == "lost-complete-ack"
        assert len(channel_a.requests) == 4
        published = endpoint_b.store.published_path("lost-complete-ack")
        assert published is not None
        assert published.read_bytes() == b"complete"

    asyncio.run(scenario())


def test_f642_corrupted_delivery_is_retransmitted(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        channel_a.corrupt.add(1)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-corrupt",
            object_id="corrupt-retry",
            content=b"verified" * 200,
            chunk_size=1_600,
        )
        assert len(channel_a.requests) == 4
        published = endpoint_b.store.published_path("corrupt-retry")
        assert published is not None
        assert published.read_bytes() == b"verified" * 200

    asyncio.run(scenario())


def test_f642_retry_exhaustion_does_not_publish(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(
            tmp_path,
            max_attempts=2,
        )
        channel_a.drop_before.update({0, 1})
        with pytest.raises(DirectTransportUnavailable) as raised:
            await endpoint_a.send_bytes(
                target_node_id="node-b",
                session_id="session-f642",
                request_id="request-f642-exhausted",
                object_id="retry-exhausted",
                content=b"never-published",
            )
        assert raised.value.code == "object-transfer-retry-exhausted"
        assert endpoint_b.store.published_path("retry-exhausted") is None

    asyncio.run(scenario())


def test_f642_peer_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        channel_a.source_peer_override = "peer-attacker"
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a.send_bytes(
                target_node_id="node-b",
                session_id="session-f642",
                request_id="request-f642-peer-mismatch",
                object_id="peer-mismatch",
                content=b"secret",
            )
        assert raised.value.code == "object-transfer-peer-identity-mismatch"
        assert endpoint_b.store.published_path("peer-mismatch") is None

    asyncio.run(scenario())


def test_f642_exact_transport_frame_replay_is_rejected(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, _, channel_a, channel_b = await _pair(tmp_path)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-replay",
            object_id="replay",
            content=b"once",
            chunk_size=4,
        )
        with pytest.raises(FederationValidationError) as raised:
            await channel_b.handler("peer-a", copy.deepcopy(channel_a.requests[0]))
        assert raised.value.code == "object-transfer-frame-replayed"

    asyncio.run(scenario())


def test_f642_staged_chunk_tampering_blocks_publication(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)

        def tamper_before_complete(request_number: int) -> None:
            if request_number != 2:
                return
            inbound = next(iter(endpoint_b._inbound.values()))
            assert inbound.manifest is not None
            path = endpoint_b.store._chunk_path(inbound.manifest.transfer_id, 0)
            path.write_bytes(b"tampered")

        channel_a.before_delivery = tamper_before_complete
        with pytest.raises(FederationOperationError) as raised:
            await endpoint_a.send_bytes(
                target_node_id="node-b",
                session_id="session-f642",
                request_id="request-f642-stage-tamper",
                object_id="stage-tamper",
                content=b"original",
                chunk_size=8,
            )
        assert raised.value.code == "object-transfer-stored-chunk-mismatch"
        assert endpoint_b.store.published_path("stage-tamper") is None

    asyncio.run(scenario())


def test_f642_source_growth_aborts_before_publication(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, channel_a, _ = await _pair(tmp_path)
        source = tmp_path / "source.bin"
        source.write_bytes(b"a" * 1_000)

        def grow_source(request_number: int) -> None:
            if request_number == 1:
                with source.open("ab") as handle:
                    handle.write(b"growth")

        channel_a.before_delivery = grow_source
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a.send_file(
                target_node_id="node-b",
                session_id="session-f642",
                request_id="request-f642-source-growth",
                object_id="source-growth",
                source_path=source,
                chunk_size=1_000,
            )
        assert raised.value.code == "object-transfer-source-size-changed"
        assert endpoint_b.store.published_path("source-growth") is None

    asyncio.run(scenario())


def test_f642_publication_conflict_preserves_existing_object(tmp_path: Path) -> None:
    async def scenario() -> None:
        endpoint_a, endpoint_b, _, _ = await _pair(tmp_path)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f642",
            request_id="request-f642-conflict-1",
            object_id="same-object-id",
            content=b"first",
            chunk_size=5,
        )
        with pytest.raises(FederationOperationError) as raised:
            await endpoint_a.send_bytes(
                target_node_id="node-b",
                session_id="session-f642",
                request_id="request-f642-conflict-2",
                object_id="same-object-id",
                content=b"second",
                chunk_size=6,
            )
        assert raised.value.code == "object-transfer-publication-conflict"
        published = endpoint_b.store.published_path("same-object-id")
        assert published is not None
        assert published.read_bytes() == b"first"

    asyncio.run(scenario())

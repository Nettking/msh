from __future__ import annotations

import asyncio
import base64
import copy
import shutil
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
from catalog.federation.errors import FederationValidationError
from catalog.federation.object_transfer import ObjectTransferManifest
from catalog.federation.resumable_chunk_transfer import (
    ResumableChunkTransferEndpoint,
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
        self.requests: list[dict] = []
        self.drop_before: set[int] = set()
        self.drop_after: set[int] = set()

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
        target = self.registry[target_peer_id]
        response = await target.handler(self.peer_id, copy.deepcopy(payload))
        if request_number in self.drop_after:
            self.drop_after.remove(request_number)
            raise TimeoutError("acknowledgement dropped after acceptance")
        return response

    async def close(self) -> None:
        self.registry.pop(self.peer_id, None)


def _endpoint(
    *,
    channel: _MemoryChannel,
    credentials: _Credentials,
    public_keys: dict[str, str],
    publication_root: Path,
    durable_root: Path,
    max_attempts: int = 1,
    clock=lambda: 100.0,
) -> ResumableChunkTransferEndpoint:
    return ResumableChunkTransferEndpoint(
        channel,
        credentials,
        resolve_public_key=public_keys.get,
        verify_signature=_verify,
        publication_root=publication_root,
        durable_root=durable_root,
        max_attempts=max_attempts,
        clock=clock,
    )


async def _connect(
    left: ResumableChunkTransferEndpoint,
    right: ResumableChunkTransferEndpoint,
) -> None:
    left_descriptor = await left.start()
    right_descriptor = await right.start()
    left.register_peer(right_descriptor)
    right.register_peer(left_descriptor)


def test_f65_sender_restart_resumes_from_remote_cursor(tmp_path: Path) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        content = b"a" * 2_500
        source.write_bytes(content)
        channel_a1 = _MemoryChannel("peer-a1")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a1 = _endpoint(
            channel=channel_a1,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a1, endpoint_b)
        channel_a1.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable) as raised:
            await endpoint_a1.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-sender-restart",
                object_id="object-sender-restart",
                source_path=source,
                chunk_size=1_000,
            )
        assert raised.value.code == "object-transfer-retry-exhausted"
        assert endpoint_b.store.stored_bytes == 1_000
        await endpoint_a1.close()

        channel_a2 = _MemoryChannel("peer-a2")
        endpoint_a2 = _endpoint(
            channel=channel_a2,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        descriptor_a2 = await endpoint_a2.start()
        endpoint_a2.register_peer(endpoint_b.descriptor)
        endpoint_b.register_peer(descriptor_a2)
        receipt = await endpoint_a2.send_file(
            target_node_id="node-b",
            session_id="session-f65-restarted",
            request_id="request-sender-restart",
            object_id="object-sender-restart",
            source_path=source,
            chunk_size=1_000,
        )
        assert receipt.total_size == len(content)
        assert len(channel_a2.requests) == 4
        published = endpoint_b.store.published_path("object-sender-restart")
        assert published is not None
        assert published.read_bytes() == content
        await endpoint_a2.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f65_receiver_restart_recovers_chunks_from_disk(tmp_path: Path) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        content = b"b" * 2_500
        source.write_bytes(content)
        channel_a = _MemoryChannel("peer-a")
        channel_b1 = _MemoryChannel("peer-b1")
        endpoint_a = _endpoint(
            channel=channel_a,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b1 = _endpoint(
            channel=channel_b1,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a, endpoint_b1)
        channel_a.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-receiver-restart",
                object_id="object-receiver-restart",
                source_path=source,
                chunk_size=1_000,
            )
        await endpoint_b1.close()

        channel_b2 = _MemoryChannel("peer-b2")
        endpoint_b2 = _endpoint(
            channel=channel_b2,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        descriptor_b2 = await endpoint_b2.start()
        endpoint_a.register_peer(descriptor_b2)
        endpoint_b2.register_peer(endpoint_a.descriptor)
        assert endpoint_b2.store.stored_bytes == 1_000
        receipt = await endpoint_a.send_file(
            target_node_id="node-b",
            session_id="session-f65-restarted",
            request_id="request-receiver-restart",
            object_id="object-receiver-restart",
            source_path=source,
            chunk_size=1_000,
        )
        assert receipt.chunk_count == 3
        published = endpoint_b2.store.published_path("object-receiver-restart")
        assert published is not None
        assert published.read_bytes() == content
        await endpoint_a.close()
        await endpoint_b2.close()

    asyncio.run(scenario())


def test_f65_completed_receipt_survives_sender_restart(tmp_path: Path) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        source.write_bytes(b"completed")
        channel_a1 = _MemoryChannel("peer-a1")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a1 = _endpoint(
            channel=channel_a1,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a1, endpoint_b)
        first = await endpoint_a1.send_file(
            target_node_id="node-b",
            session_id="session-f65",
            request_id="request-completed",
            object_id="object-completed",
            source_path=source,
            chunk_size=9,
        )
        await endpoint_a1.close()

        channel_a2 = _MemoryChannel("peer-a2")
        endpoint_a2 = _endpoint(
            channel=channel_a2,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        descriptor_a2 = await endpoint_a2.start()
        endpoint_a2.register_peer(endpoint_b.descriptor)
        endpoint_b.register_peer(descriptor_a2)
        second = await endpoint_a2.send_file(
            target_node_id="node-b",
            session_id="session-f65-new",
            request_id="request-completed",
            object_id="object-completed",
            source_path=source,
            chunk_size=9,
        )
        assert second == first
        assert channel_a2.requests == []
        await endpoint_a2.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f65_source_mutation_blocks_resume(tmp_path: Path) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        source.write_bytes(b"a" * 2_000)
        channel_a1 = _MemoryChannel("peer-a1")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a1 = _endpoint(
            channel=channel_a1,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a1, endpoint_b)
        channel_a1.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a1.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-mutated",
                object_id="object-mutated",
                source_path=source,
                chunk_size=1_000,
            )
        await endpoint_a1.close()
        source.write_bytes(b"z" * 2_000)

        channel_a2 = _MemoryChannel("peer-a2")
        endpoint_a2 = _endpoint(
            channel=channel_a2,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        descriptor_a2 = await endpoint_a2.start()
        endpoint_a2.register_peer(endpoint_b.descriptor)
        endpoint_b.register_peer(descriptor_a2)
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a2.send_file(
                target_node_id="node-b",
                session_id="session-f65-new",
                request_id="request-mutated",
                object_id="object-mutated",
                source_path=source,
                chunk_size=1_000,
            )
        assert raised.value.code == "object-transfer-resume-source-mismatch"
        assert channel_a2.requests == []
        assert endpoint_b.store.published_path("object-mutated") is None
        await endpoint_a2.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f65_tampered_durable_chunk_fails_closed_after_restart(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        source.write_bytes(b"x" * 2_000)
        channel_a = _MemoryChannel("peer-a")
        channel_b1 = _MemoryChannel("peer-b1")
        endpoint_a = _endpoint(
            channel=channel_a,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b1 = _endpoint(
            channel=channel_b1,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a, endpoint_b1)
        channel_a.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-tamper",
                object_id="object-tamper",
                source_path=source,
                chunk_size=1_000,
            )
        current = endpoint_b1.journal.incoming_records()[0]
        manifest = ObjectTransferManifest.from_dict(current["manifest"])
        chunk_path = endpoint_b1.store._chunk_path(manifest.transfer_id, 0)
        await endpoint_b1.close()
        chunk_path.write_bytes(b"tampered")

        channel_b2 = _MemoryChannel("peer-b2")
        endpoint_b2 = _endpoint(
            channel=channel_b2,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        descriptor_b2 = await endpoint_b2.start()
        endpoint_a.register_peer(descriptor_b2)
        endpoint_b2.register_peer(endpoint_a.descriptor)
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_a.send_file(
                target_node_id="node-b",
                session_id="session-f65-new",
                request_id="request-tamper",
                object_id="object-tamper",
                source_path=source,
                chunk_size=1_000,
            )
        assert raised.value.code == "object-transfer-durable-chunk-mismatch"
        assert endpoint_b2.store.published_path("object-tamper") is None
        await endpoint_a.close()
        await endpoint_b2.close()

    asyncio.run(scenario())


def test_f65_lost_completion_ack_replays_durable_receipt_after_both_restart(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        source = tmp_path / "source.bin"
        source.write_bytes(b"receipt")
        channel_a1 = _MemoryChannel("peer-a1")
        channel_b1 = _MemoryChannel("peer-b1")
        endpoint_a1 = _endpoint(
            channel=channel_a1,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b1 = _endpoint(
            channel=channel_b1,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a1, endpoint_b1)
        channel_a1.drop_after.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a1.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-lost-complete",
                object_id="object-lost-complete",
                source_path=source,
                chunk_size=7,
            )
        assert (
            endpoint_b1.journal.incoming_records()[0]["state"] == "completed"
        )
        await endpoint_a1.close()
        await endpoint_b1.close()

        channel_a2 = _MemoryChannel("peer-a2")
        channel_b2 = _MemoryChannel("peer-b2")
        endpoint_a2 = _endpoint(
            channel=channel_a2,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b2 = _endpoint(
            channel=channel_b2,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a2, endpoint_b2)
        receipt = await endpoint_a2.send_file(
            target_node_id="node-b",
            session_id="session-f65-new",
            request_id="request-lost-complete",
            object_id="object-lost-complete",
            source_path=source,
            chunk_size=7,
        )
        assert receipt.object_id == "object-lost-complete"
        assert len(channel_a2.requests) == 2
        published = endpoint_b2.store.published_path("object-lost-complete")
        assert published is not None
        assert published.read_bytes() == b"receipt"
        await endpoint_a2.close()
        await endpoint_b2.close()

    asyncio.run(scenario())


def test_f65_stale_cleanup_removes_only_active_durable_state(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        now = [100.0]

        def clock() -> float:
            return now[0]

        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        channel_a = _MemoryChannel("peer-a")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a = _endpoint(
            channel=channel_a,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
            clock=clock,
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
            clock=clock,
        )
        await _connect(endpoint_a, endpoint_b)
        channel_a.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a.send_bytes(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-cleanup",
                object_id="object-cleanup",
                content=b"c" * 2_000,
                chunk_size=1_000,
            )
        outgoing = endpoint_a.journal.load_outgoing(
            endpoint_a.journal.outgoing_key(
                "node-b",
                "request-cleanup",
                "object-cleanup",
            )
        )
        assert outgoing is not None
        assert Path(outgoing["source_path"]).is_file()
        assert endpoint_b.store.stored_bytes == 1_000

        now[0] = 1_000.0
        sender_cleanup = await endpoint_a.cleanup_abandoned(
            older_than_seconds=100,
        )
        receiver_cleanup = await endpoint_b.cleanup_abandoned(
            older_than_seconds=100,
        )
        assert sender_cleanup.outgoing_removed == 1
        assert sender_cleanup.managed_sources_removed == 1
        assert receiver_cleanup.incoming_removed == 1
        assert receiver_cleanup.staged_bytes_released == 1_000
        assert endpoint_b.store.stored_bytes == 0
        assert endpoint_b.store.published_path("object-cleanup") is None
        await endpoint_a.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f65_cleanup_preserves_completed_receipts_and_publication(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        now = [100.0]

        def clock() -> float:
            return now[0]

        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
        }
        channel_a = _MemoryChannel("peer-a")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a = _endpoint(
            channel=channel_a,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
            clock=clock,
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
            clock=clock,
        )
        await _connect(endpoint_a, endpoint_b)
        await endpoint_a.send_bytes(
            target_node_id="node-b",
            session_id="session-f65",
            request_id="request-complete-cleanup",
            object_id="object-complete-cleanup",
            content=b"done",
            chunk_size=4,
        )
        now[0] = 10_000.0
        sender_cleanup = await endpoint_a.cleanup_abandoned(
            older_than_seconds=100,
        )
        receiver_cleanup = await endpoint_b.cleanup_abandoned(
            older_than_seconds=100,
        )
        assert sender_cleanup.outgoing_removed == 0
        assert receiver_cleanup.incoming_removed == 0
        published = endpoint_b.store.published_path("object-complete-cleanup")
        assert published is not None
        assert published.read_bytes() == b"done"
        assert endpoint_b.journal.incoming_records()[0]["state"] == "completed"
        await endpoint_a.close()
        await endpoint_b.close()

    asyncio.run(scenario())


def test_f65_durable_state_remains_bound_to_enrolled_source_node(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _MemoryChannel.registry.clear()
        credentials_a = _Credentials("node-a")
        credentials_b = _Credentials("node-b")
        credentials_c = _Credentials("node-c")
        public_keys = {
            "node-a": credentials_a.identity.public_key,
            "node-b": credentials_b.identity.public_key,
            "node-c": credentials_c.identity.public_key,
        }
        source = tmp_path / "source.bin"
        source.write_bytes(b"d" * 2_000)
        channel_a = _MemoryChannel("peer-a")
        channel_b = _MemoryChannel("peer-b")
        endpoint_a = _endpoint(
            channel=channel_a,
            credentials=credentials_a,
            public_keys=public_keys,
            publication_root=tmp_path / "published-a",
            durable_root=tmp_path / "durable-a",
        )
        endpoint_b = _endpoint(
            channel=channel_b,
            credentials=credentials_b,
            public_keys=public_keys,
            publication_root=tmp_path / "published-b",
            durable_root=tmp_path / "durable-b",
        )
        await _connect(endpoint_a, endpoint_b)
        channel_a.drop_before.add(2)
        with pytest.raises(DirectTransportUnavailable):
            await endpoint_a.send_file(
                target_node_id="node-b",
                session_id="session-f65",
                request_id="request-node-binding",
                object_id="object-node-binding",
                source_path=source,
                chunk_size=1_000,
            )
        await endpoint_a.close()

        shutil.copytree(
            tmp_path / "durable-a",
            tmp_path / "durable-c",
        )
        channel_c = _MemoryChannel("peer-c")
        endpoint_c = _endpoint(
            channel=channel_c,
            credentials=credentials_c,
            public_keys=public_keys,
            publication_root=tmp_path / "published-c",
            durable_root=tmp_path / "durable-c",
        )
        descriptor_c = await endpoint_c.start()
        endpoint_c.register_peer(endpoint_b.descriptor)
        endpoint_b.register_peer(descriptor_c)
        with pytest.raises(FederationValidationError) as raised:
            await endpoint_c.send_file(
                target_node_id="node-b",
                session_id="session-f65-c",
                request_id="request-node-binding",
                object_id="object-node-binding",
                source_path=source,
                chunk_size=1_000,
            )
        assert raised.value.code == "object-transfer-resume-source-node-mismatch"
        assert endpoint_b.store.published_path("object-node-binding") is None
        await endpoint_c.close()
        await endpoint_b.close()

    asyncio.run(scenario())

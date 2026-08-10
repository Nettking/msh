from __future__ import annotations

import hashlib

import pytest

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.object_transfer import (
    OBJECT_TRANSFER_CHUNK_SCHEMA,
    OBJECT_TRANSFER_MANIFEST_SCHEMA,
    OBJECT_TRANSFER_RECEIPT_SCHEMA,
    InMemoryObjectTransferChunkStore,
    ObjectTransferChunk,
    ObjectTransferManifest,
    ObjectTransferReceiver,
)


def _hash(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _manifest(
    content: bytes = b"abcdefghij",
    *,
    chunk_size: int = 4,
) -> ObjectTransferManifest:
    return ObjectTransferManifest.create(
        transfer_id="transfer-f641",
        object_id="object-f641",
        content=content,
        chunk_size=chunk_size,
    )


def _chunks(
    manifest: ObjectTransferManifest,
    content: bytes,
) -> list[ObjectTransferChunk]:
    return [
        ObjectTransferChunk.create(
            manifest=manifest,
            index=index,
            data=content[
                index * manifest.chunk_size : (index + 1) * manifest.chunk_size
            ],
        )
        for index in range(manifest.chunk_count)
    ]


def test_f641_manifest_roundtrip_derives_bounded_chunk_layout() -> None:
    manifest = _manifest()

    assert manifest.chunk_count == 3
    assert manifest.expected_chunk_length(0) == 4
    assert manifest.expected_chunk_length(1) == 4
    assert manifest.expected_chunk_length(2) == 2
    assert manifest.to_dict()["schema"] == OBJECT_TRANSFER_MANIFEST_SCHEMA
    assert ObjectTransferManifest.from_dict(manifest.to_dict()) == manifest


def test_f641_manifest_rejects_inconsistent_chunk_count() -> None:
    with pytest.raises(FederationValidationError) as caught:
        ObjectTransferManifest(
            transfer_id="transfer-f641",
            object_id="object-f641",
            total_size=10,
            object_hash=_hash(b"abcdefghij"),
            chunk_size=4,
            chunk_count=2,
        )

    assert caught.value.code == "object-transfer-chunk-count-mismatch"


def test_f641_manifest_and_chunk_reject_unknown_protocol_schema() -> None:
    manifest_value = _manifest().to_dict()
    manifest_value["schema"] = "fcp.object_transfer.manifest.v2"
    with pytest.raises(ProtocolCompatibilityError):
        ObjectTransferManifest.from_dict(manifest_value)

    chunk_value = _chunks(_manifest(), b"abcdefghij")[0].to_dict()
    chunk_value["schema"] = "fcp.object_transfer.chunk.v2"
    with pytest.raises(ProtocolCompatibilityError):
        ObjectTransferChunk.from_dict(chunk_value)


def test_f641_chunk_roundtrip_uses_canonical_data_and_hash() -> None:
    manifest = _manifest()
    chunk = _chunks(manifest, b"abcdefghij")[0]
    value = chunk.to_dict()

    assert value["schema"] == OBJECT_TRANSFER_CHUNK_SCHEMA
    assert value["chunk_hash"] == _hash(b"abcd")
    assert ObjectTransferChunk.from_dict(value) == chunk


def test_f641_tampered_chunk_data_is_rejected_before_acceptance() -> None:
    manifest = _manifest()
    value = _chunks(manifest, b"abcdefghij")[0].to_dict()
    value["data"] = "ZWZnaA"

    with pytest.raises(FederationValidationError) as caught:
        ObjectTransferChunk.from_dict(value)

    assert caught.value.code == "object-transfer-chunk-hash-mismatch"


def test_f641_receiver_tracks_missing_chunks_and_verifies_complete_object() -> None:
    content = b"abcdefghij"
    manifest = _manifest(content)
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    chunks = _chunks(manifest, content)

    assert receiver.accept(chunks[1]) is True
    assert receiver.status().missing_chunks == (0, 2)
    assert receiver.status().object_verified is False
    assert receiver.accept(chunks[0]) is True
    assert receiver.accept(chunks[2]) is True

    receipt = receiver.verify_complete()

    assert receipt.to_dict() == {
        "schema": OBJECT_TRANSFER_RECEIPT_SCHEMA,
        "protocol_version": "1.0",
        "transfer_id": manifest.transfer_id,
        "object_id": manifest.object_id,
        "total_size": len(content),
        "object_hash": _hash(content),
        "chunk_count": 3,
        "verified": True,
    }
    assert receiver.status().to_dict()["publishable"] is True
    assert receiver.verify_complete() is receipt


def test_f641_identical_duplicate_is_idempotent() -> None:
    manifest = _manifest()
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    chunk = _chunks(manifest, b"abcdefghij")[0]

    assert receiver.accept(chunk) is True
    assert receiver.accept(chunk) is False
    assert receiver.status().accepted_chunks == 1
    assert receiver.status().accepted_bytes == 4


def test_f641_conflicting_duplicate_is_rejected() -> None:
    manifest = _manifest()
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    first = ObjectTransferChunk.create(manifest=manifest, index=0, data=b"abcd")
    conflicting = ObjectTransferChunk.create(manifest=manifest, index=0, data=b"wxyz")

    receiver.accept(first)
    with pytest.raises(FederationValidationError) as caught:
        receiver.accept(conflicting)

    assert caught.value.code == "object-transfer-conflicting-duplicate"


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"transfer_id": "another-transfer"}, "object-transfer-id-mismatch"),
        ({"object_id": "another-object"}, "object-transfer-object-mismatch"),
        ({"offset": 1}, "object-transfer-chunk-offset-mismatch"),
    ],
)
def test_f641_receiver_rejects_wrong_manifest_binding(
    change: dict[str, object],
    code: str,
) -> None:
    manifest = _manifest()
    value = _chunks(manifest, b"abcdefghij")[0].to_dict()
    value.update(change)
    chunk = ObjectTransferChunk.from_dict(value)
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )

    with pytest.raises(FederationValidationError) as caught:
        receiver.accept(chunk)

    assert caught.value.code == code


def test_f641_receiver_rejects_wrong_chunk_size_and_out_of_range_index() -> None:
    manifest = _manifest()
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    short = ObjectTransferChunk.create(manifest=manifest, index=0, data=b"abc")
    outside = ObjectTransferChunk.create(manifest=manifest, index=3, data=b"x")

    with pytest.raises(FederationValidationError) as short_error:
        receiver.accept(short)
    assert short_error.value.code == "object-transfer-chunk-size-mismatch"

    with pytest.raises(FederationValidationError) as range_error:
        receiver.accept(outside)
    assert range_error.value.code == "object-transfer-chunk-out-of-range"


def test_f641_incomplete_transfer_cannot_be_verified_or_published() -> None:
    manifest = _manifest()
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    receiver.accept(_chunks(manifest, b"abcdefghij")[0])

    with pytest.raises(FederationOperationError) as caught:
        receiver.verify_complete()

    assert caught.value.code == "object-transfer-incomplete"
    assert receiver.status().to_dict()["publishable"] is False


def test_f641_final_object_hash_mismatch_is_rejected() -> None:
    content = b"abcdefghij"
    manifest = ObjectTransferManifest(
        transfer_id="transfer-f641",
        object_id="object-f641",
        total_size=len(content),
        object_hash="sha256:" + ("0" * 64),
        chunk_size=4,
        chunk_count=3,
    )
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    for chunk in _chunks(manifest, content):
        receiver.accept(chunk)

    with pytest.raises(FederationValidationError) as caught:
        receiver.verify_complete()

    assert caught.value.code == "object-transfer-final-hash-mismatch"
    assert receiver.status().object_verified is False


def test_f641_store_tampering_after_acceptance_fails_closed() -> None:
    content = b"abcdefghij"
    manifest = _manifest(content)
    store = InMemoryObjectTransferChunkStore()
    receiver = ObjectTransferReceiver(manifest, store)
    for chunk in _chunks(manifest, content):
        receiver.accept(chunk)

    store.put(manifest.transfer_id, 1, b"zzzz")
    with pytest.raises(FederationValidationError) as caught:
        receiver.verify_complete()

    assert caught.value.code == "object-transfer-stored-chunk-mismatch"


def test_f641_in_memory_store_capacity_is_bounded() -> None:
    content = b"abcdefgh"
    manifest = _manifest(content, chunk_size=4)
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(max_bytes=4),
    )
    chunks = _chunks(manifest, content)

    receiver.accept(chunks[0])
    with pytest.raises(FederationOperationError) as caught:
        receiver.accept(chunks[1])

    assert caught.value.code == "object-transfer-store-capacity-exceeded"
    assert receiver.status().accepted_chunks == 1


def test_f641_abort_clears_process_local_state() -> None:
    manifest = _manifest()
    store = InMemoryObjectTransferChunkStore()
    receiver = ObjectTransferReceiver(manifest, store)
    receiver.accept(_chunks(manifest, b"abcdefghij")[0])

    receiver.abort()

    assert store.stored_bytes == 0
    assert receiver.status().accepted_chunks == 0
    assert receiver.status().missing_chunks == (0, 1, 2)


def test_f641_zero_byte_object_verifies_without_chunks() -> None:
    manifest = _manifest(b"")
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )

    assert manifest.chunk_count == 0
    receipt = receiver.verify_complete()
    assert receipt.total_size == 0
    assert receipt.object_hash == _hash(b"")
    assert receiver.status().to_dict()["publishable"] is True


def test_f641_verified_transfer_is_immutable() -> None:
    content = b"abcd"
    manifest = _manifest(content)
    receiver = ObjectTransferReceiver(
        manifest,
        InMemoryObjectTransferChunkStore(),
    )
    chunk = _chunks(manifest, content)[0]
    receiver.accept(chunk)
    receiver.verify_complete()

    with pytest.raises(FederationOperationError) as caught:
        receiver.accept(chunk)

    assert caught.value.code == "object-transfer-already-verified"

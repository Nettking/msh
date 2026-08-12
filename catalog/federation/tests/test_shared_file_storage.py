from __future__ import annotations

import asyncio
import gzip
import hashlib
from datetime import datetime, timezone

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.shared_file_storage import (
    FEDERATED_JSONL_CONTENT_SCHEMA,
    FEDERATED_JSONL_DATASET_SCHEMA_NAME,
    FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
    FEDERATED_JSONL_ENCODING,
    FederationLogicalStorageAuthority,
    encode_federated_jsonl_chunk,
    federated_jsonl_batch_id,
    federated_jsonl_dataset_id,
    federated_jsonl_idempotency_key,
    normalize_jsonl_relative_path,
    validate_federated_jsonl_ingest,
)

SESSION = "session-jsonl-tests"
ACTOR = "node-jsonl-tests"
GROUP = "storage-jsonl-tests"


def _hash(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _payload(relative_path: str = "uploads/batch-a/input.jsonl") -> dict[str, object]:
    raw = b'{"timestamp":"2026-08-12T00:00:00Z","machine_id":"M1"}\n'
    encoded = gzip.compress(raw, mtime=0)
    chunk_hash = _hash(encoded)
    file_hash = _hash(raw)
    dataset_id = federated_jsonl_dataset_id(ACTOR, relative_path)
    batch_id = federated_jsonl_batch_id(file_hash, 0, chunk_hash)
    return {
        "group_id": GROUP,
        "dataset_id": dataset_id,
        "batch_id": batch_id,
        "idempotency_key": federated_jsonl_idempotency_key(
            SESSION, dataset_id, batch_id
        ),
        "dataset_schema_name": FEDERATED_JSONL_DATASET_SCHEMA_NAME,
        "dataset_schema_version": FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
        "created_at": datetime(2026, 8, 12, tzinfo=timezone.utc).isoformat(),
        "content": {
            "schema": FEDERATED_JSONL_CONTENT_SCHEMA,
            "producer_node_id": ACTOR,
            "relative_path": relative_path,
            "encoding": FEDERATED_JSONL_ENCODING,
            "file_size": len(raw),
            "file_sha256": file_hash,
            "encoded_size": len(encoded),
            "encoded_sha256": _hash(encoded),
            "source_mtime_ns": 123456789,
            "chunk_index": 0,
            "chunk_count": 1,
            "chunk_offset": 0,
            "chunk_sha256": chunk_hash,
            "data": encode_federated_jsonl_chunk(encoded),
        },
    }


class _LogicalStorage:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        return PhaseDIngestOutcome(committed=True)


def test_generic_jsonl_uses_membership_authorizer_not_recorder_authorizer():
    logical = _LogicalStorage()
    membership_calls: list[tuple[str, str, str, str | None]] = []

    def recorder_authorizer(_actor: str, _session: str) -> None:
        raise AssertionError("generic JSONL must not require recorder contribution")

    def membership_authorizer(
        actor: str,
        session: str,
        group: str,
        dataset: str | None,
    ) -> None:
        membership_calls.append((actor, session, group, dataset))

    authority = FederationLogicalStorageAuthority(
        client=object(),
        logical_client=logical,
        session_id=SESSION,
        authorize_ingest=recorder_authorizer,
        authorize_read=membership_authorizer,
    )
    payload = _payload()

    response = asyncio.run(
        authority._ingest_response(
            ACTOR,
            SESSION,
            "correlation-1",
            payload,
        )
    )

    assert response["status"] == "accepted"
    assert response["operation"] == "batch.ingest"
    assert membership_calls == [
        (ACTOR, SESSION, GROUP, str(payload["dataset_id"]))
    ]
    assert len(logical.calls) == 1
    assert logical.calls[0]["dataset_schema_name"] == FEDERATED_JSONL_DATASET_SCHEMA_NAME


def test_generic_jsonl_fails_closed_without_membership_authorizer():
    logical = _LogicalStorage()
    authority = FederationLogicalStorageAuthority(
        client=object(),
        logical_client=logical,
        session_id=SESSION,
    )

    with pytest.raises(FederationValidationError) as invalid:
        asyncio.run(
            authority._ingest_response(
                ACTOR,
                SESSION,
                "correlation-no-authorizer",
                _payload(),
            )
        )

    assert invalid.value.code == "federated-jsonl-membership-authorizer-required"
    assert logical.calls == []


def test_ingest_is_bound_to_authenticated_actor():
    payload = _payload()
    with pytest.raises(FederationValidationError) as invalid:
        validate_federated_jsonl_ingest(
            actor_node_id="some-other-node",
            session_id=SESSION,
            dataset_id=str(payload["dataset_id"]),
            batch_id=str(payload["batch_id"]),
            idempotency_key=str(payload["idempotency_key"]),
            schema_name=FEDERATED_JSONL_DATASET_SCHEMA_NAME,
            schema_version=FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
            content=payload["content"],
        )
    assert invalid.value.code == "federated-jsonl-producer-mismatch"


def test_traversal_and_non_jsonl_paths_are_rejected():
    for value in (
        "../secret.jsonl",
        "/tmp/file.jsonl",
        "data/file.txt",
        "a\\b.jsonl",
    ):
        with pytest.raises(FederationValidationError):
            normalize_jsonl_relative_path(value)


def test_tampered_chunk_is_rejected():
    payload = _payload()
    content = dict(payload["content"])
    content["chunk_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(FederationValidationError) as invalid:
        validate_federated_jsonl_ingest(
            actor_node_id=ACTOR,
            session_id=SESSION,
            dataset_id=str(payload["dataset_id"]),
            batch_id=str(payload["batch_id"]),
            idempotency_key=str(payload["idempotency_key"]),
            schema_name=FEDERATED_JSONL_DATASET_SCHEMA_NAME,
            schema_version=FEDERATED_JSONL_DATASET_SCHEMA_VERSION,
            content=content,
        )
    assert invalid.value.code == "federated-jsonl-chunk-hash-mismatch"

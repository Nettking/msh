"""Federation-wide immutable JSONL file chunks over logical storage.

This extends the existing recorder logical-storage ingress without weakening its
recorder-specific authority checks. Recorder batches still use their original
validator and recorder contribution authorizer. Generic JSONL files use a
separate actor-bound, content-addressed schema and require current Federation
membership through the authority's existing read/member authorizer.
"""

from __future__ import annotations

import base64
import hashlib
import math
from pathlib import PurePosixPath
from typing import Any

from .errors import FederationValidationError
from .recorder_storage_relay import (
    RECORDER_DATASET_SCHEMA_NAME,
    RECORDER_LOGICAL_STORAGE_KIND,
    RecorderLogicalStorageAuthority,
    _outcome_dict,
    _positive,
    _text,
    _utc,
)
from .storage_protocol import StorageOperation

FEDERATED_JSONL_DATASET_SCHEMA_NAME = "fcp.federation.jsonl-file-chunk"
FEDERATED_JSONL_DATASET_SCHEMA_VERSION = 1
FEDERATED_JSONL_CONTENT_SCHEMA = "fcp.federation.jsonl-file-chunk.v1"
FEDERATED_JSONL_ENCODING = "gzip"
FEDERATED_JSONL_CHUNK_BYTES = 24 * 1024
FEDERATED_JSONL_MAX_CHUNKS = 1_000_000
FEDERATED_JSONL_MAX_FILE_BYTES = (
    FEDERATED_JSONL_CHUNK_BYTES * FEDERATED_JSONL_MAX_CHUNKS * 16
)


def _sha256_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _sha256_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
    ):
        raise FederationValidationError(
            "invalid-federated-jsonl-hash",
            field,
            "must use sha256:<64 lowercase hex characters>",
        )
    digest = value[7:]
    if any(character not in "0123456789abcdef" for character in digest):
        raise FederationValidationError(
            "invalid-federated-jsonl-hash",
            field,
            "must use lowercase hexadecimal",
        )
    return value


def _non_negative(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-federated-jsonl-integer",
            field,
            "must be a non-negative integer",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "federated-jsonl-limit-exceeded",
            field,
            f"must not exceed {maximum}",
        )
    return value


def _positive_int(value: Any, field: str, *, maximum: int | None = None) -> int:
    value = _non_negative(value, field, maximum=maximum)
    if value < 1:
        raise FederationValidationError(
            "invalid-federated-jsonl-integer",
            field,
            "must be a positive integer",
        )
    return value


def normalize_jsonl_relative_path(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 2048
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise FederationValidationError(
            "invalid-federated-jsonl-path",
            "content.relative_path",
            "must be bounded printable POSIX relative text",
        )
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != value
        or path.suffix.casefold() != ".jsonl"
    ):
        raise FederationValidationError(
            "invalid-federated-jsonl-path",
            "content.relative_path",
            "must be a normalized relative .jsonl path without traversal",
        )
    return value


def federated_jsonl_dataset_id(actor_node_id: str, relative_path: str) -> str:
    actor = _text(actor_node_id, "actor_node_id")
    normalized = normalize_jsonl_relative_path(relative_path)
    digest = hashlib.sha256(f"{actor}\0{normalized}".encode("utf-8")).hexdigest()
    return f"federated-jsonl:{digest}"


def federated_jsonl_batch_id(
    file_sha256: str,
    chunk_index: int,
    chunk_sha256: str,
) -> str:
    file_hash = _sha256_text(file_sha256, "content.file_sha256")[7:]
    chunk_hash = _sha256_text(chunk_sha256, "content.chunk_sha256")[7:]
    index = _non_negative(
        chunk_index,
        "content.chunk_index",
        maximum=FEDERATED_JSONL_MAX_CHUNKS - 1,
    )
    return f"jsonl:{file_hash}:{index:08d}:{chunk_hash[:24]}"


def federated_jsonl_idempotency_key(
    session_id: str,
    dataset_id: str,
    batch_id: str,
) -> str:
    return (
        f"{_text(session_id, 'session_id')}:"
        f"{_text(dataset_id, 'dataset_id')}:"
        f"{_text(batch_id, 'batch_id')}"
    )


def encode_federated_jsonl_chunk(data: bytes) -> str:
    if not isinstance(data, bytes) or len(data) > FEDERATED_JSONL_CHUNK_BYTES:
        raise FederationValidationError(
            "invalid-federated-jsonl-chunk",
            "content.data",
            f"must contain at most {FEDERATED_JSONL_CHUNK_BYTES} bytes",
        )
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def decode_federated_jsonl_chunk(value: Any) -> bytes:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-federated-jsonl-encoding",
            "content.data",
            "must be canonical base64url text",
        )
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-federated-jsonl-encoding",
            "content.data",
            "must be canonical base64url text",
        ) from exc
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if canonical != value:
        raise FederationValidationError(
            "invalid-federated-jsonl-encoding",
            "content.data",
            "must use canonical base64url",
        )
    if len(decoded) > FEDERATED_JSONL_CHUNK_BYTES:
        raise FederationValidationError(
            "federated-jsonl-limit-exceeded",
            "content.data",
            f"decoded chunk must not exceed {FEDERATED_JSONL_CHUNK_BYTES} bytes",
        )
    return decoded


def validate_federated_jsonl_ingest(
    *,
    actor_node_id: str,
    session_id: str,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    schema_name: str,
    schema_version: int,
    content: Any,
) -> bytes:
    """Validate one immutable gzip chunk and bind it to the authenticated actor."""

    actor = _text(actor_node_id, "actor_node_id")
    session = _text(session_id, "session_id")
    if (
        schema_name != FEDERATED_JSONL_DATASET_SCHEMA_NAME
        or schema_version != FEDERATED_JSONL_DATASET_SCHEMA_VERSION
    ):
        raise FederationValidationError(
            "unsupported-federated-jsonl-schema",
            "dataset_schema_name",
            "federated JSONL ingress requires fcp.federation.jsonl-file-chunk v1",
        )
    if (
        not isinstance(content, dict)
        or content.get("schema") != FEDERATED_JSONL_CONTENT_SCHEMA
    ):
        raise FederationValidationError(
            "unsupported-federated-jsonl-content",
            "content.schema",
            f"must be {FEDERATED_JSONL_CONTENT_SCHEMA}",
        )
    if content.get("producer_node_id") != actor:
        raise FederationValidationError(
            "federated-jsonl-producer-mismatch",
            "content.producer_node_id",
            "producer identity must match the authenticated actor",
        )
    relative_path = normalize_jsonl_relative_path(content.get("relative_path"))
    if dataset_id != federated_jsonl_dataset_id(actor, relative_path):
        raise FederationValidationError(
            "federated-jsonl-dataset-mismatch",
            "dataset_id",
            "dataset identity must be derived from producer and relative path",
        )
    if content.get("encoding") != FEDERATED_JSONL_ENCODING:
        raise FederationValidationError(
            "unsupported-federated-jsonl-encoding",
            "content.encoding",
            f"must be {FEDERATED_JSONL_ENCODING}",
        )
    _non_negative(
        content.get("file_size"),
        "content.file_size",
        maximum=FEDERATED_JSONL_MAX_FILE_BYTES,
    )
    encoded_size = _positive_int(
        content.get("encoded_size"),
        "content.encoded_size",
        maximum=FEDERATED_JSONL_MAX_FILE_BYTES,
    )
    file_sha256 = _sha256_text(content.get("file_sha256"), "content.file_sha256")
    _sha256_text(content.get("encoded_sha256"), "content.encoded_sha256")
    _non_negative(content.get("source_mtime_ns"), "content.source_mtime_ns")
    chunk_count = _positive_int(
        content.get("chunk_count"),
        "content.chunk_count",
        maximum=FEDERATED_JSONL_MAX_CHUNKS,
    )
    expected_count = max(1, math.ceil(encoded_size / FEDERATED_JSONL_CHUNK_BYTES))
    if chunk_count != expected_count:
        raise FederationValidationError(
            "federated-jsonl-chunk-count-mismatch",
            "content.chunk_count",
            f"expected {expected_count} chunks for encoded_size",
        )
    chunk_index = _non_negative(
        content.get("chunk_index"),
        "content.chunk_index",
        maximum=chunk_count - 1,
    )
    expected_offset = chunk_index * FEDERATED_JSONL_CHUNK_BYTES
    chunk_offset = _non_negative(content.get("chunk_offset"), "content.chunk_offset")
    if chunk_offset != expected_offset:
        raise FederationValidationError(
            "federated-jsonl-chunk-offset-mismatch",
            "content.chunk_offset",
            f"expected offset {expected_offset}",
        )
    decoded = decode_federated_jsonl_chunk(content.get("data"))
    expected_length = min(FEDERATED_JSONL_CHUNK_BYTES, encoded_size - expected_offset)
    if len(decoded) != expected_length:
        raise FederationValidationError(
            "federated-jsonl-chunk-length-mismatch",
            "content.data",
            f"expected {expected_length} decoded bytes",
        )
    chunk_sha256 = _sha256_text(content.get("chunk_sha256"), "content.chunk_sha256")
    if _sha256_bytes(decoded) != chunk_sha256:
        raise FederationValidationError(
            "federated-jsonl-chunk-hash-mismatch",
            "content.chunk_sha256",
            "does not match decoded chunk content",
        )
    if batch_id != federated_jsonl_batch_id(file_sha256, chunk_index, chunk_sha256):
        raise FederationValidationError(
            "federated-jsonl-batch-mismatch",
            "batch_id",
            "batch identity does not match the file/chunk content",
        )
    expected_key = federated_jsonl_idempotency_key(session, dataset_id, batch_id)
    if idempotency_key != expected_key:
        raise FederationValidationError(
            "federated-jsonl-idempotency-mismatch",
            "idempotency_key",
            "idempotency key does not match the immutable file chunk",
        )
    return decoded


class FederationLogicalStorageAuthority(RecorderLogicalStorageAuthority):
    """Keep recorder ingress strict while admitting actor-bound JSONL chunks."""

    async def _ingest_response(
        self,
        actor_node_id: str,
        session_id: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        schema_name = _text(payload.get("dataset_schema_name"), "dataset_schema_name")
        if schema_name == RECORDER_DATASET_SCHEMA_NAME:
            return await super()._ingest_response(
                actor_node_id,
                session_id,
                correlation_id,
                payload,
            )

        group_id = _text(payload.get("group_id"), "group_id")
        dataset_id = _text(payload.get("dataset_id"), "dataset_id")
        batch_id = _text(payload.get("batch_id"), "batch_id")
        idempotency_key = _text(
            payload.get("idempotency_key"),
            "idempotency_key",
            maximum=2048,
        )
        schema_version = _positive(
            payload.get("dataset_schema_version"),
            "dataset_schema_version",
        )
        created_at = _utc(payload.get("created_at"), "created_at")
        if "content" not in payload:
            raise FederationValidationError(
                "invalid-federated-jsonl-field",
                "content",
                "is required",
            )

        # Generic source-data publication is allowed only to current Federation
        # members. An authority missing that explicit authorizer is unsafe and
        # therefore fails closed rather than treating relay presence as trust.
        if self.authorize_read is None:
            raise FederationValidationError(
                "federated-jsonl-membership-authorizer-required",
                "authorize_read",
                "generic Federation JSONL ingress requires membership authorization",
            )
        self.authorize_read(actor_node_id, session_id, group_id, dataset_id)
        validate_federated_jsonl_ingest(
            actor_node_id=actor_node_id,
            session_id=session_id,
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            schema_name=schema_name,
            schema_version=schema_version,
            content=payload["content"],
        )
        outcome = await self.logical_client.ingest_batch(
            group_id=group_id,
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            content=payload["content"],
            created_at=created_at,
            dataset_schema_name=schema_name,
            dataset_schema_version=schema_version,
        )
        return {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "response",
            "correlation_id": correlation_id,
            "status": "accepted",
            "operation": StorageOperation.BATCH_INGEST.value,
            "outcome": _outcome_dict(outcome),
        }


__all__ = [
    "FEDERATED_JSONL_CHUNK_BYTES",
    "FEDERATED_JSONL_CONTENT_SCHEMA",
    "FEDERATED_JSONL_DATASET_SCHEMA_NAME",
    "FEDERATED_JSONL_DATASET_SCHEMA_VERSION",
    "FEDERATED_JSONL_ENCODING",
    "FederationLogicalStorageAuthority",
    "decode_federated_jsonl_chunk",
    "encode_federated_jsonl_chunk",
    "federated_jsonl_batch_id",
    "federated_jsonl_dataset_id",
    "federated_jsonl_idempotency_key",
    "normalize_jsonl_relative_path",
    "validate_federated_jsonl_ingest",
]

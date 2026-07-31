"""Transport-independent verified chunk contract for Phase F6.4.1."""

from __future__ import annotations

import base64
import hashlib
import math
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

OBJECT_TRANSFER_MANIFEST_SCHEMA = "msh.object_transfer.manifest.v1"
OBJECT_TRANSFER_CHUNK_SCHEMA = "msh.object_transfer.chunk.v1"
OBJECT_TRANSFER_RECEIPT_SCHEMA = "msh.object_transfer.receipt.v1"
OBJECT_TRANSFER_PROTOCOL_VERSION = "1.0"
OBJECT_TRANSFER_PROTOCOL_MAJOR = 1

DEFAULT_TRANSFER_CHUNK_BYTES = 32_768
MAX_TRANSFER_CHUNK_BYTES = 32_768
MAX_TRANSFER_CHUNKS = 65_536
MAX_TRANSFER_OBJECT_BYTES = MAX_TRANSFER_CHUNK_BYTES * MAX_TRANSFER_CHUNKS
MAX_TRANSFER_ID_BYTES = 512
DEFAULT_IN_MEMORY_TRANSFER_BYTES = 16 * 1024 * 1024

JSON = dict[str, Any]


def _bounded_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > MAX_TRANSFER_ID_BYTES
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-object-transfer-field",
            field,
            "must be non-empty bounded text without control characters",
        )
    return value


def _protocol_version(value: Any) -> str:
    value = _bounded_text(value, "protocol_version")
    try:
        major = int(value.split(".", 1)[0])
    except ValueError as exc:
        raise ProtocolCompatibilityError(
            "invalid-object-transfer-protocol",
            "protocol_version",
            "must begin with a numeric major version",
        ) from exc
    if major != OBJECT_TRANSFER_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-object-transfer-protocol",
            "protocol_version",
            f"supported major is {OBJECT_TRANSFER_PROTOCOL_MAJOR}",
        )
    return value


def _positive_int(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise FederationValidationError(
            "invalid-object-transfer-integer",
            field,
            "must be a positive integer",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "object-transfer-limit-exceeded",
            field,
            f"must not exceed {maximum}",
        )
    return value


def _nonnegative_int(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-object-transfer-integer",
            field,
            "must be a non-negative integer",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "object-transfer-limit-exceeded",
            field,
            f"must not exceed {maximum}",
        )
    return value


def _sha256(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _sha256_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
    ):
        raise FederationValidationError(
            "invalid-object-transfer-hash",
            field,
            "must be sha256 followed by 64 lowercase hexadecimal characters",
        )
    digest = value.removeprefix("sha256:")
    if any(character not in "0123456789abcdef" for character in digest):
        raise FederationValidationError(
            "invalid-object-transfer-hash",
            field,
            "must use lowercase hexadecimal characters",
        )
    return value


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: Any, *, field: str, maximum_length: int) -> bytes:
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "invalid-object-transfer-encoding",
            field,
            "must be canonical base64url text",
        )
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise FederationValidationError(
            "invalid-object-transfer-encoding",
            field,
            "must be canonical base64url text",
        ) from exc
    if _b64encode(decoded) != value:
        raise FederationValidationError(
            "invalid-object-transfer-encoding",
            field,
            "must use canonical base64url",
        )
    if len(decoded) > maximum_length:
        raise FederationValidationError(
            "object-transfer-limit-exceeded",
            field,
            f"decoded value must not exceed {maximum_length} bytes",
        )
    return decoded


@dataclass(frozen=True)
class ObjectTransferManifest:
    transfer_id: str
    object_id: str
    total_size: int
    object_hash: str
    chunk_size: int
    chunk_count: int
    protocol_version: str = OBJECT_TRANSFER_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "transfer_id",
            _bounded_text(self.transfer_id, "transfer_id"),
        )
        object.__setattr__(
            self,
            "object_id",
            _bounded_text(self.object_id, "object_id"),
        )
        object.__setattr__(
            self,
            "total_size",
            _nonnegative_int(
                self.total_size,
                "total_size",
                maximum=MAX_TRANSFER_OBJECT_BYTES,
            ),
        )
        object.__setattr__(
            self,
            "object_hash",
            _sha256_text(self.object_hash, "object_hash"),
        )
        object.__setattr__(
            self,
            "chunk_size",
            _positive_int(
                self.chunk_size,
                "chunk_size",
                maximum=MAX_TRANSFER_CHUNK_BYTES,
            ),
        )
        object.__setattr__(
            self,
            "chunk_count",
            _nonnegative_int(
                self.chunk_count,
                "chunk_count",
                maximum=MAX_TRANSFER_CHUNKS,
            ),
        )
        expected_count = (
            math.ceil(self.total_size / self.chunk_size) if self.total_size else 0
        )
        if self.chunk_count != expected_count:
            raise FederationValidationError(
                "object-transfer-chunk-count-mismatch",
                "chunk_count",
                f"expected {expected_count} chunks for total_size and chunk_size",
            )
        object.__setattr__(
            self,
            "protocol_version",
            _protocol_version(self.protocol_version),
        )

    @classmethod
    def create(
        cls,
        *,
        transfer_id: str,
        object_id: str,
        content: bytes,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
    ) -> ObjectTransferManifest:
        if not isinstance(content, bytes):
            raise FederationValidationError(
                "invalid-object-transfer-content",
                "content",
                "must be bytes",
            )
        validated_chunk_size = _positive_int(
            chunk_size,
            "chunk_size",
            maximum=MAX_TRANSFER_CHUNK_BYTES,
        )
        return cls(
            transfer_id=transfer_id,
            object_id=object_id,
            total_size=len(content),
            object_hash=_sha256(content),
            chunk_size=validated_chunk_size,
            chunk_count=(
                math.ceil(len(content) / validated_chunk_size) if content else 0
            ),
        )

    @classmethod
    def from_dict(cls, value: Any) -> ObjectTransferManifest:
        if (
            not isinstance(value, dict)
            or value.get("schema") != OBJECT_TRANSFER_MANIFEST_SCHEMA
        ):
            raise ProtocolCompatibilityError(
                "unsupported-object-transfer-manifest",
                "schema",
                "unexpected object-transfer manifest schema",
            )
        required = {
            "protocol_version",
            "transfer_id",
            "object_id",
            "total_size",
            "object_hash",
            "chunk_size",
            "chunk_count",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{field: value[field] for field in required})

    def expected_chunk_length(self, index: int) -> int:
        index = _nonnegative_int(index, "index")
        if index >= self.chunk_count:
            raise FederationValidationError(
                "object-transfer-chunk-out-of-range",
                "index",
                "chunk index is outside the manifest",
            )
        if index < self.chunk_count - 1:
            return self.chunk_size
        return self.total_size - (index * self.chunk_size)

    def to_dict(self) -> JSON:
        return {
            "schema": OBJECT_TRANSFER_MANIFEST_SCHEMA,
            "protocol_version": self.protocol_version,
            "transfer_id": self.transfer_id,
            "object_id": self.object_id,
            "total_size": self.total_size,
            "object_hash": self.object_hash,
            "chunk_size": self.chunk_size,
            "chunk_count": self.chunk_count,
        }


@dataclass(frozen=True)
class ObjectTransferChunk:
    transfer_id: str
    object_id: str
    index: int
    offset: int
    chunk_length: int
    chunk_hash: str
    data: bytes
    protocol_version: str = OBJECT_TRANSFER_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "transfer_id",
            _bounded_text(self.transfer_id, "transfer_id"),
        )
        object.__setattr__(
            self,
            "object_id",
            _bounded_text(self.object_id, "object_id"),
        )
        object.__setattr__(
            self,
            "index",
            _nonnegative_int(
                self.index,
                "index",
                maximum=MAX_TRANSFER_CHUNKS - 1,
            ),
        )
        object.__setattr__(
            self,
            "offset",
            _nonnegative_int(
                self.offset,
                "offset",
                maximum=MAX_TRANSFER_OBJECT_BYTES,
            ),
        )
        object.__setattr__(
            self,
            "chunk_length",
            _positive_int(
                self.chunk_length,
                "chunk_length",
                maximum=MAX_TRANSFER_CHUNK_BYTES,
            ),
        )
        object.__setattr__(
            self,
            "chunk_hash",
            _sha256_text(self.chunk_hash, "chunk_hash"),
        )
        if not isinstance(self.data, bytes) or not self.data:
            raise FederationValidationError(
                "invalid-object-transfer-chunk",
                "data",
                "must contain bounded non-empty bytes",
            )
        if len(self.data) > MAX_TRANSFER_CHUNK_BYTES:
            raise FederationValidationError(
                "object-transfer-limit-exceeded",
                "data",
                f"must not exceed {MAX_TRANSFER_CHUNK_BYTES} bytes",
            )
        if self.chunk_length != len(self.data):
            raise FederationValidationError(
                "object-transfer-chunk-length-mismatch",
                "chunk_length",
                "does not match decoded chunk data",
            )
        if self.chunk_hash != _sha256(self.data):
            raise FederationValidationError(
                "object-transfer-chunk-hash-mismatch",
                "chunk_hash",
                "does not match chunk data",
            )
        object.__setattr__(
            self,
            "protocol_version",
            _protocol_version(self.protocol_version),
        )

    @classmethod
    def create(
        cls,
        *,
        manifest: ObjectTransferManifest,
        index: int,
        data: bytes,
    ) -> ObjectTransferChunk:
        if not isinstance(data, bytes):
            raise FederationValidationError(
                "invalid-object-transfer-chunk",
                "data",
                "must be bytes",
            )
        return cls(
            transfer_id=manifest.transfer_id,
            object_id=manifest.object_id,
            index=index,
            offset=index * manifest.chunk_size,
            chunk_length=len(data),
            chunk_hash=_sha256(data),
            data=data,
            protocol_version=manifest.protocol_version,
        )

    @classmethod
    def from_dict(cls, value: Any) -> ObjectTransferChunk:
        if (
            not isinstance(value, dict)
            or value.get("schema") != OBJECT_TRANSFER_CHUNK_SCHEMA
        ):
            raise ProtocolCompatibilityError(
                "unsupported-object-transfer-chunk",
                "schema",
                "unexpected object-transfer chunk schema",
            )
        required = {
            "protocol_version",
            "transfer_id",
            "object_id",
            "index",
            "offset",
            "chunk_length",
            "chunk_hash",
            "data",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        decoded = _b64decode(
            value["data"],
            field="data",
            maximum_length=MAX_TRANSFER_CHUNK_BYTES,
        )
        return cls(
            protocol_version=value["protocol_version"],
            transfer_id=value["transfer_id"],
            object_id=value["object_id"],
            index=value["index"],
            offset=value["offset"],
            chunk_length=value["chunk_length"],
            chunk_hash=value["chunk_hash"],
            data=decoded,
        )

    def to_dict(self) -> JSON:
        return {
            "schema": OBJECT_TRANSFER_CHUNK_SCHEMA,
            "protocol_version": self.protocol_version,
            "transfer_id": self.transfer_id,
            "object_id": self.object_id,
            "index": self.index,
            "offset": self.offset,
            "chunk_length": self.chunk_length,
            "chunk_hash": self.chunk_hash,
            "data": _b64encode(self.data),
        }


class ObjectTransferChunkStore(Protocol):
    def put(self, transfer_id: str, index: int, data: bytes) -> None: ...

    def get(self, transfer_id: str, index: int) -> bytes: ...

    def delete(self, transfer_id: str) -> None: ...


class InMemoryObjectTransferChunkStore:
    """Bounded process-local chunk store for contract tests and F6.4.1."""

    def __init__(self, max_bytes: int = DEFAULT_IN_MEMORY_TRANSFER_BYTES) -> None:
        self.max_bytes = _positive_int(max_bytes, "max_bytes")
        self._chunks: dict[tuple[str, int], bytes] = {}
        self._stored_bytes = 0

    @property
    def stored_bytes(self) -> int:
        return self._stored_bytes

    def put(self, transfer_id: str, index: int, data: bytes) -> None:
        key = (
            _bounded_text(transfer_id, "transfer_id"),
            _nonnegative_int(index, "index"),
        )
        if not isinstance(data, bytes):
            raise FederationValidationError(
                "invalid-object-transfer-chunk",
                "data",
                "must be bytes",
            )
        previous = self._chunks.get(key)
        projected = (
            self._stored_bytes
            - (len(previous) if previous is not None else 0)
            + len(data)
        )
        if projected > self.max_bytes:
            raise FederationOperationError(
                "object-transfer-store-capacity-exceeded",
                "process-local chunk store capacity would be exceeded",
                "data",
            )
        self._chunks[key] = bytes(data)
        self._stored_bytes = projected

    def get(self, transfer_id: str, index: int) -> bytes:
        key = (transfer_id, index)
        try:
            return self._chunks[key]
        except KeyError as exc:
            raise FederationOperationError(
                "object-transfer-chunk-not-stored",
                "accepted chunk is missing from the chunk store",
                "index",
            ) from exc

    def delete(self, transfer_id: str) -> None:
        keys = [key for key in self._chunks if key[0] == transfer_id]
        for key in keys:
            self._stored_bytes -= len(self._chunks.pop(key))


@dataclass(frozen=True)
class VerifiedObjectTransfer:
    transfer_id: str
    object_id: str
    total_size: int
    object_hash: str
    chunk_count: int
    protocol_version: str = OBJECT_TRANSFER_PROTOCOL_VERSION

    def to_dict(self) -> JSON:
        return {
            "schema": OBJECT_TRANSFER_RECEIPT_SCHEMA,
            "protocol_version": self.protocol_version,
            "transfer_id": self.transfer_id,
            "object_id": self.object_id,
            "total_size": self.total_size,
            "object_hash": self.object_hash,
            "chunk_count": self.chunk_count,
            "verified": True,
        }


@dataclass(frozen=True)
class ObjectTransferStatus:
    transfer_id: str
    object_id: str
    accepted_chunks: int
    accepted_bytes: int
    missing_chunks: tuple[int, ...]
    chunks_complete: bool
    object_verified: bool

    def to_dict(self) -> JSON:
        return {
            "transfer_id": self.transfer_id,
            "object_id": self.object_id,
            "accepted_chunks": self.accepted_chunks,
            "accepted_bytes": self.accepted_bytes,
            "missing_chunks": list(self.missing_chunks),
            "chunks_complete": self.chunks_complete,
            "object_verified": self.object_verified,
            "publishable": self.object_verified,
        }


class ObjectTransferReceiver:
    """Verify one manifest-bound transfer without transport or persistence concerns."""

    def __init__(
        self,
        manifest: ObjectTransferManifest,
        store: ObjectTransferChunkStore,
    ) -> None:
        self.manifest = manifest
        self.store = store
        self._accepted: dict[int, tuple[str, int]] = {}
        self._verified_receipt: VerifiedObjectTransfer | None = None

    def status(self) -> ObjectTransferStatus:
        missing = tuple(
            index
            for index in range(self.manifest.chunk_count)
            if index not in self._accepted
        )
        return ObjectTransferStatus(
            transfer_id=self.manifest.transfer_id,
            object_id=self.manifest.object_id,
            accepted_chunks=len(self._accepted),
            accepted_bytes=sum(length for _, length in self._accepted.values()),
            missing_chunks=missing,
            chunks_complete=not missing,
            object_verified=self._verified_receipt is not None,
        )

    def accept(self, chunk: ObjectTransferChunk) -> bool:
        if self._verified_receipt is not None:
            raise FederationOperationError(
                "object-transfer-already-verified",
                "verified transfer is immutable",
            )
        self._validate_manifest_binding(chunk)
        accepted = self._accepted.get(chunk.index)
        if accepted is not None:
            existing_hash, existing_length = accepted
            existing_data = self.store.get(self.manifest.transfer_id, chunk.index)
            if (
                existing_hash == chunk.chunk_hash
                and existing_length == chunk.chunk_length
                and existing_data == chunk.data
            ):
                return False
            raise FederationValidationError(
                "object-transfer-conflicting-duplicate",
                "index",
                "chunk index was already accepted with different content",
            )
        self.store.put(self.manifest.transfer_id, chunk.index, chunk.data)
        self._accepted[chunk.index] = (chunk.chunk_hash, chunk.chunk_length)
        return True

    def verify_complete(self) -> VerifiedObjectTransfer:
        if self._verified_receipt is not None:
            return self._verified_receipt
        missing = self.status().missing_chunks
        if missing:
            raise FederationOperationError(
                "object-transfer-incomplete",
                "all manifest chunks must be accepted before final verification",
                "missing_chunks",
            )
        digest = hashlib.sha256()
        total_size = 0
        for index, data in self._iter_verified_chunks():
            expected_hash, expected_length = self._accepted[index]
            if len(data) != expected_length or _sha256(data) != expected_hash:
                raise FederationValidationError(
                    "object-transfer-stored-chunk-mismatch",
                    "index",
                    f"stored chunk {index} changed after acceptance",
                )
            digest.update(data)
            total_size += len(data)
        object_hash = f"sha256:{digest.hexdigest()}"
        if total_size != self.manifest.total_size:
            raise FederationValidationError(
                "object-transfer-final-size-mismatch",
                "total_size",
                "verified chunks do not match manifest total size",
            )
        if object_hash != self.manifest.object_hash:
            raise FederationValidationError(
                "object-transfer-final-hash-mismatch",
                "object_hash",
                "verified chunks do not match manifest object hash",
            )
        self._verified_receipt = VerifiedObjectTransfer(
            transfer_id=self.manifest.transfer_id,
            object_id=self.manifest.object_id,
            total_size=total_size,
            object_hash=object_hash,
            chunk_count=self.manifest.chunk_count,
            protocol_version=self.manifest.protocol_version,
        )
        return self._verified_receipt

    def abort(self) -> None:
        self.store.delete(self.manifest.transfer_id)
        self._accepted.clear()
        self._verified_receipt = None

    def _validate_manifest_binding(self, chunk: ObjectTransferChunk) -> None:
        manifest_major = self.manifest.protocol_version.split(".", 1)[0]
        chunk_major = chunk.protocol_version.split(".", 1)[0]
        if chunk_major != manifest_major:
            raise ProtocolCompatibilityError(
                "object-transfer-protocol-mismatch",
                "protocol_version",
                "chunk and manifest protocol majors differ",
            )
        if chunk.transfer_id != self.manifest.transfer_id:
            raise FederationValidationError(
                "object-transfer-id-mismatch",
                "transfer_id",
                "chunk belongs to another transfer",
            )
        if chunk.object_id != self.manifest.object_id:
            raise FederationValidationError(
                "object-transfer-object-mismatch",
                "object_id",
                "chunk belongs to another object",
            )
        expected_length = self.manifest.expected_chunk_length(chunk.index)
        expected_offset = chunk.index * self.manifest.chunk_size
        if chunk.offset != expected_offset:
            raise FederationValidationError(
                "object-transfer-chunk-offset-mismatch",
                "offset",
                f"expected offset {expected_offset}",
            )
        if chunk.chunk_length != expected_length:
            raise FederationValidationError(
                "object-transfer-chunk-size-mismatch",
                "chunk_length",
                f"expected {expected_length} bytes",
            )

    def _iter_verified_chunks(self) -> Iterator[tuple[int, bytes]]:
        for index in range(self.manifest.chunk_count):
            yield index, self.store.get(self.manifest.transfer_id, index)

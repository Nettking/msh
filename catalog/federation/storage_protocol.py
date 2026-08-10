"""Pure contracts for the versioned ``fcp-storage-v1`` application protocol.

This module intentionally contains no provider, network, coordinator, or database
implementation.  It freezes the request/response envelope, immutable batch ingest
semantics, stable errors, and write-authority context used by later Phase D steps.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Self

from .errors import FederationValidationError, ProtocolCompatibilityError

JSON = dict[str, Any]
STORAGE_PROTOCOL = "fcp-storage-v1"
STORAGE_PROTOCOL_MAJOR = 1
STORAGE_PROTOCOL_VERSION = "1.0"
DEFAULT_DATASET_SCHEMA_NAME = "fcp.storage.dataset.opaque"
DEFAULT_DATASET_SCHEMA_VERSION = 1


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-text", field, "must be a non-empty opaque string")
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer", field, "must be a non-negative integer"
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError("invalid-timestamp", field, "must be RFC 3339") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", field, "must be timezone-aware UTC")
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError("invalid-timestamp", field, "must use UTC")
    return value.astimezone(timezone.utc)


def _json_value(value: Any, field: str) -> Any:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain only JSON-compatible values"
        ) from exc
    return value


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _protocol_major(version: Any) -> int:
    version = _required_text(version, "protocol_version")
    parts = version.split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        raise ProtocolCompatibilityError(
            "invalid-protocol-version", "protocol_version", "must use <major>.<minor>"
        )
    return int(parts[0])


def validate_protocol_version(version: Any) -> str:
    """Accept version 1 minor revisions and reject unknown major versions."""

    version = _required_text(version, "protocol_version")
    major = _protocol_major(version)
    if major != STORAGE_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "protocol_version",
            f"expected major {STORAGE_PROTOCOL_MAJOR}, received {major}",
        )
    return version


class StorageOperation(str, Enum):
    DESCRIBE = "storage.describe"
    HEALTH = "storage.health"
    BATCH_INGEST = "batch.ingest"
    BATCH_EXISTS = "batch.exists"
    BATCH_READ = "batch.read"
    BATCH_LIST = "batch.list"
    REPLICA_ACKNOWLEDGE = "replica.acknowledge"


class StorageErrorCode(str, Enum):
    INVALID_REQUEST = "invalid-request"
    UNSUPPORTED_PROTOCOL = "unsupported-protocol"
    UNSUPPORTED_OPERATION = "unsupported-operation"
    UNAUTHORIZED = "unauthorized"
    NOT_PRIMARY = "not-primary"
    STALE_TERM = "stale-term"
    STALE_FENCING_TOKEN = "stale-fencing-token"
    LEASE_EXPIRED = "lease-expired"
    UNKNOWN_GRANT = "unknown-grant"
    BATCH_NOT_FOUND = "batch-not-found"
    IDEMPOTENCY_CONFLICT = "idempotency-conflict"
    CONTENT_HASH_MISMATCH = "content-hash-mismatch"
    INTERNAL_ERROR = "internal-error"


class BatchIngestState(str, Enum):
    STORED = "stored"
    ALREADY_STORED = "already-stored"


@dataclass(frozen=True)
class WriteAuthority:
    """Authority presented with a mutating storage request.

    Lease validity is enforced later by the provider/coordinator integration.  D0
    validates only that the complete authority context is present and well formed.
    """

    session_id: str
    group_id: str
    actor_node_id: str
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "actor_node_id", "grant_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(self, "term", _uint(self.term, "term"))
        object.__setattr__(self, "fencing_token", _uint(self.fencing_token, "fencing_token"))
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at, "lease_expires_at"))

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "authority", "must be an object")
        required = {
            "session_id",
            "group_id",
            "actor_node_id",
            "grant_id",
            "term",
            "fencing_token",
            "lease_expires_at",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", f"authority.{missing[0]}", "is required")
        return cls(**{name: value[name] for name in required})

    def to_dict(self) -> JSON:
        return {
            "session_id": self.session_id,
            "group_id": self.group_id,
            "actor_node_id": self.actor_node_id,
            "grant_id": self.grant_id,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "lease_expires_at": _timestamp(self.lease_expires_at),
        }


@dataclass(frozen=True)
class StorageRequestEnvelope:
    SCHEMA: ClassVar[str] = "fcp.storage_request.v1"

    request_id: str
    protocol: str
    protocol_version: str
    operation: StorageOperation
    session_id: str
    actor_node_id: str
    authorization_context: dict[str, Any]
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("request_id", "session_id", "actor_node_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        if self.protocol != STORAGE_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-protocol", "protocol", f"expected {STORAGE_PROTOCOL}"
            )
        object.__setattr__(self, "protocol_version", validate_protocol_version(self.protocol_version))
        try:
            object.__setattr__(self, "operation", StorageOperation(self.operation))
        except ValueError as exc:
            raise FederationValidationError(
                "unsupported-operation", "operation", "unknown storage operation"
            ) from exc
        if not isinstance(self.authorization_context, dict):
            raise FederationValidationError(
                "invalid-object", "authorization_context", "must be an object"
            )
        if not isinstance(self.payload, dict):
            raise FederationValidationError("invalid-object", "payload", "must be an object")
        _json_value(self.authorization_context, "authorization_context")
        _json_value(self.payload, "payload")

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", cls.__name__, "must be an object")
        if value.get("schema") != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {cls.SCHEMA}"
            )
        required = {
            "request_id",
            "protocol",
            "protocol_version",
            "operation",
            "session_id",
            "actor_node_id",
            "authorization_context",
            "payload",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        # Unknown additive fields are intentionally ignored within protocol major v1.
        return cls(**{name: value[name] for name in required})

    def to_dict(self) -> JSON:
        return {
            "schema": self.SCHEMA,
            "request_id": self.request_id,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "operation": self.operation.value,
            "session_id": self.session_id,
            "actor_node_id": self.actor_node_id,
            "authorization_context": self.authorization_context,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class StorageError:
    code: StorageErrorCode
    message: str
    field: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "code", StorageErrorCode(self.code))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "code", "unknown storage error") from exc
        object.__setattr__(self, "message", _required_text(self.message, "message"))
        if self.field is not None:
            object.__setattr__(self, "field", _required_text(self.field, "field"))
        if not isinstance(self.retryable, bool):
            raise FederationValidationError("invalid-boolean", "retryable", "must be boolean")

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "error", "must be an object")
        return cls(
            code=value.get("code"),
            message=value.get("message"),
            field=value.get("field"),
            retryable=value.get("retryable", False),
        )

    def to_dict(self) -> JSON:
        result: JSON = {
            "code": self.code.value,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.field is not None:
            result["field"] = self.field
        return result


@dataclass(frozen=True)
class StorageResponseEnvelope:
    SCHEMA: ClassVar[str] = "fcp.storage_response.v1"

    request_id: str
    protocol: str
    protocol_version: str
    ok: bool
    result: dict[str, Any] | None = None
    error: StorageError | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_id", _required_text(self.request_id, "request_id"))
        if self.protocol != STORAGE_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-protocol", "protocol", f"expected {STORAGE_PROTOCOL}"
            )
        object.__setattr__(self, "protocol_version", validate_protocol_version(self.protocol_version))
        if not isinstance(self.ok, bool):
            raise FederationValidationError("invalid-boolean", "ok", "must be boolean")
        if self.ok:
            if self.error is not None:
                raise FederationValidationError("invalid-response", "error", "must be absent when ok")
            if self.result is None or not isinstance(self.result, dict):
                raise FederationValidationError("invalid-response", "result", "must be an object when ok")
            _json_value(self.result, "result")
        else:
            if self.result is not None:
                raise FederationValidationError("invalid-response", "result", "must be absent on error")
            if not isinstance(self.error, StorageError):
                raise FederationValidationError("invalid-response", "error", "must be present on error")

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", cls.__name__, "must be an object")
        if value.get("schema") != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {cls.SCHEMA}"
            )
        required = {"request_id", "protocol", "protocol_version", "ok"}
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        error = value.get("error")
        return cls(
            request_id=value["request_id"],
            protocol=value["protocol"],
            protocol_version=value["protocol_version"],
            ok=value["ok"],
            result=value.get("result"),
            error=StorageError.from_dict(error) if error is not None else None,
        )

    def to_dict(self) -> JSON:
        value: JSON = {
            "schema": self.SCHEMA,
            "request_id": self.request_id,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "ok": self.ok,
        }
        if self.result is not None:
            value["result"] = self.result
        if self.error is not None:
            value["error"] = self.error.to_dict()
        return value


@dataclass(frozen=True)
class BatchIngestRequest:
    SCHEMA: ClassVar[str] = "fcp.batch_ingest.v1"

    authority: WriteAuthority
    dataset_id: str
    batch_id: str
    idempotency_key: str
    content_hash: str
    content: Any
    created_at: datetime
    dataset_schema_name: str = DEFAULT_DATASET_SCHEMA_NAME
    dataset_schema_version: int = DEFAULT_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.authority, WriteAuthority):
            raise FederationValidationError("invalid-object", "authority", "must be WriteAuthority")
        for name in ("dataset_id", "batch_id", "idempotency_key", "content_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "dataset_schema_name",
            _required_text(self.dataset_schema_name, "dataset_schema_name"),
        )
        schema_version = _uint(
            self.dataset_schema_version,
            "dataset_schema_version",
        )
        if schema_version == 0:
            raise FederationValidationError(
                "invalid-positive-integer",
                "dataset_schema_version",
                "must be greater than zero",
            )
        object.__setattr__(
            self,
            "dataset_schema_version",
            schema_version,
        )
        if not self.content_hash.startswith("sha256:") or len(self.content_hash) != 71:
            raise FederationValidationError(
                "invalid-content-hash", "content_hash", "must use sha256:<64 lowercase hex characters>"
            )
        digest = self.content_hash[7:]
        if any(char not in "0123456789abcdef" for char in digest):
            raise FederationValidationError(
                "invalid-content-hash", "content_hash", "must use lowercase hexadecimal"
            )
        _json_value(self.content, "content")
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))

    @staticmethod
    def calculate_content_hash(content: Any) -> str:
        _json_value(content, "content")
        canonical = json.dumps(
            content, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"

    def validate_content_hash(self) -> None:
        if self.calculate_content_hash(self.content) != self.content_hash:
            raise FederationValidationError(
                StorageErrorCode.CONTENT_HASH_MISMATCH.value,
                "content_hash",
                "does not match canonical content",
            )

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", cls.__name__, "must be an object")
        if value.get("schema") != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {cls.SCHEMA}"
            )
        required = {
            "authority",
            "dataset_id",
            "batch_id",
            "idempotency_key",
            "content_hash",
            "content",
            "created_at",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            authority=WriteAuthority.from_dict(value["authority"]),
            dataset_id=value["dataset_id"],
            batch_id=value["batch_id"],
            idempotency_key=value["idempotency_key"],
            content_hash=value["content_hash"],
            content=value["content"],
            created_at=value["created_at"],
            dataset_schema_name=value.get(
                "dataset_schema_name",
                DEFAULT_DATASET_SCHEMA_NAME,
            ),
            dataset_schema_version=value.get(
                "dataset_schema_version",
                DEFAULT_DATASET_SCHEMA_VERSION,
            ),
        )

    def to_dict(self) -> JSON:
        return {
            "schema": self.SCHEMA,
            "authority": self.authority.to_dict(),
            "dataset_id": self.dataset_id,
            "batch_id": self.batch_id,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "content": self.content,
            "created_at": _timestamp(self.created_at),
            "dataset_schema_name": self.dataset_schema_name,
            "dataset_schema_version": self.dataset_schema_version,
        }


@dataclass(frozen=True)
class BatchIngestResult:
    SCHEMA: ClassVar[str] = "fcp.batch_ingest_result.v1"

    batch_id: str
    idempotency_key: str
    content_hash: str
    state: BatchIngestState

    def __post_init__(self) -> None:
        for name in ("batch_id", "idempotency_key", "content_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        try:
            object.__setattr__(self, "state", BatchIngestState(self.state))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "state", "unknown ingest state") from exc

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", cls.__name__, "must be an object")
        if value.get("schema") != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {cls.SCHEMA}"
            )
        required = {"batch_id", "idempotency_key", "content_hash", "state"}
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in required})

    def to_dict(self) -> JSON:
        return {
            "schema": self.SCHEMA,
            "batch_id": self.batch_id,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "state": self.state.value,
        }


def classify_idempotent_ingest(
    *,
    existing_batch_id: str,
    existing_content_hash: str,
    requested_batch_id: str,
    requested_content_hash: str,
) -> BatchIngestState:
    """Classify a retry without touching storage.

    Reusing an idempotency key is safe only when both logical batch identity and
    canonical content identity match the previously committed record.
    """

    for name, value in (
        ("existing_batch_id", existing_batch_id),
        ("existing_content_hash", existing_content_hash),
        ("requested_batch_id", requested_batch_id),
        ("requested_content_hash", requested_content_hash),
    ):
        _required_text(value, name)
    if existing_batch_id != requested_batch_id or existing_content_hash != requested_content_hash:
        raise FederationValidationError(
            StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
            "idempotency_key",
            "was previously committed for a different batch or content hash",
        )
    return BatchIngestState.ALREADY_STORED

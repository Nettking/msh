"""Validated, JSON-compatible Phase 0 federation domain models."""

from __future__ import annotations

import hashlib
import json
from dataclasses import MISSING, asdict, dataclass, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Self

from .errors import FederationValidationError, ProtocolCompatibilityError
from .redaction import (
    is_nonpublic_location_key,
    is_nonpublic_location_text,
    is_secret_text,
    is_sensitive_field_name,
)

JSON = dict[str, Any]
MAX_PUBLIC_METADATA_TEXT_BYTES = 512


class SessionState(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSING = "closing"
    CLOSED = "closed"
    DEGRADED = "degraded"


class CapabilityStatus(str, Enum):
    REGISTERING = "registering"
    READY = "ready"
    UNAVAILABLE = "unavailable"
    DRAINING = "draining"
    DISABLED = "disabled"
    REVOKED = "revoked"


class StorageRole(str, Enum):
    UNASSIGNED = "unassigned"
    PRIMARY = "primary"
    REPLICA = "replica"
    PARTIAL_REPLICA = "partial-replica"
    CACHE = "cache"
    ARCHIVE = "archive"
    OBJECT_ONLY = "object-only"
    SPOOL = "spool"


class StorageGroupState(str, Enum):
    PENDING = "pending"
    HEALTHY = "healthy"
    UNDER_REPLICATED = "under-replicated"
    HANDOVER = "handover"
    STORAGE_DEGRADED = "storage-degraded"
    UNAVAILABLE = "unavailable"


class CommitState(str, Enum):
    PROVISIONAL = "provisional"
    PENDING_REPLICATION = "pending-replication"
    COMMITTED = "committed"
    REJECTED = "rejected"


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
        raise FederationValidationError("invalid-id", field, "must be a non-empty opaque string")
    return value


def _public_metadata_text(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if len(value.encode("utf-8")) > MAX_PUBLIC_METADATA_TEXT_BYTES:
        raise FederationValidationError(
            "metadata-too-large",
            field,
            f"must not exceed {MAX_PUBLIC_METADATA_TEXT_BYTES} UTF-8 bytes",
        )
    if is_secret_text(value):
        raise FederationValidationError(
            "secret-metadata",
            field,
            "must not contain credentials or secrets",
        )
    if is_nonpublic_location_text(value):
        raise FederationValidationError(
            "nonpublic-metadata",
            field,
            "must not expose backend paths or physical storage addresses",
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError("invalid-non-negative-integer", field, "must be a non-negative integer")
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


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, DomainModel):
        return value.to_dict()
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return value


@dataclass(frozen=True)
class DomainModel:
    SCHEMA: ClassVar[str]

    @classmethod
    def from_dict(cls, value: JSON) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", cls.__name__, "must be an object")
        schema = value.get("schema")
        if schema != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema", "schema", f"expected {cls.SCHEMA}, received {schema!r}"
            )
        model_fields = fields(cls)
        known = {field.name for field in model_fields}
        missing = [
            field.name
            for field in model_fields
            if field.default is MISSING
            and field.default_factory is MISSING
            and field.name not in value
        ]
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in known if name in value})

    def to_dict(self) -> JSON:
        return {"schema": self.SCHEMA, **_json(asdict(self))}


@dataclass(frozen=True)
class NodeIdentity(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.node.v1"
    node_id: str
    display_name: str
    public_key: str
    created_at: datetime
    identity_version: int

    def __post_init__(self) -> None:
        for name in ("node_id", "public_key"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "display_name",
            _public_metadata_text(self.display_name, "display_name"),
        )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "identity_version", _uint(self.identity_version, "identity_version"))


@dataclass(frozen=True)
class Session(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.session.v1"
    session_id: str
    display_name: str
    state: SessionState
    revision: int
    created_at: datetime
    created_by_node_id: str
    coordinator_id: str

    def __post_init__(self) -> None:
        for name in ("created_by_node_id", "coordinator_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "session_id",
            _public_metadata_text(self.session_id, "session_id"),
        )
        object.__setattr__(
            self,
            "display_name",
            _public_metadata_text(self.display_name, "display_name"),
        )
        try:
            object.__setattr__(self, "state", SessionState(self.state))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "state", "unknown session state") from exc
        object.__setattr__(self, "revision", _uint(self.revision, "revision"))
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))


@dataclass(frozen=True)
class CapabilityAnnouncement(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.capability.v1"
    capability_id: str
    node_id: str
    session_id: str
    type: str
    protocol: str
    protocol_version: str
    status: CapabilityStatus
    properties: dict[str, Any]
    announced_at: datetime

    def __post_init__(self) -> None:
        for name in ("node_id", "session_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name in ("capability_id", "type", "protocol", "protocol_version"):
            object.__setattr__(
                self,
                name,
                _public_metadata_text(getattr(self, name), name),
            )
        try:
            object.__setattr__(self, "status", CapabilityStatus(self.status))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "status", "unknown capability status") from exc
        if not isinstance(self.properties, dict):
            raise FederationValidationError("invalid-object", "properties", "must be an object")
        try:
            json.dumps(self.properties, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-json", "properties", "must contain only JSON-compatible values"
            ) from exc
        def contains_secret(value: Any) -> bool:
            if isinstance(value, dict):
                return any(is_sensitive_field_name(str(key)) or contains_secret(item)
                           for key, item in value.items())
            if isinstance(value, (list, tuple)):
                return any(contains_secret(item) for item in value)
            return is_secret_text(value)
        if contains_secret(self.properties):
            raise FederationValidationError(
                "secret-property", "properties", "must not contain credentials or secrets"
            )
        def contains_location(value: Any) -> bool:
            if isinstance(value, dict):
                return any(
                    is_nonpublic_location_key(str(key))
                    or contains_location(item)
                    for key, item in value.items()
                )
            if isinstance(value, (list, tuple)):
                return any(contains_location(item) for item in value)
            return is_nonpublic_location_text(value)
        if contains_location(self.properties):
            raise FederationValidationError(
                "nonpublic-property",
                "properties",
                "must not expose backend paths or physical storage addresses",
            )
        object.__setattr__(self, "announced_at", _utc(self.announced_at, "announced_at"))


@dataclass(frozen=True)
class SessionEvent(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.session_event.v1"
    session_id: str
    revision: int
    event_id: str
    event_type: str
    occurred_at: datetime
    actor_node_id: str
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        for name in ("session_id", "event_id", "event_type", "actor_node_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(self, "revision", _uint(self.revision, "revision"))
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        if not isinstance(self.payload, dict):
            raise FederationValidationError("invalid-object", "payload", "must be an object")


@dataclass(frozen=True)
class DatasetCoverage:
    dataset_id: str
    schema_name: str
    schema_version: int
    required: bool
    committed_revision: int
    last_contiguous_sequence: int
    missing_ranges: tuple[tuple[int, int], ...]
    batch_count: int
    object_count: int
    manifest_hash: str

    def __post_init__(self) -> None:
        for name in ("dataset_id", "schema_name", "manifest_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name in ("schema_version", "committed_revision", "last_contiguous_sequence", "batch_count", "object_count"):
            object.__setattr__(self, name, _uint(getattr(self, name), name))
        if not isinstance(self.required, bool):
            raise FederationValidationError("invalid-boolean", "required", "must be boolean")
        normalized = tuple(tuple(item) for item in self.missing_ranges)
        previous_end = -1
        for index, item in enumerate(normalized):
            if len(item) != 2:
                raise FederationValidationError("invalid-range", "missing_ranges", "ranges require start and end")
            start, end = (_uint(item[0], f"missing_ranges[{index}].start"), _uint(item[1], f"missing_ranges[{index}].end"))
            if start > end or start <= previous_end:
                raise FederationValidationError("invalid-range", "missing_ranges", "must be sorted, non-overlapping inclusive ranges")
            previous_end = end
        object.__setattr__(self, "missing_ranges", normalized)

    @classmethod
    def from_dict(cls, value: JSON) -> DatasetCoverage:
        known = {field.name for field in fields(cls)}
        missing = [name for name in known if name not in value]
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(**{name: value[name] for name in known})

    def to_dict(self) -> JSON:
        return _json(asdict(self))


def _coverages(value: Any, field: str = "datasets") -> dict[str, DatasetCoverage]:
    if not isinstance(value, dict):
        raise FederationValidationError("invalid-object", field, "must be an object")
    result = {key: item if isinstance(item, DatasetCoverage) else DatasetCoverage.from_dict(item) for key, item in value.items()}
    if any(key != item.dataset_id for key, item in result.items()):
        raise FederationValidationError("dataset-key-mismatch", field, "keys must match dataset IDs")
    return result


@dataclass(frozen=True)
class StorageManifest(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.storage_manifest.v1"
    session_id: str
    group_id: str
    revision: int
    term: int
    datasets: dict[str, DatasetCoverage]
    manifest_hash: str
    updated_at: datetime

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "manifest_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name in ("revision", "term"):
            object.__setattr__(self, name, _uint(getattr(self, name), name))
        object.__setattr__(self, "datasets", _coverages(self.datasets))
        object.__setattr__(self, "updated_at", _utc(self.updated_at, "updated_at"))

    def calculated_hash(self) -> str:
        """Hash the canonical manifest representation, excluding its hash field."""
        value = self.to_dict()
        del value["manifest_hash"]
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return f"sha256:{hashlib.sha256(canonical.encode()).hexdigest()}"


@dataclass(frozen=True)
class StorageBatch(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.storage_batch.v1"
    session_id: str
    group_id: str
    dataset_id: str
    batch_id: str
    source_node_id: str
    idempotency_key: str
    content_hash: str
    record_count: int
    first_sequence: int
    last_sequence: int
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "dataset_id", "batch_id", "source_node_id", "idempotency_key", "content_hash"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        for name in ("record_count", "first_sequence", "last_sequence"):
            object.__setattr__(self, name, _uint(getattr(self, name), name))
        if self.last_sequence < self.first_sequence:
            raise FederationValidationError("invalid-sequence-range", "last_sequence", "must not precede first_sequence")
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))


@dataclass(frozen=True)
class StorageNodeState(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.storage_node_state.v1"
    session_id: str
    group_id: str
    node_id: str
    role: StorageRole
    online: bool
    integrity_verified: bool
    schema_compatible: bool
    leader_eligible: bool
    authorized: bool
    authoritative_revision: int
    replication_lag: int
    datasets: dict[str, DatasetCoverage]
    term: int
    last_fencing_token: int
    reported_at: datetime
    operational_stability: int = 0
    available_bytes: int = 0

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "node_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        try:
            object.__setattr__(self, "role", StorageRole(self.role))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "role", "unknown storage role") from exc
        for name in ("online", "integrity_verified", "schema_compatible", "leader_eligible", "authorized"):
            if not isinstance(getattr(self, name), bool):
                raise FederationValidationError("invalid-boolean", name, "must be boolean")
        for name in ("authoritative_revision", "replication_lag", "term", "last_fencing_token", "operational_stability", "available_bytes"):
            object.__setattr__(self, name, _uint(getattr(self, name), name))
        object.__setattr__(self, "datasets", _coverages(self.datasets))
        object.__setattr__(self, "reported_at", _utc(self.reported_at, "reported_at"))


@dataclass(frozen=True)
class LeadershipGrant(DomainModel):
    SCHEMA: ClassVar[str] = "fcp.leadership_grant.v1"
    session_id: str
    group_id: str
    node_id: str
    role: StorageRole
    term: int
    fencing_token: int
    issued_at: datetime
    lease_expires_at: datetime
    grant_id: str

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "node_id", "grant_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        try:
            object.__setattr__(self, "role", StorageRole(self.role))
        except ValueError as exc:
            raise FederationValidationError("invalid-enum", "role", "unknown storage role") from exc
        for name in ("term", "fencing_token"):
            object.__setattr__(self, name, _uint(getattr(self, name), name))
        object.__setattr__(self, "issued_at", _utc(self.issued_at, "issued_at"))
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at, "lease_expires_at"))
        if self.lease_expires_at <= self.issued_at:
            raise FederationValidationError("invalid-lease", "lease_expires_at", "must be later than issued_at")


@dataclass(frozen=True)
class StorageWriteRequest:
    session_id: str
    group_id: str
    actor_node_id: str
    term: int
    fencing_token: int
    grant_id: str

    def __post_init__(self) -> None:
        for name in ("session_id", "group_id", "actor_node_id", "grant_id"):
            _required_text(getattr(self, name), name)
        _uint(self.term, "term")
        _uint(self.fencing_token, "fencing_token")

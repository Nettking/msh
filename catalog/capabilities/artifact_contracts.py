"""Versioned F7.6 artifact placement and capability-specific grant contracts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

ARTIFACT_PROTOCOL = "fcp-capability-artifacts"
ARTIFACT_PROTOCOL_VERSION = "1.0"
ARTIFACT_PROTOCOL_MAJOR = 1
ARTIFACT_DESCRIPTOR_SCHEMA = "fcp.capability-artifact.v1"
ARTIFACT_INPUT_REFERENCE_SCHEMA = "fcp.capability-artifact-input.v1"
OUTPUT_PLACEMENT_POLICY_SCHEMA = "fcp.capability-output-placement.v1"
ARTIFACT_GRANT_SCHEMA = "fcp.capability-artifact-grant.v1"
ARTIFACT_PUBLICATION_SCHEMA = "fcp.capability-artifact-publication.v1"
MAX_ARTIFACT_CONTROL_MESSAGE_BYTES = 131_072
MAX_ARTIFACT_GRANT_SECONDS = 3_600
MAX_ARTIFACTS_PER_GRANT = 256
MAX_SCHEMA_IDS_PER_POLICY = 64
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9._/-]+$")


class ArtifactEndpointKind(str, Enum):
    LOGICAL_STORAGE = "logical-storage"
    OBJECT = "object"


class ArtifactGrantScope(str, Enum):
    READ_INPUT = "artifact:read-input"
    WRITE_OUTPUT = "artifact:write-output"
    PUBLISH_RESULT = "artifact:publish-result"


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _bounded(value: dict[str, Any]) -> None:
    if len(_canonical(value).encode("utf-8")) > MAX_ARTIFACT_CONTROL_MESSAGE_BYTES:
        raise FederationValidationError(
            "artifact-control-message-too-large",
            "message",
            f"must not exceed {MAX_ARTIFACT_CONTROL_MESSAGE_BYTES} bytes",
        )


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FederationValidationError(
            "invalid-text", field, "must be a non-empty string"
        )
    return value.strip()


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise FederationValidationError(
            "invalid-timestamp", field, "must be ISO-8601"
        ) from error
    return _utc(parsed, field)


def _nonnegative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-nonnegative-integer", field, "must be a non-negative integer"
        )
    return value


def _positive(value: Any, field: str, *, maximum: int | None = None) -> int:
    number = _nonnegative(value, field)
    if number == 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    if maximum is not None and number > maximum:
        raise FederationValidationError(
            "integer-too-large", field, f"must not exceed {maximum}"
        )
    return number


def _protocol(value: Any) -> str:
    version = _text(value, "protocol_version")
    major_text = version.split(".", 1)[0]
    if not major_text.isdigit() or int(major_text) != ARTIFACT_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-artifact-protocol-major",
            "protocol_version",
            f"supported major is {ARTIFACT_PROTOCOL_MAJOR}",
        )
    return version


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual:
        raise ProtocolCompatibilityError(
            "unsupported-schema", "schema", f"expected {expected}"
        )
    actual_name, actual_major = actual.rsplit(".v", 1)
    expected_name, expected_major = expected.rsplit(".v", 1)
    if actual_name != expected_name or not actual_major.isdigit():
        raise ProtocolCompatibilityError(
            "unsupported-schema", "schema", f"expected {expected}"
        )
    if int(actual_major) != int(expected_major):
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            "schema",
            f"supported major is {expected_major}",
        )


def _content_hash(value: Any, field: str = "content_hash") -> str:
    digest = _text(value, field).lower()
    if not _SHA256_RE.fullmatch(digest):
        raise FederationValidationError(
            "invalid-content-hash", field, "must be sha256:<64 lowercase hex characters>"
        )
    return digest


def _logical_key(value: Any, field: str) -> str:
    key = _text(value, field)
    if (
        key.startswith("/")
        or key.endswith("/")
        or ".." in key.split("/")
        or "\\" in key
        or "://" in key
        or not _SAFE_KEY_RE.fullmatch(key)
    ):
        raise FederationValidationError(
            "invalid-logical-object-key",
            field,
            "must be a relative logical key without traversal, URL, or backslash",
        )
    return key


def _unique_texts(
    values: Any,
    field: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise FederationValidationError(
            "invalid-list", field, "must be a list"
        )
    normalized = tuple(_text(value, field) for value in values)
    if not allow_empty and not normalized:
        raise FederationValidationError(
            "empty-list", field, "must contain at least one value"
        )
    if len(normalized) > maximum:
        raise FederationValidationError(
            "list-too-large", field, f"must contain at most {maximum} values"
        )
    if len(set(normalized)) != len(normalized):
        raise FederationValidationError(
            "duplicate-list-value", field, "must not contain duplicates"
        )
    return normalized


@dataclass(frozen=True)
class ArtifactDescriptor:
    artifact_id: str
    session_id: str
    job_id: str
    schema_id: str
    media_type: str
    content_hash: str
    size_bytes: int
    endpoint_id: str
    object_key: str
    created_at: datetime
    transfer_id: str | None = None
    protocol_version: str = ARTIFACT_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = ARTIFACT_DESCRIPTOR_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "artifact_id",
            "session_id",
            "job_id",
            "schema_id",
            "media_type",
            "endpoint_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "content_hash", _content_hash(self.content_hash))
        object.__setattr__(self, "size_bytes", _nonnegative(self.size_bytes, "size_bytes"))
        object.__setattr__(self, "object_key", _logical_key(self.object_key, "object_key"))
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(
            self, "transfer_id", _optional_text(self.transfer_id, "transfer_id")
        )
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": ARTIFACT_PROTOCOL,
            "protocol_version": self.protocol_version,
            "artifact_id": self.artifact_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "schema_id": self.schema_id,
            "media_type": self.media_type,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "endpoint_id": self.endpoint_id,
            "object_key": self.object_key,
            "created_at": _timestamp(self.created_at),
            "transfer_id": self.transfer_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "artifact", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != ARTIFACT_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-artifact-protocol", "protocol", f"expected {ARTIFACT_PROTOCOL}"
            )
        return cls(
            artifact_id=value.get("artifact_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            schema_id=value.get("schema_id"),
            media_type=value.get("media_type"),
            content_hash=value.get("content_hash"),
            size_bytes=value.get("size_bytes"),
            endpoint_id=value.get("endpoint_id"),
            object_key=value.get("object_key"),
            created_at=_parse_timestamp(value.get("created_at"), "created_at"),
            transfer_id=value.get("transfer_id"),
            protocol_version=value.get("protocol_version"),
        )


@dataclass(frozen=True)
class ArtifactInputReference:
    artifact_id: str
    session_id: str
    job_id: str
    content_hash: str
    schema_id: str
    endpoint_id: str
    protocol_version: str = ARTIFACT_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = ARTIFACT_INPUT_REFERENCE_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "artifact_id",
            "session_id",
            "job_id",
            "schema_id",
            "endpoint_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "content_hash", _content_hash(self.content_hash))
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": ARTIFACT_PROTOCOL,
            "protocol_version": self.protocol_version,
            "artifact_id": self.artifact_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "content_hash": self.content_hash,
            "schema_id": self.schema_id,
            "endpoint_id": self.endpoint_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "input_reference", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != ARTIFACT_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-artifact-protocol", "protocol", f"expected {ARTIFACT_PROTOCOL}"
            )
        return cls(
            artifact_id=value.get("artifact_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            content_hash=value.get("content_hash"),
            schema_id=value.get("schema_id"),
            endpoint_id=value.get("endpoint_id"),
            protocol_version=value.get("protocol_version"),
        )


@dataclass(frozen=True)
class OutputPlacementPolicy:
    policy_id: str
    session_id: str
    job_id: str
    endpoint_kind: ArtifactEndpointKind
    endpoint_id: str
    namespace: str
    allowed_schema_ids: tuple[str, ...]
    max_artifact_bytes: int
    max_artifacts: int
    resumable_threshold_bytes: int
    expires_at: datetime
    protocol_version: str = ARTIFACT_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = OUTPUT_PLACEMENT_POLICY_SCHEMA

    def __post_init__(self) -> None:
        for field in ("policy_id", "session_id", "job_id", "endpoint_id"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            endpoint_kind = ArtifactEndpointKind(self.endpoint_kind)
        except (TypeError, ValueError) as error:
            raise FederationValidationError(
                "invalid-endpoint-kind", "endpoint_kind", "unsupported endpoint kind"
            ) from error
        object.__setattr__(self, "endpoint_kind", endpoint_kind)
        object.__setattr__(self, "namespace", _logical_key(self.namespace, "namespace"))
        object.__setattr__(
            self,
            "allowed_schema_ids",
            _unique_texts(
                self.allowed_schema_ids,
                "allowed_schema_ids",
                maximum=MAX_SCHEMA_IDS_PER_POLICY,
            ),
        )
        object.__setattr__(
            self,
            "max_artifact_bytes",
            _positive(self.max_artifact_bytes, "max_artifact_bytes"),
        )
        object.__setattr__(
            self,
            "max_artifacts",
            _positive(self.max_artifacts, "max_artifacts", maximum=MAX_ARTIFACTS_PER_GRANT),
        )
        threshold = _positive(
            self.resumable_threshold_bytes, "resumable_threshold_bytes"
        )
        if threshold > self.max_artifact_bytes:
            raise FederationValidationError(
                "invalid-resumable-threshold",
                "resumable_threshold_bytes",
                "must not exceed max_artifact_bytes",
            )
        object.__setattr__(self, "resumable_threshold_bytes", threshold)
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def accepts(self, descriptor: ArtifactDescriptor, *, now: datetime) -> bool:
        now = _utc(now, "now")
        prefix = f"{self.namespace}/"
        return (
            now < self.expires_at
            and descriptor.session_id == self.session_id
            and descriptor.job_id == self.job_id
            and descriptor.endpoint_id == self.endpoint_id
            and descriptor.schema_id in self.allowed_schema_ids
            and descriptor.size_bytes <= self.max_artifact_bytes
            and descriptor.object_key.startswith(prefix)
        )

    def requires_resumable_transfer(self, size_bytes: int) -> bool:
        return _nonnegative(size_bytes, "size_bytes") >= self.resumable_threshold_bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": ARTIFACT_PROTOCOL,
            "protocol_version": self.protocol_version,
            "policy_id": self.policy_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "endpoint_kind": self.endpoint_kind.value,
            "endpoint_id": self.endpoint_id,
            "namespace": self.namespace,
            "allowed_schema_ids": list(self.allowed_schema_ids),
            "max_artifact_bytes": self.max_artifact_bytes,
            "max_artifacts": self.max_artifacts,
            "resumable_threshold_bytes": self.resumable_threshold_bytes,
            "expires_at": _timestamp(self.expires_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "placement_policy", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != ARTIFACT_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-artifact-protocol", "protocol", f"expected {ARTIFACT_PROTOCOL}"
            )
        return cls(
            policy_id=value.get("policy_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            endpoint_kind=value.get("endpoint_kind"),
            endpoint_id=value.get("endpoint_id"),
            namespace=value.get("namespace"),
            allowed_schema_ids=tuple(value.get("allowed_schema_ids", ())),
            max_artifact_bytes=value.get("max_artifact_bytes"),
            max_artifacts=value.get("max_artifacts"),
            resumable_threshold_bytes=value.get("resumable_threshold_bytes"),
            expires_at=_parse_timestamp(value.get("expires_at"), "expires_at"),
            protocol_version=value.get("protocol_version"),
        )


@dataclass(frozen=True)
class ArtifactGrant:
    grant_id: str
    session_id: str
    job_id: str
    attempt_id: str
    lease_id: str
    lease_generation: int
    provider_id: str
    worker_node_id: str
    endpoint_id: str
    scopes: tuple[ArtifactGrantScope, ...]
    input_artifact_ids: tuple[str, ...]
    output_namespace: str | None
    max_output_bytes: int
    issued_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None
    protocol_version: str = ARTIFACT_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = ARTIFACT_GRANT_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "grant_id",
            "session_id",
            "job_id",
            "attempt_id",
            "lease_id",
            "provider_id",
            "worker_node_id",
            "endpoint_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "lease_generation",
            _positive(self.lease_generation, "lease_generation"),
        )
        try:
            scopes = tuple(ArtifactGrantScope(scope) for scope in self.scopes)
        except (TypeError, ValueError) as error:
            raise FederationValidationError(
                "invalid-artifact-scope", "scopes", "contains an unsupported scope"
            ) from error
        if not scopes or len(set(scopes)) != len(scopes):
            raise FederationValidationError(
                "invalid-artifact-scopes", "scopes", "must be non-empty and unique"
            )
        object.__setattr__(self, "scopes", scopes)
        inputs = _unique_texts(
            self.input_artifact_ids,
            "input_artifact_ids",
            maximum=MAX_ARTIFACTS_PER_GRANT,
            allow_empty=True,
        )
        object.__setattr__(self, "input_artifact_ids", inputs)
        namespace = (
            None
            if self.output_namespace is None
            else _logical_key(self.output_namespace, "output_namespace")
        )
        object.__setattr__(self, "output_namespace", namespace)
        max_output = _nonnegative(self.max_output_bytes, "max_output_bytes")
        object.__setattr__(self, "max_output_bytes", max_output)
        issued_at = _utc(self.issued_at, "issued_at")
        expires_at = _utc(self.expires_at, "expires_at")
        if expires_at <= issued_at:
            raise FederationValidationError(
                "invalid-grant-window", "expires_at", "must be after issued_at"
            )
        if (expires_at - issued_at).total_seconds() > MAX_ARTIFACT_GRANT_SECONDS:
            raise FederationValidationError(
                "grant-window-too-large",
                "expires_at",
                f"must not exceed {MAX_ARTIFACT_GRANT_SECONDS} seconds",
            )
        revoked_at = None if self.revoked_at is None else _utc(self.revoked_at, "revoked_at")
        if revoked_at is not None and revoked_at < issued_at:
            raise FederationValidationError(
                "invalid-revocation-time", "revoked_at", "must not precede issued_at"
            )
        object.__setattr__(self, "issued_at", issued_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "revoked_at", revoked_at)
        if ArtifactGrantScope.READ_INPUT in scopes and not inputs:
            raise FederationValidationError(
                "unbounded-input-grant",
                "input_artifact_ids",
                "read-input scope requires explicit artifact IDs",
            )
        output_scopes = {
            ArtifactGrantScope.WRITE_OUTPUT,
            ArtifactGrantScope.PUBLISH_RESULT,
        }
        if output_scopes.intersection(scopes) and (namespace is None or max_output == 0):
            raise FederationValidationError(
                "unbounded-output-grant",
                "output_namespace",
                "output scopes require a namespace and byte limit",
            )
        if not output_scopes.intersection(scopes) and (namespace is not None or max_output != 0):
            raise FederationValidationError(
                "unexpected-output-authority",
                "output_namespace",
                "read-only grants cannot contain output authority",
            )
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def active_at(self, now: datetime) -> bool:
        now = _utc(now, "now")
        return self.revoked_at is None and self.issued_at <= now < self.expires_at

    def permits_input(self, artifact_id: str) -> bool:
        return (
            ArtifactGrantScope.READ_INPUT in self.scopes
            and _text(artifact_id, "artifact_id") in self.input_artifact_ids
        )

    def permits_output(self, descriptor: ArtifactDescriptor) -> bool:
        prefix = None if self.output_namespace is None else f"{self.output_namespace}/"
        return (
            ArtifactGrantScope.WRITE_OUTPUT in self.scopes
            and prefix is not None
            and descriptor.session_id == self.session_id
            and descriptor.job_id == self.job_id
            and descriptor.endpoint_id == self.endpoint_id
            and descriptor.size_bytes <= self.max_output_bytes
            and descriptor.object_key.startswith(prefix)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": ARTIFACT_PROTOCOL,
            "protocol_version": self.protocol_version,
            "grant_id": self.grant_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "provider_id": self.provider_id,
            "worker_node_id": self.worker_node_id,
            "endpoint_id": self.endpoint_id,
            "scopes": [scope.value for scope in self.scopes],
            "input_artifact_ids": list(self.input_artifact_ids),
            "output_namespace": self.output_namespace,
            "max_output_bytes": self.max_output_bytes,
            "issued_at": _timestamp(self.issued_at),
            "expires_at": _timestamp(self.expires_at),
            "revoked_at": None if self.revoked_at is None else _timestamp(self.revoked_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "grant", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != ARTIFACT_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-artifact-protocol", "protocol", f"expected {ARTIFACT_PROTOCOL}"
            )
        revoked = value.get("revoked_at")
        return cls(
            grant_id=value.get("grant_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            attempt_id=value.get("attempt_id"),
            lease_id=value.get("lease_id"),
            lease_generation=value.get("lease_generation"),
            provider_id=value.get("provider_id"),
            worker_node_id=value.get("worker_node_id"),
            endpoint_id=value.get("endpoint_id"),
            scopes=tuple(value.get("scopes", ())),
            input_artifact_ids=tuple(value.get("input_artifact_ids", ())),
            output_namespace=value.get("output_namespace"),
            max_output_bytes=value.get("max_output_bytes"),
            issued_at=_parse_timestamp(value.get("issued_at"), "issued_at"),
            expires_at=_parse_timestamp(value.get("expires_at"), "expires_at"),
            revoked_at=None if revoked is None else _parse_timestamp(revoked, "revoked_at"),
            protocol_version=value.get("protocol_version"),
        )


@dataclass(frozen=True)
class ArtifactPublication:
    publication_id: str
    grant_id: str
    session_id: str
    job_id: str
    attempt_id: str
    lease_id: str
    lease_generation: int
    provider_id: str
    worker_node_id: str
    descriptor: ArtifactDescriptor
    published_at: datetime
    protocol_version: str = ARTIFACT_PROTOCOL_VERSION
    SCHEMA: ClassVar[str] = ARTIFACT_PUBLICATION_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "publication_id",
            "grant_id",
            "session_id",
            "job_id",
            "attempt_id",
            "lease_id",
            "provider_id",
            "worker_node_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(
            self,
            "lease_generation",
            _positive(self.lease_generation, "lease_generation"),
        )
        if not isinstance(self.descriptor, ArtifactDescriptor):
            raise FederationValidationError(
                "invalid-artifact-descriptor", "descriptor", "must be an ArtifactDescriptor"
            )
        if (
            self.descriptor.session_id != self.session_id
            or self.descriptor.job_id != self.job_id
        ):
            raise FederationValidationError(
                "publication-artifact-context-mismatch",
                "descriptor",
                "artifact must belong to the publication session and job",
            )
        object.__setattr__(self, "published_at", _utc(self.published_at, "published_at"))
        object.__setattr__(self, "protocol_version", _protocol(self.protocol_version))
        _bounded(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol": ARTIFACT_PROTOCOL,
            "protocol_version": self.protocol_version,
            "publication_id": self.publication_id,
            "grant_id": self.grant_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "lease_id": self.lease_id,
            "lease_generation": self.lease_generation,
            "provider_id": self.provider_id,
            "worker_node_id": self.worker_node_id,
            "descriptor": self.descriptor.to_dict(),
            "published_at": _timestamp(self.published_at),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "publication", "must be an object")
        _schema(value.get("schema"), cls.SCHEMA)
        if value.get("protocol") != ARTIFACT_PROTOCOL:
            raise ProtocolCompatibilityError(
                "unsupported-artifact-protocol", "protocol", f"expected {ARTIFACT_PROTOCOL}"
            )
        return cls(
            publication_id=value.get("publication_id"),
            grant_id=value.get("grant_id"),
            session_id=value.get("session_id"),
            job_id=value.get("job_id"),
            attempt_id=value.get("attempt_id"),
            lease_id=value.get("lease_id"),
            lease_generation=value.get("lease_generation"),
            provider_id=value.get("provider_id"),
            worker_node_id=value.get("worker_node_id"),
            descriptor=ArtifactDescriptor.from_dict(value.get("descriptor")),
            published_at=_parse_timestamp(value.get("published_at"), "published_at"),
            protocol_version=value.get("protocol_version"),
        )

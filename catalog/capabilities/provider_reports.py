"""Validated, short-lived resource reports for capability providers."""

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
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
)

PROVIDER_REPORT_PROTOCOL = "fcp-capability-provider-report"
PROVIDER_REPORT_PROTOCOL_VERSION = "1.0"
PROVIDER_REPORT_PROTOCOL_MAJOR = 1
PROVIDER_REPORT_SCHEMA = "fcp.capability-provider-report.v1"
SELECTION_POLICY_SCHEMA = "fcp.capability-selection-policy.v1"
MAX_PROVIDER_REPORT_BYTES = 131_072
MAX_TEXT_BYTES = 512
MAX_REPORT_TTL_SECONDS = 3_600
MAX_CONCURRENT_JOBS = 1_000_000
MAX_QUEUE_DEPTH = 1_000_000
UTILIZATION_SCALE = 1_000

_SCHEMA_RE = re.compile(r"^(?P<name>fcp\.[a-z0-9.-]+)\.v(?P<major>[0-9]+)$")


class ProviderStatus(str, Enum):
    """Scheduling availability reported by one capability provider."""

    READY = "ready"
    DRAINING = "draining"
    UNAVAILABLE = "unavailable"
    DISABLED = "disabled"
    REVOKED = "revoked"


def _text(value: Any, field: str, *, maximum: int = MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty text without surrounding whitespace",
        )
    if any(ord(character) < 32 for character in value):
        raise FederationValidationError(
            "invalid-text", field, "must not contain control characters"
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "text-too-large", field, f"must not exceed {maximum} UTF-8 bytes"
        )
    return value


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _non_negative(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    if maximum is not None and value > maximum:
        raise FederationValidationError(
            "integer-too-large", field, f"must not exceed {maximum}"
        )
    return value


def _positive(value: Any, field: str, *, maximum: int | None = None) -> int:
    value = _non_negative(value, field, maximum=maximum)
    if value == 0:
        raise FederationValidationError(
            "invalid-positive-integer", field, "must be a positive integer"
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", field, "must be RFC 3339"
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware UTC"
        )
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError(
            "invalid-timestamp", field, "must use UTC"
        )
    return value.astimezone(timezone.utc)


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in value
    ):
        raise FederationValidationError(
            "invalid-object", field, "must be an object with string keys"
        )
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain JSON-compatible finite values"
        ) from exc
    result = dict(value)
    if contains_secret_material(result):
        raise FederationValidationError(
            "secret-provider-attribute",
            field,
            "must not contain credentials or secret material",
        )
    if contains_nonpublic_location(result):
        raise FederationValidationError(
            "nonpublic-provider-attribute",
            field,
            "must not expose endpoints, addresses, or backend paths",
        )
    return result


def _array(value: Any, field: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise FederationValidationError("invalid-array", field, "must be an array")
    return tuple(value)


def _required(value: dict[str, Any], names: tuple[str, ...]) -> None:
    for name in names:
        if name not in value:
            raise FederationValidationError("missing-field", name, "is required")


def _schema(value: Any, expected: str) -> int:
    actual_text = _text(value, "schema")
    actual = _SCHEMA_RE.fullmatch(actual_text)
    wanted = _SCHEMA_RE.fullmatch(expected)
    if (
        actual is None
        or wanted is None
        or actual.group("name") != wanted.group("name")
    ):
        raise ProtocolCompatibilityError(
            "unsupported-schema",
            "schema",
            f"expected {expected}, received {actual_text!r}",
        )
    major = int(actual.group("major"))
    expected_major = int(wanted.group("major"))
    if major != expected_major:
        raise ProtocolCompatibilityError(
            "unsupported-schema-major",
            "schema",
            f"supported major is {expected_major}, received {major}",
        )
    return major


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", "value", "must contain JSON-compatible finite values"
        ) from exc


def protocol_version_parts(
    value: Any,
    field: str = "protocol_version",
) -> tuple[int, int]:
    version = _text(value, field)
    parts = version.split(".", 1)
    if not parts[0].isdigit() or (
        len(parts) == 2 and not parts[1].isdigit()
    ):
        raise ProtocolCompatibilityError(
            "invalid-protocol-version",
            field,
            "must be a numeric major or major.minor version",
        )
    return int(parts[0]), int(parts[1]) if len(parts) == 2 else 0


def validate_provider_report_protocol(value: Any) -> int:
    major, _minor = protocol_version_parts(
        value, "report_protocol_version"
    )
    if major != PROVIDER_REPORT_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "report_protocol_version",
            f"supported major is {PROVIDER_REPORT_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def provider_protocol_satisfies(required: Any, offered: Any) -> bool:
    required_major, required_minor = protocol_version_parts(
        required, "required_protocol_version"
    )
    offered_major, offered_minor = protocol_version_parts(
        offered, "offered_protocol_version"
    )
    return offered_major == required_major and offered_minor >= required_minor


@dataclass(frozen=True)
class ProviderResourceReport:
    """A bounded, short-lived scheduling snapshot from one provider."""

    SCHEMA: ClassVar[str] = PROVIDER_REPORT_SCHEMA

    capability_id: str
    node_id: str
    session_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    status: ProviderStatus
    report_revision: int
    max_concurrent_jobs: int
    active_jobs: int
    queue_depth: int
    utilization_millis: int
    attributes: dict[str, Any]
    reported_at: datetime
    expires_at: datetime
    report_protocol_version: str = PROVIDER_REPORT_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in (
            "capability_id",
            "node_id",
            "session_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "report_protocol_version",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        protocol_version_parts(self.protocol_version)
        validate_provider_report_protocol(self.report_protocol_version)
        try:
            object.__setattr__(self, "status", ProviderStatus(self.status))
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-enum", "status", "unknown provider status"
            ) from exc
        object.__setattr__(
            self,
            "report_revision",
            _non_negative(self.report_revision, "report_revision"),
        )
        object.__setattr__(
            self,
            "max_concurrent_jobs",
            _positive(
                self.max_concurrent_jobs,
                "max_concurrent_jobs",
                maximum=MAX_CONCURRENT_JOBS,
            ),
        )
        object.__setattr__(
            self,
            "active_jobs",
            _non_negative(
                self.active_jobs,
                "active_jobs",
                maximum=MAX_CONCURRENT_JOBS,
            ),
        )
        if self.active_jobs > self.max_concurrent_jobs:
            raise FederationValidationError(
                "active-jobs-exceed-capacity",
                "active_jobs",
                "must not exceed max_concurrent_jobs",
            )
        object.__setattr__(
            self,
            "queue_depth",
            _non_negative(
                self.queue_depth,
                "queue_depth",
                maximum=MAX_QUEUE_DEPTH,
            ),
        )
        object.__setattr__(
            self,
            "utilization_millis",
            _non_negative(
                self.utilization_millis,
                "utilization_millis",
                maximum=UTILIZATION_SCALE,
            ),
        )
        object.__setattr__(
            self, "attributes", _object(self.attributes, "attributes")
        )
        object.__setattr__(
            self, "reported_at", _utc(self.reported_at, "reported_at")
        )
        object.__setattr__(
            self, "expires_at", _utc(self.expires_at, "expires_at")
        )
        if self.expires_at <= self.reported_at:
            raise FederationValidationError(
                "invalid-report-expiry",
                "expires_at",
                "must be later than reported_at",
            )
        ttl_seconds = (self.expires_at - self.reported_at).total_seconds()
        if ttl_seconds > MAX_REPORT_TTL_SECONDS:
            raise FederationValidationError(
                "report-ttl-too-large",
                "expires_at",
                f"must not exceed {MAX_REPORT_TTL_SECONDS} seconds",
            )

    @property
    def available_slots(self) -> int:
        return self.max_concurrent_jobs - self.active_jobs

    def is_fresh_at(self, value: datetime) -> bool:
        observed_at = _utc(value, "evaluated_at")
        return self.reported_at <= observed_at < self.expires_at

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "provider_report", "must be an object"
            )
        names = (
            "schema",
            "report_protocol_version",
            "capability_id",
            "node_id",
            "session_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "status",
            "report_revision",
            "max_concurrent_jobs",
            "active_jobs",
            "queue_depth",
            "utilization_millis",
            "attributes",
            "reported_at",
            "expires_at",
        )
        _required(value, names)
        _schema(value["schema"], cls.SCHEMA)
        return cls(**{name: value[name] for name in names if name != "schema"})

    @classmethod
    def from_json(
        cls,
        value: str | bytes,
        *,
        max_bytes: int = MAX_PROVIDER_REPORT_BYTES,
    ) -> Self:
        encoded = value.encode("utf-8") if isinstance(value, str) else value
        if not isinstance(encoded, bytes):
            raise FederationValidationError(
                "malformed-message",
                "provider_report",
                "must be text or UTF-8 bytes",
            )
        if len(encoded) > max_bytes:
            raise FederationValidationError(
                "message-too-large",
                "provider_report",
                f"exceeds {max_bytes} bytes",
            )
        try:
            decoded = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "malformed-message",
                "provider_report",
                "must be valid UTF-8 JSON",
            ) from exc
        return cls.from_dict(decoded)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "report_protocol_version": self.report_protocol_version,
            "capability_id": self.capability_id,
            "node_id": self.node_id,
            "session_id": self.session_id,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "status": self.status.value,
            "report_revision": self.report_revision,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "active_jobs": self.active_jobs,
            "queue_depth": self.queue_depth,
            "utilization_millis": self.utilization_millis,
            "attributes": self.attributes,
            "reported_at": self.reported_at.isoformat().replace("+00:00", "Z"),
            "expires_at": self.expires_at.isoformat().replace("+00:00", "Z"),
        }

    def to_json(self, *, max_bytes: int = MAX_PROVIDER_REPORT_BYTES) -> str:
        encoded = _canonical(self.to_dict())
        if len(encoded.encode("utf-8")) > max_bytes:
            raise FederationValidationError(
                "message-too-large",
                "provider_report",
                f"exceeds {max_bytes} bytes",
            )
        return encoded


@dataclass(frozen=True)
class ProviderSelectionPolicy:
    """Explicit deterministic constraints with no hidden global state."""

    SCHEMA: ClassVar[str] = SELECTION_POLICY_SCHEMA

    minimum_available_slots: int = 1
    max_queue_depth: int = MAX_QUEUE_DEPTH
    max_utilization_millis: int = UTILIZATION_SCALE
    preferred_node_id: str | None = None
    excluded_capability_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "minimum_available_slots",
            _positive(
                self.minimum_available_slots,
                "minimum_available_slots",
                maximum=MAX_CONCURRENT_JOBS,
            ),
        )
        object.__setattr__(
            self,
            "max_queue_depth",
            _non_negative(
                self.max_queue_depth,
                "max_queue_depth",
                maximum=MAX_QUEUE_DEPTH,
            ),
        )
        object.__setattr__(
            self,
            "max_utilization_millis",
            _non_negative(
                self.max_utilization_millis,
                "max_utilization_millis",
                maximum=UTILIZATION_SCALE,
            ),
        )
        object.__setattr__(
            self,
            "preferred_node_id",
            _optional_text(self.preferred_node_id, "preferred_node_id"),
        )
        excluded = tuple(
            _text(item, f"excluded_capability_ids[{index}]")
            for index, item in enumerate(
                _array(self.excluded_capability_ids, "excluded_capability_ids")
            )
        )
        if len(excluded) != len(set(excluded)):
            raise FederationValidationError(
                "duplicate-excluded-provider",
                "excluded_capability_ids",
                "must not contain duplicates",
            )
        object.__setattr__(
            self, "excluded_capability_ids", tuple(sorted(excluded))
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "selection_policy", "must be an object"
            )
        _required(value, ("schema",))
        _schema(value["schema"], cls.SCHEMA)
        return cls(
            minimum_available_slots=value.get("minimum_available_slots", 1),
            max_queue_depth=value.get("max_queue_depth", MAX_QUEUE_DEPTH),
            max_utilization_millis=value.get(
                "max_utilization_millis", UTILIZATION_SCALE
            ),
            preferred_node_id=value.get("preferred_node_id"),
            excluded_capability_ids=_array(
                value.get("excluded_capability_ids", ()),
                "excluded_capability_ids",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "minimum_available_slots": self.minimum_available_slots,
            "max_queue_depth": self.max_queue_depth,
            "max_utilization_millis": self.max_utilization_millis,
            "preferred_node_id": self.preferred_node_id,
            "excluded_capability_ids": list(self.excluded_capability_ids),
        }

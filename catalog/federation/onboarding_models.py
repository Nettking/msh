"""Capability-first onboarding contracts.

These models are additive to Federation v1. They describe safe discovery,
inspection, benchmark, and contribution-intent data without granting authority.
"""

from __future__ import annotations

import json
import math
from dataclasses import MISSING, asdict, dataclass, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Self

from .errors import FederationValidationError, ProtocolCompatibilityError
from .redaction import contains_nonpublic_location, contains_secret_material

JSON = dict[str, Any]
MAX_TEXT_BYTES = 512
MAX_COLLECTION_ITEMS = 64
MAX_SAFE_DEPTH = 5


class FederationConnectionState(str, Enum):
    DISCOVERED = "discovered"
    VERIFICATION_REQUIRED = "verification-required"
    JOINING = "joining"
    CONNECTED = "connected"
    UNAVAILABLE = "unavailable"
    REVOKED = "revoked"


class BenchmarkState(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class BenchmarkRecommendation(str, Enum):
    RECOMMENDED = "recommended"
    USABLE = "usable"
    NOT_RECOMMENDED = "not-recommended"
    UNSUPPORTED = "unsupported"
    RERUN_REQUIRED = "rerun-required"


class ContributionDesiredState(str, Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    ASK_LATER = "ask-later"


class ContributionPolicyState(str, Enum):
    NOT_EVALUATED = "not-evaluated"
    ALLOWED = "allowed"
    CONFIRMATION_REQUIRED = "confirmation-required"
    APPROVAL_REQUIRED = "approval-required"
    BLOCKED = "blocked"


class ContributionActivationState(str, Enum):
    INACTIVE = "inactive"
    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    BLOCKED = "blocked"


def _required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be a non-empty printable string",
        )
    return value.strip()


def _safe_text(value: Any, field: str) -> str:
    value = _required_text(value, field)
    if len(value.encode("utf-8")) > MAX_TEXT_BYTES:
        raise FederationValidationError(
            "text-too-large",
            field,
            f"must not exceed {MAX_TEXT_BYTES} UTF-8 bytes",
        )
    if contains_secret_material(value):
        raise FederationValidationError(
            "secret-metadata",
            field,
            "must not contain credentials or secrets",
        )
    if contains_nonpublic_location(value):
        raise FederationValidationError(
            "nonpublic-metadata",
            field,
            "must not expose private endpoints, addresses, or backend paths",
        )
    return value


def _optional_safe_text(value: Any, field: str) -> str | None:
    return None if value is None else _safe_text(value, field)


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _positive_int(value: Any, field: str) -> int:
    value = _uint(value, field)
    if value == 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be greater than zero",
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                field,
                "must be RFC 3339",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be timezone-aware UTC",
        )
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must use UTC",
        )
    return value.astimezone(timezone.utc)


def _optional_utc(value: Any, field: str) -> datetime | None:
    return None if value is None else _utc(value, field)


def _enum(value: Any, enum_type: type[Enum], field: str) -> Enum:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-enum",
            field,
            f"unknown {field} value",
        ) from exc


def _safe_json(value: Any, field: str, *, depth: int = 0) -> Any:
    if depth > MAX_SAFE_DEPTH:
        raise FederationValidationError(
            "metadata-too-deep",
            field,
            "exceeds the maximum nesting depth",
        )
    if value is None or isinstance(value, (str, bool, int)):
        result = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise FederationValidationError(
                "invalid-number",
                field,
                "must be finite",
            )
        result = value
    elif isinstance(value, dict):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise FederationValidationError(
                "metadata-too-large",
                field,
                "contains too many keys",
            )
        result = {
            _safe_text(key, f"{field}.key"): _safe_json(
                item,
                f"{field}.{key}",
                depth=depth + 1,
            )
            for key, item in value.items()
        }
    elif isinstance(value, (list, tuple)):
        if len(value) > MAX_COLLECTION_ITEMS:
            raise FederationValidationError(
                "metadata-too-large",
                field,
                "contains too many items",
            )
        result = [
            _safe_json(item, f"{field}[{index}]", depth=depth + 1)
            for index, item in enumerate(value)
        ]
    else:
        raise FederationValidationError(
            "invalid-json",
            field,
            "must contain only JSON-compatible values",
        )
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json",
            field,
            "must contain only JSON-compatible values",
        ) from exc
    if contains_secret_material(result):
        raise FederationValidationError(
            "secret-metadata",
            field,
            "must not contain credentials or secrets",
        )
    if contains_nonpublic_location(result):
        raise FederationValidationError(
            "nonpublic-metadata",
            field,
            "must not expose private endpoints, addresses, or backend paths",
        )
    return result


def _safe_text_tuple(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise FederationValidationError(
            "invalid-list",
            field,
            "must be a list",
        )
    if len(value) > MAX_COLLECTION_ITEMS:
        raise FederationValidationError(
            "metadata-too-large",
            field,
            "contains too many items",
        )
    result = tuple(
        _safe_text(item, f"{field}[{index}]")
        for index, item in enumerate(value)
    )
    if len(set(result)) != len(result):
        raise FederationValidationError(
            "duplicate-item",
            field,
            "must not contain duplicate values",
        )
    return result


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, OnboardingModel):
        return value.to_dict()
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


@dataclass(frozen=True)
class OnboardingModel:
    SCHEMA: ClassVar[str]

    @classmethod
    def from_dict(cls, value: JSON) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object",
                cls.__name__,
                "must be an object",
            )
        schema = value.get("schema")
        if schema != cls.SCHEMA:
            raise ProtocolCompatibilityError(
                "unsupported-schema",
                "schema",
                f"expected {cls.SCHEMA}, received {schema!r}",
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
            raise FederationValidationError(
                "missing-field",
                missing[0],
                "is required",
            )
        return cls(**{name: value[name] for name in known if name in value})

    def to_dict(self) -> JSON:
        return {"schema": self.SCHEMA, **_json_value(asdict(self))}


@dataclass(frozen=True)
class FederationSessionBinding(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.federation-binding.v1"

    federation_id: str
    internal_session_id: str
    device_id: str
    state: FederationConnectionState
    revision: int
    trusted: bool
    created_at: datetime
    last_verified_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("federation_id", "internal_session_id", "device_id"):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "state",
            _enum(self.state, FederationConnectionState, "state"),
        )
        object.__setattr__(self, "revision", _uint(self.revision, "revision"))
        if not isinstance(self.trusted, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "trusted",
                "must be a boolean",
            )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(
            self,
            "last_verified_at",
            _optional_utc(self.last_verified_at, "last_verified_at"),
        )
        if (
            self.last_verified_at is not None
            and self.last_verified_at < self.created_at
        ):
            raise FederationValidationError(
                "invalid-time-order",
                "last_verified_at",
                "must not be earlier than created_at",
            )
        if self.state == FederationConnectionState.CONNECTED and not self.trusted:
            raise FederationValidationError(
                "untrusted-connected-state",
                "trusted",
                "a connected binding must already be trusted",
            )


@dataclass(frozen=True)
class FederationDiscoveryResult(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.discovery-result.v1"

    discovery_id: str
    federation_label: str
    federation_fingerprint: str
    transport_kind: str
    verification_code: str
    prior_trust: bool
    state: FederationConnectionState
    discovered_at: datetime
    expires_at: datetime
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "discovery_id",
            "federation_label",
            "federation_fingerprint",
            "transport_kind",
            "verification_code",
        ):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        if not isinstance(self.prior_trust, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "prior_trust",
                "must be a boolean",
            )
        object.__setattr__(
            self,
            "state",
            _enum(self.state, FederationConnectionState, "state"),
        )
        object.__setattr__(
            self,
            "discovered_at",
            _utc(self.discovered_at, "discovered_at"),
        )
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        object.__setattr__(
            self,
            "unavailable_reason",
            _optional_safe_text(self.unavailable_reason, "unavailable_reason"),
        )
        if self.expires_at <= self.discovered_at:
            raise FederationValidationError(
                "invalid-expiry",
                "expires_at",
                "must be later than discovered_at",
            )
        if (
            self.state == FederationConnectionState.UNAVAILABLE
            and self.unavailable_reason is None
        ):
            raise FederationValidationError(
                "missing-reason",
                "unavailable_reason",
                "is required when discovery is unavailable",
            )
        if (
            self.state != FederationConnectionState.UNAVAILABLE
            and self.unavailable_reason is not None
        ):
            raise FederationValidationError(
                "unexpected-reason",
                "unavailable_reason",
                "is only valid when discovery is unavailable",
            )


@dataclass(frozen=True)
class DeviceInspectionSnapshot(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.device-inspection.v1"

    device_id: str
    revision: int
    os_family: str
    architecture: str
    resource_observations: dict[str, Any]
    detected_services: tuple[str, ...]
    registered_handlers: tuple[str, ...]
    detected_data_sources: tuple[str, ...]
    recommended_benchmark_ids: tuple[str, ...]
    warnings: tuple[str, ...]
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in ("device_id", "os_family", "architecture"):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(self, "revision", _uint(self.revision, "revision"))
        object.__setattr__(
            self,
            "resource_observations",
            _safe_json(self.resource_observations, "resource_observations"),
        )
        for name in (
            "detected_services",
            "registered_handlers",
            "detected_data_sources",
            "recommended_benchmark_ids",
            "warnings",
        ):
            object.__setattr__(
                self,
                name,
                _safe_text_tuple(getattr(self, name), name),
            )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        if self.expires_at <= self.created_at:
            raise FederationValidationError(
                "invalid-expiry",
                "expires_at",
                "must be later than created_at",
            )


@dataclass(frozen=True)
class BenchmarkDefinition(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.benchmark-definition.v1"

    benchmark_id: str
    capability_type: str
    capability_protocol: str
    implementation_version: str
    max_duration_seconds: int
    max_parallelism: int
    prerequisites: tuple[str, ...]
    metric_names: tuple[str, ...]
    invalidation_inputs: tuple[str, ...]
    privacy_classification: str

    def __post_init__(self) -> None:
        for name in (
            "benchmark_id",
            "capability_type",
            "capability_protocol",
            "implementation_version",
            "privacy_classification",
        ):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "max_duration_seconds",
            _positive_int(self.max_duration_seconds, "max_duration_seconds"),
        )
        object.__setattr__(
            self,
            "max_parallelism",
            _positive_int(self.max_parallelism, "max_parallelism"),
        )
        for name in ("prerequisites", "metric_names", "invalidation_inputs"):
            object.__setattr__(
                self,
                name,
                _safe_text_tuple(getattr(self, name), name),
            )


@dataclass(frozen=True)
class BenchmarkResult(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.benchmark-result.v1"

    run_id: str
    device_id: str
    benchmark_id: str
    benchmark_version: str
    target_service_id: str
    state: BenchmarkState
    metrics: dict[str, Any]
    recommendation: BenchmarkRecommendation
    dependency_fingerprint: str
    diagnostics: tuple[str, ...]
    started_at: datetime
    finished_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "device_id",
            "benchmark_id",
            "benchmark_version",
            "target_service_id",
            "dependency_fingerprint",
        ):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "state",
            _enum(self.state, BenchmarkState, "state"),
        )
        object.__setattr__(
            self,
            "recommendation",
            _enum(
                self.recommendation,
                BenchmarkRecommendation,
                "recommendation",
            ),
        )
        object.__setattr__(self, "metrics", _safe_json(self.metrics, "metrics"))
        object.__setattr__(
            self,
            "diagnostics",
            _safe_text_tuple(self.diagnostics, "diagnostics"),
        )
        object.__setattr__(self, "started_at", _utc(self.started_at, "started_at"))
        object.__setattr__(
            self,
            "finished_at",
            _utc(self.finished_at, "finished_at"),
        )
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        if self.finished_at < self.started_at:
            raise FederationValidationError(
                "invalid-time-order",
                "finished_at",
                "must not be earlier than started_at",
            )
        if self.expires_at <= self.finished_at:
            raise FederationValidationError(
                "invalid-expiry",
                "expires_at",
                "must be later than finished_at",
            )
        if (
            self.state != BenchmarkState.PASSED
            and self.recommendation
            in {
                BenchmarkRecommendation.RECOMMENDED,
                BenchmarkRecommendation.USABLE,
            }
        ):
            raise FederationValidationError(
                "invalid-recommendation",
                "recommendation",
                "a non-passing result cannot recommend activation",
            )


@dataclass(frozen=True)
class ContributionCandidate(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.contribution-candidate.v1"

    candidate_id: str
    device_id: str
    capability_type: str
    capability_protocol: str
    display_label: str
    inspection_revision: int
    benchmark_run_ids: tuple[str, ...]
    capacity_envelope: dict[str, Any]
    missing_prerequisites: tuple[str, ...]
    policy_state: ContributionPolicyState
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "candidate_id",
            "device_id",
            "capability_type",
            "capability_protocol",
            "display_label",
        ):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "inspection_revision",
            _uint(self.inspection_revision, "inspection_revision"),
        )
        object.__setattr__(
            self,
            "benchmark_run_ids",
            _safe_text_tuple(self.benchmark_run_ids, "benchmark_run_ids"),
        )
        object.__setattr__(
            self,
            "capacity_envelope",
            _safe_json(self.capacity_envelope, "capacity_envelope"),
        )
        object.__setattr__(
            self,
            "missing_prerequisites",
            _safe_text_tuple(
                self.missing_prerequisites,
                "missing_prerequisites",
            ),
        )
        object.__setattr__(
            self,
            "policy_state",
            _enum(
                self.policy_state,
                ContributionPolicyState,
                "policy_state",
            ),
        )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "expires_at", _utc(self.expires_at, "expires_at"))
        if self.expires_at <= self.created_at:
            raise FederationValidationError(
                "invalid-expiry",
                "expires_at",
                "must be later than created_at",
            )
        if (
            self.policy_state == ContributionPolicyState.ALLOWED
            and self.missing_prerequisites
        ):
            raise FederationValidationError(
                "prerequisites-missing",
                "missing_prerequisites",
                "an allowed candidate cannot have missing prerequisites",
            )


@dataclass(frozen=True)
class ContributionIntent(OnboardingModel):
    SCHEMA: ClassVar[str] = "msh.onboarding.contribution-intent.v1"

    candidate_id: str
    device_id: str
    desired_state: ContributionDesiredState
    decision_revision: int
    policy_state: ContributionPolicyState
    activation_state: ContributionActivationState
    reason: str | None
    decided_at: datetime

    def __post_init__(self) -> None:
        for name in ("candidate_id", "device_id"):
            object.__setattr__(
                self,
                name,
                _safe_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "desired_state",
            _enum(
                self.desired_state,
                ContributionDesiredState,
                "desired_state",
            ),
        )
        object.__setattr__(
            self,
            "decision_revision",
            _uint(self.decision_revision, "decision_revision"),
        )
        object.__setattr__(
            self,
            "policy_state",
            _enum(
                self.policy_state,
                ContributionPolicyState,
                "policy_state",
            ),
        )
        object.__setattr__(
            self,
            "activation_state",
            _enum(
                self.activation_state,
                ContributionActivationState,
                "activation_state",
            ),
        )
        object.__setattr__(
            self,
            "reason",
            _optional_safe_text(self.reason, "reason"),
        )
        object.__setattr__(self, "decided_at", _utc(self.decided_at, "decided_at"))
        if (
            self.desired_state == ContributionDesiredState.DISABLED
            and self.activation_state
            not in {
                ContributionActivationState.INACTIVE,
                ContributionActivationState.SUSPENDED,
            }
        ):
            raise FederationValidationError(
                "invalid-activation-state",
                "activation_state",
                "a disabled contribution cannot be pending, active, or blocked",
            )
        if (
            self.activation_state == ContributionActivationState.ACTIVE
            and (
                self.desired_state != ContributionDesiredState.ENABLED
                or self.policy_state != ContributionPolicyState.ALLOWED
            )
        ):
            raise FederationValidationError(
                "invalid-active-intent",
                "activation_state",
                "an active contribution must be enabled and allowed",
            )
        if (
            self.activation_state == ContributionActivationState.BLOCKED
            and self.reason is None
        ):
            raise FederationValidationError(
                "missing-reason",
                "reason",
                "is required when activation is blocked",
            )

"""Versioned, bounded contracts for federation execution observations.

This module is pure data and validation. It performs no storage, selection,
dispatch, execution, or authority work. Every field is optional except the
identity of the executed work and whether it succeeded: a node that cannot
measure GPU memory still produces a useful observation.

Three separate descriptors travel together:

``WorkloadDescriptor``
    *what* was executed, in scheduler terms. It carries a capability type, a
    job kind and bounded scalar features. Features are deliberately open: new
    features can be added later without replacing the architecture because the
    learning key is derived from the descriptor rather than hard-coded.
``ExecutionEnvironment``
    *how* it was executed: provider, service, model, configuration, software
    and hardware identity. Its fingerprint segments learned state, so a model
    or software upgrade cannot silently reuse pre-upgrade assumptions.
``ExecutionMeasurements``
    *what it cost*: timings, outcome, resource use, throughput and quality.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Self

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
)

EFFICIENCY_PROTOCOL = "fcp-execution-efficiency"
EFFICIENCY_PROTOCOL_VERSION = "1.0"
EFFICIENCY_PROTOCOL_MAJOR = 1

WORKLOAD_DESCRIPTOR_SCHEMA = "fcp.workload-descriptor.v1"
EXECUTION_ENVIRONMENT_SCHEMA = "fcp.execution-environment.v1"
EXECUTION_MEASUREMENTS_SCHEMA = "fcp.execution-measurements.v1"
EXECUTION_OBSERVATION_SCHEMA = "fcp.execution-observation.v1"

MAX_OBSERVATION_BYTES = 16_384
MAX_TEXT_BYTES = 256
MAX_FEATURE_TEXT_BYTES = 64
MAX_FEATURES = 24
MAX_CONFIGURATION_BYTES = 2_048
#: Thirty days. A single execution longer than this is not scheduling evidence.
MAX_DURATION_MILLIS = 30 * 24 * 60 * 60 * 1_000
MAX_UNITS = 10**15
MAX_BYTES_METRIC = 1 << 50
MAX_RETRIES = 1_000
UTILIZATION_SCALE = 1_000

#: Ordered coarsening of one workload descriptor, most specific first.
WORKLOAD_CLASS_TIERS = ("exact", "family", "kind", "capability")

_SCHEMA_RE = re.compile(r"^(?P<name>fcp\.[a-z0-9.-]+)\.v(?P<major>[0-9]+)$")
_FEATURE_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")

#: Scalar types a workload feature may take. ``float`` subsumes ``int``.
FeatureValue = str | bool | float


def _text(value: Any, field_name: str, *, maximum: int = MAX_TEXT_BYTES) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field_name,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "text-too-large", field_name, f"must not exceed {maximum} UTF-8 bytes"
        )
    return value


def _optional_text(
    value: Any, field_name: str, *, maximum: int = MAX_TEXT_BYTES
) -> str | None:
    return None if value is None else _text(value, field_name, maximum=maximum)


def _bounded_int(
    value: Any, field_name: str, *, maximum: int, minimum: int = 0
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FederationValidationError(
            "invalid-integer", field_name, "must be an integer"
        )
    if value < minimum or value > maximum:
        raise FederationValidationError(
            "metric-out-of-range",
            field_name,
            f"must be between {minimum} and {maximum}",
        )
    return value


def _optional_int(
    value: Any, field_name: str, *, maximum: int, minimum: int = 0
) -> int | None:
    if value is None:
        return None
    return _bounded_int(value, field_name, maximum=maximum, minimum=minimum)


def _optional_float(
    value: Any, field_name: str, *, maximum: float, minimum: float = 0.0
) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FederationValidationError(
            "invalid-number", field_name, "must be a finite number"
        )
    number = float(value)
    if not math.isfinite(number):
        raise FederationValidationError(
            "invalid-number", field_name, "must be a finite number"
        )
    if number < minimum or number > maximum:
        raise FederationValidationError(
            "metric-out-of-range",
            field_name,
            f"must be between {minimum} and {maximum}",
        )
    return number


def _utc(value: Any, field_name: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", field_name, "must be RFC 3339"
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp", field_name, "must be timezone-aware"
        )
    if value.utcoffset().total_seconds() != 0:
        raise FederationValidationError(
            "invalid-timestamp", field_name, "must use UTC"
        )
    return value.astimezone(timezone.utc)


def stamp(value: datetime) -> str:
    """Render one UTC timestamp in the repository's wire format."""

    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def canonical_json(value: Any, field_name: str = "value") -> str:
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
            "invalid-json", field_name, "must contain JSON-compatible finite values"
        ) from exc


def _safe_object(
    value: Any, field_name: str, *, maximum: int = MAX_CONFIGURATION_BYTES
) -> dict[str, Any]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in value
    ):
        raise FederationValidationError(
            "invalid-object", field_name, "must be an object with string keys"
        )
    encoded = canonical_json(value, field_name)
    if len(encoded.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "object-too-large", field_name, f"must not exceed {maximum} UTF-8 bytes"
        )
    result = dict(value)
    if contains_secret_material(result):
        raise FederationValidationError(
            "secret-observation-field",
            field_name,
            "must not contain credentials or secret material",
        )
    if contains_nonpublic_location(result):
        raise FederationValidationError(
            "nonpublic-observation-field",
            field_name,
            "must not expose endpoints, addresses, or backend paths",
        )
    return result


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


def protocol_major(value: Any, field_name: str = "protocol_version") -> int:
    version = _text(value, field_name)
    parts = version.split(".", 1)
    if not parts[0].isdigit() or (len(parts) == 2 and not parts[1].isdigit()):
        raise ProtocolCompatibilityError(
            "invalid-protocol-version",
            field_name,
            "must be a numeric major or major.minor version",
        )
    return int(parts[0])


def validate_efficiency_protocol(value: Any) -> int:
    major = protocol_major(value)
    if major != EFFICIENCY_PROTOCOL_MAJOR:
        raise ProtocolCompatibilityError(
            "unsupported-protocol-major",
            "protocol_version",
            f"supported major is {EFFICIENCY_PROTOCOL_MAJOR}, received {major}",
        )
    return major


def fingerprint(value: Any, *, prefix: str) -> str:
    """Return a short stable fingerprint for one canonical JSON value."""

    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}:{digest[:16]}"


def requirements_fingerprint(requirements: Any) -> str:
    """Fingerprint the requested capability requirements of one job."""

    return fingerprint(_safe_object(requirements, "requirements"), prefix="req")


def _feature_value(value: Any, field_name: str) -> FeatureValue:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return _bounded_int(
            value, field_name, maximum=MAX_UNITS, minimum=-MAX_UNITS
        )
    if isinstance(value, float):
        if not math.isfinite(value):
            raise FederationValidationError(
                "invalid-number", field_name, "must be a finite number"
            )
        if abs(value) > MAX_UNITS:
            raise FederationValidationError(
                "metric-out-of-range", field_name, "is out of range"
            )
        return float(value)
    if isinstance(value, str):
        return _text(value, field_name, maximum=MAX_FEATURE_TEXT_BYTES)
    raise FederationValidationError(
        "invalid-feature",
        field_name,
        "must be text, boolean, or a finite number",
    )


def _bucket(value: float) -> str:
    """Bucket one magnitude on a log2 scale.

    Workload size rarely matters exactly; order of magnitude almost always
    does. A 4 000-token prompt and a 4 400-token prompt land in one class,
    while a 40-token prompt does not.
    """

    magnitude = abs(float(value))
    if magnitude < 1e-9:
        return "0"
    exponent = math.floor(math.log2(magnitude))
    sign = "-" if value < 0 else ""
    return f"{sign}2^{exponent}"


def _feature_token(value: FeatureValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return _bucket(value)
    return value


@dataclass(frozen=True)
class WorkloadDescriptor:
    """A bounded, extensible description of one unit of federation work."""

    SCHEMA: ClassVar[str] = WORKLOAD_DESCRIPTOR_SCHEMA

    capability_type: str
    job_kind: str
    features: dict[str, FeatureValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("capability_type", "job_kind"):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        features = self.features
        if not isinstance(features, dict):
            raise FederationValidationError(
                "invalid-object", "features", "must be an object"
            )
        if len(features) > MAX_FEATURES:
            raise FederationValidationError(
                "too-many-features",
                "features",
                f"must not exceed {MAX_FEATURES} entries",
            )
        validated: dict[str, FeatureValue] = {}
        for key in sorted(features):
            if not isinstance(key, str) or _FEATURE_KEY_RE.fullmatch(key) is None:
                raise FederationValidationError(
                    "invalid-feature-name",
                    "features",
                    "feature names must be lowercase snake_case identifiers",
                )
            validated[key] = _feature_value(features[key], f"features.{key}")
        _safe_object(validated, "features")
        object.__setattr__(self, "features", validated)

    @property
    def categorical_features(self) -> dict[str, str | bool]:
        return {
            key: value
            for key, value in self.features.items()
            if isinstance(value, (str, bool))
        }

    def class_key(self, tier: str = "exact") -> str:
        """Return the learning key for one coarsening tier of this workload."""

        if tier not in WORKLOAD_CLASS_TIERS:
            raise FederationValidationError(
                "invalid-workload-tier", "tier", "unknown workload class tier"
            )
        if tier == "capability":
            return self.capability_type
        if tier == "kind":
            return f"{self.capability_type}/{self.job_kind}"
        source = (
            self.features if tier == "exact" else self.categorical_features
        )
        tokens = ",".join(
            f"{key}={_feature_token(source[key])}" for key in sorted(source)
        )
        # The key describes the features, not the tier that produced it, so a
        # workload with only categorical features collapses "exact" and
        # "family" into one profile instead of two identical ones.
        return f"{self.capability_type}/{self.job_kind}/[{tokens}]"

    def class_keys(self) -> tuple[tuple[str, str], ...]:
        """Return ``(tier, class_key)`` from most to least specific.

        Duplicate keys are collapsed so a featureless workload does not
        consume four identical profile rows.
        """

        seen: dict[str, str] = {}
        for tier in WORKLOAD_CLASS_TIERS:
            key = self.class_key(tier)
            if key not in seen:
                seen[key] = tier
        return tuple((tier, key) for key, tier in seen.items())

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "workload", "must be an object"
            )
        names = ("schema", "capability_type", "job_kind")
        _required(value, names)
        _schema(value["schema"], cls.SCHEMA)
        return cls(
            capability_type=value["capability_type"],
            job_kind=value["job_kind"],
            features=value.get("features") or {},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "capability_type": self.capability_type,
            "job_kind": self.job_kind,
            "features": dict(self.features),
        }


@dataclass(frozen=True)
class ExecutionEnvironment:
    """Provider, model, configuration, software and hardware identity.

    Every field is optional. The derived :attr:`fingerprint` segments learned
    state: when a node upgrades its model or runtime, observations recorded
    before the upgrade keep their own identity instead of being folded into
    the new one.
    """

    SCHEMA: ClassVar[str] = EXECUTION_ENVIRONMENT_SCHEMA

    provider_kind: str | None = None
    service_id: str | None = None
    model_id: str | None = None
    software_version: str | None = None
    runtime_version: str | None = None
    hardware_class: str | None = None
    accelerator: str | None = None
    os_family: str | None = None
    architecture: str | None = None
    configuration: dict[str, Any] = field(default_factory=dict)

    _TEXT_FIELDS: ClassVar[tuple[str, ...]] = (
        "provider_kind",
        "service_id",
        "model_id",
        "software_version",
        "runtime_version",
        "hardware_class",
        "accelerator",
        "os_family",
        "architecture",
    )

    def __post_init__(self) -> None:
        for name in self._TEXT_FIELDS:
            object.__setattr__(
                self, name, _optional_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "configuration", _safe_object(self.configuration or {}, "configuration")
        )

    @property
    def identity(self) -> dict[str, Any]:
        values: dict[str, Any] = {
            name: getattr(self, name) for name in self._TEXT_FIELDS
        }
        values["configuration"] = dict(self.configuration)
        return values

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.identity, prefix="env")

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "environment", "must be an object"
            )
        _required(value, ("schema",))
        _schema(value["schema"], cls.SCHEMA)
        return cls(
            **{name: value.get(name) for name in cls._TEXT_FIELDS},
            configuration=value.get("configuration") or {},
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"schema": self.SCHEMA}
        payload.update({name: getattr(self, name) for name in self._TEXT_FIELDS})
        payload["configuration"] = dict(self.configuration)
        return payload


@dataclass(frozen=True)
class ExecutionMeasurements:
    """What one execution actually cost. Only ``succeeded`` is required."""

    SCHEMA: ClassVar[str] = EXECUTION_MEASUREMENTS_SCHEMA

    succeeded: bool
    error_code: str | None = None
    retries: int = 0
    queue_millis: int | None = None
    execution_millis: int | None = None
    total_millis: int | None = None
    cpu_utilization_millis: int | None = None
    gpu_utilization_millis: int | None = None
    memory_peak_bytes: int | None = None
    vram_peak_bytes: int | None = None
    input_units: int | None = None
    output_units: int | None = None
    throughput_units_per_second: float | None = None
    quality_score: float | None = None
    energy_millijoules: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.succeeded, bool):
            raise FederationValidationError(
                "invalid-boolean", "succeeded", "must be a boolean"
            )
        object.__setattr__(
            self, "error_code", _optional_text(self.error_code, "error_code")
        )
        if self.succeeded and self.error_code is not None:
            raise FederationValidationError(
                "unexpected-error-code",
                "error_code",
                "a successful execution has no error code",
            )
        object.__setattr__(
            self,
            "retries",
            _bounded_int(self.retries, "retries", maximum=MAX_RETRIES),
        )
        for name in ("queue_millis", "execution_millis", "total_millis"):
            object.__setattr__(
                self,
                name,
                _optional_int(
                    getattr(self, name), name, maximum=MAX_DURATION_MILLIS
                ),
            )
        for name in ("cpu_utilization_millis", "gpu_utilization_millis"):
            object.__setattr__(
                self,
                name,
                _optional_int(
                    getattr(self, name), name, maximum=UTILIZATION_SCALE
                ),
            )
        for name in ("memory_peak_bytes", "vram_peak_bytes"):
            object.__setattr__(
                self,
                name,
                _optional_int(getattr(self, name), name, maximum=MAX_BYTES_METRIC),
            )
        for name in ("input_units", "output_units", "energy_millijoules"):
            object.__setattr__(
                self,
                name,
                _optional_int(getattr(self, name), name, maximum=MAX_UNITS),
            )
        object.__setattr__(
            self,
            "throughput_units_per_second",
            _optional_float(
                self.throughput_units_per_second,
                "throughput_units_per_second",
                maximum=float(MAX_UNITS),
            ),
        )
        object.__setattr__(
            self,
            "quality_score",
            _optional_float(self.quality_score, "quality_score", maximum=1.0),
        )
        total = self.total_millis
        if total is not None:
            for name in ("queue_millis", "execution_millis"):
                part = getattr(self, name)
                if part is not None and part > total:
                    raise FederationValidationError(
                        "inconsistent-duration",
                        name,
                        "must not exceed total_millis",
                    )

    @property
    def completion_millis(self) -> int | None:
        """Best available end-to-end cost, derived when not reported."""

        if self.total_millis is not None:
            return self.total_millis
        if self.execution_millis is None:
            return None
        return self.execution_millis + (self.queue_millis or 0)

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "measurements", "must be an object"
            )
        _required(value, ("schema", "succeeded"))
        _schema(value["schema"], cls.SCHEMA)
        return cls(
            succeeded=value["succeeded"],
            retries=value.get("retries") or 0,
            **{
                name: value.get(name)
                for name in MEASUREMENT_FIELDS
                if name not in {"succeeded", "retries"}
            },
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"schema": self.SCHEMA}
        for name in MEASUREMENT_FIELDS:
            payload[name] = getattr(self, name)
        return payload


#: Every measurement field, in wire order. Listed explicitly so the schema is
#: readable and so adding a field is a deliberate, versioned act.
MEASUREMENT_FIELDS = (
    "succeeded",
    "error_code",
    "retries",
    "queue_millis",
    "execution_millis",
    "total_millis",
    "cpu_utilization_millis",
    "gpu_utilization_millis",
    "memory_peak_bytes",
    "vram_peak_bytes",
    "input_units",
    "output_units",
    "throughput_units_per_second",
    "quality_score",
    "energy_millijoules",
)


@dataclass(frozen=True)
class ExecutionObservation:
    """One completed federation execution, as scheduling evidence."""

    SCHEMA: ClassVar[str] = EXECUTION_OBSERVATION_SCHEMA

    observation_id: str
    session_id: str
    job_id: str
    attempt_id: str
    node_id: str
    capability_id: str
    workload: WorkloadDescriptor
    environment: ExecutionEnvironment
    measurements: ExecutionMeasurements
    observed_at: datetime
    requested_protocol: str | None = None
    requested_protocol_version: str | None = None
    requirements_fingerprint: str | None = None
    protocol_version: str = EFFICIENCY_PROTOCOL_VERSION

    def __post_init__(self) -> None:
        for name in (
            "observation_id",
            "session_id",
            "job_id",
            "attempt_id",
            "node_id",
            "capability_id",
        ):
            object.__setattr__(self, name, _text(getattr(self, name), name))
        validate_efficiency_protocol(self.protocol_version)
        if not isinstance(self.workload, WorkloadDescriptor):
            object.__setattr__(
                self, "workload", WorkloadDescriptor.from_dict(self.workload)
            )
        if not isinstance(self.environment, ExecutionEnvironment):
            object.__setattr__(
                self,
                "environment",
                ExecutionEnvironment.from_dict(self.environment),
            )
        if not isinstance(self.measurements, ExecutionMeasurements):
            object.__setattr__(
                self,
                "measurements",
                ExecutionMeasurements.from_dict(self.measurements),
            )
        for name in ("requested_protocol", "requested_protocol_version"):
            object.__setattr__(
                self, name, _optional_text(getattr(self, name), name)
            )
        if self.requested_protocol_version is not None:
            protocol_major(
                self.requested_protocol_version, "requested_protocol_version"
            )
        object.__setattr__(
            self,
            "requirements_fingerprint",
            _optional_text(self.requirements_fingerprint, "requirements_fingerprint"),
        )
        object.__setattr__(self, "observed_at", _utc(self.observed_at, "observed_at"))

    @property
    def capability_type(self) -> str:
        return self.workload.capability_type

    @property
    def environment_fingerprint(self) -> str:
        return self.environment.fingerprint

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-object", "observation", "must be an object"
            )
        names = (
            "schema",
            "protocol_version",
            "observation_id",
            "session_id",
            "job_id",
            "attempt_id",
            "node_id",
            "capability_id",
            "workload",
            "environment",
            "measurements",
            "observed_at",
        )
        _required(value, names)
        _schema(value["schema"], cls.SCHEMA)
        return cls(
            observation_id=value["observation_id"],
            session_id=value["session_id"],
            job_id=value["job_id"],
            attempt_id=value["attempt_id"],
            node_id=value["node_id"],
            capability_id=value["capability_id"],
            workload=WorkloadDescriptor.from_dict(value["workload"]),
            environment=ExecutionEnvironment.from_dict(value["environment"]),
            measurements=ExecutionMeasurements.from_dict(value["measurements"]),
            observed_at=value["observed_at"],
            requested_protocol=value.get("requested_protocol"),
            requested_protocol_version=value.get("requested_protocol_version"),
            requirements_fingerprint=value.get("requirements_fingerprint"),
            protocol_version=value["protocol_version"],
        )

    @classmethod
    def from_json(
        cls, value: str | bytes, *, max_bytes: int = MAX_OBSERVATION_BYTES
    ) -> Self:
        encoded = value.encode("utf-8") if isinstance(value, str) else value
        if not isinstance(encoded, bytes):
            raise FederationValidationError(
                "malformed-message", "observation", "must be text or UTF-8 bytes"
            )
        if len(encoded) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "observation", f"exceeds {max_bytes} bytes"
            )
        try:
            decoded = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FederationValidationError(
                "malformed-message", "observation", "must be valid UTF-8 JSON"
            ) from exc
        return cls.from_dict(decoded)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "protocol_version": self.protocol_version,
            "observation_id": self.observation_id,
            "session_id": self.session_id,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "node_id": self.node_id,
            "capability_id": self.capability_id,
            "workload": self.workload.to_dict(),
            "environment": self.environment.to_dict(),
            "measurements": self.measurements.to_dict(),
            "observed_at": stamp(self.observed_at),
            "requested_protocol": self.requested_protocol,
            "requested_protocol_version": self.requested_protocol_version,
            "requirements_fingerprint": self.requirements_fingerprint,
        }

    def to_json(self, *, max_bytes: int = MAX_OBSERVATION_BYTES) -> str:
        encoded = canonical_json(self.to_dict(), "observation")
        if len(encoded.encode("utf-8")) > max_bytes:
            raise FederationValidationError(
                "message-too-large", "observation", f"exceeds {max_bytes} bytes"
            )
        return encoded


__all__ = [
    "EFFICIENCY_PROTOCOL",
    "EFFICIENCY_PROTOCOL_MAJOR",
    "EFFICIENCY_PROTOCOL_VERSION",
    "EXECUTION_ENVIRONMENT_SCHEMA",
    "EXECUTION_MEASUREMENTS_SCHEMA",
    "EXECUTION_OBSERVATION_SCHEMA",
    "MAX_OBSERVATION_BYTES",
    "MEASUREMENT_FIELDS",
    "WORKLOAD_CLASS_TIERS",
    "WORKLOAD_DESCRIPTOR_SCHEMA",
    "ExecutionEnvironment",
    "ExecutionMeasurements",
    "ExecutionObservation",
    "WorkloadDescriptor",
    "canonical_json",
    "fingerprint",
    "protocol_major",
    "requirements_fingerprint",
    "stamp",
    "validate_efficiency_protocol",
]

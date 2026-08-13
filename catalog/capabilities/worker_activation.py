"""Trusted F8.2-backed activation of explicit local compute handlers.

F8.4 never discovers or transfers executable code. A current provider report may
name only a logical handler descriptor that was already registered locally by the
application. Every dispatch is revalidated immediately before the local handler
object is called; F7.3 ownership, F7.4 dispatch and duplicate suppression, F7.5
lifecycle, and F7.6 artifact authority remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Protocol, Self

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.redaction import redact_secrets

from .dispatch import (
    CapabilityHandler,
    CapabilityWorker,
    DispatchRequest,
    DispatchResponse,
    ExecutionResult,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from .jobs import JobContract, protocol_major
from .provider_health import (
    FederatedProviderHealthService,
    ProviderHealthRecord,
    ProviderHealthState,
)
from .provider_reports import ProviderStatus

COMPUTE_HANDLER_DESCRIPTOR_SCHEMA = "fcp.compute-handler-descriptor.v1"
COMPUTE_HANDLER_ACTIVATION_SCHEMA = "fcp.compute-handler-activation.v1"
COMPUTE_HANDLER_ACTIVATION_KIND = "local-handler"
MAX_LOCAL_COMPUTE_HANDLERS = 128
MAX_COMPUTE_HANDLER_ATTRIBUTES_BYTES = 32_768
MAX_HANDLER_TEXT_BYTES = 256

_LOGICAL_ID = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_RESERVED_ACTIVATION_ATTRIBUTE = "activation"
_FORBIDDEN_KEYS = frozenset(
    {
        "address",
        "api_key",
        "backend_path",
        "base_url",
        "binary",
        "bytecode",
        "code",
        "command",
        "container",
        "container_image",
        "credential",
        "credentials",
        "endpoint",
        "env",
        "environment",
        "executable",
        "executable_path",
        "image",
        "import",
        "module",
        "module_path",
        "package",
        "package_name",
        "password",
        "path",
        "private_address",
        "process",
        "script",
        "secret",
        "shell",
        "source",
        "token",
        "url",
    }
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: Any, field: str, *, maximum: int = MAX_HANDLER_TEXT_BYTES) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-compute-handler-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "compute-handler-text-too-large",
            field,
            f"must not exceed {maximum} UTF-8 bytes",
        )
    return value


def _logical_id(value: Any, field: str) -> str:
    value = _text(value, field)
    if _LOGICAL_ID.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-compute-handler-id",
            field,
            "must be lowercase logical text separated by single hyphens",
        )
    return value


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FederationValidationError(
            "invalid-positive-integer",
            field,
            "must be a positive integer",
        )
    return value


def _non_negative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _canonical(value: Any, field: str = "value") -> str:
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
            "invalid-compute-handler-json",
            field,
            "must contain JSON-compatible finite values",
        ) from exc


def _hash(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _schema(value: Any, expected: str) -> None:
    actual = _text(value, "schema")
    if ".v" not in actual:
        raise ProtocolCompatibilityError(
            "unsupported-compute-handler-schema",
            "schema",
            f"expected {expected}",
        )
    name, major = actual.rsplit(".v", 1)
    expected_name, expected_major = expected.rsplit(".v", 1)
    if name != expected_name or not major.isdigit():
        raise ProtocolCompatibilityError(
            "unsupported-compute-handler-schema",
            "schema",
            f"expected {expected}, received {actual!r}",
        )
    if int(major) != int(expected_major):
        raise ProtocolCompatibilityError(
            "unsupported-compute-handler-schema-major",
            "schema",
            f"supported major is {expected_major}, received {major}",
        )


def _safe_key(value: Any, field: str) -> str:
    key = _text(value, field, maximum=128)
    normalized = key.casefold().replace("-", "_")
    if normalized in _FORBIDDEN_KEYS:
        raise FederationValidationError(
            "unsafe-compute-handler-metadata",
            field,
            "executable, location, secret, or process metadata is forbidden",
        )
    return key


def _validate_safe_value(value: Any, field: str) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not (-float("inf") < value < float("inf")):
            raise FederationValidationError(
                "invalid-compute-handler-json",
                field,
                "must contain finite numeric values",
            )
        return value
    if isinstance(value, str):
        text = _text(value, field, maximum=2_048)
        if redact_secrets(text) != text:
            raise FederationValidationError(
                "unsafe-compute-handler-metadata",
                field,
                "must not contain credentials, private locations, or backend paths",
            )
        return text
    if isinstance(value, (list, tuple)):
        return [
            _validate_safe_value(item, f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = _safe_key(raw_key, f"{field}.key")
            result[key] = _validate_safe_value(item, f"{field}.{key}")
        return result
    raise FederationValidationError(
        "invalid-compute-handler-json",
        field,
        "must contain JSON-compatible values only",
    )


def _safe_attributes(value: Any, field: str = "attributes") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise FederationValidationError(
            "invalid-compute-handler-attributes",
            field,
            "must be an object",
        )
    attributes = _validate_safe_value(value, field)
    if not isinstance(attributes, dict):  # pragma: no cover - validated above
        raise FederationValidationError(
            "invalid-compute-handler-attributes",
            field,
            "must be an object",
        )
    if _RESERVED_ACTIVATION_ATTRIBUTE in attributes:
        raise FederationValidationError(
            "reserved-compute-handler-attribute",
            f"{field}.{_RESERVED_ACTIVATION_ATTRIBUTE}",
            "is reserved for F8 activation binding",
        )
    encoded = _canonical(attributes, field).encode("utf-8")
    if len(encoded) > MAX_COMPUTE_HANDLER_ATTRIBUTES_BYTES:
        raise FederationValidationError(
            "compute-handler-attributes-too-large",
            field,
            f"must not exceed {MAX_COMPUTE_HANDLER_ATTRIBUTES_BYTES} UTF-8 bytes",
        )
    return attributes


def _requirements_satisfied(required: Any, offered: Any) -> bool:
    if isinstance(required, dict):
        return isinstance(offered, dict) and all(
            key in offered and _requirements_satisfied(item, offered[key])
            for key, item in required.items()
        )
    if isinstance(required, list):
        return isinstance(offered, list) and all(item in offered for item in required)
    return required == offered


@dataclass(frozen=True, init=False)
class LocalComputeHandlerDescriptor:
    """Immutable safe metadata for one preinstalled local handler object."""

    SCHEMA: ClassVar[str] = COMPUTE_HANDLER_DESCRIPTOR_SCHEMA

    handler_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    _attributes_json: str
    descriptor_fingerprint: str

    def __init__(
        self,
        handler_id: str,
        capability_type: str,
        protocol: str,
        protocol_version: str,
        attributes: dict[str, Any],
    ) -> None:
        handler_id = _logical_id(handler_id, "handler_id")
        capability_type = _logical_id(capability_type, "capability_type")
        protocol = _logical_id(protocol, "protocol")
        protocol_version = _text(protocol_version, "protocol_version", maximum=32)
        protocol_major(protocol_version)
        safe_attributes = _safe_attributes(attributes)
        attributes_json = _canonical(safe_attributes, "attributes")
        identity = {
            "schema": self.SCHEMA,
            "handler_id": handler_id,
            "capability_type": capability_type,
            "protocol": protocol,
            "protocol_version": protocol_version,
            "attributes": safe_attributes,
        }
        object.__setattr__(self, "handler_id", handler_id)
        object.__setattr__(self, "capability_type", capability_type)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "protocol_version", protocol_version)
        object.__setattr__(self, "_attributes_json", attributes_json)
        object.__setattr__(self, "descriptor_fingerprint", _hash(_canonical(identity)))

    @property
    def attributes(self) -> dict[str, Any]:
        return json.loads(self._attributes_json)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "handler_id": self.handler_id,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "attributes": self.attributes,
            "descriptor_fingerprint": self.descriptor_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-compute-handler-descriptor",
                "descriptor",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        names = (
            "handler_id",
            "capability_type",
            "protocol",
            "protocol_version",
            "attributes",
            "descriptor_fingerprint",
        )
        for name in names:
            if name not in value:
                raise FederationValidationError("missing-field", name, "is required")
        descriptor = cls(
            handler_id=value["handler_id"],
            capability_type=value["capability_type"],
            protocol=value["protocol"],
            protocol_version=value["protocol_version"],
            attributes=value["attributes"],
        )
        fingerprint = _text(value["descriptor_fingerprint"], "descriptor_fingerprint")
        if _FINGERPRINT.fullmatch(fingerprint) is None:
            raise FederationValidationError(
                "invalid-compute-handler-fingerprint",
                "descriptor_fingerprint",
                "must be a lowercase SHA-256 identifier",
            )
        if descriptor.descriptor_fingerprint != fingerprint:
            raise FederationValidationError(
                "compute-handler-fingerprint-mismatch",
                "descriptor_fingerprint",
                "does not match the canonical descriptor",
            )
        return descriptor


@dataclass(frozen=True)
class ComputeHandlerActivationReference:
    """Safe report metadata selecting one preinstalled logical handler."""

    SCHEMA: ClassVar[str] = COMPUTE_HANDLER_ACTIVATION_SCHEMA

    handler_id: str
    descriptor_fingerprint: str
    kind: str = COMPUTE_HANDLER_ACTIVATION_KIND

    def __post_init__(self) -> None:
        object.__setattr__(self, "handler_id", _logical_id(self.handler_id, "handler_id"))
        if self.kind != COMPUTE_HANDLER_ACTIVATION_KIND:
            raise FederationValidationError(
                "unsupported-compute-activation-kind",
                "kind",
                f"must be {COMPUTE_HANDLER_ACTIVATION_KIND}",
            )
        fingerprint = _text(self.descriptor_fingerprint, "descriptor_fingerprint")
        if _FINGERPRINT.fullmatch(fingerprint) is None:
            raise FederationValidationError(
                "invalid-compute-handler-fingerprint",
                "descriptor_fingerprint",
                "must be a lowercase SHA-256 identifier",
            )
        object.__setattr__(self, "descriptor_fingerprint", fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "kind": self.kind,
            "handler_id": self.handler_id,
            "descriptor_fingerprint": self.descriptor_fingerprint,
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-compute-activation-reference",
                "activation",
                "must be an object",
            )
        _schema(value.get("schema"), cls.SCHEMA)
        return cls(
            handler_id=value.get("handler_id"),
            descriptor_fingerprint=value.get("descriptor_fingerprint"),
            kind=value.get("kind"),
        )


@dataclass(frozen=True)
class LocalComputeHandlerBinding:
    """One explicit local descriptor/handler association and inventory revision."""

    binding_id: str
    revision: int
    descriptor: LocalComputeHandlerDescriptor
    handler: CapabilityHandler

    def __post_init__(self) -> None:
        object.__setattr__(self, "binding_id", _text(self.binding_id, "binding_id"))
        object.__setattr__(self, "revision", _positive(self.revision, "revision"))
        if not isinstance(self.descriptor, LocalComputeHandlerDescriptor):
            raise FederationValidationError(
                "invalid-compute-handler-descriptor",
                "descriptor",
                "must be a LocalComputeHandlerDescriptor",
            )
        if not callable(getattr(self.handler, "execute", None)):
            raise FederationValidationError(
                "invalid-compute-handler",
                "handler",
                "must provide an async execute method",
            )


class LocalComputeHandlerInventory:
    """Bounded explicit local inventory with no discovery or dynamic imports."""

    def __init__(self, *, maximum_handlers: int = MAX_LOCAL_COMPUTE_HANDLERS) -> None:
        if (
            isinstance(maximum_handlers, bool)
            or not isinstance(maximum_handlers, int)
            or not 1 <= maximum_handlers <= MAX_LOCAL_COMPUTE_HANDLERS
        ):
            raise FederationValidationError(
                "invalid-compute-handler-limit",
                "maximum_handlers",
                f"must be between 1 and {MAX_LOCAL_COMPUTE_HANDLERS}",
            )
        self.maximum_handlers = maximum_handlers
        self._lock = threading.RLock()
        self._bindings: dict[str, LocalComputeHandlerBinding] = {}
        self._revisions: dict[str, int] = {}

    def register(
        self,
        descriptor: LocalComputeHandlerDescriptor,
        handler: CapabilityHandler,
    ) -> LocalComputeHandlerBinding:
        if not isinstance(descriptor, LocalComputeHandlerDescriptor):
            raise FederationValidationError(
                "invalid-compute-handler-descriptor",
                "descriptor",
                "must be a LocalComputeHandlerDescriptor",
            )
        if not callable(getattr(handler, "execute", None)):
            raise FederationValidationError(
                "invalid-compute-handler",
                "handler",
                "must provide an async execute method",
            )
        with self._lock:
            current = self._bindings.get(descriptor.handler_id)
            if current is not None:
                if (
                    current.descriptor == descriptor
                    and current.handler is handler
                ):
                    return current
                raise FederationValidationError(
                    "compute-handler-id-conflict",
                    "handler_id",
                    "handler ID is already registered with another binding",
                )
            if len(self._bindings) >= self.maximum_handlers:
                raise FederationValidationError(
                    "too-many-local-compute-handlers",
                    "inventory",
                    "bounded local handler inventory is full",
                )
            revision = self._revisions.get(descriptor.handler_id, 0) + 1
            binding = LocalComputeHandlerBinding(
                binding_id=f"chb-{uuid.uuid4().hex}",
                revision=revision,
                descriptor=descriptor,
                handler=handler,
            )
            self._bindings[descriptor.handler_id] = binding
            self._revisions[descriptor.handler_id] = revision
            return binding

    def remove(
        self,
        handler_id: str,
        *,
        expected_fingerprint: str | None = None,
    ) -> bool:
        handler_id = _logical_id(handler_id, "handler_id")
        with self._lock:
            current = self._bindings.get(handler_id)
            if current is None:
                return False
            if (
                expected_fingerprint is not None
                and current.descriptor.descriptor_fingerprint
                != expected_fingerprint
            ):
                raise FederationValidationError(
                    "compute-handler-binding-changed",
                    "descriptor_fingerprint",
                    "current local descriptor differs from the expected binding",
                )
            self._bindings.pop(handler_id)
            return True

    def get(self, handler_id: str) -> LocalComputeHandlerBinding | None:
        handler_id = _logical_id(handler_id, "handler_id")
        with self._lock:
            return self._bindings.get(handler_id)

    def list_bindings(self) -> tuple[LocalComputeHandlerBinding, ...]:
        with self._lock:
            return tuple(
                self._bindings[key] for key in sorted(self._bindings)
            )


@dataclass(frozen=True)
class ComputeWorkerActivationSnapshot:
    """Exact immutable authority evidence for one activated F7.4 worker."""

    record: ProviderHealthRecord
    reference: ComputeHandlerActivationReference
    binding_id: str
    binding_revision: int
    descriptor: LocalComputeHandlerDescriptor

    @property
    def provider_id(self) -> str:
        return self.record.capability_id

    @property
    def node_id(self) -> str:
        return self.record.node_id

    @property
    def session_id(self) -> str:
        return self.record.session_id

    @property
    def provider_generation(self) -> int:
        return self.record.provider_generation

    @property
    def report_revision(self) -> int:
        return self.record.report_revision


class ComputeWorkerActivationAuthority:
    """Revalidate F8.1, F8.2, report, node, and local inventory bindings."""

    def __init__(
        self,
        health: FederatedProviderHealthService,
        inventory: LocalComputeHandlerInventory,
        *,
        session_id: str,
        local_node_id: str,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(health, FederatedProviderHealthService):
            raise FederationValidationError(
                "invalid-health-service",
                "health",
                "must be a FederatedProviderHealthService",
            )
        if not isinstance(inventory, LocalComputeHandlerInventory):
            raise FederationValidationError(
                "invalid-compute-handler-inventory",
                "inventory",
                "must be a LocalComputeHandlerInventory",
            )
        self.health = health
        self.inventory = inventory
        self.session_id = _text(session_id, "session_id")
        self.local_node_id = _text(local_node_id, "local_node_id")
        self._clock = clock

    def now(self) -> datetime:
        value = self._clock()
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset().total_seconds() != 0
        ):
            raise FederationValidationError(
                "invalid-compute-activation-clock",
                "clock",
                "must return a UTC timestamp",
            )
        return value.astimezone(timezone.utc)

    @staticmethod
    def _reference(record: ProviderHealthRecord) -> ComputeHandlerActivationReference:
        activation = record.report.attributes.get(_RESERVED_ACTIVATION_ATTRIBUTE)
        try:
            return ComputeHandlerActivationReference.from_dict(activation)
        except (FederationValidationError, ProtocolCompatibilityError) as exc:
            raise FederationOperationError(
                "compute-activation-metadata-invalid",
                "current provider health lacks a valid local handler reference",
                "attributes.activation",
            ) from exc

    @staticmethod
    def _offered_attributes(record: ProviderHealthRecord) -> dict[str, Any]:
        attributes = dict(record.report.attributes)
        attributes.pop(_RESERVED_ACTIVATION_ATTRIBUTE, None)
        return _safe_attributes(attributes, "report.attributes")

    def current_snapshot(
        self,
        provider_id: str,
        *,
        provider_generation: int | None = None,
        report_revision: int | None = None,
        expected_binding_id: str | None = None,
    ) -> ComputeWorkerActivationSnapshot:
        provider_id = _text(provider_id, "provider_id")
        if provider_generation is not None:
            provider_generation = _positive(
                provider_generation,
                "provider_generation",
            )
        if report_revision is not None:
            report_revision = _non_negative(report_revision, "report_revision")
        observation = self.health.observe(
            session_id=self.session_id,
            capability_id=provider_id,
            actor_node_id=self.local_node_id,
            provider_generation=provider_generation,
        )
        if observation.state is not ProviderHealthState.CURRENT:
            raise FederationOperationError(
                "compute-provider-not-current",
                f"provider health is {observation.reason_code}",
                "provider_id",
            )
        record = self.health.store.get(
            session_id=self.session_id,
            capability_id=provider_id,
        )
        if record is None:
            raise FederationOperationError(
                "compute-provider-health-missing",
                "current provider health record is unavailable",
                "provider_id",
            )
        report = record.report
        if record.node_id != self.local_node_id:
            raise FederationOperationError(
                "compute-provider-not-local",
                "only the provider's authenticated local node may activate it",
                "node_id",
            )
        if provider_generation is not None and (
            record.provider_generation != provider_generation
        ):
            raise FederationOperationError(
                "compute-provider-generation-mismatch",
                "activation generation is no longer current",
                "provider_generation",
            )
        if report_revision is not None and record.report_revision != report_revision:
            raise FederationOperationError(
                "compute-provider-report-revision-mismatch",
                "activation report revision is no longer current",
                "report_revision",
            )
        if report.status is not ProviderStatus.READY or not record.is_fresh_at(
            self.now()
        ):
            raise FederationOperationError(
                "compute-provider-health-expired",
                "provider is not ready with current health",
                "expires_at",
            )
        if report.capability_type == "language-model":
            raise FederationOperationError(
                "compute-provider-type-invalid",
                "language-model providers are activated by F8.3",
                "capability_type",
            )
        reference = self._reference(record)
        binding = self.inventory.get(reference.handler_id)
        if binding is None:
            raise FederationOperationError(
                "compute-handler-not-installed",
                "referenced logical handler is not installed locally",
                "handler_id",
            )
        descriptor = binding.descriptor
        identity = (
            descriptor.capability_type == report.capability_type
            and descriptor.protocol == report.protocol
            and protocol_major(descriptor.protocol_version)
            == protocol_major(report.protocol_version)
        )
        if not identity:
            raise FederationOperationError(
                "compute-handler-identity-mismatch",
                "local descriptor differs from current provider health identity",
                "descriptor",
            )
        if descriptor.descriptor_fingerprint != reference.descriptor_fingerprint:
            raise FederationOperationError(
                "compute-handler-fingerprint-mismatch",
                "current local descriptor differs from the advertised binding",
                "descriptor_fingerprint",
            )
        if not _requirements_satisfied(
            self._offered_attributes(record),
            descriptor.attributes,
        ):
            raise FederationOperationError(
                "compute-handler-attributes-mismatch",
                "local descriptor does not implement current provider attributes",
                "attributes",
            )
        if expected_binding_id is not None and binding.binding_id != expected_binding_id:
            raise FederationOperationError(
                "compute-handler-binding-replaced",
                "local handler binding was removed or replaced",
                "handler_id",
            )
        return ComputeWorkerActivationSnapshot(
            record=record,
            reference=reference,
            binding_id=binding.binding_id,
            binding_revision=binding.revision,
            descriptor=descriptor,
        )


class _ActivationFencedHandler:
    """Invoke only the exact handler object that remains current at call time."""

    def __init__(
        self,
        authority: ComputeWorkerActivationAuthority,
        snapshot: ComputeWorkerActivationSnapshot,
    ) -> None:
        self.authority = authority
        self.snapshot = snapshot

    async def execute(self, job: JobContract) -> ExecutionResult:
        try:
            current = self.authority.current_snapshot(
                self.snapshot.provider_id,
                provider_generation=self.snapshot.provider_generation,
                report_revision=self.snapshot.report_revision,
                expected_binding_id=self.snapshot.binding_id,
            )
        except (FederationOperationError, FederationValidationError):
            return ExecutionResult(False, {}, "compute-activation-not-current")
        if (
            job.session_id != current.session_id
            or job.capability.capability_type
            != current.descriptor.capability_type
            or job.capability.protocol != current.descriptor.protocol
            or protocol_major(job.capability.protocol_version)
            != protocol_major(current.descriptor.protocol_version)
            or not _requirements_satisfied(
                job.capability.requirements,
                current.descriptor.attributes,
            )
        ):
            return ExecutionResult(False, {}, "compute-handler-job-mismatch")
        binding = self.authority.inventory.get(current.reference.handler_id)
        if binding is None or binding.binding_id != current.binding_id:
            return ExecutionResult(False, {}, "compute-activation-not-current")
        result = await binding.handler.execute(job)
        if not isinstance(result, ExecutionResult):
            result = ExecutionResult.from_dict(result)
        return result


class ActivatedComputeWorker:
    """F7.4-compatible worker proxy carrying immutable F8.4 activation evidence.

    When the binder was given a lifecycle-capable worker factory the wrapped
    worker also answers F7.5 cancellation, and this proxy forwards it. The
    activation evidence and fencing are identical in both cases; only the worker
    contract the endpoint requires differs.
    """

    def __init__(
        self,
        worker: CapabilityWorker,
        snapshot: ComputeWorkerActivationSnapshot,
    ) -> None:
        self._worker = worker
        self.snapshot = snapshot
        self.registration = worker.registration

    @property
    def cancellable(self) -> bool:
        return callable(getattr(self._worker, "cancel", None))

    async def handle(
        self,
        request: DispatchRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> DispatchResponse:
        return await self._worker.handle(
            request,
            authenticated_actor=authenticated_actor,
            authenticated_session=authenticated_session,
        )

    async def cancel(
        self,
        request,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ):
        cancel = getattr(self._worker, "cancel", None)
        if not callable(cancel):
            raise FederationOperationError(
                "compute-worker-not-cancellable",
                "activated worker was not bound with a lifecycle-capable factory",
                "worker",
            )
        return await cancel(
            request,
            authenticated_actor=authenticated_actor,
            authenticated_session=authenticated_session,
        )


class ComputeWorkerEndpoint(Protocol):
    def register_worker(self, provider_id: str, worker: CapabilityWorker) -> None: ...

    def unregister_worker(
        self,
        provider_id: str,
        *,
        expected_worker: CapabilityWorker | None = None,
    ) -> bool: ...


class TrustedComputeWorkerBinder:
    """Create activated F7.4 workers from current health and local inventory."""

    def __init__(
        self,
        authority: ComputeWorkerActivationAuthority,
        inbox_factory: Callable[[str], SQLiteDispatchInbox],
        *,
        clock: Callable[[], datetime] = _utc_now,
        worker_factory: Callable[..., CapabilityWorker] | None = None,
    ) -> None:
        if not callable(inbox_factory):
            raise FederationValidationError(
                "invalid-dispatch-inbox-factory",
                "inbox_factory",
                "must be callable",
            )
        if worker_factory is not None and not callable(worker_factory):
            raise FederationValidationError(
                "invalid-compute-worker-factory",
                "worker_factory",
                "must be callable",
            )
        self.authority = authority
        self._inbox_factory = inbox_factory
        self._clock = clock
        # Default preserves the original F7.4 worker exactly. A caller that needs
        # F7.5 cancellation supplies CancellableCapabilityWorker (with a matching
        # lifecycle inbox); the activation fencing below is unchanged either way.
        self._worker_factory = worker_factory or CapabilityWorker

    def activate(self, provider_id: str) -> ActivatedComputeWorker:
        snapshot = self.authority.current_snapshot(provider_id)
        registration = WorkerRegistration(
            session_id=snapshot.session_id,
            node_id=snapshot.node_id,
            provider_id=snapshot.provider_id,
            capability_type=snapshot.descriptor.capability_type,
            protocol=snapshot.descriptor.protocol,
            protocol_version=snapshot.descriptor.protocol_version,
            attributes=snapshot.descriptor.attributes,
        )
        inbox = self._inbox_factory(snapshot.provider_id)
        if not isinstance(inbox, SQLiteDispatchInbox):
            raise FederationValidationError(
                "invalid-dispatch-inbox",
                "inbox_factory",
                "must return SQLiteDispatchInbox",
            )
        worker = self._worker_factory(
            registration,
            _ActivationFencedHandler(self.authority, snapshot),
            inbox,
            clock=self._clock,
        )
        if not isinstance(worker, CapabilityWorker):
            raise FederationValidationError(
                "invalid-compute-worker",
                "worker_factory",
                "must return a CapabilityWorker",
            )
        return ActivatedComputeWorker(worker, snapshot)

    def activate_all(self) -> tuple[ActivatedComputeWorker, ...]:
        reports = self.authority.health.fresh_reports(
            session_id=self.authority.session_id,
            actor_node_id=self.authority.local_node_id,
        )
        workers: list[ActivatedComputeWorker] = []
        for report in reports:
            if report.node_id != self.authority.local_node_id:
                continue
            if report.capability_type == "language-model":
                continue
            try:
                workers.append(self.activate(report.capability_id))
            except (FederationOperationError, FederationValidationError):
                continue
        return tuple(sorted(workers, key=lambda item: item.snapshot.provider_id))

    def activate_on(
        self,
        endpoint: ComputeWorkerEndpoint,
        provider_id: str,
    ) -> ActivatedComputeWorker:
        worker = self.activate(provider_id)
        endpoint.register_worker(provider_id, worker)  # type: ignore[arg-type]
        return worker

    @staticmethod
    def deactivate_from(
        endpoint: ComputeWorkerEndpoint,
        worker: ActivatedComputeWorker,
    ) -> bool:
        return endpoint.unregister_worker(
            worker.snapshot.provider_id,
            expected_worker=worker,  # type: ignore[arg-type]
        )

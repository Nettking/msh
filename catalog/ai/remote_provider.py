"""Trusted F8.2-backed remote provider adapters for the F7.7 AI runtime."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthRecord,
    ProviderHealthState,
)
from catalog.capabilities.provider_reports import ProviderResourceReport
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)

from .remote_contracts import (
    MAX_REMOTE_AI_INVOCATION_TTL_SECONDS,
    RemoteAIInvocationOutcome,
    RemoteAIInvocationRequest,
    RemoteAIInvocationResponse,
)
from .runtime import (
    AIProviderInvocationError,
    LANGUAGE_MODEL_CAPABILITY,
    LANGUAGE_MODEL_PROTOCOL,
    _safe_provider_name,
)
from .runtime_contracts import (
    AIProviderFailureClass,
    AIRuntimeRequest,
    _logical_id,
    _text,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RemoteAIInvocationTransport(Protocol):
    """Blocking bridge used by the existing synchronous provider protocol."""

    def invoke(
        self,
        request: RemoteAIInvocationRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> RemoteAIInvocationResponse: ...


@dataclass(frozen=True)
class RemoteLanguageModelSnapshot:
    """Safe current metadata derived only from one fresh F8.2 report."""

    record: ProviderHealthRecord
    display_name: str
    models: tuple[str, ...]
    modalities: tuple[str, ...]
    supports_timeout: bool
    supports_cancellation: bool

    @property
    def report(self) -> ProviderResourceReport:
        return self.record.report


class RemoteAIHealthAuthority:
    """Revalidate current enrollment, announcement, health, and generation."""

    def __init__(
        self,
        health: FederatedProviderHealthService,
        *,
        session_id: str,
        actor_node_id: str,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(health, FederatedProviderHealthService):
            raise FederationValidationError(
                "invalid-health-service",
                "health",
                "must be a FederatedProviderHealthService",
            )
        self.health = health
        self.session_id = _logical_id(session_id, "session_id")
        self.actor_node_id = _logical_id(actor_node_id, "actor_node_id")
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
                "invalid-authority-clock",
                "clock",
                "must return a UTC timestamp",
            )
        return value.astimezone(timezone.utc)

    def current_record(
        self,
        capability_id: str,
        *,
        provider_generation: int | None = None,
    ) -> ProviderHealthRecord:
        capability_id = _logical_id(capability_id, "capability_id")
        observation = self.health.observe(
            session_id=self.session_id,
            capability_id=capability_id,
            actor_node_id=self.actor_node_id,
            provider_generation=provider_generation,
        )
        if observation.state is not ProviderHealthState.CURRENT:
            raise FederationOperationError(
                "remote-provider-not-current",
                f"provider health is {observation.reason_code}",
                "capability_id",
            )
        record = self.health.store.get(
            session_id=self.session_id,
            capability_id=capability_id,
        )
        if record is None:
            raise FederationOperationError(
                "remote-provider-health-missing",
                "current provider health record is unavailable",
                "capability_id",
            )
        if provider_generation is not None and (
            isinstance(provider_generation, bool)
            or not isinstance(provider_generation, int)
            or provider_generation <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-generation",
                "provider_generation",
                "must be a positive integer",
            )
        if (
            provider_generation is not None
            and record.provider_generation != provider_generation
        ):
            raise FederationOperationError(
                "remote-provider-generation-mismatch",
                "remote adapter generation is not the current provider generation",
                "provider_generation",
            )
        report = record.report
        if report.capability_type != LANGUAGE_MODEL_CAPABILITY:
            raise FederationOperationError(
                "remote-provider-type-mismatch",
                "provider is not a language-model capability",
                "capability_type",
            )
        if report.protocol != LANGUAGE_MODEL_PROTOCOL:
            raise FederationOperationError(
                "remote-provider-protocol-mismatch",
                "provider does not use the language-model protocol",
                "protocol",
            )
        if not record.is_fresh_at(self.now()):
            raise FederationOperationError(
                "remote-provider-health-expired",
                "provider health expired before binding or invocation",
                "expires_at",
            )
        return record

    def current_snapshot(
        self,
        capability_id: str,
        *,
        provider_generation: int | None = None,
    ) -> RemoteLanguageModelSnapshot:
        record = self.current_record(
            capability_id,
            provider_generation=provider_generation,
        )
        attributes = record.report.attributes
        models_value = attributes.get("models")
        modalities_value = attributes.get("modalities")
        if not isinstance(models_value, (list, tuple)) or not models_value:
            raise FederationOperationError(
                "remote-provider-models-missing",
                "current provider health does not advertise models",
                "attributes.models",
            )
        if not isinstance(modalities_value, (list, tuple)) or not modalities_value:
            raise FederationOperationError(
                "remote-provider-modalities-missing",
                "current provider health does not advertise modalities",
                "attributes.modalities",
            )
        models = tuple(
            _text(item, f"attributes.models[{index}]", maximum=256)
            for index, item in enumerate(models_value)
        )
        modalities = tuple(
            _logical_id(item, f"attributes.modalities[{index}]")
            for index, item in enumerate(modalities_value)
        )
        if len(models) != len(set(models)) or len(modalities) != len(
            set(modalities)
        ):
            raise FederationOperationError(
                "remote-provider-advertisement-duplicate",
                "provider models and modalities must be unique",
                "attributes",
            )
        features = attributes.get("features", {})
        if not isinstance(features, dict):
            raise FederationOperationError(
                "remote-provider-features-invalid",
                "provider features must be an object",
                "attributes.features",
            )
        timeout = features.get("timeout", True)
        cancellation = features.get("cancellation", False)
        if not isinstance(timeout, bool) or not isinstance(cancellation, bool):
            raise FederationOperationError(
                "remote-provider-features-invalid",
                "timeout and cancellation features must be boolean",
                "attributes.features",
            )
        label = attributes.get("label", record.capability_id)
        display_name = _safe_provider_name(label)
        return RemoteLanguageModelSnapshot(
            record=record,
            display_name=display_name,
            models=models,
            modalities=modalities,
            supports_timeout=timeout,
            supports_cancellation=cancellation,
        )


class RemoteLanguageModelProvider:
    """F7.7 provider adapter with no remote endpoint or credential fields."""

    def __init__(
        self,
        authority: RemoteAIHealthAuthority,
        transport: RemoteAIInvocationTransport,
        *,
        capability_id: str,
        provider_generation: int,
    ) -> None:
        if not hasattr(transport, "invoke"):
            raise FederationValidationError(
                "invalid-remote-ai-transport",
                "transport",
                "must implement invoke",
            )
        self._authority = authority
        self._transport = transport
        self._capability_id = _logical_id(capability_id, "capability_id")
        if (
            isinstance(provider_generation, bool)
            or not isinstance(provider_generation, int)
            or provider_generation <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-generation",
                "provider_generation",
                "must be a positive integer",
            )
        self._provider_generation = provider_generation
        initial = self._snapshot()
        self._node_id = initial.record.node_id
        self._session_id = initial.record.session_id
        self._protocol = initial.report.protocol
        self._protocol_version = initial.report.protocol_version
        self._display_name = initial.display_name

    def _snapshot(self) -> RemoteLanguageModelSnapshot:
        snapshot = self._authority.current_snapshot(
            self._capability_id,
            provider_generation=self._provider_generation,
        )
        if hasattr(self, "_node_id") and snapshot.record.node_id != self._node_id:
            raise FederationOperationError(
                "remote-provider-node-changed",
                "provider identity moved to another node",
                "node_id",
            )
        return snapshot

    @property
    def capability_id(self) -> str:
        return self._capability_id

    @property
    def provider_generation(self) -> int:
        return self._provider_generation

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def display_name(self) -> str:
        return self._display_name

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def protocol_version(self) -> str:
        return self._protocol_version

    @property
    def models(self) -> tuple[str, ...]:
        return self._snapshot().models

    @property
    def modalities(self) -> tuple[str, ...]:
        return self._snapshot().modalities

    @property
    def max_concurrent_jobs(self) -> int:
        return self._snapshot().report.max_concurrent_jobs

    @property
    def supports_timeout(self) -> bool:
        return self._snapshot().supports_timeout

    @property
    def supports_cancellation(self) -> bool:
        return self._snapshot().supports_cancellation

    def resource_report(self, now: datetime) -> ProviderResourceReport | None:
        try:
            snapshot = self._snapshot()
            if not snapshot.record.is_fresh_at(now):
                return None
            return snapshot.report
        except (FederationOperationError, FederationValidationError):
            return None

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        if not isinstance(request, AIRuntimeRequest):
            raise FederationValidationError(
                "invalid-ai-request",
                "request",
                "must be an AIRuntimeRequest",
            )
        if cancellation_event is not None and cancellation_event.is_set():
            raise AIProviderInvocationError(
                "provider-request-cancelled",
                "The remote provider request was cancelled before transmission.",
                failure_class=AIProviderFailureClass.CANCELLED,
                retryable=False,
            )
        try:
            snapshot = self._snapshot()
        except FederationOperationError as exc:
            raise AIProviderInvocationError(
                "provider-unavailable",
                "The remote provider is no longer current.",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
            ) from exc
        if request.session_id != self.session_id:
            raise AIProviderInvocationError(
                "provider-authorization-error",
                "The remote provider does not belong to the request session.",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
                retryable=False,
            )
        if request.model not in snapshot.models:
            raise AIProviderInvocationError(
                "provider-model-unavailable",
                "The remote provider does not advertise the requested model.",
                failure_class=AIProviderFailureClass.PROTOCOL,
                retryable=False,
            )
        if request.modality.value not in snapshot.modalities:
            raise AIProviderInvocationError(
                "provider-modality-unavailable",
                "The remote provider does not advertise the requested modality.",
                failure_class=AIProviderFailureClass.PROTOCOL,
                retryable=False,
            )
        sent_at = self._authority.now()
        requested_timeout = (
            float(request.timeout_seconds)
            if timeout_seconds is None
            else min(float(timeout_seconds), float(request.timeout_seconds))
        )
        ttl = max(
            1.0,
            min(requested_timeout, float(MAX_REMOTE_AI_INVOCATION_TTL_SECONDS)),
        )
        invocation = RemoteAIInvocationRequest(
            invocation_id=f"rai-{uuid.uuid4().hex}",
            session_id=self.session_id,
            requester_node_id=self._authority.actor_node_id,
            provider_node_id=self.node_id,
            capability_id=self.capability_id,
            provider_generation=self.provider_generation,
            health_report_revision=snapshot.record.report_revision,
            request=request,
            sent_at=sent_at,
            expires_at=sent_at + timedelta(seconds=ttl),
        )
        try:
            response = self._transport.invoke(
                invocation,
                timeout_seconds=requested_timeout,
                cancellation_event=cancellation_event,
            )
        except AIProviderInvocationError:
            raise
        except TimeoutError as exc:
            raise AIProviderInvocationError(
                "provider-timeout",
                "The remote provider did not answer before the timeout.",
                failure_class=AIProviderFailureClass.TIMEOUT,
                retryable=True,
            ) from exc
        except Exception as exc:
            raise AIProviderInvocationError(
                "provider-unavailable",
                "The authenticated remote provider route failed.",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
            ) from exc
        self._validate_response(invocation, response)
        if response.outcome is RemoteAIInvocationOutcome.SUCCEEDED:
            if response.content is None:
                raise AIProviderInvocationError(
                    "provider-invalid-response",
                    "The remote provider returned an incomplete success response.",
                    failure_class=AIProviderFailureClass.INVALID_RESPONSE,
                    retryable=False,
                )
            return response.content
        if response.error_code is None or response.failure_class is None:
            raise AIProviderInvocationError(
                "provider-invalid-response",
                "The remote provider returned an incomplete failure response.",
                failure_class=AIProviderFailureClass.INVALID_RESPONSE,
                retryable=False,
            )
        raise AIProviderInvocationError(
            response.error_code,
            "The remote provider rejected or failed the invocation.",
            failure_class=response.failure_class,
            retryable=response.retryable,
        )

    @staticmethod
    def _validate_response(
        request: RemoteAIInvocationRequest,
        response: RemoteAIInvocationResponse,
    ) -> None:
        if not isinstance(response, RemoteAIInvocationResponse):
            raise AIProviderInvocationError(
                "provider-invalid-response",
                "The remote route returned an invalid response object.",
                failure_class=AIProviderFailureClass.INVALID_RESPONSE,
                retryable=False,
            )
        expected = (
            request.invocation_id,
            request.session_id,
            request.requester_node_id,
            request.provider_node_id,
            request.capability_id,
            request.provider_generation,
            request.request.request_id,
        )
        actual = (
            response.invocation_id,
            response.session_id,
            response.requester_node_id,
            response.provider_node_id,
            response.capability_id,
            response.provider_generation,
            response.request_id,
        )
        if actual != expected:
            raise AIProviderInvocationError(
                "provider-invalid-response",
                "The remote response identity does not match the invocation.",
                failure_class=AIProviderFailureClass.INVALID_RESPONSE,
                retryable=False,
            )


class TrustedRemoteLanguageModelBinder:
    """Create F7.7 adapters only from current F8.2 language-model health."""

    def __init__(
        self,
        authority: RemoteAIHealthAuthority,
        transport_factory: Callable[
            [str, str], RemoteAIInvocationTransport
        ],
    ) -> None:
        if not callable(transport_factory):
            raise FederationValidationError(
                "invalid-transport-factory",
                "transport_factory",
                "must be callable",
            )
        self.authority = authority
        self._transport_factory = transport_factory

    def bind(self, capability_id: str) -> RemoteLanguageModelProvider:
        capability_id = _logical_id(capability_id, "capability_id")
        snapshot = self.authority.current_snapshot(capability_id)
        transport = self._transport_factory(
            snapshot.record.node_id,
            capability_id,
        )
        return RemoteLanguageModelProvider(
            self.authority,
            transport,
            capability_id=capability_id,
            provider_generation=snapshot.record.provider_generation,
        )

    def bind_all(self) -> tuple[RemoteLanguageModelProvider, ...]:
        reports = self.authority.health.fresh_reports(
            session_id=self.authority.session_id,
            actor_node_id=self.authority.actor_node_id,
            capability_type=LANGUAGE_MODEL_CAPABILITY,
        )
        providers = [self.bind(report.capability_id) for report in reports]
        return tuple(sorted(providers, key=lambda provider: provider.capability_id))

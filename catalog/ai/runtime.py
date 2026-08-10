"""Bounded multi-provider scheduler for F7.7 language-model capabilities."""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Protocol

from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderSelectionPolicy,
    ProviderStatus,
)
from catalog.capabilities.provider_selection import ProviderSelection, select_provider
from catalog.federation.errors import FederationValidationError

from .runtime_contracts import (
    AIProviderAttempt,
    AIProviderFailureClass,
    AIRuntimeError,
    AIRuntimeRequest,
    AIRuntimeResult,
    AISelectionStatus,
    NON_FALLBACK_FAILURE_CLASSES,
    _logical_id,
    _text,
)

LANGUAGE_MODEL_CAPABILITY = "language-model"
LANGUAGE_MODEL_PROTOCOL = "fcp-language-model"
LANGUAGE_MODEL_PROTOCOL_VERSION = "1.0"
MAX_REGISTERED_AI_PROVIDERS = 128
MAX_PENDING_AI_REQUESTS = 1024

_ADDRESS_RE = re.compile(
    r"(?:https?://|[a-z]+://|(?:^|[^0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:[^0-9]|$)|"
    r"\[[0-9a-fA-F:]+\]|(?:^|[^a-zA-Z0-9])[a-zA-Z0-9.-]+:[0-9]{2,5}(?:[^0-9]|$)|@|[/\\])"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_provider_name(value: str) -> str:
    value = _text(value, "provider_name", maximum=80)
    if _ADDRESS_RE.search(value):
        raise FederationValidationError(
            "unsafe-provider-name",
            "provider_name",
            "must be a display label, not an endpoint, credential, address, or path",
        )
    return value


class LanguageModelProvider(Protocol):
    capability_id: str
    node_id: str
    session_id: str
    display_name: str
    protocol: str
    protocol_version: str
    models: tuple[str, ...]
    modalities: tuple[str, ...]
    max_concurrent_jobs: int
    supports_timeout: bool
    supports_cancellation: bool

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        """Execute one already-authorized logical language-model request."""


class AIProviderInvocationError(AIRuntimeError):
    """Structured provider failure consumed by the runtime fallback policy."""


@dataclass(frozen=True)
class AIRuntimePolicy:
    """Explicit queue, selection, and fallback policy for one runtime."""

    selection_policy: ProviderSelectionPolicy = field(
        default_factory=ProviderSelectionPolicy
    )
    max_pending_requests: int = 32
    max_fallbacks: int = 1
    fallback_error_codes: tuple[str, ...] = (
        "provider-unavailable",
        "provider-overloaded",
        "provider-timeout",
    )
    queue_wait_interval_seconds: float = 0.05

    def __post_init__(self) -> None:
        if not isinstance(self.selection_policy, ProviderSelectionPolicy):
            raise FederationValidationError(
                "invalid-selection-policy",
                "selection_policy",
                "must be ProviderSelectionPolicy",
            )
        if (
            isinstance(self.max_pending_requests, bool)
            or not isinstance(self.max_pending_requests, int)
            or self.max_pending_requests <= 0
            or self.max_pending_requests > MAX_PENDING_AI_REQUESTS
        ):
            raise FederationValidationError(
                "invalid-ai-queue-bound",
                "max_pending_requests",
                f"must be between 1 and {MAX_PENDING_AI_REQUESTS}",
            )
        if (
            isinstance(self.max_fallbacks, bool)
            or not isinstance(self.max_fallbacks, int)
            or self.max_fallbacks < 0
            or self.max_fallbacks >= 32
        ):
            raise FederationValidationError(
                "invalid-ai-fallback-bound",
                "max_fallbacks",
                "must be between 0 and 31",
            )
        codes = tuple(
            _logical_id(item, f"fallback_error_codes[{index}]")
            for index, item in enumerate(self.fallback_error_codes)
        )
        if len(codes) != len(set(codes)):
            raise FederationValidationError(
                "duplicate-fallback-error-code",
                "fallback_error_codes",
                "must not contain duplicates",
            )
        object.__setattr__(self, "fallback_error_codes", codes)
        if (
            isinstance(self.queue_wait_interval_seconds, bool)
            or not isinstance(self.queue_wait_interval_seconds, (int, float))
            or self.queue_wait_interval_seconds <= 0
            or self.queue_wait_interval_seconds > 5
        ):
            raise FederationValidationError(
                "invalid-queue-wait-interval",
                "queue_wait_interval_seconds",
                "must be greater than 0 and at most 5 seconds",
            )


@dataclass
class _ProviderState:
    provider: LanguageModelProvider
    status: ProviderStatus = ProviderStatus.READY
    active_jobs: int = 0
    queue_depth: int = 0
    report_revision: int = 0


class LanguageModelRuntime:
    """Select and execute logical language-model providers with bounded waiting.

    Provider endpoint data remains inside the provider adapter. Scheduling sees
    only logical IDs, models, modalities, health, capacity, and safe labels.
    """

    def __init__(
        self,
        *,
        session_id: str,
        providers: tuple[LanguageModelProvider, ...] = (),
        policy: AIRuntimePolicy | None = None,
        clock: Callable[[], datetime] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.session_id = _logical_id(session_id, "session_id")
        self.policy = policy or AIRuntimePolicy()
        if not isinstance(self.policy, AIRuntimePolicy):
            raise FederationValidationError(
                "invalid-ai-runtime-policy",
                "policy",
                "must be AIRuntimePolicy",
            )
        self._clock = clock
        self._monotonic = monotonic
        self._condition = threading.Condition(threading.RLock())
        self._providers: dict[str, _ProviderState] = {}
        self._pending_requests = 0
        for provider in providers:
            self.register(provider)

    def register(
        self,
        provider: LanguageModelProvider,
        *,
        status: ProviderStatus = ProviderStatus.READY,
    ) -> None:
        capability_id = _logical_id(
            getattr(provider, "capability_id", None), "capability_id"
        )
        node_id = _logical_id(getattr(provider, "node_id", None), "node_id")
        provider_session = _logical_id(
            getattr(provider, "session_id", None), "provider.session_id"
        )
        if provider_session != self.session_id:
            raise FederationValidationError(
                "cross-session-ai-provider",
                "provider.session_id",
                "provider must belong to the runtime session",
            )
        _safe_provider_name(getattr(provider, "display_name", None))
        _text(getattr(provider, "protocol", None), "provider.protocol", maximum=128)
        _text(
            getattr(provider, "protocol_version", None),
            "provider.protocol_version",
            maximum=32,
        )
        models = tuple(
            _text(item, f"provider.models[{index}]", maximum=256)
            for index, item in enumerate(getattr(provider, "models", ()))
        )
        modalities = tuple(
            _logical_id(item, f"provider.modalities[{index}]")
            for index, item in enumerate(getattr(provider, "modalities", ()))
        )
        if not models or not modalities:
            raise FederationValidationError(
                "incomplete-ai-provider",
                "provider",
                "must advertise at least one model and modality",
            )
        max_concurrent = getattr(provider, "max_concurrent_jobs", None)
        if (
            isinstance(max_concurrent, bool)
            or not isinstance(max_concurrent, int)
            or max_concurrent <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-capacity",
                "max_concurrent_jobs",
                "must be a positive integer",
            )
        try:
            provider_status = ProviderStatus(status)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-provider-status", "status", "unknown provider status"
            ) from exc
        with self._condition:
            if capability_id in self._providers:
                raise FederationValidationError(
                    "duplicate-ai-provider",
                    "capability_id",
                    "provider is already registered",
                )
            if len(self._providers) >= MAX_REGISTERED_AI_PROVIDERS:
                raise FederationValidationError(
                    "too-many-ai-providers",
                    "providers",
                    f"must not exceed {MAX_REGISTERED_AI_PROVIDERS}",
                )
            self._providers[capability_id] = _ProviderState(
                provider=provider,
                status=provider_status,
            )
            self._condition.notify_all()
        del node_id, models, modalities

    def set_provider_status(
        self,
        capability_id: str,
        status: ProviderStatus,
        *,
        queue_depth: int | None = None,
    ) -> None:
        capability_id = _logical_id(capability_id, "capability_id")
        try:
            provider_status = ProviderStatus(status)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "invalid-provider-status", "status", "unknown provider status"
            ) from exc
        if queue_depth is not None and (
            isinstance(queue_depth, bool)
            or not isinstance(queue_depth, int)
            or queue_depth < 0
        ):
            raise FederationValidationError(
                "invalid-provider-queue-depth",
                "queue_depth",
                "must be a non-negative integer",
            )
        with self._condition:
            state = self._providers.get(capability_id)
            if state is None:
                raise FederationValidationError(
                    "unknown-ai-provider", "capability_id", "provider is not registered"
                )
            state.status = provider_status
            if queue_depth is not None:
                state.queue_depth = queue_depth
            state.report_revision += 1
            self._condition.notify_all()

    def provider_status(self) -> tuple[dict[str, object], ...]:
        """Return a safe status surface with no endpoint or credential fields."""
        with self._condition:
            values = []
            for capability_id, state in sorted(self._providers.items()):
                provider = state.provider
                values.append(
                    {
                        "capability_id": capability_id,
                        "provider_name": _safe_provider_name(provider.display_name),
                        "status": state.status.value,
                        "models": list(provider.models),
                        "modalities": list(provider.modalities),
                        "active_jobs": state.active_jobs,
                        "max_concurrent_jobs": provider.max_concurrent_jobs,
                        "queue_depth": state.queue_depth,
                        "supports_timeout": bool(provider.supports_timeout),
                        "supports_cancellation": bool(provider.supports_cancellation),
                    }
                )
            return tuple(values)

    def execute(
        self,
        request: AIRuntimeRequest,
        *,
        cancellation_event: threading.Event | None = None,
    ) -> AIRuntimeResult:
        if not isinstance(request, AIRuntimeRequest):
            raise FederationValidationError(
                "invalid-ai-request", "request", "must be AIRuntimeRequest"
            )
        if request.session_id != self.session_id:
            raise AIRuntimeError(
                "cross-session-ai-request",
                "The AI request does not belong to this runtime session.",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
            )
        if cancellation_event is not None and not isinstance(
            cancellation_event, threading.Event
        ):
            raise FederationValidationError(
                "invalid-cancellation-event",
                "cancellation_event",
                "must be threading.Event",
            )
        self._reserve_pending(request, cancellation_event)
        deadline = self._monotonic() + request.timeout_seconds
        attempted_ids: set[str] = set()
        attempts: list[AIProviderAttempt] = []
        try:
            while True:
                self._raise_if_cancelled(cancellation_event, attempts)
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise AIRuntimeError(
                        "ai-runtime-timeout",
                        "The AI request timed out before a provider completed it.",
                        failure_class=AIProviderFailureClass.TIMEOUT,
                        retryable=False,
                        attempts=tuple(attempts),
                    )
                provider, selection = self._select_and_reserve(
                    request,
                    attempted_ids=attempted_ids,
                    deadline=deadline,
                    cancellation_event=cancellation_event,
                )
                capability_id = provider.capability_id
                provider_name = _safe_provider_name(provider.display_name)
                try:
                    timeout = remaining if provider.supports_timeout else None
                    content = provider.invoke(
                        request,
                        timeout_seconds=timeout,
                        cancellation_event=(
                            cancellation_event
                            if provider.supports_cancellation
                            else None
                        ),
                    )
                    content = _text(content, "provider_response", maximum=4 * 1024 * 1024)
                    attempts.append(
                        AIProviderAttempt(
                            capability_id=capability_id,
                            provider_name=provider_name,
                            outcome="succeeded",
                        )
                    )
                    return AIRuntimeResult(
                        request_id=request.request_id,
                        model=request.model,
                        modality=request.modality,
                        content=content,
                        provider_capability_id=capability_id,
                        provider_name=provider_name,
                        selection=selection,
                        attempts=tuple(attempts),
                        completed_at=self._clock(),
                    )
                except AIProviderInvocationError as error:
                    attempts.append(
                        AIProviderAttempt(
                            capability_id=capability_id,
                            provider_name=provider_name,
                            outcome="failed",
                            error_code=error.code,
                        )
                    )
                    attempted_ids.add(capability_id)
                    if not self._allows_fallback(error, attempts):
                        raise AIRuntimeError(
                            error.code,
                            error.message,
                            failure_class=error.failure_class,
                            retryable=error.retryable,
                            provider_capability_id=capability_id,
                            selection=selection,
                            attempts=tuple(attempts),
                        ) from error
                except AIRuntimeError:
                    raise
                except Exception as error:
                    attempts.append(
                        AIProviderAttempt(
                            capability_id=capability_id,
                            provider_name=provider_name,
                            outcome="failed",
                            error_code="provider-internal-error",
                        )
                    )
                    raise AIRuntimeError(
                        "provider-internal-error",
                        "The selected AI provider failed unexpectedly.",
                        failure_class=AIProviderFailureClass.INTERNAL,
                        retryable=False,
                        provider_capability_id=capability_id,
                        selection=selection,
                        attempts=tuple(attempts),
                    ) from error
                finally:
                    self._release_provider(capability_id)
        finally:
            with self._condition:
                self._pending_requests -= 1
                self._condition.notify_all()

    def _reserve_pending(
        self,
        request: AIRuntimeRequest,
        cancellation_event: threading.Event | None,
    ) -> None:
        self._raise_if_cancelled(cancellation_event, ())
        with self._condition:
            if self._pending_requests >= self.policy.max_pending_requests:
                raise AIRuntimeError(
                    "ai-queue-full",
                    "The AI request queue is full. Try again after current work completes.",
                    failure_class=AIProviderFailureClass.OVERLOADED,
                    retryable=True,
                )
            self._pending_requests += 1

    def _select_and_reserve(
        self,
        request: AIRuntimeRequest,
        *,
        attempted_ids: set[str],
        deadline: float,
        cancellation_event: threading.Event | None,
    ) -> tuple[LanguageModelProvider, AISelectionStatus]:
        job = self._job_for(request)
        while True:
            self._raise_if_cancelled(cancellation_event, ())
            now = self._clock()
            with self._condition:
                reports = self._reports(now)
                excluded = tuple(
                    sorted(
                        set(self.policy.selection_policy.excluded_capability_ids)
                        | attempted_ids
                    )
                )
                selection_policy = ProviderSelectionPolicy(
                    minimum_available_slots=self.policy.selection_policy.minimum_available_slots,
                    max_queue_depth=self.policy.selection_policy.max_queue_depth,
                    max_utilization_millis=self.policy.selection_policy.max_utilization_millis,
                    preferred_node_id=self.policy.selection_policy.preferred_node_id,
                    excluded_capability_ids=excluded,
                )
                selection = select_provider(
                    job,
                    reports,
                    evaluated_at=now,
                    policy=selection_policy,
                )
                status = self._selection_status(selection)
                selected_id = selection.selected_capability_id
                if selected_id is not None:
                    state = self._providers[selected_id]
                    if state.active_jobs < state.provider.max_concurrent_jobs:
                        state.active_jobs += 1
                        state.report_revision += 1
                        return state.provider, status
                if not self._only_capacity_rejections(selection):
                    raise AIRuntimeError(
                        "no-eligible-ai-provider",
                        "No registered AI provider satisfies the requested model and modality.",
                        failure_class=AIProviderFailureClass.TRANSIENT,
                        retryable=True,
                        selection=status,
                    )
                remaining = deadline - self._monotonic()
                if remaining <= 0:
                    raise AIRuntimeError(
                        "ai-queue-timeout",
                        "The AI request timed out while waiting for provider capacity.",
                        failure_class=AIProviderFailureClass.TIMEOUT,
                        retryable=True,
                        selection=status,
                    )
                self._condition.wait(
                    timeout=min(self.policy.queue_wait_interval_seconds, remaining)
                )

    def _reports(self, now: datetime) -> tuple[ProviderResourceReport, ...]:
        expires = now + timedelta(seconds=30)
        reports = []
        waiting = max(0, self._pending_requests - sum(s.active_jobs for s in self._providers.values()))
        for capability_id, state in sorted(self._providers.items()):
            provider = state.provider
            reports.append(
                ProviderResourceReport(
                    capability_id=capability_id,
                    node_id=provider.node_id,
                    session_id=provider.session_id,
                    capability_type=LANGUAGE_MODEL_CAPABILITY,
                    protocol=provider.protocol,
                    protocol_version=provider.protocol_version,
                    status=state.status,
                    report_revision=state.report_revision,
                    max_concurrent_jobs=provider.max_concurrent_jobs,
                    active_jobs=state.active_jobs,
                    queue_depth=state.queue_depth + waiting,
                    utilization_millis=(
                        int(1000 * state.active_jobs / provider.max_concurrent_jobs)
                    ),
                    attributes={
                        "models": list(provider.models),
                        "modalities": list(provider.modalities),
                        "features": {
                            "timeout": bool(provider.supports_timeout),
                            "cancellation": bool(provider.supports_cancellation),
                        },
                    },
                    reported_at=now,
                    expires_at=expires,
                )
            )
        return tuple(reports)

    @staticmethod
    def _job_for(request: AIRuntimeRequest) -> JobContract:
        return JobContract(
            job_id=f"ai-job-{request.request_id}",
            session_id=request.session_id,
            request_id=request.request_id,
            idempotency_key=request.idempotency_key,
            capability=CapabilityRequirement(
                capability_type=LANGUAGE_MODEL_CAPABILITY,
                protocol=LANGUAGE_MODEL_PROTOCOL,
                protocol_version=LANGUAGE_MODEL_PROTOCOL_VERSION,
                requirements={
                    "models": [request.model],
                    "modalities": [request.modality.value],
                },
            ),
            inputs=(),
            outputs=(),
            retry_policy=RetryPolicy(
                max_attempts=1,
                backoff_seconds=(),
                retryable_error_codes=(),
            ),
            timeout_policy=TimeoutPolicy(
                overall_timeout_seconds=request.timeout_seconds,
                queue_timeout_seconds=request.timeout_seconds,
                start_timeout_seconds=request.timeout_seconds,
                run_timeout_seconds=request.timeout_seconds,
                cancellation_grace_seconds=min(5, request.timeout_seconds),
            ),
        )

    def _selection_status(self, selection: ProviderSelection) -> AISelectionStatus:
        name = None
        if selection.selected_capability_id is not None:
            name = _safe_provider_name(
                self._providers[selection.selected_capability_id].provider.display_name
            )
        return AISelectionStatus(
            decision=selection.decision,
            selected_capability_id=selection.selected_capability_id,
            selected_provider_name=name,
            eligible_capability_ids=selection.eligible_capability_ids,
            rejected=selection.rejected,
            reason=selection.reason,
            evaluated_at=selection.evaluated_at,
        )

    @staticmethod
    def _only_capacity_rejections(selection: ProviderSelection) -> bool:
        if not selection.rejected:
            return False
        allowed = {
            "insufficient-available-slots",
            "provider-queue-limit-exceeded",
            "provider-utilization-limit-exceeded",
            "provider-excluded",
        }
        reasons = {
            reason
            for provider_reasons in selection.rejected.values()
            for reason in provider_reasons
        }
        return bool(reasons) and reasons.issubset(allowed)

    def _allows_fallback(
        self,
        error: AIProviderInvocationError,
        attempts: list[AIProviderAttempt],
    ) -> bool:
        if error.failure_class in NON_FALLBACK_FAILURE_CLASSES:
            return False
        if error.code not in self.policy.fallback_error_codes:
            return False
        fallbacks_used = max(0, len(attempts) - 1)
        return fallbacks_used < self.policy.max_fallbacks

    def _release_provider(self, capability_id: str) -> None:
        with self._condition:
            state = self._providers[capability_id]
            if state.active_jobs <= 0:
                raise RuntimeError("AI provider active-job counter underflow")
            state.active_jobs -= 1
            state.report_revision += 1
            self._condition.notify_all()

    @staticmethod
    def _raise_if_cancelled(
        cancellation_event: threading.Event | None,
        attempts: tuple[AIProviderAttempt, ...] | list[AIProviderAttempt],
    ) -> None:
        if cancellation_event is not None and cancellation_event.is_set():
            raise AIRuntimeError(
                "ai-request-cancelled",
                "The AI request was cancelled.",
                failure_class=AIProviderFailureClass.CANCELLED,
                retryable=False,
                attempts=tuple(attempts),
            )

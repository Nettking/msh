"""Shared provider-capacity accounting across local and hosted AI invocation paths."""

from __future__ import annotations

import threading
import time

from .runtime import AIProviderInvocationError, LanguageModelProvider
from .runtime_contracts import AIProviderFailureClass, AIRuntimeRequest

MAX_SHARED_CAPACITY_WAITERS = 128
_CAPACITY_WAIT_SLICE_SECONDS = 0.05


class SharedCapacityLanguageModelProvider:
    """Serialize all callers through one bounded provider-capacity domain.

    ``LanguageModelRuntime`` keeps process-local scheduling state. A provider that
    is also exposed through the Federation host therefore needs one additional
    gate shared by both invocation paths; otherwise one local request plus one
    remote request can both consume a provider that advertises capacity one.
    """

    def __init__(
        self,
        provider: LanguageModelProvider,
        *,
        max_waiters: int = MAX_SHARED_CAPACITY_WAITERS,
    ) -> None:
        if (
            isinstance(max_waiters, bool)
            or not isinstance(max_waiters, int)
            or max_waiters <= 0
            or max_waiters > MAX_SHARED_CAPACITY_WAITERS
        ):
            raise ValueError(
                f"max_waiters must be between 1 and {MAX_SHARED_CAPACITY_WAITERS}"
            )
        capacity = getattr(provider, "max_concurrent_jobs", None)
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or capacity <= 0
        ):
            raise ValueError("provider max_concurrent_jobs must be a positive integer")
        self._provider = provider
        self._max_waiters = max_waiters
        self._condition = threading.Condition(threading.RLock())
        self._active_jobs = 0
        self._queue_depth = 0

    def wraps(self, provider: object) -> bool:
        return self._provider is provider

    @property
    def capability_id(self) -> str:
        return self._provider.capability_id

    @property
    def node_id(self) -> str:
        return self._provider.node_id

    @property
    def session_id(self) -> str:
        return self._provider.session_id

    @property
    def display_name(self) -> str:
        return self._provider.display_name

    @property
    def protocol(self) -> str:
        return self._provider.protocol

    @property
    def protocol_version(self) -> str:
        return self._provider.protocol_version

    @property
    def models(self) -> tuple[str, ...]:
        return tuple(self._provider.models)

    @property
    def modalities(self) -> tuple[str, ...]:
        return tuple(self._provider.modalities)

    @property
    def max_concurrent_jobs(self) -> int:
        return int(self._provider.max_concurrent_jobs)

    @property
    def supports_timeout(self) -> bool:
        return bool(self._provider.supports_timeout)

    @property
    def supports_cancellation(self) -> bool:
        return bool(self._provider.supports_cancellation)

    def capacity_snapshot(self) -> tuple[int, int]:
        """Return the actual shared active and waiting invocation counts."""

        with self._condition:
            return self._active_jobs, self._queue_depth

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        deadline = (
            None
            if timeout_seconds is None
            else time.monotonic() + max(0.0, float(timeout_seconds))
        )
        with self._condition:
            if self._active_jobs >= self.max_concurrent_jobs:
                if self._queue_depth >= self._max_waiters:
                    raise AIProviderInvocationError(
                        "provider-overloaded",
                        "The selected AI provider has reached its shared queue limit.",
                        failure_class=AIProviderFailureClass.OVERLOADED,
                        retryable=True,
                    )
                self._queue_depth += 1
                try:
                    while self._active_jobs >= self.max_concurrent_jobs:
                        if (
                            cancellation_event is not None
                            and cancellation_event.is_set()
                        ):
                            raise AIProviderInvocationError(
                                "provider-cancelled",
                                "The AI request was cancelled while waiting for provider capacity.",
                                failure_class=AIProviderFailureClass.CANCELLED,
                                retryable=False,
                            )
                        if deadline is None:
                            wait_seconds = _CAPACITY_WAIT_SLICE_SECONDS
                        else:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                raise AIProviderInvocationError(
                                    "provider-timeout",
                                    "The selected AI provider did not become available before the timeout.",
                                    failure_class=AIProviderFailureClass.TIMEOUT,
                                    retryable=True,
                                )
                            wait_seconds = min(
                                _CAPACITY_WAIT_SLICE_SECONDS,
                                remaining,
                            )
                        self._condition.wait(timeout=wait_seconds)
                finally:
                    self._queue_depth -= 1
            self._active_jobs += 1

        try:
            return self._provider.invoke(
                request,
                timeout_seconds=timeout_seconds,
                cancellation_event=cancellation_event,
            )
        finally:
            with self._condition:
                if self._active_jobs <= 0:
                    raise RuntimeError("shared AI provider active-job counter underflow")
                self._active_jobs -= 1
                self._condition.notify_all()


__all__ = [
    "MAX_SHARED_CAPACITY_WAITERS",
    "SharedCapacityLanguageModelProvider",
]

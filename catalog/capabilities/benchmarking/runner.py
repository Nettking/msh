"""Bounded local execution for registered capability benchmarks."""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from catalog.federation.onboarding_models import (
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
)

from .errors import InvalidBenchmarkOutputError
from .fingerprints import dependency_fingerprint, request_fingerprint
from .registry import BenchmarkRegistry, RegisteredBenchmark
from .safety import bounded_public_mapping, safe_diagnostics
from .store import SQLiteBenchmarkResultStore


class BenchmarkCancelled(RuntimeError):
    """Cooperative cancellation signal available to local benchmark probes."""


class CancellationToken:
    """Thread-safe cancellation token controlled by the local caller."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(frozen=True)
class BenchmarkExecutionContext:
    device_id: str
    target_service_id: str
    dependency_inputs: Mapping[str, Any]
    _deadline: float
    _monotonic: Callable[[], float]
    _cancelled: Callable[[], bool]

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._deadline - self._monotonic())

    @property
    def cancelled(self) -> bool:
        return self._cancelled()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise BenchmarkCancelled("benchmark cancelled")
        if self.remaining_seconds <= 0:
            raise TimeoutError("benchmark deadline exceeded")


@dataclass(frozen=True)
class BenchmarkObservation:
    """Safe logical result returned by a trusted local benchmark probe."""

    metrics: Mapping[str, Any] = field(default_factory=dict)
    recommendation: BenchmarkRecommendation = BenchmarkRecommendation.USABLE
    diagnostics: tuple[str, ...] = ()
    state: BenchmarkState = BenchmarkState.PASSED


@dataclass(frozen=True)
class BenchmarkRunRequest:
    run_id: str
    device_id: str
    benchmark_id: str
    target_service_id: str
    dependency_inputs: Mapping[str, Any] = field(default_factory=dict)
    available_prerequisites: frozenset[str] = frozenset()
    timeout_seconds: float | None = None
    cancellation_token: CancellationToken | None = None


class BenchmarkRunner:
    """Run trusted local probes with bounded wait and durable idempotency."""

    def __init__(
        self,
        registry: BenchmarkRegistry,
        store: SQLiteBenchmarkResultStore,
        *,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
        poll_interval_seconds: float = 0.02,
    ) -> None:
        if poll_interval_seconds <= 0:
            raise ValueError("poll interval must be positive")
        self._registry = registry
        self._store = store
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic
        self._poll_interval_seconds = poll_interval_seconds

    def run(self, request: BenchmarkRunRequest) -> BenchmarkResult:
        entry = self._registry.get(request.benchmark_id)
        definition = entry.definition
        dependency = dependency_fingerprint(definition, request.dependency_inputs)
        fingerprint = request_fingerprint(
            run_id=request.run_id,
            device_id=request.device_id,
            benchmark_id=definition.benchmark_id,
            benchmark_version=definition.implementation_version,
            target_service_id=request.target_service_id,
            dependency_fingerprint_value=dependency,
        )
        timeout = self._effective_timeout(request, entry)
        started_at = self._utc_now()
        replay = self._store.reserve_run(
            run_id=request.run_id,
            request_fingerprint=fingerprint,
            deadline_at=started_at + timedelta(seconds=timeout + 1.0),
            now=started_at,
        )
        if replay is not None:
            return replay

        try:
            missing = tuple(
                sorted(
                    set(definition.prerequisites)
                    - set(request.available_prerequisites)
                )
            )
            if missing:
                result = self._result(
                    request=request,
                    entry=entry,
                    dependency=dependency,
                    started_at=started_at,
                    state=BenchmarkState.SKIPPED,
                    recommendation=BenchmarkRecommendation.UNSUPPORTED,
                    diagnostics=safe_diagnostics(
                        f"missing prerequisites: {', '.join(missing)}"
                    ),
                )
                return self._store.complete_run(
                    result=result,
                    request_fingerprint=fingerprint,
                )

            if (
                request.cancellation_token is not None
                and request.cancellation_token.cancelled
            ):
                result = self._result(
                    request=request,
                    entry=entry,
                    dependency=dependency,
                    started_at=started_at,
                    state=BenchmarkState.CANCELLED,
                    recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                    diagnostics=("benchmark cancelled before execution",),
                )
                return self._store.complete_run(
                    result=result,
                    request_fingerprint=fingerprint,
                )

            observation = self._execute(
                entry,
                request,
                deadline=self._monotonic() + timeout,
            )
            try:
                result = self._result_from_observation(
                    request=request,
                    entry=entry,
                    dependency=dependency,
                    started_at=started_at,
                    observation=observation,
                )
            except (InvalidBenchmarkOutputError, TypeError, ValueError) as exc:
                result = self._result(
                    request=request,
                    entry=entry,
                    dependency=dependency,
                    started_at=started_at,
                    state=BenchmarkState.FAILED,
                    recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                    diagnostics=safe_diagnostics(exc),
                )
            return self._store.complete_run(
                result=result,
                request_fingerprint=fingerprint,
            )
        except Exception:
            self._store.release_run(
                run_id=request.run_id,
                request_fingerprint=fingerprint,
            )
            raise

    def _execute(
        self,
        entry: RegisteredBenchmark,
        request: BenchmarkRunRequest,
        *,
        deadline: float,
    ) -> BenchmarkObservation:
        internal_cancel = threading.Event()
        output: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

        def is_cancelled() -> bool:
            return internal_cancel.is_set() or (
                request.cancellation_token is not None
                and request.cancellation_token.cancelled
            )

        acquired = False
        while not acquired:
            if is_cancelled():
                return BenchmarkObservation(
                    state=BenchmarkState.CANCELLED,
                    recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                    diagnostics=("benchmark cancelled",),
                )
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return BenchmarkObservation(
                    state=BenchmarkState.FAILED,
                    recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                    diagnostics=("benchmark timed out waiting for an execution slot",),
                )
            acquired = entry.execution_slots.acquire(
                timeout=min(self._poll_interval_seconds, remaining)
            )

        context = BenchmarkExecutionContext(
            device_id=request.device_id,
            target_service_id=request.target_service_id,
            dependency_inputs=request.dependency_inputs,
            _deadline=deadline,
            _monotonic=self._monotonic,
            _cancelled=is_cancelled,
        )

        def invoke() -> None:
            try:
                value = entry.probe(context)
                output.put_nowait(("result", value))
            except BaseException as exc:  # contained and converted without traceback
                try:
                    output.put_nowait(("error", exc))
                except queue.Full:
                    pass
            finally:
                entry.execution_slots.release()

        thread = threading.Thread(
            target=invoke,
            name=f"msh-benchmark-{entry.definition.benchmark_id}",
            daemon=True,
        )
        try:
            thread.start()
        except Exception:
            entry.execution_slots.release()
            raise

        while True:
            if is_cancelled():
                internal_cancel.set()
                return BenchmarkObservation(
                    state=BenchmarkState.CANCELLED,
                    recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                    diagnostics=("benchmark cancelled",),
                )
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                internal_cancel.set()
                return BenchmarkObservation(
                    state=BenchmarkState.FAILED,
                    recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                    diagnostics=("benchmark timed out",),
                )
            try:
                kind, value = output.get(
                    timeout=min(self._poll_interval_seconds, remaining)
                )
            except queue.Empty:
                continue
            if kind == "error":
                if isinstance(value, BenchmarkCancelled):
                    return BenchmarkObservation(
                        state=BenchmarkState.CANCELLED,
                        recommendation=BenchmarkRecommendation.RERUN_REQUIRED,
                        diagnostics=("benchmark cancelled",),
                    )
                if isinstance(value, TimeoutError):
                    return BenchmarkObservation(
                        state=BenchmarkState.FAILED,
                        recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                        diagnostics=("benchmark timed out",),
                    )
                return BenchmarkObservation(
                    state=BenchmarkState.FAILED,
                    recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                    diagnostics=safe_diagnostics(value),
                )
            if not isinstance(value, BenchmarkObservation):
                return BenchmarkObservation(
                    state=BenchmarkState.FAILED,
                    recommendation=BenchmarkRecommendation.NOT_RECOMMENDED,
                    diagnostics=("benchmark returned an invalid result type",),
                )
            return value

    @staticmethod
    def _effective_timeout(
        request: BenchmarkRunRequest,
        entry: RegisteredBenchmark,
    ) -> float:
        maximum = float(entry.definition.max_duration_seconds)
        if request.timeout_seconds is None:
            return maximum
        if isinstance(request.timeout_seconds, bool) or request.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        return min(float(request.timeout_seconds), maximum)

    def _result_from_observation(
        self,
        *,
        request: BenchmarkRunRequest,
        entry: RegisteredBenchmark,
        dependency: str,
        started_at: datetime,
        observation: BenchmarkObservation,
    ) -> BenchmarkResult:
        definition = entry.definition
        try:
            state = BenchmarkState(observation.state)
            recommendation = BenchmarkRecommendation(observation.recommendation)
        except (TypeError, ValueError) as exc:
            raise InvalidBenchmarkOutputError(
                "benchmark returned an unknown state or recommendation"
            ) from exc
        if state == BenchmarkState.EXPIRED:
            raise InvalidBenchmarkOutputError(
                "benchmark probes cannot create expired results"
            )
        metrics = bounded_public_mapping(observation.metrics)
        undeclared = sorted(set(metrics) - set(definition.metric_names))
        if undeclared:
            raise InvalidBenchmarkOutputError(
                f"benchmark returned undeclared metric: {undeclared[0]}"
            )
        diagnostics = safe_diagnostics(*observation.diagnostics)
        return self._result(
            request=request,
            entry=entry,
            dependency=dependency,
            started_at=started_at,
            state=state,
            recommendation=recommendation,
            metrics=metrics,
            diagnostics=diagnostics,
        )

    def _result(
        self,
        *,
        request: BenchmarkRunRequest,
        entry: RegisteredBenchmark,
        dependency: str,
        started_at: datetime,
        state: BenchmarkState,
        recommendation: BenchmarkRecommendation,
        metrics: Mapping[str, Any] | None = None,
        diagnostics: tuple[str, ...] = (),
    ) -> BenchmarkResult:
        finished_at = max(started_at, self._utc_now())
        return BenchmarkResult(
            run_id=request.run_id,
            device_id=request.device_id,
            benchmark_id=entry.definition.benchmark_id,
            benchmark_version=entry.definition.implementation_version,
            target_service_id=request.target_service_id,
            state=state,
            metrics=dict(metrics or {}),
            recommendation=recommendation,
            dependency_fingerprint=dependency,
            diagnostics=diagnostics,
            started_at=started_at,
            finished_at=finished_at,
            expires_at=finished_at
            + timedelta(seconds=entry.result_ttl_seconds),
        )

    def _utc_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("benchmark clock must return timezone-aware values")
        return value.astimezone(timezone.utc)

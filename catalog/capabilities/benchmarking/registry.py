"""In-process registry for trusted local benchmark implementations."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import BoundedSemaphore, RLock
from typing import Protocol

from catalog.federation.onboarding_models import BenchmarkDefinition
from catalog.federation.redaction import (
    is_nonpublic_location_key,
    is_sensitive_field_name,
)

from .errors import DuplicateBenchmarkError, UnknownBenchmarkError


class BenchmarkProbe(Protocol):
    """Trusted local probe contract used by the benchmark runner."""

    def __call__(self, context: object) -> object: ...


@dataclass(frozen=True)
class RegisteredBenchmark:
    definition: BenchmarkDefinition
    probe: BenchmarkProbe
    result_ttl_seconds: int
    execution_slots: BoundedSemaphore = field(compare=False, repr=False)


class BenchmarkRegistry:
    """Thread-safe registry with deterministic listing and no implicit discovery."""

    def __init__(self) -> None:
        self._entries: dict[str, RegisteredBenchmark] = {}
        self._lock = RLock()

    def register(
        self,
        definition: BenchmarkDefinition,
        probe: BenchmarkProbe,
        *,
        result_ttl_seconds: int,
    ) -> RegisteredBenchmark:
        if not callable(probe):
            raise TypeError("benchmark probe must be callable")
        if isinstance(result_ttl_seconds, bool) or result_ttl_seconds <= 0:
            raise ValueError("result_ttl_seconds must be positive")
        for metric_name in definition.metric_names:
            if is_sensitive_field_name(metric_name) or is_nonpublic_location_key(
                metric_name
            ):
                raise ValueError(
                    f"unsafe metric name in benchmark {definition.benchmark_id}: "
                    f"{metric_name}"
                )
        entry = RegisteredBenchmark(
            definition=definition,
            probe=probe,
            result_ttl_seconds=int(result_ttl_seconds),
            execution_slots=BoundedSemaphore(definition.max_parallelism),
        )
        with self._lock:
            if definition.benchmark_id in self._entries:
                raise DuplicateBenchmarkError(
                    f"benchmark already registered: {definition.benchmark_id}"
                )
            self._entries[definition.benchmark_id] = entry
        return entry

    def get(self, benchmark_id: str) -> RegisteredBenchmark:
        with self._lock:
            try:
                return self._entries[benchmark_id]
            except KeyError as exc:
                raise UnknownBenchmarkError(
                    f"unknown benchmark: {benchmark_id}"
                ) from exc

    def definitions(self) -> tuple[BenchmarkDefinition, ...]:
        with self._lock:
            return tuple(
                self._entries[key].definition for key in sorted(self._entries)
            )

    def benchmark_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._entries))

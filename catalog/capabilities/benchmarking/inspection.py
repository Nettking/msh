"""Bounded local device inspection for capability-first onboarding."""

from __future__ import annotations

import os
import platform
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from catalog.federation.onboarding_models import DeviceInspectionSnapshot
from catalog.federation.redaction import REDACTED

from .registry import BenchmarkRegistry
from .safety import bounded_public_mapping, safe_diagnostics, safe_text


def _band(value: int | None, boundaries: Sequence[int], labels: Sequence[str]) -> str:
    if value is None or value < 0:
        return "unknown"
    for boundary, label in zip(boundaries, labels, strict=True):
        if value < boundary:
            return label
    return labels[-1]


def _core_band(value: int | None) -> str:
    return _band(
        value,
        (2, 4, 8, 16, 32, 65),
        ("1", "2-3", "4-7", "8-15", "16-31", "32+"),
    )


def _byte_band(value: int | None) -> str:
    gib = 1024**3
    return _band(
        value,
        (2 * gib, 4 * gib, 8 * gib, 16 * gib, 32 * gib, 64 * gib, 128 * gib),
        (
            "under-2-gib",
            "2-3-gib",
            "4-7-gib",
            "8-15-gib",
            "16-31-gib",
            "32-63-gib",
            "64+-gib",
        ),
    )


def _memory_bytes() -> int | None:
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(page_size, int) or not isinstance(page_count, int):
        return None
    return page_size * page_count


def _disk_free_bytes() -> int | None:
    try:
        return shutil.disk_usage(os.path.abspath(os.sep)).free
    except OSError:
        return None


def _safe_values(values: Sequence[object]) -> set[str]:
    result: set[str] = set()
    for value in values:
        candidate = safe_text(value)
        if candidate != REDACTED:
            result.add(candidate)
    return result


@dataclass(frozen=True)
class InspectionContext:
    device_id: str
    registered_benchmark_ids: tuple[str, ...]


@dataclass(frozen=True)
class InspectionFinding:
    """Safe finding returned by a trusted local inspection adapter."""

    resource_observations: Mapping[str, Any] = field(default_factory=dict)
    detected_services: tuple[str, ...] = ()
    registered_handlers: tuple[str, ...] = ()
    detected_data_sources: tuple[str, ...] = ()
    available_prerequisites: tuple[str, ...] = ()
    recommended_benchmark_ids: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class InspectionProbe(Protocol):
    def __call__(self, context: InspectionContext) -> InspectionFinding: ...


class DeviceInspector:
    """Collect coarse local observations without exposing endpoints or paths."""

    def __init__(
        self,
        registry: BenchmarkRegistry,
        *,
        probes: Sequence[InspectionProbe] = (),
        inspection_ttl_seconds: int = 900,
        now: Callable[[], datetime] | None = None,
        system_observer: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        if isinstance(inspection_ttl_seconds, bool) or inspection_ttl_seconds <= 0:
            raise ValueError("inspection_ttl_seconds must be positive")
        self._registry = registry
        self._probes = tuple(probes)
        self._inspection_ttl_seconds = int(inspection_ttl_seconds)
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._system_observer = system_observer or self._default_system_observations

    def inspect(self, *, device_id: str, revision: int) -> DeviceInspectionSnapshot:
        created_at = self._utc_now()
        benchmark_ids = self._registry.benchmark_ids()
        context = InspectionContext(
            device_id=device_id,
            registered_benchmark_ids=benchmark_ids,
        )

        observations = dict(self._system_observer())
        services: set[str] = set()
        handlers: set[str] = set()
        data_sources: set[str] = set()
        prerequisites: set[str] = set()
        requested_benchmarks: set[str] = set()
        warnings: list[str] = []

        for probe in self._probes:
            try:
                finding = probe(context)
                if not isinstance(finding, InspectionFinding):
                    warnings.extend(
                        safe_diagnostics("inspection probe returned an invalid finding")
                    )
                    continue
                observations.update(finding.resource_observations)
                services.update(_safe_values(finding.detected_services))
                handlers.update(_safe_values(finding.registered_handlers))
                data_sources.update(_safe_values(finding.detected_data_sources))
                prerequisites.update(_safe_values(finding.available_prerequisites))
                requested_benchmarks.update(
                    _safe_values(finding.recommended_benchmark_ids)
                )
                warnings.extend(safe_diagnostics(*finding.warnings))
            except Exception as exc:  # noqa: BLE001 - isolate local probes
                warnings.extend(safe_diagnostics(exc))

        registered = set(benchmark_ids)
        unknown = sorted(requested_benchmarks - registered)
        for benchmark_id in unknown:
            warnings.extend(
                safe_diagnostics(
                    f"inspection recommended an unregistered benchmark: "
                    f"{safe_text(benchmark_id)}"
                )
            )

        recommended = requested_benchmarks & registered
        for definition in self._registry.definitions():
            if set(definition.prerequisites).issubset(prerequisites):
                recommended.add(definition.benchmark_id)

        safe_observations = bounded_public_mapping(observations)
        safe_warnings = tuple(dict.fromkeys(safe_diagnostics(*warnings)))
        return DeviceInspectionSnapshot(
            device_id=device_id,
            revision=revision,
            os_family=safe_text(platform.system().casefold() or "unknown"),
            architecture=safe_text(platform.machine().casefold() or "unknown"),
            resource_observations=safe_observations,
            detected_services=tuple(sorted(services)),
            registered_handlers=tuple(sorted(handlers)),
            detected_data_sources=tuple(sorted(data_sources)),
            recommended_benchmark_ids=tuple(sorted(recommended)),
            warnings=safe_warnings,
            created_at=created_at,
            expires_at=created_at
            + timedelta(seconds=self._inspection_ttl_seconds),
        )

    @staticmethod
    def _default_system_observations() -> Mapping[str, Any]:
        return {
            "cpu": {"logical_cores_band": _core_band(os.cpu_count())},
            "memory": {"capacity_band": _byte_band(_memory_bytes())},
            "accelerator": {"status": "not-inspected"},
            "disk": {"free_capacity_band": _byte_band(_disk_free_bytes())},
            "network": {"status": "not-benchmarked"},
        }

    def _utc_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("inspection clock must return timezone-aware values")
        return value.astimezone(timezone.utc)

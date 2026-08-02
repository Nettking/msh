"""Registration helper for concrete CF2-B adapter instances."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ..inspection import InspectionProbe
from ..registry import BenchmarkRegistry


@dataclass(frozen=True)
class ConcreteAdapterRegistration:
    inspection_probes: tuple[InspectionProbe, ...]
    benchmark_ids: tuple[str, ...]


def register_concrete_adapters(
    registry: BenchmarkRegistry,
    adapters: Sequence[Any],
) -> ConcreteAdapterRegistration:
    """Register explicit trusted adapters without discovering or activating anything."""

    probes: list[InspectionProbe] = []
    benchmark_ids: list[str] = []
    for adapter in adapters:
        definition = getattr(adapter, "definition", None)
        benchmark = getattr(adapter, "benchmark", None)
        ttl = getattr(adapter, "result_ttl_seconds", None)
        if definition is None or not callable(benchmark):
            raise TypeError("adapter must expose definition and benchmark")
        registry.register(definition, benchmark, result_ttl_seconds=ttl)
        if not callable(adapter):
            raise TypeError("adapter must also be an inspection probe")
        probes.append(adapter)
        benchmark_ids.append(definition.benchmark_id)
    return ConcreteAdapterRegistration(
        inspection_probes=tuple(probes),
        benchmark_ids=tuple(sorted(benchmark_ids)),
    )

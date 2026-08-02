"""Expiry and dependency-invalidation evaluation for durable results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from catalog.federation.onboarding_models import BenchmarkDefinition, BenchmarkResult

from .fingerprints import dependency_fingerprint


class BenchmarkValidityReason(str, Enum):
    CURRENT = "current"
    EXPIRED = "expired"
    DEFINITION_CHANGED = "definition-changed"
    DEPENDENCY_CHANGED = "dependency-changed"
    BENCHMARK_MISMATCH = "benchmark-mismatch"


@dataclass(frozen=True)
class BenchmarkValidity:
    current: bool
    reason: BenchmarkValidityReason

    @property
    def rerun_required(self) -> bool:
        return not self.current


def evaluate_result(
    result: BenchmarkResult,
    definition: BenchmarkDefinition,
    dependency_inputs: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> BenchmarkValidity:
    """Determine whether existing evidence still matches current local inputs."""

    current_time = now or datetime.now(timezone.utc)
    if result.benchmark_id != definition.benchmark_id:
        return BenchmarkValidity(False, BenchmarkValidityReason.BENCHMARK_MISMATCH)
    if result.benchmark_version != definition.implementation_version:
        return BenchmarkValidity(False, BenchmarkValidityReason.DEFINITION_CHANGED)
    if result.expires_at <= current_time:
        return BenchmarkValidity(False, BenchmarkValidityReason.EXPIRED)
    expected = dependency_fingerprint(definition, dependency_inputs)
    if result.dependency_fingerprint != expected:
        return BenchmarkValidity(False, BenchmarkValidityReason.DEPENDENCY_CHANGED)
    return BenchmarkValidity(True, BenchmarkValidityReason.CURRENT)

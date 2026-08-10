"""Product policy for durable capability evidence lifetimes and reuse."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from catalog.federation.onboarding_models import BenchmarkDefinition, BenchmarkResult

from .fingerprints import dependency_fingerprint
from .validity import BenchmarkValidity, BenchmarkValidityReason

# Inspection and standard benchmark evidence is explicitly refreshed by the
# operator. The finite timestamp remains in the frozen evidence contract, but
# the installed FCP product does not use age alone as a refresh trigger.
DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS = 315_360_000


def evaluate_run_once_result(
    result: BenchmarkResult,
    definition: BenchmarkDefinition,
    dependency_inputs: Mapping[str, Any],
) -> BenchmarkValidity:
    """Validate persisted evidence without treating age alone as invalidation.

    The product-level run-once policy deliberately ignores ``expires_at`` while
    preserving every structural invalidation that can make a saved benchmark
    describe a different capability: benchmark identity, implementation version,
    and the declared dependency fingerprint.
    """

    if result.benchmark_id != definition.benchmark_id:
        return BenchmarkValidity(False, BenchmarkValidityReason.BENCHMARK_MISMATCH)
    if result.benchmark_version != definition.implementation_version:
        return BenchmarkValidity(False, BenchmarkValidityReason.DEFINITION_CHANGED)
    expected = dependency_fingerprint(definition, dependency_inputs)
    if result.dependency_fingerprint != expected:
        return BenchmarkValidity(False, BenchmarkValidityReason.DEPENDENCY_CHANGED)
    return BenchmarkValidity(True, BenchmarkValidityReason.CURRENT)


__all__ = [
    "DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS",
    "evaluate_run_once_result",
]

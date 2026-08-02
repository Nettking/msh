"""Deterministic fingerprints for benchmark requests and invalidation inputs."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from catalog.federation.onboarding_models import BenchmarkDefinition

from .errors import MissingDependencyInputError


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("fingerprint inputs must be finite")
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_value(item) for item in value]
    raise TypeError("fingerprint inputs must be JSON-compatible")


def _digest(value: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def dependency_fingerprint(
    definition: BenchmarkDefinition,
    dependency_inputs: Mapping[str, Any],
) -> str:
    """Bind a result to the definition version and declared invalidation inputs."""

    missing = [
        name for name in definition.invalidation_inputs if name not in dependency_inputs
    ]
    if missing:
        raise MissingDependencyInputError(
            "missing dependency input for benchmark "
            f"{definition.benchmark_id}: {missing[0]}"
        )
    selected = {
        name: dependency_inputs[name]
        for name in sorted(definition.invalidation_inputs)
    }
    return _digest(
        {
            "benchmark_id": definition.benchmark_id,
            "benchmark_version": definition.implementation_version,
            "inputs": selected,
        }
    )


def request_fingerprint(
    *,
    run_id: str,
    device_id: str,
    benchmark_id: str,
    benchmark_version: str,
    target_service_id: str,
    dependency_fingerprint_value: str,
) -> str:
    """Bind a durable run ID to exactly one logical benchmark request."""

    return _digest(
        {
            "run_id": run_id,
            "device_id": device_id,
            "benchmark_id": benchmark_id,
            "benchmark_version": benchmark_version,
            "target_service_id": target_service_id,
            "dependency_fingerprint": dependency_fingerprint_value,
        }
    )

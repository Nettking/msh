"""Shared bounds and deterministic helpers for trusted local adapters."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any

from catalog.federation.redaction import REDACTED

from ..safety import safe_text

MAX_ADAPTER_TARGETS = 64
MAX_PROBE_PAYLOAD_BYTES = 4 * 1024
MAX_PROBE_RESPONSE_BYTES = 64 * 1024


def stable_fingerprint(value: Mapping[str, Any]) -> str:
    """Return a deterministic SHA-256 fingerprint for JSON-compatible inputs."""

    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def safe_identifier(value: object, field: str) -> str:
    """Validate one bounded public logical identifier without echoing bad input."""

    candidate = safe_text(value)
    if candidate == REDACTED or candidate == "unspecified":
        raise ValueError(f"{field} must be safe public logical text")
    return candidate


def bounded_positive_int(
    value: object,
    field: str,
    *,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be between 1 and {maximum}")
    return value


def bounded_seconds(value: object, field: str, *, maximum: float) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive number") from exc
    if not math.isfinite(result) or not 0 < result <= maximum:
        raise ValueError(f"{field} must be greater than zero and at most {maximum}")
    return result


def elapsed_ms(started: float, finished: float) -> float:
    return round(max(0.0, finished - started) * 1000.0, 3)


def dependency_matches(
    supplied: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> bool:
    return all(supplied.get(key) == value for key, value in expected.items())

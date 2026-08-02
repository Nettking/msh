"""Safe, bounded public-output helpers for local inspection and benchmarks."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from catalog.federation.redaction import (
    REDACTED,
    is_nonpublic_location_key,
    is_nonpublic_location_text,
    is_sensitive_field_name,
    is_secret_text,
)

MAX_PUBLIC_DEPTH = 5
MAX_PUBLIC_ITEMS = 64
MAX_PUBLIC_TEXT_BYTES = 384
MAX_PUBLIC_JSON_BYTES = 16 * 1024
_EMBEDDED_POSIX_PATH = re.compile(r"(?<![:\w])/(?:[^/\s]+/)*[^/\s]+")


def _bounded_text(value: object) -> str:
    text = str(value).strip() or "unspecified"
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= MAX_PUBLIC_TEXT_BYTES:
        return text
    clipped = encoded[: MAX_PUBLIC_TEXT_BYTES - 3].decode("utf-8", errors="ignore")
    return f"{clipped}..."


def safe_text(value: object) -> str:
    """Return a bounded diagnostic string with secrets and locations removed."""

    text = _bounded_text(value)
    if (
        is_secret_text(text)
        or is_nonpublic_location_text(text)
        or _EMBEDDED_POSIX_PATH.search(text) is not None
    ):
        return REDACTED
    return text


def safe_diagnostics(*values: object) -> tuple[str, ...]:
    """Normalize diagnostics into unique, safe strings accepted by CF1 models."""

    result: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, BaseException):
            class_name = value.__class__.__name__
            message = safe_text(value)
            candidate = f"{class_name}: {message}"
        elif isinstance(value, (Mapping, list, tuple)):
            candidate = json.dumps(
                sanitize_public_value(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
        else:
            candidate = safe_text(value)
        candidate = safe_text(candidate)
        if candidate not in result:
            result.append(candidate)
        if len(result) >= MAX_PUBLIC_ITEMS:
            break
    return tuple(result)


def sanitize_public_value(value: Any, *, _depth: int = 0) -> Any:
    """Recursively remove private fields and bound JSON-compatible output."""

    if _depth > MAX_PUBLIC_DEPTH:
        return "[truncated]"
    if value is None or isinstance(value, bool) or (
        isinstance(value, int) and not isinstance(value, bool)
    ):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return safe_text(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in list(value.items())[:MAX_PUBLIC_ITEMS]:
            key = safe_text(raw_key)
            if (
                key == REDACTED
                or is_sensitive_field_name(key)
                or is_nonpublic_location_key(key)
            ):
                continue
            result[key] = sanitize_public_value(item, _depth=_depth + 1)
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            sanitize_public_value(item, _depth=_depth + 1)
            for item in list(value)[:MAX_PUBLIC_ITEMS]
        ]
    return safe_text(value)


def bounded_public_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    """Sanitize a mapping and reject output that remains unreasonably large."""

    sanitized = sanitize_public_value(value)
    if not isinstance(sanitized, dict):
        raise TypeError("expected a mapping")
    payload = json.dumps(
        sanitized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(payload) > MAX_PUBLIC_JSON_BYTES:
        raise ValueError("public benchmark output exceeds the size limit")
    return sanitized

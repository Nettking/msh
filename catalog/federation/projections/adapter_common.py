"""Shared helpers for safe, read-only Federation projection adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .safety import assert_public_projection, safe_text


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _value(source: object, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _enum_value(value: object, default: str = "unknown") -> str:
    if isinstance(value, Enum):
        value = value.value
    if not isinstance(value, str):
        return default
    return safe_text(value, "adapter.value", fallback=default)


def _public_text(value: object, fallback: str = "Details unavailable") -> str:
    return safe_text(value, "adapter.text", fallback=fallback)


def _optional_public_text(value: object) -> str | None:
    if value is None:
        return None
    return _public_text(value)


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(timezone.utc)


def _safe_mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    try:
        return assert_public_projection(dict(value), "adapter.mapping")
    except Exception:  # noqa: BLE001
        return {}


def _sequence(value: object) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return tuple(value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(value)
    return ()

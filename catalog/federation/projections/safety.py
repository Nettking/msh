"""Public-projection safety boundary for Federation product view models."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum
from typing import Any

from catalog.federation.errors import FederationValidationError
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
    is_nonpublic_location_key,
    is_sensitive_field_name,
)

MAX_PUBLIC_TEXT_BYTES = 512
MAX_PUBLIC_COLLECTION_ITEMS = 256
MAX_PUBLIC_DEPTH = 8

_PUBLIC_ROUTE = re.compile(r"^/[A-Za-z0-9._~!$&'()*+,;=:@%/?#-]*$")
_BLOCKED_KEYS = frozenset(
    {
        "actor_node_id",
        "authorization",
        "credential",
        "credentials",
        "endpoint",
        "fencing_token",
        "grant_id",
        "internal_session_id",
        "invitation_token",
        "lease_id",
        "local_path",
        "password",
        "private_key",
        "raw_token",
        "secret",
        "session_id",
        "signature",
        "token",
    }
)


def normalized_key(value: object) -> str:
    return str(value).casefold().replace("-", "_").replace(" ", "_")


def is_public_route(value: object) -> bool:
    if not isinstance(value, str) or _PUBLIC_ROUTE.fullmatch(value) is None:
        return False
    return "//" not in value and ".." not in value and "\\" not in value


def safe_text(value: object, field: str, *, fallback: str | None = None) -> str:
    if not isinstance(value, str):
        if fallback is not None:
            return fallback
        raise FederationValidationError(
            "invalid-projection-text",
            field,
            "must be text",
        )
    candidate = value.strip()
    if not candidate or any(ord(character) < 32 for character in candidate):
        if fallback is not None:
            return fallback
        raise FederationValidationError(
            "invalid-projection-text",
            field,
            "must be non-empty printable text",
        )
    if len(candidate.encode("utf-8")) > MAX_PUBLIC_TEXT_BYTES:
        if fallback is not None:
            return fallback
        raise FederationValidationError(
            "projection-text-too-large",
            field,
            f"must not exceed {MAX_PUBLIC_TEXT_BYTES} UTF-8 bytes",
        )
    if contains_secret_material(candidate) or contains_nonpublic_location(candidate):
        if fallback is not None:
            return fallback
        raise FederationValidationError(
            "unsafe-projection-text",
            field,
            "must not expose credentials, endpoints, addresses, or local paths",
        )
    return candidate


def safe_route(value: object, field: str) -> str:
    if not is_public_route(value):
        raise FederationValidationError(
            "invalid-public-route",
            field,
            "must be a relative application route",
        )
    return str(value)


def safe_reason_code(value: object, fallback: str = "details-unavailable") -> str:
    if not isinstance(value, str):
        return fallback
    normalized = value.strip().casefold().replace("_", "-")
    if not re.fullmatch(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", normalized):
        return fallback
    if contains_secret_material(normalized) or contains_nonpublic_location(normalized):
        return fallback
    return normalized


def public_scalar(value: object, field: str) -> str | int | float | bool | None:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise FederationValidationError(
                "invalid-projection-timestamp",
                field,
                "must be timezone-aware",
            )
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return safe_text(value.value, field)
    return safe_text(value, field)


def assert_public_projection(
    value: Any,
    field: str = "projection",
    *,
    depth: int = 0,
) -> Any:
    """Validate and normalize a complete public view-model payload.

    Relative application routes are permitted only in ``url``-suffixed fields.
    Raw session identifiers and authority-fencing values are rejected even if
    they are otherwise ordinary text.
    """

    if depth > MAX_PUBLIC_DEPTH:
        raise FederationValidationError(
            "projection-too-deep",
            field,
            "exceeds the maximum public projection depth",
        )
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, datetime):
        return public_scalar(value, field)
    if isinstance(value, Enum):
        return public_scalar(value, field)
    if isinstance(value, str):
        return safe_text(value, field)
    if isinstance(value, Mapping):
        if len(value) > MAX_PUBLIC_COLLECTION_ITEMS:
            raise FederationValidationError(
                "projection-too-large",
                field,
                "contains too many fields",
            )
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = safe_text(str(raw_key), f"{field}.key")
            normalized = normalized_key(key)
            if (
                normalized in _BLOCKED_KEYS
                or is_sensitive_field_name(normalized)
                or (
                    is_nonpublic_location_key(normalized)
                    and normalized != "url"
                    and not normalized.endswith("_url")
                )
            ):
                raise FederationValidationError(
                    "private-projection-field",
                    f"{field}.{key}",
                    "must not be present in a public Federation view model",
                )
            if normalized == "url" or normalized.endswith("_url"):
                result[key] = safe_route(item, f"{field}.{key}")
            else:
                result[key] = assert_public_projection(
                    item,
                    f"{field}.{key}",
                    depth=depth + 1,
                )
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) > MAX_PUBLIC_COLLECTION_ITEMS:
            raise FederationValidationError(
                "projection-too-large",
                field,
                "contains too many items",
            )
        return [
            assert_public_projection(item, f"{field}[{index}]", depth=depth + 1)
            for index, item in enumerate(value)
        ]
    raise FederationValidationError(
        "invalid-projection-value",
        field,
        "must contain only public JSON-compatible values",
    )

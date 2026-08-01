"""Logical endpoint identifier validation for F7.6 control messages."""

from __future__ import annotations

import ipaddress
import re
from typing import Any

from catalog.federation.errors import FederationValidationError

_LOGICAL_ENDPOINT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")


def validate_logical_artifact_endpoint(value: Any, field: str = "endpoint_id") -> str:
    """Return one logical ID and reject network locations or credentials."""

    if not isinstance(value, str) or not value.strip():
        raise FederationValidationError(
            "invalid-logical-endpoint-id",
            field,
            "must be a non-empty logical endpoint identifier",
        )
    endpoint_id = value.strip()
    unsafe_token = any(
        token in endpoint_id for token in ("://", ":", "@", "?", "#", "\\")
    )
    unsafe_path = (
        endpoint_id.startswith("/")
        or endpoint_id.endswith("/")
        or ".." in endpoint_id.split("/")
    )
    try:
        ipaddress.ip_address(endpoint_id.strip("[]"))
    except ValueError:
        is_ip_literal = False
    else:
        is_ip_literal = True
    if (
        unsafe_token
        or unsafe_path
        or is_ip_literal
        or not _LOGICAL_ENDPOINT_RE.fullmatch(endpoint_id)
    ):
        raise FederationValidationError(
            "invalid-logical-endpoint-id",
            field,
            "must be a logical ID, not an address, URL, credential, or traversal path",
        )
    return endpoint_id

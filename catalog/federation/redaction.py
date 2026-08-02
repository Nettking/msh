"""Defense-in-depth classification and redaction for federation data."""

from __future__ import annotations

import re
from typing import Any, Final

REDACTED: Final = "[redacted]"
SECRET_FIELD_NAMES: Final = frozenset(
    {
        "api_key",
        "authorization",
        "client_secret",
        "credential",
        "enrollment_token",
        "invitation_token",
        "password",
        "private_key",
        "raw_token",
        "refresh_token",
        "secret",
        "signature",
        "token",
    }
)
SECRET_TEXT_PREFIXES: Final = (
    "msh_enroll_",
    "msh_join_",
    "bearer ",
    "-----begin private key-----",
    "-----begin encrypted private key-----",
)
NONPUBLIC_LOCATION_KEYS: Final = frozenset(
    {
        "address",
        "backend_root",
        "database",
        "database_id",
        "directory",
        "dsn",
        "endpoint",
        "file",
        "file_path",
        "filesystem_path",
        "host",
        "path",
        "physical_address",
        "port",
        "root",
        "storage_address",
        "uri",
        "url",
    }
)
NONPUBLIC_LOCATION_PREFIXES: Final = (
    "/",
    "\\",
    "file:",
    "http://",
    "https://",
    "postgres:",
    "postgresql:",
    "s3:",
    "sqlite:",
)
EMBEDDED_LOCATION_MARKERS: Final = tuple(
    prefix for prefix in NONPUBLIC_LOCATION_PREFIXES if prefix not in {"/", "\\"}
)
_IPV4_LOCATION = re.compile(
    r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?"
)
_WINDOWS_LOCATION = re.compile(r"(?<![A-Za-z0-9])[A-Za-z]:[\\/]")


def is_secret_text(value: object) -> bool:
    """Recognize MSH credentials and common private credential encodings."""

    return isinstance(value, str) and any(
        marker in value.casefold() for marker in SECRET_TEXT_PREFIXES
    )


def is_sensitive_field_name(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.casefold().replace("-", "_").replace(" ", "_")
    parts = set(normalized.split("_"))
    return (
        normalized in SECRET_FIELD_NAMES
        or bool(
            parts
            & {
                "authorization",
                "credential",
                "password",
                "secret",
                "signature",
                "token",
            }
        )
        or normalized.endswith(("_api_key", "_private_key"))
    )


def is_nonpublic_location_key(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.casefold().replace("-", "_").replace(" ", "_")
    return normalized in NONPUBLIC_LOCATION_KEYS or normalized.endswith(
        ("_address", "_directory", "_dsn", "_path", "_root", "_uri", "_url")
    )


def is_nonpublic_location_text(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip().casefold()
    return (
        normalized.startswith(NONPUBLIC_LOCATION_PREFIXES)
        or any(marker in normalized for marker in EMBEDDED_LOCATION_MARKERS)
        or _IPV4_LOCATION.search(normalized) is not None
        or _WINDOWS_LOCATION.search(value) is not None
    )


def contains_secret_material(value: Any) -> bool:
    """Return whether a value contains credential-shaped data."""

    if is_secret_text(value):
        return True
    if isinstance(value, dict):
        return any(
            is_sensitive_field_name(str(key)) or contains_secret_material(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(contains_secret_material(item) for item in value)
    return False


def contains_nonpublic_location(value: Any) -> bool:
    """Return whether a value exposes backend or physical location details."""

    if is_nonpublic_location_text(value):
        return True
    if isinstance(value, dict):
        return any(
            is_nonpublic_location_key(str(key)) or contains_nonpublic_location(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(contains_nonpublic_location(item) for item in value)
    return False


def contains_nonpublic_transport_data(value: Any) -> bool:
    """Validate the combined relay policy without conflating its two reasons."""

    return contains_secret_material(value) or contains_nonpublic_location(value)


def redact_secret_material(value: Any) -> Any:
    """Recursively redact credentials while preserving ordinary location text."""

    if is_secret_text(value):
        return REDACTED
    if isinstance(value, dict):
        return {
            key: (
                REDACTED
                if is_sensitive_field_name(str(key))
                else redact_secret_material(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_secret_material(item) for item in value]
    return value


def redact_nonpublic_data(value: Any) -> Any:
    """Recursively redact both credentials and backend location details."""

    if is_secret_text(value) or is_nonpublic_location_text(value):
        return REDACTED
    if isinstance(value, dict):
        return {
            key: (
                REDACTED
                if is_sensitive_field_name(str(key))
                or is_nonpublic_location_key(str(key))
                else redact_nonpublic_data(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_nonpublic_data(item) for item in value]
    return value


def redact_secrets(value: Any) -> Any:
    """Backward-compatible strict redaction used by existing Phase 2 callers."""

    return redact_nonpublic_data(value)

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
    "fcp_enroll_",
    "fcp_join_",
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
_MTCONNECT_CONTENT_SCHEMA: Final = "fcp.mtconnect.observations.v1"


def is_secret_text(value: object) -> bool:
    """Recognize FCP credentials and common private credential encodings."""

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


def _is_public_mtconnect_component_path(value: object) -> bool:
    """Recognize the recorder's logical component path, never a backend path."""

    if value is None:
        return True
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 512
        or "\\" in value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or is_nonpublic_location_text(value)
    ):
        return False
    parts = value.split("/")
    return bool(parts) and all(part not in {"", ".", ".."} for part in parts)


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


def _redact_mtconnect_observation(value: Any) -> Any:
    if not isinstance(value, dict):
        return redact_nonpublic_data(value)
    return {
        key: (
            item
            if key == "component_path" and _is_public_mtconnect_component_path(item)
            else REDACTED
            if is_sensitive_field_name(str(key))
            or is_nonpublic_location_key(str(key))
            else redact_nonpublic_data(item)
        )
        for key, item in value.items()
    }


def redact_nonpublic_data(value: Any) -> Any:
    """Recursively redact credentials and backend location details.

    MTConnect recorder observations contain a logical ``component_path`` such
    as ``Linear/X``. The name ends in ``_path`` but the value is not a filesystem
    or network location. Preserve only that one field when it occurs directly
    in an exact recorder observation schema and its value passes the bounded
    logical-path check above. Every other ``*_path`` field remains redacted.
    """

    if is_secret_text(value) or is_nonpublic_location_text(value):
        return REDACTED
    if isinstance(value, dict):
        recorder_content = value.get("schema") == _MTCONNECT_CONTENT_SCHEMA
        return {
            key: (
                [_redact_mtconnect_observation(entry) for entry in item]
                if recorder_content
                and key == "observations"
                and isinstance(item, (list, tuple))
                else REDACTED
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

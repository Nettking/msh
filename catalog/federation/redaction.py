"""Shared defense-in-depth redaction for federation status and audit data."""

from __future__ import annotations

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
    return normalized.startswith(NONPUBLIC_LOCATION_PREFIXES) or (
        len(normalized) >= 3
        and normalized[0].isalpha()
        and normalized[1] == ":"
        and normalized[2] in {"\\", "/"}
    )


def redact_secrets(value: Any) -> Any:
    """Recursively redact secret-named fields and credential-shaped strings."""

    if is_secret_text(value) or is_nonpublic_location_text(value):
        return REDACTED
    if isinstance(value, dict):
        return {
            key: (
                REDACTED
                if is_sensitive_field_name(str(key))
                or is_nonpublic_location_key(str(key))
                else redact_secrets(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_secrets(item) for item in value]
    return value

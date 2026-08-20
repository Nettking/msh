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
_MTCONNECT_OBSERVATION_SCHEMA: Final = "fcp.mtconnect.observation.v2"
_MTCONNECT_COMPONENT_KEYS: Final = frozenset({"type", "id", "name", "native_name"})
_MAX_MTCONNECT_COMPONENT_DEPTH: Final = 64
_MAX_MTCONNECT_COMPONENT_TEXT_BYTES: Final = 512


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


def _is_public_mtconnect_component_text(
    value: object,
    *,
    required: bool,
) -> bool:
    if value is None:
        return not required
    if (
        not isinstance(value, str)
        or (required and not value)
        or value != value.strip()
        or len(value.encode("utf-8")) > _MAX_MTCONNECT_COMPONENT_TEXT_BYTES
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
        or is_secret_text(value)
        or is_nonpublic_location_text(value)
    ):
        return False
    return bool(value) or not required


def _is_public_mtconnect_component_path(value: object) -> bool:
    """Recognize the recorder's bounded structural MTConnect probe path.

    ``parse_probe`` stores ``component_path`` as a list of component metadata
    dictionaries, not as a filesystem-style string.  Admit only that exact
    public structure.  Every textual element is independently checked so a
    path-shaped field cannot be used to smuggle a credential, URL, IP address,
    drive path, or absolute path through the relay filter.
    """

    if not isinstance(value, (list, tuple)) or len(value) > _MAX_MTCONNECT_COMPONENT_DEPTH:
        return False
    for component in value:
        if not isinstance(component, dict) or set(component) != _MTCONNECT_COMPONENT_KEYS:
            return False
        if not _is_public_mtconnect_component_text(component.get("type"), required=True):
            return False
        for key in ("id", "name", "native_name"):
            if not _is_public_mtconnect_component_text(component.get(key), required=False):
                return False
    return True


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
    recorder_observation = value.get("schema") == _MTCONNECT_OBSERVATION_SCHEMA
    return {
        key: (
            item
            if recorder_observation
            and key == "component_path"
            and _is_public_mtconnect_component_path(item)
            else REDACTED
            if is_sensitive_field_name(str(key))
            or is_nonpublic_location_key(str(key))
            else redact_nonpublic_data(item)
        )
        for key, item in value.items()
    }


def redact_nonpublic_data(value: Any) -> Any:
    """Recursively redact credentials and backend location details.

    Recorder observations carry ``component_path`` as probe-derived component
    metadata such as ``[{"type": "Linear", "id": "x", ...}]``.  Its key ends
    in ``_path`` even though it identifies an MTConnect model hierarchy rather
    than a filesystem or network location.  Preserve only that exact bounded
    structure inside the exact recorder content and observation schemas.  Every
    other ``*_path`` field remains redacted.
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

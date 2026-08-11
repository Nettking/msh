"""Federation-scoped public device-name validation.

Device names are human-facing metadata only. They never replace the stable
cryptographic ``node_id`` or grant membership, routing, provider, storage,
compute, or software-update authority.
"""

from __future__ import annotations

from .errors import FederationValidationError
from .redaction import is_nonpublic_location_text, is_secret_text

DEVICE_NAME_EVENT = "node.display-name.changed"
MAX_DEVICE_NAME_BYTES = 96
_RESERVED_NAMES = frozenset(
    {
        "this device",
        "this fcp device",
        "trusted fcp device",
    }
)


def validate_device_name(value: object) -> str:
    """Return one bounded public-safe Federation device name."""

    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-device-name",
            "display_name",
            "must be text",
        )
    name = value.strip()
    if not name or any(ord(character) < 32 for character in name):
        raise FederationValidationError(
            "invalid-device-name",
            "display_name",
            "must be non-empty text without control characters",
        )
    if len(name.encode("utf-8")) > MAX_DEVICE_NAME_BYTES:
        raise FederationValidationError(
            "device-name-too-large",
            "display_name",
            f"must not exceed {MAX_DEVICE_NAME_BYTES} UTF-8 bytes",
        )
    if name.casefold() in _RESERVED_NAMES:
        raise FederationValidationError(
            "reserved-device-name",
            "display_name",
            "must distinguish this device from generic Federation labels",
        )
    if is_secret_text(name):
        raise FederationValidationError(
            "secret-device-name",
            "display_name",
            "must not contain credentials or secrets",
        )
    if is_nonpublic_location_text(name):
        raise FederationValidationError(
            "nonpublic-device-name",
            "display_name",
            "must not expose backend paths or physical storage addresses",
        )
    return name


__all__ = ["DEVICE_NAME_EVENT", "MAX_DEVICE_NAME_BYTES", "validate_device_name"]

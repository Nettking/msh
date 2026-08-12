"""Shared wire-level primitives for Federation control-plane commands.

This module deliberately stops at command metadata and wire validation. It is
not a scheduler, job store, transport, authorization policy, or executor.
Feature-specific commands retain their existing schemas and execution semantics.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Sequence

MAX_COMMAND_REQUEST_ID_LENGTH = 128
MAX_COMMAND_NODE_ID_LENGTH = 512
MAX_COMMAND_TARGETS = 256
COMMAND_CLOCK_SKEW = timedelta(minutes=1)


def stamp_utc(value: datetime) -> str:
    """Serialize an aware timestamp in the existing Federation UTC wire form."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("malformed_timestamp")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc_stamp(value: object) -> datetime:
    """Parse one aware Federation timestamp and normalize it to UTC."""

    if not isinstance(value, str):
        raise TypeError("malformed_timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("malformed_timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("malformed_timestamp")
    return parsed.astimezone(timezone.utc)


def ensure_bounded_json(value: object, *, max_bytes: int, error_code: str) -> None:
    """Apply the existing canonical JSON byte-size guard to a wire payload."""

    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > max_bytes:
        raise ValueError(error_code)


def correlated_event_request_id(prefix: str, request_id: str, node_id: str) -> str:
    """Return the stable bounded event request ID used by control-plane reports."""

    digest = hashlib.sha256(f"{request_id}\0{node_id}".encode()).hexdigest()[:32]
    return f"{prefix}-{digest}"


def _request_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_COMMAND_REQUEST_ID_LENGTH
    ):
        raise ValueError("malformed_request_id")
    return value


def _targets(
    value: object,
    *,
    max_targets: int,
    deduplicate: bool,
    require_unique: bool,
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("malformed_targets")
    targets = tuple(value)
    if any(
        not isinstance(item, str)
        or not item
        or len(item) > MAX_COMMAND_NODE_ID_LENGTH
        for item in targets
    ):
        raise ValueError("malformed_targets")
    if deduplicate:
        targets = tuple(dict.fromkeys(targets))
    if not 1 <= len(targets) <= max_targets:
        raise ValueError("malformed_targets")
    if require_unique and len(set(targets)) != len(targets):
        raise ValueError("malformed_targets")
    return targets


@dataclass(frozen=True)
class ControlCommandEnvelope:
    """Common metadata carried by bounded Federation control commands."""

    request_id: str
    target_node_ids: tuple[str, ...]
    created_at: datetime
    expires_at: datetime

    @classmethod
    def issue(
        cls,
        *,
        request_id: str,
        target_node_ids: Sequence[str],
        created_at: datetime,
        expires_at: datetime,
        max_lifetime: timedelta,
        max_targets: int = MAX_COMMAND_TARGETS,
    ) -> "ControlCommandEnvelope":
        request = _request_id(request_id)
        targets = _targets(
            tuple(target_node_ids),
            max_targets=max_targets,
            deduplicate=True,
            require_unique=False,
        )
        created = _aware_utc(created_at)
        expires = _aware_utc(expires_at)
        if expires <= created or expires - created > max_lifetime:
            raise ValueError("invalid_lifetime")
        return cls(request, targets, created, expires)

    @classmethod
    def parse_payload(
        cls,
        value: Mapping[str, object],
        *,
        max_lifetime: timedelta,
        max_targets: int = MAX_COMMAND_TARGETS,
        require_unique_targets: bool,
        now: datetime | None = None,
    ) -> "ControlCommandEnvelope":
        request = _request_id(value.get("request_id"))
        targets = _targets(
            value.get("target_node_ids"),
            max_targets=max_targets,
            deduplicate=False,
            require_unique=require_unique_targets,
        )
        created = parse_utc_stamp(value.get("created_at"))
        expires = parse_utc_stamp(value.get("expires_at"))
        reference = _aware_utc(now or datetime.now(timezone.utc))
        if (
            created > reference + COMMAND_CLOCK_SKEW
            or expires <= reference
            or expires <= created
            or expires - created > max_lifetime
        ):
            raise ValueError("expired_or_invalid_request")
        return cls(request, targets, created, expires)

    def payload_fields(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "target_node_ids": list(self.target_node_ids),
            "created_at": stamp_utc(self.created_at),
            "expires_at": stamp_utc(self.expires_at),
        }


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("malformed_timestamp")
    return value.astimezone(timezone.utc)


__all__ = [
    "COMMAND_CLOCK_SKEW",
    "MAX_COMMAND_NODE_ID_LENGTH",
    "MAX_COMMAND_REQUEST_ID_LENGTH",
    "MAX_COMMAND_TARGETS",
    "ControlCommandEnvelope",
    "correlated_event_request_id",
    "ensure_bounded_json",
    "parse_utc_stamp",
    "stamp_utc",
]

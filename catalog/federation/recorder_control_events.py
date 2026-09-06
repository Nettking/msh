"""Bounded Federation control protocol for MTConnect recorder nodes.

Recorder control is declarative and session-scoped.  Trusted Federation members
may request a bounded private-network scan or change the set of sources selected
from the recorder's latest scan.  Commands never carry shell text, credentials,
or arbitrary executable/network authority: scans are still constrained by the
recorder's existing RFC1918 /24 discovery validator and source additions refer
only to opaque IDs from the recorder's own latest scan.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from ipaddress import IPv4Network, ip_network
from typing import Any, Final

from .errors import FederationValidationError
from .redaction import REDACTED

SCHEMA: Final = "fcp.recorder-control.v1"
SCAN_REQUEST_EVENT: Final = "recorder.control.scan.requested"
SCAN_REPORT_EVENT: Final = "recorder.control.scan.reported"
SOURCES_REQUEST_EVENT: Final = "recorder.control.sources.requested"
SOURCES_REPORT_EVENT: Final = "recorder.control.sources.reported"

#: The only event types whose payload may carry a bounded scan network.
SCAN_EVENTS: Final = frozenset({SCAN_REQUEST_EVENT, SCAN_REPORT_EVENT})
COMMAND_TTL: Final = timedelta(minutes=2)
MAX_COMMAND_LIFETIME: Final = timedelta(minutes=5)
MAX_SOURCE_ITEMS: Final = 64
MAX_SCAN_RESULT_ITEMS: Final = 64
MAX_SCAN_ADDRESSES: Final = 256
MIN_SCAN_PREFIX_LENGTH: Final = 24

#: The same RFC1918 space the recorder's discovery validator admits.
_RFC1918_NETWORKS: Final = (
    IPv4Network("10.0.0.0/8"),
    IPv4Network("172.16.0.0/12"),
    IPv4Network("192.168.0.0/16"),
)


def is_publishable_scan_cidr(value: Any) -> bool:
    """Return whether ``value`` is a scan CIDR the recorder would itself accept.

    "Publishable" means the relay may route it, not that the address is public.
    The predicate admits *only* private RFC1918 space; a publicly routable
    address is exactly what it refuses.

    This is the relay-side mirror of the recorder's discovery security
    contract: an explicit IPv4 RFC1918 network, ``/24`` or smaller, of at most
    ``MAX_SCAN_ADDRESSES`` addresses. It is deliberately a predicate rather
    than a parser -- it decides only whether a value may be exempted from the
    generic privacy filter, and it never widens what a scan may target.

    ``catalog.flask_app.services.mtconnect_discovery_service.validate_scan_cidr``
    remains the enforcing validator on the recorder side. The relay cannot
    import it without taking a dependency on the Flask application layer, so
    the contract is restated here and pinned to that validator by test.
    """

    if not isinstance(value, str):
        return False
    raw = value.strip()
    if "/" not in raw or raw != value:
        return False
    try:
        network = ip_network(raw, strict=False)
    except ValueError:
        return False
    return (
        isinstance(network, IPv4Network)
        and network.prefixlen >= MIN_SCAN_PREFIX_LENGTH
        and network.num_addresses <= MAX_SCAN_ADDRESSES
        and any(network.subnet_of(private) for private in _RFC1918_NETWORKS)
    )


def _is_bounded_scan_port(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and 1 <= value <= 65535


def _is_scan_control_payload(value: Any) -> bool:
    """Return whether ``value`` is exactly one recorder-control scan payload.

    Both ``recorder.control.scan.requested`` and
    ``recorder.control.scan.reported`` carry ``command == "scan"`` under this
    schema; the source-change events carry ``"sources"`` and neither declares a
    network field. The relay validates a payload without its event type, so the
    payload's own ``command`` is what distinguishes them.
    """

    return (
        isinstance(value, dict)
        and value.get("schema") == SCHEMA
        and value.get("command") == "scan"
    )


def mask_recorder_control_scan_event_payload(value: Any) -> Any:
    """Mask only the scan fields of one recorder-control scan event payload.

    The relay rejects any payload the generic redaction pass would alter.
    Recorder control intentionally carries a bounded private scan network, so
    two fields of a scan payload are always altered by that pass and the whole
    protocol is unroutable without an exemption:

    * ``cidr`` is redacted by *value*, because it reads as an address.
    * ``port`` is redacted by *key*, because ``port`` is a location key. This
      holds even when ``cidr`` is empty, so allowing ``cidr`` alone would not
      make a single scan event routable.

    Returning those two fields pre-masked lets the relay compare against an
    expected redaction instead of relaxing the filter.

    This deliberately does **not** recurse. The exemption belongs to the event
    payload at the protocol boundary and to nothing else, so an unrelated outer
    payload cannot carry a scan-shaped object in a nested field or a list and
    inherit the allowance for it. The caller is additionally responsible for
    applying this only to the two scan event types; the payload alone cannot
    prove which event carried it.

    A ``cidr`` outside the recorder's RFC1918 ``/24``-or-smaller contract, or a
    port outside 1-65535, is left untouched so the ordinary filter still
    rejects it.
    """

    if not _is_scan_control_payload(value):
        return value
    masked = dict(value)
    cidr = value.get("cidr")
    # A scan payload earns the exemption only as a whole. An absent or empty
    # CIDR is the ordinary "let the recorder infer it" request; a CIDR that
    # satisfies the contract is masked. Anything else -- IPv6, public space,
    # wider than /24, malformed -- earns nothing at all, not even the port
    # exemption, so such a payload stays rejected exactly as it was before
    # this allowance existed.
    if cidr is None or cidr == "":
        if _is_bounded_scan_port(value.get("port")):
            masked["port"] = REDACTED
    elif is_publishable_scan_cidr(cidr):
        masked["cidr"] = REDACTED
        if _is_bounded_scan_port(value.get("port")):
            masked["port"] = REDACTED
    return masked


def _stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-recorder-control-time",
            "timestamp",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-recorder-control-time",
            field,
            "must be RFC 3339 text",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FederationValidationError(
            "invalid-recorder-control-time",
            field,
            "must be RFC 3339 text",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FederationValidationError(
            "invalid-recorder-control-time",
            field,
            "must be timezone-aware",
        )
    return parsed.astimezone(timezone.utc)


def _text(value: Any, field: str, *, maximum: int = 512, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-recorder-control-field",
            field,
            "must be text",
        )
    normalized = value.strip()
    if (
        (not normalized and not allow_empty)
        or len(normalized.encode("utf-8")) > maximum
        or any(ord(character) < 32 for character in normalized)
    ):
        raise FederationValidationError(
            "invalid-recorder-control-field",
            field,
            f"must be printable text no longer than {maximum} bytes",
        )
    return normalized


def _list(value: Any, field: str, *, maximum: int = MAX_SOURCE_ITEMS) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum:
        raise FederationValidationError(
            "invalid-recorder-control-field",
            field,
            f"must be a list with at most {maximum} items",
        )
    return tuple(dict.fromkeys(_text(item, field, maximum=512) for item in value))


def _command_base(
    *,
    request_id: str,
    target_node_id: str,
    now: datetime,
) -> dict[str, Any]:
    created = now.astimezone(timezone.utc)
    return {
        "schema": SCHEMA,
        "request_id": _text(request_id, "request_id", maximum=128),
        "target_node_id": _text(target_node_id, "target_node_id"),
        "created_at": _stamp(created),
        "expires_at": _stamp(created + COMMAND_TTL),
    }


def scan_command_payload(
    *,
    request_id: str,
    target_node_id: str,
    cidr: str | None,
    port: int,
    now: datetime,
) -> dict[str, Any]:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise FederationValidationError(
            "invalid-recorder-control-field",
            "port",
            "must be between 1 and 65535",
        )
    return {
        **_command_base(request_id=request_id, target_node_id=target_node_id, now=now),
        "command": "scan",
        "cidr": _text(cidr or "", "cidr", maximum=64, allow_empty=True),
        "port": port,
    }


def sources_command_payload(
    *,
    request_id: str,
    target_node_id: str,
    scan_id: str,
    add_source_ids: list[str] | tuple[str, ...],
    remove_source_names: list[str] | tuple[str, ...],
    now: datetime,
) -> dict[str, Any]:
    additions = _list(list(add_source_ids), "add_source_ids")
    removals = _list(list(remove_source_names), "remove_source_names")
    if not additions and not removals:
        raise FederationValidationError(
            "empty-recorder-control-change",
            "sources",
            "select at least one source to add or remove",
        )
    return {
        **_command_base(request_id=request_id, target_node_id=target_node_id, now=now),
        "command": "sources",
        "scan_id": _text(scan_id, "scan_id", maximum=128),
        "add_source_ids": list(additions),
        "remove_source_names": list(removals),
    }


def parse_command(
    event_type: str,
    payload: Any,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("schema") != SCHEMA:
        raise FederationValidationError(
            "invalid-recorder-control-schema",
            "payload",
            "does not use the supported recorder-control schema",
        )
    request_id = _text(payload.get("request_id"), "request_id", maximum=128)
    target_node_id = _text(payload.get("target_node_id"), "target_node_id")
    created = _parse_stamp(payload.get("created_at"), "created_at")
    expires = _parse_stamp(payload.get("expires_at"), "expires_at")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if (
        created > current + timedelta(seconds=30)
        or expires <= current
        or expires <= created
        or expires - created > MAX_COMMAND_LIFETIME
    ):
        raise FederationValidationError(
            "expired-recorder-control-command",
            "expires_at",
            "the recorder-control command is expired or has an invalid lifetime",
        )
    if event_type == SCAN_REQUEST_EVENT:
        if payload.get("command") != "scan":
            raise FederationValidationError(
                "invalid-recorder-control-command",
                "command",
                "does not match the scan event type",
            )
        port = payload.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise FederationValidationError(
                "invalid-recorder-control-field",
                "port",
                "must be between 1 and 65535",
            )
        return {
            "command": "scan",
            "request_id": request_id,
            "target_node_id": target_node_id,
            "cidr": _text(payload.get("cidr") or "", "cidr", maximum=64, allow_empty=True),
            "port": port,
        }
    if event_type == SOURCES_REQUEST_EVENT:
        if payload.get("command") != "sources":
            raise FederationValidationError(
                "invalid-recorder-control-command",
                "command",
                "does not match the source-change event type",
            )
        return {
            "command": "sources",
            "request_id": request_id,
            "target_node_id": target_node_id,
            "scan_id": _text(payload.get("scan_id"), "scan_id", maximum=128),
            "add_source_ids": _list(payload.get("add_source_ids"), "add_source_ids"),
            "remove_source_names": _list(payload.get("remove_source_names"), "remove_source_names"),
        }
    raise FederationValidationError(
        "unsupported-recorder-control-event",
        "event_type",
        "is not a supported recorder-control command",
    )


def scan_report_payload(
    *,
    request_id: str,
    target_node_id: str,
    scan_id: str,
    state: str,
    cidr: str,
    port: int,
    results: list[dict[str, Any]],
    configured_source_names: list[str] | tuple[str, ...],
    message: str,
    error_code: str | None = None,
    now: datetime,
) -> dict[str, Any]:
    safe_results: list[dict[str, Any]] = []
    for item in results[:MAX_SCAN_RESULT_ITEMS]:
        if not isinstance(item, dict):
            continue
        safe_results.append(
            {
                "source_id": _text(item.get("source_id"), "source_id", maximum=128),
                "source_name": _text(item.get("source_name"), "source_name", maximum=128),
                "display_name": _text(item.get("display_name"), "display_name", maximum=256),
                "machine_count": max(1, min(int(item.get("machine_count") or 1), 64)),
            }
        )
    return {
        "schema": SCHEMA,
        "command": "scan",
        "request_id": _text(request_id, "request_id", maximum=128),
        "target_node_id": _text(target_node_id, "target_node_id"),
        "scan_id": _text(scan_id, "scan_id", maximum=128),
        "state": _text(state, "state", maximum=64),
        "cidr": _text(cidr, "cidr", maximum=64, allow_empty=True),
        "port": port,
        "results": safe_results,
        "configured_source_names": list(_list(list(configured_source_names), "configured_source_names")),
        "message": _text(message, "message", maximum=512, allow_empty=True),
        "error_code": None if error_code is None else _text(error_code, "error_code", maximum=128),
        "completed_at": _stamp(now),
    }


def sources_report_payload(
    *,
    request_id: str,
    target_node_id: str,
    scan_id: str,
    state: str,
    configured_source_names: list[str] | tuple[str, ...],
    message: str,
    error_code: str | None = None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "command": "sources",
        "request_id": _text(request_id, "request_id", maximum=128),
        "target_node_id": _text(target_node_id, "target_node_id"),
        "scan_id": _text(scan_id, "scan_id", maximum=128),
        "state": _text(state, "state", maximum=64),
        "configured_source_names": list(_list(list(configured_source_names), "configured_source_names")),
        "message": _text(message, "message", maximum=512, allow_empty=True),
        "error_code": None if error_code is None else _text(error_code, "error_code", maximum=128),
        "completed_at": _stamp(now),
    }


__all__ = [
    "COMMAND_TTL",
    "MAX_SCAN_ADDRESSES",
    "MAX_SCAN_RESULT_ITEMS",
    "MAX_SOURCE_ITEMS",
    "MIN_SCAN_PREFIX_LENGTH",
    "SCAN_EVENTS",
    "SCAN_REPORT_EVENT",
    "SCAN_REQUEST_EVENT",
    "SCHEMA",
    "SOURCES_REPORT_EVENT",
    "SOURCES_REQUEST_EVENT",
    "is_publishable_scan_cidr",
    "mask_recorder_control_scan_event_payload",
    "parse_command",
    "scan_command_payload",
    "scan_report_payload",
    "sources_command_payload",
    "sources_report_payload",
]

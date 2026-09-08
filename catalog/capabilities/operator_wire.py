"""Closed decoding of the existing public provider operator projection.

This is display data, never authority to execute a provider or approve a request.
Exact announcement metadata stays within the relay's existing provider authority.
"""

from __future__ import annotations

import json
from dataclasses import fields
from datetime import datetime, timezone

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorAction,
    ProviderOperatorSnapshot,
    ProviderOperatorView,
)
from catalog.capabilities.provider_enrollment import ProviderEnrollmentState
from catalog.capabilities.provider_health import ProviderHealthState
from catalog.capabilities.provider_reports import ProviderStatus
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityStatus
from catalog.federation.protocol import MAX_MESSAGE_BYTES
from catalog.federation.redaction import redact_secrets


def _invalid() -> FederationOperationError:
    return FederationOperationError(
        "invalid-provider-operator-response",
        "relay did not return a consistent public operator projection",
    )


def _closed(value: object, names: set[str]) -> dict:
    if not isinstance(value, dict) or set(value) != names:
        raise _invalid()
    try:
        size = len(json.dumps(value, allow_nan=False).encode("utf-8"))
    except (TypeError, ValueError, RecursionError) as exc:
        raise _invalid() from exc
    if size > MAX_MESSAGE_BYTES or redact_secrets(value) != value:
        raise _invalid()
    return value


def _text(value: object) -> str:
    try:
        size = len(value.encode("utf-8")) if isinstance(value, str) else 0
    except UnicodeEncodeError as exc:
        raise _invalid() from exc
    if (
        not isinstance(value, str) or not value or value != value.strip()
        or size > 512
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise _invalid()
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise _invalid()
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _invalid() from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise _invalid()
    return result.astimezone(timezone.utc)


def operator_snapshot_from_dict(
    value: object, *, session_id: str, capability_id: str | None = None,
) -> ProviderOperatorSnapshot:
    names = {field.name for field in fields(ProviderOperatorSnapshot)}
    raw = _closed(value, names | {"schema"})
    if (
        raw["schema"] != ProviderOperatorSnapshot.SCHEMA
        or raw["session_id"] != session_id
        or (capability_id is not None and raw["capability_id"] != capability_id)
    ):
        raise _invalid()
    decoded = {key: raw[key] for key in names}
    for name in (
        "session_id", "capability_id", "node_id", "capability_type", "protocol",
        "protocol_version", "announcement_status", "enrollment_state",
        "enrollment_reason_code", "health_state", "health_reason_code",
        "activation_reason_code",
    ):
        _text(raw[name])
    for name in ("discovered", "can_manage"):
        if type(raw[name]) is not bool:
            raise _invalid()
    if raw["inventory_compatible"] is not None and type(raw["inventory_compatible"]) is not bool:
        raise _invalid()
    for name in (
        "enrollment_revision", "provider_generation", "report_revision",
        "max_concurrent_jobs", "active_jobs", "queue_depth", "utilization_millis",
    ):
        number = raw[name]
        minimum = 1 if name in {"enrollment_revision", "provider_generation", "report_revision"} else 0
        if number is not None and (type(number) is not int or not minimum <= number <= 2**63 - 1):
            raise _invalid()
    for name in ("announced_at", "reported_at", "expires_at"):
        decoded[name] = None if raw[name] is None else _timestamp(raw[name])
    if raw["announcement_status"] not in {item.value for item in CapabilityStatus} | {"absent"}:
        raise _invalid()
    if raw["enrollment_state"] not in {item.value for item in ProviderEnrollmentState} | {"not-requested"}:
        raise _invalid()
    if raw["health_state"] not in {item.value for item in ProviderHealthState}:
        raise _invalid()
    if raw["provider_status"] is not None and (
        not isinstance(raw["provider_status"], str)
        or raw["provider_status"] not in {item.value for item in ProviderStatus}
    ):
        raise _invalid()
    actions = raw["allowed_actions"]
    if not isinstance(actions, (list, tuple)) or len(actions) > len(ProviderOperatorAction):
        raise _invalid()
    try:
        decoded["activation_state"] = ProviderActivationState(raw["activation_state"])
        decoded["allowed_actions"] = tuple(ProviderOperatorAction(action) for action in actions)
    except (TypeError, ValueError) as exc:
        raise _invalid() from exc
    if len(set(decoded["allowed_actions"])) != len(actions) or (actions and not raw["can_manage"]):
        raise _invalid()
    return ProviderOperatorSnapshot(**decoded)


def operator_view_from_dict(
    value: object, *, session_id: str, actor_node_id: str,
) -> ProviderOperatorView:
    raw = _closed(value, {
        "schema", "session_id", "actor_node_id", "is_owner", "generated_at", "providers",
    })
    if (
        raw["schema"] != ProviderOperatorView.SCHEMA
        or raw["session_id"] != session_id or raw["actor_node_id"] != actor_node_id
        or type(raw["is_owner"]) is not bool or not isinstance(raw["providers"], list)
    ):
        raise _invalid()
    providers = tuple(
        operator_snapshot_from_dict(row, session_id=session_id)
        for row in raw["providers"]
    )
    if (
        len({row.capability_id for row in providers}) != len(providers)
        or any(row.can_manage != raw["is_owner"] for row in providers)
    ):
        raise _invalid()
    return ProviderOperatorView(
        session_id, actor_node_id, raw["is_owner"], _timestamp(raw["generated_at"]), providers,
    )

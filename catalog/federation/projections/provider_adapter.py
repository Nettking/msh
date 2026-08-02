"""Adapter for the existing provider operator surface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .adapter_common import (
    _enum_value,
    _optional_public_text,
    _public_text,
    _sequence,
    _value,
)
from .safety import safe_reason_code


@dataclass(frozen=True)
class ProviderRecord:
    capability_id: str
    node_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    enrollment_state: str
    health_state: str
    provider_status: str | None
    activation_state: str
    reason_code: str
    max_concurrent_jobs: int | None
    active_jobs: int | None
    queue_depth: int | None
    utilization_millis: int | None
    can_manage: bool
    allowed_actions: tuple[str, ...]


@dataclass(frozen=True)
class ProviderSnapshot:
    available: bool
    reason_code: str
    is_owner: bool = False
    providers: tuple[ProviderRecord, ...] = ()


class ProviderOperatorSurfaceLike(Protocol):
    def view(self) -> object: ...


class ProviderOperatorAdapter:
    """Narrow the existing safe operator surface to product-facing fields."""

    def __init__(self, surface: ProviderOperatorSurfaceLike | None) -> None:
        self._surface = surface

    def snapshot(self) -> ProviderSnapshot:
        if self._surface is None:
            return ProviderSnapshot(False, "provider-surface-unavailable")
        try:
            view = self._surface.view()
            providers: list[ProviderRecord] = []
            for provider in _sequence(_value(view, "providers", ())):
                providers.append(
                    ProviderRecord(
                        capability_id=_public_text(
                            _value(provider, "capability_id"),
                            "unknown-capability",
                        ),
                        node_id=_public_text(
                            _value(provider, "node_id"),
                            "unknown-device",
                        ),
                        capability_type=_public_text(
                            _value(provider, "capability_type"),
                            "unknown-capability",
                        ),
                        protocol=_public_text(
                            _value(provider, "protocol"),
                            "unknown-protocol",
                        ),
                        protocol_version=_public_text(
                            _value(provider, "protocol_version"),
                            "unknown",
                        ),
                        enrollment_state=_enum_value(
                            _value(provider, "enrollment_state"),
                            "not-requested",
                        ),
                        health_state=_enum_value(
                            _value(provider, "health_state"),
                            "absent",
                        ),
                        provider_status=_optional_public_text(
                            _value(provider, "provider_status")
                        ),
                        activation_state=_enum_value(
                            _value(provider, "activation_state"),
                            "unavailable",
                        ),
                        reason_code=safe_reason_code(
                            _value(provider, "activation_reason_code"),
                            "details-unavailable",
                        ),
                        max_concurrent_jobs=(
                            _value(provider, "max_concurrent_jobs")
                            if isinstance(_value(provider, "max_concurrent_jobs"), int)
                            else None
                        ),
                        active_jobs=(
                            _value(provider, "active_jobs")
                            if isinstance(_value(provider, "active_jobs"), int)
                            else None
                        ),
                        queue_depth=(
                            _value(provider, "queue_depth")
                            if isinstance(_value(provider, "queue_depth"), int)
                            else None
                        ),
                        utilization_millis=(
                            _value(provider, "utilization_millis")
                            if isinstance(_value(provider, "utilization_millis"), int)
                            else None
                        ),
                        can_manage=bool(_value(provider, "can_manage", False)),
                        allowed_actions=tuple(
                            _enum_value(action, "unknown")
                            for action in _sequence(
                                _value(provider, "allowed_actions", ())
                            )
                        ),
                    )
                )
            return ProviderSnapshot(
                True,
                "current",
                bool(_value(view, "is_owner", False)),
                tuple(sorted(providers, key=lambda item: item.capability_id)),
            )
        except Exception as exc:  # noqa: BLE001
            return ProviderSnapshot(
                False,
                safe_reason_code(getattr(exc, "code", None), "provider-projection-failed"),
            )

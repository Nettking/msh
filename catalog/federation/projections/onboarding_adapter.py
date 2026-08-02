"""Adapters for frozen CF1 onboarding and contribution contracts."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .adapter_common import (
    _enum_value,
    _optional_public_text,
    _public_text,
    _safe_mapping,
    _sequence,
    _timestamp,
    _value,
)
from .safety import safe_reason_code


@dataclass(frozen=True)
class ContributionRecord:
    candidate_id: str
    device_id: str
    capability_type: str
    protocol: str
    label: str
    capacity: dict[str, Any]
    missing_prerequisites: tuple[str, ...]
    policy_state: str
    desired_state: str
    activation_state: str
    reason: str | None


@dataclass(frozen=True)
class OnboardingSnapshot:
    available: bool
    reason_code: str
    federation_id: str | None = None
    connection_state: str = "unavailable"
    trusted: bool = False
    device_id: str | None = None
    inspection_revision: int | None = None
    inspection_expires_at: datetime | None = None
    os_family: str | None = None
    architecture: str | None = None
    resource_observations: dict[str, Any] = field(default_factory=dict)
    detected_services: tuple[str, ...] = ()
    registered_handlers: tuple[str, ...] = ()
    detected_data_sources: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    contributions: tuple[ContributionRecord, ...] = ()


class OnboardingContractsAdapter:
    """Adapt frozen CF1 models without exposing ``internal_session_id``."""

    def __init__(
        self,
        *,
        binding: object | None = None,
        inspection: object | None = None,
        candidates: Iterable[object] = (),
        intents: Iterable[object] = (),
    ) -> None:
        self._binding = binding
        self._inspection = inspection
        self._candidates = tuple(candidates)
        self._intents = tuple(intents)

    def snapshot(self) -> OnboardingSnapshot:
        if self._binding is None and self._inspection is None:
            return OnboardingSnapshot(False, "onboarding-state-unavailable")
        try:
            intents = {
                _public_text(_value(intent, "candidate_id"), "unknown-candidate"): intent
                for intent in self._intents
            }
            contributions: list[ContributionRecord] = []
            for candidate in self._candidates:
                candidate_id = _public_text(
                    _value(candidate, "candidate_id"),
                    "unknown-candidate",
                )
                intent = intents.get(candidate_id)
                capacity = _safe_mapping(_value(candidate, "capacity_envelope", {}))
                contributions.append(
                    ContributionRecord(
                        candidate_id=candidate_id,
                        device_id=_public_text(
                            _value(candidate, "device_id"),
                            "unknown-device",
                        ),
                        capability_type=_public_text(
                            _value(candidate, "capability_type"),
                            "unknown-capability",
                        ),
                        protocol=_public_text(
                            _value(candidate, "capability_protocol"),
                            "unknown-protocol",
                        ),
                        label=_public_text(
                            _value(candidate, "display_label"),
                            "Unnamed service",
                        ),
                        capacity=capacity,
                        missing_prerequisites=tuple(
                            _public_text(item, "Unspecified prerequisite")
                            for item in _sequence(
                                _value(candidate, "missing_prerequisites", ())
                            )
                        ),
                        policy_state=_enum_value(
                            _value(candidate, "policy_state"),
                            "not-evaluated",
                        ),
                        desired_state=_enum_value(
                            _value(intent, "desired_state") if intent else None,
                            "ask-later",
                        ),
                        activation_state=_enum_value(
                            _value(intent, "activation_state") if intent else None,
                            "inactive",
                        ),
                        reason=_optional_public_text(
                            _value(intent, "reason") if intent else None
                        ),
                    )
                )

            binding = self._binding
            inspection = self._inspection
            federation_id = (
                None
                if binding is None
                else _public_text(
                    _value(binding, "federation_id"),
                    "unknown-federation",
                )
            )
            device_id = _value(inspection, "device_id") if inspection else None
            if device_id is None and binding is not None:
                device_id = _value(binding, "device_id")
            warnings = tuple(
                _public_text(item, "A local inspection warning is available")
                for item in _sequence(_value(inspection, "warnings", ()))
            )
            return OnboardingSnapshot(
                available=True,
                reason_code="current",
                federation_id=federation_id,
                connection_state=_enum_value(
                    _value(binding, "state") if binding else None,
                    "unavailable",
                ),
                trusted=bool(_value(binding, "trusted", False)),
                device_id=(
                    None
                    if device_id is None
                    else _public_text(device_id, "unknown-device")
                ),
                inspection_revision=(
                    _value(inspection, "revision")
                    if isinstance(_value(inspection, "revision"), int)
                    else None
                ),
                inspection_expires_at=_timestamp(_value(inspection, "expires_at")),
                os_family=_optional_public_text(_value(inspection, "os_family")),
                architecture=_optional_public_text(
                    _value(inspection, "architecture")
                ),
                resource_observations=_safe_mapping(
                    _value(inspection, "resource_observations", {})
                ),
                detected_services=tuple(
                    _public_text(item, "Detected service")
                    for item in _sequence(
                        _value(inspection, "detected_services", ())
                    )
                ),
                registered_handlers=tuple(
                    _public_text(item, "Registered handler")
                    for item in _sequence(
                        _value(inspection, "registered_handlers", ())
                    )
                ),
                detected_data_sources=tuple(
                    _public_text(item, "Detected data source")
                    for item in _sequence(
                        _value(inspection, "detected_data_sources", ())
                    )
                ),
                warnings=warnings,
                contributions=tuple(
                    sorted(contributions, key=lambda item: item.candidate_id)
                ),
            )
        except Exception as exc:  # noqa: BLE001 - adapter must degrade safely
            return OnboardingSnapshot(
                False,
                safe_reason_code(
                    getattr(exc, "code", None),
                    "onboarding-projection-failed",
                ),
            )

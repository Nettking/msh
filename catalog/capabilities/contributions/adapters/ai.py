"""Language-model candidate and runtime-authority adapters."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    DeviceInspectionSnapshot,
)

from ..types import AdapterOutcome, LocalContributionDescriptor


@dataclass(frozen=True)
class AICandidateSpec:
    service_id: str
    protocol: str
    display_label: str
    capacity_envelope: dict[str, Any]
    missing_prerequisites: tuple[str, ...] = ()


class AICandidateSource:
    def __init__(self, services: Mapping[str, AICandidateSpec]) -> None:
        self._services = dict(services)

    def descriptors(
        self,
        inspection: DeviceInspectionSnapshot,
    ) -> tuple[LocalContributionDescriptor, ...]:
        detected = set(inspection.detected_services)
        return tuple(
            LocalContributionDescriptor(
                logical_service_id=spec.service_id,
                capability_type="language-model",
                capability_protocol=spec.protocol,
                display_label=spec.display_label,
                capacity_envelope={
                    **spec.capacity_envelope,
                    "provider_id": spec.service_id,
                },
                missing_prerequisites=spec.missing_prerequisites,
            )
            for service_id, spec in sorted(self._services.items())
            if service_id in detected
        )


class AIContributionAdapter:
    """Bridge only to AI runtime registration; it has no compute/storage access."""

    candidate_only = False

    def __init__(
        self,
        *,
        enable_provider: Callable[[ContributionCandidate], None],
        disable_provider: Callable[[str], None],
        is_provider_active: Callable[[str], bool],
    ) -> None:
        self._enable_provider = enable_provider
        self._disable_provider = disable_provider
        self._is_provider_active = is_provider_active

    @classmethod
    def for_runtime_manager(
        cls,
        manager: Any,
        *,
        provider_factory: Callable[[ContributionCandidate], Any],
    ) -> "AIContributionAdapter":
        def enable(candidate: ContributionCandidate) -> None:
            provider_id = cls._provider_id(candidate)
            if provider_id in manager.additional_provider_ids():
                return
            provider = provider_factory(candidate)
            if getattr(provider, "capability_id", None) != provider_id:
                raise ValueError("AI provider identity does not match candidate")
            manager.register(provider)

        return cls(
            enable_provider=enable,
            disable_provider=lambda provider_id: manager.unregister(provider_id),
            is_provider_active=lambda provider_id: (
                provider_id in manager.additional_provider_ids()
            ),
        )

    def supports(self, candidate: ContributionCandidate) -> bool:
        return candidate.capability_type == "language-model"

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._require(candidate)
        self._enable_provider(candidate)
        return AdapterOutcome(ContributionActivationState.ACTIVE)

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._require(candidate)
        self._disable_provider(self._provider_id(candidate))
        return AdapterOutcome(ContributionActivationState.INACTIVE)

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        self._require(candidate)
        self._disable_provider(self._provider_id(candidate))
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        self._require(candidate)
        provider_id = self._provider_id(candidate)
        if desired_state is ContributionDesiredState.DISABLED:
            self._disable_provider(provider_id)
            return AdapterOutcome(ContributionActivationState.INACTIVE)
        if not self._is_provider_active(provider_id):
            self._enable_provider(candidate)
        return AdapterOutcome(ContributionActivationState.ACTIVE)

    @staticmethod
    def _provider_id(candidate: ContributionCandidate) -> str:
        value = candidate.capacity_envelope.get("provider_id")
        if not isinstance(value, str) or not value:
            raise ValueError("AI candidate is missing provider_id")
        return value

    def _require(self, candidate: ContributionCandidate) -> None:
        if not self.supports(candidate):
            raise ValueError("AI adapter received an unsupported candidate")

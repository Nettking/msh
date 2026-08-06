"""Storage candidate adapter with no authority-assignment operation."""

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
class StorageCandidateSpec:
    provider_id: str
    protocol: str
    display_label: str
    capacity_envelope: dict[str, Any]
    missing_prerequisites: tuple[str, ...] = ()


class StorageCandidateSource:
    def __init__(self, providers: Mapping[str, StorageCandidateSpec]) -> None:
        self._providers = dict(providers)

    def descriptors(
        self,
        inspection: DeviceInspectionSnapshot,
    ) -> tuple[LocalContributionDescriptor, ...]:
        detected = set(inspection.detected_services)
        return tuple(
            LocalContributionDescriptor(
                logical_service_id=spec.provider_id,
                capability_type="storage",
                capability_protocol=spec.protocol,
                display_label=spec.display_label,
                capacity_envelope={
                    **spec.capacity_envelope,
                    "provider_id": spec.provider_id,
                    "authority": "candidate-only",
                },
                missing_prerequisites=spec.missing_prerequisites,
            )
            for provider_id, spec in sorted(self._providers.items())
            if provider_id in detected
        )


class StorageContributionAdapter:
    """Observe existing control-plane assignment; never create it."""

    candidate_only = True

    def __init__(
        self,
        *,
        is_assigned: Callable[[str], bool],
        fence_candidate: Callable[[str], None],
    ) -> None:
        self._is_assigned = is_assigned
        self._fence_candidate = fence_candidate

    def supports(self, candidate: ContributionCandidate) -> bool:
        return (
            candidate.capability_type == "storage"
            and candidate.capacity_envelope.get("authority") == "candidate-only"
        )

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        provider_id = self._provider_id(candidate)
        if self._is_assigned(provider_id):
            return AdapterOutcome(
                ContributionActivationState.ACTIVE,
                authority_confirmed=True,
            )
        return AdapterOutcome(
            ContributionActivationState.PENDING,
            "Waiting for federation storage assignment. Your preference is saved, "
            "but this device has not been granted storage authority.",
        )

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._fence_candidate(self._provider_id(candidate))
        return AdapterOutcome(ContributionActivationState.INACTIVE)

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        self._fence_candidate(self._provider_id(candidate))
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        provider_id = self._provider_id(candidate)
        if desired_state is ContributionDesiredState.DISABLED:
            self._fence_candidate(provider_id)
            return AdapterOutcome(ContributionActivationState.INACTIVE)
        if self._is_assigned(provider_id):
            return AdapterOutcome(
                ContributionActivationState.ACTIVE,
                authority_confirmed=True,
            )
        return AdapterOutcome(
            ContributionActivationState.PENDING,
            "Waiting for federation storage assignment. Your preference is saved, "
            "but this device has not been granted storage authority.",
        )

    def _provider_id(self, candidate: ContributionCandidate) -> str:
        if not self.supports(candidate):
            raise ValueError("storage adapter received an unsupported candidate")
        value = candidate.capacity_envelope.get("provider_id")
        if not isinstance(value, str) or not value:
            raise ValueError("storage candidate is missing provider_id")
        return value

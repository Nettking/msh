"""Recorder candidate and authority adapters."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    DeviceInspectionSnapshot,
)

from ..types import AdapterOutcome, LocalContributionDescriptor


class RecorderControlAuthority(Protocol):
    def set_enabled(self, enabled: bool, settings: Any) -> tuple[bool, str]: ...

    def status(self, settings: Any) -> dict[str, Any]: ...


class RecorderCandidateSource:
    def __init__(self, *, protocol: str = "mtconnect") -> None:
        self._protocol = protocol

    def descriptors(
        self,
        inspection: DeviceInspectionSnapshot,
    ) -> tuple[LocalContributionDescriptor, ...]:
        source_ids = tuple(sorted(inspection.detected_data_sources))
        if not source_ids:
            return ()
        return (
            LocalContributionDescriptor(
                logical_service_id="recorder-local",
                capability_type="recorder",
                capability_protocol=self._protocol,
                display_label="Local data recorder",
                capacity_envelope={
                    "source_count": len(source_ids),
                    "source_ids": source_ids,
                },
            ),
        )


class RecorderContributionAdapter:
    candidate_only = False

    def __init__(
        self,
        authority: RecorderControlAuthority,
        *,
        settings_provider: Callable[[], Any],
    ) -> None:
        self._authority = authority
        self._settings_provider = settings_provider

    def supports(self, candidate: ContributionCandidate) -> bool:
        return candidate.capability_type == "recorder"

    def enable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._require(candidate)
        _, message = self._authority.set_enabled(True, self._settings_provider())
        return AdapterOutcome(ContributionActivationState.ACTIVE, message)

    def disable(self, candidate: ContributionCandidate) -> AdapterOutcome:
        self._require(candidate)
        _, message = self._authority.set_enabled(False, self._settings_provider())
        return AdapterOutcome(ContributionActivationState.INACTIVE, message)

    def suspend(
        self,
        candidate: ContributionCandidate,
        *,
        reason: str,
    ) -> AdapterOutcome:
        self._require(candidate)
        self._authority.set_enabled(False, self._settings_provider())
        return AdapterOutcome(ContributionActivationState.SUSPENDED, reason)

    def reconcile(
        self,
        candidate: ContributionCandidate,
        *,
        desired_state: ContributionDesiredState,
    ) -> AdapterOutcome:
        self._require(candidate)
        settings = self._settings_provider()
        status = self._authority.status(settings)
        if desired_state is ContributionDesiredState.DISABLED:
            if bool(status.get("requested_enabled")):
                self._authority.set_enabled(False, settings)
            return AdapterOutcome(ContributionActivationState.INACTIVE)
        if bool(status.get("requested_enabled")):
            return AdapterOutcome(ContributionActivationState.ACTIVE)
        _, message = self._authority.set_enabled(True, settings)
        return AdapterOutcome(ContributionActivationState.ACTIVE, message)

    def _require(self, candidate: ContributionCandidate) -> None:
        if not self.supports(candidate):
            raise ValueError("recorder adapter received an unsupported candidate")

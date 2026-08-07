"""Align legacy Federation device rows with capability-first read-only state."""

from __future__ import annotations

from dataclasses import replace

from catalog.federation.projections import (
    FederationAuthoritySnapshot,
    OnboardingSnapshot,
    ProviderSnapshot,
)


class CapabilityFirstFederationDeviceAdapter:
    """Enrich existing member rows without creating membership or authority.

    The Federation authority remains the source of the visible device set. This
    adapter only aligns presentation fields that the installed product knows
    more accurately through the authenticated capability-first composition:

    * the local device is connected when its saved binding is connected/trusted;
    * visible service counts include deduplicated contribution/provider IDs;
    * a remote device's default self-label is rendered relative to this viewer.

    Legacy capability counts are retained as a lower bound because the legacy
    status surface exposes only a count, not IDs that can be safely deduplicated.
    """

    def __init__(
        self,
        authority: object,
        onboarding: object,
        providers: object,
    ) -> None:
        self._authority = authority
        self._onboarding = onboarding
        self._providers = providers

    def snapshot(self) -> FederationAuthoritySnapshot:
        federation = self._authority.snapshot()
        onboarding = self._onboarding.snapshot()
        providers = self._providers.snapshot()
        if not isinstance(federation, FederationAuthoritySnapshot):
            return federation
        if not isinstance(onboarding, OnboardingSnapshot):
            return federation
        if not isinstance(providers, ProviderSnapshot):
            return federation

        service_ids: dict[str, set[str]] = {}
        for contribution in onboarding.contributions:
            service_ids.setdefault(contribution.device_id, set()).add(
                contribution.candidate_id
            )
        for provider in providers.providers:
            service_ids.setdefault(provider.node_id, set()).add(
                provider.capability_id
            )

        aligned = []
        for device in federation.devices:
            is_local = device.node_id == onboarding.device_id
            label = device.label
            if (
                not is_local
                and onboarding.device_id is not None
                and label.strip().casefold() == "this msh device"
            ):
                label = "Trusted MSH device"

            state = device.state
            if (
                is_local
                and onboarding.connection_state == "connected"
                and onboarding.trusted
            ):
                state = "connected"

            capability_count = max(
                device.capability_count,
                len(service_ids.get(device.node_id, ())),
            )
            aligned.append(
                replace(
                    device,
                    label=label,
                    state=state,
                    capability_count=capability_count,
                )
            )

        generic_remote_indexes = [
            index
            for index, device in enumerate(aligned)
            if device.node_id != onboarding.device_id
            and device.label == "Trusted MSH device"
        ]
        if len(generic_remote_indexes) > 1:
            for ordinal, index in enumerate(generic_remote_indexes, start=1):
                aligned[index] = replace(
                    aligned[index],
                    label=f"Trusted MSH device {ordinal}",
                )

        return replace(federation, devices=tuple(aligned))


__all__ = ["CapabilityFirstFederationDeviceAdapter"]

"""Align Federation device labels with capability-first read-only state."""

from __future__ import annotations

from dataclasses import replace

from catalog.federation.projections import (
    FederationAuthoritySnapshot,
    OnboardingSnapshot,
)


class CapabilityFirstFederationDeviceAdapter:
    """Keep the Federation authority as the device/status/service source.

    The previous adapter locally inflated the current device's service count from
    inspection candidates and locally forced its connection state to connected.
    Those fields were therefore different depending on which trusted member was
    viewing the same Federation. Service metadata and liveness now come from the
    authenticated shared coordinator; this adapter only makes a generic remote
    self-label relative to the current viewer.
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
        if not isinstance(federation, FederationAuthoritySnapshot):
            return federation
        if not isinstance(onboarding, OnboardingSnapshot):
            return federation

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
            aligned.append(replace(device, label=label))

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

"""Align Federation device labels with capability-first read-only state."""

from __future__ import annotations

from dataclasses import replace

from catalog.federation.projections import (
    FederationAuthoritySnapshot,
    OnboardingSnapshot,
)


class CapabilityFirstFederationDeviceAdapter:
    """Keep Federation authority as the shared device and capability source.

    The coordinator exposes the complete announced capability inventory, including
    disabled and unavailable entries. The Devices surface, however, describes a
    device's currently usable services. When shared capability metadata is
    available, therefore, the displayed service count includes only capabilities
    whose coordinator-authorized status is ``ready``. Disabled capabilities remain
    visible on the Services surface and are never discarded from authority state.

    The adapter also makes a generic remote self-label relative to this viewer.
    It never creates membership, liveness, provider, storage, or compute authority.
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

        # ``capability_count`` on the authority record is the complete announced
        # inventory. The product-facing Devices card should instead answer the
        # operator question "how many services can this device contribute now?".
        # Only translate the count when detailed shared capability metadata exists;
        # retaining the authority count is the compatibility fallback for older
        # snapshots that expose only the aggregate.
        active_by_node: dict[str, int] | None = None
        if federation.capabilities:
            active_by_node = {}
            for capability in federation.capabilities:
                if capability.status == "ready":
                    active_by_node[capability.node_id] = (
                        active_by_node.get(capability.node_id, 0) + 1
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
            capability_count = device.capability_count
            if active_by_node is not None:
                capability_count = active_by_node.get(device.node_id, 0)
            aligned.append(
                replace(
                    device,
                    label=label,
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

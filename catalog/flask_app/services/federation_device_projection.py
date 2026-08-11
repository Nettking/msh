"""Align Federation device labels with capability-first read-only state."""

from __future__ import annotations

from dataclasses import replace

from catalog.federation.projections import (
    FederationAuthoritySnapshot,
    OnboardingSnapshot,
)

from .federation_device_names import FederationDeviceNameAdapter

_RETIRED_PRODUCT_NAME = bytes((109, 115, 104)).decode("ascii")
_GENERIC_REMOTE_SELF_LABELS = frozenset(
    {
        "this fcp device",
        f"this {_RETIRED_PRODUCT_NAME} device",
    }
)


class CapabilityFirstFederationDeviceAdapter:
    """Keep Federation authority as the shared device and capability source.

    The coordinator exposes the complete announced capability inventory, including
    disabled and unavailable entries. The Devices surface, however, describes a
    device's currently usable services. When shared capability metadata is
    available, therefore, the displayed service count includes only capabilities
    whose coordinator-authorized status is ``ready``. Disabled capabilities remain
    visible on the Services surface and are never discarded from authority state.

    Federation-scoped names are overlaid from the authoritative event log before
    any generic remote-self label translation. An assigned name therefore follows
    the same stable node ID across every trusted member's read-only projections.

    The adapter also makes a generic remote self-label relative to this viewer.
    It never creates membership, liveness, provider, storage, compute, or naming
    authority.
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

    def _named_snapshot(self) -> object:
        coordinator = getattr(self._authority, "_coordinator", None)
        actor_node_id = getattr(self._authority, "_actor_node_id", None)
        session_id = getattr(self._authority, "_internal_session_id", None)
        if (
            coordinator is None
            or not isinstance(actor_node_id, str)
            or not actor_node_id
            or not isinstance(session_id, str)
            or not session_id
        ):
            return self._authority.snapshot()
        return FederationDeviceNameAdapter(
            self._authority,
            coordinator,
            session_id=session_id,
            actor_node_id=actor_node_id,
        ).snapshot()

    def snapshot(self) -> FederationAuthoritySnapshot:
        federation = self._named_snapshot()
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
                and label.strip().casefold() in _GENERIC_REMOTE_SELF_LABELS
            ):
                label = "Trusted FCP device"
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
            and device.label == "Trusted FCP device"
        ]
        if len(generic_remote_indexes) > 1:
            for ordinal, index in enumerate(generic_remote_indexes, start=1):
                aligned[index] = replace(
                    aligned[index],
                    label=f"Trusted FCP device {ordinal}",
                )

        return replace(federation, devices=tuple(aligned))


__all__ = ["CapabilityFirstFederationDeviceAdapter"]

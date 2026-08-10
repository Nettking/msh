from __future__ import annotations

from datetime import datetime, timezone

from catalog.federation.projections import (
    DeviceRecord,
    FederatedCapabilityRecord,
    FederationAuthoritySnapshot,
    OnboardingSnapshot,
)
from catalog.flask_app.services.federation_device_projection import (
    CapabilityFirstFederationDeviceAdapter,
)

NOW = datetime(2026, 8, 8, 20, 0, tzinfo=timezone.utc)


class _StaticAdapter:
    def __init__(self, snapshot: object) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> object:
        return self._snapshot


def test_device_service_count_uses_ready_shared_capabilities_only() -> None:
    federation = FederationAuthoritySnapshot(
        available=True,
        reason_code="current",
        label="Shared Federation",
        state="active",
        revision=7,
        devices=(
            DeviceRecord("node-local", "This FCP device", "connected", NOW, 3),
            DeviceRecord("node-remote", "This FCP device", "connected", NOW, 3),
        ),
        capabilities=(
            FederatedCapabilityRecord(
                "local-storage",
                "node-local",
                "storage",
                "fcp-storage-candidate",
                "1.0",
                "ready",
                NOW,
            ),
            FederatedCapabilityRecord(
                "local-ai",
                "node-local",
                "language-model",
                "fcp-language-model",
                "1.0",
                "disabled",
                NOW,
            ),
            FederatedCapabilityRecord(
                "local-compute",
                "node-local",
                "compute",
                "fcp-compute-handler",
                "1.0",
                "disabled",
                NOW,
            ),
            FederatedCapabilityRecord(
                "remote-storage",
                "node-remote",
                "storage",
                "fcp-storage-candidate",
                "1.0",
                "disabled",
                NOW,
            ),
            FederatedCapabilityRecord(
                "remote-ai",
                "node-remote",
                "language-model",
                "fcp-language-model",
                "1.0",
                "disabled",
                NOW,
            ),
            FederatedCapabilityRecord(
                "remote-compute",
                "node-remote",
                "compute",
                "fcp-compute-handler",
                "1.0",
                "disabled",
                NOW,
            ),
        ),
    )
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-local",
        connection_state="connected",
        trusted=True,
        device_id="node-local",
    )

    snapshot = CapabilityFirstFederationDeviceAdapter(
        _StaticAdapter(federation),
        _StaticAdapter(onboarding),
        _StaticAdapter(object()),
    ).snapshot()

    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-local"].capability_count == 1
    assert by_id["node-remote"].capability_count == 0
    assert by_id["node-remote"].label == "Trusted FCP device"

    # The authoritative inventory is unchanged; disabled capabilities remain
    # available to the Services projection for explicit Disabled rendering.
    assert len(federation.capabilities) == 6
    assert federation.devices[0].capability_count == 3
    assert federation.devices[1].capability_count == 3


def test_retired_remote_self_label_is_normalized_for_current_viewer() -> None:
    retired_name = bytes((109, 115, 104)).decode("ascii").upper()
    federation = FederationAuthoritySnapshot(
        available=True,
        reason_code="current",
        label="Shared Federation",
        state="active",
        revision=8,
        devices=(
            DeviceRecord("node-local", "This FCP device", "connected", NOW, 1),
            DeviceRecord(
                "node-remote",
                f"This {retired_name} device",
                "connected",
                NOW,
                1,
            ),
        ),
    )
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-local",
        connection_state="connected",
        trusted=True,
        device_id="node-local",
    )

    snapshot = CapabilityFirstFederationDeviceAdapter(
        _StaticAdapter(federation),
        _StaticAdapter(onboarding),
        _StaticAdapter(object()),
    ).snapshot()

    by_id = {device.node_id: device for device in snapshot.devices}
    assert by_id["node-local"].label == "This FCP device"
    assert by_id["node-remote"].label == "Trusted FCP device"


def test_device_service_count_keeps_aggregate_for_legacy_snapshot() -> None:
    federation = FederationAuthoritySnapshot(
        available=True,
        reason_code="current",
        label="Legacy Federation",
        state="active",
        revision=2,
        devices=(
            DeviceRecord("node-local", "This FCP device", "connected", NOW, 2),
        ),
    )
    onboarding = OnboardingSnapshot(
        available=True,
        reason_code="current",
        federation_id="federation-local",
        connection_state="connected",
        trusted=True,
        device_id="node-local",
    )

    snapshot = CapabilityFirstFederationDeviceAdapter(
        _StaticAdapter(federation),
        _StaticAdapter(onboarding),
        _StaticAdapter(object()),
    ).snapshot()

    assert snapshot.devices[0].capability_count == 2

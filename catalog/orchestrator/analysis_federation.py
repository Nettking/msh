"""Bind this device to the F8 authorities its analysis capability needs.

Two deployments, one architecture:

*Federated* — the device is enrolled in a real federation. The coordinator, the
relay client, enrollment, health and dispatch are the real federation ones, and
work reaches a provider over ``RelayLifecycleEndpoint``. That path is used even
when the selected provider happens to be this node, so a one-node federation
behaves exactly like a multi-node one.

*Standalone* — the device never joined a federation. It still runs the whole
durable job architecture against a device-local ``SessionCoordinator`` holding a
genuine single-node session that this device leads. Enrollment, approval, health
and F8.4 activation are the real components; only the carrier differs, because
there is no relay to carry anything.

The standalone coordinator database is deliberately separate from the product's
federation coordinator so a standalone install never manufactures phantom
sessions or members in federation views.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from catalog.capabilities.analysis.artifact_carrier import (
    LocalAnalysisArtifactCarrier,
    RelayAnalysisArtifactEndpoint,
)
from catalog.capabilities.analysis.gateway import AnalysisArtifactGateway
from catalog.capabilities.analysis.provisioning import lifecycle_worker_factory
from catalog.capabilities.lifecycle_worker import SQLiteLifecycleDispatchInbox
from catalog.capabilities.local_lifecycle import LocalLifecycleTransport
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.capabilities.worker_activation import (
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.node.identity import IdentityStore


@dataclass
class DeviceFederationAuthority:
    """The F8 authorities and carriers this device analyses JSONL with."""

    coordinator: SessionCoordinator | None
    enrollments: FederatedProviderEnrollmentService | None
    health: Any
    inventory: LocalComputeHandlerInventory
    binder: TrustedComputeWorkerBinder | None
    data_owner_node_id: str
    capability_root: Path
    clock: Callable[[], datetime]
    relay_client: Any | None = None
    _lifecycle_transport: Any | None = None

    # ------------------------------------------------------------------

    def lifecycle_transport(self):
        """Return the F7.5 carrier: the relay when federated, local otherwise."""

        if self._lifecycle_transport is None:
            if self.relay_client is not None:
                self._lifecycle_transport = RelayLifecycleEndpoint(self.relay_client)
            else:
                self._lifecycle_transport = LocalLifecycleTransport(
                    local_node_id=self.data_owner_node_id
                )
        return self._lifecycle_transport

    def artifact_carrier(self, gateway: AnalysisArtifactGateway):
        """Return the carrier for authorized F6 artifact frames."""

        if self.relay_client is not None:
            transport = self.lifecycle_transport()
            return RelayAnalysisArtifactEndpoint(
                self.relay_client,
                gateway,
                clock=self.clock,
                # One component may consume the relay client's inbound stream;
                # the lifecycle endpoint owns it and forwards the rest.
                message_source=transport,
            )
        return LocalAnalysisArtifactCarrier(
            gateway, local_node_id=self.data_owner_node_id, clock=self.clock
        )

    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        *,
        coordinator: SessionCoordinator,
        capability_root: Path,
        session_id: str,
        node_id: str,
        clock: Callable[[], datetime],
        relay_client: Any | None = None,
    ) -> DeviceFederationAuthority:
        enrollments = FederatedProviderEnrollmentService(
            coordinator,
            SQLiteProviderEnrollmentStore(
                capability_root / "provider_enrollment.sqlite3"
            ),
            clock=clock,
        )
        health = FederatedProviderHealthService(
            enrollments,
            SQLiteProviderHealthStore(capability_root / "provider_health.sqlite3"),
            clock=clock,
        )
        inventory = LocalComputeHandlerInventory()
        authority = ComputeWorkerActivationAuthority(
            health,
            inventory,
            session_id=session_id,
            local_node_id=node_id,
            clock=clock,
        )
        binder = TrustedComputeWorkerBinder(
            authority,
            lambda provider_id: SQLiteLifecycleDispatchInbox(
                capability_root / f"dispatch_inbox_{provider_id}.sqlite3"
            ),
            clock=clock,
            # F7.5 endpoints require a cancellable worker; activation fencing is
            # unchanged, only the worker contract differs.
            worker_factory=lifecycle_worker_factory,
        )
        return cls(
            coordinator=coordinator,
            enrollments=enrollments,
            health=health,
            inventory=inventory,
            binder=binder,
            data_owner_node_id=node_id,
            capability_root=capability_root,
            clock=clock,
            relay_client=relay_client,
        )

    @classmethod
    def without_authority(
        cls,
        *,
        capability_root: Path,
        node_id: str,
        clock: Callable[[], datetime],
    ) -> DeviceFederationAuthority:
        """Return an authority that can host nothing and offers no provider.

        Used when this device cannot be enrolled at all. Jobs still submit and
        queue durably; they simply have no eligible provider until the device
        joins a federation or its identity becomes enrollable.
        """

        return cls(
            coordinator=None,
            enrollments=None,
            health=_EmptyHealthAuthority(),
            inventory=LocalComputeHandlerInventory(),
            binder=None,
            data_owner_node_id=node_id,
            capability_root=capability_root,
            clock=clock,
        )

    @classmethod
    def for_device(
        cls,
        *,
        capability_root: Path,
        identity,
        clock: Callable[[], datetime],
        coordinator: SessionCoordinator | None = None,
        relay_client: Any | None = None,
    ) -> DeviceFederationAuthority:
        """Build the authority chain, bootstrapping a single-node session if needed."""

        if coordinator is None:
            coordinator = _standalone_coordinator(
                capability_root=capability_root,
                session_id=identity.session_id,
                node_id=identity.node_id,
                clock=clock,
            )
        return cls.build(
            coordinator=coordinator,
            capability_root=capability_root,
            session_id=identity.session_id,
            node_id=identity.node_id,
            clock=clock,
            relay_client=relay_client,
        )


def _standalone_coordinator(
    *,
    capability_root: Path,
    session_id: str,
    node_id: str,
    clock: Callable[[], datetime],
) -> SessionCoordinator:
    """Return a device-local coordinator holding this device's own session.

    The device enrols itself and creates the session it leads. This is a real
    federation of one, not a bypass: the same membership, enrollment, approval
    and activation rules apply, and the device is genuinely the authority.
    """

    capability_root.mkdir(parents=True, exist_ok=True)
    coordinator = SessionCoordinator(
        capability_root / "standalone_control.sqlite3", clock=clock
    )
    try:
        coordinator.require_active_node(node_id)
    except Exception:  # noqa: BLE001 - first run enrols this device
        credentials = IdentityStore(
            capability_root / "standalone_identity",
            display_name="This device",
        ).load_or_create(now=clock())
        if credentials.identity.node_id != node_id:
            # F8 membership requires a key-derived node identity. A caller that
            # supplies some other identifier cannot be enrolled, so the device
            # ends up with no coordinator, no provider, and queued jobs.
            raise FederationValidationError(
                "analysis-identity-not-enrollable",
                "node_id",
                "standalone analysis requires this device's own node identity",
            )
        token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)["token"]
        coordinator.enroll_node(credentials.identity, token=token)
    if session_id not in coordinator.session_ids_for_node(node_id):
        coordinator.create_session(
            actor_node_id=node_id,
            display_name="Standalone analysis",
            request_id=f"create-{session_id}",
            session_id=session_id,
        )
    return coordinator


class _EmptyHealthAuthority:
    """Health authority for a device with no federation membership at all."""

    store = None

    def fresh_reports(self, **_kwargs):
        return ()


__all__ = ["DeviceFederationAuthority"]

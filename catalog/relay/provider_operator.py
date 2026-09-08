"""C03 delegation into the relay's existing, separate provider authority.

No capability properties cross this RPC and no sidecar is copied or replicated.
The runtime lock fences the synchronous authority operation against local
leadership changes; the existing service owns revisions, idempotency and audit.
"""

from __future__ import annotations

from contextlib import contextmanager

from catalog.capabilities.product_provider_operator import (
    CapabilityFirstProviderEnrollmentService,
    CapabilityFirstProviderOperatorSurface,
)
from catalog.capabilities.provider_health import FederatedProviderHealthService
from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_replication import ControlPlaneError
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)

OPERATOR_MESSAGES = frozenset({"provider.operator.view", "provider.operator.execute"})


@contextmanager
def _operator_authority(coordinator):
    runtime = coordinator.runtime
    with runtime._lifecycle_lock:
        try:
            if not runtime.ready:
                raise FederationOperationError(
                    "provider-operator-unavailable", "replicated authority is not sealed",
                )
            runtime.require_quorum_leader()
            runtime.materialize()
            if not runtime.ready:
                raise FederationOperationError(
                    "provider-operator-unavailable", "replicated authority is not sealed",
                )
            yield
        except ControlPlaneError as exc:
            # Later policy rechecks can also observe lost quorum. Keep those
            # refusals structured without exposing internals or acknowledging.
            raise FederationOperationError(
                "provider-operator-authority-unavailable", "replicated authority is unavailable",
            ) from exc


def dispatch_provider_operator(relay, record, request) -> dict:
    coordinator = relay.coordinator
    if not isinstance(coordinator, PhysicalReadyReplicatedSessionCoordinator):
        raise FederationOperationError(
            "provider-operator-unavailable", "this relay has no C03 operator authority",
        )
    session_id = relay._required_session(request)
    names = set(request.payload)
    if (
        request.message_type not in OPERATOR_MESSAGES
        or (request.message_type == "provider.operator.view" and names)
        or (request.message_type == "provider.operator.execute" and (
            not {"action", "capability_id"} <= names
            or names - {"action", "capability_id", "expected_revision", "reason_code"}
        ))
    ):
        raise FederationValidationError(
            "invalid-provider-operator-command", "payload", "unexpected operator fields",
        )
    with _operator_authority(coordinator):
        coordinator.store.require_membership(session_id=session_id, node_id=record.node_id)
        # Reuse the exact stores already owned by this relay; never open an
        # onboarding-local mirror or rebuild announcements from redacted status.
        enrollment = CapabilityFirstProviderEnrollmentService(
            coordinator, relay.provider_enrollment.store, clock=relay.provider_enrollment._clock,
        )
        health = FederatedProviderHealthService(
            enrollment, relay.provider_health.store, clock=relay.provider_health._clock,
        )
        surface = CapabilityFirstProviderOperatorSurface(
            enrollment, health, session_id=session_id, actor_node_id=record.node_id,
            clock=relay.provider_enrollment._clock,
        )
        if request.message_type == "provider.operator.view":
            return {"view": surface.view().to_dict()}
        # The leader is checked again by the unchanged policy. This explicit
        # fence also covers replay and malformed/forged actions before stores.
        coordinator.require_session_leader(session_id=session_id, actor_node_id=record.node_id)
        result = surface.execute(
            request.payload["action"], capability_id=request.payload["capability_id"],
            expected_revision=request.payload.get("expected_revision"),
            reason_code=request.payload.get("reason_code"), command_id=request.request_id,
        )
        return {"provider": result.to_dict()}

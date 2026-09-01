"""Production composition for registered-compute contribution provider state.

CF4 persists the operator's contribution decision first. This module projects that
already-durable decision into the existing F8 provider lifecycle without creating
a second authority: locally hosted federations reuse the relay sidecar databases;
remotely paired members send the same mutations through the authenticated relay.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from catalog.capabilities.provider_enrollment import (
    ProviderEnrollmentRecord,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.registered_compute_provider import (
    RegisteredComputeProviderRuntime,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionIntent,
)
from catalog.relay.provider_service import provider_authority_paths

from .federation_active_leader_provider_runtime import (
    ActiveLeaderProviderEnrollmentService,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _runtime_request(
    runtime: object,
    state: object,
    message_type: str,
    *,
    payload: dict[str, Any],
    request_id: str,
) -> dict[str, Any]:
    """Send one authenticated provider-authority command over pairing runtime."""

    async def request_authority() -> dict[str, Any]:
        ensure = getattr(runtime, "_ensure_connected", None)
        connected = getattr(runtime, "_connected_client", None)
        if not callable(ensure) or not callable(connected):
            raise FederationOperationError(
                "registered-compute-relay-unavailable",
                "paired provider authority cannot be reached",
            )
        await ensure(state)
        client = connected()
        binding = getattr(state, "binding", None)
        session_id = getattr(binding, "internal_session_id", None)
        if not isinstance(session_id, str) or not session_id:
            raise FederationOperationError(
                "registered-compute-relay-binding-missing",
                "paired provider authority is missing its Federation session",
            )
        return await client.request(
            message_type,
            session_id=session_id,
            payload=payload,
            request_id=request_id,
        )

    submit = getattr(runtime, "_submit", None)
    if not callable(submit):
        raise FederationOperationError(
            "registered-compute-relay-unavailable",
            "paired provider authority cannot execute requests",
        )
    result = submit(request_authority())
    if not isinstance(result, dict):
        raise FederationOperationError(
            "registered-compute-relay-response-invalid",
            "provider authority did not return an object",
        )
    return result


class _RelayCoordinatorAdapter:
    """Mutable coordinator seam over one authenticated remote pairing."""

    def __init__(self, facade: object, *, node_id: str) -> None:
        self.facade = facade
        self.runtime = getattr(facade, "runtime", None)
        self.state = getattr(facade, "state", None)
        self.node_id = node_id
        self.store = self
        if self.runtime is None or self.state is None:
            raise FederationOperationError(
                "registered-compute-relay-unavailable",
                "remote Federation context lacks its authenticated pairing runtime",
            )

    def list_capabilities(self, *, session_id: str) -> tuple[CapabilityAnnouncement, ...]:
        status = getattr(self.facade, "status", None)
        if not callable(status):
            raise FederationOperationError(
                "registered-compute-relay-status-unavailable",
                "remote Federation capability state cannot be read",
            )
        snapshot = status(actor_node_id=self.node_id)
        values = snapshot.get("capabilities") if isinstance(snapshot, dict) else None
        if values is None:
            return ()
        if not isinstance(values, list):
            raise FederationOperationError(
                "registered-compute-relay-status-invalid",
                "remote Federation capability state is malformed",
            )
        announcements: list[CapabilityAnnouncement] = []
        for value in values:
            if not isinstance(value, dict):
                raise FederationOperationError(
                    "registered-compute-relay-status-invalid",
                    "remote Federation capability state is malformed",
                )
            announcement = CapabilityAnnouncement.from_dict(value)
            if announcement.session_id == session_id:
                announcements.append(announcement)
        return tuple(announcements)

    def announce_capability(
        self,
        announcement: CapabilityAnnouncement,
        *,
        actor_node_id: str,
        request_id: str,
    ) -> CapabilityAnnouncement:
        if actor_node_id != self.node_id:
            raise FederationValidationError(
                "registered-compute-actor-mismatch",
                "actor_node_id",
                "provider announcement must use the authenticated local member",
            )
        announce = getattr(self.runtime, "announce_capability", None)
        if callable(announce):
            return announce(self.state, announcement, request_id=request_id)
        result = _runtime_request(
            self.runtime,
            self.state,
            "capability.announce",
            payload={"announcement": announcement.to_dict()},
            request_id=request_id,
        )
        value = result.get("announcement")
        if not isinstance(value, dict):
            raise FederationOperationError(
                "registered-compute-relay-response-invalid",
                "provider authority did not return the accepted announcement",
            )
        accepted = CapabilityAnnouncement.from_dict(value)
        if accepted != announcement:
            raise FederationOperationError(
                "registered-compute-relay-response-mismatch",
                "provider authority returned different announcement metadata",
            )
        return accepted


class _RelayEnrollmentAdapter:
    """F8.1 request seam over the authenticated provider-authority relay."""

    def __init__(self, coordinator: _RelayCoordinatorAdapter) -> None:
        self.coordinator = coordinator
        self.store = self

    def get(self, *, session_id: str, capability_id: str) -> object:
        """Treat retired relay-visible bindings as requiring F8.1 reconciliation.

        The coordinator has just supplied the authoritative older announcement.
        The relay owns the actual enrollment store, so this seam intentionally
        returns a sentinel rather than manufacturing local enrollment state.
        ``request`` below then reconciles the real remote record idempotently.
        """

        del session_id, capability_id
        return self

    def request(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
    ) -> ProviderEnrollmentRecord:
        if actor_node_id != self.coordinator.node_id:
            raise FederationValidationError(
                "registered-compute-actor-mismatch",
                "actor_node_id",
                "enrollment request must use the authenticated local member",
            )
        result = _runtime_request(
            self.coordinator.runtime,
            self.coordinator.state,
            "provider.enrollment.request",
            payload={"capability_id": capability_id},
            request_id=command_id,
        )
        value = result.get("enrollment")
        if not isinstance(value, dict):
            raise FederationOperationError(
                "registered-compute-relay-response-invalid",
                "provider authority did not return an enrollment record",
            )
        record = ProviderEnrollmentRecord.from_dict(value)
        if record.session_id != session_id or record.capability_id != capability_id:
            raise FederationOperationError(
                "registered-compute-relay-response-mismatch",
                "provider authority returned a different enrollment identity",
            )
        return record


class _RelayHealthAdapter:
    """Constructor seam; contribution reconciliation does not publish health."""

    def __init__(self, enrollments: _RelayEnrollmentAdapter) -> None:
        self.enrollments = enrollments


def _existing_registered_compute(
    runtime: RegisteredComputeProviderRuntime,
    candidate: ContributionCandidate,
    intent: ContributionIntent,
    *,
    session_id: str,
    node_id: str,
) -> bool:
    binding = runtime.binding(
        candidate,
        intent,
        session_id=session_id,
        node_id=node_id,
    )
    for announcement in runtime.coordinator.store.list_capabilities(
        session_id=session_id,
    ):
        if announcement.node_id != node_id:
            continue
        properties = announcement.properties
        if (
            properties.get("kind") == "registered-compute-handler"
            and properties.get("handler_id") == binding.handler_id
        ):
            return True
    return False


def reconcile_registered_compute_provider(
    *,
    onboarding_service: object,
    candidate: ContributionCandidate,
    intent: ContributionIntent,
    clock: Any = _utc_now,
) -> None:
    """Project one durable registered-compute intent into existing F8 authority.

    Never-active built-in/pending candidates are not materialized as disabled
    provider rows. Once a registered-compute provider has been published, however,
    inactive or suspended intent is reconciled so authoritative eligibility is
    fenced durably.
    """

    if (
        candidate.capability_type != "compute"
        or candidate.capacity_envelope.get("kind") != "registered-compute-handler"
    ):
        return
    context_loader = getattr(onboarding_service, "authorized_context", None)
    context = context_loader() if callable(context_loader) else None
    if context is None:
        raise FederationOperationError(
            "contribution-federation-required",
            "a trusted federation connection is required before provider reconciliation",
            "binding",
        )
    binding = getattr(context, "binding", None)
    credentials = getattr(context, "credentials", None)
    identity = getattr(credentials, "identity", None)
    session_id = getattr(binding, "internal_session_id", None)
    node_id = getattr(identity, "node_id", None)
    coordinator = getattr(context, "coordinator", None)
    if not isinstance(session_id, str) or not isinstance(node_id, str):
        raise FederationOperationError(
            "registered-compute-context-incomplete",
            "trusted Federation context is incomplete",
        )

    if isinstance(coordinator, SessionCoordinator):
        enrollment_path, health_path = provider_authority_paths(coordinator)
        enrollments = ActiveLeaderProviderEnrollmentService(
            coordinator,
            SQLiteProviderEnrollmentStore(enrollment_path),
            clock=clock,
        )
        health = FederatedProviderHealthService(
            enrollments,
            SQLiteProviderHealthStore(health_path),
            clock=clock,
        )
        runtime = RegisteredComputeProviderRuntime(
            enrollments=enrollments,
            health=health,
            clock=clock,
        )
    else:
        relay_coordinator = _RelayCoordinatorAdapter(coordinator, node_id=node_id)
        relay_enrollments = _RelayEnrollmentAdapter(relay_coordinator)
        runtime = RegisteredComputeProviderRuntime(
            enrollments=relay_enrollments,  # type: ignore[arg-type]
            health=_RelayHealthAdapter(relay_enrollments),  # type: ignore[arg-type]
            clock=clock,
        )

    active = intent.activation_state is ContributionActivationState.ACTIVE
    if not active and not _existing_registered_compute(
        runtime,
        candidate,
        intent,
        session_id=session_id,
        node_id=node_id,
    ):
        return
    runtime.reconcile_contribution(
        candidate,
        intent,
        session_id=session_id,
        node_id=node_id,
    )


__all__ = ["reconcile_registered_compute_provider"]

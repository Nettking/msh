"""F7.5 lifecycle carrier for a device that is not enrolled in a federation.

A standalone installation has no relay connection, so ``RelayLifecycleEndpoint``
cannot exist. This module supplies the *carrier only*: the same
``ResilientDispatchCoordinator`` above it and the same activated
``CancellableCapabilityWorker`` below it, with identical job, attempt, lease,
cancellation, and fencing semantics. Nothing here decides that work runs
locally — provider selection still does.

The carrier is authenticated by construction rather than by the relay: it
accepts only requests whose coordinator and target are this node's own
identity, and it refuses anything that claims another actor.
"""

from __future__ import annotations

from typing import Any

from catalog.federation.errors import FederationValidationError

from .dispatch import (
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
)
from .lifecycle_contracts import (
    CancellationRequest,
    CancellationResponse,
    CancellationState,
)
from .lifecycle_worker import CancellableCapabilityWorker


class LocalLifecycleTransport:
    """Deliver F7.5 dispatch and cancellation to a worker hosted in-process."""

    def __init__(
        self,
        *,
        local_node_id: str,
        workers: dict[str, CancellableCapabilityWorker] | None = None,
    ) -> None:
        if not isinstance(local_node_id, str) or not local_node_id:
            raise ValueError("local_node_id must be non-empty text")
        self.local_node_id = local_node_id
        self.workers: dict[str, CancellableCapabilityWorker] = {}
        for provider_id, worker in dict(workers or {}).items():
            self.register_worker(provider_id, worker)

    # ------------------------------------------------------------------
    # Worker hosting (same surface the relay endpoints expose)
    # ------------------------------------------------------------------

    def register_worker(
        self,
        provider_id: str,
        worker: CancellableCapabilityWorker,
    ) -> None:
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
        registration = getattr(worker, "registration", None)
        if registration is None or registration.provider_id != provider_id:
            raise FederationValidationError(
                "worker-provider-mismatch",
                "provider_id",
                "registration differs from the transport worker key",
            )
        if registration.node_id != self.local_node_id:
            raise FederationValidationError(
                "worker-node-mismatch",
                "node_id",
                "registration differs from the local node identity",
            )
        self.workers[provider_id] = worker

    def unregister_worker(
        self,
        provider_id: str,
        *,
        expected_worker: Any | None = None,
    ) -> bool:
        current = self.workers.get(provider_id)
        if current is None:
            return False
        if expected_worker is not None and current is not expected_worker:
            raise FederationValidationError(
                "worker-registration-changed",
                "provider_id",
                "registered worker differs from the expected activation",
            )
        self.workers.pop(provider_id)
        return True

    # ------------------------------------------------------------------
    # LifecycleTransport
    # ------------------------------------------------------------------

    def _authenticate(self, *, target_node_id: str, actor_node_id: str) -> None:
        """Refuse anything this node is not itself both sending and receiving."""

        if target_node_id != self.local_node_id:
            raise FederationValidationError(
                "capability-route-failed",
                "target_node_id",
                "the local carrier only reaches this node; a peer needs the relay",
            )
        if actor_node_id != self.local_node_id:
            raise FederationValidationError(
                "dispatch-actor-mismatch",
                "coordinator_node_id",
                "in-process delivery requires the local coordinator identity",
            )

    async def request(
        self,
        *,
        target_node_id: str,
        request: DispatchRequest,
    ) -> DispatchResponse:
        if target_node_id != request.target_node_id:
            raise FederationValidationError(
                "dispatch-target-mismatch",
                "target_node_id",
                "transport target differs from the dispatch target",
            )
        self._authenticate(
            target_node_id=target_node_id,
            actor_node_id=request.coordinator_node_id,
        )
        worker = self.workers.get(request.provider_id)
        if worker is None:
            return DispatchResponse(
                request.dispatch_id,
                request.session_id,
                request.job.job_id,
                request.provider_id,
                request.attempt_id,
                (
                    DispatchEvent(
                        1,
                        DispatchState.REJECTED,
                        request.sent_at,
                        reason_code="provider-not-hosted",
                    ),
                ),
            )
        return await worker.handle(
            request,
            authenticated_actor=request.coordinator_node_id,
            authenticated_session=request.session_id,
        )

    async def cancel(
        self,
        *,
        target_node_id: str,
        request: CancellationRequest,
    ) -> CancellationResponse:
        if target_node_id != request.target_node_id:
            raise FederationValidationError(
                "cancellation-target-mismatch",
                "target_node_id",
                "transport target differs from the cancellation target",
            )
        self._authenticate(
            target_node_id=target_node_id,
            actor_node_id=request.coordinator_node_id,
        )
        worker = self.workers.get(request.provider_id)
        if worker is None:
            return CancellationResponse(
                cancellation_id=request.cancellation_id,
                session_id=request.session_id,
                provider_id=request.provider_id,
                job_id=request.job_id,
                attempt_id=request.attempt_id,
                state=CancellationState.REJECTED,
                occurred_at=request.requested_at,
                reason_code="provider-not-hosted",
            )
        return await worker.cancel(
            request,
            authenticated_actor=request.coordinator_node_id,
            authenticated_session=request.session_id,
        )


__all__ = ["LocalLifecycleTransport"]

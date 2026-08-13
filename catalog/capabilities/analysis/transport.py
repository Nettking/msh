"""Node routing for the existing dispatch contract.

``DispatchCoordinator`` already knows how to speak to a worker; what the runtime
was missing is the adapter that decides *which* carrier reaches a given node. A
locally hosted worker is reached in-process; every other node is delegated to the
federation transport (today the authenticated relay dispatch endpoint).

No new dispatch protocol is introduced: both paths carry the same
``DispatchRequest`` and return the same ``DispatchResponse``.
"""

from __future__ import annotations

from catalog.federation.errors import FederationValidationError

from ..dispatch import (
    CapabilityWorker,
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
    DispatchTransport,
)


class NodeRoutedDispatchTransport:
    """Route dispatch to an in-process worker or to the federation transport."""

    def __init__(
        self,
        *,
        local_node_id: str,
        local_workers: dict[str, CapabilityWorker] | None = None,
        remote: DispatchTransport | None = None,
    ) -> None:
        self.local_node_id = local_node_id
        self.local_workers: dict[str, CapabilityWorker] = dict(local_workers or {})
        self.remote = remote

    def register_worker(self, provider_id: str, worker: CapabilityWorker) -> None:
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
                "worker registration differs from the local node identity",
            )
        self.local_workers[provider_id] = worker

    def unregister_worker(self, provider_id: str) -> bool:
        return self.local_workers.pop(provider_id, None) is not None

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
        if target_node_id != self.local_node_id:
            if self.remote is None:
                raise FederationValidationError(
                    "capability-route-failed",
                    "target_node_id",
                    "no federation transport is configured for remote dispatch",
                )
            return await self.remote.request(
                target_node_id=target_node_id, request=request
            )
        if request.coordinator_node_id != self.local_node_id:
            # In-process delivery carries no network authentication, so it may
            # only be used by the local coordinator for its own workers. A
            # request claiming another coordinator must travel the relay.
            raise FederationValidationError(
                "dispatch-actor-mismatch",
                "coordinator_node_id",
                "in-process dispatch requires the local coordinator identity",
            )
        worker = self.local_workers.get(request.provider_id)
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
        # The in-process carrier is authenticated by construction: the request was
        # built by this coordinator for this node, so those identities are the
        # authenticated ones the worker re-validates against its registration.
        return await worker.handle(
            request,
            authenticated_actor=request.coordinator_node_id,
            authenticated_session=request.session_id,
        )


__all__ = ["NodeRoutedDispatchTransport"]

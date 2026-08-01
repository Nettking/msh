"""F6.6.2 route-aware adapter for the authenticated Phase 2 relay."""

from __future__ import annotations

from typing import Any

from catalog.federation.errors import AuthorizationError, FederationValidationError
from catalog.federation.protocol import RelayEnvelope
from catalog.federation.session_route_rendezvous import (
    MAX_ROUTE_GENERATION,
    SessionRouteDescriptor,
    SessionRouteRegistry,
)
from catalog.node.identity import verify_signature

from .service import (
    RelayServer,
    RelayTransportError,
    _LiveConnection,
    _payload_object,
    _payload_text,
)


class RouteRendezvousRelayServer(RelayServer):
    """RelayServer with bounded, non-authoritative session route rendezvous."""

    def __init__(
        self,
        *args: Any,
        route_registry: SessionRouteRegistry | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.route_registry = route_registry or SessionRouteRegistry()

    def _route_now(self):
        return self.coordinator._clock()  # noqa: SLF001 - shared internal clock boundary

    async def _dispatch(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        if request.message_type == "route.publish":
            await self._publish_route(record, request)
            return
        if request.message_type == "route.resolve":
            await self._resolve_route(record, request)
            return
        if request.message_type == "route.withdraw":
            await self._withdraw_route(record, request)
            return
        await super()._dispatch(record, request)

    async def _publish_route(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        session_id = self._required_session(request)
        descriptor = SessionRouteDescriptor.from_dict(
            _payload_object(request.payload, "descriptor")
        )
        if descriptor.session_id != session_id:
            raise AuthorizationError(
                "wrong-session",
                "route and envelope session IDs do not match",
                "session_id",
            )
        if descriptor.node_id != record.node_id:
            raise AuthorizationError(
                "route-owner-mismatch",
                "authenticated actor does not own this route",
                "node_id",
            )
        authorization_context = self.coordinator.authorize_route(
            session_id=session_id,
            actor_node_id=record.node_id,
            target_node_id=record.node_id,
        )
        identity = self.coordinator.public_identity(record.node_id)
        created = self.route_registry.publish(
            descriptor,
            public_key=identity.public_key,
            verify_signature=verify_signature,
            now=self._route_now(),
        )
        await self._send_live(
            record,
            self._response_envelope(
                request,
                message_type="route.publish.accepted",
                payload={
                    "created": created,
                    "node_id": descriptor.node_id,
                    "generation": descriptor.generation,
                    "expires_at": descriptor.to_dict()["expires_at"],
                },
                authorization_context={
                    **authorization_context,
                    "kind": "validated-session-route-publication",
                },
            ),
        )

    async def _resolve_route(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        session_id = self._required_session(request)
        target_node_id = _payload_text(request.payload, "target_node_id")
        if target_node_id == record.node_id:
            raise FederationValidationError(
                "session-route-self-resolution",
                "target_node_id",
                "a node must use its local route directly",
            )
        authorization_context = self.coordinator.authorize_route(
            session_id=session_id,
            actor_node_id=record.node_id,
            target_node_id=target_node_id,
        )
        descriptor = self.route_registry.resolve(
            session_id=session_id,
            node_id=target_node_id,
            now=self._route_now(),
        )
        if descriptor is None:
            raise RelayTransportError(
                "session-route-unavailable",
                "target node has no current rendezvous route",
                "target_node_id",
            )
        identity = self.coordinator.public_identity(target_node_id)
        await self._send_live(
            record,
            self._response_envelope(
                request,
                message_type="route.resolve.accepted",
                payload={
                    "descriptor": descriptor.to_dict(),
                    "identity": identity.to_dict(),
                },
                authorization_context={
                    **authorization_context,
                    "kind": "validated-session-route-resolution",
                },
            ),
        )

    async def _withdraw_route(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        session_id = self._required_session(request)
        generation = request.payload.get("generation")
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or not 1 <= generation <= MAX_ROUTE_GENERATION
        ):
            raise FederationValidationError(
                "invalid-session-route-generation",
                "generation",
                f"must be between 1 and {MAX_ROUTE_GENERATION}",
            )
        authorization_context = self.coordinator.authorize_route(
            session_id=session_id,
            actor_node_id=record.node_id,
            target_node_id=record.node_id,
        )
        removed = self.route_registry.withdraw(
            session_id=session_id,
            node_id=record.node_id,
            generation=generation,
            now=self._route_now(),
        )
        await self._send_live(
            record,
            self._response_envelope(
                request,
                message_type="route.withdraw.accepted",
                payload={
                    "removed": removed,
                    "node_id": record.node_id,
                    "generation": generation,
                },
                authorization_context={
                    **authorization_context,
                    "kind": "validated-session-route-withdrawal",
                },
            ),
        )

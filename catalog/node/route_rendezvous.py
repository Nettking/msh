"""F6.6.2 node helper for authenticated session route publication and lookup."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol

from catalog.federation.direct_peer import DirectPeerDescriptor
from catalog.federation.errors import FederationValidationError
from catalog.federation.models import NodeIdentity
from catalog.federation.session_route_rendezvous import SessionRouteDescriptor

from .client import RelayNodeClient, RelayRemoteError
from .identity import derive_node_id_from_public_key, verify_signature


class DirectPeerRegistrar(Protocol):
    def register_peer(self, descriptor: DirectPeerDescriptor) -> None: ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SessionRouteRendezvousClient:
    """Publish and resolve signed routes through one authenticated relay client."""

    def __init__(
        self,
        client: RelayNodeClient,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(client, RelayNodeClient):
            raise TypeError("client must be a RelayNodeClient")
        self.client = client
        self._clock = clock or getattr(client, "_clock", _utc_now)
        self._generations: dict[str, int] = {}

    @property
    def node_id(self) -> str:
        return self.client.node_id

    async def publish(
        self,
        *,
        session_id: str,
        descriptor: DirectPeerDescriptor,
        ttl_seconds: int = 300,
        generation: int | None = None,
    ) -> SessionRouteDescriptor:
        if descriptor.node_id != self.node_id:
            raise FederationValidationError(
                "session-route-local-owner-mismatch",
                "node_id",
                "only the local node can publish this route",
            )
        previous = self._generations.get(session_id, 0)
        next_generation = previous + 1 if generation is None else generation
        signed = SessionRouteDescriptor.create(
            session_id=session_id,
            descriptor=descriptor,
            generation=next_generation,
            issued_at=self._clock(),
            ttl_seconds=ttl_seconds,
            sign=self.client.credentials.sign,
        )
        result = await self.client.request(
            "route.publish",
            session_id=session_id,
            payload={"descriptor": signed.to_dict()},
            authorization_context={"kind": "signed-session-route"},
        )
        accepted_generation = result.get("generation")
        if accepted_generation != signed.generation:
            raise RelayRemoteError(
                "invalid-route-publication-ack",
                "relay acknowledged another route generation",
            )
        self._generations[session_id] = max(previous, signed.generation)
        return signed

    async def resolve_signed(
        self,
        *,
        session_id: str,
        target_node_id: str,
    ) -> SessionRouteDescriptor:
        result = await self.client.request(
            "route.resolve",
            session_id=session_id,
            payload={"target_node_id": target_node_id},
            authorization_context={"kind": "session-route-query"},
        )
        descriptor = SessionRouteDescriptor.from_dict(result.get("descriptor"))
        identity = NodeIdentity.from_dict(result.get("identity"))
        if identity.node_id != target_node_id or descriptor.node_id != target_node_id:
            raise RelayRemoteError(
                "route-identity-mismatch",
                "resolved route does not belong to the requested node",
            )
        if descriptor.session_id != session_id:
            raise RelayRemoteError(
                "route-session-mismatch",
                "resolved route belongs to another session",
            )
        if derive_node_id_from_public_key(identity.public_key) != identity.node_id:
            raise RelayRemoteError(
                "route-public-identity-mismatch",
                "resolved public identity is inconsistent",
            )
        try:
            valid = verify_signature(
                identity.public_key,
                descriptor.signing_bytes(),
                descriptor.signature,
            )
        except FederationValidationError as exc:
            raise RelayRemoteError(
                "invalid-resolved-route-signature",
                "resolved route signature could not be verified",
            ) from exc
        if not valid:
            raise RelayRemoteError(
                "invalid-resolved-route-signature",
                "resolved route is not signed by the target node",
            )
        descriptor.validate_window(now=self._clock(), max_ttl_seconds=600)
        return descriptor

    async def resolve(
        self,
        *,
        session_id: str,
        target_node_id: str,
    ) -> DirectPeerDescriptor:
        descriptor = await self.resolve_signed(
            session_id=session_id,
            target_node_id=target_node_id,
        )
        return descriptor.to_direct_peer_descriptor()

    async def resolve_and_register(
        self,
        registrar: DirectPeerRegistrar,
        *,
        session_id: str,
        target_node_id: str,
    ) -> DirectPeerDescriptor:
        descriptor = await self.resolve(
            session_id=session_id,
            target_node_id=target_node_id,
        )
        registrar.register_peer(descriptor)
        return descriptor

    async def withdraw(
        self,
        *,
        session_id: str,
        generation: int | None = None,
    ) -> bool:
        current = self._generations.get(session_id, 0)
        withdrawal_generation = current if generation is None else generation
        if withdrawal_generation < 1:
            raise FederationValidationError(
                "missing-session-route-generation",
                "generation",
                "publish a route before withdrawing it",
            )
        result = await self.client.request(
            "route.withdraw",
            session_id=session_id,
            payload={"generation": withdrawal_generation},
            authorization_context={"kind": "session-route-withdrawal"},
        )
        removed = result.get("removed")
        if not isinstance(removed, bool):
            raise RelayRemoteError(
                "invalid-route-withdrawal-ack",
                "relay returned an invalid withdrawal result",
            )
        return removed

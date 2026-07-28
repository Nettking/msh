"""D5 location-transparent storage client and relay transport boundary."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from .errors import FederationValidationError
from .storage_control_plane import StorageControlPlaneStore
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)


class StorageRequestTransport(Protocol):
    """Request/reply transport used by the logical client."""

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope: ...


@dataclass(frozen=True)
class StorageRoute:
    session_id: str
    group_id: str
    provider_id: str
    node_id: str


class StorageRouteResolver:
    """Resolve a logical storage group to its currently assigned primary only."""

    def __init__(self, control_plane: StorageControlPlaneStore) -> None:
        self.control_plane = control_plane

    def primary(self, *, session_id: str, group_id: str) -> StorageRoute:
        snapshot = self.control_plane.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "group_id",
                "storage group has no assigned primary",
            )
        provider = snapshot.providers.get(assignment.primary_provider_id)
        if provider is None:
            raise FederationValidationError(
                "unknown-provider",
                "provider_id",
                "assigned primary provider is not registered",
            )
        if not provider.assignable:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "provider_id",
                "assigned primary provider is not ready, authorized, and protocol-compatible",
            )
        return StorageRoute(session_id, group_id, provider.provider_id, provider.node_id)


class CallableStorageTransport:
    """Small adapter for relay clients or test transports exposing an async callable."""

    def __init__(
        self,
        sender: Callable[[str, StorageRequestEnvelope], Awaitable[StorageResponseEnvelope]],
    ) -> None:
        self._sender = sender

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        return await self._sender(target_node_id, envelope)


class RelayNodeStorageTransport:
    """Adapter for ``RelayNodeClient.request`` request/reply routing.

    The relay-facing message type is deliberately storage-specific. The relay or
    provider-side bridge must return a serialized ``StorageResponseEnvelope`` in
    the ``response`` field. D5 does not add retries or failover.
    """

    def __init__(self, relay_client: object) -> None:
        self.relay_client = relay_client

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        request = getattr(self.relay_client, "request", None)
        if request is None:
            raise FederationValidationError(
                "invalid-transport", "relay_client", "must expose an async request method"
            )
        result = await request(
            "storage.request",
            session_id=envelope.session_id,
            target_node_id=target_node_id,
            request_id=envelope.request_id,
            authorization_context=envelope.authorization_context,
            payload={"request": envelope.to_dict()},
        )
        response = result.get("response") if isinstance(result, dict) else None
        if not isinstance(response, dict):
            raise FederationValidationError(
                "invalid-storage-response",
                "response",
                "relay response did not contain a storage response envelope",
            )
        return StorageResponseEnvelope.from_dict(response)


class LogicalStorageClient:
    """Provider-agnostic API addressed by session and logical storage group."""

    def __init__(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        resolver: StorageRouteResolver,
        transport: StorageRequestTransport,
    ) -> None:
        self.session_id = session_id
        self.actor_node_id = actor_node_id
        self.resolver = resolver
        self.transport = transport

    async def ingest(self, request: BatchIngestRequest) -> BatchIngestResult:
        if request.authority.session_id != self.session_id:
            raise FederationValidationError(
                StorageErrorCode.INVALID_REQUEST.value,
                "authority.session_id",
                "request belongs to another session",
            )
        if request.authority.actor_node_id != self.actor_node_id:
            raise FederationValidationError(
                StorageErrorCode.INVALID_REQUEST.value,
                "authority.actor_node_id",
                "request actor differs from the logical client actor",
            )
        response = await self._request(
            group_id=request.authority.group_id,
            operation=StorageOperation.BATCH_INGEST,
            payload=request.to_dict(),
        )
        return BatchIngestResult.from_dict(response)

    async def exists(self, *, group_id: str, batch_id: str) -> bool:
        result = await self._request(
            group_id=group_id,
            operation=StorageOperation.BATCH_EXISTS,
            payload={"group_id": group_id, "batch_id": batch_id},
        )
        exists = result.get("exists")
        if not isinstance(exists, bool):
            raise FederationValidationError(
                "invalid-storage-response", "exists", "must be boolean"
            )
        return exists

    async def read(self, *, group_id: str, batch_id: str) -> object:
        result = await self._request(
            group_id=group_id,
            operation=StorageOperation.BATCH_READ,
            payload={"group_id": group_id, "batch_id": batch_id},
        )
        if "content" not in result:
            raise FederationValidationError(
                "invalid-storage-response", "content", "is required"
            )
        return result["content"]

    async def _request(
        self,
        *,
        group_id: str,
        operation: StorageOperation,
        payload: dict[str, object],
    ) -> dict[str, object]:
        route = self.resolver.primary(session_id=self.session_id, group_id=group_id)
        envelope = StorageRequestEnvelope(
            request_id=f"storage-{uuid.uuid4().hex}",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=operation,
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
            authorization_context={
                "kind": "storage-primary-route",
                "group_id": group_id,
                "provider_id": route.provider_id,
            },
            payload=payload,
        )
        response = await self.transport.request(
            target_node_id=route.node_id,
            envelope=envelope,
        )
        if response.request_id != envelope.request_id:
            raise FederationValidationError(
                "response-request-mismatch",
                "request_id",
                "storage response belongs to another request",
            )
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "storage",
                response.error.message,
            )
        assert response.result is not None
        return response.result

"""Carriers that move F6 artifact frames between data owner and worker.

Both carriers move the *same* F6 ``ObjectTransferManifest`` and
``ObjectTransferChunk`` values produced by the data owner's authorized gateway.
Neither defines chunk hashing, sizing, resumability, or integrity rules: those
belong to :mod:`catalog.federation.object_transfer` and are unchanged.

Only the carrier differs, exactly as it does for dispatch: in-process for a
standalone device, and the authenticated relay for a federation peer.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Protocol

from catalog.federation.errors import FederationValidationError
from catalog.federation.object_transfer import (
    ObjectTransferChunk,
    ObjectTransferManifest,
)

from ..artifact_contracts import ArtifactInputReference
from .gateway import AnalysisArtifactGateway

RELAY_ARTIFACT_KIND = "fcp-analysis-artifact-v1"
MAX_PENDING_ARTIFACT_REQUESTS = 64


class LocalAnalysisArtifactCarrier:
    """Serve authorized artifact frames from the gateway hosted in this process."""

    def __init__(
        self,
        gateway: AnalysisArtifactGateway,
        *,
        local_node_id: str,
        clock,
    ) -> None:
        self.gateway = gateway
        self.local_node_id = local_node_id
        self.clock = clock

    def _check_target(self, target_node_id: str) -> None:
        if target_node_id != self.local_node_id:
            raise FederationValidationError(
                "artifact-route-failed",
                "target_node_id",
                "the local carrier only reaches this node; a peer needs the relay",
            )

    async def manifest(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferManifest:
        self._check_target(target_node_id)
        return await asyncio.to_thread(
            self.gateway.transfer_manifest,
            grant_id=grant_id,
            reference=reference,
            authenticated_session_id=session_id,
            authenticated_worker_node_id=worker_node_id,
            provider_id=provider_id,
            now=self.clock(),
        )

    async def chunk(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        index: int,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferChunk:
        self._check_target(target_node_id)
        return await asyncio.to_thread(
            self.gateway.transfer_chunk,
            grant_id=grant_id,
            reference=reference,
            index=index,
            authenticated_session_id=session_id,
            authenticated_worker_node_id=worker_node_id,
            provider_id=provider_id,
            now=self.clock(),
        )


class RelayMessageClient(Protocol):
    node_id: str

    async def send_message(
        self,
        *,
        session_id: str,
        target_node_id: str,
        payload: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]: ...

    async def receive_message(self, *, timeout: float | None = None): ...


class RelayAnalysisArtifactEndpoint:
    """Request/serve authorized artifact frames over the authenticated relay.

    The relay authenticates actor, target and session. This endpoint repeats
    those bindings and then hands the request to the local artifact authority,
    which is the component that actually decides whether the caller may read.
    """

    def __init__(
        self,
        relay_client: RelayMessageClient,
        gateway: AnalysisArtifactGateway | None = None,
        *,
        clock,
        request_timeout: float = 30.0,
    ) -> None:
        self.relay_client = relay_client
        self.gateway = gateway
        self.clock = clock
        self.request_timeout = float(request_timeout)
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._sequence = 0
        self._closed = False

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("relay artifact endpoint is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-analysis-artifact-{self.relay_client.node_id}",
            )

    async def close(self) -> None:
        self._closed = True
        reader, self._reader_task = self._reader_task, None
        if reader is not None:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        error = RuntimeError("relay artifact endpoint closed")
        for pending in tuple(self._pending.values()):
            if not pending.done():
                pending.set_exception(error)
        self._pending.clear()

    # ------------------------------------------------------------------

    async def _exchange(
        self,
        *,
        target_node_id: str,
        session_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        await self.start()
        if len(self._pending) >= MAX_PENDING_ARTIFACT_REQUESTS:
            raise FederationValidationError(
                "too-many-pending-artifact-requests",
                "request",
                "bounded pending artifact request limit reached",
            )
        self._sequence += 1
        exchange_id = f"{self.relay_client.node_id}:{self._sequence}"
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[exchange_id] = future
        try:
            delivery = await self.relay_client.send_message(
                session_id=session_id,
                target_node_id=target_node_id,
                payload={
                    "kind": RELAY_ARTIFACT_KIND,
                    "message": "request",
                    "exchange_id": exchange_id,
                    **payload,
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "artifact-route-failed",
                    "target_node_id",
                    "relay did not confirm artifact delivery",
                )
            reply = await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(exchange_id, None)
        if reply.get("error"):
            raise FederationValidationError(
                str(reply["error"]),
                "artifact",
                "data owner refused the artifact request",
            )
        return reply

    async def manifest(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferManifest:
        reply = await self._exchange(
            target_node_id=target_node_id,
            session_id=session_id,
            payload={
                "operation": "manifest",
                "grant_id": grant_id,
                "reference": reference.to_dict(),
                "provider_id": provider_id,
                "worker_node_id": worker_node_id,
            },
        )
        return ObjectTransferManifest.from_dict(reply["manifest"])

    async def chunk(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        index: int,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferChunk:
        reply = await self._exchange(
            target_node_id=target_node_id,
            session_id=session_id,
            payload={
                "operation": "chunk",
                "grant_id": grant_id,
                "reference": reference.to_dict(),
                "index": int(index),
                "provider_id": provider_id,
                "worker_node_id": worker_node_id,
            },
        )
        return ObjectTransferChunk.from_dict(reply["chunk"])

    # ------------------------------------------------------------------

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                incoming = await self.relay_client.receive_message()
                payload = getattr(incoming, "payload", None)
                if (
                    not isinstance(payload, dict)
                    or payload.get("kind") != RELAY_ARTIFACT_KIND
                ):
                    continue
                if payload.get("message") == "response":
                    self._accept_response(payload)
                elif payload.get("message") == "request":
                    await self._serve(incoming, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            for pending in tuple(self._pending.values()):
                if not pending.done():
                    pending.set_exception(exc)

    def _accept_response(self, payload: dict[str, Any]) -> None:
        pending = self._pending.get(str(payload.get("exchange_id")))
        if pending is not None and not pending.done():
            pending.set_result(payload)

    async def _serve(self, incoming: Any, payload: dict[str, Any]) -> None:
        actor = getattr(incoming, "actor_node_id", None)
        session = getattr(incoming, "session_id", None)
        reply: dict[str, Any] = {
            "kind": RELAY_ARTIFACT_KIND,
            "message": "response",
            "exchange_id": payload.get("exchange_id"),
        }
        if self.gateway is None:
            reply["error"] = "artifact-authority-unavailable"
        elif not isinstance(actor, str) or not isinstance(session, str):
            return
        else:
            try:
                reply.update(self._answer(payload, actor=actor, session=session))
            except Exception as exc:  # noqa: BLE001 - refusals must stay bounded
                reply["error"] = str(getattr(exc, "code", "artifact-request-failed"))
        if isinstance(actor, str) and isinstance(session, str):
            await self.relay_client.send_message(
                session_id=session,
                target_node_id=actor,
                payload=reply,
            )

    def _answer(
        self, payload: dict[str, Any], *, actor: str, session: str
    ) -> dict[str, Any]:
        assert self.gateway is not None
        reference = ArtifactInputReference.from_dict(payload.get("reference"))
        # The relay already proved who is asking. The worker node identity the
        # artifact authority checks is therefore the authenticated relay actor,
        # never a value the caller supplied in the body.
        if payload.get("worker_node_id") != actor:
            raise FederationValidationError(
                "artifact-worker-mismatch",
                "worker_node_id",
                "request body does not match the authenticated relay actor",
            )
        grant_id = str(payload.get("grant_id"))
        provider_id = str(payload.get("provider_id"))
        now: datetime = self.clock()
        if payload.get("operation") == "manifest":
            manifest = self.gateway.transfer_manifest(
                grant_id=grant_id,
                reference=reference,
                authenticated_session_id=session,
                authenticated_worker_node_id=actor,
                provider_id=provider_id,
                now=now,
            )
            return {"manifest": manifest.to_dict()}
        chunk = self.gateway.transfer_chunk(
            grant_id=grant_id,
            reference=reference,
            index=int(payload.get("index", -1)),
            authenticated_session_id=session,
            authenticated_worker_node_id=actor,
            provider_id=provider_id,
            now=now,
        )
        return {"chunk": chunk.to_dict()}


__all__ = [
    "RELAY_ARTIFACT_KIND",
    "LocalAnalysisArtifactCarrier",
    "RelayAnalysisArtifactEndpoint",
]

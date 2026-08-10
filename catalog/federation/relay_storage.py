"""Storage request/reply bridge over the authenticated Phase 2 relay channel."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import FederationValidationError
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    StorageError,
    StorageErrorCode,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)

RELAY_STORAGE_KIND = "fcp-storage-v1"


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


class AsyncStorageService(Protocol):
    async def dispatch(self, envelope: StorageRequestEnvelope) -> StorageResponseEnvelope: ...


@dataclass(frozen=True)
class _PendingRequest:
    future: asyncio.Future[StorageResponseEnvelope]
    session_id: str
    target_node_id: str
    provider_id: str


class RelayStorageEndpoint:
    """Own one node client's application relay queue for storage traffic.

    The endpoint multiplexes correlated responses and provider-side request handling
    without changing the Phase 2 relay protocol. Unrelated relay messages are retained
    in a bounded queue for optional consumers instead of being silently discarded.
    """

    def __init__(
        self,
        relay_client: RelayMessageClient,
        services: Mapping[str, AsyncStorageService] | None = None,
        *,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.services = dict(services or {})
        self.request_timeout = float(request_timeout)
        self._pending: dict[str, _PendingRequest] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._other_messages: asyncio.Queue[Any] = asyncio.Queue(maxsize=other_message_limit)

    def register_service(self, provider_id: str, service: AsyncStorageService) -> None:
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
        self.services[provider_id] = service

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("relay storage endpoint is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-storage-relay-{self.relay_client.node_id}",
            )

    async def close(self) -> None:
        self._closed = True
        reader, self._reader_task = self._reader_task, None
        if reader is not None:
            reader.cancel()
        for task in tuple(self._handler_tasks):
            task.cancel()
        tasks = tuple(task for task in (reader, *self._handler_tasks) if task is not None)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._handler_tasks.clear()
        error = RuntimeError("relay storage endpoint closed")
        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending.clear()

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        await self.start()
        if envelope.request_id in self._pending:
            raise FederationValidationError(
                "duplicate-local-request-id", "request_id", "request is already in flight"
            )
        provider_id = envelope.authorization_context.get("provider_id")
        if not isinstance(provider_id, str) or not provider_id:
            raise FederationValidationError(
                "missing-field", "authorization_context.provider_id", "target provider is required"
            )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[StorageResponseEnvelope] = loop.create_future()
        self._pending[envelope.request_id] = _PendingRequest(
            future,
            envelope.session_id,
            target_node_id,
            provider_id,
        )
        try:
            delivery = await self.relay_client.send_message(
                session_id=envelope.session_id,
                target_node_id=target_node_id,
                request_id=f"relay-{envelope.request_id}",
                payload={
                    "kind": RELAY_STORAGE_KIND,
                    "message": "request",
                    "provider_id": provider_id,
                    "frame": json.dumps(
                        envelope.to_dict(),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ),
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "storage-route-failed", "target_node_id", "relay did not confirm delivery"
                )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(envelope.request_id, None)

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._other_messages.get()
        return await asyncio.wait_for(self._other_messages.get(), timeout=timeout)

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                message = await self.relay_client.receive_message()
                payload = getattr(message, "payload", None)
                if not isinstance(payload, dict) or payload.get("kind") != RELAY_STORAGE_KIND:
                    try:
                        self._other_messages.put_nowait(message)
                    except asyncio.QueueFull:
                        pass
                    continue
                message_kind = payload.get("message")
                if message_kind == "response":
                    self._accept_response(message, payload)
                    continue
                if message_kind == "request":
                    task = asyncio.create_task(self._handle_request(message, payload))
                    self._handler_tasks.add(task)
                    task.add_done_callback(self._finish_handler)
                    continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            for pending in tuple(self._pending.values()):
                if not pending.future.done():
                    pending.future.set_exception(exc)

    def _finish_handler(self, task: asyncio.Task[None]) -> None:
        self._handler_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _accept_response(
        self,
        relay_message: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = payload.get("frame")
        if not isinstance(frame, str):
            return
        try:
            response_value = json.loads(frame)
        except json.JSONDecodeError:
            return
        if not isinstance(response_value, dict):
            return
        request_id = response_value.get("request_id")
        if not isinstance(request_id, str):
            return
        pending = self._pending.get(request_id)
        if pending is None:
            return
        response_provider_id = payload.get("provider_id")
        if (
            getattr(relay_message, "actor_node_id", None)
            != pending.target_node_id
            or getattr(relay_message, "session_id", None) != pending.session_id
            or (
                response_provider_id is not None
                and response_provider_id != pending.provider_id
            )
        ):
            return
        try:
            response = StorageResponseEnvelope.from_dict(response_value)
        except FederationValidationError:
            # A malformed response must not terminate the shared reader loop.
            # The expected request remains pending so a valid authenticated
            # response can still arrive before its timeout.
            return
        if not pending.future.done():
            pending.future.set_result(response)

    async def _handle_request(self, relay_message: Any, payload: dict[str, Any]) -> None:
        frame = payload.get("frame")
        provider_id = payload.get("provider_id")
        if not isinstance(frame, str) or not isinstance(provider_id, str):
            return
        try:
            request_value = json.loads(frame)
            request = StorageRequestEnvelope.from_dict(request_value)
            authenticated_actor = getattr(relay_message, "actor_node_id", None)
            authenticated_session = getattr(relay_message, "session_id", None)
            if request.actor_node_id != authenticated_actor:
                raise FederationValidationError(
                    "unauthorized",
                    "actor_node_id",
                    "storage frame actor does not match the authenticated relay sender",
                )
            if request.session_id != authenticated_session:
                raise FederationValidationError(
                    "session-mismatch",
                    "session_id",
                    "storage frame session does not match the authorized relay route",
                )
            service = self.services.get(provider_id)
            if service is None:
                response = StorageResponseEnvelope(
                    request_id=request.request_id,
                    protocol=STORAGE_PROTOCOL,
                    protocol_version=request.protocol_version,
                    ok=False,
                    error=StorageError(
                        code=StorageErrorCode.UNAUTHORIZED,
                        message="target node does not host the requested storage provider",
                        field="provider_id",
                    ),
                )
            else:
                response = await service.dispatch(request)
        except (FederationValidationError, json.JSONDecodeError) as exc:
            request_id = (
                str(request_value.get("request_id", "invalid-storage-request"))
                if isinstance(locals().get("request_value"), dict)
                else "invalid-storage-request"
            )
            if isinstance(exc, FederationValidationError):
                message = exc.message
                field = exc.field
            else:
                message = "storage frame is not valid JSON"
                field = "frame"
            response = StorageResponseEnvelope(
                request_id=request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.INVALID_REQUEST,
                    message=message,
                    field=field,
                ),
            )
        target_node_id = getattr(relay_message, "actor_node_id", None)
        session_id = getattr(relay_message, "session_id", None)
        if not isinstance(target_node_id, str) or not isinstance(session_id, str):
            return
        await self.relay_client.send_message(
            session_id=session_id,
            target_node_id=target_node_id,
            request_id=f"relay-response-{response.request_id}",
            payload={
                "kind": RELAY_STORAGE_KIND,
                "message": "response",
                "provider_id": provider_id,
                "frame": json.dumps(
                    response.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ),
            },
        )

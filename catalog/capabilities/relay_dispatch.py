"""F7.4 capability dispatch over the existing authenticated relay queue."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

from catalog.federation.errors import FederationValidationError

from .dispatch import (
    CapabilityWorker,
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
)

RELAY_DISPATCH_KIND = "fcp-capability-dispatch-v1"
MAX_PENDING_DISPATCHES = 128


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


@dataclass
class _Pending:
    future: asyncio.Future[DispatchResponse]
    session_id: str
    target_node_id: str


class RelayDispatchEndpoint:
    """Multiplex request/reply dispatch through ``relay.message``.

    The relay authenticates actor, target, and session. This endpoint repeats
    those bindings before allowing a local worker to see a dispatch.
    """

    def __init__(
        self,
        relay_client: RelayMessageClient,
        workers: dict[str, CapabilityWorker] | None = None,
        *,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.workers: dict[str, CapabilityWorker] = {}
        for provider_id, worker in dict(workers or {}).items():
            self._validate_worker(provider_id, worker)
            self.workers[provider_id] = worker
        self.request_timeout = float(request_timeout)
        self._pending: dict[str, _Pending] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._other_messages: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=other_message_limit
        )
        self._closed = False

    def _validate_worker(self, provider_id: str, worker: CapabilityWorker) -> None:
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
        registration = getattr(worker, "registration", None)
        if registration is None or registration.provider_id != provider_id:
            raise FederationValidationError(
                "worker-provider-mismatch",
                "provider_id",
                "registration differs from the endpoint worker key",
            )
        if registration.node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "worker-node-mismatch",
                "node_id",
                "worker registration differs from the local node identity",
            )

    def register_worker(self, provider_id: str, worker: CapabilityWorker) -> None:
        self._validate_worker(provider_id, worker)
        self.workers[provider_id] = worker

    def replace_workers(
        self,
        workers: dict[str, CapabilityWorker],
        *,
        replace_provider_ids: tuple[str, ...],
    ) -> dict[str, CapabilityWorker]:
        """Atomically replace one explicit reconciler-owned worker subset."""

        replacement_ids = tuple(sorted(set(replace_provider_ids)))
        if any(not isinstance(item, str) or not item for item in replacement_ids):
            raise ValueError("replace_provider_ids must contain non-empty text")
        replacement_set = set(replacement_ids)
        normalized = dict(workers)
        for provider_id, worker in normalized.items():
            if provider_id not in replacement_set:
                raise FederationValidationError(
                    "unowned-worker-replacement",
                    "provider_id",
                    "replacement worker must be included in the explicit owned set",
                )
            self._validate_worker(provider_id, worker)
        previous = {
            provider_id: self.workers[provider_id]
            for provider_id in replacement_ids
            if provider_id in self.workers
        }
        if (
            set(previous) == set(normalized)
            and all(previous[key] is normalized[key] for key in normalized)
        ):
            return previous
        for provider_id in replacement_ids:
            self.workers.pop(provider_id, None)
        self.workers.update(normalized)
        return previous

    def unregister_worker(
        self,
        provider_id: str,
        *,
        expected_worker: Any | None = None,
    ) -> bool:
        """Remove one exact local worker without affecting a replacement."""

        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
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

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("relay dispatch endpoint is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-dispatch-relay-{self.relay_client.node_id}",
            )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        reader, self._reader_task = self._reader_task, None
        tasks = tuple(
            task for task in (reader, *self._handler_tasks) if task is not None
        )
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._handler_tasks.clear()
        error = RuntimeError("relay dispatch endpoint closed")
        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending.clear()

    async def request(
        self,
        *,
        target_node_id: str,
        request: DispatchRequest,
    ) -> DispatchResponse:
        await self.start()
        if target_node_id != request.target_node_id:
            raise FederationValidationError(
                "dispatch-target-mismatch",
                "target_node_id",
                "transport target differs from the dispatch target",
            )
        if request.coordinator_node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "dispatch-actor-mismatch",
                "coordinator_node_id",
                "dispatch coordinator must be the authenticated local node",
            )
        if len(self._pending) >= MAX_PENDING_DISPATCHES:
            raise FederationValidationError(
                "too-many-pending-dispatches",
                "dispatch",
                "bounded pending dispatch limit reached",
            )
        if request.dispatch_id in self._pending:
            raise FederationValidationError(
                "duplicate-local-dispatch-id",
                "dispatch_id",
                "dispatch is already in flight",
            )
        future: asyncio.Future[DispatchResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request.dispatch_id] = _Pending(
            future,
            request.session_id,
            target_node_id,
        )
        try:
            delivery = await self.relay_client.send_message(
                session_id=request.session_id,
                target_node_id=target_node_id,
                payload={
                    "kind": RELAY_DISPATCH_KIND,
                    "message": "request",
                    "frame": request.to_json(),
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "capability-route-failed",
                    "target_node_id",
                    "relay did not confirm dispatch delivery",
                )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(request.dispatch_id, None)

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._other_messages.get()
        return await asyncio.wait_for(self._other_messages.get(), timeout=timeout)

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                message = await self.relay_client.receive_message()
                payload = getattr(message, "payload", None)
                if (
                    not isinstance(payload, dict)
                    or payload.get("kind") != RELAY_DISPATCH_KIND
                ):
                    try:
                        self._other_messages.put_nowait(message)
                    except asyncio.QueueFull:
                        pass
                    continue
                if payload.get("message") == "response":
                    self._accept_response(message, payload)
                elif payload.get("message") == "request":
                    task = asyncio.create_task(
                        self._handle_request(message, payload)
                    )
                    self._handler_tasks.add(task)
                    task.add_done_callback(self._finish_handler)
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

    def _accept_response(self, message: Any, payload: dict[str, Any]) -> None:
        frame = payload.get("frame")
        if not isinstance(frame, str):
            return
        response = DispatchResponse.from_json(frame)
        pending = self._pending.get(response.dispatch_id)
        if pending is None or pending.future.done():
            return
        actor = getattr(message, "actor_node_id", None)
        session = getattr(message, "session_id", None)
        if actor != pending.target_node_id or session != pending.session_id:
            pending.future.set_exception(
                FederationValidationError(
                    "dispatch-response-authentication-mismatch",
                    "response",
                    "response actor or session differs from its relay route",
                )
            )
            return
        pending.future.set_result(response)

    async def _handle_request(
        self,
        message: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = payload.get("frame")
        if not isinstance(frame, str):
            return
        request = DispatchRequest.from_json(frame)
        actor = getattr(message, "actor_node_id", None)
        session = getattr(message, "session_id", None)
        worker = self.workers.get(request.provider_id)
        if worker is None:
            response = DispatchResponse(
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
        else:
            response = await worker.handle(
                request,
                authenticated_actor=str(actor or ""),
                authenticated_session=str(session or ""),
            )
        if not isinstance(actor, str) or not isinstance(session, str):
            return
        await self.relay_client.send_message(
            session_id=session,
            target_node_id=actor,
            payload={
                "kind": RELAY_DISPATCH_KIND,
                "message": "response",
                "frame": response.to_json(),
            },
        )

"""F7.5 lifecycle messages over the existing authenticated relay queue."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

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
    JobHeartbeat,
)
from .lifecycle_worker import CancellableCapabilityWorker

RELAY_LIFECYCLE_KIND = "fcp-capability-lifecycle-v1"
MAX_PENDING_LIFECYCLE_REQUESTS = 128
MAX_PENDING_HEARTBEATS = 256


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


class RelayMessageSource(Protocol):
    """Upstream single-reader multiplexer forwarding frames it does not own."""

    async def receive_other(self, *, timeout: float | None = None): ...


@dataclass
class _PendingDispatch:
    future: asyncio.Future[DispatchResponse]
    session_id: str
    target_node_id: str


@dataclass
class _PendingCancellation:
    future: asyncio.Future[CancellationResponse]
    session_id: str
    target_node_id: str


@dataclass(frozen=True)
class AuthenticatedHeartbeat:
    heartbeat: JobHeartbeat
    actor_node_id: str
    session_id: str


class RelayLifecycleEndpoint:
    """Multiplex F7.5 dispatch, cancellation, and heartbeat traffic.

    A relay client has one inbound peer-message stream. ``message_source`` lets a
    product compose this endpoint behind an existing owner of that stream (for
    example remote-AI), while this endpoint in turn forwards non-lifecycle frames
    through :meth:`receive_other` to artifact transport. Exactly one component
    therefore calls ``relay_client.receive_message``.
    """

    def __init__(
        self,
        relay_client: RelayMessageClient,
        workers: dict[str, CancellableCapabilityWorker] | None = None,
        *,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
        message_source: RelayMessageSource | None = None,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.message_source = message_source
        self.workers = dict(workers or {})
        self.request_timeout = float(request_timeout)
        self._dispatches: dict[str, _PendingDispatch] = {}
        self._cancellations: dict[str, _PendingCancellation] = {}
        self._heartbeats: asyncio.Queue[AuthenticatedHeartbeat] = asyncio.Queue(
            maxsize=MAX_PENDING_HEARTBEATS
        )
        self._other: asyncio.Queue[Any] = asyncio.Queue(maxsize=other_message_limit)
        self._reader_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._closed = False

    def _validate_worker(
        self,
        provider_id: str,
        worker: CancellableCapabilityWorker,
    ) -> None:
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
        if worker.registration.provider_id != provider_id:
            raise FederationValidationError(
                "worker-provider-mismatch",
                "provider_id",
                "registration differs from the endpoint worker key",
            )
        if worker.registration.node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "worker-node-mismatch",
                "node_id",
                "registration differs from the local node identity",
            )

    def register_worker(
        self,
        provider_id: str,
        worker: CancellableCapabilityWorker,
    ) -> None:
        self._validate_worker(provider_id, worker)
        self.workers[provider_id] = worker

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

    def replace_workers(
        self,
        workers: dict[str, CancellableCapabilityWorker],
        *,
        replace_provider_ids: tuple[str, ...],
    ) -> dict[str, CancellableCapabilityWorker]:
        """Atomically replace one explicit reconciler-owned worker subset.

        Mirrors ``RelayDispatchEndpoint.replace_workers`` so F8.6 can retain the
        exact pre-mutation mapping and restore it if checkpoint validation fails.
        Validation therefore has no side effects before ``previous`` is captured.
        """

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

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("relay lifecycle endpoint is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-lifecycle-relay-{self.relay_client.node_id}",
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
        error = RuntimeError("relay lifecycle endpoint closed")
        for pending in tuple(self._dispatches.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        for pending in tuple(self._cancellations.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._dispatches.clear()
        self._cancellations.clear()

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
                "transport target differs from dispatch target",
            )
        if request.coordinator_node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "dispatch-actor-mismatch",
                "coordinator_node_id",
                "dispatch coordinator must be the authenticated local node",
            )
        if len(self._dispatches) >= MAX_PENDING_LIFECYCLE_REQUESTS:
            raise FederationValidationError(
                "too-many-pending-dispatches",
                "dispatch",
                "bounded pending dispatch limit reached",
            )
        if request.dispatch_id in self._dispatches:
            raise FederationValidationError(
                "duplicate-local-dispatch-id",
                "dispatch_id",
                "dispatch is already in flight",
            )
        future: asyncio.Future[DispatchResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._dispatches[request.dispatch_id] = _PendingDispatch(
            future, request.session_id, target_node_id
        )
        try:
            await self._deliver(
                session_id=request.session_id,
                target_node_id=target_node_id,
                message="dispatch-request",
                frame=request.to_json(),
            )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._dispatches.pop(request.dispatch_id, None)

    async def cancel(
        self,
        *,
        target_node_id: str,
        request: CancellationRequest,
    ) -> CancellationResponse:
        await self.start()
        if target_node_id != request.target_node_id:
            raise FederationValidationError(
                "cancellation-target-mismatch",
                "target_node_id",
                "transport target differs from cancellation target",
            )
        if request.coordinator_node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "cancellation-actor-mismatch",
                "coordinator_node_id",
                "cancellation coordinator must be the authenticated local node",
            )
        if len(self._cancellations) >= MAX_PENDING_LIFECYCLE_REQUESTS:
            raise FederationValidationError(
                "too-many-pending-cancellations",
                "cancellation",
                "bounded pending cancellation limit reached",
            )
        if request.cancellation_id in self._cancellations:
            raise FederationValidationError(
                "duplicate-local-cancellation-id",
                "cancellation_id",
                "cancellation is already in flight",
            )
        future: asyncio.Future[CancellationResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._cancellations[request.cancellation_id] = _PendingCancellation(
            future, request.session_id, target_node_id
        )
        try:
            await self._deliver(
                session_id=request.session_id,
                target_node_id=target_node_id,
                message="cancellation-request",
                frame=request.to_json(),
            )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._cancellations.pop(request.cancellation_id, None)

    async def send_heartbeat(
        self,
        *,
        target_node_id: str,
        heartbeat: JobHeartbeat,
    ) -> None:
        await self.start()
        if heartbeat.worker_node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "heartbeat-actor-mismatch",
                "worker_node_id",
                "heartbeat worker must be the authenticated local node",
            )
        await self._deliver(
            session_id=heartbeat.session_id,
            target_node_id=target_node_id,
            message="heartbeat",
            frame=heartbeat.to_json(),
        )

    async def receive_heartbeat(
        self,
        *,
        timeout: float | None = None,
    ) -> AuthenticatedHeartbeat:
        if timeout is None:
            return await self._heartbeats.get()
        return await asyncio.wait_for(self._heartbeats.get(), timeout=timeout)

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._other.get()
        return await asyncio.wait_for(self._other.get(), timeout=timeout)

    async def _receive(self):
        if self.message_source is not None:
            return await self.message_source.receive_other()
        return await self.relay_client.receive_message()

    async def _deliver(
        self,
        *,
        session_id: str,
        target_node_id: str,
        message: str,
        frame: str,
    ) -> None:
        delivery = await self.relay_client.send_message(
            session_id=session_id,
            target_node_id=target_node_id,
            payload={
                "kind": RELAY_LIFECYCLE_KIND,
                "message": message,
                "frame": frame,
            },
        )
        if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
            raise FederationValidationError(
                "capability-route-failed",
                "target_node_id",
                "relay did not confirm lifecycle delivery",
            )

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                incoming = await self._receive()
                payload = getattr(incoming, "payload", None)
                if (
                    not isinstance(payload, dict)
                    or payload.get("kind") != RELAY_LIFECYCLE_KIND
                ):
                    try:
                        self._other.put_nowait(incoming)
                    except asyncio.QueueFull:
                        pass
                    continue
                message = payload.get("message")
                if message == "dispatch-response":
                    self._accept_dispatch_response(incoming, payload)
                elif message == "cancellation-response":
                    self._accept_cancellation_response(incoming, payload)
                elif message == "heartbeat":
                    self._accept_heartbeat(incoming, payload)
                elif message in {"dispatch-request", "cancellation-request"}:
                    task = asyncio.create_task(
                        self._handle_request(incoming, payload)
                    )
                    self._handler_tasks.add(task)
                    task.add_done_callback(self._finish_handler)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            for pending in tuple(self._dispatches.values()):
                if not pending.future.done():
                    pending.future.set_exception(exc)
            for pending in tuple(self._cancellations.values()):
                if not pending.future.done():
                    pending.future.set_exception(exc)

    def _finish_handler(self, task: asyncio.Task[None]) -> None:
        self._handler_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    @staticmethod
    def _frame(payload: dict[str, Any]) -> str | None:
        frame = payload.get("frame")
        return frame if isinstance(frame, str) else None

    def _accept_dispatch_response(
        self,
        incoming: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = self._frame(payload)
        if frame is None:
            return
        response = DispatchResponse.from_json(frame)
        pending = self._dispatches.get(response.dispatch_id)
        if pending is None or pending.future.done():
            return
        self._authenticate_response(
            pending.future,
            incoming,
            expected_actor=pending.target_node_id,
            expected_session=pending.session_id,
        )
        if not pending.future.done():
            pending.future.set_result(response)

    def _accept_cancellation_response(
        self,
        incoming: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = self._frame(payload)
        if frame is None:
            return
        response = CancellationResponse.from_json(frame)
        pending = self._cancellations.get(response.cancellation_id)
        if pending is None or pending.future.done():
            return
        self._authenticate_response(
            pending.future,
            incoming,
            expected_actor=pending.target_node_id,
            expected_session=pending.session_id,
        )
        if not pending.future.done():
            pending.future.set_result(response)

    @staticmethod
    def _authenticate_response(
        future: asyncio.Future[Any],
        incoming: Any,
        *,
        expected_actor: str,
        expected_session: str,
    ) -> None:
        if (
            getattr(incoming, "actor_node_id", None) != expected_actor
            or getattr(incoming, "session_id", None) != expected_session
        ):
            future.set_exception(
                FederationValidationError(
                    "lifecycle-response-authentication-mismatch",
                    "response",
                    "response actor or session differs from its relay route",
                )
            )

    def _accept_heartbeat(
        self,
        incoming: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = self._frame(payload)
        actor = getattr(incoming, "actor_node_id", None)
        session = getattr(incoming, "session_id", None)
        if frame is None or not isinstance(actor, str) or not isinstance(session, str):
            return
        heartbeat = JobHeartbeat.from_json(frame)
        try:
            self._heartbeats.put_nowait(
                AuthenticatedHeartbeat(heartbeat, actor, session)
            )
        except asyncio.QueueFull as exc:
            raise FederationValidationError(
                "heartbeat-queue-full",
                "heartbeat",
                "bounded heartbeat queue is full",
            ) from exc

    async def _handle_request(
        self,
        incoming: Any,
        payload: dict[str, Any],
    ) -> None:
        frame = self._frame(payload)
        actor = getattr(incoming, "actor_node_id", None)
        session = getattr(incoming, "session_id", None)
        if frame is None or not isinstance(actor, str) or not isinstance(session, str):
            return
        message = payload.get("message")
        if message == "dispatch-request":
            request = DispatchRequest.from_json(frame)
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
                    authenticated_actor=actor,
                    authenticated_session=session,
                )
            await self._deliver(
                session_id=session,
                target_node_id=actor,
                message="dispatch-response",
                frame=response.to_json(),
            )
            return
        request = CancellationRequest.from_json(frame)
        worker = self.workers.get(request.provider_id)
        if worker is None:
            response = CancellationResponse(
                request.cancellation_id,
                request.session_id,
                request.provider_id,
                request.job_id,
                request.attempt_id,
                CancellationState.REJECTED,
                request.requested_at,
                reason_code="provider-not-hosted",
            )
        else:
            response = await worker.cancel(
                request,
                authenticated_actor=actor,
                authenticated_session=session,
            )
        await self._deliver(
            session_id=session,
            target_node_id=actor,
            message="cancellation-response",
            frame=response.to_json(),
        )
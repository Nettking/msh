"""Authenticated remote language-model invocation over the existing relay."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)

from .remote_contracts import (
    RemoteAIInvocationRequest,
    RemoteAIInvocationResponse,
    _node_id,
)
from .remote_provider import (
    RemoteAIHealthAuthority,
    RemoteAIInvocationTransport,
)
from .runtime import AIProviderInvocationError, LanguageModelProvider
from .runtime_contracts import (
    AIProviderFailureClass,
    AIRuntimeError,
    _logical_id,
)

RELAY_REMOTE_AI_KIND = "msh-remote-ai-invocation-v1"
MAX_PENDING_REMOTE_AI_INVOCATIONS = 128
MAX_REMOTE_AI_REPLAY_ENTRIES = 1_024


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
class _PendingInvocation:
    future: asyncio.Future[RemoteAIInvocationResponse]
    session_id: str
    target_node_id: str
    capability_id: str
    provider_generation: int
    request_id: str


@dataclass
class _ReplayEntry:
    fingerprint: str
    future: asyncio.Future[RemoteAIInvocationResponse]


class RemoteAIProviderHost:
    """Invoke one explicitly registered local provider after fresh revalidation."""

    def __init__(
        self,
        provider: LanguageModelProvider,
        authority: RemoteAIHealthAuthority,
        *,
        provider_generation: int,
        clock: Callable[[], datetime] = _utc_now,
        replay_limit: int = MAX_REMOTE_AI_REPLAY_ENTRIES,
    ) -> None:
        if (
            isinstance(provider_generation, bool)
            or not isinstance(provider_generation, int)
            or provider_generation <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-generation",
                "provider_generation",
                "must be a positive integer",
            )
        if (
            isinstance(replay_limit, bool)
            or not isinstance(replay_limit, int)
            or replay_limit <= 0
            or replay_limit > MAX_REMOTE_AI_REPLAY_ENTRIES
        ):
            raise FederationValidationError(
                "invalid-remote-ai-replay-limit",
                "replay_limit",
                f"must be between 1 and {MAX_REMOTE_AI_REPLAY_ENTRIES}",
            )
        self.provider = provider
        self.authority = authority
        self.provider_generation = provider_generation
        self._clock = clock
        self._replay_limit = replay_limit
        self._replay_lock = asyncio.Lock()
        self._replays: dict[str, _ReplayEntry] = {}
        snapshot = authority.current_snapshot(
            provider.capability_id,
            provider_generation=provider_generation,
        )
        expected = (
            snapshot.record.capability_id,
            snapshot.record.node_id,
            snapshot.record.session_id,
            snapshot.report.protocol,
            snapshot.report.protocol_version,
        )
        actual = (
            _logical_id(provider.capability_id, "provider.capability_id"),
            _node_id(provider.node_id, "provider.node_id"),
            _logical_id(provider.session_id, "provider.session_id"),
            provider.protocol,
            provider.protocol_version,
        )
        if actual != expected:
            raise FederationValidationError(
                "remote-host-provider-identity-mismatch",
                "provider",
                "local provider identity differs from current F8.2 health",
            )
        if not set(snapshot.models).issubset(set(provider.models)) or not set(
            snapshot.modalities
        ).issubset(set(provider.modalities)):
            raise FederationValidationError(
                "remote-host-provider-advertisement-mismatch",
                "provider",
                "local provider does not implement its current advertised models and modalities",
            )

    @property
    def capability_id(self) -> str:
        return self.provider.capability_id

    @property
    def node_id(self) -> str:
        return self.provider.node_id

    @property
    def session_id(self) -> str:
        return self.provider.session_id

    async def handle(
        self,
        request: RemoteAIInvocationRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> RemoteAIInvocationResponse:
        failure = self._validate_route(
            request,
            authenticated_actor=authenticated_actor,
            authenticated_session=authenticated_session,
        )
        if failure is not None:
            return failure
        fingerprint = request.to_json()
        owner = False
        async with self._replay_lock:
            replay = self._replays.get(request.invocation_id)
            if replay is not None:
                if replay.fingerprint != fingerprint:
                    return RemoteAIInvocationResponse.failed(
                        request,
                        error_code="remote-invocation-conflict",
                        failure_class=AIProviderFailureClass.PROTOCOL,
                        retryable=False,
                        completed_at=self._clock(),
                    )
                future = replay.future
            else:
                future = asyncio.get_running_loop().create_future()
                self._replays[request.invocation_id] = _ReplayEntry(
                    fingerprint=fingerprint,
                    future=future,
                )
                owner = True
        if not owner:
            return await asyncio.shield(future)
        try:
            response = await self._execute(request)
            if not future.done():
                future.set_result(response)
            return response
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            async with self._replay_lock:
                self._prune_replays()

    def _validate_route(
        self,
        request: RemoteAIInvocationRequest,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ) -> RemoteAIInvocationResponse | None:
        if authenticated_actor != request.requester_node_id:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-invocation-actor-mismatch",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
                retryable=False,
                completed_at=self._clock(),
            )
        if authenticated_session != request.session_id:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-invocation-session-mismatch",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
                retryable=False,
                completed_at=self._clock(),
            )
        if (
            request.provider_node_id != self.node_id
            or request.capability_id != self.capability_id
            or request.session_id != self.session_id
        ):
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-invocation-target-mismatch",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
                retryable=False,
                completed_at=self._clock(),
            )
        if request.provider_generation != self.provider_generation:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-provider-generation-mismatch",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
                completed_at=self._clock(),
            )
        now = self._clock()
        if now < request.sent_at or now >= request.expires_at:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-invocation-expired",
                failure_class=AIProviderFailureClass.TIMEOUT,
                retryable=True,
                completed_at=now,
            )
        try:
            snapshot = self.authority.current_snapshot(
                self.capability_id,
                provider_generation=self.provider_generation,
            )
        except (FederationOperationError, FederationValidationError):
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-unavailable",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
                completed_at=now,
            )
        if request.health_report_revision > snapshot.record.report_revision:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="remote-health-revision-ahead",
                failure_class=AIProviderFailureClass.PROTOCOL,
                retryable=False,
                completed_at=now,
            )
        if request.request.model not in snapshot.models:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-model-unavailable",
                failure_class=AIProviderFailureClass.PROTOCOL,
                retryable=False,
                completed_at=now,
            )
        if request.request.modality.value not in snapshot.modalities:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-modality-unavailable",
                failure_class=AIProviderFailureClass.PROTOCOL,
                retryable=False,
                completed_at=now,
            )
        return None

    async def _execute(
        self,
        request: RemoteAIInvocationRequest,
    ) -> RemoteAIInvocationResponse:
        now = self._clock()
        timeout = min(
            float(request.request.timeout_seconds),
            max(0.001, (request.expires_at - now).total_seconds()),
        )
        cancellation_event = threading.Event()
        try:
            content = await asyncio.wait_for(
                asyncio.to_thread(
                    self.provider.invoke,
                    request.request,
                    timeout_seconds=timeout,
                    cancellation_event=(
                        cancellation_event
                        if bool(self.provider.supports_cancellation)
                        else None
                    ),
                ),
                timeout=timeout,
            )
            return RemoteAIInvocationResponse.succeeded(
                request,
                content=content,
                completed_at=self._clock(),
            )
        except TimeoutError:
            cancellation_event.set()
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-timeout",
                failure_class=AIProviderFailureClass.TIMEOUT,
                retryable=True,
                completed_at=self._clock(),
            )
        except AIProviderInvocationError as exc:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code=exc.code,
                failure_class=exc.failure_class,
                retryable=exc.retryable,
                completed_at=self._clock(),
            )
        except AIRuntimeError as exc:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code=exc.code,
                failure_class=exc.failure_class,
                retryable=exc.retryable,
                completed_at=self._clock(),
            )
        except Exception:
            return RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-internal-error",
                failure_class=AIProviderFailureClass.INTERNAL,
                retryable=False,
                completed_at=self._clock(),
            )

    def _prune_replays(self) -> None:
        if len(self._replays) <= self._replay_limit:
            return
        for invocation_id in tuple(self._replays):
            entry = self._replays[invocation_id]
            if entry.future.done():
                self._replays.pop(invocation_id, None)
            if len(self._replays) <= self._replay_limit:
                break


class RelayRemoteAIEndpoint:
    """Multiplex authenticated remote AI request/reply frames through relay.message."""

    def __init__(
        self,
        relay_client: RelayMessageClient,
        hosts: dict[str, RemoteAIProviderHost] | None = None,
        *,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.hosts = dict(hosts or {})
        self.request_timeout = float(request_timeout)
        self._pending: dict[str, _PendingInvocation] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._other_messages: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=other_message_limit
        )
        self._closed = False

    def register_host(self, host: RemoteAIProviderHost) -> None:
        if not isinstance(host, RemoteAIProviderHost):
            raise FederationValidationError(
                "invalid-remote-ai-host",
                "host",
                "must be a RemoteAIProviderHost",
            )
        if host.node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "remote-ai-host-node-mismatch",
                "node_id",
                "host must belong to the authenticated local relay node",
            )
        if host.capability_id in self.hosts:
            raise FederationValidationError(
                "duplicate-remote-ai-host",
                "capability_id",
                "host is already registered",
            )
        self.hosts[host.capability_id] = host

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("remote AI relay endpoint is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"msh-remote-ai-relay-{self.relay_client.node_id}",
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
        error = RuntimeError("remote AI relay endpoint closed")
        for pending in tuple(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending.clear()

    async def request(
        self,
        *,
        target_node_id: str,
        request: RemoteAIInvocationRequest,
    ) -> RemoteAIInvocationResponse:
        await self.start()
        if not isinstance(request, RemoteAIInvocationRequest):
            raise FederationValidationError(
                "invalid-remote-ai-request",
                "request",
                "must be a RemoteAIInvocationRequest",
            )
        if target_node_id != request.provider_node_id:
            raise FederationValidationError(
                "remote-ai-target-mismatch",
                "target_node_id",
                "transport target differs from the provider node",
            )
        if request.requester_node_id != self.relay_client.node_id:
            raise FederationValidationError(
                "remote-ai-actor-mismatch",
                "requester_node_id",
                "requester must be the authenticated local relay node",
            )
        if len(self._pending) >= MAX_PENDING_REMOTE_AI_INVOCATIONS:
            raise FederationValidationError(
                "too-many-pending-remote-ai-invocations",
                "invocation",
                "bounded pending invocation limit reached",
            )
        if request.invocation_id in self._pending:
            raise FederationValidationError(
                "duplicate-local-remote-ai-invocation",
                "invocation_id",
                "invocation is already in flight",
            )
        future: asyncio.Future[RemoteAIInvocationResponse] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[request.invocation_id] = _PendingInvocation(
            future=future,
            session_id=request.session_id,
            target_node_id=target_node_id,
            capability_id=request.capability_id,
            provider_generation=request.provider_generation,
            request_id=request.request.request_id,
        )
        try:
            delivery = await self.relay_client.send_message(
                session_id=request.session_id,
                target_node_id=target_node_id,
                payload={
                    "kind": RELAY_REMOTE_AI_KIND,
                    "message": "request",
                    "frame": request.to_json(),
                },
            )
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "remote-ai-route-failed",
                    "target_node_id",
                    "relay did not confirm remote invocation delivery",
                )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(request.invocation_id, None)

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
                    or payload.get("kind") != RELAY_REMOTE_AI_KIND
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
        except Exception as exc:
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
        try:
            response = RemoteAIInvocationResponse.from_json(frame)
        except Exception as exc:
            for pending in tuple(self._pending.values()):
                if not pending.future.done():
                    pending.future.set_exception(exc)
            return
        pending = self._pending.get(response.invocation_id)
        if pending is None or pending.future.done():
            return
        actor = getattr(message, "actor_node_id", None)
        session = getattr(message, "session_id", None)
        expected = (
            pending.session_id,
            pending.target_node_id,
            pending.capability_id,
            pending.provider_generation,
            pending.request_id,
        )
        actual = (
            response.session_id,
            actor,
            response.capability_id,
            response.provider_generation,
            response.request_id,
        )
        if session != pending.session_id or actual != expected:
            pending.future.set_exception(
                FederationValidationError(
                    "remote-ai-response-authentication-mismatch",
                    "response",
                    "response actor, session, capability, generation, or request differs from its relay route",
                )
            )
            return
        if response.requester_node_id != self.relay_client.node_id:
            pending.future.set_exception(
                FederationValidationError(
                    "remote-ai-response-requester-mismatch",
                    "response.requester_node_id",
                    "response requester differs from the authenticated local node",
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
        try:
            request = RemoteAIInvocationRequest.from_json(frame)
        except Exception:
            return
        actor = getattr(message, "actor_node_id", None)
        session = getattr(message, "session_id", None)
        if not isinstance(actor, str) or not isinstance(session, str):
            return
        host = self.hosts.get(request.capability_id)
        if host is None:
            response = RemoteAIInvocationResponse.failed(
                request,
                error_code="provider-not-hosted",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
                completed_at=_utc_now(),
            )
        else:
            response = await host.handle(
                request,
                authenticated_actor=actor,
                authenticated_session=session,
            )
        await self.relay_client.send_message(
            session_id=session,
            target_node_id=actor,
            payload={
                "kind": RELAY_REMOTE_AI_KIND,
                "message": "response",
                "frame": response.to_json(),
            },
        )


class BlockingRelayAIInvocationTransport(RemoteAIInvocationTransport):
    """Submit relay coroutines to an already running application event loop."""

    def __init__(
        self,
        endpoint: RelayRemoteAIEndpoint,
        event_loop: asyncio.AbstractEventLoop,
        *,
        target_node_id: str,
        capability_id: str,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        if not isinstance(endpoint, RelayRemoteAIEndpoint):
            raise FederationValidationError(
                "invalid-remote-ai-endpoint",
                "endpoint",
                "must be a RelayRemoteAIEndpoint",
            )
        if not isinstance(event_loop, asyncio.AbstractEventLoop):
            raise FederationValidationError(
                "invalid-event-loop",
                "event_loop",
                "must be an asyncio event loop",
            )
        if poll_interval_seconds <= 0 or poll_interval_seconds > 1:
            raise FederationValidationError(
                "invalid-remote-ai-poll-interval",
                "poll_interval_seconds",
                "must be greater than 0 and at most 1 second",
            )
        self.endpoint = endpoint
        self.event_loop = event_loop
        self.target_node_id = _node_id(target_node_id, "target_node_id")
        self.capability_id = _logical_id(capability_id, "capability_id")
        self.poll_interval_seconds = float(poll_interval_seconds)

    def invoke(
        self,
        request: RemoteAIInvocationRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> RemoteAIInvocationResponse:
        if request.provider_node_id != self.target_node_id:
            raise FederationValidationError(
                "remote-ai-target-mismatch",
                "provider_node_id",
                "request provider differs from the bound transport target",
            )
        if request.capability_id != self.capability_id:
            raise FederationValidationError(
                "remote-ai-capability-mismatch",
                "capability_id",
                "request capability differs from the bound transport",
            )
        if not self.event_loop.is_running():
            raise AIProviderInvocationError(
                "provider-unavailable",
                "The remote AI relay event loop is not running.",
                failure_class=AIProviderFailureClass.TRANSIENT,
                retryable=True,
            )
        future = asyncio.run_coroutine_threadsafe(
            self.endpoint.request(
                target_node_id=self.target_node_id,
                request=request,
            ),
            self.event_loop,
        )
        timeout = (
            max(0.001, (request.expires_at - request.sent_at).total_seconds())
            if timeout_seconds is None
            else max(0.001, float(timeout_seconds))
        )
        deadline = time.monotonic() + timeout
        while True:
            if cancellation_event is not None and cancellation_event.is_set():
                future.cancel()
                raise AIProviderInvocationError(
                    "provider-request-cancelled",
                    "The remote provider request was cancelled.",
                    failure_class=AIProviderFailureClass.CANCELLED,
                    retryable=False,
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                future.cancel()
                raise AIProviderInvocationError(
                    "provider-timeout",
                    "The remote provider did not answer before the timeout.",
                    failure_class=AIProviderFailureClass.TIMEOUT,
                    retryable=True,
                )
            try:
                return future.result(
                    timeout=min(self.poll_interval_seconds, remaining)
                )
            except concurrent.futures.TimeoutError:
                continue
            except concurrent.futures.CancelledError as exc:
                raise AIProviderInvocationError(
                    "provider-request-cancelled",
                    "The remote provider request was cancelled.",
                    failure_class=AIProviderFailureClass.CANCELLED,
                    retryable=False,
                ) from exc

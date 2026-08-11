"""Outbound-only authenticated node client for the relay-first session network."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import math
import ssl
import sys
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, SessionEvent
from catalog.federation.protocol import (
    MAX_MESSAGE_BYTES,
    PROTOCOL_VERSION,
    RelayEnvelope,
    utc_now,
)
from catalog.relay.authentication import authentication_message

from .identity import IdentityStore, NodeCredentials
from .state import (
    ConnectionState,
    EnrollmentState,
    EventApplyStatus,
    NodeState,
    NodeStateError,
    ReconnectPolicy,
)

DEFAULT_HEARTBEAT_INTERVAL = 10.0
DEFAULT_REQUEST_TIMEOUT = 15.0
DEFAULT_REPLAY_PAGE_TIMEOUT = 60.0
MAX_CLIENT_INTERVAL_SECONDS = 3_600.0
MAX_INBOUND_MESSAGES = 256
MAX_PENDING_MESSAGE_RESPONSES = 128
MAX_PENDING_REQUESTS = 128
MAX_REPLAY_PAGES_PER_PASS = 1_024
MAX_STATUS_PAGES = 1_024
MAX_STATUS_SNAPSHOT_RESTARTS = 3


class RelayRemoteError(FederationOperationError):
    """A structured rejection returned by the relay."""


@dataclass(frozen=True)
class _PendingMessageResponse:
    session_id: str
    target_node_id: str
    future: asyncio.Future[RelayEnvelope]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _request_id() -> str:
    return f"request-{uuid.uuid4().hex}"


def _validate_relay_url(
    relay_url: str, *, allow_insecure_local: bool
) -> None:
    parsed = urlsplit(relay_url)
    if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
        raise NodeStateError(
            "invalid-relay-url",
            "relay_url",
            "must be a ws:// or wss:// URL with a host",
        )
    if parsed.username or parsed.password:
        raise NodeStateError(
            "relay-url-credentials",
            "relay_url",
            "credentials must not be embedded in the relay URL",
        )
    if parsed.scheme == "ws":
        loopback = parsed.hostname.lower() in {"127.0.0.1", "::1", "localhost"}
        if not loopback or not allow_insecure_local:
            raise NodeStateError(
                "insecure-relay-url",
                "relay_url",
                "plaintext WebSocket is allowed only for explicitly enabled loopback development",
            )


class RelayNodeClient:
    """One node identity with one permanent outbound relay connection."""

    def __init__(
        self,
        *,
        state_directory: Path | str,
        relay_url: str,
        display_name: str,
        allow_insecure_local: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        reconnect_policy: ReconnectPolicy | None = None,
        clock: Callable[[], datetime] = _now,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        _validate_relay_url(
            relay_url, allow_insecure_local=allow_insecure_local
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 < value <= MAX_CLIENT_INTERVAL_SECONDS
            for value in (heartbeat_interval, request_timeout)
        ):
            raise NodeStateError(
                "invalid-client-timeout",
                "timeout",
                (
                    "heartbeat and request timeouts must be finite, positive, "
                    f"and at most {MAX_CLIENT_INTERVAL_SECONDS:g} seconds"
                ),
            )
        self.state_directory = Path(state_directory)
        self.relay_url = relay_url
        self.credentials: NodeCredentials = IdentityStore(
            self.state_directory, display_name=display_name
        ).load_or_create(now=clock())
        self.state = NodeState(
            self.state_directory / "node_state.sqlite3",
            node_id=self.credentials.identity.node_id,
            now=clock(),
        )
        self.ssl_context = ssl_context
        self.heartbeat_interval = heartbeat_interval
        self.request_timeout = request_timeout
        self.reconnect_policy = reconnect_policy or ReconnectPolicy()
        self._clock = clock
        self._sleep = sleep
        self._websocket: ClientConnection | None = None
        self._send_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_message_responses: dict[
            str, _PendingMessageResponse
        ] = {}
        self._replay_tasks: dict[
            str, asyncio.Task[dict[str, Any]]
        ] = {}
        self._gap_replay_tasks: dict[str, asyncio.Task[None]] = {}
        self._receiver_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._inbound: asyncio.Queue[RelayEnvelope] = asyncio.Queue(
            maxsize=MAX_INBOUND_MESSAGES
        )
        self.connected_event = asyncio.Event()
        self.disconnected_event = asyncio.Event()
        self.disconnected_event.set()

    @property
    def node_id(self) -> str:
        return self.credentials.identity.node_id

    @property
    def inbound_listener_ports(self) -> tuple[int, ...]:
        """Node agents never bind a network listener in relay-first Phase 2."""

        return ()

    async def connect(self, *, enrollment_token: str | None = None) -> None:
        if self._websocket is not None:
            raise NodeStateError(
                "connection-already-open",
                "connection",
                "node already has an outbound relay connection",
            )
        self.state.set_connection_state(
            ConnectionState.CONNECTING, now=self._clock()
        )
        websocket: ClientConnection | None = None
        try:
            websocket = await connect(
                self.relay_url,
                ssl=self.ssl_context,
                proxy=None,
                open_timeout=self.request_timeout,
                close_timeout=min(self.request_timeout, 5.0),
                ping_interval=20,
                ping_timeout=20,
                max_size=MAX_MESSAGE_BYTES,
                max_queue=16,
                write_limit=MAX_MESSAGE_BYTES,
                compression=None,
            )
            challenge_raw = await asyncio.wait_for(
                websocket.recv(), timeout=self.request_timeout
            )
            challenge = RelayEnvelope.from_json(challenge_raw)
            if (
                challenge.message_type != "auth.challenge"
                or challenge.actor_node_id == self.node_id
                or not isinstance(challenge.payload.get("nonce"), str)
            ):
                raise RelayRemoteError(
                    "invalid-auth-challenge",
                    "relay did not provide a valid authentication challenge",
                )
            if enrollment_token is not None:
                await websocket.send(
                    self._envelope(
                        message_type="node.enroll",
                        payload={
                            "identity": self.credentials.identity.to_dict(),
                            "token": enrollment_token,
                        },
                        authorization_context={"kind": "enrollment-token"},
                    ).to_json()
                )
            signed = authentication_message(
                challenge_id=challenge.request_id,
                nonce=challenge.payload["nonce"],
                node_id=self.node_id,
                protocol_version=PROTOCOL_VERSION,
            )
            await websocket.send(
                self._envelope(
                    message_type="auth.response",
                    request_id=challenge.request_id,
                    payload={
                        "challenge_id": challenge.request_id,
                        "nonce": challenge.payload["nonce"],
                        "signature": self.credentials.sign(signed),
                    },
                    authorization_context={"kind": "node-identity-proof"},
                ).to_json()
            )
            authenticated = False
            enrollment_accepted = enrollment_token is None
            for _ in range(3):
                response_raw = await asyncio.wait_for(
                    websocket.recv(), timeout=self.request_timeout
                )
                response = RelayEnvelope.from_json(response_raw)
                if response.message_type == "relay.error":
                    raise self._remote_error(response)
                if response.message_type == "node.enroll.accepted":
                    enrollment_accepted = True
                    authenticated = (
                        response.payload.get("authentication_state")
                        == "authenticated"
                    )
                if response.message_type == "auth.accepted":
                    authenticated = True
                if authenticated and enrollment_accepted:
                    break
            if not authenticated or not enrollment_accepted:
                raise RelayRemoteError(
                    "authentication-incomplete",
                    "relay did not complete authenticated enrollment",
                )
        except (
            ConnectionClosed,
            OSError,
            TimeoutError,
            FederationOperationError,
            FederationValidationError,
            ValueError,
        ) as error:
            if websocket is not None:
                await websocket.close()
            if (
                isinstance(error, RelayRemoteError)
                and error.code == "revoked-node"
            ):
                self.state.set_enrollment_state(
                    EnrollmentState.REVOKED, now=self._clock()
                )
            else:
                self.state.set_connection_state(
                    ConnectionState.ERROR,
                    now=self._clock(),
                    error_code="connection-failed",
                )
            raise

        self._websocket = websocket
        self.state.set_enrollment_state(
            EnrollmentState.ENROLLED, now=self._clock()
        )
        self.state.set_connection_state(
            ConnectionState.CONNECTED, now=self._clock()
        )
        self.disconnected_event.clear()
        self.connected_event.set()
        self._receiver_task = asyncio.create_task(
            self._receiver_loop(), name=f"fcp-receiver-{self.node_id}"
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"fcp-heartbeat-{self.node_id}"
        )
        try:
            coordinator_status = await self.coordinator_status()
            self._reconcile_sessions(coordinator_status)
            for session in self.state.joined_sessions():
                await self.request_replay(session.session_id)
            for capability in self.state.advertised_capabilities():
                await self.announce_capability(capability)
        except asyncio.CancelledError:
            await self.disconnect(error_code="initial-replay-cancelled")
            raise
        except (
            ConnectionClosed,
            OSError,
            TimeoutError,
            FederationOperationError,
            FederationValidationError,
            KeyError,
            TypeError,
            ValueError,
        ):
            await self.disconnect(error_code="initial-replay-failed")
            raise

    def _reconcile_sessions(
        self, coordinator_status: dict[str, Any]
    ) -> None:
        session_values = coordinator_status.get("sessions")
        if not isinstance(session_values, list):
            raise RelayRemoteError(
                "invalid-coordinator-status",
                "coordinator status did not contain a session list",
            )
        remote_session_ids: set[str] = set()
        for value in session_values:
            if not isinstance(value, dict):
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator session status was malformed",
                )
            session_id = value.get("session_id")
            if not isinstance(session_id, str) or not session_id:
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator session ID was malformed",
                )
            remote_session_ids.add(session_id)
        now = self._clock()
        local_session_ids = {
            session.session_id for session in self.state.joined_sessions()
        }
        for session_id in sorted(local_session_ids - remote_session_ids):
            self.state.remove_session(session_id, now=now)
        for session_id in sorted(remote_session_ids - local_session_ids):
            self.state.join_session(session_id, now=now)

    async def disconnect(self, *, error_code: str | None = None) -> None:
        current = asyncio.current_task()
        heartbeat_task, self._heartbeat_task = self._heartbeat_task, None
        receiver_task, self._receiver_task = self._receiver_task, None
        replay_tasks = tuple(self._replay_tasks.items())
        gap_replay_tasks = tuple(self._gap_replay_tasks.items())
        websocket, self._websocket = self._websocket, None
        tasks = [
            task
            for task in (
                heartbeat_task,
                receiver_task,
                *(task for _, task in replay_tasks),
                *(task for _, task in gap_replay_tasks),
            )
            if task is not None and task is not current
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for session_id, task in replay_tasks:
            if self._replay_tasks.get(session_id) is task:
                self._replay_tasks.pop(session_id, None)
        for session_id, task in gap_replay_tasks:
            if (
                task is not current
                and self._gap_replay_tasks.get(session_id) is task
            ):
                self._gap_replay_tasks.pop(session_id, None)
        if websocket is not None:
            await websocket.close()
        error = RelayRemoteError(
            error_code or "connection-closed", "relay connection closed"
        )
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for pending in tuple(self._pending_message_responses.values()):
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending_message_responses.clear()
        self.connected_event.clear()
        self.disconnected_event.set()
        enrollment_state = self.state.status()["enrollment_state"]
        if enrollment_state == EnrollmentState.REVOKED.value:
            status = ConnectionState.REVOKED
        else:
            status = (
                ConnectionState.ERROR
                if error_code
                else ConnectionState.DISCONNECTED
            )
        self.state.set_connection_state(
            status,
            now=self._clock(),
            error_code=(
                None if status is ConnectionState.REVOKED else error_code
            ),
        )

    async def run_forever(self) -> None:
        """Reconnect with a finite, capped schedule and stop in explicit error."""

        last_error = "reconnect-exhausted"
        for attempt in range(self.reconnect_policy.max_attempts):
            try:
                await self.connect()
                await self.disconnected_event.wait()
                last_error = "relay-disconnected"
            except (
                ConnectionClosed,
                OSError,
                TimeoutError,
                FederationOperationError,
                FederationValidationError,
            ) as exc:
                last_error = getattr(exc, "code", "relay-unavailable")
            if (
                self.state.status()["enrollment_state"]
                == EnrollmentState.REVOKED.value
            ):
                return
            if attempt + 1 < self.reconnect_policy.max_attempts:
                await self._sleep(self.reconnect_policy.delay_for_attempt(attempt))
        self.state.set_connection_state(
            ConnectionState.ERROR,
            now=self._clock(),
            error_code=last_error
            if isinstance(last_error, str) and last_error.replace("-", "").isalnum()
            else "reconnect-exhausted",
        )

    async def request(
        self,
        message_type: str,
        *,
        payload: dict[str, Any],
        session_id: str | None = None,
        target_node_id: str | None = None,
        request_id: str | None = None,
        authorization_context: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        websocket = self._websocket
        if websocket is None or self._receiver_task is None:
            raise NodeStateError(
                "connection-not-open",
                "connection",
                "request requires an authenticated relay connection",
            )
        envelope = self._envelope(
            message_type=message_type,
            payload=payload,
            session_id=session_id,
            target_node_id=target_node_id,
            request_id=request_id,
            authorization_context=authorization_context,
        )
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        if len(self._pending) >= MAX_PENDING_REQUESTS:
            raise NodeStateError(
                "too-many-pending-requests",
                "request",
                "bounded pending request limit reached",
            )
        if envelope.request_id in self._pending:
            raise NodeStateError(
                "duplicate-local-request-id",
                "request_id",
                "request ID already has an in-flight operation",
            )
        self._pending[envelope.request_id] = future
        try:
            async with self._send_lock:
                await websocket.send(envelope.to_json())
            return await asyncio.wait_for(
                future,
                timeout=self.request_timeout if timeout is None else timeout,
            )
        finally:
            self._pending.pop(envelope.request_id, None)

    async def create_session(
        self, display_name: str, *, request_id: str | None = None
    ) -> dict[str, Any]:
        result = await self.request(
            "session.create",
            payload={"display_name": display_name},
            request_id=request_id,
        )
        session = result["session"]
        self.state.join_session(session["session_id"], now=self._clock())
        await self.request_replay(session["session_id"])
        return session

    async def create_invitation(
        self,
        session_id: str,
        *,
        ttl_seconds: int = 600,
        max_uses: int = 1,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.request(
            "session.invite",
            session_id=session_id,
            payload={"ttl_seconds": ttl_seconds, "max_uses": max_uses},
            request_id=request_id,
        )

    async def join_session(
        self, token: str, *, request_id: str | None = None
    ) -> dict[str, Any]:
        result = await self.request(
            "session.join",
            payload={"token": token},
            request_id=request_id,
            authorization_context={"kind": "session-invitation"},
        )
        session = result["session"]
        self.state.join_session(session["session_id"], now=self._clock())
        await self.request_replay(session["session_id"])
        return session

    async def announce_capability(
        self, capability: CapabilityAnnouncement
    ) -> dict[str, Any]:
        result = await self.request(
            "capability.announce",
            session_id=capability.session_id,
            payload={"announcement": capability.to_dict()},
        )
        save = getattr(self.state, "save_capability", None)
        if save is not None:
            save(capability, now=self._clock())
        await self.request_replay(capability.session_id)
        return result

    async def append_event(
        self,
        *,
        session_id: str,
        event_type: str,
        payload: dict[str, Any],
        request_id: str | None = None,
    ) -> SessionEvent:
        result = await self.request(
            "event.append",
            session_id=session_id,
            payload={"event_type": event_type, "payload": payload},
            request_id=request_id,
        )
        event = SessionEvent.from_dict(result["event"])
        applied = self.state.apply_event(event, now=self._clock())
        if applied.status is EventApplyStatus.GAP:
            await self.request_replay(session_id)
        return event

    async def send_message(
        self,
        *,
        session_id: str,
        target_node_id: str,
        payload: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self.request(
            "relay.message",
            session_id=session_id,
            target_node_id=target_node_id,
            payload=payload,
            request_id=request_id,
        )

    async def request_message_response(
        self,
        session_id: str,
        target_node_id: str,
        payload: dict[str, Any],
        correlation_id: str,
        timeout: float | None = None,
    ) -> RelayEnvelope:
        """Send one routed message and await its authenticated correlated reply."""

        if not isinstance(correlation_id, str) or not correlation_id:
            raise NodeStateError(
                "invalid-correlation-id",
                "correlation_id",
                "correlation ID must be a non-empty string",
            )
        if (
            "correlation_id" in payload
            and payload["correlation_id"] != correlation_id
        ):
            raise NodeStateError(
                "correlation-id-mismatch",
                "correlation_id",
                "payload correlation ID does not match the requested reply",
            )
        if correlation_id in self._pending_message_responses:
            raise NodeStateError(
                "duplicate-local-correlation-id",
                "correlation_id",
                "correlation ID already has an in-flight message response",
            )
        if (
            len(self._pending_message_responses)
            >= MAX_PENDING_MESSAGE_RESPONSES
        ):
            raise NodeStateError(
                "too-many-pending-message-responses",
                "correlation_id",
                "bounded pending message response limit reached",
            )

        outbound_payload = dict(payload)
        outbound_payload["correlation_id"] = correlation_id
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RelayEnvelope] = loop.create_future()
        pending = _PendingMessageResponse(
            session_id=session_id,
            target_node_id=target_node_id,
            future=future,
        )
        self._pending_message_responses[correlation_id] = pending
        try:
            delivery = await self.send_message(
                session_id=session_id,
                target_node_id=target_node_id,
                payload=outbound_payload,
            )
            if (
                not isinstance(delivery, dict)
                or delivery.get("delivered") is not True
            ):
                raise RelayRemoteError(
                    "message-delivery-not-confirmed",
                    "relay did not confirm routed message delivery",
                )
            return await asyncio.wait_for(
                future,
                timeout=self.request_timeout if timeout is None else timeout,
            )
        finally:
            if self._pending_message_responses.get(correlation_id) is pending:
                self._pending_message_responses.pop(correlation_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # A disconnect can fail both the delivery request and this reply
                # future. Retrieve the latter even when delivery fails first.
                future.exception()

    async def request_replay(self, session_id: str) -> dict[str, Any]:
        task = self._replay_tasks.get(session_id)
        if task is None or task.done():
            task = asyncio.create_task(
                self._request_replay_pass(session_id),
                name=f"fcp-replay-{session_id}",
            )
            self._replay_tasks[session_id] = task
            task.add_done_callback(
                lambda completed: self._observe_replay_task(
                    session_id, completed
                )
            )
        return await asyncio.shield(task)

    async def _request_replay_pass(self, session_id: str) -> dict[str, Any]:
        self.state.mark_session_replaying(session_id, now=self._clock())
        result: dict[str, Any] = {}
        for _ in range(MAX_REPLAY_PAGES_PER_PASS):
            before_revision = self.state.last_applied_revision(session_id)
            result = await self.request(
                "event.replay",
                session_id=session_id,
                payload={"last_applied_revision": before_revision},
                timeout=max(
                    self.request_timeout,
                    DEFAULT_REPLAY_PAGE_TIMEOUT,
                ),
            )
            current_revision = result.get(
                "current_revision", result.get("last_revision")
            )
            last_revision = result.get("last_revision", current_revision)
            has_more = result.get("has_more", False)
            if (
                isinstance(current_revision, bool)
                or not isinstance(current_revision, int)
                or isinstance(last_revision, bool)
                or not isinstance(last_revision, int)
                or not isinstance(has_more, bool)
            ):
                raise RelayRemoteError(
                    "invalid-replay-completion",
                    "relay did not provide a valid bounded replay completion",
                )
            after_revision = self.state.last_applied_revision(session_id)
            if (
                last_revision < before_revision
                or after_revision < last_revision
                or current_revision < last_revision
            ):
                raise RelayRemoteError(
                    "invalid-replay-completion",
                    "relay replay revisions were inconsistent",
                )
            if has_more:
                if after_revision <= before_revision:
                    raise RelayRemoteError(
                        "revision-gap",
                        "paged replay made no durable progress",
                    )
                self.state.mark_session_replaying(
                    session_id,
                    now=self._clock(),
                    target_revision=current_revision,
                )
                continue
            completed = self.state.complete_replay(
                session_id,
                final_revision=current_revision,
                now=self._clock(),
            )
            if not completed.replaying:
                return result
            if after_revision <= before_revision:
                raise RelayRemoteError(
                    "revision-gap",
                    "coordinator replay did not supply a required revision",
                )
        raise RelayRemoteError(
            "replay-page-limit-exceeded",
            "coordinator replay exceeded the bounded page count",
        )

    def _observe_replay_task(
        self,
        session_id: str,
        task: asyncio.Task[dict[str, Any]],
    ) -> None:
        if self._replay_tasks.get(session_id) is task:
            self._replay_tasks.pop(session_id, None)
        if not task.cancelled():
            task.exception()

    async def coordinator_replay_page(
        self,
        *,
        session_id: str,
        last_applied_revision: int,
        limit: int,
    ) -> tuple[tuple[SessionEvent, ...], int]:
        """Refresh authority and return one bounded cached replay page."""

        result = await self.request_replay(session_id)
        current_revision = result.get("current_revision")
        if (
            isinstance(current_revision, bool)
            or not isinstance(current_revision, int)
            or current_revision < 0
        ):
            raise RelayRemoteError(
                "invalid-replay-completion",
                "relay did not provide a valid replay revision",
            )
        events = self.state.applied_event_page(
            session_id,
            after_revision=last_applied_revision,
            through_revision=current_revision,
            limit=limit,
        )
        expected_revision = last_applied_revision + 1
        for event in events:
            if event.revision != expected_revision:
                raise RelayRemoteError(
                    "revision-gap",
                    "cached replay did not contain a contiguous event page",
                )
            expected_revision += 1
        expected_page_end = min(
            current_revision,
            last_applied_revision + limit,
        )
        if expected_revision != expected_page_end + 1:
            raise RelayRemoteError(
                "revision-gap",
                "cached replay did not contain the complete event page",
            )
        return events, current_revision

    async def coordinator_status(self) -> dict[str, Any]:
        """Read one complete status snapshot before returning it to callers."""

        for attempt in range(MAX_STATUS_SNAPSHOT_RESTARTS):
            try:
                return await self._coordinator_status_snapshot()
            except RelayRemoteError as error:
                if (
                    error.code != "status-snapshot-changed"
                    or attempt + 1 >= MAX_STATUS_SNAPSHOT_RESTARTS
                ):
                    raise
        raise RelayRemoteError(
            "status-snapshot-changed",
            "coordinator status did not stabilize within the retry bound",
        )

    async def _coordinator_status_snapshot(self) -> dict[str, Any]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        expected_start = 0
        expected_total: int | None = None
        result: dict[str, Any] | None = None

        for _ in range(MAX_STATUS_PAGES):
            page = await self.request(
                "status.get",
                payload={} if cursor is None else {"cursor": cursor},
            )
            if not isinstance(page, dict):
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator status page was not an object",
                )
            schema = page.get("schema")
            coordinator_id = page.get("coordinator_id")
            if (
                schema != "fcp.coordinator_status.v1"
                or not isinstance(coordinator_id, str)
                or not coordinator_id
            ):
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator status identity or schema was malformed",
                )

            page_sections: dict[str, list[Any]] = {}
            for section in ("sessions", "nodes", "capabilities"):
                values = page.get(section)
                if not isinstance(values, list):
                    raise RelayRemoteError(
                        "invalid-coordinator-status",
                        f"coordinator {section} status was malformed",
                    )
                page_sections[section] = values

            pagination = page.get("pagination")
            if pagination is None:
                if cursor is not None:
                    raise RelayRemoteError(
                        "invalid-coordinator-status",
                        "a paginated coordinator status ended without pagination metadata",
                    )
                return page
            if not isinstance(pagination, dict):
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator pagination metadata was malformed",
                )

            page_start = pagination.get("page_start")
            item_count = pagination.get("item_count")
            total_items = pagination.get("total_items")
            has_more = pagination.get("has_more")
            next_cursor = pagination.get("next_cursor")
            actual_count = sum(len(values) for values in page_sections.values())
            if (
                pagination.get("schema")
                != "fcp.coordinator_status.pagination.v1"
                or isinstance(page_start, bool)
                or not isinstance(page_start, int)
                or page_start != expected_start
                or isinstance(item_count, bool)
                or not isinstance(item_count, int)
                or item_count != actual_count
                or item_count < 0
                or isinstance(total_items, bool)
                or not isinstance(total_items, int)
                or total_items < 0
                or not isinstance(has_more, bool)
            ):
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator pagination values were inconsistent",
                )
            if expected_total is None:
                expected_total = total_items
            elif total_items != expected_total:
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator pagination total changed between pages",
                )
            if expected_start + item_count > total_items:
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator pagination advanced past its total",
                )

            if result is None:
                result = {
                    "schema": schema,
                    "coordinator_id": coordinator_id,
                    "sessions": [],
                    "nodes": [],
                    "capabilities": [],
                }
            elif result["coordinator_id"] != coordinator_id:
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator identity changed between status pages",
                )
            for section, values in page_sections.items():
                result[section].extend(values)

            expected_start += item_count
            if has_more:
                if (
                    item_count == 0
                    or not isinstance(next_cursor, str)
                    or not next_cursor
                    or next_cursor in seen_cursors
                ):
                    raise RelayRemoteError(
                        "invalid-coordinator-status",
                        "coordinator pagination did not make bounded progress",
                    )
                seen_cursors.add(next_cursor)
                cursor = next_cursor
                continue
            if next_cursor is not None or expected_start != total_items:
                raise RelayRemoteError(
                    "invalid-coordinator-status",
                    "coordinator pagination ended before the complete status",
                )
            return result

        raise RelayRemoteError(
            "status-page-limit-exceeded",
            "coordinator status exceeded the bounded page count",
        )

    async def receive_message(
        self, *, timeout: float | None = None
    ) -> RelayEnvelope:
        if timeout is None:
            return await self._inbound.get()
        return await asyncio.wait_for(self._inbound.get(), timeout=timeout)

    async def status(self) -> dict[str, Any]:
        local = self.state.status()
        if self.connected_event.is_set():
            local["coordinator"] = await self.coordinator_status()
        else:
            local["coordinator"] = None
        return local

    def _envelope(
        self,
        *,
        message_type: str,
        payload: dict[str, Any],
        session_id: str | None = None,
        target_node_id: str | None = None,
        request_id: str | None = None,
        authorization_context: dict[str, Any] | None = None,
    ) -> RelayEnvelope:
        return RelayEnvelope(
            request_id=request_id or _request_id(),
            session_id=session_id,
            actor_node_id=self.node_id,
            target_node_id=target_node_id,
            message_type=message_type,
            authorization_context=authorization_context
            or {"kind": "authenticated-node"},
            payload=payload,
            sent_at=utc_now(),
        )

    async def _receiver_loop(self) -> None:
        websocket = self._websocket
        if websocket is None:  # pragma: no cover - task construction invariant
            return
        error_code: str | None = None
        try:
            async for raw in websocket:
                envelope = RelayEnvelope.from_json(raw)
                if envelope.target_node_id not in {None, self.node_id}:
                    raise RelayRemoteError(
                        "wrong-target",
                        "relay delivered an envelope for another node",
                    )
                if envelope.message_type == "relay.error":
                    remote_error = self._remote_error(envelope)
                    pending = self._pending.get(envelope.request_id)
                    if pending is not None and not pending.done():
                        pending.set_exception(remote_error)
                    if remote_error.code == "revoked-node":
                        self.state.set_enrollment_state(
                            EnrollmentState.REVOKED, now=self._clock()
                        )
                        error_code = "revoked-node"
                        break
                    continue
                if envelope.message_type in {
                    "event.replay",
                    "event.replay.event",
                } and "event" in envelope.payload:
                    await self._apply_replayed_event(envelope)
                    continue
                if envelope.message_type == "relay.message":
                    if self._resolve_pending_message_response(envelope):
                        continue
                    try:
                        self._inbound.put_nowait(envelope)
                    except asyncio.QueueFull as exc:
                        raise RelayRemoteError(
                            "inbound-queue-full",
                            "bounded inbound relay queue is full",
                        ) from exc
                    continue
                if (
                    envelope.message_type == "session.removed"
                    and envelope.session_id is not None
                ):
                    self.state.remove_session(
                        envelope.session_id, now=self._clock()
                    )
                    continue
                if envelope.message_type.endswith(".accepted") or envelope.message_type in {
                    "event.replay.complete",
                    "replay.complete",
                }:
                    pending = self._pending.get(envelope.request_id)
                    if pending is not None and not pending.done():
                        pending.set_result(envelope.payload)
        except ConnectionClosed:
            error_code = error_code or None
        except (
            FederationOperationError,
            FederationValidationError,
            ValueError,
        ) as exc:
            error_code = getattr(exc, "code", "invalid-relay-message")
        finally:
            if self._websocket is websocket:
                await self.disconnect(error_code=error_code)

    def _resolve_pending_message_response(
        self, envelope: RelayEnvelope
    ) -> bool:
        correlation_id = envelope.payload.get("correlation_id")
        if not isinstance(correlation_id, str):
            return False
        pending = self._pending_message_responses.get(correlation_id)
        if pending is None:
            return False
        if (
            envelope.session_id != pending.session_id
            or envelope.actor_node_id != pending.target_node_id
        ):
            return False
        if not pending.future.done():
            pending.future.set_result(envelope)
        return True

    async def _apply_replayed_event(self, envelope: RelayEnvelope) -> None:
        event = SessionEvent.from_dict(envelope.payload["event"])
        result = self.state.apply_event(event, now=self._clock())
        if result.status is EventApplyStatus.GAP:
            self._schedule_gap_replay(event.session_id)

    def _schedule_gap_replay(self, session_id: str) -> asyncio.Task[None]:
        existing = self._gap_replay_tasks.get(session_id)
        if existing is not None and not existing.done():
            return existing
        task = asyncio.create_task(
            self._run_gap_replay(session_id),
            name=f"fcp-gap-replay-{session_id}",
        )
        self._gap_replay_tasks[session_id] = task
        task.add_done_callback(self._observe_gap_replay_task)
        return task

    async def _run_gap_replay(self, session_id: str) -> None:
        try:
            await self.request_replay(session_id)
        except asyncio.CancelledError:
            raise
        # This background-task boundary must map every failure into durable
        # status; otherwise asyncio can report an unobserved task exception.
        except Exception as error:  # noqa: BLE001
            error_code = self._gap_replay_error_code(error)
            try:
                self.state.mark_session_replaying(
                    session_id, now=self._clock()
                )
            except NodeStateError as state_error:
                if state_error.code not in {
                    "node-revoked",
                    "session-not-joined",
                }:
                    error_code = "gap-replay-state-failed"
            await self.disconnect(error_code=error_code)
        finally:
            current = asyncio.current_task()
            if self._gap_replay_tasks.get(session_id) is current:
                self._gap_replay_tasks.pop(session_id, None)

    @staticmethod
    def _observe_gap_replay_task(task: asyncio.Task[None]) -> None:
        """Retrieve any unexpected task failure so the event loop never loses it."""

        if not task.cancelled():
            task.exception()

    @staticmethod
    def _gap_replay_error_code(error: Exception) -> str:
        code = getattr(error, "code", None)
        allowed = frozenset("abcdefghijklmnopqrstuvwxyz0123456789.-")
        initial = frozenset("abcdefghijklmnopqrstuvwxyz0123456789")
        if (
            isinstance(code, str)
            and 0 < len(code) <= 128
            and code[0] in initial
            and all(character in allowed for character in code)
        ):
            return code
        return "gap-replay-failed"

    async def _heartbeat_loop(self) -> None:
        try:
            while self._websocket is not None:
                await self._sleep(self.heartbeat_interval)
                await self.request("heartbeat", payload={})
                self.state.record_heartbeat(now=self._clock())
        except asyncio.CancelledError:
            raise
        except (
            ConnectionClosed,
            OSError,
            TimeoutError,
            FederationOperationError,
            FederationValidationError,
        ) as exc:
            if self._websocket is not None:
                await self.disconnect(
                    error_code=getattr(exc, "code", "heartbeat-failed")
                )

    @staticmethod
    def _remote_error(envelope: RelayEnvelope) -> RelayRemoteError:
        error = envelope.payload.get("error")
        if not isinstance(error, dict):
            return RelayRemoteError(
                "relay-rejected", "relay rejected the request"
            )
        code = error.get("code")
        message = error.get("message")
        field = error.get("field")
        return RelayRemoteError(
            str(code) if isinstance(code, str) else "relay-rejected",
            str(message) if isinstance(message, str) else "relay rejected the request",
            str(field) if isinstance(field, str) else None,
        )


def _secret_from_input(prompt: str) -> str:
    value = (
        getpass.getpass(prompt)
        if sys.stdin.isatty()
        else sys.stdin.readline().strip()
    )
    if not value:
        raise SystemExit("A token is required on protected input.")
    return value


def _common_client_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--relay", required=True)
    parser.add_argument("--display-name", required=True)
    parser.add_argument(
        "--allow-insecure-local",
        action="store_true",
        help="Permit ws:// only when the relay host is loopback.",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    identity = commands.add_parser("initialize-identity")
    identity.add_argument("--state-dir", required=True)
    identity.add_argument("--display-name", required=True)
    local_status = commands.add_parser("local-status")
    local_status.add_argument("--state-dir", required=True)
    local_status.add_argument("--display-name", required=True)
    for name in (
        "enroll",
        "create-session",
        "create-session-invitation",
        "join-session",
        "run",
        "send",
        "status",
        "append-event",
        "announce-capability",
    ):
        command = commands.add_parser(name)
        _common_client_arguments(command)
        if name == "create-session":
            command.add_argument("--name", required=True)
        elif name == "create-session-invitation":
            command.add_argument("--session-id", required=True)
            command.add_argument("--ttl-seconds", type=int, default=600)
            command.add_argument("--max-uses", type=int, default=1)
        elif name == "run":
            command.add_argument("--send-session-id")
            command.add_argument("--send-target-node-id")
            command.add_argument(
                "--send-payload",
                default='{"text":"FCP relay test"}',
            )
        elif name == "send":
            command.add_argument("--session-id", required=True)
            command.add_argument("--target-node-id", required=True)
            command.add_argument("--payload", default='{"text":"FCP relay test"}')
        elif name == "append-event":
            command.add_argument("--session-id", required=True)
            command.add_argument("--event-type", required=True)
            command.add_argument("--payload", default="{}")
        elif name == "announce-capability":
            command.add_argument("--session-id", required=True)
            command.add_argument("--capability-id", required=True)
            command.add_argument("--type", required=True)
            command.add_argument("--protocol", required=True)
            command.add_argument("--protocol-version", default="1.0")
            command.add_argument("--status", default="ready")
            command.add_argument("--properties", default="{}")
    return parser


async def _print_inbound_messages(client: RelayNodeClient) -> None:
    while True:
        envelope = await client.receive_message()
        print(
            json.dumps(
                {
                    "schema": "fcp.relay.received_message.v1",
                    "request_id": envelope.request_id,
                    "session_id": envelope.session_id,
                    "actor_node_id": envelope.actor_node_id,
                    "target_node_id": envelope.target_node_id,
                    "payload": envelope.payload,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )


async def _run_agent_command(
    client: RelayNodeClient, arguments: argparse.Namespace
) -> None:
    has_session = arguments.send_session_id is not None
    has_target = arguments.send_target_node_id is not None
    if has_session != has_target:
        raise SystemExit(
            "--send-session-id and --send-target-node-id must be provided together"
        )
    runner = asyncio.create_task(
        client.run_forever(), name=f"fcp-run-{client.node_id}"
    )
    connection_waiter = asyncio.create_task(client.connected_event.wait())
    printer: asyncio.Task[None] | None = None
    try:
        done, _ = await asyncio.wait(
            {runner, connection_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if runner in done:
            await runner
            return
        printer = asyncio.create_task(
            _print_inbound_messages(client),
            name=f"fcp-print-{client.node_id}",
        )
        if has_session:
            result = await client.send_message(
                session_id=arguments.send_session_id,
                target_node_id=arguments.send_target_node_id,
                payload=json.loads(arguments.send_payload),
            )
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        await runner
    finally:
        connection_waiter.cancel()
        if printer is not None:
            printer.cancel()
        await asyncio.gather(
            connection_waiter,
            *(task for task in (printer,) if task is not None),
            return_exceptions=True,
        )
        if not runner.done():
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        if client.connected_event.is_set():
            await client.disconnect()


async def _run_command(arguments: argparse.Namespace) -> int:
    if arguments.command == "initialize-identity":
        credentials = IdentityStore(
            arguments.state_dir, display_name=arguments.display_name
        ).load_or_create()
        print(json.dumps(credentials.identity.to_dict(), sort_keys=True))
        return 0
    if arguments.command == "local-status":
        credentials = IdentityStore(
            arguments.state_dir, display_name=arguments.display_name
        ).load()
        state = NodeState(
            Path(arguments.state_dir) / "node_state.sqlite3",
            node_id=credentials.identity.node_id,
        )
        print(json.dumps(state.status(), ensure_ascii=False, sort_keys=True))
        return 0
    client = RelayNodeClient(
        state_directory=arguments.state_dir,
        relay_url=arguments.relay,
        display_name=arguments.display_name,
        allow_insecure_local=arguments.allow_insecure_local,
    )
    if arguments.command == "run":
        await _run_agent_command(client, arguments)
        print(json.dumps(client.state.status(), sort_keys=True))
        return 0
    enrollment_token = (
        _secret_from_input("Enrollment token: ")
        if arguments.command == "enroll"
        else None
    )
    await client.connect(enrollment_token=enrollment_token)
    try:
        if arguments.command == "enroll":
            result: Any = await client.status()
        elif arguments.command == "create-session":
            result = await client.create_session(arguments.name)
        elif arguments.command == "create-session-invitation":
            result = await client.create_invitation(
                arguments.session_id,
                ttl_seconds=arguments.ttl_seconds,
                max_uses=arguments.max_uses,
            )
        elif arguments.command == "join-session":
            result = await client.join_session(
                _secret_from_input("Session invitation: ")
            )
        elif arguments.command == "send":
            result = await client.send_message(
                session_id=arguments.session_id,
                target_node_id=arguments.target_node_id,
                payload=json.loads(arguments.payload),
            )
        elif arguments.command == "status":
            result = await client.status()
        elif arguments.command == "append-event":
            event = await client.append_event(
                session_id=arguments.session_id,
                event_type=arguments.event_type,
                payload=json.loads(arguments.payload),
            )
            result = event.to_dict()
        elif arguments.command == "announce-capability":
            capability = CapabilityAnnouncement(
                capability_id=arguments.capability_id,
                node_id=client.node_id,
                session_id=arguments.session_id,
                type=arguments.type,
                protocol=arguments.protocol,
                protocol_version=arguments.protocol_version,
                status=arguments.status,
                properties=json.loads(arguments.properties),
                announced_at=_now(),
            )
            result = await client.announce_capability(capability)
        else:  # pragma: no cover - argparse exhaustiveness
            raise SystemExit(f"Unknown command: {arguments.command}")
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    finally:
        await client.disconnect()
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    try:
        return asyncio.run(_run_command(arguments))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

"""Independently runnable authenticated WebSocket relay for Phase 2.

The :class:`RelayServer` keeps only live connection handles and bounded
delivery queues in memory. Enrollment, revocation, membership, capabilities,
request deduplication, and authoritative session history remain in the
transactional coordinator store.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import hmac
import ipaddress
import json
import logging
import math
import secrets
import signal
import sqlite3
import ssl
import sys
import uuid
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Final, Self

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

from catalog.common.federation_paths import DEFAULT_COORDINATOR_DATABASE
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.models import (
    CapabilityAnnouncement,
    NodeIdentity,
    SessionEvent,
)
from catalog.federation.protocol import (
    MAX_MESSAGE_BYTES,
    PROTOCOL_VERSION,
    RelayEnvelope,
    utc_now,
)
from catalog.federation.redaction import redact_secrets
from catalog.federation.shared_file_storage import mask_public_jsonl_chunk_paths

from .authentication import (
    create_authentication_nonce,
    verify_authentication_signature,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE: Final = DEFAULT_COORDINATOR_DATABASE
DEFAULT_HOST: Final = "127.0.0.1"
DEFAULT_PORT: Final = 8765
DEFAULT_AUTH_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_SEND_TIMEOUT_SECONDS: Final = 10.0
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS: Final = 45.0
DEFAULT_SWEEP_INTERVAL_SECONDS: Final = 5.0
DEFAULT_INBOUND_QUEUE_SIZE: Final = 16
DEFAULT_OUTBOUND_QUEUE_SIZE: Final = 32
DEFAULT_MAX_CONNECTIONS: Final = 256
MAX_TOKEN_TEXT_BYTES: Final = 512
MAX_COMMAND_PAYLOAD_BYTES: Final = 48_000
MAX_REPLAY_PAGE_EVENTS: Final = 32
MAX_TOKEN_TTL_SECONDS: Final = 7 * 24 * 60 * 60
MAX_TOKEN_USES: Final = 100
MAX_RELAY_INTERVAL_SECONDS: Final = 3_600.0

class RelayConfigurationError(ValueError):
    """The relay was configured with an unsafe or inconsistent transport."""


class RelayTransportError(FederationOperationError):
    """A bounded live-delivery operation could not be completed."""


@dataclass(slots=True)
class _AuthenticationResult:
    node_id: str
    response_request: RelayEnvelope
    response_type: str
    enrolled_during_handshake: bool


@dataclass(slots=True)
class _LiveConnection:
    node_id: str
    connection_id: str
    websocket: ServerConnection
    queue: asyncio.Queue[str | None]
    writer_task: asyncio.Task[None] | None = None
    close_error: str | None = None
    event_delivery_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _is_loopback_host(host: str | None) -> bool:
    if host is None:
        return False
    normalized = host.strip().lower()
    if normalized == "localhost":
        return True
    if "%" in normalized:
        normalized = normalized.split("%", 1)[0]
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _bounded_number(
    value: object,
    *,
    field: str,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise FederationValidationError(
            "invalid-integer",
            field,
            f"must be between {minimum} and {maximum}",
        )
    return value


def _bounded_positive_float(value: float, *, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= MAX_RELAY_INTERVAL_SECONDS
    ):
        raise RelayConfigurationError(
            f"{field} must be finite, positive, and at most "
            f"{MAX_RELAY_INTERVAL_SECONDS:g} seconds"
        )
    return float(value)


def _payload_text(
    payload: dict[str, Any],
    field: str,
    *,
    maximum: int = 512,
) -> str:
    value = payload.get(field)
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-command-field",
            field,
            "must be non-empty bounded text without control characters",
        )
    if redact_secrets(value) != value:
        raise FederationValidationError(
            "nonpublic-command-field",
            field,
            "must not contain credentials, backend paths, or physical addresses",
        )
    return value


def _payload_object(payload: dict[str, Any], field: str) -> dict[str, Any]:
    value = payload.get(field)
    if not isinstance(value, dict):
        raise FederationValidationError(
            "invalid-command-field", field, "must be an object"
        )
    return value


def _bounded_token(payload: dict[str, Any], field: str = "token") -> str:
    value = payload.get(field)
    if (
        not isinstance(value, str)
        or len(value) < 32
        or len(value.encode("utf-8")) > MAX_TOKEN_TEXT_BYTES
        or not value.isascii()
        or any(ord(character) < 33 for character in value)
    ):
        raise FederationValidationError(
            "invalid-token",
            field,
            "must be bounded opaque ASCII text",
        )
    return value


def _ensure_bounded_json(value: object, *, field: str) -> None:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FederationValidationError(
            "invalid-json", field, "must contain JSON-compatible values"
        ) from exc
    if len(encoded) > MAX_COMMAND_PAYLOAD_BYTES:
        raise FederationValidationError(
            "payload-too-large",
            field,
            f"exceeds the relay command bound of {MAX_COMMAND_PAYLOAD_BYTES} bytes",
        )
    redacted = redact_secrets(value)
    # A federated JSONL chunk must name the file it carries, and every key
    # ending in ``_path`` is redacted by the generic pass. Comparing against an
    # expectation in which only that already-validated schema field is masked
    # admits exactly one field of one schema; everything else still fails.
    #
    # Only pay for that second walk when the generic pass actually changed
    # something. Every payload the relay routes goes through here, including
    # storage batch traffic that has no redactable field at all, so the clean
    # case must cost exactly one walk as it did before the allowance existed.
    if redacted != value and redacted != mask_public_jsonl_chunk_paths(value):
        raise FederationValidationError(
            "nonpublic-payload",
            field,
            "must not contain credentials, backend paths, or physical addresses",
        )


def _safe_error_payload(
    error: FederationValidationError | FederationOperationError,
) -> dict[str, Any]:
    if isinstance(error, AuthenticationError):
        message = "authentication rejected"
    elif isinstance(error, AuthorizationError):
        message = "authorization rejected"
    elif isinstance(error, ProtocolCompatibilityError):
        message = "protocol rejected"
    elif isinstance(error, FederationValidationError):
        message = "invalid request"
    else:
        message = "operation rejected"
    payload = {"code": error.code, "message": message}
    if error.field is not None:
        payload["field"] = error.field
    return {"error": payload}


def _redact_secret_fields(value: Any) -> Any:
    """Defense-in-depth redaction for locally printed audit information."""
    return redact_secrets(value)


def _connection_close_code(error: ConnectionClosedError) -> int | None:
    if error.rcvd is not None:
        return error.rcvd.code
    if error.sent is not None:
        return error.sent.code
    return None


class RelayServer:
    """Authenticated, relay-first control-plane WebSocket server."""

    def __init__(
        self,
        coordinator: SessionCoordinator,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        tls_cert: Path | str | None = None,
        tls_key: Path | str | None = None,
        tls_key_password: str | None = None,
        ssl_context: ssl.SSLContext | None = None,
        unsafe_development_plaintext: bool = False,
        auth_timeout_seconds: float = DEFAULT_AUTH_TIMEOUT_SECONDS,
        send_timeout_seconds: float = DEFAULT_SEND_TIMEOUT_SECONDS,
        heartbeat_timeout_seconds: float = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
        sweep_interval_seconds: float = DEFAULT_SWEEP_INTERVAL_SECONDS,
        inbound_queue_size: int = DEFAULT_INBOUND_QUEUE_SIZE,
        outbound_queue_size: int = DEFAULT_OUTBOUND_QUEUE_SIZE,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
    ) -> None:
        if not isinstance(coordinator, SessionCoordinator):
            raise TypeError("coordinator must be a SessionCoordinator")
        if not isinstance(host, str) or not host.strip():
            raise RelayConfigurationError("host must be non-empty text")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise RelayConfigurationError("port must be between 0 and 65535")
        if isinstance(inbound_queue_size, bool) or not 1 <= inbound_queue_size <= 1024:
            raise RelayConfigurationError(
                "inbound_queue_size must be between 1 and 1024"
            )
        if isinstance(outbound_queue_size, bool) or not 1 <= outbound_queue_size <= 1024:
            raise RelayConfigurationError(
                "outbound_queue_size must be between 1 and 1024"
            )
        if (
            isinstance(max_connections, bool)
            or not isinstance(max_connections, int)
            or not 1 <= max_connections <= 10_000
        ):
            raise RelayConfigurationError(
                "max_connections must be between 1 and 10000"
            )
        if ssl_context is not None and (tls_cert is not None or tls_key is not None):
            raise RelayConfigurationError(
                "provide an SSL context or TLS certificate paths, not both"
            )
        if (tls_cert is None) != (tls_key is None):
            raise RelayConfigurationError(
                "TLS certificate and private-key paths must be provided together"
            )
        self.coordinator = coordinator
        self.host = host.strip()
        self.port = port
        self.tls_cert = Path(tls_cert) if tls_cert is not None else None
        self.tls_key = Path(tls_key) if tls_key is not None else None
        self._tls_key_password = tls_key_password
        self._provided_ssl_context = ssl_context
        self.unsafe_development_plaintext = unsafe_development_plaintext
        self.auth_timeout_seconds = _bounded_positive_float(
            auth_timeout_seconds, field="auth_timeout_seconds"
        )
        self.send_timeout_seconds = _bounded_positive_float(
            send_timeout_seconds, field="send_timeout_seconds"
        )
        self.heartbeat_timeout_seconds = _bounded_positive_float(
            heartbeat_timeout_seconds, field="heartbeat_timeout_seconds"
        )
        self.sweep_interval_seconds = _bounded_positive_float(
            sweep_interval_seconds, field="sweep_interval_seconds"
        )
        self.inbound_queue_size = inbound_queue_size
        self.outbound_queue_size = outbound_queue_size
        self.max_connections = max_connections
        self._connection_slots = asyncio.BoundedSemaphore(max_connections)
        self._server: Server | None = None
        self._ssl_context: ssl.SSLContext | None = None
        self._connections: dict[str, _LiveConnection] = {}
        self._connections_lock = asyncio.Lock()
        self._sweep_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._bound_port: int | None = None

    @property
    def is_tls(self) -> bool:
        return (
            self._provided_ssl_context is not None
            or self.tls_cert is not None
        )

    @property
    def bound_port(self) -> int:
        if self._bound_port is None:
            raise RelayConfigurationError("relay has not started")
        return self._bound_port

    @property
    def url(self) -> str:
        scheme = "wss" if self.is_tls else "ws"
        host = self.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{scheme}://{host}:{self.bound_port}"

    def _validate_transport_security(self) -> None:
        if (
            not self.is_tls
            and not _is_loopback_host(self.host)
            and not self.unsafe_development_plaintext
        ):
            raise RelayConfigurationError(
                "plaintext relay binds are limited to loopback; configure TLS or "
                "explicitly opt into --unsafe-development-plaintext"
            )

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        if self._provided_ssl_context is not None:
            return self._provided_ssl_context
        if self.tls_cert is None or self.tls_key is None:
            return None
        if not self.tls_cert.is_file() or not self.tls_key.is_file():
            raise RelayConfigurationError(
                "TLS certificate and private-key paths must be readable files"
            )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        try:
            context.load_cert_chain(
                certfile=str(self.tls_cert),
                keyfile=str(self.tls_key),
                password=self._tls_key_password,
            )
        except (OSError, ssl.SSLError) as exc:
            raise RelayConfigurationError(
                "TLS certificate or private key could not be loaded"
            ) from exc
        return context

    async def start(self) -> None:
        """Start listening and return only after the socket is ready."""

        if self._server is not None:
            raise RelayConfigurationError("relay is already running")
        self._validate_transport_security()
        self._ssl_context = self._build_ssl_context()
        self._stop_event.clear()
        self.coordinator.relay_started()
        self._server = await serve(
            self._handle_connection,
            self.host,
            self.port,
            ssl=self._ssl_context,
            max_size=MAX_MESSAGE_BYTES,
            max_queue=self.inbound_queue_size,
            write_limit=MAX_MESSAGE_BYTES,
            compression=None,
            open_timeout=self.auth_timeout_seconds,
            close_timeout=self.send_timeout_seconds,
            ping_interval=20,
            ping_timeout=20,
        )
        sockets = self._server.sockets
        if not sockets:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            raise RelayConfigurationError("relay did not create a listening socket")
        self._bound_port = int(sockets[0].getsockname()[1])
        self._sweep_task = asyncio.create_task(
            self._stale_sweep_loop(), name="fcp-relay-stale-sweep"
        )
        LOGGER.info(
            "FCP relay listening with %s transport on %s:%d",
            "TLS" if self.is_tls else "loopback plaintext",
            self.host,
            self.bound_port,
        )

    async def stop(self) -> None:
        """Close listeners, live connections, and background work cleanly."""

        server = self._server
        if server is None:
            return
        self._stop_event.set()
        if self._sweep_task is not None:
            self._sweep_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._sweep_task
            self._sweep_task = None
        server.close()
        async with self._connections_lock:
            records = tuple(self._connections.values())
        if records:
            await asyncio.gather(
                *(
                    self._close_record(
                        record,
                        code=1001,
                        reason="relay stopping",
                        error=None,
                    )
                    for record in records
                ),
                return_exceptions=True,
            )
        await server.wait_closed()
        self._server = None
        self._bound_port = None
        LOGGER.info("FCP relay stopped")

    async def serve_forever(self) -> None:
        await self.start()
        if self._server is None:  # pragma: no cover - start establishes invariant
            raise RelayConfigurationError("relay failed to start")
        try:
            await self._server.serve_forever()
        finally:
            await self.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.stop()

    async def revoke_node(
        self,
        *,
        node_id: str,
        reason: str,
        request_id: str | None = None,
        revoked_by: str = "local-admin",
    ) -> bool:
        """Durably revoke a node and immediately close its live connection."""

        effective_request_id = request_id or f"admin-revoke-{uuid.uuid4().hex}"
        session_ids = self.coordinator.session_ids_for_node(node_id)
        changed = self.coordinator.revoke_node(
            node_id=node_id,
            reason=reason,
            request_id=effective_request_id,
            revoked_by=revoked_by,
        )
        if changed:
            for session_id in session_ids:
                event = self.coordinator.event_for_request(
                    session_id=session_id,
                    actor_node_id=self.coordinator.coordinator_id,
                    request_id=f"{effective_request_id}:{session_id}",
                )
                if event is not None:
                    await self._fanout_event(
                        event,
                        request_id=effective_request_id,
                    )
        record = await self._connection_for(node_id)
        if record is not None:
            rejection = AuthenticationError(
                "revoked-node",
                "node identity is revoked",
                "actor_node_id",
            )
            with suppress(ConnectionClosed, RelayTransportError):
                await self._send_live(
                    record,
                    self._error_envelope(
                        None, rejection, target_node_id=node_id
                    ),
                )
                await self._flush_live(record)
            await self._close_record(
                record,
                code=4003,
                reason="node revoked",
                error="revoked-node",
            )
        return changed

    async def _handle_connection(self, websocket: ServerConnection) -> None:
        acquired = False
        try:
            await asyncio.wait_for(
                self._connection_slots.acquire(),
                timeout=min(1.0, self.auth_timeout_seconds),
            )
            acquired = True
        except asyncio.TimeoutError:
            error = RelayTransportError(
                "connection-limit",
                "relay connection capacity is temporarily exhausted",
            )
            self._audit_rejection(
                action="connection.admit",
                reason=error.code,
            )
            with suppress(
                ConnectionClosed,
                asyncio.TimeoutError,
                FederationValidationError,
            ):
                await self._send_direct(
                    websocket,
                    self._error_envelope(
                        None, error, target_node_id=None
                    ),
                )
                await asyncio.wait_for(
                    websocket.close(
                        code=1013, reason="relay connection limit reached"
                    ),
                    timeout=self.send_timeout_seconds,
                )
            return
        try:
            await self._serve_connection(websocket)
        finally:
            if acquired:
                self._connection_slots.release()

    async def _serve_connection(self, websocket: ServerConnection) -> None:
        record: _LiveConnection | None = None
        try:
            authentication = await self._authenticate(websocket)
            if authentication is None:
                return
            try:
                record = await self._register_connection(
                    websocket, authentication.node_id
                )
            except (FederationValidationError, FederationOperationError) as error:
                self._audit_rejection(
                    action="connection.authenticate",
                    reason=error.code,
                    actor_node_id=authentication.node_id,
                    request=authentication.response_request,
                )
                await self._send_direct(
                    websocket,
                    self._error_envelope(
                        authentication.response_request,
                        error,
                        target_node_id=None,
                    ),
                )
                await websocket.close(code=4003, reason="authentication rejected")
                return
            accepted = self._response_envelope(
                authentication.response_request,
                message_type=authentication.response_type,
                payload={
                    "node_id": authentication.node_id,
                    "authentication_state": "authenticated",
                    "enrollment_state": (
                        "enrolled"
                        if authentication.enrolled_during_handshake
                        else "unchanged"
                    ),
                },
                authorization_context={
                    "kind": "authenticated-connection",
                    "actor_node_id": authentication.node_id,
                    "validated_by": self.coordinator.coordinator_id,
                },
            )
            await self._send_live(record, accepted)
            await self._command_loop(record)
        except ConnectionClosed:
            pass
        except (FederationValidationError, FederationOperationError) as error:
            actor = record.node_id if record is not None else None
            self._audit_rejection(
                action="relay.connection",
                reason=error.code,
                actor_node_id=actor,
            )
            if record is not None:
                with suppress(ConnectionClosed, RelayTransportError):
                    await self._send_live(
                        record,
                        self._error_envelope(
                            None, error, target_node_id=record.node_id
                        ),
                    )
                await self._close_record(
                    record,
                    code=1011,
                    reason="relay operation rejected",
                    error=error.code,
                )
        except Exception as error:  # noqa: BLE001 - structured public boundary
            actor = record.node_id if record is not None else None
            self._audit_rejection(
                action="relay.connection",
                reason="internal-relay-error",
                actor_node_id=actor,
            )
            LOGGER.error(
                "relay connection stopped after an internal %s",
                type(error).__name__,
            )
            if record is not None:
                await self._close_record(
                    record,
                    code=1011,
                    reason="internal relay error",
                    error="internal-relay-error",
                )
            else:
                with suppress(ConnectionClosed):
                    await websocket.close(code=1011, reason="internal relay error")
        finally:
            if record is not None:
                await self._unregister_connection(record)

    async def _authenticate(
        self, websocket: ServerConnection
    ) -> _AuthenticationResult | None:
        challenge = RelayEnvelope(
            request_id=f"challenge-{uuid.uuid4().hex}",
            actor_node_id=self.coordinator.coordinator_id,
            message_type="auth.challenge",
            payload={
                "algorithm": "Ed25519",
                "nonce": create_authentication_nonce(),
                "expires_in_seconds": int(self.auth_timeout_seconds),
            },
            authorization_context={"kind": "pre-authentication"},
            sent_at=utc_now(),
            protocol_version=PROTOCOL_VERSION,
        )
        await self._send_direct(websocket, challenge)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.auth_timeout_seconds
        request: RelayEnvelope | None = None
        alleged_actor: str | None = None
        try:
            request = await self._receive_before(websocket, deadline)
            alleged_actor = request.actor_node_id
            if request.session_id is not None or request.target_node_id is not None:
                raise AuthenticationError(
                    "authentication-scope-invalid",
                    "connection authentication must not be session scoped",
                    "session_id",
                )
            enrollment_request: RelayEnvelope | None = None
            enrollment_identity: NodeIdentity | None = None
            enrollment_token: str | None = None
            if request.message_type == "node.enroll":
                enrollment_request = request
                identity_value = request.payload.get("identity")
                if not isinstance(identity_value, dict):
                    raise FederationValidationError(
                        "invalid-command-field",
                        "identity",
                        "must be a public node identity object",
                    )
                enrollment_identity = NodeIdentity.from_dict(identity_value)
                if not hmac.compare_digest(
                    enrollment_identity.node_id, request.actor_node_id
                ):
                    raise AuthenticationError(
                        "actor-identity-mismatch",
                        "submitted identity does not match the envelope actor",
                        "actor_node_id",
                    )
                enrollment_token = _bounded_token(request.payload)
                proof_required = self._response_envelope(
                    request,
                    message_type="node.enroll.proof-required",
                    payload={
                        "algorithm": "Ed25519",
                        "challenge_id": challenge.request_id,
                    },
                    authorization_context={"kind": "pre-authentication"},
                )
                await self._send_direct(websocket, proof_required)
                request = await self._receive_before(websocket, deadline)
            if request.message_type != "auth.response":
                raise AuthenticationError(
                    "authentication-message-required",
                    "expected an authentication response",
                    "message_type",
                )
            if request.session_id is not None or request.target_node_id is not None:
                raise AuthenticationError(
                    "authentication-scope-invalid",
                    "connection authentication must not be session scoped",
                    "session_id",
                )
            expected_actor = (
                enrollment_request.actor_node_id
                if enrollment_request is not None
                else request.actor_node_id
            )
            if not hmac.compare_digest(request.actor_node_id, expected_actor):
                raise AuthenticationError(
                    "actor-identity-mismatch",
                    "authentication actor does not match the enrolled identity",
                    "actor_node_id",
                )
            if not hmac.compare_digest(request.request_id, challenge.request_id):
                raise AuthenticationError(
                    "challenge-id-mismatch",
                    "authentication response does not match this challenge",
                    "request_id",
                )
            response_nonce = _payload_text(
                request.payload, "nonce", maximum=128
            )
            challenge_nonce = str(challenge.payload["nonce"])
            if not hmac.compare_digest(response_nonce, challenge_nonce):
                raise AuthenticationError(
                    "challenge-nonce-mismatch",
                    "authentication response does not match this challenge",
                    "nonce",
                )
            signature = _payload_text(
                request.payload, "signature", maximum=256
            )
            identity = enrollment_identity or self.coordinator.public_identity(
                request.actor_node_id
            )
            if not verify_authentication_signature(
                public_key=identity.public_key,
                signature=signature,
                challenge_id=challenge.request_id,
                nonce=challenge_nonce,
                node_id=request.actor_node_id,
                protocol_version=challenge.protocol_version,
            ):
                raise AuthenticationError(
                    "invalid-authentication-signature",
                    "signature did not prove possession of the enrolled identity",
                    "signature",
                )
            if enrollment_identity is not None:
                if enrollment_token is None:  # pragma: no cover - state invariant
                    raise AuthenticationError(
                        "enrollment-state-invalid",
                        "enrollment token was not retained through proof",
                        "token",
                    )
                try:
                    self.coordinator.enroll_node(
                        enrollment_identity, token=enrollment_token
                    )
                finally:
                    enrollment_token = None
            enrolled_during_handshake = enrollment_identity is not None
            response_request = enrollment_request or request
            return _AuthenticationResult(
                node_id=request.actor_node_id,
                response_request=response_request,
                response_type=(
                    "node.enroll.accepted"
                    if enrolled_during_handshake
                    else "auth.accepted"
                ),
                enrolled_during_handshake=enrolled_during_handshake,
            )
        except ConnectionClosedError as error:
            close_code = _connection_close_code(error)
            reason = (
                "message-too-large"
                if close_code == 1009
                else "authentication-connection-closed"
            )
            self._audit_rejection(
                action="connection.authenticate",
                reason=reason,
                actor_node_id=alleged_actor,
                request=request,
            )
            return None
        except asyncio.TimeoutError:
            error = AuthenticationError(
                "authentication-timeout",
                "authentication did not complete within its bound",
            )
            self._audit_rejection(
                action="connection.authenticate",
                reason=error.code,
                actor_node_id=alleged_actor,
                request=request,
            )
            with suppress(ConnectionClosed):
                await self._send_direct(
                    websocket,
                    self._error_envelope(request, error, target_node_id=None),
                )
                await websocket.close(code=4003, reason="authentication timeout")
            return None
        except (FederationValidationError, FederationOperationError) as error:
            self._audit_rejection(
                action=(
                    request.message_type
                    if request is not None
                    else "connection.authenticate"
                ),
                reason=error.code,
                actor_node_id=alleged_actor,
                request=request,
            )
            with suppress(ConnectionClosed):
                await self._send_direct(
                    websocket,
                    self._error_envelope(request, error, target_node_id=None),
                )
                await websocket.close(code=4003, reason="authentication rejected")
            return None

    async def _receive_before(
        self, websocket: ServerConnection, deadline: float
    ) -> RelayEnvelope:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError
        frame = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        return RelayEnvelope.from_json(frame, max_bytes=MAX_MESSAGE_BYTES)

    async def _register_connection(
        self, websocket: ServerConnection, node_id: str
    ) -> _LiveConnection:
        record = _LiveConnection(
            node_id=node_id,
            connection_id=f"connection-{secrets.token_urlsafe(18)}",
            websocket=websocket,
            queue=asyncio.Queue(maxsize=self.outbound_queue_size),
        )
        record.writer_task = asyncio.create_task(
            self._writer(record), name=f"fcp-relay-writer-{node_id}"
        )
        async with self._connections_lock:
            previous = self._connections.get(node_id)
            self._connections[node_id] = record
        try:
            self.coordinator.connected(
                node_id=node_id, connection_id=record.connection_id
            )
        except (FederationValidationError, FederationOperationError):
            async with self._connections_lock:
                if self._connections.get(node_id) is record:
                    if previous is None:
                        self._connections.pop(node_id, None)
                    else:
                        self._connections[node_id] = previous
            if record.writer_task is not None:
                record.writer_task.cancel()
                with suppress(asyncio.CancelledError):
                    await record.writer_task
            raise
        if previous is not None:
            await self._close_record(
                previous,
                code=4000,
                reason="connection replaced",
                error="connection-replaced",
            )
        return record

    async def _unregister_connection(self, record: _LiveConnection) -> None:
        removed = False
        async with self._connections_lock:
            if self._connections.get(record.node_id) is record:
                self._connections.pop(record.node_id, None)
                removed = True
        if record.writer_task is not None:
            record.writer_task.cancel()
            with suppress(asyncio.CancelledError):
                await record.writer_task
            record.writer_task = None
        if removed:
            events = self.coordinator.disconnected(
                node_id=record.node_id, error=record.close_error
            )
            for event in events:
                await self._fanout_event(
                    event,
                    request_id=event.event_id,
                )

    async def _writer(self, record: _LiveConnection) -> None:
        try:
            while True:
                frame = await record.queue.get()
                try:
                    if frame is None:
                        return
                    await asyncio.wait_for(
                        record.websocket.send(frame),
                        timeout=self.send_timeout_seconds,
                    )
                finally:
                    record.queue.task_done()
        except ConnectionClosed:
            return
        except (
            asyncio.TimeoutError,
            OSError,
            RuntimeError,
        ):
            record.close_error = "send-failed"
            with suppress(ConnectionClosed, asyncio.TimeoutError):
                await asyncio.wait_for(
                    record.websocket.close(
                        code=1011, reason="bounded relay send failed"
                    ),
                    timeout=self.send_timeout_seconds,
                )

    async def _send_direct(
        self, websocket: ServerConnection, envelope: RelayEnvelope
    ) -> None:
        frame = envelope.to_json(max_bytes=MAX_MESSAGE_BYTES)
        await asyncio.wait_for(
            websocket.send(frame), timeout=self.send_timeout_seconds
        )

    async def _send_live(
        self, record: _LiveConnection, envelope: RelayEnvelope
    ) -> None:
        writer = record.writer_task
        if writer is None or writer.done():
            raise RelayTransportError(
                "connection-unavailable",
                "target connection is not available",
                "target_node_id",
            )
        frame = envelope.to_json(max_bytes=MAX_MESSAGE_BYTES)
        try:
            await asyncio.wait_for(
                record.queue.put(frame), timeout=self.send_timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            raise RelayTransportError(
                "outbound-queue-timeout",
                "bounded target delivery queue did not become available",
                "target_node_id",
            ) from exc

    async def _flush_live(self, record: _LiveConnection) -> None:
        try:
            await asyncio.wait_for(
                record.queue.join(), timeout=self.send_timeout_seconds
            )
        except asyncio.TimeoutError as exc:
            raise RelayTransportError(
                "outbound-flush-timeout",
                "bounded target delivery did not complete",
                "target_node_id",
            ) from exc

    async def _close_record(
        self,
        record: _LiveConnection,
        *,
        code: int,
        reason: str,
        error: str | None,
    ) -> None:
        if error is not None:
            record.close_error = error
        with suppress(ConnectionClosed, asyncio.TimeoutError):
            await asyncio.wait_for(
                record.websocket.close(code=code, reason=reason),
                timeout=self.send_timeout_seconds,
            )

    async def _connection_for(self, node_id: str) -> _LiveConnection | None:
        async with self._connections_lock:
            return self._connections.get(node_id)

    async def _command_loop(self, record: _LiveConnection) -> None:
        try:
            async for frame in record.websocket:
                request: RelayEnvelope | None = None
                try:
                    request = RelayEnvelope.from_json(
                        frame, max_bytes=MAX_MESSAGE_BYTES
                    )
                    if not hmac.compare_digest(
                        request.actor_node_id, record.node_id
                    ):
                        raise AuthenticationError(
                            "actor-identity-mismatch",
                            "envelope actor does not match the authenticated connection",
                            "actor_node_id",
                        )
                    self.coordinator.require_active_node(record.node_id)
                    await self._dispatch(record, request)
                except (
                    FederationValidationError,
                    FederationOperationError,
                ) as error:
                    self._audit_rejection(
                        action=(
                            request.message_type
                            if request is not None
                            else "relay.receive"
                        ),
                        reason=error.code,
                        actor_node_id=record.node_id,
                        request=request,
                    )
                    with suppress(RelayTransportError, ConnectionClosed):
                        await self._send_live(
                            record,
                            self._error_envelope(
                                request,
                                error,
                                target_node_id=record.node_id,
                            ),
                        )
                    if isinstance(error, AuthenticationError):
                        record.close_error = error.code
                        with suppress(RelayTransportError):
                            await self._flush_live(record)
                        await self._close_record(
                            record,
                            code=4003,
                            reason="authenticated connection rejected",
                            error=error.code,
                        )
                        return
                except Exception as error:  # noqa: BLE001 - structured boundary
                    self._audit_rejection(
                        action=(
                            request.message_type
                            if request is not None
                            else "relay.receive"
                        ),
                        reason="internal-relay-error",
                        actor_node_id=record.node_id,
                        request=request,
                    )
                    LOGGER.error(
                        "relay command stopped after an internal %s",
                        type(error).__name__,
                    )
                    record.close_error = "internal-relay-error"
                    await self._close_record(
                        record,
                        code=1011,
                        reason="internal relay error",
                        error="internal-relay-error",
                    )
                    return
        except ConnectionClosedError as error:
            close_code = _connection_close_code(error)
            if close_code == 1009:
                record.close_error = "message-too-large"
                self._audit_rejection(
                    action="relay.receive",
                    reason="message-too-large",
                    actor_node_id=record.node_id,
                )

    async def _dispatch(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        if request.message_type == "heartbeat":
            self.coordinator.heartbeat(record.node_id)
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="heartbeat.accepted",
                    payload={"connection_state": "connected"},
                    authorization_context={
                        "kind": "authenticated-connection",
                        "actor_node_id": record.node_id,
                        "validated_by": self.coordinator.coordinator_id,
                    },
                ),
            )
            return

        if request.message_type == "session.create":
            display_name = _payload_text(request.payload, "display_name")
            payload_session_id = request.payload.get("session_id")
            if payload_session_id is not None and not isinstance(
                payload_session_id, str
            ):
                raise FederationValidationError(
                    "invalid-command-field",
                    "session_id",
                    "must be opaque text",
                )
            if (
                request.session_id is not None
                and payload_session_id is not None
                and request.session_id != payload_session_id
            ):
                raise AuthorizationError(
                    "wrong-session",
                    "envelope and payload session IDs do not match",
                    "session_id",
                )
            session = self.coordinator.create_session(
                actor_node_id=record.node_id,
                display_name=display_name,
                request_id=request.request_id,
                session_id=request.session_id or payload_session_id,
            )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="session.create.accepted",
                    payload={"session": session.to_dict()},
                    session_id=session.session_id,
                    authorization_context={
                        "kind": "validated-session-creator",
                        "session_id": session.session_id,
                        "actor_node_id": record.node_id,
                        "validated_by": self.coordinator.coordinator_id,
                    },
                ),
            )
            return

        if request.message_type == "session.invite":
            session_id = self._required_session(request)
            ttl_seconds = _bounded_number(
                request.payload.get("ttl_seconds", 600),
                field="ttl_seconds",
                minimum=1,
                maximum=MAX_TOKEN_TTL_SECONDS,
            )
            max_uses = _bounded_number(
                request.payload.get("max_uses", 1),
                field="max_uses",
                minimum=1,
                maximum=MAX_TOKEN_USES,
            )
            invitation = self.coordinator.create_invitation(
                session_id=session_id,
                actor_node_id=record.node_id,
                ttl_seconds=ttl_seconds,
                max_uses=max_uses,
                request_id=request.request_id,
            )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="session.invite.accepted",
                    payload=invitation,
                    authorization_context=self._membership_context(
                        session_id, record.node_id
                    ),
                ),
            )
            return

        if request.message_type == "session.join":
            token = _bounded_token(request.payload)
            session = self.coordinator.join_session(
                node_id=record.node_id,
                token=token,
                request_id=request.request_id,
                expected_session_id=request.session_id,
            )
            joined_event = self.coordinator.event_for_request(
                session_id=session.session_id,
                actor_node_id=record.node_id,
                request_id=f"{request.request_id}:joined",
            )
            if joined_event is not None:
                await self._fanout_event(
                    joined_event,
                    request_id=request.request_id,
                    exclude_node_ids=frozenset({record.node_id}),
                )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="session.join.accepted",
                    payload={"session": session.to_dict()},
                    session_id=session.session_id,
                    authorization_context={
                        "kind": "validated-session-invitation",
                        "session_id": session.session_id,
                        "actor_node_id": record.node_id,
                        "validated_by": self.coordinator.coordinator_id,
                    },
                ),
            )
            return

        if request.message_type == "session.remove":
            session_id = self._required_session(request)
            target_node_id = _payload_text(
                request.payload, "target_node_id"
            )
            reason = _payload_text(
                request.payload, "reason", maximum=512
            )
            removed = self.coordinator.remove_member(
                session_id=session_id,
                actor_node_id=record.node_id,
                target_node_id=target_node_id,
                request_id=request.request_id,
                reason=reason,
            )
            if removed:
                removed_event = self.coordinator.event_for_request(
                    session_id=session_id,
                    actor_node_id=record.node_id,
                    request_id=f"{request.request_id}:left",
                )
                if removed_event is not None:
                    await self._fanout_event(
                        removed_event,
                        request_id=request.request_id,
                    )
                target = await self._connection_for(target_node_id)
                if target is not None:
                    with suppress(RelayTransportError, ConnectionClosed):
                        await self._send_live(
                            target,
                            RelayEnvelope(
                                request_id=request.request_id,
                                session_id=session_id,
                                actor_node_id=self.coordinator.coordinator_id,
                                target_node_id=target_node_id,
                                message_type="session.removed",
                                authorization_context={
                                    "kind": "coordinator-membership-removal",
                                    "session_id": session_id,
                                    "validated_by": self.coordinator.coordinator_id,
                                },
                                payload={"reason": "membership-removed"},
                                sent_at=utc_now(),
                            ),
                        )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="session.remove.accepted",
                    payload={
                        "removed": removed,
                        "target_node_id": target_node_id,
                    },
                    authorization_context=self._membership_context(
                        session_id, record.node_id
                    ),
                ),
            )
            return

        if request.message_type == "capability.announce":
            session_id = self._required_session(request)
            announcement_value = request.payload.get("announcement")
            if not isinstance(announcement_value, dict):
                raise FederationValidationError(
                    "invalid-command-field",
                    "announcement",
                    "must contain a CapabilityAnnouncement object",
                )
            capability = CapabilityAnnouncement.from_dict(announcement_value)
            if capability.session_id != session_id:
                raise AuthorizationError(
                    "wrong-session",
                    "capability and envelope session IDs do not match",
                    "session_id",
                )
            capability, created = self.coordinator.announce_capability(
                capability,
                actor_node_id=record.node_id,
                request_id=request.request_id,
            )
            capability_event = self.coordinator.event_for_request(
                session_id=session_id,
                actor_node_id=record.node_id,
                request_id=f"{request.request_id}:capability",
            )
            if capability_event is not None:
                await self._fanout_event(
                    capability_event,
                    request_id=request.request_id,
                )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="capability.announce.accepted",
                    payload={
                        "announcement": capability.to_dict(),
                        "created": created,
                    },
                    authorization_context=self._membership_context(
                        session_id, record.node_id
                    ),
                ),
            )
            return

        if request.message_type == "event.append":
            session_id = self._required_session(request)
            event_type = _payload_text(request.payload, "event_type")
            event_payload = _payload_object(request.payload, "payload")
            _ensure_bounded_json(event_payload, field="payload")
            event, created = self.coordinator.append_event(
                session_id=session_id,
                actor_node_id=record.node_id,
                request_id=request.request_id,
                event_type=event_type,
                payload=event_payload,
            )
            if created:
                await self._fanout_event(event, request_id=request.request_id)
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="event.append.accepted",
                    payload={"event": event.to_dict(), "created": created},
                    authorization_context=self._membership_context(
                        session_id, record.node_id
                    ),
                ),
            )
            return

        if request.message_type == "event.replay":
            session_id = self._required_session(request)
            last_applied_revision = _bounded_number(
                request.payload.get("last_applied_revision"),
                field="last_applied_revision",
                minimum=0,
                maximum=2**63 - 1,
            )
            # Keep a replay snapshot and its completion contiguous with respect
            # to live event fanout for this connection.
            async with record.event_delivery_lock:
                events, authoritative_revision = self.coordinator.replay_page(
                    session_id=session_id,
                    actor_node_id=record.node_id,
                    last_applied_revision=last_applied_revision,
                    limit=MAX_REPLAY_PAGE_EVENTS,
                )
                context = self._membership_context(session_id, record.node_id)
                for event in events:
                    await self._send_live(
                        record,
                        self._response_envelope(
                            request,
                            message_type="event.replay",
                            payload={"event": event.to_dict()},
                            authorization_context=context,
                        ),
                    )
                last_revision = (
                    events[-1].revision if events else last_applied_revision
                )
                await self._send_live(
                    record,
                    self._response_envelope(
                        request,
                        message_type="event.replay.complete",
                        payload={
                            "current_revision": authoritative_revision,
                            "event_count": len(events),
                            "last_revision": last_revision,
                            "has_more": last_revision < authoritative_revision,
                        },
                        authorization_context=context,
                    ),
                )
            return

        if request.message_type == "status.get":
            cursor_value = request.payload.get("cursor")
            cursor = (
                None
                if cursor_value is None
                else _payload_text(request.payload, "cursor")
            )
            status = self.coordinator.status(
                actor_node_id=record.node_id,
                cursor=cursor,
            )
            await self._send_live(
                record,
                self._response_envelope(
                    request,
                    message_type="status.get.accepted",
                    payload=status,
                    authorization_context={
                        "kind": "authenticated-status",
                        "actor_node_id": record.node_id,
                        "validated_by": self.coordinator.coordinator_id,
                    },
                ),
            )
            return

        if request.message_type == "relay.message":
            await self._route_message(record, request)
            return

        raise FederationValidationError(
            "unsupported-message-type",
            "message_type",
            "message type is not supported by this protocol major",
        )

    async def _route_message(
        self, record: _LiveConnection, request: RelayEnvelope
    ) -> None:
        session_id = self._required_session(request)
        target_node_id = request.target_node_id
        if target_node_id is None:
            raise FederationValidationError(
                "missing-field", "target_node_id", "is required for relay routing"
            )
        _ensure_bounded_json(request.payload, field="payload")
        authorization_context = self.coordinator.authorize_route(
            session_id=session_id,
            actor_node_id=record.node_id,
            target_node_id=target_node_id,
        )
        target = await self._connection_for(target_node_id)
        if target is None:
            raise RelayTransportError(
                "target-disconnected",
                "target node has no active relay connection",
                "target_node_id",
            )
        accepted = self.coordinator.accept_request(
            actor_node_id=record.node_id,
            request_id=request.request_id,
            message_type=request.message_type,
            request_hash=request.fingerprint(),
            session_id=session_id,
            target_node_id=target_node_id,
        )
        if accepted:
            await self._send_live(
                target,
                RelayEnvelope(
                    request_id=request.request_id,
                    session_id=session_id,
                    actor_node_id=record.node_id,
                    target_node_id=target_node_id,
                    message_type="relay.message",
                    authorization_context=authorization_context,
                    payload=request.payload,
                    sent_at=utc_now(),
                ),
            )
        await self._send_live(
            record,
            self._response_envelope(
                request,
                message_type="relay.message.accepted",
                payload={
                    "delivered": accepted,
                    "duplicate": not accepted,
                    "target_node_id": target_node_id,
                },
                authorization_context=authorization_context,
            ),
        )

    async def _fanout_event(
        self,
        event: SessionEvent,
        *,
        request_id: str,
        exclude_node_ids: frozenset[str] = frozenset(),
    ) -> None:
        async with self._connections_lock:
            records = tuple(
                sorted(self._connections.values(), key=lambda item: item.node_id)
        )
        for target in records:
            if target.node_id in exclude_node_ids:
                continue
            try:
                context = self.coordinator.authorize_route(
                    session_id=event.session_id,
                    actor_node_id=target.node_id,
                    target_node_id=target.node_id,
                )
            except (AuthenticationError, AuthorizationError):
                continue
            context = {
                **context,
                "kind": "validated-event-delivery",
                "event_actor_node_id": event.actor_node_id,
            }
            try:
                async with target.event_delivery_lock:
                    await self._send_live(
                        target,
                        RelayEnvelope(
                            request_id=request_id,
                            session_id=event.session_id,
                            actor_node_id=self.coordinator.coordinator_id,
                            target_node_id=target.node_id,
                            message_type="event.replay",
                            authorization_context=context,
                            payload={"event": event.to_dict()},
                            sent_at=utc_now(),
                        ),
                    )
            except (ConnectionClosed, RelayTransportError) as error:
                reason = (
                    error.code
                    if isinstance(error, RelayTransportError)
                    else "connection-closed"
                )
                self._audit_rejection(
                    action="event.deliver",
                    reason=reason,
                    actor_node_id=target.node_id,
                    session_id=event.session_id,
                    request_id=request_id,
                )
                await self._close_record(
                    target,
                    code=1011,
                    reason="bounded event delivery failed",
                    error=reason,
                )

    @staticmethod
    def _required_session(request: RelayEnvelope) -> str:
        if request.session_id is None:
            raise FederationValidationError(
                "missing-field", "session_id", "is required for this command"
            )
        return request.session_id

    def _membership_context(
        self, session_id: str, actor_node_id: str
    ) -> dict[str, Any]:
        return {
            "kind": "validated-session-membership",
            "session_id": session_id,
            "actor_node_id": actor_node_id,
            "validated_by": self.coordinator.coordinator_id,
        }

    def _response_envelope(
        self,
        request: RelayEnvelope,
        *,
        message_type: str,
        payload: dict[str, Any],
        authorization_context: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> RelayEnvelope:
        return RelayEnvelope(
            request_id=request.request_id,
            session_id=session_id or request.session_id,
            actor_node_id=self.coordinator.coordinator_id,
            target_node_id=request.actor_node_id,
            message_type=message_type,
            authorization_context=authorization_context
            or {"kind": "relay-response"},
            payload=payload,
            sent_at=utc_now(),
            protocol_version=PROTOCOL_VERSION,
        )

    def _error_envelope(
        self,
        request: RelayEnvelope | None,
        error: FederationValidationError | FederationOperationError,
        *,
        target_node_id: str | None,
    ) -> RelayEnvelope:
        return RelayEnvelope(
            request_id=(
                request.request_id
                if request is not None
                else f"rejection-{uuid.uuid4().hex}"
            ),
            session_id=request.session_id if request is not None else None,
            actor_node_id=self.coordinator.coordinator_id,
            target_node_id=target_node_id,
            message_type="relay.error",
            authorization_context={"kind": "relay-rejection"},
            payload=_safe_error_payload(error),
            sent_at=utc_now(),
            protocol_version=PROTOCOL_VERSION,
        )

    def _audit_rejection(
        self,
        *,
        action: str,
        reason: str,
        actor_node_id: str | None = None,
        session_id: str | None = None,
        request_id: str | None = None,
        request: RelayEnvelope | None = None,
    ) -> None:
        self.coordinator.audit_rejection(
            action=action,
            reason=reason,
            actor_node_id=actor_node_id,
            session_id=(
                session_id
                if session_id is not None
                else (request.session_id if request is not None else None)
            ),
            request_id=(
                request_id
                if request_id is not None
                else (request.request_id if request is not None else None)
            ),
        )

    async def _stale_sweep_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.sweep_interval_seconds,
                    )
                    continue
                except asyncio.TimeoutError:
                    pass
                stale_nodes, stale_events = self.coordinator.sweep_stale(
                    heartbeat_timeout_seconds=self.heartbeat_timeout_seconds
                )
                for event in stale_events:
                    await self._fanout_event(
                        event,
                        request_id=event.event_id,
                    )
                for node_id in stale_nodes:
                    record = await self._connection_for(node_id)
                    if record is not None:
                        await self._close_record(
                            record,
                            code=4000,
                            reason="stale heartbeat",
                            error="stale-heartbeat",
                        )
                async with self._connections_lock:
                    records = tuple(self._connections.values())
                for record in records:
                    try:
                        self.coordinator.require_active_node(record.node_id)
                    except AuthenticationError as error:
                        if error.code == "revoked-node":
                            for event in self.coordinator.revocation_events(
                                record.node_id
                            ):
                                await self._fanout_event(
                                    event,
                                    request_id=event.event_id,
                                )
                        self._audit_rejection(
                            action="connection.maintain",
                            reason=error.code,
                            actor_node_id=record.node_id,
                        )
                        with suppress(ConnectionClosed, RelayTransportError):
                            await self._send_live(
                                record,
                                self._error_envelope(
                                    None,
                                    error,
                                    target_node_id=record.node_id,
                                ),
                            )
                            await self._flush_live(record)
                        await self._close_record(
                            record,
                            code=4003,
                            reason="node no longer active",
                            error=error.code,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - structured task boundary
            with suppress(
                FederationValidationError,
                FederationOperationError,
                sqlite3.Error,
            ):
                self._audit_rejection(
                    action="heartbeat.sweep",
                    reason="internal-relay-error",
                )
            LOGGER.error(
                "relay stale-heartbeat sweep stopped after an internal %s",
                type(error).__name__,
            )
            self._stop_event.set()
            if self._server is not None:
                self._server.close()
            async with self._connections_lock:
                records = tuple(self._connections.values())
            if records:
                await asyncio.gather(
                    *(
                        self._close_record(
                            record,
                            code=1011,
                            reason="relay heartbeat authority failed",
                            error="heartbeat-sweep-failed",
                        )
                        for record in records
                    ),
                    return_exceptions=True,
                )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m catalog.relay.service",
        description="FCP relay-first Phase 2 control-plane service",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser(
        "serve", help="run the independent WebSocket relay"
    )
    serve_parser.add_argument("--database", default=DEFAULT_DATABASE)
    serve_parser.add_argument("--host", default=DEFAULT_HOST)
    serve_parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    serve_parser.add_argument("--tls-cert")
    serve_parser.add_argument("--tls-key")
    serve_parser.add_argument(
        "--tls-key-password-prompt",
        action="store_true",
        help="read an encrypted TLS key password without shell echo",
    )
    serve_parser.add_argument(
        "--unsafe-development-plaintext",
        action="store_true",
        help=(
            "allow a non-loopback plaintext bind for isolated development only; "
            "never use this as a production security setting"
        ),
    )
    serve_parser.add_argument(
        "--auth-timeout-seconds",
        type=float,
        default=DEFAULT_AUTH_TIMEOUT_SECONDS,
    )
    serve_parser.add_argument(
        "--send-timeout-seconds",
        type=float,
        default=DEFAULT_SEND_TIMEOUT_SECONDS,
    )
    serve_parser.add_argument(
        "--heartbeat-timeout-seconds",
        type=float,
        default=DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
    )
    serve_parser.add_argument(
        "--sweep-interval-seconds",
        type=float,
        default=DEFAULT_SWEEP_INTERVAL_SECONDS,
    )
    serve_parser.add_argument(
        "--max-connections",
        type=int,
        default=DEFAULT_MAX_CONNECTIONS,
    )

    token_parser = subparsers.add_parser(
        "create-enrollment-token",
        help="create and print one bounded enrollment token exactly once",
    )
    token_parser.add_argument("--database", default=DEFAULT_DATABASE)
    token_parser.add_argument("--ttl-seconds", type=int, default=600)
    token_parser.add_argument("--max-uses", type=int, default=1)

    revoke_parser = subparsers.add_parser(
        "revoke-node", help="durably revoke an enrolled node identity"
    )
    revoke_parser.add_argument("--database", default=DEFAULT_DATABASE)
    revoke_parser.add_argument("--node-id", required=True)
    revoke_parser.add_argument("--reason", required=True)
    revoke_parser.add_argument("--request-id")

    audit_parser = subparsers.add_parser(
        "audit-status", help="print the coordinator's redacted durable audit view"
    )
    audit_parser.add_argument("--database", default=DEFAULT_DATABASE)
    audit_parser.add_argument("--node-id")
    audit_parser.add_argument("--limit", type=int, default=200)
    return parser


async def _serve_from_args(args: argparse.Namespace) -> None:
    password = (
        getpass.getpass("TLS private-key password: ")
        if args.tls_key_password_prompt
        else None
    )
    coordinator = SessionCoordinator(args.database)
    relay = RelayServer(
        coordinator,
        host=args.host,
        port=args.port,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        tls_key_password=password,
        unsafe_development_plaintext=args.unsafe_development_plaintext,
        auth_timeout_seconds=args.auth_timeout_seconds,
        send_timeout_seconds=args.send_timeout_seconds,
        heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
        sweep_interval_seconds=args.sweep_interval_seconds,
        max_connections=args.max_connections,
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, stop_requested.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed_signals.append(shutdown_signal)
    await relay.start()
    try:
        if installed_signals:
            await stop_requested.wait()
        elif relay._server is not None:
            await relay._server.serve_forever()
    finally:
        for shutdown_signal in installed_signals:
            loop.remove_signal_handler(shutdown_signal)
        await relay.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "serve":
            logging.basicConfig(
                level=logging.INFO, format="%(levelname)s %(message)s"
            )
            try:
                asyncio.run(_serve_from_args(args))
            except KeyboardInterrupt:
                return 0
            return 0

        coordinator = SessionCoordinator(args.database)
        if args.command == "create-enrollment-token":
            result = coordinator.create_enrollment_token(
                ttl_seconds=args.ttl_seconds,
                max_uses=args.max_uses,
            )
            # This is the only intentional raw-token output. The coordinator
            # stores only its digest, and the value is never sent to a logger.
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.command == "revoke-node":
            changed = coordinator.revoke_node(
                node_id=args.node_id,
                reason=args.reason,
                request_id=args.request_id
                or f"admin-revoke-{uuid.uuid4().hex}",
            )
            print(
                json.dumps(
                    {"node_id": args.node_id, "revoked": changed},
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "audit-status":
            limit = _bounded_number(
                args.limit, field="limit", minimum=1, maximum=10_000
            )
            entries = coordinator.audit_entries(
                limit=limit,
                actor_node_id=args.node_id,
            )
            visible_entries = tuple(
                {
                    key: value
                    for key, value in entry.items()
                    if key != "audit_id"
                }
                for entry in entries
            )
            print(
                json.dumps(
                    {
                        "schema": "fcp.relay.audit_status.v1",
                        "entries": _redact_secret_fields(visible_entries),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
    except (
        FederationValidationError,
        FederationOperationError,
        RelayConfigurationError,
        OSError,
        sqlite3.Error,
    ) as error:
        code = getattr(error, "code", "relay-command-failed")
        print(f"relay command failed ({code})", file=sys.stderr)
        return 2
    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

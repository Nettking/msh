"""Storage request/reply bridge over the authenticated Phase 2 relay channel."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import FederationOperationError, FederationValidationError
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    StorageError,
    StorageErrorCode,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)

RELAY_STORAGE_KIND = "fcp-storage-v1"
_LOGGER = logging.getLogger(__name__)


def _log_failure(message: str, *, extra: dict[str, Any]) -> None:
    # Default container logging formats only the message. Keep the already
    # redacted correlation fields visible without enabling all INFO.
    _LOGGER.error("%s %s", message, json.dumps(extra, sort_keys=True, separators=(",", ":"), allow_nan=False), extra=extra)


def _log_rejection(message: str, *, extra: dict[str, Any]) -> None:
    # Default container logging formats only the message. Preserve the reason
    # and redacted request correlation when a late or malformed reply is dropped.
    _LOGGER.warning("%s %s", message, json.dumps(extra, sort_keys=True, separators=(",", ":"), allow_nan=False), extra=extra)


def _diagnostic_text(value: Any, *, maximum: int = 2048) -> str | None:
    if value is None:
        return None
    if (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and all(character.isprintable() for character in value)
    ):
        # Correlation fields can contain credential-shaped data. Keep stable
        # joins in diagnostics without emitting raw identifiers.
        return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()
    return "invalid"


def _request_diagnostic_fields(envelope: StorageRequestEnvelope) -> dict[str, Any]:
    payload = envelope.payload
    authorization = envelope.authorization_context
    idempotency_key = _diagnostic_text(payload.get("idempotency_key"))
    content_hash = payload.get("content_hash")
    diagnostic_content_hash = (
        content_hash
        if isinstance(content_hash, str)
        and re.fullmatch(r"sha256:[0-9a-f]{64}", content_hash)
        else _diagnostic_text(content_hash)
    )
    return {
        "storage_operation": getattr(envelope.operation, "value", "unknown"),
        "storage_group_id": _diagnostic_text(authorization.get("group_id")),
        "storage_dataset_id": _diagnostic_text(payload.get("dataset_id")),
        "storage_batch_id": _diagnostic_text(payload.get("batch_id")),
        "storage_content_hash": diagnostic_content_hash,
        "storage_idempotency_key_sha256": (
            idempotency_key.removeprefix("sha256:")
            if isinstance(idempotency_key, str) and idempotency_key.startswith("sha256:")
            else idempotency_key
        ),
    }


def _late_response_diagnostic_fields(
    value: dict[str, Any], *, expected_request_id: str | None,
) -> dict[str, Any]:
    """Summarize a late response without accepting or logging its payload."""
    response = None
    try:
        # Keep diagnostic classification aligned with the canonical protocol
        # parser. This only describes a dropped late frame; it never accepts it.
        response = StorageResponseEnvelope.from_dict(value)
    except (TypeError, ValueError):
        pass
    response_id = value.get("request_id")
    request_id_matches = (
        None if expected_request_id is None else response_id == expected_request_id
    )
    response_shape_valid = response is not None and request_id_matches is not False
    return {
        "storage_late_response_valid": response_shape_valid,
        "storage_late_response_request_id_matches": request_id_matches,
        "storage_late_response_ok": response.ok if response_shape_valid else None,
        "storage_late_response_error_code": (
            response.error.code.value
            if response_shape_valid and response.error is not None
            else None
        ),
    }


def _response_failure_fields(
    response: StorageResponseEnvelope,
    error: Exception,
) -> dict[str, Any]:
    """Describe the failed response send without exposing its payload/message."""
    response_error = response.error
    relay_code = getattr(error, "code", None)
    safe_relay_codes = {
        "target-disconnected", "target-unavailable", "request-id-conflict",
        "unauthorized", "session-mismatch",
        "connection-closed", "connection-replaced", "heartbeat-failed", "relay-rejected",
    }
    return {
        "storage_response_ok": response.ok if type(response.ok) is bool else None,
        "storage_response_error_code": (
            response_error.code.value
            if isinstance(response_error, StorageError)
            and isinstance(response_error.code, StorageErrorCode)
            else None
        ),
        "storage_relay_request_id": _diagnostic_text(f"relay-response-{response.request_id}"),
        "storage_relay_error_code": (
            relay_code if isinstance(relay_code, str) and relay_code in safe_relay_codes else None
        ),
        "storage_relay_error_code_sha256": _diagnostic_text(relay_code),
    }


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


class AsyncStorageService(Protocol):
    async def dispatch(self, envelope: StorageRequestEnvelope) -> StorageResponseEnvelope: ...


@dataclass(frozen=True)
class _PendingRequest:
    future: asyncio.Future[StorageResponseEnvelope]
    session_id: str
    target_node_id: str
    provider_id: str
    diagnostic_fields: dict[str, Any]


class RelayStorageEndpoint:
    """Own one logical application-relay stage for storage traffic.

    By default the endpoint reads directly from a node client's application queue.
    ``message_source`` composes it behind an existing single-reader multiplexer so
    several product protocols can share one authenticated node connection without
    racing to consume ``relay.message`` frames. Unrelated messages are retained in
    a bounded queue for the next downstream consumer.
    """

    def __init__(
        self,
        relay_client: RelayMessageClient,
        services: Mapping[str, AsyncStorageService] | None = None,
        *,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
        message_source: RelayMessageSource | None = None,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.message_source = message_source
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
        self.check_reader()
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-storage-relay-{self.relay_client.node_id}",
            )

    def check_reader(self) -> None:
        """Let the owning authority recover an unexpectedly ended stage.

        A completed task is still installed until close(). Reusing it would
        send new work that cannot receive its response. Do not start another
        reader here: the authority's bounded supervisor owns reconstruction.
        """
        task = self._reader_task
        if self._closed or task is None or not task.done():
            return
        error = asyncio.CancelledError() if task.cancelled() else task.exception()
        raise FederationOperationError(
            "storage-relay-reader-stopped",
            "the storage relay reader ended before the authority stopped",
            "connection",
        ) from error

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
        diagnostic_fields = _request_diagnostic_fields(envelope)
        self._pending[envelope.request_id] = _PendingRequest(
            future,
            envelope.session_id,
            target_node_id,
            provider_id,
            diagnostic_fields,
        )
        try:
            delivery_started = time.monotonic()
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
            except Exception as exc:
                _log_failure("storage request relay delivery failed", extra={
                    "storage_stage": "request_delivery_failed",
                    "storage_request_id": _diagnostic_text(envelope.request_id),
                    "storage_session_id": _diagnostic_text(envelope.session_id),
                    "storage_target_node_id": _diagnostic_text(target_node_id),
                    "storage_provider_id": _diagnostic_text(provider_id),
                    "storage_delivery_elapsed_seconds": round(time.monotonic() - delivery_started, 6),
                    "storage_exception_type": type(exc).__name__,
                    **diagnostic_fields,
                })
                raise
            delivery_elapsed = time.monotonic() - delivery_started
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                _log_failure("storage request delivery not confirmed", extra={
                    "storage_stage": "request_delivery", "storage_request_id": _diagnostic_text(envelope.request_id),
                    "storage_session_id": _diagnostic_text(envelope.session_id), "storage_target_node_id": _diagnostic_text(target_node_id),
                    "storage_provider_id": _diagnostic_text(provider_id), "storage_delivery_confirmed": False,
                    "storage_delivery_elapsed_seconds": round(delivery_elapsed, 6),
                    **diagnostic_fields})
                raise FederationValidationError(
                    "storage-route-failed", "target_node_id", "relay did not confirm delivery"
                )
            started = time.monotonic()
            _LOGGER.info("storage request delivery confirmed", extra={
                "storage_stage": "request_delivery", "storage_request_id": _diagnostic_text(envelope.request_id),
                "storage_session_id": _diagnostic_text(envelope.session_id), "storage_target_node_id": _diagnostic_text(target_node_id),
                "storage_provider_id": _diagnostic_text(provider_id), "storage_delivery_confirmed": True,
                "storage_delivery_elapsed_seconds": round(delivery_elapsed, 6),
                **diagnostic_fields})
            try:
                response = await asyncio.wait_for(future, timeout=self.request_timeout)
            except asyncio.TimeoutError:
                _log_failure("storage response wait timed out", extra={
                    "storage_stage": "response_wait", "storage_request_id": _diagnostic_text(envelope.request_id),
                    "storage_session_id": _diagnostic_text(envelope.session_id), "storage_target_node_id": _diagnostic_text(target_node_id),
                    "storage_provider_id": _diagnostic_text(provider_id), "storage_elapsed_seconds": round(time.monotonic()-started, 6),
                    "storage_delivery_elapsed_seconds": round(delivery_elapsed, 6),
                    **diagnostic_fields})
                raise
            _LOGGER.info("storage response accepted", extra={
                "storage_stage": "response_accepted", "storage_request_id": _diagnostic_text(envelope.request_id),
                "storage_session_id": _diagnostic_text(envelope.session_id), "storage_target_node_id": _diagnostic_text(target_node_id),
                "storage_provider_id": _diagnostic_text(provider_id), "storage_response_ok": response.ok,
                "storage_delivery_elapsed_seconds": round(delivery_elapsed, 6),
                "storage_response_wait_elapsed_seconds": round(time.monotonic()-started, 6),
                **diagnostic_fields})
            return response
        finally:
            self._pending.pop(envelope.request_id, None)

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._other_messages.get()
        return await asyncio.wait_for(self._other_messages.get(), timeout=timeout)

    async def _receive(self):
        if self.message_source is not None:
            return await self.message_source.receive_other()
        return await self.relay_client.receive_message()

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                message = await self._receive()
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
            if not self._closed:
                error = FederationOperationError(
                    "storage-relay-reader-cancelled",
                    "the storage relay reader was interrupted",
                    "connection",
                )
                for pending in tuple(self._pending.values()):
                    if not pending.future.done():
                        pending.future.set_exception(error)
                _log_failure("storage relay reader interrupted", extra={
                    "storage_stage": "storage_reader_cancelled", "storage_pending_requests": len(self._pending)})
            raise
        except Exception as exc:
            for pending in tuple(self._pending.values()):
                if not pending.future.done():
                    pending.future.set_exception(exc)
            _log_failure("storage relay reader failed", extra={
                "storage_stage": "storage_reader_failed", "storage_exception_type": type(exc).__name__,
                "storage_pending_requests": len(self._pending)})
            raise

    def _finish_handler(self, task: asyncio.Task[None]) -> None:
        self._handler_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _accept_response(
        self,
        relay_message: Any,
        payload: dict[str, Any],
    ) -> None:
        relay_request_id = getattr(relay_message, "request_id", None)
        relay_request_context = {
            "storage_relay_request_id": _diagnostic_text(relay_request_id)
        }
        frame = payload.get("frame")
        if not isinstance(frame, str):
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "missing_frame",
                    **relay_request_context,
                },
            )
            return
        try:
            response_value = json.loads(frame)
        except json.JSONDecodeError:
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "invalid_json",
                    **relay_request_context,
                },
            )
            return
        if not isinstance(response_value, dict):
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "invalid_object",
                    **relay_request_context,
                },
            )
            return
        request_id = response_value.get("request_id")
        if not isinstance(request_id, str):
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "missing_request_id",
                    **relay_request_context,
                },
            )
            return
        pending = self._pending.get(request_id)
        diagnostic_fields = pending.diagnostic_fields if pending is not None else {}
        actor = getattr(relay_message, "actor_node_id", None)
        session = getattr(relay_message, "session_id", None)
        response_provider_id = payload.get("provider_id")
        response_context = {
            "storage_request_id": _diagnostic_text(request_id),
            **relay_request_context,
            "storage_actor_node_id": _diagnostic_text(actor),
            "storage_session_id": _diagnostic_text(session),
            "storage_provider_id": _diagnostic_text(response_provider_id),
        }
        if pending is None:
            response_request_prefix = "relay-response-"
            expected_late_request_id = (
                relay_request_id.removeprefix(response_request_prefix)
                if isinstance(relay_request_id, str)
                and relay_request_id.startswith(response_request_prefix)
                else None
            )
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "request_not_pending",
                    **response_context,
                    **_late_response_diagnostic_fields(
                        response_value, expected_request_id=expected_late_request_id,
                    ),
                },
            )
            return
        if (session != pending.session_id or actor != pending.target_node_id
                or response_provider_id != pending.provider_id):
            reason = (
                "provider_mismatch"
                if response_provider_id != pending.provider_id
                else "route_mismatch"
            )
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": reason,
                    **response_context,
                    **diagnostic_fields,
                },
            )
            return
        try:
            response = StorageResponseEnvelope.from_dict(response_value)
        except FederationValidationError as exc:
            _log_rejection(
                "storage response rejected",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "invalid_envelope",
                    "storage_validation_code": exc.code,
                    "storage_validation_field": exc.field,
                    **response_context,
                    **diagnostic_fields,
                },
            )
            return
        if not pending.future.done():
            pending.future.set_result(response)
            _LOGGER.info(
                "storage response matched pending request",
                extra={
                    "storage_stage": "response_accepted",
                    **response_context,
                    "storage_response_ok": response.ok,
                    **diagnostic_fields,
                },
            )
        else:
            _log_rejection(
                "storage response arrived after request completed",
                extra={
                    "storage_stage": "response_rejected",
                    "storage_rejection_reason": "future_already_done",
                    **response_context,
                    **diagnostic_fields,
                },
            )

    async def _handle_request(self, relay_message: Any, payload: dict[str, Any]) -> None:
        frame = payload.get("frame")
        provider_id = payload.get("provider_id")
        if not isinstance(frame, str) or not isinstance(provider_id, str):
            return
        diagnostic_fields: dict[str, Any] = {}
        try:
            request_value = json.loads(frame)
            request = StorageRequestEnvelope.from_dict(request_value)
            diagnostic_fields = _request_diagnostic_fields(request)
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
                started = time.monotonic()
                try:
                    response = await service.dispatch(request)
                except TimeoutError as exc:
                    _log_failure("storage provider dispatch timed out", extra={
                        "storage_stage": "provider_dispatch_timeout",
                        "storage_request_id": _diagnostic_text(request.request_id),
                        "storage_session_id": _diagnostic_text(request.session_id),
                        "storage_actor_node_id": _diagnostic_text(
                            getattr(relay_message, "actor_node_id", None)
                        ),
                        "storage_provider_id": _diagnostic_text(provider_id),
                        "storage_elapsed_seconds": round(time.monotonic() - started, 6),
                        "storage_exception_type": type(exc).__name__,
                        **diagnostic_fields,
                    })
                    raise
                dispatch_elapsed = time.monotonic() - started
                dispatch_fields = {
                    "storage_stage": "provider_dispatch_complete",
                    "storage_request_id": _diagnostic_text(request.request_id),
                    "storage_session_id": _diagnostic_text(request.session_id),
                    "storage_actor_node_id": _diagnostic_text(
                        getattr(relay_message, "actor_node_id", None)
                    ),
                    "storage_provider_id": _diagnostic_text(provider_id),
                    "storage_response_ok": response.ok,
                    "storage_elapsed_seconds": round(dispatch_elapsed, 6),
                    **diagnostic_fields,
                }
                if dispatch_elapsed >= min(10.0, self.request_timeout * (2 / 3)):
                    _LOGGER.warning(
                        "storage provider dispatch was slow %s",
                        json.dumps(dispatch_fields, sort_keys=True, separators=(",", ":"), allow_nan=False),
                        extra=dispatch_fields,
                    )
                _LOGGER.info("storage provider dispatch completed", extra=dispatch_fields)
        except (FederationValidationError, json.JSONDecodeError) as exc:
            raw_request_id = (
                request_value.get("request_id")
                if isinstance(locals().get("request_value"), dict)
                else None
            )
            request_id = (
                raw_request_id
                if isinstance(raw_request_id, str)
                and raw_request_id.strip()
                and all(ord(character) >= 32 for character in raw_request_id)
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
        delivery_started = time.monotonic()
        try:
            delivery = await self.relay_client.send_message(
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
        except asyncio.CancelledError:
            _LOGGER.info("storage response relay delivery cancelled", extra={
                "storage_stage": "response_delivery_cancelled",
                "storage_request_id": _diagnostic_text(response.request_id),
                "storage_session_id": _diagnostic_text(session_id),
                "storage_target_node_id": _diagnostic_text(target_node_id),
                "storage_provider_id": _diagnostic_text(provider_id),
                "storage_elapsed_seconds": round(time.monotonic() - delivery_started, 6),
                **diagnostic_fields,
            })
            raise
        except Exception as exc:
            _log_failure("storage response relay delivery failed", extra={
                "storage_stage": "response_delivery_failed",
                "storage_request_id": _diagnostic_text(response.request_id),
                "storage_session_id": _diagnostic_text(session_id),
                "storage_target_node_id": _diagnostic_text(target_node_id),
                "storage_provider_id": _diagnostic_text(provider_id),
                "storage_elapsed_seconds": round(time.monotonic() - delivery_started, 6),
                "storage_exception_type": type(exc).__name__,
                **_response_failure_fields(response, exc),
                **diagnostic_fields,
            })
            raise
        delivery_elapsed = time.monotonic() - delivery_started
        delivery_fields = {
            "storage_stage": "response_delivery",
            "storage_request_id": _diagnostic_text(response.request_id),
            "storage_session_id": _diagnostic_text(session_id),
            "storage_target_node_id": _diagnostic_text(target_node_id),
            "storage_provider_id": _diagnostic_text(provider_id),
            "storage_delivery_confirmed": isinstance(delivery, dict) and delivery.get("delivered") is True,
            "storage_elapsed_seconds": round(delivery_elapsed, 6),
            **diagnostic_fields,
        }
        if delivery_elapsed >= min(10.0, self.request_timeout * (2 / 3)):
            _LOGGER.warning(
                "storage response relay delivery was slow %s",
                json.dumps(delivery_fields, sort_keys=True, separators=(",", ":"), allow_nan=False),
                extra=delivery_fields,
            )
        _LOGGER.info("storage response relay delivery result", extra=delivery_fields)

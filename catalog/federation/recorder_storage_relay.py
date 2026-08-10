"""Durable recorder publication through the Federation logical-storage authority.

Recorder nodes never address a physical storage provider directly.  A recorder
sends one bounded, idempotent batch request to the session's storage-control
authority.  That authority executes the existing coordinator-side
:class:`PhaseDLogicalStorageClient`, so assignment, grants, fencing, replication,
and manifest commit remain owned by the existing storage control plane.

The application messages use ``relay.message`` and are deliberately kept below
the relay command-payload bound.  Pairing/session credentials are never embedded
in the payload.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from .errors import FederationValidationError
from .live_failover import (
    STORAGE_CONTROL_REFRESH_MESSAGE,
    STORAGE_CONTROL_RELAY_KIND,
    STORAGE_FAILOVER_RELAY_KIND,
    StorageControlRelayChannel,
)
from .phase_d_client import PhaseDIngestOutcome
from .storage_protocol import BatchIngestResult

RECORDER_LOGICAL_STORAGE_KIND = "fcp-recorder-logical-storage-v1"
STORAGE_CONTROL_CAPABILITY_TYPE = "storage-control"
STORAGE_CONTROL_CAPABILITY_PROTOCOL = "fcp.storage-control"
STORAGE_CONTROL_CAPABILITY_VERSION = "1"
RECORDER_RELAY_MAX_PAYLOAD_BYTES = 46_000
RECORDER_RELAY_SAFE_CONTENT_BYTES = 30_000


def _text(value: Any, field: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-recorder-storage-field",
            field,
            f"must be non-empty printable text no longer than {maximum} bytes",
        )
    return value.strip()


def _positive(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise FederationValidationError(
            "invalid-recorder-storage-field",
            field,
            "must be a positive integer",
        )
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-recorder-storage-field",
                field,
                "must be RFC 3339 text",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-recorder-storage-field",
            field,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _bounded_payload(value: dict[str, Any]) -> None:
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
            "invalid-recorder-storage-payload",
            "payload",
            "must contain JSON-compatible values",
        ) from exc
    if len(encoded) > RECORDER_RELAY_MAX_PAYLOAD_BYTES:
        raise FederationValidationError(
            "recorder-storage-payload-too-large",
            "payload",
            "recorder publication exceeds the bounded relay application payload",
        )


def _outcome_dict(outcome: PhaseDIngestOutcome) -> dict[str, Any]:
    return {
        "committed": bool(outcome.committed),
        "result": None if outcome.result is None else outcome.result.to_dict(),
        "retryable": bool(outcome.retryable),
        "error_code": outcome.error_code,
        "message": outcome.message,
    }


def _outcome_from_dict(value: Any) -> PhaseDIngestOutcome:
    if not isinstance(value, dict):
        raise FederationValidationError(
            "invalid-recorder-storage-response",
            "outcome",
            "must be an object",
        )
    committed = value.get("committed")
    retryable = value.get("retryable", False)
    if not isinstance(committed, bool) or not isinstance(retryable, bool):
        raise FederationValidationError(
            "invalid-recorder-storage-response",
            "outcome",
            "committed and retryable must be boolean",
        )
    result_value = value.get("result")
    result = None
    if result_value is not None:
        if not isinstance(result_value, dict):
            raise FederationValidationError(
                "invalid-recorder-storage-response",
                "outcome.result",
                "must be an object",
            )
        result = BatchIngestResult.from_dict(result_value)
    error_code = value.get("error_code")
    message = value.get("message")
    if error_code is not None and not isinstance(error_code, str):
        raise FederationValidationError(
            "invalid-recorder-storage-response",
            "outcome.error_code",
            "must be text or null",
        )
    if message is not None and not isinstance(message, str):
        raise FederationValidationError(
            "invalid-recorder-storage-response",
            "outcome.message",
            "must be text or null",
        )
    return PhaseDIngestOutcome(
        committed=committed,
        result=result,
        retryable=retryable,
        error_code=error_code,
        message=message,
    )


class RecorderAwareStorageControlRelayChannel(StorageControlRelayChannel):
    """Extend the existing storage-authority demultiplexer with recorder ingress."""

    def __init__(self, client: Any, endpoint: Any, *, timeout: float = 15.0) -> None:
        super().__init__(client, endpoint, timeout=timeout)
        self._recorder_handler: (
            Callable[[str, str, dict[str, Any]], Awaitable[None]] | None
        ) = None
        self._recorder_tasks: set[asyncio.Task[None]] = set()

    def set_recorder_ingest_handler(
        self,
        handler: Callable[[str, str, dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._recorder_handler = handler

    async def close(self) -> None:
        for task in tuple(self._recorder_tasks):
            task.cancel()
        if self._recorder_tasks:
            await asyncio.gather(*self._recorder_tasks, return_exceptions=True)
        self._recorder_tasks.clear()
        await super().close()

    async def _receiver_loop(self) -> None:
        while True:
            message = await self.endpoint.receive_other()
            payload = getattr(message, "payload", None)
            if not isinstance(payload, dict):
                continue
            kind = payload.get("kind")
            message_kind = payload.get("message")
            if kind == STORAGE_CONTROL_RELAY_KIND:
                if message_kind == "response":
                    self._accept_response(message, payload)
                elif message_kind == STORAGE_CONTROL_REFRESH_MESSAGE:
                    self._accept_refresh(message, payload)
                continue
            if (
                kind == STORAGE_FAILOVER_RELAY_KIND
                and message_kind == "report-response"
            ):
                self._accept_report(message, payload)
                continue
            if (
                kind == RECORDER_LOGICAL_STORAGE_KIND
                and message_kind == "request"
            ):
                self._accept_recorder_request(message, payload)

    def _accept_recorder_request(
        self,
        message: Any,
        payload: dict[str, Any],
    ) -> None:
        handler = self._recorder_handler
        actor = getattr(message, "actor_node_id", None)
        session_id = getattr(message, "session_id", None)
        if (
            handler is None
            or not isinstance(actor, str)
            or not isinstance(session_id, str)
        ):
            return
        task = asyncio.create_task(handler(actor, session_id, payload))
        self._recorder_tasks.add(task)
        task.add_done_callback(self._recorder_tasks.discard)


class RecorderLogicalStorageAuthority:
    """Accept recorder batches and commit them through coordinator-owned storage."""

    def __init__(
        self,
        *,
        client: Any,
        logical_client: Any,
        session_id: str,
    ) -> None:
        self.client = client
        self.logical_client = logical_client
        self.session_id = _text(session_id, "session_id")

    async def handle_request(
        self,
        actor_node_id: str,
        session_id: str,
        payload: dict[str, Any],
    ) -> None:
        correlation_id = payload.get("correlation_id")
        response: dict[str, Any]
        try:
            actor_node_id = _text(actor_node_id, "actor_node_id")
            if session_id != self.session_id:
                raise FederationValidationError(
                    "recorder-storage-session-mismatch",
                    "session_id",
                    "recorder request belongs to another Federation session",
                )
            correlation_id = _text(correlation_id, "correlation_id")
            group_id = _text(payload.get("group_id"), "group_id")
            dataset_id = _text(payload.get("dataset_id"), "dataset_id")
            batch_id = _text(payload.get("batch_id"), "batch_id")
            idempotency_key = _text(
                payload.get("idempotency_key"),
                "idempotency_key",
                maximum=2048,
            )
            schema_name = _text(
                payload.get("dataset_schema_name"),
                "dataset_schema_name",
            )
            schema_version = _positive(
                payload.get("dataset_schema_version"),
                "dataset_schema_version",
            )
            created_at = _utc(payload.get("created_at"), "created_at")
            if "content" not in payload:
                raise FederationValidationError(
                    "invalid-recorder-storage-field",
                    "content",
                    "is required",
                )
            _bounded_payload(payload)
            outcome = await self.logical_client.ingest_batch(
                group_id=group_id,
                dataset_id=dataset_id,
                batch_id=batch_id,
                idempotency_key=idempotency_key,
                content=payload["content"],
                created_at=created_at,
                dataset_schema_name=schema_name,
                dataset_schema_version=schema_version,
            )
            response = {
                "kind": RECORDER_LOGICAL_STORAGE_KIND,
                "message": "response",
                "correlation_id": correlation_id,
                "status": "accepted",
                "outcome": _outcome_dict(outcome),
            }
        except FederationValidationError as exc:
            response = {
                "kind": RECORDER_LOGICAL_STORAGE_KIND,
                "message": "response",
                "correlation_id": (
                    correlation_id if isinstance(correlation_id, str) else "invalid"
                ),
                "status": "rejected",
                "error": {
                    "code": exc.code,
                    "field": exc.field,
                    "message": exc.message,
                },
            }
        await self.client.send_message(
            session_id=self.session_id,
            target_node_id=actor_node_id,
            request_id=f"recorder-storage-response-{uuid.uuid4().hex}",
            payload=response,
        )


class RelayRecorderStorageClient:
    """Recorder-facing logical-storage client over one authenticated relay connection."""

    def __init__(
        self,
        relay_client: Any,
        *,
        session_id: str,
        authority_node_id: str,
        request_timeout: float = 15.0,
        other_message_limit: int = 64,
    ) -> None:
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.relay_client = relay_client
        self.session_id = _text(session_id, "session_id")
        self.authority_node_id = _text(
            authority_node_id,
            "authority_node_id",
        )
        self.request_timeout = float(request_timeout)
        self._pending: dict[str, asyncio.Future[PhaseDIngestOutcome]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._other: asyncio.Queue[Any] = asyncio.Queue(maxsize=other_message_limit)
        self._closed = False

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("recorder storage client is closed")
        if self._reader_task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(),
                name=f"fcp-recorder-storage-{self.relay_client.node_id}",
            )

    async def close(self) -> None:
        self._closed = True
        task, self._reader_task = self._reader_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        error = RuntimeError("recorder storage client closed")
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def ingest_batch(
        self,
        *,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at: datetime,
        dataset_schema_name: str = "fcp.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ) -> PhaseDIngestOutcome:
        await self.start()
        correlation_id = f"recorder-storage-{uuid.uuid4().hex}"
        payload = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "correlation_id": correlation_id,
            "group_id": _text(group_id, "group_id"),
            "dataset_id": _text(dataset_id, "dataset_id"),
            "batch_id": _text(batch_id, "batch_id"),
            "idempotency_key": _text(
                idempotency_key,
                "idempotency_key",
                maximum=2048,
            ),
            "dataset_schema_name": _text(
                dataset_schema_name,
                "dataset_schema_name",
            ),
            "dataset_schema_version": _positive(
                dataset_schema_version,
                "dataset_schema_version",
            ),
            "created_at": _utc(created_at, "created_at").isoformat(),
            "content": content,
        }
        _bounded_payload(payload)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[PhaseDIngestOutcome] = loop.create_future()
        self._pending[correlation_id] = future
        try:
            delivery = await self.relay_client.send_message(
                session_id=self.session_id,
                target_node_id=self.authority_node_id,
                request_id=f"recorder-storage-route-{uuid.uuid4().hex}",
                payload=payload,
            )
            if (
                not isinstance(delivery, dict)
                or delivery.get("delivered") is not True
            ):
                raise FederationValidationError(
                    "recorder-storage-route-failed",
                    "authority_node_id",
                    "relay did not confirm delivery to the logical-storage authority",
                )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(correlation_id, None)

    async def receive_other(self, *, timeout: float | None = None) -> Any:
        if timeout is None:
            return await self._other.get()
        return await asyncio.wait_for(self._other.get(), timeout=timeout)

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                message = await self.relay_client.receive_message()
                payload = getattr(message, "payload", None)
                if (
                    isinstance(payload, dict)
                    and payload.get("kind") == RECORDER_LOGICAL_STORAGE_KIND
                    and payload.get("message") == "response"
                ):
                    self._accept_response(message, payload)
                    continue
                try:
                    self._other.put_nowait(message)
                except asyncio.QueueFull:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - shared transport retry boundary
            for future in tuple(self._pending.values()):
                if not future.done():
                    future.set_exception(exc)

    def _accept_response(self, message: Any, payload: dict[str, Any]) -> None:
        correlation_id = payload.get("correlation_id")
        if not isinstance(correlation_id, str):
            return
        future = self._pending.get(correlation_id)
        if future is None or future.done():
            return
        if (
            getattr(message, "actor_node_id", None) != self.authority_node_id
            or getattr(message, "session_id", None) != self.session_id
        ):
            return
        if payload.get("status") == "rejected":
            error = payload.get("error")
            if isinstance(error, dict):
                future.set_exception(
                    FederationValidationError(
                        str(error.get("code") or "recorder-storage-rejected"),
                        str(error.get("field") or "storage"),
                        str(error.get("message") or "logical storage rejected the batch"),
                    )
                )
            else:
                future.set_exception(
                    FederationValidationError(
                        "recorder-storage-rejected",
                        "storage",
                        "logical storage rejected the batch",
                    )
                )
            return
        if payload.get("status") != "accepted":
            return
        try:
            outcome = _outcome_from_dict(payload.get("outcome"))
        except FederationValidationError as exc:
            future.set_exception(exc)
            return
        future.set_result(outcome)


__all__ = [
    "RECORDER_LOGICAL_STORAGE_KIND",
    "RECORDER_RELAY_MAX_PAYLOAD_BYTES",
    "RECORDER_RELAY_SAFE_CONTENT_BYTES",
    "RecorderAwareStorageControlRelayChannel",
    "RecorderLogicalStorageAuthority",
    "RelayRecorderStorageClient",
    "STORAGE_CONTROL_CAPABILITY_PROTOCOL",
    "STORAGE_CONTROL_CAPABILITY_TYPE",
    "STORAGE_CONTROL_CAPABILITY_VERSION",
]

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
import re
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from .errors import FederationOperationError, FederationValidationError
from .live_failover import (
    STORAGE_CONTROL_REFRESH_MESSAGE,
    STORAGE_CONTROL_RELAY_KIND,
    STORAGE_FAILOVER_RELAY_KIND,
    StorageControlRelayChannel,
)
from .phase_d_client import PhaseDIngestOutcome
from .storage_catalog import (
    CommittedBatchDeltaPage,
    CommittedBatchPage,
    CommittedBatchReference,
)
from .storage_protocol import (
    BatchIngestRequest,
    BatchIngestResult,
    StorageOperation,
)

RECORDER_LOGICAL_STORAGE_KIND = "fcp-recorder-logical-storage-v1"
STORAGE_CONTROL_CAPABILITY_TYPE = "storage-control"
STORAGE_CONTROL_CAPABILITY_PROTOCOL = "fcp.storage-control"
STORAGE_CONTROL_CAPABILITY_VERSION = "1"
RECORDER_RELAY_MAX_PAYLOAD_BYTES = 46_000
RECORDER_RELAY_SAFE_CONTENT_BYTES = 30_000
RECORDER_CATALOG_MAX_PAGE_SIZE = 20
RECORDER_DATASET_SCHEMA_NAME = "fcp.mtconnect.observations"
RECORDER_DATASET_SCHEMA_VERSION = 1
RECORDER_CONTENT_SCHEMA = "fcp.mtconnect.observations.v1"
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")
_SOURCE_SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


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


def _non_negative(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-recorder-storage-field",
            field,
            "must be a non-negative integer",
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


def _validate_recorder_ingest(
    *,
    actor_node_id: str,
    session_id: str,
    group_id: str,
    dataset_id: str,
    batch_id: str,
    idempotency_key: str,
    schema_name: str,
    schema_version: int,
    content: Any,
) -> None:
    """Bind recorder telemetry to the authenticated producer and immutable IDs."""

    del group_id  # The selected logical group remains independently authorized.
    prefix = f"mtconnect:{actor_node_id}:"
    if not dataset_id.startswith(prefix):
        raise FederationValidationError(
            "recorder-dataset-actor-mismatch",
            "dataset_id",
            "recorder dataset namespace must match the authenticated producer",
        )
    source_slug = dataset_id.removeprefix(prefix)
    if _SOURCE_SLUG.fullmatch(source_slug) is None:
        raise FederationValidationError(
            "invalid-recorder-dataset",
            "dataset_id",
            "recorder dataset source must be a bounded portable slug",
        )
    if (
        schema_name != RECORDER_DATASET_SCHEMA_NAME
        or schema_version != RECORDER_DATASET_SCHEMA_VERSION
    ):
        raise FederationValidationError(
            "unsupported-recorder-dataset-schema",
            "dataset_schema_name",
            "recorder ingress accepts only the MTConnect observations schema v1",
        )
    if (
        not isinstance(content, dict)
        or content.get("schema") != RECORDER_CONTENT_SCHEMA
    ):
        raise FederationValidationError(
            "unsupported-recorder-content-schema",
            "content.schema",
            "recorder content must use fcp.mtconnect.observations.v1",
        )
    source_name = content.get("source_name")
    if not isinstance(source_name, str) or not source_name.strip():
        raise FederationValidationError(
            "invalid-recorder-content",
            "content.source_name",
            "source_name must be non-empty text",
        )
    normalized_source = (
        re.sub(r"[^A-Za-z0-9._-]+", "-", source_name.strip()).strip("-._") or "unknown"
    )
    if normalized_source != source_slug:
        raise FederationValidationError(
            "recorder-source-dataset-mismatch",
            "content.source_name",
            "source identity does not match the authenticated dataset namespace",
        )
    observations = content.get("observations")
    count = content.get("observation_count")
    if (
        not isinstance(observations, list)
        or not observations
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count != len(observations)
    ):
        raise FederationValidationError(
            "invalid-recorder-content",
            "content.observations",
            "observations must be a non-empty list matching observation_count",
        )
    sequences: list[int] = []
    for index, observation in enumerate(observations):
        if not isinstance(observation, dict):
            raise FederationValidationError(
                "invalid-recorder-content",
                f"content.observations[{index}]",
                "each observation must be an object",
            )
        sequence = observation.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise FederationValidationError(
                "invalid-recorder-content",
                f"content.observations[{index}].sequence",
                "sequence must be a non-negative integer",
            )
        sequences.append(sequence)
    if sequences != list(range(sequences[0], sequences[-1] + 1)):
        raise FederationValidationError(
            "invalid-recorder-sequence-range",
            "content.observations",
            "observation sequences must be contiguous and strictly increasing",
        )
    if (
        content.get("first_sequence") != sequences[0]
        or content.get("last_sequence") != sequences[-1]
    ):
        raise FederationValidationError(
            "invalid-recorder-sequence-range",
            "content.first_sequence",
            "declared sequence bounds must match the observations",
        )
    instance_id = content.get("agent_instance_id")
    if (
        isinstance(instance_id, bool)
        or not isinstance(instance_id, int)
        or instance_id < 0
    ):
        raise FederationValidationError(
            "invalid-recorder-content",
            "content.agent_instance_id",
            "agent instance ID must be a non-negative integer",
        )
    raw_hash = content.get("raw_sha256")
    if not isinstance(raw_hash, str) or _SHA256.fullmatch(raw_hash) is None:
        raise FederationValidationError(
            "invalid-recorder-content",
            "content.raw_sha256",
            "raw_sha256 must be a lowercase SHA-256 digest",
        )
    expected_batch_id = (
        f"{source_slug}:{instance_id}:{sequences[0]}:{sequences[-1]}:{raw_hash[7:]}"
    )
    if batch_id != expected_batch_id:
        raise FederationValidationError(
            "recorder-batch-identity-mismatch",
            "batch_id",
            "batch ID does not match the validated recorder content",
        )
    expected_idempotency_key = f"{session_id}:{dataset_id}:{batch_id}"
    if idempotency_key != expected_idempotency_key:
        raise FederationValidationError(
            "recorder-idempotency-mismatch",
            "idempotency_key",
            "idempotency key does not match the authenticated immutable batch",
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
            if kind == RECORDER_LOGICAL_STORAGE_KIND and message_kind == "request":
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
        authorize_ingest: Callable[[str, str], None] | None = None,
        authorize_read: (Callable[[str, str, str, str | None], None] | None) = None,
        read_requests_per_minute: int = 240,
    ) -> None:
        self.client = client
        self.logical_client = logical_client
        self.session_id = _text(session_id, "session_id")
        self.authorize_ingest = authorize_ingest
        self.authorize_read = authorize_read
        if (
            isinstance(read_requests_per_minute, bool)
            or not isinstance(read_requests_per_minute, int)
            or read_requests_per_minute < 1
        ):
            raise ValueError("read_requests_per_minute must be a positive integer")
        self.read_requests_per_minute = read_requests_per_minute
        self._read_windows: dict[str, deque[float]] = {}

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
            _bounded_payload(payload)
            operation = payload.get(
                "operation",
                StorageOperation.BATCH_INGEST.value,
            )
            if operation == StorageOperation.BATCH_LIST.value:
                response = self._list_response(
                    actor_node_id,
                    session_id,
                    correlation_id,
                    payload,
                )
            elif operation == StorageOperation.BATCH_READ.value:
                response = await self._read_response(
                    actor_node_id,
                    session_id,
                    correlation_id,
                    payload,
                )
            elif operation == StorageOperation.BATCH_INGEST.value:
                response = await self._ingest_response(
                    actor_node_id,
                    session_id,
                    correlation_id,
                    payload,
                )
            else:
                raise FederationValidationError(
                    "unsupported-recorder-storage-operation",
                    "operation",
                    "logical storage supports only batch ingest, list, and read",
                )
            _bounded_payload(response)
        except (FederationValidationError, FederationOperationError) as exc:
            response = {
                "kind": RECORDER_LOGICAL_STORAGE_KIND,
                "message": "response",
                "correlation_id": (
                    correlation_id if isinstance(correlation_id, str) else "invalid"
                ),
                "status": "rejected",
                "error": {
                    "code": exc.code,
                    "field": exc.field or "storage",
                    "message": exc.message,
                },
            }
        await self.client.send_message(
            session_id=self.session_id,
            target_node_id=actor_node_id,
            request_id=f"recorder-storage-response-{uuid.uuid4().hex}",
            payload=response,
        )

    async def _ingest_response(
        self,
        actor_node_id: str,
        session_id: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
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
        if self.authorize_ingest is not None:
            self.authorize_ingest(actor_node_id, session_id)
        _validate_recorder_ingest(
            actor_node_id=actor_node_id,
            session_id=session_id,
            group_id=group_id,
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            schema_name=schema_name,
            schema_version=schema_version,
            content=payload["content"],
        )
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
        return {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "response",
            "correlation_id": correlation_id,
            "status": "accepted",
            "operation": StorageOperation.BATCH_INGEST.value,
            "outcome": _outcome_dict(outcome),
        }

    def _list_response(
        self,
        actor_node_id: str,
        session_id: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        group_id = _text(payload.get("group_id"), "group_id")
        dataset_value = payload.get("dataset_id")
        dataset_id = (
            None if dataset_value is None else _text(dataset_value, "dataset_id")
        )
        limit = _positive(payload.get("limit", 20), "limit")
        if limit > RECORDER_CATALOG_MAX_PAGE_SIZE:
            raise FederationValidationError(
                "recorder-catalog-page-too-large",
                "limit",
                f"must not exceed {RECORDER_CATALOG_MAX_PAGE_SIZE}",
            )
        cursor_value = payload.get("cursor")
        cursor = (
            None
            if cursor_value is None
            else _text(cursor_value, "cursor", maximum=8192)
        )
        self._authorize_read_access(
            actor_node_id,
            session_id,
            group_id,
            dataset_id,
        )
        listing_mode = payload.get("listing_mode", "full")
        if listing_mode == "full":
            if (
                "baseline_manifest_revision" in payload
                or "baseline_manifest_hash" in payload
            ):
                raise FederationValidationError(
                    "manifest-baseline-mode-required",
                    "listing_mode",
                    "manifest baseline fields require delta listing mode",
                )
            page = self.logical_client.list_committed_batches(
                group_id=group_id,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
            if not isinstance(page, CommittedBatchPage):
                raise FederationValidationError(
                    "invalid-storage-response",
                    "page",
                    "logical storage returned an invalid committed batch page",
                )
        elif listing_mode == "delta":
            if "baseline_manifest_revision" not in payload:
                raise FederationValidationError(
                    "missing-field",
                    "baseline_manifest_revision",
                    "is required for a manifest delta listing",
                )
            if "baseline_manifest_hash" not in payload:
                raise FederationValidationError(
                    "missing-field",
                    "baseline_manifest_hash",
                    "is required for a manifest delta listing",
                )
            baseline_revision = _non_negative(
                payload.get("baseline_manifest_revision"),
                "baseline_manifest_revision",
            )
            baseline_hash = _text(
                payload.get("baseline_manifest_hash"),
                "baseline_manifest_hash",
                maximum=71,
            )
            page = self.logical_client.list_committed_batch_delta(
                group_id=group_id,
                baseline_manifest_revision=baseline_revision,
                baseline_manifest_hash=baseline_hash,
                dataset_id=dataset_id,
                limit=limit,
                cursor=cursor,
            )
            if not isinstance(page, CommittedBatchDeltaPage):
                raise FederationValidationError(
                    "invalid-storage-response",
                    "page",
                    "logical storage returned an invalid committed batch delta page",
                )
        else:
            raise FederationValidationError(
                "unsupported-recorder-storage-listing-mode",
                "listing_mode",
                "batch listing mode must be full or delta",
            )
        return {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "response",
            "correlation_id": correlation_id,
            "status": "accepted",
            "operation": StorageOperation.BATCH_LIST.value,
            "page": page.to_dict(),
        }

    async def _read_response(
        self,
        actor_node_id: str,
        session_id: str,
        correlation_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        reference = CommittedBatchReference.from_dict(payload.get("reference"))
        self._authorize_read_access(
            actor_node_id,
            session_id,
            reference.group_id,
            reference.dataset_id,
        )
        content = await self.logical_client.read_committed_batch(
            reference=reference,
        )
        return {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "response",
            "correlation_id": correlation_id,
            "status": "accepted",
            "operation": StorageOperation.BATCH_READ.value,
            "content": content,
        }

    def _authorize_read_access(
        self,
        actor_node_id: str,
        session_id: str,
        group_id: str,
        dataset_id: str | None,
    ) -> None:
        if self.authorize_read is not None:
            self.authorize_read(
                actor_node_id,
                session_id,
                group_id,
                dataset_id,
            )
        now = time.monotonic()
        cutoff = now - 60.0
        window = self._read_windows.setdefault(actor_node_id, deque())
        while window and window[0] <= cutoff:
            window.popleft()
        if len(window) >= self.read_requests_per_minute:
            raise FederationValidationError(
                "storage-read-rate-limit",
                "actor_node_id",
                "bounded logical-storage read rate exceeded; retry later",
            )
        window.append(now)
        if len(self._read_windows) > 1024:
            self._read_windows = {
                actor: values
                for actor, values in self._read_windows.items()
                if values and values[-1] > cutoff
            }


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
        if callable(getattr(self.relay_client, "request_message_response", None)):
            return
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
        if callable(getattr(self.relay_client, "request_message_response", None)):
            response = await self._request_response(
                payload,
                correlation_id=correlation_id,
                operation=StorageOperation.BATCH_INGEST,
            )
            return _outcome_from_dict(response.get("outcome"))
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
            if not isinstance(delivery, dict) or delivery.get("delivered") is not True:
                raise FederationValidationError(
                    "recorder-storage-route-failed",
                    "authority_node_id",
                    "relay did not confirm delivery to the logical-storage authority",
                )
            return await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            self._pending.pop(correlation_id, None)

    async def list_committed_batches(
        self,
        *,
        group_id: str,
        dataset_id: str | None = None,
        limit: int = RECORDER_CATALOG_MAX_PAGE_SIZE,
        cursor: str | None = None,
    ) -> CommittedBatchPage:
        """List manifest-authoritative batches visible to this session member."""

        if not callable(getattr(self.relay_client, "request_message_response", None)):
            raise FederationValidationError(
                "correlated-relay-required",
                "relay_client",
                "federated batch discovery requires the shared correlated relay client",
            )
        correlation_id = f"storage-catalog-{uuid.uuid4().hex}"
        payload: dict[str, Any] = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "operation": StorageOperation.BATCH_LIST.value,
            "correlation_id": correlation_id,
            "group_id": _text(group_id, "group_id"),
            "dataset_id": (
                None if dataset_id is None else _text(dataset_id, "dataset_id")
            ),
            "limit": _positive(limit, "limit"),
            "cursor": (
                None if cursor is None else _text(cursor, "cursor", maximum=8192)
            ),
        }
        if limit > RECORDER_CATALOG_MAX_PAGE_SIZE:
            raise FederationValidationError(
                "recorder-catalog-page-too-large",
                "limit",
                f"must not exceed {RECORDER_CATALOG_MAX_PAGE_SIZE}",
            )
        _bounded_payload(payload)
        response = await self._request_response(
            payload,
            correlation_id=correlation_id,
            operation=StorageOperation.BATCH_LIST,
        )
        return CommittedBatchPage.from_dict(response.get("page"))

    async def list_committed_batch_delta(
        self,
        *,
        group_id: str,
        baseline_manifest_revision: int,
        baseline_manifest_hash: str,
        dataset_id: str | None = None,
        limit: int = RECORDER_CATALOG_MAX_PAGE_SIZE,
        cursor: str | None = None,
    ) -> CommittedBatchDeltaPage:
        """List commits added after an exact manifest over the shared relay."""

        if not callable(getattr(self.relay_client, "request_message_response", None)):
            raise FederationValidationError(
                "correlated-relay-required",
                "relay_client",
                "federated batch discovery requires the shared correlated relay client",
            )
        if limit > RECORDER_CATALOG_MAX_PAGE_SIZE:
            raise FederationValidationError(
                "recorder-catalog-page-too-large",
                "limit",
                f"must not exceed {RECORDER_CATALOG_MAX_PAGE_SIZE}",
            )
        correlation_id = f"storage-catalog-delta-{uuid.uuid4().hex}"
        payload: dict[str, Any] = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "operation": StorageOperation.BATCH_LIST.value,
            "listing_mode": "delta",
            "correlation_id": correlation_id,
            "group_id": _text(group_id, "group_id"),
            "dataset_id": (
                None if dataset_id is None else _text(dataset_id, "dataset_id")
            ),
            "baseline_manifest_revision": _non_negative(
                baseline_manifest_revision,
                "baseline_manifest_revision",
            ),
            "baseline_manifest_hash": _text(
                baseline_manifest_hash,
                "baseline_manifest_hash",
                maximum=71,
            ),
            "limit": _positive(limit, "limit"),
            "cursor": (
                None if cursor is None else _text(cursor, "cursor", maximum=8192)
            ),
        }
        _bounded_payload(payload)
        response = await self._request_response(
            payload,
            correlation_id=correlation_id,
            operation=StorageOperation.BATCH_LIST,
        )
        return CommittedBatchDeltaPage.from_dict(response.get("page"))

    async def read_committed_batch(
        self,
        reference: CommittedBatchReference | dict[str, Any],
    ) -> object:
        """Read by signed manifest evidence, never by a provider path."""

        if isinstance(reference, dict):
            reference = CommittedBatchReference.from_dict(reference)
        if not isinstance(reference, CommittedBatchReference):
            raise FederationValidationError(
                "invalid-batch-reference",
                "reference",
                "must be a committed batch reference",
            )
        if not callable(getattr(self.relay_client, "request_message_response", None)):
            raise FederationValidationError(
                "correlated-relay-required",
                "relay_client",
                "federated committed reads require the shared correlated relay client",
            )
        correlation_id = f"storage-read-{uuid.uuid4().hex}"
        payload = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "operation": StorageOperation.BATCH_READ.value,
            "correlation_id": correlation_id,
            "reference": reference.to_dict(),
        }
        _bounded_payload(payload)
        response = await self._request_response(
            payload,
            correlation_id=correlation_id,
            operation=StorageOperation.BATCH_READ,
        )
        if "content" not in response:
            raise FederationValidationError(
                "invalid-recorder-storage-response",
                "content",
                "accepted read response must contain content",
            )
        content = response["content"]
        if BatchIngestRequest.calculate_content_hash(content) != reference.content_hash:
            raise FederationValidationError(
                "content-hash-mismatch",
                "content_hash",
                "authority response does not match the committed reference",
            )
        return content

    async def _request_response(
        self,
        payload: dict[str, Any],
        *,
        correlation_id: str,
        operation: StorageOperation,
    ) -> dict[str, Any]:
        request = getattr(self.relay_client, "request_message_response")
        message = await request(
            session_id=self.session_id,
            target_node_id=self.authority_node_id,
            payload=payload,
            correlation_id=correlation_id,
            timeout=self.request_timeout,
        )
        response = getattr(message, "payload", None)
        if (
            getattr(message, "actor_node_id", None) != self.authority_node_id
            or getattr(message, "session_id", None) != self.session_id
            or not isinstance(response, dict)
            or response.get("kind") != RECORDER_LOGICAL_STORAGE_KIND
            or response.get("message") != "response"
            or response.get("correlation_id") != correlation_id
        ):
            raise FederationValidationError(
                "invalid-recorder-storage-response",
                "response",
                "relay response does not match the authenticated request",
            )
        if response.get("status") == "rejected":
            self._raise_rejected(response)
        if response.get("status") != "accepted":
            raise FederationValidationError(
                "invalid-recorder-storage-response",
                "status",
                "logical storage response must be accepted or rejected",
            )
        response_operation = response.get("operation")
        if response_operation not in {None, operation.value} or (
            response_operation is None
            and operation is not StorageOperation.BATCH_INGEST
        ):
            raise FederationValidationError(
                "invalid-recorder-storage-response",
                "operation",
                "logical storage response operation does not match the request",
            )
        return response

    @staticmethod
    def _raise_rejected(payload: dict[str, Any]) -> None:
        error = payload.get("error")
        if isinstance(error, dict):
            raise FederationValidationError(
                str(error.get("code") or "recorder-storage-rejected"),
                str(error.get("field") or "storage"),
                str(error.get("message") or "logical storage rejected the request"),
            )
        raise FederationValidationError(
            "recorder-storage-rejected",
            "storage",
            "logical storage rejected the request",
        )

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
                        str(
                            error.get("message") or "logical storage rejected the batch"
                        ),
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

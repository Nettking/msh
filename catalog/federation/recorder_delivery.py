"""Durable recorder-to-logical-storage delivery for Phase D."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from .errors import FederationValidationError
from .outbox import SQLiteOutbox
from .phase_d_client import PhaseDIngestOutcome
from .storage_protocol import BatchIngestRequest

RECORDER_STORAGE_SCHEMA = "msh.recorder.storage_delivery.v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RecorderStorageClient(Protocol):
    async def ingest_batch(
        self,
        *,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at: datetime,
        dataset_schema_name: str = "msh.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ) -> PhaseDIngestOutcome: ...


@dataclass(frozen=True)
class RecorderDeliveryRunResult:
    attempted: int
    committed: int
    pending: int


class DurableRecorderDeliveryQueue:
    """Retain recorder batches locally until the remote policy reports commit."""

    def __init__(
        self,
        *,
        outbox: SQLiteOutbox,
        client: RecorderStorageClient,
        session_id: str,
        clock=_now,
    ) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise FederationValidationError(
                "invalid-recorder-delivery",
                "session_id",
                "must identify the authenticated Federation session",
            )
        self.outbox = outbox
        self.client = client
        self.session_id = session_id.strip()
        self.clock = clock

    def enqueue(
        self,
        *,
        session_id: str,
        group_id: str,
        dataset_id: str,
        batch_id: str,
        idempotency_key: str,
        content: object,
        created_at: datetime,
        dataset_schema_name: str = "msh.storage.dataset.opaque",
        dataset_schema_version: int = 1,
    ):
        if session_id != self.session_id:
            raise FederationValidationError(
                "recorder-session-mismatch",
                "session_id",
                "must match the queue's authenticated Federation session",
            )
        content_hash = BatchIngestRequest.calculate_content_hash(content)
        return self.outbox.enqueue(
            session_id=session_id,
            destination_id=group_id,
            schema_id=RECORDER_STORAGE_SCHEMA,
            payload={
                "group_id": group_id,
                "dataset_id": dataset_id,
                "dataset_schema_name": dataset_schema_name,
                "dataset_schema_version": dataset_schema_version,
                "batch_id": batch_id,
                "idempotency_key": idempotency_key,
                "content": content,
                "created_at": created_at.isoformat(),
            },
            idempotency_key=idempotency_key,
            content_hash=content_hash,
            now=self.clock(),
        )

    @staticmethod
    def _ordering_key(entry) -> tuple[str, str, str] | None:
        payload = entry.payload
        if not isinstance(payload, dict):
            return None
        dataset_id = payload.get("dataset_id")
        if not isinstance(dataset_id, str) or not dataset_id:
            return None
        return entry.session_id, entry.destination_id, dataset_id

    async def run_once(self, *, limit: int = 100) -> RecorderDeliveryRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-limit",
                "limit",
                "must be a positive integer",
            )

        # Preserve recorder sequence order independently per logical dataset.
        # A failed or not-yet-due older entry fences newer entries for that same
        # session/group/dataset until the older entry commits. Other datasets
        # remain free to make progress.
        now = self.clock()
        pending_entries = tuple(
            sorted(
                (
                    entry
                    for entry in self.outbox.pending()
                    if entry.schema_id == RECORDER_STORAGE_SCHEMA
                    # The client is authenticated for exactly this session.
                    # Rows from prior sessions remain durable for a worker
                    # holding the matching authority; they are never sent here.
                    and entry.session_id == self.session_id
                ),
                key=lambda entry: entry.outbox_id,
            )
        )
        blocked: set[tuple[str, str, str]] = set()
        attempted = 0
        committed = 0
        pending = 0

        for entry in pending_entries:
            if attempted >= limit:
                break
            ordering_key = self._ordering_key(entry)
            if ordering_key is not None and ordering_key in blocked:
                continue
            if entry.next_attempt_at > now:
                if ordering_key is not None:
                    blocked.add(ordering_key)
                continue

            attempted += 1
            payload = entry.payload
            failed = False
            try:
                if not isinstance(payload, dict):
                    raise FederationValidationError(
                        "invalid-recorder-delivery",
                        "payload",
                        "must be an object",
                    )
                created_at = datetime.fromisoformat(
                    str(payload["created_at"]).replace("Z", "+00:00")
                )
                outcome = await self.client.ingest_batch(
                    group_id=str(payload["group_id"]),
                    dataset_id=str(payload["dataset_id"]),
                    dataset_schema_name=str(
                        payload.get(
                            "dataset_schema_name",
                            "msh.storage.dataset.opaque",
                        )
                    ),
                    dataset_schema_version=int(
                        payload.get("dataset_schema_version", 1)
                    ),
                    batch_id=str(payload["batch_id"]),
                    idempotency_key=str(payload["idempotency_key"]),
                    content=payload["content"],
                    created_at=created_at,
                )
                if outcome.committed:
                    self.outbox.acknowledge(entry.outbox_id, now=self.clock())
                    committed += 1
                else:
                    self.outbox.record_failure(
                        entry.outbox_id,
                        error=outcome.message or "storage acknowledgement is pending",
                        now=self.clock(),
                    )
                    pending += 1
                    failed = True
            except (
                FederationValidationError,
                KeyError,
                TypeError,
                ValueError,
                OSError,
                RuntimeError,
            ) as exc:
                self.outbox.record_failure(
                    entry.outbox_id,
                    error=str(exc),
                    now=self.clock(),
                )
                pending += 1
                failed = True

            if failed and ordering_key is not None:
                blocked.add(ordering_key)

        return RecorderDeliveryRunResult(attempted, committed, pending)

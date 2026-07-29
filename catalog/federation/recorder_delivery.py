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
        clock=_now,
    ) -> None:
        self.outbox = outbox
        self.client = client
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

    async def run_once(self, *, limit: int = 100) -> RecorderDeliveryRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError("invalid-limit", "limit", "must be a positive integer")
        due = tuple(
            entry
            for entry in self.outbox.pending(now=self.clock())
            if entry.schema_id == RECORDER_STORAGE_SCHEMA
        )[:limit]
        committed = 0
        pending = 0
        for entry in due:
            payload = entry.payload
            try:
                if not isinstance(payload, dict):
                    raise FederationValidationError("invalid-recorder-delivery", "payload", "must be an object")
                created_at = datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00"))
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
            except (FederationValidationError, KeyError, TypeError, ValueError, OSError, RuntimeError) as exc:
                self.outbox.record_failure(entry.outbox_id, error=str(exc), now=self.clock())
                pending += 1
        return RecorderDeliveryRunResult(len(due), committed, pending)

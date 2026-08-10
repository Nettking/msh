"""D6 durable primary-to-replica replication using the SQLite outbox."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from .errors import FederationValidationError
from .outbox import OutboxEntry, SQLiteOutbox
from .storage_control_plane import StorageControlPlaneStore
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)

REPLICATION_SCHEMA = "fcp.storage_replication.v1"
PHASE_D_SERVICE_REPLICATION_OWNER = "phase-d-storage-service"


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ReplicationTarget:
    provider_id: str
    node_id: str


class ReplicationTransport(Protocol):
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope: ...


class CallableReplicationTransport:
    def __init__(
        self,
        sender: Callable[[str, StorageRequestEnvelope], Awaitable[StorageResponseEnvelope]],
    ) -> None:
        self._sender = sender

    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope:
        return await self._sender(target_node_id, envelope)


class ReplicationPlanner:
    """Resolve the currently assigned replicas for one logical storage group."""

    def __init__(self, control_plane: StorageControlPlaneStore) -> None:
        self.control_plane = control_plane

    def targets(self, *, session_id: str, group_id: str) -> tuple[ReplicationTarget, ...]:
        snapshot = self.control_plane.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None:
            raise FederationValidationError(
                "unknown-storage-group", "group_id", "storage group is not registered"
            )
        targets: list[ReplicationTarget] = []
        for provider_id in assignment.replica_provider_ids:
            provider = snapshot.providers.get(provider_id)
            if provider is None or not provider.assignable:
                continue
            targets.append(ReplicationTarget(provider.provider_id, provider.node_id))
        return tuple(targets)


class DurableReplicationOutbox:
    """Persist immutable replication intent before delivery is attempted."""

    def __init__(self, outbox: SQLiteOutbox, planner: ReplicationPlanner) -> None:
        self.outbox = outbox
        self.planner = planner

    def enqueue_ingest(
        self,
        request: BatchIngestRequest,
        *,
        now: datetime | None = None,
    ) -> tuple[OutboxEntry, ...]:
        timestamp = now or _now()
        entries: list[OutboxEntry] = []
        for target in self.planner.targets(
            session_id=request.authority.session_id,
            group_id=request.authority.group_id,
        ):
            entry, _ = self.outbox.enqueue(
                session_id=request.authority.session_id,
                destination_id=target.provider_id,
                schema_id=REPLICATION_SCHEMA,
                payload={
                    "target_node_id": target.node_id,
                    "target_provider_id": target.provider_id,
                    "group_id": request.authority.group_id,
                    "request": request.to_dict(),
                },
                idempotency_key=f"{request.idempotency_key}:{target.provider_id}",
                content_hash=request.content_hash,
                now=timestamp,
            )
            entries.append(entry)
        return tuple(entries)


@dataclass(frozen=True)
class ReplicationRunResult:
    attempted: int
    completed: int
    failed: int


class ReplicationWorker:
    """Deliver due outbox entries at least once and retain failures durably."""

    def __init__(
        self,
        *,
        outbox: SQLiteOutbox,
        transport: ReplicationTransport,
        actor_node_id: str,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.outbox = outbox
        self.transport = transport
        self.actor_node_id = actor_node_id
        self.clock = clock

    async def run_once(self, *, limit: int = 100) -> ReplicationRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-limit", "limit", "must be a positive integer"
            )
        due = tuple(
            entry
            for entry in self.outbox.pending(now=self.clock())
            if entry.schema_id == REPLICATION_SCHEMA
            and (
                not isinstance(entry.payload, dict)
                or entry.payload.get("delivery_owner")
                != PHASE_D_SERVICE_REPLICATION_OWNER
            )
        )[:limit]
        completed = 0
        failed = 0
        for entry in due:
            try:
                await self._deliver(entry)
                self.outbox.acknowledge(entry.outbox_id, now=self.clock())
                completed += 1
            except (
                FederationValidationError,
                OSError,
                RuntimeError,
                TimeoutError,
                TypeError,
                ValueError,
            ) as error:
                self.outbox.record_failure(
                    entry.outbox_id,
                    error=str(error),
                    now=self.clock(),
                )
                failed += 1
        return ReplicationRunResult(len(due), completed, failed)

    async def _deliver(self, entry: OutboxEntry) -> None:
        if entry.schema_id != REPLICATION_SCHEMA or not isinstance(entry.payload, dict):
            raise FederationValidationError(
                "invalid-replication-entry", "payload", "outbox entry is not a D6 replication request"
            )
        target_node_id = entry.payload.get("target_node_id")
        target_provider_id = entry.payload.get("target_provider_id")
        group_id = entry.payload.get("group_id")
        request_value = entry.payload.get("request")
        if not all(isinstance(value, str) and value for value in (
            target_node_id,
            target_provider_id,
            group_id,
        )) or not isinstance(request_value, dict):
            raise FederationValidationError(
                "invalid-replication-entry", "payload", "replication routing metadata is malformed"
            )
        request = BatchIngestRequest.from_dict(request_value)
        envelope = StorageRequestEnvelope(
            request_id=f"replication-{entry.outbox_id}-{entry.attempt_count}",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=entry.session_id,
            actor_node_id=self.actor_node_id,
            authorization_context={
                "kind": "storage-replication",
                "group_id": group_id,
                "provider_id": target_provider_id,
                "source_outbox_id": entry.outbox_id,
            },
            payload=request.to_dict(),
        )
        response = await self.transport.request(
            target_node_id=target_node_id,
            envelope=envelope,
        )
        if response.request_id != envelope.request_id:
            raise FederationValidationError(
                "response-request-mismatch", "request_id", "replication response belongs to another request"
            )
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "replication",
                response.error.message,
            )
        result = BatchIngestResult.from_dict(response.result)
        if (
            result.batch_id != request.batch_id
            or result.idempotency_key != request.idempotency_key
            or result.content_hash != request.content_hash
        ):
            raise FederationValidationError(
                "replication-identity-mismatch",
                "result",
                "replica response does not match the immutable batch identity",
            )

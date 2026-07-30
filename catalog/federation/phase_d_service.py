"""Integrated Phase D storage service: authority, replication, and acknowledgements."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol

from .commit_tracking import DurableAcknowledgementStore
from .errors import FederationValidationError
from .local_storage import BatchStorageProvider, LocalStorageService
from .outbox import OutboxEntry, OutboxState, SQLiteOutbox
from .replication import REPLICATION_SCHEMA, ReplicationTransport
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageError,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
)


class OperationalControlPlane(Protocol):
    def snapshot(self, session_id: str): ...

    def acknowledgement_policy(self, session_id: str, group_id: str): ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PhaseDStorageService:
    """Storage provider endpoint used by both primary clients and replica traffic.

    Primary ingest records replication intent before provider mutation. Successful
    ingest activates those intents, delivers due replicas, and reports success only
    after the configured acknowledgement policy is durably satisfied.
    """

    def __init__(
        self,
        *,
        provider_id: str,
        provider: BatchStorageProvider,
        control_plane: OperationalControlPlane,
        outbox: SQLiteOutbox | None = None,
        acknowledgements: DurableAcknowledgementStore | None = None,
        replication_transport: ReplicationTransport | None = None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        if not isinstance(provider_id, str) or not provider_id:
            raise ValueError("provider_id must be non-empty text")
        self.provider_id = provider_id
        self.provider = provider
        self.control_plane = control_plane
        self.outbox = outbox
        self.acknowledgements = acknowledgements
        self.replication_transport = replication_transport
        self.clock = clock
        self.local = LocalStorageService(provider)

    async def dispatch(self, envelope: StorageRequestEnvelope) -> StorageResponseEnvelope:
        try:
            if envelope.operation is not StorageOperation.BATCH_INGEST:
                return self.local.dispatch(envelope)
            request = BatchIngestRequest.from_dict(envelope.payload)
            if request.authority.session_id != envelope.session_id:
                raise FederationValidationError(
                    StorageErrorCode.INVALID_REQUEST.value,
                    "session_id",
                    "envelope and authority session differ",
                )
            kind = envelope.authorization_context.get("kind")
            if kind == "storage-replication":
                result = self._ingest_replica(envelope, request)
                return self._success(envelope, result.to_dict())
            result, status = await self._ingest_primary(envelope, request)
            if status.committed:
                payload = result.to_dict()
                payload.update(
                    {
                        "commit_state": "committed",
                        "required_replica_acks": status.required_replica_acks,
                        "acknowledged_replica_ids": list(status.acknowledged_replica_ids),
                    }
                )
                return self._success(envelope, payload)
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.INTERNAL_ERROR,
                    message="primary is durable but acknowledgement policy is not yet satisfied",
                    field="replication",
                    retryable=True,
                ),
            )
        except FederationValidationError as exc:
            try:
                code = StorageErrorCode(exc.code)
            except ValueError:
                code = StorageErrorCode.INVALID_REQUEST
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(code=code, message=exc.message, field=exc.field),
            )
        except (OSError, sqlite3.Error, TimeoutError, RuntimeError, TypeError, ValueError) as exc:
            return StorageResponseEnvelope(
                request_id=envelope.request_id,
                protocol=STORAGE_PROTOCOL,
                protocol_version=envelope.protocol_version,
                ok=False,
                error=StorageError(
                    code=StorageErrorCode.INTERNAL_ERROR,
                    message=str(exc) or type(exc).__name__,
                    retryable=True,
                ),
            )

    def _success(self, envelope: StorageRequestEnvelope, result: dict[str, object]) -> StorageResponseEnvelope:
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=envelope.protocol_version,
            ok=True,
            result=result,
        )

    def _validate_grant(self, request: BatchIngestRequest, *, require_primary_provider: bool) -> tuple[object, object, dict]:
        authority = request.authority
        snapshot = self.control_plane.snapshot(authority.session_id)
        assignment = snapshot.groups.get(authority.group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "authority.group_id",
                "storage group has no assigned primary",
            )
        primary = snapshot.providers.get(assignment.primary_provider_id)
        if primary is None or not primary.assignable:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "authority.actor_node_id",
                "assigned primary provider is not active and authorized",
            )
        if require_primary_provider and assignment.primary_provider_id != self.provider_id:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "provider_id",
                "request was delivered to a provider that is not the assigned primary",
            )
        grant = snapshot.leader_grants.get(authority.group_id)
        if grant is None or grant.get("provider_id") != assignment.primary_provider_id:
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.grant_id",
                "no active grant exists for the assigned primary",
            )
        if authority.actor_node_id != primary.node_id:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "authority.actor_node_id",
                "authority actor is not the node hosting the assigned primary",
            )
        if authority.grant_id != grant.get("grant_id"):
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.grant_id",
                "grant is not the active coordinator-issued grant",
            )
        if authority.term != int(grant["term"]):
            raise FederationValidationError(StorageErrorCode.STALE_TERM.value, "authority.term", "term is stale")
        if authority.fencing_token != int(grant["fencing_token"]):
            raise FederationValidationError(
                StorageErrorCode.STALE_FENCING_TOKEN.value,
                "authority.fencing_token",
                "fencing token is stale",
            )
        expiry = datetime.fromisoformat(str(grant["lease_expires_at"]).replace("Z", "+00:00"))
        if authority.lease_expires_at != expiry:
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.lease_expires_at",
                "lease expiry does not match the active grant",
            )
        if self.clock().astimezone(timezone.utc) >= expiry:
            raise FederationValidationError(
                StorageErrorCode.LEASE_EXPIRED.value,
                "authority.lease_expires_at",
                "write lease has expired",
            )
        if StorageOperation.BATCH_INGEST.value not in set(grant.get("scopes", ())):
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "operation",
                "active grant does not authorize batch ingest",
            )
        return snapshot, assignment, grant

    async def _ingest_primary(self, envelope: StorageRequestEnvelope, request: BatchIngestRequest):
        snapshot, assignment, _grant = self._validate_grant(request, require_primary_provider=True)
        context = envelope.authorization_context
        if context.get("provider_id") != self.provider_id or context.get("group_id") != request.authority.group_id:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "authorization_context",
                "logical route does not match the receiving primary provider",
            )
        if self.outbox is None or self.acknowledgements is None:
            raise FederationValidationError(
                "phase-d-runtime-missing",
                "service",
                "primary service requires an outbox and acknowledgement store",
            )
        request.validate_content_hash()
        policy = self.control_plane.acknowledgement_policy(
            request.authority.session_id, request.authority.group_id
        )
        replica_ids = tuple(assignment.replica_provider_ids)
        status = self.acknowledgements.prepare(
            request,
            policy=policy,
            replica_provider_ids=replica_ids,
            now=self.clock(),
        )
        prepared = self._prepare_replication(snapshot, request, replica_ids)
        result = self.provider.ingest(request)
        status = self.acknowledgements.mark_primary_committed(
            request.authority.session_id,
            request.authority.group_id,
            request.batch_id,
            now=self.clock(),
        )
        for entry in prepared:
            if entry.state is OutboxState.PREPARED:
                self.outbox.activate(entry.outbox_id, now=self.clock())
        if not status.committed and self.replication_transport is not None:
            await self._deliver_batch(request)
            status = self.acknowledgements.status(
                request.authority.session_id,
                request.authority.group_id,
                request.batch_id,
            )
            assert status is not None
        return result, status

    def _prepare_replication(self, snapshot, request: BatchIngestRequest, replica_ids: tuple[str, ...]) -> tuple[OutboxEntry, ...]:
        assert self.outbox is not None
        entries: list[OutboxEntry] = []
        for provider_id in replica_ids:
            provider = snapshot.providers.get(provider_id)
            if provider is None:
                raise FederationValidationError(
                    "unknown-provider", "provider_id", "assigned replica is not registered"
                )
            entry, _created = self.outbox.prepare(
                session_id=request.authority.session_id,
                destination_id=provider_id,
                schema_id=REPLICATION_SCHEMA,
                payload={
                    "target_node_id": provider.node_id,
                    "target_provider_id": provider_id,
                    "group_id": request.authority.group_id,
                    "request": request.to_dict(),
                },
                idempotency_key=f"{request.idempotency_key}:{provider_id}",
                content_hash=request.content_hash,
                now=self.clock(),
            )
            entries.append(entry)
        return tuple(entries)

    def _ingest_replica(self, envelope: StorageRequestEnvelope, request: BatchIngestRequest):
        _snapshot, assignment, _grant = self._validate_grant(request, require_primary_provider=False)
        context = envelope.authorization_context
        if self.provider_id not in assignment.replica_provider_ids:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "provider_id",
                "replication target is not an assigned replica",
            )
        if context.get("provider_id") != self.provider_id or context.get("group_id") != request.authority.group_id:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "authorization_context",
                "replication route does not match the receiving replica",
            )
        if envelope.actor_node_id != request.authority.actor_node_id:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "actor_node_id",
                "replication sender is not the active primary node",
            )
        return self.provider.ingest(request)

    async def _deliver_batch(self, request: BatchIngestRequest) -> None:
        assert self.outbox is not None
        assert self.acknowledgements is not None
        assert self.replication_transport is not None
        for entry in self.outbox.pending(now=self.clock()):
            if not self._entry_matches(entry, request):
                continue
            try:
                await self._deliver_entry(entry)
                self.outbox.acknowledge(entry.outbox_id, now=self.clock())
                payload = entry.payload
                self.acknowledgements.acknowledge(
                    request.authority.session_id,
                    request.authority.group_id,
                    request.batch_id,
                    str(payload["target_provider_id"]),
                    now=self.clock(),
                )
            except (FederationValidationError, OSError, RuntimeError, TimeoutError, TypeError, ValueError) as exc:
                self.outbox.record_failure(entry.outbox_id, error=str(exc), now=self.clock())

    @staticmethod
    def _entry_matches(entry: OutboxEntry, request: BatchIngestRequest) -> bool:
        if entry.schema_id != REPLICATION_SCHEMA or not isinstance(entry.payload, dict):
            return False
        value = entry.payload.get("request")
        if not isinstance(value, dict):
            return False
        return (
            value.get("batch_id") == request.batch_id
            and value.get("authority", {}).get("session_id") == request.authority.session_id
            and value.get("authority", {}).get("group_id") == request.authority.group_id
        )

    async def _deliver_entry(self, entry: OutboxEntry) -> None:
        assert self.replication_transport is not None
        if not isinstance(entry.payload, dict):
            raise FederationValidationError("invalid-replication-entry", "payload", "must be an object")
        target_node_id = entry.payload.get("target_node_id")
        target_provider_id = entry.payload.get("target_provider_id")
        group_id = entry.payload.get("group_id")
        request_value = entry.payload.get("request")
        if not all(isinstance(value, str) and value for value in (target_node_id, target_provider_id, group_id)):
            raise FederationValidationError("invalid-replication-entry", "payload", "routing metadata is malformed")
        if not isinstance(request_value, dict):
            raise FederationValidationError("invalid-replication-entry", "request", "request is malformed")
        request = BatchIngestRequest.from_dict(request_value)
        envelope = StorageRequestEnvelope(
            request_id=f"replication-{entry.outbox_id}-{entry.attempt_count}",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=entry.session_id,
            actor_node_id=request.authority.actor_node_id,
            authorization_context={
                "kind": "storage-replication",
                "group_id": group_id,
                "provider_id": target_provider_id,
                "source_outbox_id": entry.outbox_id,
            },
            payload=request.to_dict(),
        )
        response = await self.replication_transport.request(
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

    def reconcile_prepared(self) -> int:
        """Activate intents left prepared after primary storage became durable."""

        if self.outbox is None or self.acknowledgements is None:
            return 0
        activated = 0
        for entry in self.outbox.prepared():
            if entry.schema_id != REPLICATION_SCHEMA or not isinstance(entry.payload, dict):
                continue
            request_value = entry.payload.get("request")
            if not isinstance(request_value, dict):
                continue
            request = BatchIngestRequest.from_dict(request_value)
            if not self.provider.exists(
                session_id=request.authority.session_id,
                group_id=request.authority.group_id,
                batch_id=request.batch_id,
            ):
                continue
            self.acknowledgements.mark_primary_committed(
                request.authority.session_id,
                request.authority.group_id,
                request.batch_id,
                now=self.clock(),
            )
            self.outbox.activate(entry.outbox_id, now=self.clock())
            activated += 1
        return activated

"""Recorder-facing logical storage client for the completed Phase D runtime."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .commit_tracking import DurableAcknowledgementStore
from .errors import FederationValidationError
from .manifest import ManifestItemKind
from .manifest_store import ManifestCommitIntent
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    WriteAuthority,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LogicalStorageTransport(Protocol):
    async def request(
        self,
        *,
        target_node_id: str,
        envelope: StorageRequestEnvelope,
    ) -> StorageResponseEnvelope: ...


@dataclass(frozen=True)
class PhaseDIngestOutcome:
    committed: bool
    result: BatchIngestResult | None = None
    retryable: bool = False
    error_code: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class _Route:
    provider_id: str
    node_id: str
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime


class PhaseDLogicalStorageClient:
    """Address storage by session/group while deriving primary authority dynamically.

    When ``acknowledgements`` is supplied, this client is the coordinator-side
    distributed commit boundary. It persists a manifest intent before the remote
    provider is called and advances the coordinator-owned manifest only after an
    authenticated successful response contains evidence satisfying the frozen
    acknowledgement policy.

    The remote provider may keep its own local manifest as restart evidence. That
    local chain is not the distributed authority and may have another revision
    after a primary change. In distributed mode, the coordinator-finalized chain
    is therefore the manifest reference used for acceptance.
    """

    def __init__(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        control_plane,
        transport: LogicalStorageTransport,
        acknowledgements: DurableAcknowledgementStore | None = None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.session_id = session_id
        self.actor_node_id = actor_node_id
        self.control_plane = control_plane
        self.transport = transport
        self.acknowledgements = acknowledgements
        self.clock = clock

    def _route(self, group_id: str) -> _Route:
        snapshot = self.control_plane.snapshot(self.session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "group_id",
                "storage group has no assigned primary",
            )
        provider = snapshot.providers.get(assignment.primary_provider_id)
        if provider is None or not provider.assignable:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "provider_id",
                "assigned primary is unavailable or unauthorized",
            )
        grant = snapshot.leader_grants.get(group_id)
        if grant is None or grant.get("provider_id") != provider.provider_id:
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "group_id",
                "assigned primary has no active grant",
            )
        expiry = datetime.fromisoformat(
            str(grant["lease_expires_at"]).replace("Z", "+00:00")
        )
        return _Route(
            provider.provider_id,
            provider.node_id,
            str(grant["grant_id"]),
            int(grant["term"]),
            int(grant["fencing_token"]),
            expiry,
        )

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
        placeholder = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=self.session_id,
                group_id=group_id,
                actor_node_id=self.actor_node_id,
                grant_id="route-resolved",
                term=0,
                fencing_token=0,
                lease_expires_at=created_at,
            ),
            dataset_id=dataset_id,
            batch_id=batch_id,
            idempotency_key=idempotency_key,
            content_hash=BatchIngestRequest.calculate_content_hash(content),
            content=content,
            created_at=created_at,
            dataset_schema_name=dataset_schema_name,
            dataset_schema_version=dataset_schema_version,
        )
        return await self.ingest(placeholder)

    async def ingest(self, request: BatchIngestRequest) -> PhaseDIngestOutcome:
        if request.authority.session_id != self.session_id:
            raise FederationValidationError(
                StorageErrorCode.INVALID_REQUEST.value,
                "authority.session_id",
                "request belongs to another session",
            )
        group_id = request.authority.group_id
        route = self._route(group_id)
        routed = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=self.session_id,
                group_id=group_id,
                actor_node_id=route.node_id,
                grant_id=route.grant_id,
                term=route.term,
                fencing_token=route.fencing_token,
                lease_expires_at=route.lease_expires_at,
            ),
            dataset_id=request.dataset_id,
            batch_id=request.batch_id,
            idempotency_key=request.idempotency_key,
            content_hash=request.content_hash,
            content=request.content,
            created_at=request.created_at,
            dataset_schema_name=request.dataset_schema_name,
            dataset_schema_version=request.dataset_schema_version,
        )
        intent = self._prepare_distributed_manifest(routed, route)
        response = await self.transport.request(
            target_node_id=route.node_id,
            envelope=StorageRequestEnvelope(
                request_id=f"storage-{uuid.uuid4().hex}",
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.BATCH_INGEST,
                session_id=self.session_id,
                actor_node_id=self.actor_node_id,
                authorization_context={
                    "kind": "storage-primary-route",
                    "group_id": group_id,
                    "provider_id": route.provider_id,
                },
                payload=routed.to_dict(),
            ),
        )
        if response.ok:
            return self._accept_success(
                request=request,
                routed=routed,
                group_id=group_id,
                value=response.result,
                intent=intent,
            )
        assert response.error is not None
        if response.error.retryable:
            return PhaseDIngestOutcome(
                committed=False,
                retryable=True,
                error_code=response.error.code.value,
                message=response.error.message,
            )
        raise FederationValidationError(
            response.error.code.value,
            response.error.field or "storage",
            response.error.message,
        )

    def _prepare_distributed_manifest(
        self,
        request: BatchIngestRequest,
        route: _Route,
    ) -> ManifestCommitIntent | None:
        if self.acknowledgements is None:
            return None
        snapshot = self.control_plane.snapshot(self.session_id)
        assignment = snapshot.groups.get(request.authority.group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != route.provider_id
        ):
            raise FederationValidationError(
                "manifest-primary-mismatch",
                "provider_id",
                "resolved route changed before distributed intent preparation",
            )
        status = self.acknowledgements.prepare(
            request,
            policy=self.control_plane.acknowledgement_policy(
                self.session_id,
                request.authority.group_id,
            ),
            replica_provider_ids=tuple(assignment.replica_provider_ids),
            now=self.clock(),
        )
        return self.control_plane.prepare_batch_manifest(
            request,
            status,
            primary_provider_id=route.provider_id,
            now=self.clock(),
        )

    def _accept_success(
        self,
        *,
        request: BatchIngestRequest,
        routed: BatchIngestRequest,
        group_id: str,
        value: dict[str, Any] | None,
        intent: ManifestCommitIntent | None,
    ) -> PhaseDIngestOutcome:
        if not isinstance(value, dict):
            return self._invalid_success("storage success payload is missing")
        try:
            result = BatchIngestResult.from_dict(value)
        except FederationValidationError as exc:
            return self._invalid_success(exc.message)
        if (
            result.batch_id != request.batch_id
            or result.idempotency_key != request.idempotency_key
            or result.content_hash != request.content_hash
        ):
            return self._invalid_success(
                "storage success identity does not match the requested batch"
            )
        remote_manifest_revision = value.get("manifest_revision")
        remote_manifest_hash = value.get("manifest_hash")
        if not self._valid_manifest_reference(
            value,
            remote_manifest_revision,
            remote_manifest_hash,
        ):
            return self._invalid_success(
                "storage success lacks valid manifest evidence"
            )

        authoritative_revision = remote_manifest_revision
        authoritative_hash = remote_manifest_hash
        if intent is not None:
            try:
                coordinated = self._finalize_distributed_manifest(
                    routed,
                    intent,
                    value,
                )
            except FederationValidationError as exc:
                return self._invalid_success(
                    f"distributed manifest evidence was rejected: {exc.message}"
                )
            authoritative_revision = coordinated.revision
            authoritative_hash = coordinated.manifest_hash

        history_reader = getattr(
            self.control_plane,
            "manifest_history",
            None,
        )
        if not callable(history_reader):
            return self._invalid_success(
                "authoritative manifest verification is unavailable"
            )
        try:
            history = history_reader(self.session_id, group_id)
            manifest = history[authoritative_revision]
        except (
            FederationValidationError,
            IndexError,
            TypeError,
            ValueError,
        ) as exc:
            return self._invalid_success(
                f"manifest evidence cannot be verified: {exc}"
            )
        item = next(
            (
                candidate
                for candidate in manifest.items
                if candidate.item_id == request.batch_id
            ),
            None,
        )
        if (
            manifest.revision != authoritative_revision
            or manifest.manifest_hash != authoritative_hash
            or item is None
            or item.kind is not ManifestItemKind.BATCH
            or item.dataset_id != request.dataset_id
            or item.schema_name != request.dataset_schema_name
            or item.schema_version != request.dataset_schema_version
            or item.idempotency_key != request.idempotency_key
            or item.content_hash != request.content_hash
        ):
            return self._invalid_success(
                "manifest does not contain the acknowledged batch"
            )
        return PhaseDIngestOutcome(
            committed=True,
            result=result,
        )

    @staticmethod
    def _valid_manifest_reference(
        value: dict[str, Any],
        manifest_revision: Any,
        manifest_hash: Any,
    ) -> bool:
        return bool(
            value.get("commit_state") == "committed"
            and not isinstance(manifest_revision, bool)
            and isinstance(manifest_revision, int)
            and manifest_revision >= 1
            and isinstance(manifest_hash, str)
            and len(manifest_hash) == 71
            and manifest_hash.startswith("sha256:")
            and all(
                character in "0123456789abcdef"
                for character in manifest_hash[7:]
            )
        )

    def _finalize_distributed_manifest(
        self,
        request: BatchIngestRequest,
        intent: ManifestCommitIntent,
        value: dict[str, Any],
    ):
        assert self.acknowledgements is not None
        required = value.get("required_replica_acks")
        acknowledged_value = value.get("acknowledged_replica_ids")
        if (
            isinstance(required, bool)
            or not isinstance(required, int)
            or required < 0
            or not isinstance(acknowledged_value, list)
            or any(
                not isinstance(provider_id, str) or not provider_id
                for provider_id in acknowledged_value
            )
            or len(acknowledged_value) != len(set(acknowledged_value))
        ):
            raise FederationValidationError(
                "invalid-manifest-acknowledgements",
                "acknowledged_replica_ids",
                "remote commit acknowledgement evidence is malformed",
            )
        acknowledged = tuple(sorted(acknowledged_value))
        if (
            required != intent.required_replica_acks
            or not set(acknowledged).issubset(intent.assigned_replica_ids)
            or len(acknowledged) < required
        ):
            raise FederationValidationError(
                "manifest-ack-policy-mismatch",
                "acknowledged_replica_ids",
                "remote evidence does not satisfy the frozen coordinator policy",
            )
        status = self.acknowledgements.mark_primary_committed(
            request.authority.session_id,
            request.authority.group_id,
            request.batch_id,
            now=self.clock(),
        )
        for provider_id in acknowledged:
            status = self.acknowledgements.acknowledge(
                request.authority.session_id,
                request.authority.group_id,
                request.batch_id,
                provider_id,
                now=self.clock(),
            )
        if not status.committed:
            raise FederationValidationError(
                "manifest-commit-not-ready",
                "acknowledged_replica_ids",
                "coordinator acknowledgement state is not committed",
            )
        return self.control_plane.finalize_batch_manifest_intent(
            intent,
            status,
            now=self.clock(),
        )

    @staticmethod
    def _invalid_success(message: str) -> PhaseDIngestOutcome:
        return PhaseDIngestOutcome(
            committed=False,
            retryable=True,
            error_code="invalid-storage-response",
            message=message,
        )

    async def exists(self, *, group_id: str, batch_id: str) -> bool:
        result = await self._read_request(
            group_id=group_id,
            operation=StorageOperation.BATCH_EXISTS,
            payload={"group_id": group_id, "batch_id": batch_id},
        )
        exists = result.get("exists")
        if not isinstance(exists, bool):
            raise FederationValidationError(
                "invalid-storage-response",
                "exists",
                "must be boolean",
            )
        return exists

    async def read(self, *, group_id: str, batch_id: str) -> object:
        result = await self._read_request(
            group_id=group_id,
            operation=StorageOperation.BATCH_READ,
            payload={"group_id": group_id, "batch_id": batch_id},
        )
        if "content" not in result:
            raise FederationValidationError(
                "invalid-storage-response",
                "content",
                "is required",
            )
        return result["content"]

    async def _read_request(
        self,
        *,
        group_id: str,
        operation: StorageOperation,
        payload: dict[str, object],
    ) -> dict[str, object]:
        route = self._route(group_id)
        envelope = StorageRequestEnvelope(
            request_id=f"storage-{uuid.uuid4().hex}",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=operation,
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
            authorization_context={
                "kind": "storage-primary-route",
                "group_id": group_id,
                "provider_id": route.provider_id,
            },
            payload=payload,
        )
        response = await self.transport.request(
            target_node_id=route.node_id,
            envelope=envelope,
        )
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "storage",
                response.error.message,
            )
        assert response.result is not None
        return response.result

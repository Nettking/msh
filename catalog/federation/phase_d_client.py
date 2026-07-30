"""Recorder-facing logical storage client for the completed Phase D runtime."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .errors import FederationValidationError
from .manifest import ManifestItemKind
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
    """Address storage by session/group while deriving primary authority dynamically."""

    def __init__(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        control_plane,
        transport: LogicalStorageTransport,
    ) -> None:
        self.session_id = session_id
        self.actor_node_id = actor_node_id
        self.control_plane = control_plane
        self.transport = transport

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
        expiry = datetime.fromisoformat(str(grant["lease_expires_at"]).replace("Z", "+00:00"))
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
        dataset_schema_name: str = "msh.storage.dataset.opaque",
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
            value = response.result
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
            manifest_revision = value.get("manifest_revision")
            manifest_hash = value.get("manifest_hash")
            if (
                value.get("commit_state") != "committed"
                or isinstance(manifest_revision, bool)
                or not isinstance(manifest_revision, int)
                or manifest_revision < 1
                or not isinstance(manifest_hash, str)
                or len(manifest_hash) != 71
                or not manifest_hash.startswith("sha256:")
                or any(
                    character not in "0123456789abcdef"
                    for character in manifest_hash[7:]
                )
            ):
                return self._invalid_success(
                    "storage success lacks valid authoritative manifest evidence"
                )
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
                manifest = history[manifest_revision]
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
                manifest.revision != manifest_revision
                or manifest.manifest_hash != manifest_hash
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
            raise FederationValidationError("invalid-storage-response", "exists", "must be boolean")
        return exists

    async def read(self, *, group_id: str, batch_id: str) -> object:
        result = await self._read_request(
            group_id=group_id,
            operation=StorageOperation.BATCH_READ,
            payload={"group_id": group_id, "batch_id": batch_id},
        )
        if "content" not in result:
            raise FederationValidationError("invalid-storage-response", "content", "is required")
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
        response = await self.transport.request(target_node_id=route.node_id, envelope=envelope)
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "storage",
                response.error.message,
            )
        assert response.result is not None
        return response.result

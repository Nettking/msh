"""D4 write-authority enforcement for mutating storage operations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from .errors import FederationValidationError
from .local_storage import BatchStorageProvider, LocalStorageService
from .storage_control_plane import StorageControlPlaneStore
from .storage_protocol import (
    BatchIngestRequest,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
)


class StorageWriteAuthorityValidator:
    """Validate a write against the replayed coordinator-issued grant state."""

    def __init__(
        self,
        control_plane: StorageControlPlaneStore,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.control_plane = control_plane
        self._now = now or (lambda: datetime.now(timezone.utc))

    def validate(
        self,
        request: BatchIngestRequest,
        *,
        operation: StorageOperation = StorageOperation.BATCH_INGEST,
    ) -> None:
        authority = request.authority
        snapshot = self.control_plane.snapshot(authority.session_id)
        assignment = snapshot.groups.get(authority.group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "authority.group_id",
                "storage group has no assigned primary",
            )

        provider = snapshot.providers.get(assignment.primary_provider_id)
        if provider is None or not provider.assignable:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "authority.actor_node_id",
                "assigned primary provider is not active and authorized",
            )
        if authority.actor_node_id != provider.node_id:
            raise FederationValidationError(
                StorageErrorCode.NOT_PRIMARY.value,
                "authority.actor_node_id",
                "actor is not the node hosting the assigned primary provider",
            )

        grant = snapshot.leader_grants.get(authority.group_id)
        if grant is None or grant.get("provider_id") != provider.provider_id:
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.grant_id",
                "no active grant exists for the assigned primary",
            )
        if authority.grant_id != grant.get("grant_id"):
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.grant_id",
                "grant is not the active coordinator-issued grant",
            )

        active_term = int(grant["term"])
        if authority.term != active_term:
            raise FederationValidationError(
                StorageErrorCode.STALE_TERM.value,
                "authority.term",
                "term does not match the active grant",
            )
        active_token = int(grant["fencing_token"])
        if authority.fencing_token != active_token:
            raise FederationValidationError(
                StorageErrorCode.STALE_FENCING_TOKEN.value,
                "authority.fencing_token",
                "fencing token does not match the active grant",
            )

        expiry = datetime.fromisoformat(str(grant["lease_expires_at"]).replace("Z", "+00:00"))
        now = self._now().astimezone(timezone.utc)
        if now >= expiry or now >= authority.lease_expires_at:
            raise FederationValidationError(
                StorageErrorCode.LEASE_EXPIRED.value,
                "authority.lease_expires_at",
                "write lease has expired",
            )
        if authority.lease_expires_at != expiry:
            raise FederationValidationError(
                StorageErrorCode.UNKNOWN_GRANT.value,
                "authority.lease_expires_at",
                "lease expiry does not match the active grant",
            )

        scopes = set(grant.get("scopes", ()))
        if operation.value not in scopes:
            raise FederationValidationError(
                StorageErrorCode.UNAUTHORIZED.value,
                "operation",
                "active grant does not authorize this write operation",
            )


class AuthorizedStorageService(LocalStorageService):
    """Local dispatcher that fences every mutating request before provider access."""

    def __init__(
        self,
        provider: BatchStorageProvider,
        authority_validator: StorageWriteAuthorityValidator,
    ) -> None:
        super().__init__(provider)
        self.authority_validator = authority_validator

    def _dispatch(self, envelope: StorageRequestEnvelope) -> dict[str, object]:
        if envelope.operation is StorageOperation.BATCH_INGEST:
            request = BatchIngestRequest.from_dict(envelope.payload)
            if request.authority.session_id != envelope.session_id:
                raise FederationValidationError(
                    StorageErrorCode.INVALID_REQUEST.value,
                    "session_id",
                    "envelope and authority session differ",
                )
            if request.authority.actor_node_id != envelope.actor_node_id:
                raise FederationValidationError(
                    StorageErrorCode.INVALID_REQUEST.value,
                    "actor_node_id",
                    "envelope and authority actor differ",
                )
            self.authority_validator.validate(request, operation=envelope.operation)
        return super()._dispatch(envelope)

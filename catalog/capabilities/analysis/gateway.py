"""Data-owner side of the analysis artifact boundary.

A worker never touches the data owner's filesystem. It asks this gateway for one
explicitly allowlisted artifact, and the gateway re-validates the request against
the durable artifact authority before a single byte is produced:

* the authenticated session must be the grant's session,
* the authenticated worker node and provider must be the grant's holder,
* the grant must be unexpired, unrevoked, and still match live job ownership,
* the artifact must be named in the grant's explicit input allowlist,
* the reference's content hash, schema, and endpoint must match the registry.

Any failure denies access and is audited by the authority. Nothing here widens
authority; it only enforces and then serves already-authorized bytes.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Protocol

from catalog.federation.errors import FederationValidationError

from ..artifact_contracts import ArtifactDescriptor, ArtifactInputReference
from ..artifact_secure_runtime import SQLiteCapabilityArtifactAuthority
from .content_store import ContentIdentity, LocalArtifactContentStore
from .contracts import ANALYSIS_ENDPOINT_ID


class AnalysisInputTransport(Protocol):
    """Authorized retrieval of one job input artifact body."""

    def fetch(
        self,
        *,
        grant_id: str,
        reference: ArtifactInputReference,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> Iterator[bytes]: ...


class AnalysisArtifactGateway:
    """Register data-owner artifacts and serve them only to authorized workers."""

    def __init__(
        self,
        authority: SQLiteCapabilityArtifactAuthority,
        content_store: LocalArtifactContentStore,
        *,
        endpoint_id: str = ANALYSIS_ENDPOINT_ID,
    ) -> None:
        self.authority = authority
        self.content_store = content_store
        self.endpoint_id = endpoint_id

    def register_input(
        self,
        *,
        artifact_id: str,
        session_id: str,
        job_id: str,
        schema_id: str,
        media_type: str,
        object_key: str,
        identity: ContentIdentity,
        authority_node_id: str,
        now: datetime,
    ) -> ArtifactDescriptor:
        descriptor = ArtifactDescriptor(
            artifact_id=artifact_id,
            session_id=session_id,
            job_id=job_id,
            schema_id=schema_id,
            media_type=media_type,
            content_hash=identity.content_hash,
            size_bytes=identity.size_bytes,
            endpoint_id=self.endpoint_id,
            object_key=object_key,
            created_at=now,
        )
        return self.authority.register_artifact(
            descriptor,
            authority_node_id=authority_node_id,
            now=now,
        )

    def input_reference(self, descriptor: ArtifactDescriptor) -> ArtifactInputReference:
        return ArtifactInputReference(
            artifact_id=descriptor.artifact_id,
            session_id=descriptor.session_id,
            job_id=descriptor.job_id,
            content_hash=descriptor.content_hash,
            schema_id=descriptor.schema_id,
            endpoint_id=descriptor.endpoint_id,
        )

    def fetch(
        self,
        *,
        grant_id: str,
        reference: ArtifactInputReference,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> Iterator[bytes]:
        """Authorize first, then stream. Never the other way round."""

        descriptor = self.authority.authorize_input(
            grant_id,
            reference,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        if descriptor.endpoint_id != self.endpoint_id:
            raise FederationValidationError(
                "artifact-endpoint-mismatch",
                "endpoint_id",
                "authorized artifact belongs to another logical endpoint",
            )
        return self.content_store.stream(
            descriptor.object_key,
            content_hash=descriptor.content_hash,
            size_bytes=descriptor.size_bytes,
        )


__all__ = ["AnalysisArtifactGateway", "AnalysisInputTransport"]

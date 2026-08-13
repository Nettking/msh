"""Data-owner side of the analysis artifact boundary.

A worker never touches the data owner's filesystem. It asks this gateway for one
explicitly allowlisted artifact, and the gateway re-validates the request against
the durable F7.6 artifact authority before a single byte is produced:

* the authenticated session must be the grant's session,
* the authenticated worker node and provider must be the grant's holder,
* the grant must be unexpired, unrevoked, and still match live job ownership
  (attempt, lease ID and lease generation included),
* the artifact must be named in the grant's explicit input allowlist,
* the reference's content hash, schema, and endpoint must match the registry.

Bytes then move on the existing F6 object-transfer contract
(:class:`ObjectTransferManifest` / :class:`ObjectTransferChunk`), so chunk
hashing, sizing, duplicate handling, and final integrity verification are the
repository's existing rules rather than anything invented here.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol

from catalog.federation.errors import FederationValidationError
from catalog.federation.object_transfer import (
    DEFAULT_TRANSFER_CHUNK_BYTES,
    ObjectTransferChunk,
    ObjectTransferManifest,
    ObjectTransferReceiver,
)
from catalog.federation.object_transfer_staging import (
    FilesystemObjectTransferChunkStore,
)

from ..artifact_contracts import ArtifactDescriptor, ArtifactInputReference
from ..artifact_secure_runtime import SQLiteCapabilityArtifactAuthority
from .content_store import ContentIdentity, LocalArtifactContentStore
from .contracts import ANALYSIS_ENDPOINT_ID


class AnalysisArtifactTransport(Protocol):
    """Carrier for the F6 manifest and chunks of one authorized artifact."""

    async def manifest(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferManifest: ...

    async def chunk(
        self,
        *,
        target_node_id: str,
        grant_id: str,
        reference: ArtifactInputReference,
        index: int,
        worker_node_id: str,
        provider_id: str,
        session_id: str,
    ) -> ObjectTransferChunk: ...


class AnalysisArtifactGateway:
    """Register data-owner artifacts and serve them only to authorized workers."""

    def __init__(
        self,
        authority: SQLiteCapabilityArtifactAuthority,
        content_store: LocalArtifactContentStore,
        *,
        endpoint_id: str = ANALYSIS_ENDPOINT_ID,
        chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
    ) -> None:
        self.authority = authority
        self.content_store = content_store
        self.endpoint_id = endpoint_id
        self.chunk_size = int(chunk_size)

    # ------------------------------------------------------------------
    # Registration (data owner)
    # ------------------------------------------------------------------

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
        transfer_id: str | None = None,
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
            transfer_id=transfer_id or f"transfer-{artifact_id}",
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

    # ------------------------------------------------------------------
    # Authorized serving (data owner)
    # ------------------------------------------------------------------

    def _authorized(
        self,
        *,
        grant_id: str,
        reference: ArtifactInputReference,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> ArtifactDescriptor:
        """Authorize first, then and only then look at any bytes."""

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
        if descriptor.transfer_id is None:
            raise FederationValidationError(
                "artifact-transfer-id-required",
                "transfer_id",
                "transferred artifacts require an explicit stable transfer ID",
            )
        return descriptor

    def transfer_manifest(
        self,
        *,
        grant_id: str,
        reference: ArtifactInputReference,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> ObjectTransferManifest:
        descriptor = self._authorized(
            grant_id=grant_id,
            reference=reference,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        return ObjectTransferManifest(
            transfer_id=descriptor.transfer_id,
            object_id=descriptor.artifact_id,
            total_size=descriptor.size_bytes,
            object_hash=descriptor.content_hash,
            chunk_size=self.chunk_size,
            chunk_count=_chunk_count(descriptor.size_bytes, self.chunk_size),
        )

    def transfer_chunk(
        self,
        *,
        grant_id: str,
        reference: ArtifactInputReference,
        index: int,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> ObjectTransferChunk:
        descriptor = self._authorized(
            grant_id=grant_id,
            reference=reference,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        manifest = ObjectTransferManifest(
            transfer_id=descriptor.transfer_id,
            object_id=descriptor.artifact_id,
            total_size=descriptor.size_bytes,
            object_hash=descriptor.content_hash,
            chunk_size=self.chunk_size,
            chunk_count=_chunk_count(descriptor.size_bytes, self.chunk_size),
        )
        length = manifest.expected_chunk_length(index)
        data = self.content_store.read_range(
            descriptor.object_key,
            offset=index * self.chunk_size,
            length=length,
            content_hash=descriptor.content_hash,
            size_bytes=descriptor.size_bytes,
        )
        return ObjectTransferChunk.create(manifest=manifest, index=index, data=data)


def _chunk_count(size_bytes: int, chunk_size: int) -> int:
    return -(-size_bytes // chunk_size) if size_bytes else 0


async def retrieve_authorized_artifact(
    transport: AnalysisArtifactTransport,
    *,
    target_node_id: str,
    grant_id: str,
    reference: ArtifactInputReference,
    worker_node_id: str,
    provider_id: str,
    session_id: str,
    staging_root: Path,
    publication_root: Path,
) -> Path:
    """Pull one authorized artifact using the existing F6 receiver and store.

    The receiver owns duplicate detection, per-chunk hash checks, total-size and
    whole-object verification. Nothing about integrity is re-implemented here.
    """

    manifest = await transport.manifest(
        target_node_id=target_node_id,
        grant_id=grant_id,
        reference=reference,
        worker_node_id=worker_node_id,
        provider_id=provider_id,
        session_id=session_id,
    )
    store = FilesystemObjectTransferChunkStore(staging_root, publication_root)
    receiver = ObjectTransferReceiver(manifest, store)
    try:
        for index in range(manifest.chunk_count):
            chunk = await transport.chunk(
                target_node_id=target_node_id,
                grant_id=grant_id,
                reference=reference,
                index=index,
                worker_node_id=worker_node_id,
                provider_id=provider_id,
                session_id=session_id,
            )
            receiver.accept(chunk)
        receipt = receiver.verify_complete()
        return store.publish_verified(manifest, receipt)
    except BaseException:
        receiver.abort()
        raise


__all__ = [
    "AnalysisArtifactGateway",
    "AnalysisArtifactTransport",
    "retrieve_authorized_artifact",
]

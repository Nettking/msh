"""Public F7.6 authority with fail-closed logical endpoint enforcement."""

from __future__ import annotations

from .artifact_authority import ArtifactPublicationResult
from .artifact_contracts import (
    ArtifactDescriptor,
    ArtifactGrant,
    ArtifactGrantScope,
    ArtifactInputReference,
    ArtifactPublication,
    OutputPlacementPolicy,
)
from .artifact_endpoint import validate_logical_artifact_endpoint
from .artifact_runtime import (
    SQLiteCapabilityArtifactAuthority as _DurableArtifactAuthority,
)


class SQLiteCapabilityArtifactAuthority(_DurableArtifactAuthority):
    """Durable artifact authority that rejects network locations in control data."""

    def register_artifact(
        self,
        descriptor: ArtifactDescriptor,
        *,
        authority_node_id: str,
        now,
    ) -> ArtifactDescriptor:
        validate_logical_artifact_endpoint(descriptor.endpoint_id)
        return super().register_artifact(
            descriptor,
            authority_node_id=authority_node_id,
            now=now,
        )

    def issue_grant(
        self,
        job_id: str,
        *,
        coordinator_node_id: str,
        grant_id: str,
        worker_node_id: str,
        endpoint_id: str,
        scopes: tuple[ArtifactGrantScope, ...],
        input_references: tuple[ArtifactInputReference, ...] = (),
        placement_policy: OutputPlacementPolicy | None = None,
        expires_at,
        now,
    ) -> ArtifactGrant:
        endpoint_id = validate_logical_artifact_endpoint(endpoint_id)
        for reference in input_references:
            if isinstance(reference, ArtifactInputReference):
                validate_logical_artifact_endpoint(reference.endpoint_id)
        if placement_policy is not None:
            validate_logical_artifact_endpoint(placement_policy.endpoint_id)
        return super().issue_grant(
            job_id,
            coordinator_node_id=coordinator_node_id,
            grant_id=grant_id,
            worker_node_id=worker_node_id,
            endpoint_id=endpoint_id,
            scopes=scopes,
            input_references=input_references,
            placement_policy=placement_policy,
            expires_at=expires_at,
            now=now,
        )

    def authorize_input(
        self,
        grant_id: str,
        reference: ArtifactInputReference,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now,
    ) -> ArtifactDescriptor:
        validate_logical_artifact_endpoint(reference.endpoint_id)
        return super().authorize_input(
            grant_id,
            reference,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )

    def authorize_output(
        self,
        grant_id: str,
        descriptor: ArtifactDescriptor,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now,
    ) -> None:
        validate_logical_artifact_endpoint(descriptor.endpoint_id)
        validate_logical_artifact_endpoint(placement_policy.endpoint_id)
        return super().authorize_output(
            grant_id,
            descriptor,
            placement_policy,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )

    def publish(
        self,
        publication: ArtifactPublication,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now,
    ) -> ArtifactPublicationResult:
        validate_logical_artifact_endpoint(publication.descriptor.endpoint_id)
        validate_logical_artifact_endpoint(placement_policy.endpoint_id)
        return super().publish(
            publication,
            placement_policy,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )

"""Coordinator integration between F7.6 publication and the F7.5 result fence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from catalog.federation.errors import FederationValidationError

from .artifact_authority import ArtifactPublicationResult
from .artifact_contracts import ArtifactPublication, OutputPlacementPolicy, _text, _utc
from .artifact_runtime import SQLiteCapabilityArtifactAuthority
from .jobs import ArtifactReference
from .lifecycle_contracts import ResultCommit
from .lifecycle_store import ResultMutation, SQLiteJobLifecycleStore


@dataclass(frozen=True)
class PublishedJobResult:
    artifact_publication: ArtifactPublicationResult
    result_mutation: ResultMutation

    @property
    def result(self) -> ResultCommit:
        return self.result_mutation.result

    @property
    def changed(self) -> bool:
        return self.artifact_publication.changed or self.result_mutation.changed


class ArtifactResultCoordinator:
    """Publish one authorized artifact and commit its logical job reference.

    Artifact publication happens first and is durable/idempotent. If the
    coordinator stops before the F7.5 result transaction, replaying the same
    publication and command completes the result without duplicating data.
    """

    def __init__(
        self,
        authority: SQLiteCapabilityArtifactAuthority,
        job_store: SQLiteJobLifecycleStore,
        *,
        coordinator_node_id: str,
    ) -> None:
        self.authority = authority
        self.job_store = job_store
        self.coordinator_node_id = _text(
            coordinator_node_id, "coordinator_node_id"
        )
        if authority.job_store is not job_store:
            raise FederationValidationError(
                "artifact-job-store-mismatch",
                "job_store",
                "artifact authority and result coordinator must share one job store",
            )

    def publish_result(
        self,
        publication: ArtifactPublication,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        command_id: str,
        now: datetime,
    ) -> PublishedJobResult:
        if not isinstance(publication, ArtifactPublication):
            raise FederationValidationError(
                "invalid-artifact-publication",
                "publication",
                "must be an ArtifactPublication",
            )
        command_id = _text(command_id, "command_id")
        now = _utc(now, "now")
        snapshot = self.job_store.snapshot(publication.job_id)
        ownership = snapshot.ownership
        if ownership is None:
            committed = self.job_store.result_commit(publication.job_id)
            if committed is None:
                raise FederationValidationError(
                    "job-not-owned",
                    "job_id",
                    "result publication requires active ownership",
                )
            reference = self._reference(publication)
            if (
                committed.attempt_id == publication.attempt_id
                and committed.provider_id == publication.provider_id
                and committed.lease_id == publication.lease_id
                and committed.lease_generation == publication.lease_generation
                and committed.reference == reference
            ):
                return PublishedJobResult(
                    artifact_publication=ArtifactPublicationResult(
                        publication=publication,
                        changed=False,
                    ),
                    result_mutation=ResultMutation(
                        snapshot=snapshot,
                        result=committed,
                        changed=False,
                    ),
                )
            raise FederationValidationError(
                "result-commit-conflict",
                "publication",
                "job already has a different committed result",
            )
        if ownership.granted_by_coordinator_id != self.coordinator_node_id:
            raise FederationValidationError(
                "publication-coordinator-mismatch",
                "coordinator_node_id",
                "only the granting coordinator may publish the result",
            )
        if (
            ownership.attempt_id != publication.attempt_id
            or ownership.owner_provider_id != publication.provider_id
            or ownership.lease_id != publication.lease_id
            or ownership.lease_generation != publication.lease_generation
        ):
            raise FederationValidationError(
                "stale-artifact-publication",
                "lease_id",
                "publication does not match active job ownership",
            )
        artifact_result = self.authority.publish(
            publication,
            placement_policy,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        current = self.job_store.snapshot(publication.job_id)
        mutation = self.job_store.commit_result(
            publication.job_id,
            coordinator_id=self.coordinator_node_id,
            owner_provider_id=publication.provider_id,
            attempt_id=publication.attempt_id,
            lease_id=publication.lease_id,
            lease_generation=publication.lease_generation,
            command_id=command_id,
            expected_revision=current.revision,
            reference=self._reference(publication),
            now=now,
        )
        return PublishedJobResult(
            artifact_publication=artifact_result,
            result_mutation=mutation,
        )

    @staticmethod
    def _reference(publication: ArtifactPublication) -> ArtifactReference:
        descriptor = publication.descriptor
        return ArtifactReference(
            reference_id=descriptor.artifact_id,
            session_id=descriptor.session_id,
            schema_name=descriptor.schema_id,
            media_type=descriptor.media_type,
            content_hash=descriptor.content_hash,
            size_bytes=descriptor.size_bytes,
        )

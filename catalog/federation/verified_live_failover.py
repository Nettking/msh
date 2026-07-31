"""Production policy for failover-time replica verification."""

from __future__ import annotations

from typing import Any

from .errors import FederationValidationError
from .live_failover import StorageFailoverCoordinator
from .manifest import AuthoritativeStorageManifest
from .reporting import StorageReplicaAssessment


class VerifiedStorageFailoverCoordinator(StorageFailoverCoordinator):
    """Always obtain new integrity evidence for the current outage observation.

    An older accepted report may still match the authoritative manifest while
    the replica has suffered later corruption.  Automatic promotion therefore
    never reuses the old report as the decision input.  Its revision is used
    only to allocate the next monotonic report revision.
    """

    async def _candidate_assessments(
        self,
        observation_id: str,
        provider_ids: tuple[str, ...],
        providers: dict[str, Any],
        status: dict[str, Any],
        manifest: AuthoritativeStorageManifest,
    ) -> tuple[StorageReplicaAssessment, ...]:
        assessments: list[StorageReplicaAssessment] = []
        for provider_id in sorted(provider_ids):
            provider = providers.get(provider_id)
            if (
                provider is None
                or not provider.assignable
                or not self._provider_online(
                    provider_id,
                    provider.node_id,
                    status,
                )
            ):
                continue
            previous = self.control_plane.latest_storage_replica_assessment(
                self.session_id,
                manifest.group_id,
                provider_id,
            )
            revision = 1 if previous is None else previous.report_revision + 1
            try:
                report = await self.channel.request_replica_report(
                    failover_id=observation_id,
                    manifest=manifest,
                    provider_id=provider_id,
                    node_id=provider.node_id,
                    report_revision=revision,
                    reported_at=self.clock(),
                )
                assessment = self.control_plane.submit_storage_replica_report(
                    report,
                    actor_node_id=provider.node_id,
                )
            except (FederationValidationError, TimeoutError):
                continue
            assessments.append(assessment)
        return tuple(assessments)

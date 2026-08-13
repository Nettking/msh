"""Federation scheduling for analysis jobs.

This is the smallest orchestration layer that can sit on top of the existing
capability primitives. It owns no policy of its own: eligibility and ranking come
from :mod:`catalog.capabilities.provider_selection`, ownership from the durable
job store, authorization from the artifact authority, and delivery from the
existing dispatch contracts.

Discovering data never implies executing it. The node that holds the JSONL is the
data owner; it becomes the worker only when it advertises the capability and the
same selection pass picks it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

from ..artifact_contracts import ArtifactGrantScope, ArtifactInputReference
from ..dispatch import DispatchTransport
from ..job_store import DurableJobSnapshot
from ..jobs import AttemptStatus, JobContract, JobStatus
from ..lifecycle_contracts import LifecycleAction
from ..lifecycle_coordinator import (
    JobLifecycleCoordinator,
    ResilientDispatchCoordinator,
)
from ..lifecycle_store import SQLiteJobLifecycleStore
from ..provider_reports import ProviderResourceReport, ProviderSelectionPolicy
from ..provider_selection import ProviderSelection, select_provider
from .content_store import ContentIdentity
from .contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_GRANT_SECONDS,
    ANALYSIS_LEASE_SECONDS,
    ANALYSIS_RETRYABLE_ERROR_CODES,
    AnalysisWorkSlice,
    analysis_dispatch_id,
    analysis_grant_id,
    build_analysis_job,
    plan_artifact_id,
    slice_artifact_id,
)
from .gateway import AnalysisArtifactGateway
from .packaging import write_slice_archive

DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 15 * 60

DECISION_DISPATCHED = "dispatched"
DECISION_NO_PROVIDER = "no-eligible-provider"
DECISION_TERMINAL = "already-terminal"
DECISION_WAITING = "waiting"
DECISION_DISPATCH_FAILED = "dispatch-failed"


class ProviderReportSource(Protocol):
    """Current, short-lived reports for providers in one federation session."""

    def reports(
        self,
        *,
        session_id: str,
        capability_type: str,
    ) -> tuple[ProviderResourceReport, ...]: ...


@dataclass(frozen=True)
class SubmissionOutcome:
    """Result of submitting one logical unit of analysis work."""

    job_id: str
    created: bool
    status: JobStatus
    idempotency_key: str


@dataclass(frozen=True)
class SchedulingOutcome:
    """Result of one scheduling pass for one durable job."""

    job_id: str
    decision: str
    reason: str
    provider_id: str | None = None
    node_id: str | None = None
    attempt_id: str | None = None
    selection: ProviderSelection | None = None
    snapshot: DurableJobSnapshot | None = None


class FederatedAnalysisScheduler:
    """Submit, own, authorize, and dispatch durable analysis jobs."""

    def __init__(
        self,
        *,
        store: SQLiteJobLifecycleStore,
        gateway: AnalysisArtifactGateway,
        transport: DispatchTransport,
        report_source: ProviderReportSource,
        coordinator_node_id: str,
        clock,
        selection_policy: ProviderSelectionPolicy | None = None,
        lease_seconds: int = ANALYSIS_LEASE_SECONDS,
        grant_seconds: int = ANALYSIS_GRANT_SECONDS,
        heartbeat_timeout_seconds: int = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
    ) -> None:
        self.store = store
        self.gateway = gateway
        self.report_source = report_source
        self.coordinator_node_id = coordinator_node_id
        self.clock = clock
        self.selection_policy = selection_policy or ProviderSelectionPolicy()
        self.lease_seconds = int(lease_seconds)
        self.grant_seconds = int(grant_seconds)
        self.dispatcher = ResilientDispatchCoordinator(
            store,
            transport,  # type: ignore[arg-type]
            coordinator_node_id=coordinator_node_id,
        )
        self.lifecycle = JobLifecycleCoordinator(
            store,
            coordinator_node_id=coordinator_node_id,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        )

    # ------------------------------------------------------------------
    # Data owner: publish inputs, then submit the durable job
    # ------------------------------------------------------------------

    def submit(
        self,
        work: AnalysisWorkSlice,
        *,
        slice_files: Sequence[Path],
        slice_root: Path,
    ) -> SubmissionOutcome:
        """Register the input artifacts and submit/queue the durable job.

        Submission is idempotent: the same logical work produces the same job
        identity, so repeated discovery recognizes the existing job instead of
        creating another one.
        """

        now = self.clock()
        prefix = work.object_key_prefix
        plan_key = f"{prefix}/plan.json"
        slice_key = f"{prefix}/slice.tar.gz"

        plan_identity = self.gateway.content_store.write_bytes(
            plan_key, work.plan_bytes()
        )
        slice_identity = self._ensure_slice_archive(
            slice_key, files=slice_files, root=slice_root
        )

        job = build_analysis_job(
            work,
            plan_hash=plan_identity.content_hash,
            plan_size=plan_identity.size_bytes,
            slice_hash=slice_identity.content_hash,
            slice_size=slice_identity.size_bytes,
        )
        existed = self._job_exists(job.job_id)
        self.store.submit(job, coordinator_id=self.coordinator_node_id, now=now)

        self.gateway.register_input(
            artifact_id=plan_artifact_id(work),
            session_id=work.session_id,
            job_id=work.job_id,
            schema_id=job.inputs[0].schema_name,
            media_type=job.inputs[0].media_type,
            object_key=plan_key,
            identity=plan_identity,
            authority_node_id=self.coordinator_node_id,
            now=now,
        )
        self.gateway.register_input(
            artifact_id=slice_artifact_id(work),
            session_id=work.session_id,
            job_id=work.job_id,
            schema_id=job.inputs[1].schema_name,
            media_type=job.inputs[1].media_type,
            object_key=slice_key,
            identity=slice_identity,
            authority_node_id=self.coordinator_node_id,
            now=now,
        )

        snapshot = self.store.snapshot(work.job_id)
        if snapshot.job.status is JobStatus.SUBMITTED:
            snapshot = self.store.queue(
                work.job_id,
                coordinator_id=self.coordinator_node_id,
                command_id=f"{work.job_id}:queue",
                expected_revision=snapshot.revision,
                now=now,
            ).snapshot
        return SubmissionOutcome(
            job_id=work.job_id,
            created=not existed,
            status=snapshot.job.status,
            idempotency_key=work.idempotency_key,
        )

    def _job_exists(self, job_id: str) -> bool:
        try:
            self.store.snapshot(job_id)
        except FederationValidationError:
            return False
        return True

    def _ensure_slice_archive(
        self,
        object_key: str,
        *,
        files: Sequence[Path],
        root: Path,
    ) -> ContentIdentity:
        store = self.gateway.content_store
        destination = store.resolve(object_key)
        if not destination.is_file():
            write_slice_archive(
                destination,
                files=list(files),
                root=root,
                max_bytes=store.max_bytes,
            )
        return store.identity(object_key)

    # ------------------------------------------------------------------
    # Coordinator: select, own, authorize, dispatch
    # ------------------------------------------------------------------

    def evaluate(self, job_id: str) -> LifecycleAction:
        """Apply the existing timeout/loss lifecycle rules to one job."""

        decision = self.lifecycle.evaluate(
            job_id,
            now=self.clock(),
            command_prefix=f"{job_id}:lifecycle",
        )
        return decision.action

    async def schedule(self, job_id: str) -> SchedulingOutcome:
        """Run one scheduling pass for one durable job."""

        self.evaluate(job_id)
        snapshot = self.store.snapshot(job_id)
        if snapshot.job.terminal:
            return SchedulingOutcome(
                job_id,
                DECISION_TERMINAL,
                snapshot.job.status.value,
                snapshot=snapshot,
            )
        if snapshot.ownership is not None:
            # A crash between claim and dispatch leaves durable ownership behind.
            # Re-dispatching the same attempt is suppressed by the worker inbox.
            return await self._dispatch(snapshot)
        if snapshot.job.status is JobStatus.RETRY_WAIT:
            return await self._reassign(snapshot)
        if snapshot.job.status is not JobStatus.QUEUED:
            return SchedulingOutcome(
                job_id,
                DECISION_WAITING,
                f"job-status-{snapshot.job.status.value}",
                snapshot=snapshot,
            )
        return await self._claim_and_dispatch(snapshot)

    def _reports(self, job: JobContract) -> tuple[ProviderResourceReport, ...]:
        return tuple(
            self.report_source.reports(
                session_id=job.session_id,
                capability_type=ANALYSIS_CAPABILITY_TYPE,
            )
        )

    async def _claim_and_dispatch(
        self, snapshot: DurableJobSnapshot
    ) -> SchedulingOutcome:
        job_id = snapshot.job.job_id
        now = self.clock()
        selection = select_provider(
            snapshot.job,
            self._reports(snapshot.job),
            evaluated_at=now,
            policy=self.selection_policy,
        )
        if selection.selected_capability_id is None or selection.selected_node_id is None:
            # Explicit and safe: the job stays queued and visible. There is no
            # undocumented local fallback.
            return SchedulingOutcome(
                job_id,
                DECISION_NO_PROVIDER,
                selection.reason,
                selection=selection,
                snapshot=snapshot,
            )
        generation = snapshot.attempt_generation + 1
        attempt_id = f"{job_id}-attempt-{generation}"
        lease_id = f"{job_id}-lease-{generation}"
        claimed = self.store.claim(
            job_id,
            coordinator_id=self.coordinator_node_id,
            owner_provider_id=selection.selected_capability_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            command_id=f"{job_id}:claim:{generation}",
            expected_revision=snapshot.revision,
            lease_expires_at=now + timedelta(seconds=self.lease_seconds),
            now=now,
        ).snapshot
        return await self._dispatch(
            claimed,
            target_node_id=selection.selected_node_id,
            selection=selection,
        )

    async def _reassign(self, snapshot: DurableJobSnapshot) -> SchedulingOutcome:
        job_id = snapshot.job.job_id
        now = self.clock()
        generation = snapshot.attempt_generation + 1
        try:
            outcome = self.lifecycle.reassign(
                job_id,
                reports=self._reports(snapshot.job),
                policy=self.selection_policy,
                attempt_id=f"{job_id}-attempt-{generation}",
                lease_id=f"{job_id}-lease-{generation}",
                lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                command_prefix=f"{job_id}:retry:{generation}",
                now=now,
            )
        except (FederationValidationError, FederationOperationError) as exc:
            return SchedulingOutcome(
                job_id,
                DECISION_WAITING,
                getattr(exc, "code", "retry-not-ready"),
                snapshot=snapshot,
            )
        return await self._dispatch(
            outcome.snapshot,
            target_node_id=outcome.target_node_id,
            selection=outcome.selection,
        )

    def _grant_input_access(
        self,
        snapshot: DurableJobSnapshot,
        *,
        worker_node_id: str,
        now: datetime,
    ) -> str:
        """Issue the narrowest grant that lets the selected worker read inputs."""

        ownership = snapshot.ownership
        assert ownership is not None  # callers dispatch only owned jobs
        grant_id = analysis_grant_id(snapshot.job.job_id, ownership.attempt_id)
        references = tuple(
            ArtifactInputReference(
                artifact_id=reference.reference_id,
                session_id=reference.session_id,
                job_id=snapshot.job.job_id,
                content_hash=reference.content_hash,
                schema_id=reference.schema_name,
                endpoint_id=self.gateway.endpoint_id,
            )
            for reference in snapshot.job.inputs
        )
        expires_at = min(
            ownership.lease_expires_at,
            now + timedelta(seconds=self.grant_seconds),
        )
        self.gateway.authority.issue_grant(
            snapshot.job.job_id,
            coordinator_node_id=self.coordinator_node_id,
            grant_id=grant_id,
            worker_node_id=worker_node_id,
            endpoint_id=self.gateway.endpoint_id,
            scopes=(ArtifactGrantScope.READ_INPUT,),
            input_references=references,
            expires_at=expires_at,
            now=now,
        )
        return grant_id

    async def _dispatch(
        self,
        snapshot: DurableJobSnapshot,
        *,
        target_node_id: str | None = None,
        selection: ProviderSelection | None = None,
    ) -> SchedulingOutcome:
        ownership = snapshot.ownership
        job_id = snapshot.job.job_id
        if ownership is None:
            return SchedulingOutcome(
                job_id, DECISION_WAITING, "job-not-owned", snapshot=snapshot
            )
        node_id = target_node_id or self._owner_node_id(snapshot)
        if node_id is None:
            return SchedulingOutcome(
                job_id,
                DECISION_NO_PROVIDER,
                "owner-node-unknown",
                provider_id=ownership.owner_provider_id,
                attempt_id=ownership.attempt_id,
                snapshot=snapshot,
            )
        now = self.clock()
        try:
            self._grant_input_access(snapshot, worker_node_id=node_id, now=now)
            outcome = await self.dispatcher.dispatch(
                job_id,
                dispatch_id=analysis_dispatch_id(job_id, ownership.attempt_id),
                target_node_id=node_id,
                now=now,
            )
        except (
            FederationValidationError,
            FederationOperationError,
            ProtocolCompatibilityError,
            OSError,
            TimeoutError,
        ) as exc:
            return self._record_dispatch_failure(
                job_id, reason=getattr(exc, "code", "capability-route-failed")
            )
        return SchedulingOutcome(
            job_id,
            DECISION_DISPATCHED,
            outcome.disposition.value,
            provider_id=ownership.owner_provider_id,
            node_id=node_id,
            attempt_id=ownership.attempt_id,
            selection=selection,
            snapshot=outcome.snapshot,
        )

    def _owner_node_id(self, snapshot: DurableJobSnapshot) -> str | None:
        ownership = snapshot.ownership
        if ownership is None:
            return None
        for report in self._reports(snapshot.job):
            if report.capability_id == ownership.owner_provider_id:
                return report.node_id
        return None

    def _record_dispatch_failure(self, job_id: str, *, reason: str) -> SchedulingOutcome:
        """Release a failed delivery through the existing retry semantics."""

        snapshot = self.store.snapshot(job_id)
        ownership = snapshot.ownership
        if ownership is None or snapshot.job.terminal:
            return SchedulingOutcome(
                job_id, DECISION_DISPATCH_FAILED, reason, snapshot=snapshot
            )
        now = self.clock()
        try:
            mutation = self.store.schedule_retry(
                job_id,
                coordinator_id=self.coordinator_node_id,
                owner_provider_id=ownership.owner_provider_id,
                attempt_id=ownership.attempt_id,
                lease_id=ownership.lease_id,
                lease_generation=ownership.lease_generation,
                command_id=f"{job_id}:dispatch-failed:{ownership.attempt_number}",
                expected_revision=snapshot.revision,
                terminal_status=AttemptStatus.LOST,
                error_code=(
                    reason
                    if reason in ANALYSIS_RETRYABLE_ERROR_CODES
                    else "capability-route-failed"
                ),
                now=now,
            )
        except (FederationValidationError, FederationOperationError):
            return SchedulingOutcome(
                job_id, DECISION_DISPATCH_FAILED, reason, snapshot=snapshot
            )
        return SchedulingOutcome(
            job_id,
            DECISION_DISPATCH_FAILED,
            reason,
            provider_id=ownership.owner_provider_id,
            attempt_id=ownership.attempt_id,
            snapshot=mutation.snapshot,
        )


__all__ = [
    "DECISION_DISPATCHED",
    "DECISION_DISPATCH_FAILED",
    "DECISION_NO_PROVIDER",
    "DECISION_TERMINAL",
    "DECISION_WAITING",
    "FederatedAnalysisScheduler",
    "ProviderReportSource",
    "SchedulingOutcome",
    "SubmissionOutcome",
]

"""Coordinator-owned F7.5 timeout, retry, reassignment, and completion logic."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Protocol

from catalog.federation.errors import FederationValidationError

from .dispatch import (
    DispatchEvent,
    DispatchRequest,
    DispatchResponse,
    DispatchState,
    DispatchTransport,
)
from .job_store import DurableJobSnapshot, _text, _utc
from .jobs import ArtifactReference, AttemptStatus, JobAttempt, JobStatus
from .lifecycle_contracts import (
    CancellationRequest,
    CancellationResponse,
    CancellationState,
    CompletionDisposition,
    JobHeartbeat,
    LifecycleAction,
    MAX_HEARTBEAT_TIMEOUT_SECONDS,
    ResultCommit,
    RetryState,
    _positive,
)
from .lifecycle_store import SQLiteJobLifecycleStore
from .provider_reports import ProviderResourceReport, ProviderSelectionPolicy
from .provider_selection import ProviderSelection, select_provider


@dataclass(frozen=True)
class ReassignmentOutcome:
    selection: ProviderSelection
    snapshot: DurableJobSnapshot
    target_node_id: str


@dataclass(frozen=True)
class CompletionOutcome:
    disposition: CompletionDisposition
    snapshot: DurableJobSnapshot
    result: ResultCommit | None = None
    retry: RetryState | None = None


@dataclass(frozen=True)
class LifecycleDecision:
    action: LifecycleAction
    reason_code: str | None
    snapshot: DurableJobSnapshot
    retry: RetryState | None = None


class LifecycleTransport(DispatchTransport, Protocol):
    async def cancel(
        self,
        *,
        target_node_id: str,
        request: CancellationRequest,
    ) -> CancellationResponse: ...


class ResilientDispatchCoordinator:
    """Apply at-least-once worker messages through the F7.3/F7.5 fence."""

    def __init__(
        self,
        store: SQLiteJobLifecycleStore,
        transport: LifecycleTransport,
        *,
        coordinator_node_id: str,
    ) -> None:
        self.store = store
        self.transport = transport
        self.coordinator_node_id = _text(
            coordinator_node_id, "coordinator_node_id"
        )

    @staticmethod
    def _attempt(
        snapshot: DurableJobSnapshot,
        attempt_id: str,
    ) -> JobAttempt:
        for attempt in snapshot.job.attempts:
            if attempt.attempt_id == attempt_id:
                return attempt
        raise FederationValidationError(
            "attempt-not-found", "attempt_id", "attempt is missing"
        )

    @staticmethod
    def _reference(event: DispatchEvent) -> ArtifactReference:
        details = event.details or {}
        value = details.get("result_reference")
        if not isinstance(value, dict):
            raise FederationValidationError(
                "missing-result-reference",
                "details.result_reference",
                "successful completion requires one result reference",
            )
        return ArtifactReference.from_dict(value)

    async def dispatch(
        self,
        job_id: str,
        *,
        dispatch_id: str,
        target_node_id: str,
        now: datetime,
    ) -> CompletionOutcome:
        current = self.store.snapshot(job_id)
        ownership = current.ownership
        now = _utc(now, "now")
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned", "job_id", "job has no owner"
            )
        if ownership.granted_by_coordinator_id != self.coordinator_node_id:
            raise FederationValidationError(
                "dispatch-coordinator-mismatch",
                "coordinator_node_id",
                "only the granting coordinator may dispatch",
            )
        if now >= ownership.lease_expires_at:
            raise FederationValidationError(
                "ownership-lease-expired",
                "lease_expires_at",
                "ownership has expired",
            )
        request = DispatchRequest.from_snapshot(
            current,
            dispatch_id=dispatch_id,
            target_node_id=target_node_id,
            sent_at=now,
        )
        response = (
            await self.transport.request(
                target_node_id=target_node_id,
                request=request,
            )
        ).validate_for(request)
        return self.apply_response(request, response)

    def apply_response(
        self,
        request: DispatchRequest,
        response: DispatchResponse,
    ) -> CompletionOutcome:
        response.validate_for(request)
        current = self.store.snapshot(request.job.job_id)
        committed = self.store.result_commit(request.job.job_id)
        if current.job.status is JobStatus.CANCELLED:
            return CompletionOutcome(
                CompletionDisposition.CANCELLED_IGNORED,
                current,
                result=committed,
            )
        if current.job.terminal:
            same_committed_attempt = (
                committed is not None
                and committed.attempt_id == request.attempt_id
                and committed.provider_id == request.provider_id
                and committed.lease_id == request.lease_id
                and committed.lease_generation == request.lease_generation
            )
            if same_committed_attempt and response.state is DispatchState.SUCCEEDED:
                try:
                    reference = self._reference(response.events[-1])
                except FederationValidationError:
                    reference = None
                if reference == committed.reference:
                    return CompletionOutcome(
                        CompletionDisposition.DUPLICATE,
                        current,
                        result=committed,
                    )
            return CompletionOutcome(
                CompletionDisposition.STALE_IGNORED,
                current,
                result=committed,
            )
        ownership = current.ownership
        if (
            ownership is None
            or ownership.attempt_id != request.attempt_id
            or ownership.owner_provider_id != request.provider_id
            or ownership.lease_id != request.lease_id
            or ownership.lease_generation != request.lease_generation
        ):
            return CompletionOutcome(
                CompletionDisposition.STALE_IGNORED,
                current,
                result=committed,
            )

        for event in response.events:
            attempt = self._attempt(current, request.attempt_id)
            if event.state is DispatchState.ACCEPTED:
                if attempt.status in {
                    AttemptStatus.ACCEPTED,
                    AttemptStatus.RUNNING,
                    AttemptStatus.CANCELLATION_REQUESTED,
                }:
                    continue
                current = self.store.record_attempt_status(
                    request.job.job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{request.dispatch_id}:accepted",
                    expected_revision=current.revision,
                    target_status=AttemptStatus.ACCEPTED,
                    now=event.occurred_at,
                ).snapshot
            elif event.state is DispatchState.RUNNING:
                if attempt.status in {
                    AttemptStatus.RUNNING,
                    AttemptStatus.CANCELLATION_REQUESTED,
                }:
                    continue
                current = self.store.record_attempt_status(
                    request.job.job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    command_id=f"{request.dispatch_id}:running",
                    expected_revision=current.revision,
                    target_status=AttemptStatus.RUNNING,
                    now=event.occurred_at,
                ).snapshot
            elif event.state is DispatchState.SUCCEEDED:
                if current.job.status is JobStatus.CANCELLATION_REQUESTED:
                    return CompletionOutcome(
                        CompletionDisposition.CANCELLED_IGNORED,
                        current,
                    )
                mutation = self.store.commit_result(
                    request.job.job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    lease_generation=request.lease_generation,
                    command_id=f"{request.dispatch_id}:result",
                    expected_revision=current.revision,
                    reference=self._reference(event),
                    now=event.occurred_at,
                )
                return CompletionOutcome(
                    (
                        CompletionDisposition.APPLIED
                        if mutation.changed
                        else CompletionDisposition.DUPLICATE
                    ),
                    mutation.snapshot,
                    result=mutation.result,
                )
            elif event.state in {DispatchState.FAILED, DispatchState.REJECTED}:
                reason = event.reason_code or "worker-rejected"
                if (
                    current.job.status is JobStatus.CANCELLATION_REQUESTED
                    or reason in {"worker-cancelled", "cancelled-before-start"}
                ):
                    return CompletionOutcome(
                        CompletionDisposition.CANCELLED_IGNORED,
                        current,
                    )
                mutation = self.store.schedule_retry(
                    request.job.job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=request.provider_id,
                    attempt_id=request.attempt_id,
                    lease_id=request.lease_id,
                    lease_generation=request.lease_generation,
                    command_id=f"{request.dispatch_id}:terminal",
                    expected_revision=current.revision,
                    terminal_status=AttemptStatus.FAILED,
                    error_code=reason,
                    now=event.occurred_at,
                )
                return CompletionOutcome(
                    (
                        CompletionDisposition.RETRY_SCHEDULED
                        if mutation.retry is not None
                        else CompletionDisposition.FINAL_FAILURE
                    ),
                    mutation.snapshot,
                    retry=mutation.retry,
                )
        return CompletionOutcome(
            CompletionDisposition.APPLIED,
            current,
            result=committed,
        )

    async def cancel(
        self,
        job_id: str,
        *,
        cancellation_id: str,
        target_node_id: str | None,
        reason: str,
        now: datetime,
    ) -> DurableJobSnapshot:
        current = self.store.snapshot(job_id)
        if current.job.status is JobStatus.CANCELLED:
            return current
        ownership = current.ownership
        request = (
            None
            if ownership is None
            else CancellationRequest.from_snapshot(
                current,
                cancellation_id=cancellation_id,
                target_node_id=_text(target_node_id, "target_node_id"),
                requested_at=now,
                reason=reason,
            )
        )
        requested = self.store.request_cancellation(
            job_id,
            coordinator_id=self.coordinator_node_id,
            cancellation_id=cancellation_id,
            command_id=f"{cancellation_id}:request",
            expected_revision=current.revision,
            reason=reason,
            now=now,
        ).snapshot
        if request is None:
            return requested
        try:
            response = (
                await self.transport.cancel(
                    target_node_id=request.target_node_id,
                    request=request,
                )
            ).validate_for(request)
        except (OSError, TimeoutError):
            return requested
        if response.state is CancellationState.REJECTED:
            return requested
        return self.store.finalize_cancellation(
            job_id,
            coordinator_id=self.coordinator_node_id,
            owner_provider_id=request.provider_id,
            attempt_id=request.attempt_id,
            lease_id=request.lease_id,
            lease_generation=request.lease_generation,
            command_id=f"{cancellation_id}:complete",
            expected_revision=requested.revision,
            now=response.occurred_at,
        ).snapshot


class JobLifecycleCoordinator:
    """Deterministic coordinator decisions for timeout, loss, and reassignment."""

    def __init__(
        self,
        store: SQLiteJobLifecycleStore,
        *,
        coordinator_node_id: str,
        heartbeat_timeout_seconds: int,
    ) -> None:
        self.store = store
        self.coordinator_node_id = _text(
            coordinator_node_id, "coordinator_node_id"
        )
        self.heartbeat_timeout_seconds = _positive(
            heartbeat_timeout_seconds,
            "heartbeat_timeout_seconds",
            maximum=MAX_HEARTBEAT_TIMEOUT_SECONDS,
        )

    @staticmethod
    def _attempt(
        snapshot: DurableJobSnapshot,
        attempt_id: str,
    ) -> JobAttempt:
        for attempt in snapshot.job.attempts:
            if attempt.attempt_id == attempt_id:
                return attempt
        raise FederationValidationError(
            "attempt-not-found", "attempt_id", "attempt is missing"
        )

    def record_heartbeat(
        self,
        heartbeat: JobHeartbeat,
        *,
        authenticated_worker_node_id: str,
        authenticated_session_id: str,
    ) -> bool:
        return self.store.record_heartbeat(
            heartbeat,
            coordinator_id=self.coordinator_node_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            authenticated_session_id=authenticated_session_id,
        )

    def evaluate(
        self,
        job_id: str,
        *,
        now: datetime,
        command_prefix: str,
    ) -> LifecycleDecision:
        now = _utc(now, "now")
        command_prefix = _text(command_prefix, "command_prefix")
        snapshot = self.store.snapshot(job_id)
        if snapshot.job.terminal:
            return LifecycleDecision(LifecycleAction.NONE, None, snapshot)
        audit = self.store.audit_trail(job_id)
        submitted_at = audit[0].event_at if audit else snapshot.updated_at
        policy = snapshot.job.timeout_policy
        if now >= submitted_at + timedelta(
            seconds=policy.overall_timeout_seconds
        ):
            if snapshot.ownership is None:
                updated = self.store.timeout_unowned(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    command_id=f"{command_prefix}:overall-timeout",
                    expected_revision=snapshot.revision,
                    reason_code="overall-timeout",
                    now=now,
                ).snapshot
            else:
                ownership = snapshot.ownership
                updated = self.store.timeout_active_final(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    owner_provider_id=ownership.owner_provider_id,
                    attempt_id=ownership.attempt_id,
                    lease_id=ownership.lease_id,
                    lease_generation=ownership.lease_generation,
                    command_id=f"{command_prefix}:overall-timeout",
                    expected_revision=snapshot.revision,
                    error_code="overall-timeout",
                    now=now,
                ).snapshot
            return LifecycleDecision(
                LifecycleAction.FINAL_TIMEOUT,
                "overall-timeout",
                updated,
            )

        if snapshot.job.status is JobStatus.CANCELLATION_REQUESTED:
            deadline = snapshot.updated_at + timedelta(
                seconds=policy.cancellation_grace_seconds
            )
            if now >= deadline:
                updated = self.store.force_cancellation(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    command_id=f"{command_prefix}:force-cancel",
                    expected_revision=snapshot.revision,
                    now=now,
                ).snapshot
                return LifecycleDecision(
                    LifecycleAction.CANCELLATION_FORCED,
                    "cancellation-grace-expired",
                    updated,
                )
            return LifecycleDecision(LifecycleAction.NONE, None, snapshot)

        if snapshot.ownership is None:
            if snapshot.job.status in {
                JobStatus.SUBMITTED,
                JobStatus.QUEUED,
            } and now >= snapshot.updated_at + timedelta(
                seconds=policy.queue_timeout_seconds
            ):
                updated = self.store.timeout_unowned(
                    job_id,
                    coordinator_id=self.coordinator_node_id,
                    command_id=f"{command_prefix}:queue-timeout",
                    expected_revision=snapshot.revision,
                    reason_code="queue-timeout",
                    now=now,
                ).snapshot
                return LifecycleDecision(
                    LifecycleAction.FINAL_TIMEOUT,
                    "queue-timeout",
                    updated,
                )
            return LifecycleDecision(
                LifecycleAction.NONE,
                None,
                snapshot,
                retry=self.store.retry_state(job_id),
            )

        ownership = snapshot.ownership
        reason: str | None = None
        terminal = AttemptStatus.LOST
        if now >= ownership.lease_expires_at:
            reason = "lease-expired"
        else:
            heartbeat_at = self.store.heartbeat_at(job_id)
            heartbeat_base = snapshot.updated_at if heartbeat_at is None else heartbeat_at
            if now >= heartbeat_base + timedelta(
                seconds=self.heartbeat_timeout_seconds
            ):
                reason = "worker-heartbeat-lost"
            else:
                attempt = self._attempt(snapshot, ownership.attempt_id)
                if attempt.status in {
                    AttemptStatus.ASSIGNED,
                    AttemptStatus.ACCEPTED,
                } and now >= snapshot.updated_at + timedelta(
                    seconds=policy.start_timeout_seconds
                ):
                    reason = "start-timeout"
                    terminal = AttemptStatus.TIMED_OUT
                elif (
                    attempt.status is AttemptStatus.RUNNING
                    and now >= snapshot.updated_at + timedelta(
                        seconds=policy.run_timeout_seconds
                    )
                ):
                    reason = "run-timeout"
                    terminal = AttemptStatus.TIMED_OUT
        if reason is None:
            return LifecycleDecision(LifecycleAction.NONE, None, snapshot)
        mutation = self.store.schedule_retry(
            job_id,
            coordinator_id=self.coordinator_node_id,
            owner_provider_id=ownership.owner_provider_id,
            attempt_id=ownership.attempt_id,
            lease_id=ownership.lease_id,
            lease_generation=ownership.lease_generation,
            command_id=f"{command_prefix}:{reason}",
            expected_revision=snapshot.revision,
            terminal_status=terminal,
            error_code=reason,
            now=now,
        )
        if mutation.retry is not None:
            action = LifecycleAction.RETRY_SCHEDULED
        elif terminal is AttemptStatus.TIMED_OUT:
            action = LifecycleAction.FINAL_TIMEOUT
        else:
            action = LifecycleAction.FINAL_FAILURE
        return LifecycleDecision(
            action,
            reason,
            mutation.snapshot,
            retry=mutation.retry,
        )

    def reassign(
        self,
        job_id: str,
        *,
        reports: tuple[ProviderResourceReport, ...],
        policy: ProviderSelectionPolicy | None,
        attempt_id: str,
        lease_id: str,
        lease_expires_at: datetime,
        command_prefix: str,
        now: datetime,
    ) -> ReassignmentOutcome:
        snapshot = self.store.snapshot(job_id)
        retry = self.store.retry_state(job_id)
        if snapshot.job.status is not JobStatus.RETRY_WAIT or retry is None:
            raise FederationValidationError(
                "job-not-waiting-for-retry",
                "status",
                "job has no retry to reassign",
            )
        base_policy = policy or ProviderSelectionPolicy()
        excluded = tuple(
            sorted(
                set(base_policy.excluded_capability_ids)
                | {retry.source_provider_id}
            )
        )
        selection = select_provider(
            snapshot.job,
            reports,
            evaluated_at=now,
            policy=replace(
                base_policy,
                excluded_capability_ids=excluded,
            ),
        )
        if (
            selection.selected_capability_id is None
            or selection.selected_node_id is None
        ):
            raise FederationValidationError(
                "no-eligible-reassignment-provider",
                "reports",
                "no new qualified worker is available",
            )
        queued = self.store.activate_retry(
            job_id,
            coordinator_id=self.coordinator_node_id,
            command_id=f"{command_prefix}:activate",
            expected_revision=snapshot.revision,
            now=now,
        ).snapshot
        claimed = self.store.claim(
            job_id,
            coordinator_id=self.coordinator_node_id,
            owner_provider_id=selection.selected_capability_id,
            attempt_id=attempt_id,
            lease_id=lease_id,
            command_id=f"{command_prefix}:claim",
            expected_revision=queued.revision,
            lease_expires_at=lease_expires_at,
            now=now,
        ).snapshot
        return ReassignmentOutcome(
            selection=selection,
            snapshot=claimed,
            target_node_id=selection.selected_node_id,
        )

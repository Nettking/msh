"""E7.3-E8 verification, eligibility completion, and diagnostics.

This module completes a relay-repaired former-primary recovery without granting
leadership.  The provider remains a fenced replica and becomes selectable only
after exact local verification and an authenticated coordinator-eligible report.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .errors import FederationValidationError
from .former_primary_recovery import (
    FormerPrimaryItemStatus,
    FormerPrimaryRecoveryPlan,
    FormerPrimaryRecoveryPlanner,
    FormerPrimaryRecoveryState,
)
from .former_primary_repair import (
    FormerPrimaryRepairProgressStore,
    FormerPrimaryRepairWorker,
)
from .local_storage import BatchStorageProvider
from .reporting import StorageReplicaAssessment, StorageReplicaReport

FORMER_PRIMARY_COMPLETION_SCHEMA = "msh.storage_former_primary_completion.v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                "timestamp",
                "must be RFC 3339",
            ) from exc
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp",
            "timestamp",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-id",
            field,
            "must be non-empty opaque text",
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


@dataclass(frozen=True)
class FormerPrimaryRecoveryCompletion:
    schema: str
    recovery_id: str
    final_report_revision: int
    final_report_hash: str
    assessment_accepted: bool
    assessment_eligibility: bool
    eligibility_reason: str
    assessed_at: datetime
    completed_at: datetime

    def __post_init__(self) -> None:
        if self.schema != FORMER_PRIMARY_COMPLETION_SCHEMA:
            raise FederationValidationError(
                "unsupported-schema",
                "schema",
                f"expected {FORMER_PRIMARY_COMPLETION_SCHEMA}",
            )
        object.__setattr__(self, "recovery_id", _text(self.recovery_id, "recovery_id"))
        object.__setattr__(
            self,
            "final_report_revision",
            _uint(self.final_report_revision, "final_report_revision"),
        )
        object.__setattr__(
            self,
            "final_report_hash",
            _text(self.final_report_hash, "final_report_hash"),
        )
        object.__setattr__(
            self,
            "eligibility_reason",
            _text(self.eligibility_reason, "eligibility_reason"),
        )
        if not isinstance(self.assessment_accepted, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "assessment_accepted",
                "must be boolean",
            )
        if not isinstance(self.assessment_eligibility, bool):
            raise FederationValidationError(
                "invalid-boolean",
                "assessment_eligibility",
                "must be boolean",
            )
        if (
            not self.assessment_accepted
            or not self.assessment_eligibility
            or self.eligibility_reason != "eligible"
        ):
            raise FederationValidationError(
                "ineligible-recovery-completion",
                "assessment",
                "completion requires an accepted eligible assessment",
            )
        object.__setattr__(self, "assessed_at", _utc(self.assessed_at))
        object.__setattr__(self, "completed_at", _utc(self.completed_at))

    def immutable_binding(self) -> tuple[Any, ...]:
        return (
            self.schema,
            self.recovery_id,
            self.final_report_revision,
            self.final_report_hash,
            self.assessment_accepted,
            self.assessment_eligibility,
            self.eligibility_reason,
            self.assessed_at,
            self.completed_at,
        )


@dataclass(frozen=True)
class FormerPrimaryCompletionResult:
    status: str
    code: str
    reason: str
    plan: FormerPrimaryRecoveryPlan
    assessment: StorageReplicaAssessment | None = None
    completion: FormerPrimaryRecoveryCompletion | None = None


@dataclass(frozen=True)
class FormerPrimaryRecoveryDiagnostics:
    recovery_id: str
    session_id: str
    group_id: str
    returning_provider_id: str
    returning_node_id: str
    provider_role: str
    current_primary_provider_id: str | None
    state: str
    manifest_revision: int
    manifest_hash: str
    initial_report_revision: int
    initial_report_hash: str
    latest_report_revision: int | None
    latest_report_hash: str | None
    integrity_verified: bool | None
    synchronization_state: str | None
    provider_reported_eligible: bool | None
    provider_eligibility_reasons: tuple[str, ...]
    coordinator_eligibility: bool | None
    coordinator_eligibility_reason: str | None
    item_status_counts: tuple[tuple[str, int], ...]
    pending_item_ids: tuple[str, ...]
    conflict_item_ids: tuple[str, ...]
    latest_error_code: str | None
    latest_error_reason: str | None
    degraded_state: dict[str, Any] | None
    completed_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "recovery_id": self.recovery_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "returning_provider_id": self.returning_provider_id,
            "returning_node_id": self.returning_node_id,
            "provider_role": self.provider_role,
            "current_primary_provider_id": self.current_primary_provider_id,
            "state": self.state,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "initial_report_revision": self.initial_report_revision,
            "initial_report_hash": self.initial_report_hash,
            "latest_report_revision": self.latest_report_revision,
            "latest_report_hash": self.latest_report_hash,
            "integrity_verified": self.integrity_verified,
            "synchronization_state": self.synchronization_state,
            "provider_reported_eligible": self.provider_reported_eligible,
            "provider_eligibility_reasons": list(self.provider_eligibility_reasons),
            "coordinator_eligibility": self.coordinator_eligibility,
            "coordinator_eligibility_reason": self.coordinator_eligibility_reason,
            "item_status_counts": dict(self.item_status_counts),
            "pending_item_ids": list(self.pending_item_ids),
            "conflict_item_ids": list(self.conflict_item_ids),
            "latest_error_code": self.latest_error_code,
            "latest_error_reason": self.latest_error_reason,
            "degraded_state": self.degraded_state,
            "completed_at": (
                None if self.completed_at is None else _timestamp(self.completed_at)
            ),
        }


class FormerPrimaryCompletionStore(FormerPrimaryRepairProgressStore):
    """Durable verification and final-assessment evidence."""

    def __init__(self, database: Path | str) -> None:
        super().__init__(database)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_former_primary_recovery_completions (
                    recovery_id TEXT PRIMARY KEY,
                    schema TEXT NOT NULL,
                    final_report_revision INTEGER NOT NULL
                        CHECK(final_report_revision >= 0),
                    final_report_hash TEXT NOT NULL,
                    assessment_accepted INTEGER NOT NULL
                        CHECK(assessment_accepted IN (0, 1)),
                    assessment_eligibility INTEGER NOT NULL
                        CHECK(assessment_eligibility IN (0, 1)),
                    eligibility_reason TEXT NOT NULL,
                    assessed_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    FOREIGN KEY(recovery_id)
                        REFERENCES storage_former_primary_recoveries(recovery_id)
                        ON DELETE CASCADE
                );
                """
            )

    def mark_verified(
        self,
        recovery_id: str,
        *,
        now: datetime,
    ) -> FormerPrimaryRecoveryPlan:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT item_id, status
                   FROM storage_former_primary_recovery_items
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchall()
            record_row = connection.execute(
                """SELECT item_count FROM storage_former_primary_recoveries
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchone()
            if record_row is None:
                connection.rollback()
                raise FederationValidationError(
                    "unknown-former-primary-recovery",
                    "recovery_id",
                    "durable recovery plan does not exist",
                )
            if len(rows) != int(record_row["item_count"]):
                connection.rollback()
                raise FederationValidationError(
                    "corrupt-former-primary-recovery",
                    "items",
                    "recovery item ledger does not match item_count",
                )
            invalid = sorted(
                str(row["item_id"])
                for row in rows
                if row["status"]
                not in (
                    FormerPrimaryItemStatus.PRESENT.value,
                    FormerPrimaryItemStatus.DELIVERED.value,
                    FormerPrimaryItemStatus.VERIFIED.value,
                )
            )
            if invalid:
                connection.rollback()
                raise FederationValidationError(
                    "repair-verification-incomplete",
                    "items",
                    "all items must be present or delivered before verification",
                )
            connection.execute(
                """UPDATE storage_former_primary_recovery_items
                   SET status=?, last_error_code=NULL, last_error_reason=NULL,
                       updated_at=?
                   WHERE recovery_id=? AND status IN (?, ?)""",
                (
                    FormerPrimaryItemStatus.VERIFIED.value,
                    _timestamp(now),
                    recovery_id,
                    FormerPrimaryItemStatus.PRESENT.value,
                    FormerPrimaryItemStatus.DELIVERED.value,
                ),
            )
            connection.execute(
                """UPDATE storage_former_primary_recoveries
                   SET state=?, latest_error_code=NULL, latest_error_reason=NULL,
                       updated_at=?
                   WHERE recovery_id=? AND state IN (?, ?, ?, ?)""",
                (
                    FormerPrimaryRecoveryState.AWAITING_REPORT.value,
                    _timestamp(now),
                    recovery_id,
                    FormerPrimaryRecoveryState.PLANNED.value,
                    FormerPrimaryRecoveryState.TRANSFERRING.value,
                    FormerPrimaryRecoveryState.VERIFYING.value,
                    FormerPrimaryRecoveryState.RETRYABLE.value,
                ),
            )
            connection.commit()
        return self.require_plan(recovery_id)

    def record_report_wait(
        self,
        recovery_id: str,
        *,
        code: str,
        reason: str,
        now: datetime,
    ) -> FormerPrimaryRecoveryPlan:
        return self.transition_record(
            recovery_id,
            expected_states=(FormerPrimaryRecoveryState.AWAITING_REPORT,),
            state=FormerPrimaryRecoveryState.AWAITING_REPORT,
            now=now,
            error_code=code,
            error_reason=reason,
        )

    def completion(
        self,
        recovery_id: str,
    ) -> FormerPrimaryRecoveryCompletion | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_former_primary_recovery_completions
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            return FormerPrimaryRecoveryCompletion(
                schema=str(row["schema"]),
                recovery_id=str(row["recovery_id"]),
                final_report_revision=int(row["final_report_revision"]),
                final_report_hash=str(row["final_report_hash"]),
                assessment_accepted=bool(row["assessment_accepted"]),
                assessment_eligibility=bool(row["assessment_eligibility"]),
                eligibility_reason=str(row["eligibility_reason"]),
                assessed_at=str(row["assessed_at"]),
                completed_at=str(row["completed_at"]),
            )
        except (KeyError, TypeError, ValueError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-former-primary-completion",
                "storage_former_primary_recovery_completions",
                "durable completion evidence is malformed",
            ) from exc

    def complete(
        self,
        recovery_id: str,
        assessment: StorageReplicaAssessment,
        *,
        now: datetime,
    ) -> tuple[FormerPrimaryRecoveryPlan, FormerPrimaryRecoveryCompletion]:
        completion = FormerPrimaryRecoveryCompletion(
            schema=FORMER_PRIMARY_COMPLETION_SCHEMA,
            recovery_id=recovery_id,
            final_report_revision=assessment.report_revision,
            final_report_hash=assessment.report_hash,
            assessment_accepted=assessment.accepted,
            assessment_eligibility=assessment.eligibility,
            eligibility_reason=assessment.eligibility_reason,
            assessed_at=assessment.assessed_at,
            completed_at=now,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT * FROM storage_former_primary_recovery_completions
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """INSERT INTO storage_former_primary_recovery_completions
                       (recovery_id, schema, final_report_revision,
                        final_report_hash, assessment_accepted,
                        assessment_eligibility, eligibility_reason,
                        assessed_at, completed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        completion.recovery_id,
                        completion.schema,
                        completion.final_report_revision,
                        completion.final_report_hash,
                        int(completion.assessment_accepted),
                        int(completion.assessment_eligibility),
                        completion.eligibility_reason,
                        _timestamp(completion.assessed_at),
                        _timestamp(completion.completed_at),
                    ),
                )
            else:
                restored = FormerPrimaryRecoveryCompletion(
                    schema=str(existing["schema"]),
                    recovery_id=str(existing["recovery_id"]),
                    final_report_revision=int(existing["final_report_revision"]),
                    final_report_hash=str(existing["final_report_hash"]),
                    assessment_accepted=bool(existing["assessment_accepted"]),
                    assessment_eligibility=bool(existing["assessment_eligibility"]),
                    eligibility_reason=str(existing["eligibility_reason"]),
                    assessed_at=str(existing["assessed_at"]),
                    completed_at=str(existing["completed_at"]),
                )
                if restored.immutable_binding() != completion.immutable_binding():
                    connection.rollback()
                    raise FederationValidationError(
                        "conflicting-former-primary-completion",
                        "recovery_id",
                        "recovery already completed with different report evidence",
                    )
                completion = restored
            connection.execute(
                """UPDATE storage_former_primary_recoveries
                   SET state=?, latest_error_code=NULL, latest_error_reason=NULL,
                       updated_at=?, completed_at=?
                   WHERE recovery_id=? AND state IN (?, ?)""",
                (
                    FormerPrimaryRecoveryState.COMPLETED.value,
                    _timestamp(completion.completed_at),
                    _timestamp(completion.completed_at),
                    recovery_id,
                    FormerPrimaryRecoveryState.AWAITING_REPORT.value,
                    FormerPrimaryRecoveryState.COMPLETED.value,
                ),
            )
            connection.commit()
        return self.require_plan(recovery_id), completion


class FormerPrimaryCompletionWorker:
    """Verify repaired items and durably gate final replica eligibility."""

    def __init__(
        self,
        *,
        planner: FormerPrimaryRecoveryPlanner,
        returning_provider: BatchStorageProvider,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.planner = planner
        self.control = planner.control
        self.returning_provider = returning_provider
        self.clock = clock
        self.progress = FormerPrimaryCompletionStore(planner.control.database)

    def verify(self, recovery_id: str) -> FormerPrimaryCompletionResult:
        plan = self.progress.require_plan(recovery_id)
        completion = self.progress.completion(recovery_id)
        if plan.record.state is FormerPrimaryRecoveryState.COMPLETED:
            return FormerPrimaryCompletionResult(
                "completed",
                "recovery-completed",
                "former-primary recovery is already complete",
                plan,
                completion=completion,
            )
        if plan.record.state in (
            FormerPrimaryRecoveryState.OPERATOR_ATTENTION,
            FormerPrimaryRecoveryState.FAILED,
        ):
            return FormerPrimaryCompletionResult(
                "operator-attention",
                plan.record.latest_error_code or "recovery-not-verifiable",
                plan.record.latest_error_reason
                or "recovery requires operator attention",
                plan,
            )
        if plan.record.state is FormerPrimaryRecoveryState.AWAITING_REPORT:
            return FormerPrimaryCompletionResult(
                "awaiting-report",
                "repair-awaiting-report",
                "verified recovery requires a newer synchronized report",
                plan,
            )
        try:
            self._validate_authority(plan)
        except FederationValidationError as error:
            failed = self.progress.transition_record(
                recovery_id,
                expected_states=(
                    FormerPrimaryRecoveryState.PLANNED,
                    FormerPrimaryRecoveryState.TRANSFERRING,
                    FormerPrimaryRecoveryState.VERIFYING,
                    FormerPrimaryRecoveryState.RETRYABLE,
                ),
                state=FormerPrimaryRecoveryState.OPERATOR_ATTENTION,
                now=self.clock(),
                error_code=error.code,
                error_reason=str(error),
            )
            return FormerPrimaryCompletionResult(
                "operator-attention",
                error.code,
                str(error),
                failed,
            )
        for item in plan.items:
            if item.status is FormerPrimaryItemStatus.MISSING:
                return FormerPrimaryCompletionResult(
                    "retryable",
                    "repair-transfer-incomplete",
                    "one or more authoritative items remain missing",
                    plan,
                )
            if item.status in (
                FormerPrimaryItemStatus.CONFLICT,
                FormerPrimaryItemStatus.FAILED,
            ):
                return FormerPrimaryCompletionResult(
                    "operator-attention",
                    item.last_error_code or "repair-item-conflict",
                    item.last_error_reason or "recovery item is not verifiable",
                    plan,
                )
            identity = self.returning_provider.committed_identity(
                session_id=item.session_id,
                group_id=item.group_id,
                batch_id=item.item_id,
            )
            try:
                if identity is None:
                    raise FederationValidationError(
                        "repair-target-evidence-missing",
                        "item_id",
                        "returning provider no longer exposes the repaired identity",
                    )
                FormerPrimaryRepairWorker._validate_identity(
                    item,
                    identity,
                    target_side=True,
                )
            except FederationValidationError as error:
                failed = self.progress.record_operator_attention(
                    recovery_id,
                    item.item_id,
                    expected_statuses=(
                        FormerPrimaryItemStatus.PRESENT,
                        FormerPrimaryItemStatus.DELIVERED,
                        FormerPrimaryItemStatus.VERIFIED,
                    ),
                    code=error.code,
                    reason=str(error),
                    now=self.clock(),
                )
                return FormerPrimaryCompletionResult(
                    "operator-attention",
                    error.code,
                    str(error),
                    failed,
                )
        verified = self.progress.mark_verified(recovery_id, now=self.clock())
        return FormerPrimaryCompletionResult(
            "awaiting-report",
            "repair-verified",
            "all authoritative items are verified; submit a newer synchronized report",
            verified,
        )

    def submit_report(
        self,
        recovery_id: str,
        report: StorageReplicaReport,
        *,
        actor_node_id: str,
    ) -> FormerPrimaryCompletionResult:
        plan = self.progress.require_plan(recovery_id)
        completion = self.progress.completion(recovery_id)
        if plan.record.state is FormerPrimaryRecoveryState.COMPLETED:
            return FormerPrimaryCompletionResult(
                "completed",
                "recovery-completed",
                "former-primary recovery is already complete",
                plan,
                completion=completion,
            )
        if plan.record.state is not FormerPrimaryRecoveryState.AWAITING_REPORT:
            verified = self.verify(recovery_id)
            if verified.status != "awaiting-report":
                return verified
            plan = verified.plan
        try:
            self._validate_authority(plan)
        except FederationValidationError as error:
            failed = self.progress.transition_record(
                recovery_id,
                expected_states=(FormerPrimaryRecoveryState.AWAITING_REPORT,),
                state=FormerPrimaryRecoveryState.OPERATOR_ATTENTION,
                now=self.clock(),
                error_code=error.code,
                error_reason=str(error),
            )
            return FormerPrimaryCompletionResult(
                "operator-attention",
                error.code,
                str(error),
                failed,
            )
        self._validate_final_report(plan, report)
        assessment = self.control.submit_storage_replica_report(
            report,
            actor_node_id=actor_node_id,
        )
        if (
            not assessment.accepted
            or not assessment.eligibility
            or assessment.eligibility_reason != "eligible"
            or assessment.report is None
            or assessment.report.report_hash() != report.report_hash()
        ):
            waiting = self.progress.record_report_wait(
                recovery_id,
                code="recovery-report-not-eligible",
                reason=assessment.eligibility_reason,
                now=self.clock(),
            )
            return FormerPrimaryCompletionResult(
                "awaiting-report",
                "recovery-report-not-eligible",
                assessment.eligibility_reason,
                waiting,
                assessment=assessment,
            )
        completed_plan, completed = self.progress.complete(
            recovery_id,
            assessment,
            now=self.clock(),
        )
        return FormerPrimaryCompletionResult(
            "completed",
            "former-primary-recovery-complete",
            "returning former primary is an exact eligible synchronized replica",
            completed_plan,
            assessment=assessment,
            completion=completed,
        )

    def diagnostics(self, recovery_id: str) -> FormerPrimaryRecoveryDiagnostics:
        plan = self.progress.require_plan(recovery_id)
        record = plan.record
        snapshot = self.control.snapshot(record.session_id)
        assignment = snapshot.groups.get(record.group_id)
        current_primary = (
            None if assignment is None else assignment.primary_provider_id
        )
        if current_primary == record.returning_provider_id:
            role = "primary"
        elif (
            assignment is not None
            and record.returning_provider_id in assignment.replica_provider_ids
        ):
            role = "replica"
        else:
            role = "unassigned"
        assessment = self.control.latest_storage_replica_assessment(
            record.session_id,
            record.group_id,
            record.returning_provider_id,
        )
        report = None if assessment is None else assessment.report
        counts = Counter(item.status.value for item in plan.items)
        pending = tuple(
            sorted(
                item.item_id
                for item in plan.items
                if item.status
                in (
                    FormerPrimaryItemStatus.MISSING,
                    FormerPrimaryItemStatus.DELIVERED,
                    FormerPrimaryItemStatus.PRESENT,
                )
            )
        )
        conflicts = tuple(
            sorted(
                item.item_id
                for item in plan.items
                if item.status
                in (
                    FormerPrimaryItemStatus.CONFLICT,
                    FormerPrimaryItemStatus.FAILED,
                )
            )
        )
        return FormerPrimaryRecoveryDiagnostics(
            recovery_id=record.recovery_id,
            session_id=record.session_id,
            group_id=record.group_id,
            returning_provider_id=record.returning_provider_id,
            returning_node_id=record.returning_node_id,
            provider_role=role,
            current_primary_provider_id=current_primary,
            state=record.state.value,
            manifest_revision=record.manifest_revision,
            manifest_hash=record.manifest_hash,
            initial_report_revision=record.report_revision,
            initial_report_hash=record.report_hash,
            latest_report_revision=(
                None if assessment is None else assessment.report_revision
            ),
            latest_report_hash=(None if assessment is None else assessment.report_hash),
            integrity_verified=(
                None if report is None else report.integrity_verified
            ),
            synchronization_state=(
                None if report is None else report.synchronization_state
            ),
            provider_reported_eligible=(None if report is None else report.eligible),
            provider_eligibility_reasons=(
                () if report is None else report.eligibility_reasons
            ),
            coordinator_eligibility=(
                None if assessment is None else assessment.eligibility
            ),
            coordinator_eligibility_reason=(
                None if assessment is None else assessment.eligibility_reason
            ),
            item_status_counts=tuple(sorted(counts.items())),
            pending_item_ids=pending,
            conflict_item_ids=conflicts,
            latest_error_code=record.latest_error_code,
            latest_error_reason=record.latest_error_reason,
            degraded_state=self.control.storage_degraded_state(
                record.session_id,
                record.group_id,
            ),
            completed_at=record.completed_at,
        )

    def _validate_authority(self, plan: FormerPrimaryRecoveryPlan) -> None:
        record = plan.record
        binding = self.planner._binding(
            record.session_id,
            record.group_id,
            record.promotion_id,
            returning_provider_id=record.returning_provider_id,
        )
        publication = binding["publication"]
        if (
            record.final_authority_identity
            != binding["finalization"].final_authority_identity
            or record.publication_control_revision != publication.control_revision
            or record.manifest_revision != binding["manifest"].revision
            or record.manifest_hash != binding["manifest"].manifest_hash
            or record.grant_id != publication.grant_id
            or record.term != publication.term
            or record.fencing_token != publication.fencing_token
            or record.lease_expires_at != publication.lease_expires_at
        ):
            raise FederationValidationError(
                "former-primary-replan-required",
                "recovery_id",
                "manifest or active authority changed during recovery",
            )

    @staticmethod
    def _validate_final_report(
        plan: FormerPrimaryRecoveryPlan,
        report: StorageReplicaReport,
    ) -> None:
        record = plan.record
        if (
            report.session_id != record.session_id
            or report.group_id != record.group_id
            or report.provider_id != record.returning_provider_id
        ):
            raise FederationValidationError(
                "recovery-report-scope-mismatch",
                "report",
                "final report does not match the recovery scope",
            )
        if report.report_revision <= record.report_revision:
            raise FederationValidationError(
                "recovery-report-not-newer",
                "report_revision",
                "final report must advance beyond the planning evidence",
            )
        if (
            report.manifest_revision != record.manifest_revision
            or report.manifest_hash != record.manifest_hash
        ):
            raise FederationValidationError(
                "recovery-report-manifest-mismatch",
                "manifest_hash",
                "final report must match the authoritative recovery manifest",
            )
        if (
            not report.integrity_verified
            or report.synchronization_state != "synchronized"
            or not report.eligible
            or report.eligibility_reasons != ("eligible",)
        ):
            raise FederationValidationError(
                "recovery-report-not-synchronized",
                "report",
                "final report must assert verified synchronized eligibility",
            )

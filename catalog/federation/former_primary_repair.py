"""E7.2 relay-first transfer for durable former-primary recovery plans."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .errors import FederationValidationError
from .former_primary_recovery import (
    FormerPrimaryItemStatus,
    FormerPrimaryRecoveryItem,
    FormerPrimaryRecoveryPlan,
    FormerPrimaryRecoveryPlanner,
    FormerPrimaryRecoveryState,
    FormerPrimaryRecoveryStore,
)
from .local_storage import BatchStorageProvider, CommittedBatchIdentity
from .replication import ReplicationTransport
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp",
            "timestamp",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class FormerPrimaryRepairRunResult:
    status: str
    code: str
    reason: str
    attempted: int
    delivered: int
    reconciled: int
    plan: FormerPrimaryRecoveryPlan


class FormerPrimaryRepairProgressStore:
    """Compare-and-set state transitions over the E7 recovery ledger."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        self.plans = FormerPrimaryRecoveryStore(database)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def transition_record(
        self,
        recovery_id: str,
        *,
        expected_states: tuple[FormerPrimaryRecoveryState, ...],
        state: FormerPrimaryRecoveryState,
        now: datetime,
        error_code: str | None = None,
        error_reason: str | None = None,
    ) -> FormerPrimaryRecoveryPlan:
        if (error_code is None) != (error_reason is None):
            raise FederationValidationError(
                "invalid-recovery-error",
                "error_code",
                "error code and reason must appear together",
            )
        placeholders = ",".join("?" for _ in expected_states)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                f"""UPDATE storage_former_primary_recoveries
                    SET state=?, latest_error_code=?, latest_error_reason=?,
                        updated_at=?
                    WHERE recovery_id=? AND state IN ({placeholders})""",
                (
                    state.value,
                    error_code,
                    error_reason,
                    _timestamp(now),
                    recovery_id,
                    *(expected.value for expected in expected_states),
                ),
            )
            if cursor.rowcount not in (0, 1):
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-recovery-change",
                    "recovery_id",
                    "record transition affected an unexpected number of rows",
                )
            connection.commit()
        plan = self.plans.get(recovery_id)
        if plan is None:
            raise FederationValidationError(
                "unknown-former-primary-recovery",
                "recovery_id",
                "durable recovery plan does not exist",
            )
        return plan

    def begin_attempt(
        self,
        recovery_id: str,
        item_id: str,
        *,
        now: datetime,
    ) -> FormerPrimaryRecoveryPlan:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE storage_former_primary_recovery_items
                   SET attempt_count=attempt_count + 1,
                       last_error_code=NULL, last_error_reason=NULL,
                       updated_at=?
                   WHERE recovery_id=? AND item_id=? AND status=?""",
                (
                    _timestamp(now),
                    recovery_id,
                    item_id,
                    FormerPrimaryItemStatus.MISSING.value,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-recovery-change",
                    "item_id",
                    "repair attempt could not reserve the missing item",
                )
            connection.commit()
        plan = self.plans.get(recovery_id)
        assert plan is not None
        return plan

    def transition_item(
        self,
        recovery_id: str,
        item_id: str,
        *,
        expected_statuses: tuple[FormerPrimaryItemStatus, ...],
        status: FormerPrimaryItemStatus,
        now: datetime,
        error_code: str | None = None,
        error_reason: str | None = None,
    ) -> FormerPrimaryRecoveryPlan:
        if (error_code is None) != (error_reason is None):
            raise FederationValidationError(
                "invalid-recovery-error",
                "error_code",
                "error code and reason must appear together",
            )
        placeholders = ",".join("?" for _ in expected_statuses)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                f"""UPDATE storage_former_primary_recovery_items
                    SET status=?, last_error_code=?, last_error_reason=?,
                        updated_at=?
                    WHERE recovery_id=? AND item_id=?
                      AND status IN ({placeholders})""",
                (
                    status.value,
                    error_code,
                    error_reason,
                    _timestamp(now),
                    recovery_id,
                    item_id,
                    *(value.value for value in expected_statuses),
                ),
            )
            if cursor.rowcount not in (0, 1):
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-recovery-change",
                    "item_id",
                    "item transition affected an unexpected number of rows",
                )
            connection.commit()
        plan = self.plans.get(recovery_id)
        if plan is None:
            raise FederationValidationError(
                "unknown-former-primary-recovery",
                "recovery_id",
                "durable recovery plan does not exist",
            )
        return plan

    def record_retryable_failure(
        self,
        recovery_id: str,
        item_id: str,
        *,
        code: str,
        reason: str,
        now: datetime,
    ) -> FormerPrimaryRecoveryPlan:
        self.transition_item(
            recovery_id,
            item_id,
            expected_statuses=(FormerPrimaryItemStatus.MISSING,),
            status=FormerPrimaryItemStatus.MISSING,
            now=now,
            error_code=code,
            error_reason=reason,
        )
        return self.transition_record(
            recovery_id,
            expected_states=(
                FormerPrimaryRecoveryState.TRANSFERRING,
                FormerPrimaryRecoveryState.RETRYABLE,
            ),
            state=FormerPrimaryRecoveryState.RETRYABLE,
            now=now,
            error_code=code,
            error_reason=reason,
        )

    def record_operator_attention(
        self,
        recovery_id: str,
        item_id: str,
        *,
        code: str,
        reason: str,
        now: datetime,
    ) -> FormerPrimaryRecoveryPlan:
        self.transition_item(
            recovery_id,
            item_id,
            expected_statuses=(FormerPrimaryItemStatus.MISSING,),
            status=FormerPrimaryItemStatus.CONFLICT,
            now=now,
            error_code=code,
            error_reason=reason,
        )
        return self.transition_record(
            recovery_id,
            expected_states=(
                FormerPrimaryRecoveryState.PLANNED,
                FormerPrimaryRecoveryState.TRANSFERRING,
                FormerPrimaryRecoveryState.RETRYABLE,
            ),
            state=FormerPrimaryRecoveryState.OPERATOR_ATTENTION,
            now=now,
            error_code=code,
            error_reason=reason,
        )


class FormerPrimaryRepairWorker:
    """Transfer missing authoritative batches over the existing relay route."""

    _FATAL_CODES = {
        "repair-source-identity-mismatch",
        "repair-source-item-missing",
        "repair-source-hash-mismatch",
        "repair-target-identity-conflict",
        "repair-target-evidence-missing",
        "repair-response-identity-mismatch",
        "response-request-mismatch",
    }

    def __init__(
        self,
        *,
        planner: FormerPrimaryRecoveryPlanner,
        source_provider: BatchStorageProvider,
        returning_provider: BatchStorageProvider,
        transport: ReplicationTransport,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self.planner = planner
        self.source_provider = source_provider
        self.returning_provider = returning_provider
        self.transport = transport
        self.clock = clock
        self.progress = FormerPrimaryRepairProgressStore(planner.control.database)

    async def run_once(
        self,
        recovery_id: str,
        *,
        limit: int = 100,
    ) -> FormerPrimaryRepairRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError(
                "invalid-limit",
                "limit",
                "must be a positive integer",
            )
        try:
            plan = self.planner.validate_plan(recovery_id)
        except FederationValidationError as error:
            stored = self.progress.plans.get(recovery_id)
            if stored is None:
                raise
            return FormerPrimaryRepairRunResult(
                "operator-attention",
                error.code,
                str(error),
                0,
                0,
                0,
                stored,
            )
        if plan.record.state in (
            FormerPrimaryRecoveryState.OPERATOR_ATTENTION,
            FormerPrimaryRecoveryState.FAILED,
        ):
            return self._result(
                "operator-attention",
                plan.record.latest_error_code or "recovery-not-transferable",
                plan.record.latest_error_reason or "recovery requires operator attention",
                plan,
            )
        pending = tuple(
            item
            for item in plan.items
            if item.status is FormerPrimaryItemStatus.MISSING
        )[:limit]
        if not pending:
            verifying = self.progress.transition_record(
                recovery_id,
                expected_states=(
                    FormerPrimaryRecoveryState.PLANNED,
                    FormerPrimaryRecoveryState.TRANSFERRING,
                    FormerPrimaryRecoveryState.RETRYABLE,
                    FormerPrimaryRecoveryState.VERIFYING,
                ),
                state=FormerPrimaryRecoveryState.VERIFYING,
                now=self.clock(),
            )
            return self._result(
                "awaiting-verification",
                "repair-transfer-complete",
                "all missing items are durably delivered",
                verifying,
            )
        self.progress.transition_record(
            recovery_id,
            expected_states=(
                FormerPrimaryRecoveryState.PLANNED,
                FormerPrimaryRecoveryState.TRANSFERRING,
                FormerPrimaryRecoveryState.RETRYABLE,
            ),
            state=FormerPrimaryRecoveryState.TRANSFERRING,
            now=self.clock(),
        )
        attempted = 0
        delivered = 0
        reconciled = 0
        retryable_failure = False
        for original in pending:
            try:
                plan = self.planner.validate_plan(recovery_id)
                item = self._item(plan, original.item_id)
                target = self.returning_provider.committed_identity(
                    session_id=item.session_id,
                    group_id=item.group_id,
                    batch_id=item.item_id,
                )
                if target is not None:
                    self._validate_identity(item, target, target_side=True)
                    self.progress.transition_item(
                        recovery_id,
                        item.item_id,
                        expected_statuses=(FormerPrimaryItemStatus.MISSING,),
                        status=FormerPrimaryItemStatus.DELIVERED,
                        now=self.clock(),
                    )
                    reconciled += 1
                    continue
                plan = self.progress.begin_attempt(
                    recovery_id,
                    item.item_id,
                    now=self.clock(),
                )
                item = self._item(plan, item.item_id)
                attempted += 1
                await self._deliver(plan, item)
                target = self.returning_provider.committed_identity(
                    session_id=item.session_id,
                    group_id=item.group_id,
                    batch_id=item.item_id,
                )
                if target is None:
                    raise FederationValidationError(
                        "repair-target-evidence-missing",
                        "item_id",
                        "returning provider did not expose durable identity after success",
                    )
                self._validate_identity(item, target, target_side=True)
                self.progress.transition_item(
                    recovery_id,
                    item.item_id,
                    expected_statuses=(FormerPrimaryItemStatus.MISSING,),
                    status=FormerPrimaryItemStatus.DELIVERED,
                    now=self.clock(),
                )
                delivered += 1
            except FederationValidationError as error:
                if error.code in self._FATAL_CODES:
                    failed = self.progress.record_operator_attention(
                        recovery_id,
                        original.item_id,
                        code=error.code,
                        reason=str(error),
                        now=self.clock(),
                    )
                    return FormerPrimaryRepairRunResult(
                        "operator-attention",
                        error.code,
                        str(error),
                        attempted,
                        delivered,
                        reconciled,
                        failed,
                    )
                self.progress.record_retryable_failure(
                    recovery_id,
                    original.item_id,
                    code=error.code,
                    reason=str(error),
                    now=self.clock(),
                )
                retryable_failure = True
            except (OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
                self.progress.record_retryable_failure(
                    recovery_id,
                    original.item_id,
                    code="repair-delivery-failed",
                    reason=str(error),
                    now=self.clock(),
                )
                retryable_failure = True
        final = self.progress.plans.get(recovery_id)
        assert final is not None
        remaining = any(
            item.status is FormerPrimaryItemStatus.MISSING for item in final.items
        )
        if remaining or retryable_failure:
            final = self.progress.transition_record(
                recovery_id,
                expected_states=(
                    FormerPrimaryRecoveryState.TRANSFERRING,
                    FormerPrimaryRecoveryState.RETRYABLE,
                ),
                state=FormerPrimaryRecoveryState.RETRYABLE,
                now=self.clock(),
                error_code=("repair-incomplete" if remaining else None),
                error_reason=(
                    "one or more authoritative items remain undelivered"
                    if remaining
                    else None
                ),
            )
            return FormerPrimaryRepairRunResult(
                "retryable",
                "repair-incomplete",
                "one or more authoritative items remain undelivered",
                attempted,
                delivered,
                reconciled,
                final,
            )
        final = self.progress.transition_record(
            recovery_id,
            expected_states=(
                FormerPrimaryRecoveryState.TRANSFERRING,
                FormerPrimaryRecoveryState.RETRYABLE,
            ),
            state=FormerPrimaryRecoveryState.VERIFYING,
            now=self.clock(),
        )
        return FormerPrimaryRepairRunResult(
            "awaiting-verification",
            "repair-transfer-complete",
            "all missing items are durably delivered",
            attempted,
            delivered,
            reconciled,
            final,
        )

    async def _deliver(
        self,
        plan: FormerPrimaryRecoveryPlan,
        item: FormerPrimaryRecoveryItem,
    ) -> None:
        record = plan.record
        if item.kind.value != "batch" or item.idempotency_key is None:
            raise FederationValidationError(
                "repair-item-unsupported",
                "item_id",
                "relay-first E7.2 currently transfers immutable batches",
            )
        source_identity = self.source_provider.committed_identity(
            session_id=item.session_id,
            group_id=item.group_id,
            batch_id=item.item_id,
        )
        if source_identity is None:
            raise FederationValidationError(
                "repair-source-item-missing",
                "item_id",
                "active primary cannot expose an authoritative item identity",
            )
        self._validate_identity(item, source_identity, target_side=False)
        content = self.source_provider.read(
            session_id=item.session_id,
            group_id=item.group_id,
            batch_id=item.item_id,
        )
        if content is None:
            raise FederationValidationError(
                "repair-source-item-missing",
                "item_id",
                "active primary cannot read an authoritative item",
            )
        if BatchIngestRequest.calculate_content_hash(content) != item.content_hash:
            raise FederationValidationError(
                "repair-source-hash-mismatch",
                "content_hash",
                "active primary content differs from the recovery ledger",
            )
        request = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=record.session_id,
                group_id=record.group_id,
                actor_node_id=record.source_node_id,
                grant_id=record.grant_id,
                term=record.term,
                fencing_token=record.fencing_token,
                lease_expires_at=record.lease_expires_at,
            ),
            dataset_id=item.dataset_id,
            batch_id=item.item_id,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            content=content,
            created_at=record.created_at,
            dataset_schema_name=item.schema_name,
            dataset_schema_version=item.schema_version,
        )
        envelope = StorageRequestEnvelope(
            request_id=item.delivery_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=record.session_id,
            actor_node_id=record.source_node_id,
            authorization_context={
                "kind": "storage-replication",
                "group_id": record.group_id,
                "provider_id": record.returning_provider_id,
                "recovery_id": record.recovery_id,
                "delivery_id": item.delivery_id,
            },
            payload=request.to_dict(),
        )
        response = await self.transport.request(
            target_node_id=record.returning_node_id,
            envelope=envelope,
        )
        if response.request_id != envelope.request_id:
            raise FederationValidationError(
                "response-request-mismatch",
                "request_id",
                "repair response belongs to another request",
            )
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "repair",
                response.error.message,
            )
        result = BatchIngestResult.from_dict(response.result)
        if (
            result.batch_id != item.item_id
            or result.idempotency_key != item.idempotency_key
            or result.content_hash != item.content_hash
        ):
            raise FederationValidationError(
                "repair-response-identity-mismatch",
                "result",
                "returning provider response differs from recovery identity",
            )

    @staticmethod
    def _item(
        plan: FormerPrimaryRecoveryPlan,
        item_id: str,
    ) -> FormerPrimaryRecoveryItem:
        for item in plan.items:
            if item.item_id == item_id:
                return item
        raise FederationValidationError(
            "unknown-recovery-item",
            "item_id",
            "recovery item does not exist",
        )

    @staticmethod
    def _validate_identity(
        item: FormerPrimaryRecoveryItem,
        identity: CommittedBatchIdentity,
        *,
        target_side: bool,
    ) -> None:
        expected = (
            item.session_id,
            item.group_id,
            item.dataset_id,
            item.schema_name,
            item.schema_version,
            item.item_id,
            item.idempotency_key,
            item.content_hash,
        )
        actual = (
            identity.session_id,
            identity.group_id,
            identity.dataset_id,
            identity.dataset_schema_name,
            identity.dataset_schema_version,
            identity.batch_id,
            identity.idempotency_key,
            identity.content_hash,
        )
        if actual != expected:
            raise FederationValidationError(
                (
                    "repair-target-identity-conflict"
                    if target_side
                    else "repair-source-identity-mismatch"
                ),
                "item_id",
                "provider-local immutable identity differs from recovery ledger",
            )

    @staticmethod
    def _result(
        status: str,
        code: str,
        reason: str,
        plan: FormerPrimaryRecoveryPlan,
    ) -> FormerPrimaryRepairRunResult:
        return FormerPrimaryRepairRunResult(status, code, reason, 0, 0, 0, plan)

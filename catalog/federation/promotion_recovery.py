"""Deterministic restart recovery for Phase E promotion transactions."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from .errors import FederationValidationError
from .promotion_finalization import PromotionFinalizationRecord
from .promotion_publication import publish_storage_promotion
from .promotion_transaction import PromotionTransactionRecord
from .provider_fencing import ProviderFenceStore

if TYPE_CHECKING:
    from .phase_d_control import PhaseDControlPlane

RecoveryStatus = Literal["completed", "retryable", "failed", "operator-attention"]

_LOCKS_GUARD = threading.Lock()
_RECOVERY_LOCKS: dict[tuple[str, str, str, str], threading.Lock] = {}


def _recovery_lock(
    database: str,
    session_id: str,
    group_id: str,
    promotion_id: str,
) -> threading.Lock:
    key = (str(Path(database).resolve()), session_id, group_id, promotion_id)
    with _LOCKS_GUARD:
        return _RECOVERY_LOCKS.setdefault(key, threading.Lock())


@dataclass(frozen=True)
class PromotionRecoveryResult:
    status: RecoveryStatus
    stage: str
    code: str
    reason: str
    actions: tuple[str, ...]
    transaction: PromotionTransactionRecord | None
    finalization: PromotionFinalizationRecord | None


def _validate_stage_shape(record: PromotionTransactionRecord) -> None:
    fence = record.fencing_acknowledged and record.fencing_ack_identity is not None
    grant = record.grant_acknowledged and record.grant_ack_identity is not None
    reserved = record.reserved_term is not None and record.grant_id is not None
    if record.fencing_acknowledged != (record.fencing_ack_identity is not None):
        raise FederationValidationError(
            "impossible-promotion-stage",
            "fencing_acknowledged",
            "fencing acknowledgement flag and identity disagree",
        )
    if record.grant_acknowledged != (record.grant_ack_identity is not None):
        raise FederationValidationError(
            "impossible-promotion-stage",
            "grant_acknowledged",
            "grant acknowledgement flag and identity disagree",
        )
    if (record.reserved_term is None) != (record.grant_id is None):
        raise FederationValidationError(
            "impossible-promotion-stage",
            "reserved_term",
            "reserved term and durable grant identity must appear together",
        )
    expected = {
        "validated": (False, False, False),
        "fenced-old-authority": (True, False, False),
        "allocated-term": (True, True, False),
        "granted-new-authority": (True, True, True),
        "finalized": (True, True, True),
    }
    if record.state in expected and (fence, reserved, grant) != expected[record.state]:
        raise FederationValidationError(
            "impossible-promotion-stage",
            "state",
            f"{record.state} contradicts its durable evidence",
        )
    if record.state == "failed" and (
        record.failure_code is None or record.failure_reason is None
    ):
        raise FederationValidationError(
            "corrupt-failed-promotion",
            "failure_code",
            "failed transaction requires durable diagnostics",
        )
    if record.state == "pending":
        raise FederationValidationError(
            "unvalidated-promotion",
            "state",
            "pending transaction has no validated immutable bindings",
        )


def _result(
    *,
    status: RecoveryStatus,
    stage: str,
    code: str,
    reason: str,
    actions: list[str],
    transaction: PromotionTransactionRecord | None,
    finalization: PromotionFinalizationRecord | None = None,
) -> PromotionRecoveryResult:
    return PromotionRecoveryResult(
        status=status,
        stage=stage,
        code=code,
        reason=reason,
        actions=tuple(actions),
        transaction=transaction,
        finalization=finalization,
    )


def recover_storage_promotion(
    control: PhaseDControlPlane,
    session_id: str,
    group_id: str,
    promotion_id: str,
    *,
    provider_fence_store: ProviderFenceStore,
    selected_provider_store: ProviderFenceStore,
    fencing_delivery_available: bool = True,
    grant_delivery_available: bool = True,
    now: datetime | None = None,
) -> PromotionRecoveryResult:
    """Resume one promotion exclusively from durable coordinator/provider state."""

    actions: list[str] = []
    lock = _recovery_lock(control.database, session_id, group_id, promotion_id)
    with lock:
        for _ in range(10):
            try:
                record = control.promotion_transaction(
                    session_id,
                    group_id,
                    promotion_id,
                )
                if record is None:
                    return _result(
                        status="operator-attention",
                        stage="missing",
                        code="unknown-promotion",
                        reason="durable promotion transaction does not exist",
                        actions=actions,
                        transaction=None,
                    )
                _validate_stage_shape(record)
                if record.state == "failed":
                    return _result(
                        status="failed",
                        stage="failed",
                        code=record.failure_code or "promotion-failed",
                        reason=record.failure_reason or "promotion failed",
                        actions=actions,
                        transaction=record,
                    )
                control.revalidate_promotion_bindings(record)

                if record.state == "validated":
                    command = control.promotion_fencing_command(
                        session_id,
                        group_id,
                        promotion_id,
                    )
                    evidence = provider_fence_store.acknowledgement(group_id)
                    if evidence is None:
                        if not fencing_delivery_available:
                            retryable = (
                                control.record_promotion_fencing_delivery_failure(
                                    session_id,
                                    group_id,
                                    promotion_id,
                                    reason=(
                                        "previous provider unavailable; fencing "
                                        "delivery remains retryable"
                                    ),
                                    now=now,
                                )
                            )
                            return _result(
                                status="retryable",
                                stage="validated",
                                code="fencing-delivery-unavailable",
                                reason=(
                                    retryable.failure_reason
                                    or "fencing delivery unavailable"
                                ),
                                actions=actions,
                                transaction=retryable,
                            )
                        evidence = provider_fence_store.apply(command, now=now)
                        actions.append("persisted-provider-fence")
                    previous = control.snapshot(session_id).providers.get(
                        record.previous_provider_id or ""
                    )
                    if previous is None:
                        raise FederationValidationError(
                            "previous-provider-missing",
                            "previous_provider_id",
                            "previous provider registration is unavailable",
                        )
                    control.acknowledge_promotion_fencing(
                        evidence,
                        actor_node_id=previous.node_id,
                        provider_fence_store=provider_fence_store,
                    )
                    actions.append("recorded-fencing-acknowledgement")
                    continue

                if record.state == "fenced-old-authority":
                    control.reserve_promotion_term(
                        session_id,
                        group_id,
                        promotion_id,
                        provider_fence_store=provider_fence_store,
                        now=now,
                    )
                    actions.append("reserved-term")
                    continue

                if record.state == "allocated-term":
                    command = control.promotion_grant_command(
                        session_id,
                        group_id,
                        promotion_id,
                        provider_fence_store=provider_fence_store,
                    )
                    evidence = selected_provider_store.grant_acknowledgement(
                        group_id,
                        promotion_id,
                    )
                    if evidence is None:
                        if not grant_delivery_available:
                            retryable = (
                                control.record_promotion_grant_delivery_failure(
                                    session_id,
                                    group_id,
                                    promotion_id,
                                    reason=(
                                        "selected provider unavailable; grant "
                                        "delivery remains retryable"
                                    ),
                                    now=now,
                                )
                            )
                            return _result(
                                status="retryable",
                                stage="allocated-term",
                                code="grant-delivery-unavailable",
                                reason=(
                                    retryable.failure_reason
                                    or "grant delivery unavailable"
                                ),
                                actions=actions,
                                transaction=retryable,
                            )
                        evidence = selected_provider_store.apply_grant(
                            command,
                            expected_promotion_id=record.promotion_id,
                            expected_group_id=record.group_id,
                            expected_manifest_revision=(
                                record.selected_manifest_revision
                            ),
                            expected_manifest_hash=record.selected_manifest_hash,
                            expected_report_revision=record.selected_report_revision,
                            expected_report_hash=record.selected_report_hash,
                            now=now,
                        )
                        actions.append("persisted-provider-grant")
                    selected = control.snapshot(session_id).providers.get(
                        record.selected_provider_id
                    )
                    if selected is None:
                        raise FederationValidationError(
                            "selected-provider-missing",
                            "selected_provider_id",
                            "selected provider registration is unavailable",
                        )
                    control.acknowledge_promotion_grant(
                        evidence,
                        actor_node_id=selected.node_id,
                        provider_fence_store=provider_fence_store,
                        selected_provider_store=selected_provider_store,
                    )
                    actions.append("recorded-grant-acknowledgement")
                    continue

                if record.state == "granted-new-authority":
                    control.persist_promotion_finalization(
                        session_id,
                        group_id,
                        promotion_id,
                        provider_fence_store=provider_fence_store,
                        selected_provider_store=selected_provider_store,
                        now=now,
                    )
                    actions.append("persisted-finalization")
                    continue

                if record.state == "finalized":
                    existing_finalization = control.promotion_finalization(
                        session_id,
                        group_id,
                        promotion_id,
                    )
                    _publication, published = publish_storage_promotion(
                        control,
                        session_id,
                        group_id,
                        promotion_id,
                        now=now,
                    )
                    if published:
                        actions.append("published-coordinator-authority")
                    finalization = control.complete_promotion_finalization(
                        session_id,
                        group_id,
                        promotion_id,
                        provider_fence_store=provider_fence_store,
                        selected_provider_store=selected_provider_store,
                        now=now,
                    )
                    if not finalization.degraded_cleared:
                        raise FederationValidationError(
                            "incomplete-finalization",
                            "degraded_cleared",
                            "finalization did not complete degraded-state exit",
                        )
                    if (
                        existing_finalization is not None
                        and not existing_finalization.degraded_cleared
                    ) or "persisted-finalization" in actions:
                        actions.append("cleared-matching-degraded-state")
                    return _result(
                        status="completed",
                        stage="finalized",
                        code="promotion-finalized",
                        reason=(
                            "durable promotion finalization and coordinator "
                            "publication are complete"
                        ),
                        actions=actions,
                        transaction=control.promotion_transaction(
                            session_id,
                            group_id,
                            promotion_id,
                        ),
                        finalization=finalization,
                    )
            except FederationValidationError as exc:
                transaction = None
                try:
                    transaction = control.promotion_transaction(
                        session_id,
                        group_id,
                        promotion_id,
                    )
                except FederationValidationError:
                    pass
                return _result(
                    status="operator-attention",
                    stage="corrupt-or-contradictory",
                    code=exc.code,
                    reason=str(exc),
                    actions=actions,
                    transaction=transaction,
                )
        return _result(
            status="operator-attention",
            stage="recovery-loop",
            code="promotion-recovery-did-not-converge",
            reason=(
                "promotion recovery exceeded the bounded durable stage count"
            ),
            actions=actions,
            transaction=control.promotion_transaction(
                session_id,
                group_id,
                promotion_id,
            ),
        )

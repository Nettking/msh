from __future__ import annotations

import asyncio
from pathlib import Path

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.former_primary_recovery import (
    FormerPrimaryItemStatus,
    FormerPrimaryRecoveryPlanner,
    FormerPrimaryRecoveryState,
)
from catalog.federation.former_primary_repair import FormerPrimaryRepairWorker
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.phase_d_service import PhaseDStorageService
from catalog.federation.replication import CallableReplicationTransport
from catalog.federation.storage_protocol import (
    BatchIngestResult,
    BatchIngestState,
    StorageResponseEnvelope,
)

from .test_phase_e63_provider_fencing import NOW
from .test_phase_e71_former_primary_recovery import (
    GROUP_ID,
    NEW_PROVIDER,
    OLD_PROVIDER,
    PROMOTION_ID,
    SESSION_ID,
    Runtime,
)


def _returning_service(runtime: Runtime, tmp_path: Path) -> PhaseDStorageService:
    return PhaseDStorageService(
        provider_id=OLD_PROVIDER,
        provider=runtime.returning,
        control_plane=runtime.control,
        outbox=SQLiteOutbox(tmp_path / "returning-outbox.sqlite3"),
        acknowledgements=DurableAcknowledgementStore(
            tmp_path / "returning-acks.sqlite3"
        ),
        clock=lambda: NOW,
    )


def _plan(runtime: Runtime):
    request = runtime.ingest()
    runtime.report()
    planner = runtime.planner()
    return request, planner, planner.plan(SESSION_ID, GROUP_ID, PROMOTION_ID)


def _worker(
    runtime: Runtime,
    planner: FormerPrimaryRecoveryPlanner,
    transport: CallableReplicationTransport,
    *,
    source=None,
) -> FormerPrimaryRepairWorker:
    return FormerPrimaryRepairWorker(
        planner=planner,
        source_provider=source or runtime.source,
        returning_provider=runtime.returning,
        transport=transport,
        clock=lambda: NOW,
    )


# F4-004: repair uses the real assigned-replica service path and preserves fencing.
def test_e72_relay_first_transfer_marks_item_delivered_and_verifying(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request, planner, plan = _plan(runtime)
    service = _returning_service(runtime, tmp_path)
    calls: list[str] = []

    async def send(target_node_id, envelope):
        calls.append(target_node_id)
        return await service.dispatch(envelope)

    result = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(send),
        ).run_once(plan.record.recovery_id)
    )

    assert result.status == "awaiting-verification"
    assert result.delivered == 1
    assert result.reconciled == 0
    assert calls == ["node-old"]
    assert result.plan.record.state is FormerPrimaryRecoveryState.VERIFYING
    assert result.plan.items[0].status is FormerPrimaryItemStatus.DELIVERED
    assert result.plan.items[0].attempt_count == 1
    identity = runtime.returning.committed_identity(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    assert identity is not None and identity.content_hash == request.content_hash
    assert runtime.old_fence.high_water_term(GROUP_ID) == 5


# F4-005: transient delivery state survives coordinator/worker restart.
def test_e72_retryable_failure_persists_then_restart_completes(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, plan = _plan(runtime)

    async def unavailable(_target_node_id, _envelope):
        raise TimeoutError("relay unavailable")

    failed = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(unavailable),
        ).run_once(plan.record.recovery_id)
    )
    assert failed.status == "retryable"
    assert failed.plan.record.state is FormerPrimaryRecoveryState.RETRYABLE
    assert failed.plan.items[0].status is FormerPrimaryItemStatus.MISSING
    assert failed.plan.items[0].attempt_count == 1
    assert failed.plan.items[0].last_error_code == "repair-delivery-failed"

    restarted_control = PhaseDControlPlane(runtime.coordinator_path)
    restarted_planner = FormerPrimaryRecoveryPlanner(
        control=restarted_control,
        returning_provider=runtime.returning,
        former_provider_fence_store=runtime.old_fence,
        clock=lambda: NOW,
    )
    service = PhaseDStorageService(
        provider_id=OLD_PROVIDER,
        provider=runtime.returning,
        control_plane=restarted_control,
        outbox=SQLiteOutbox(tmp_path / "restart-outbox.sqlite3"),
        acknowledgements=DurableAcknowledgementStore(
            tmp_path / "restart-acks.sqlite3"
        ),
        clock=lambda: NOW,
    )

    async def send(_target_node_id, envelope):
        return await service.dispatch(envelope)

    completed = asyncio.run(
        FormerPrimaryRepairWorker(
            planner=restarted_planner,
            source_provider=runtime.source,
            returning_provider=runtime.returning,
            transport=CallableReplicationTransport(send),
            clock=lambda: NOW,
        ).run_once(plan.record.recovery_id)
    )

    assert completed.status == "awaiting-verification"
    assert completed.plan.record.state is FormerPrimaryRecoveryState.VERIFYING
    assert completed.plan.items[0].status is FormerPrimaryItemStatus.DELIVERED
    assert completed.plan.items[0].attempt_count == 2


# F4-005: provider persistence followed by a lost response is reconciled without resend.
def test_e72_lost_response_reconciles_target_identity_without_second_send(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request, planner, plan = _plan(runtime)
    service = _returning_service(runtime, tmp_path)
    sends = 0

    async def persist_then_drop(_target_node_id, envelope):
        nonlocal sends
        sends += 1
        response = await service.dispatch(envelope)
        assert response.ok
        raise TimeoutError("response lost")

    first = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(persist_then_drop),
        ).run_once(plan.record.recovery_id)
    )
    assert first.status == "retryable"
    assert first.plan.items[0].attempt_count == 1
    assert runtime.returning.committed_identity(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    ) is not None

    async def must_not_send(_target_node_id, _envelope):
        raise AssertionError("exact target identity should reconcile before send")

    second = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(must_not_send),
        ).run_once(plan.record.recovery_id)
    )

    assert sends == 1
    assert second.status == "awaiting-verification"
    assert second.delivered == 0
    assert second.reconciled == 1
    assert second.plan.items[0].status is FormerPrimaryItemStatus.DELIVERED
    assert second.plan.items[0].attempt_count == 1


# F4-005: completed transfer replay has no side effects.
def test_e72_duplicate_run_replays_verifying_without_delivery(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, plan = _plan(runtime)
    service = _returning_service(runtime, tmp_path)
    calls = 0

    async def send(_target_node_id, envelope):
        nonlocal calls
        calls += 1
        return await service.dispatch(envelope)

    worker = _worker(runtime, planner, CallableReplicationTransport(send))
    first = asyncio.run(worker.run_once(plan.record.recovery_id))
    second = asyncio.run(worker.run_once(plan.record.recovery_id))

    assert first.status == second.status == "awaiting-verification"
    assert calls == 1
    assert second.attempted == second.delivered == second.reconciled == 0
    assert second.plan.items[0].attempt_count == 1


# F3-008/F3-009: changed assignment stops repair before transport.
def test_e72_changed_authority_stops_before_send(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, plan = _plan(runtime)
    runtime.control.change_assignment(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        NEW_PROVIDER,
        (),
    )

    async def must_not_send(_target_node_id, _envelope):
        raise AssertionError("stale authority must fail before transport")

    result = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(must_not_send),
        ).run_once(plan.record.recovery_id)
    )

    assert result.status == "operator-attention"
    assert result.code == "former-primary-authority-mismatch"
    assert result.attempted == 0


# F0-015: durable former-primary fence remains mandatory during transfer.
def test_e72_missing_fence_stops_before_send(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, plan = _plan(runtime)
    with runtime.old_fence._connect() as connection:
        connection.execute("DELETE FROM provider_storage_fences")

    async def must_not_send(_target_node_id, _envelope):
        raise AssertionError("missing fence must fail before transport")

    result = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(must_not_send),
        ).run_once(plan.record.recovery_id)
    )

    assert result.status == "operator-attention"
    assert result.code == "former-primary-not-fenced"
    assert result.attempted == 0


# F4-003: source disappearance/corruption cannot be converted into replica authority.
def test_e72_missing_source_identity_becomes_operator_attention(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request, planner, plan = _plan(runtime)
    empty_source = FilesystemBatchStorageProvider(tmp_path / "empty-source")

    async def must_not_send(_target_node_id, _envelope):
        raise AssertionError("missing source identity must fail before transport")

    result = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(must_not_send),
            source=empty_source,
        ).run_once(plan.record.recovery_id)
    )

    assert result.status == "operator-attention"
    assert result.code == "repair-source-item-missing"
    assert result.plan.record.state is FormerPrimaryRecoveryState.OPERATOR_ATTENTION
    assert result.plan.items[0].status is FormerPrimaryItemStatus.CONFLICT
    assert runtime.returning.committed_identity(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    ) is None


# F4-003: successful transport with mismatched immutable result fails closed.
def test_e72_mismatched_success_response_becomes_operator_attention(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, plan = _plan(runtime)

    async def forged(_target_node_id, envelope):
        return StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=envelope.protocol,
            protocol_version=envelope.protocol_version,
            ok=True,
            result=BatchIngestResult(
                batch_id="other-batch",
                idempotency_key="other-idempotency",
                content_hash="sha256:" + "f" * 64,
                state=BatchIngestState.STORED,
            ).to_dict(),
        )

    result = asyncio.run(
        _worker(
            runtime,
            planner,
            CallableReplicationTransport(forged),
        ).run_once(plan.record.recovery_id)
    )

    assert result.status == "operator-attention"
    assert result.code == "repair-response-identity-mismatch"
    assert result.plan.items[0].status is FormerPrimaryItemStatus.CONFLICT

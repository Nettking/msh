from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.former_primary_recovery import (
    FormerPrimaryItemStatus,
    FormerPrimaryRecoveryPlanner,
    FormerPrimaryRecoveryState,
    FormerPrimaryRecoveryStore,
)
from catalog.federation.former_primary_completion import (
    FormerPrimaryCompletionStore,
    FormerPrimaryCompletionWorker,
)
from catalog.federation.former_primary_repair import FormerPrimaryRepairWorker
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.replication import CallableReplicationTransport
from catalog.federation.reporting import StorageReplicaReport

from .test_phase_e63_provider_fencing import NOW
from .test_phase_e71_former_primary_recovery import (
    GROUP_ID,
    NEW_PROVIDER,
    OLD_PROVIDER,
    SESSION_ID,
    Runtime,
)
from .test_phase_e72_former_primary_transfer import _plan, _returning_service


def _repair(runtime: Runtime, tmp_path: Path):
    request, planner, plan = _plan(runtime)
    service = _returning_service(runtime, tmp_path)

    async def send(_target_node_id, envelope):
        return await service.dispatch(envelope)

    result = asyncio.run(
        FormerPrimaryRepairWorker(
            planner=planner,
            source_provider=runtime.source,
            returning_provider=runtime.returning,
            transport=CallableReplicationTransport(send),
            clock=lambda: NOW,
        ).run_once(plan.record.recovery_id)
    )
    assert result.status == "awaiting-verification"
    return request, planner, result.plan


def _final_report(
    runtime: Runtime,
    *,
    revision: int = 3,
    hashes: tuple[str, ...] | None = None,
    integrity_verified: bool = True,
    synchronization_state: str = "synchronized",
    eligible: bool = True,
    reasons: tuple[str, ...] = ("eligible",),
) -> StorageReplicaReport:
    manifest = runtime.control.manifest(SESSION_ID, GROUP_ID)
    return StorageReplicaReport(
        schema="msh.storage_replica_report.v1",
        protocol_version="1.0",
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        provider_id=OLD_PROVIDER,
        report_revision=revision,
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
        committed_item_hashes=(
            tuple(item.content_hash for item in manifest.items)
            if hashes is None
            else hashes
        ),
        datasets=manifest.datasets,
        integrity_verified=integrity_verified,
        synchronization_state=synchronization_state,
        eligible=eligible,
        eligibility_reasons=reasons,
        reported_at=NOW,
    )


def _completion_worker(
    runtime: Runtime,
    planner: FormerPrimaryRecoveryPlanner,
    *,
    control: PhaseDControlPlane | None = None,
    provider=None,
) -> FormerPrimaryCompletionWorker:
    selected_planner = planner
    if control is not None:
        selected_planner = FormerPrimaryRecoveryPlanner(
            control=control,
            returning_provider=runtime.returning,
            former_provider_fence_store=runtime.old_fence,
            clock=lambda: NOW,
        )
    return FormerPrimaryCompletionWorker(
        planner=selected_planner,
        returning_provider=provider or runtime.returning,
        clock=lambda: NOW,
    )


# F3-007..F3-010 and F4-004: complete relay-first recovery end to end.
def test_e78_verified_report_completes_recovery_without_restoring_old_authority(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request, planner, transferred = _repair(runtime, tmp_path)
    worker = _completion_worker(runtime, planner)

    verified = worker.verify(transferred.record.recovery_id)
    assert verified.status == "awaiting-report"
    assert verified.plan.record.state is FormerPrimaryRecoveryState.AWAITING_REPORT
    assert verified.plan.items[0].status is FormerPrimaryItemStatus.VERIFIED

    completed = worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime),
        actor_node_id="node-old",
    )

    assert completed.status == "completed"
    assert completed.plan.record.state is FormerPrimaryRecoveryState.COMPLETED
    assert completed.completion is not None
    assert completed.completion.final_report_revision == 3
    assert completed.assessment is not None
    assert completed.assessment.accepted
    assert completed.assessment.eligibility
    assert completed.assessment.eligibility_reason == "eligible"
    snapshot = runtime.control.snapshot(SESSION_ID)
    assert snapshot.groups[GROUP_ID].primary_provider_id == NEW_PROVIDER
    assert OLD_PROVIDER in snapshot.groups[GROUP_ID].replica_provider_ids
    assert runtime.old_fence.high_water_term(GROUP_ID) == 5
    identity = runtime.returning.committed_identity(
        session_id=SESSION_ID,
        group_id=GROUP_ID,
        batch_id=request.batch_id,
    )
    assert identity is not None and identity.content_hash == request.content_hash


# E7.5: restart after verification reuses the durable ledger and completion gate.
def test_e78_restart_after_verification_completes_once(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    first_worker = _completion_worker(runtime, planner)
    first_worker.verify(transferred.record.recovery_id)

    restarted_control = PhaseDControlPlane(runtime.coordinator_path)
    restarted_worker = _completion_worker(
        runtime,
        planner,
        control=restarted_control,
    )
    completed = restarted_worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime),
        actor_node_id="node-old",
    )
    replay = restarted_worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime),
        actor_node_id="node-old",
    )

    assert completed.status == replay.status == "completed"
    assert completed.completion == replay.completion
    completion = FormerPrimaryCompletionStore(
        runtime.coordinator_path
    ).completion(transferred.record.recovery_id)
    assert completion == completed.completion


# E7.4: coordinator eligibility remains authoritative even if the provider claims it.
def test_e78_ineligible_report_waits_for_newer_corrected_report(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    worker = _completion_worker(runtime, planner)
    worker.verify(transferred.record.recovery_id)

    rejected = worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime, revision=3, hashes=()),
        actor_node_id="node-old",
    )
    assert rejected.status == "awaiting-report"
    assert rejected.code == "recovery-report-not-eligible"
    assert rejected.assessment is not None
    assert not rejected.assessment.eligibility
    assert rejected.assessment.eligibility_reason == "committed-hash-conflict"

    corrected = worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime, revision=4),
        actor_node_id="node-old",
    )
    assert corrected.status == "completed"
    assert corrected.completion is not None
    assert corrected.completion.final_report_revision == 4


# E7.4: authenticated sender binding rejects a forged former-primary report.
def test_e78_wrong_report_sender_is_rejected_and_not_completed(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    worker = _completion_worker(runtime, planner)
    worker.verify(transferred.record.recovery_id)

    with pytest.raises(FederationValidationError):
        worker.submit_report(
            transferred.record.recovery_id,
            _final_report(runtime),
            actor_node_id="node-forged",
        )

    plan = FormerPrimaryRecoveryStore(runtime.coordinator_path).get(
        transferred.record.recovery_id
    )
    assert plan is not None
    assert plan.record.state is FormerPrimaryRecoveryState.AWAITING_REPORT
    assert FormerPrimaryCompletionStore(runtime.coordinator_path).completion(
        transferred.record.recovery_id
    ) is None


# F4-003: target evidence disappearing after delivery fails closed.
def test_e78_verification_detects_missing_target_identity(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)

    class MissingIdentityProvider:
        def committed_identity(self, **_kwargs):
            return None

    result = _completion_worker(
        runtime,
        planner,
        provider=MissingIdentityProvider(),
    ).verify(transferred.record.recovery_id)

    assert result.status == "operator-attention"
    assert result.code == "repair-target-evidence-missing"
    assert result.plan.record.state is FormerPrimaryRecoveryState.OPERATOR_ATTENTION
    assert result.plan.items[0].status is FormerPrimaryItemStatus.CONFLICT


# F4-007: manifest advancement after transfer prevents stale completion.
def test_e78_manifest_change_before_verification_fails_closed(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    runtime.ingest("2")

    result = _completion_worker(runtime, planner).verify(
        transferred.record.recovery_id
    )

    assert result.status == "operator-attention"
    assert result.code == "former-primary-replan-required"
    assert result.plan.record.state is FormerPrimaryRecoveryState.OPERATOR_ATTENTION


# E8: diagnostics expose role, authority, integrity, sync, eligibility, and items.
def test_e8_diagnostics_expose_completed_recovery_state(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    worker = _completion_worker(runtime, planner)
    worker.verify(transferred.record.recovery_id)
    worker.submit_report(
        transferred.record.recovery_id,
        _final_report(runtime),
        actor_node_id="node-old",
    )

    diagnostics = worker.diagnostics(transferred.record.recovery_id)
    payload = diagnostics.to_dict()

    assert diagnostics.state == "completed"
    assert diagnostics.provider_role == "replica"
    assert diagnostics.current_primary_provider_id == NEW_PROVIDER
    assert diagnostics.integrity_verified is True
    assert diagnostics.synchronization_state == "synchronized"
    assert diagnostics.coordinator_eligibility is True
    assert diagnostics.coordinator_eligibility_reason == "eligible"
    assert dict(diagnostics.item_status_counts) == {"verified": 1}
    assert diagnostics.pending_item_ids == ()
    assert diagnostics.conflict_item_ids == ()
    assert payload["degraded_state"] is None
    assert payload["completed_at"] is not None


# F4-008: completion of one recovery does not mutate another group ledger.
def test_e78_recovery_state_is_independent_per_group(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    _request, planner, transferred = _repair(runtime, tmp_path)
    store = FormerPrimaryRecoveryStore(runtime.coordinator_path)
    original = transferred
    other_recovery_id = "recovery-storage-aux"
    other_item = replace(
        original.items[0],
        recovery_id=other_recovery_id,
        group_id="storage-aux",
        item_id="batch-aux",
        delivery_id="delivery-aux",
        status=FormerPrimaryItemStatus.MISSING,
        attempt_count=0,
        last_error_code=None,
        last_error_reason=None,
    )
    other_record = replace(
        original.record,
        recovery_id=other_recovery_id,
        group_id="storage-aux",
        state=FormerPrimaryRecoveryState.PLANNED,
        present_count=0,
        missing_count=1,
        conflict_count=0,
        latest_error_code=None,
        latest_error_reason=None,
        completed_at=None,
    )
    store.create_plan(other_record, (other_item,))

    worker = _completion_worker(runtime, planner)
    worker.verify(original.record.recovery_id)
    worker.submit_report(
        original.record.recovery_id,
        _final_report(runtime),
        actor_node_id="node-old",
    )

    untouched = store.get(other_recovery_id)
    assert untouched is not None
    assert untouched.record.state is FormerPrimaryRecoveryState.PLANNED
    assert untouched.items[0].status is FormerPrimaryItemStatus.MISSING

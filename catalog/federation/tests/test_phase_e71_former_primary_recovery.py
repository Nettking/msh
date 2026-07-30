from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.former_primary_recovery import (
    FORMER_PRIMARY_RECOVERY_SCHEMA,
    FormerPrimaryItemStatus,
    FormerPrimaryRecoveryItem,
    FormerPrimaryRecoveryPlanner,
    FormerPrimaryRecoveryRecord,
    FormerPrimaryRecoveryState,
    FormerPrimaryRecoveryStore,
)
from catalog.federation.local_storage import FilesystemBatchStorageProvider
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.phase_d_service import PhaseDStorageService
from catalog.federation.provider_fencing import ProviderFenceStore
from catalog.federation.reporting import StorageReplicaReport
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)

from .test_phase_e63_provider_fencing import NOW
from .test_phase_e66_recovery import _base, _recover

SESSION_ID = "session-1"
GROUP_ID = "storage-main"
PROMOTION_ID = "promotion-1"
OLD_PROVIDER = "provider-old"
NEW_PROVIDER = "provider-new"


class Runtime:
    def __init__(self, tmp_path: Path) -> None:
        control, old_fence, new_fence, coordinator_path = _base(tmp_path)
        completed = _recover(control, old_fence, new_fence)
        assert completed.status == "completed"

        self.control = control
        self.coordinator_path = coordinator_path
        self.old_fence = old_fence
        self.source = FilesystemBatchStorageProvider(tmp_path / "new-primary")
        self.returning = FilesystemBatchStorageProvider(tmp_path / "old-returning")
        self.outbox = SQLiteOutbox(tmp_path / "new-primary-outbox.sqlite3")
        self.acks = DurableAcknowledgementStore(tmp_path / "new-primary-acks.sqlite3")
        self.control.set_acknowledgement_policy(
            SESSION_ID,
            GROUP_ID,
            AcknowledgementMode.PRIMARY,
        )
        self.service = PhaseDStorageService(
            provider_id=NEW_PROVIDER,
            provider=self.source,
            control_plane=self.control,
            outbox=self.outbox,
            acknowledgements=self.acks,
            clock=lambda: NOW,
        )
        self.requests: list[BatchIngestRequest] = []

    def ingest(self, suffix: str = "1") -> BatchIngestRequest:
        snapshot = self.control.snapshot(SESSION_ID)
        grant = snapshot.leader_grants[GROUP_ID]
        content = {
            "source": "recorder-a",
            "observations": [{"sequence": int(suffix), "value": 42.0}],
        }
        request = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=SESSION_ID,
                group_id=GROUP_ID,
                actor_node_id="node-new",
                grant_id=str(grant["grant_id"]),
                term=int(grant["term"]),
                fencing_token=int(grant["fencing_token"]),
                lease_expires_at=datetime.fromisoformat(
                    str(grant["lease_expires_at"]).replace("Z", "+00:00")
                ),
            ),
            dataset_id="telemetry",
            batch_id=f"batch-{suffix}",
            idempotency_key=f"recorder-a:{suffix}",
            content_hash=BatchIngestRequest.calculate_content_hash(content),
            content=content,
            created_at=NOW,
            dataset_schema_name="msh.telemetry.observations",
            dataset_schema_version=3,
        )
        envelope = StorageRequestEnvelope(
            request_id=f"request-{suffix}",
            protocol="msh-storage-v1",
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=SESSION_ID,
            actor_node_id="recorder-node",
            authorization_context={
                "kind": "storage-primary-route",
                "group_id": GROUP_ID,
                "provider_id": NEW_PROVIDER,
            },
            payload=request.to_dict(),
        )
        response = asyncio.run(self.service.dispatch(envelope))
        assert response.ok
        self.requests.append(request)
        return request

    def report(
        self,
        *,
        hashes: tuple[str, ...] = (),
        manifest_revision: int | None = None,
        manifest_hash: str | None = None,
        integrity_verified: bool = True,
        report_revision: int = 2,
    ) -> StorageReplicaReport:
        manifest = self.control.manifest(SESSION_ID, GROUP_ID)
        transaction = self.control.promotion_transaction(
            SESSION_ID,
            GROUP_ID,
            PROMOTION_ID,
        )
        assert transaction is not None
        report = StorageReplicaReport(
            schema="msh.storage_replica_report.v1",
            protocol_version="1.0",
            session_id=SESSION_ID,
            group_id=GROUP_ID,
            provider_id=OLD_PROVIDER,
            report_revision=report_revision,
            manifest_revision=(
                transaction.selected_manifest_revision
                if manifest_revision is None
                else manifest_revision
            ),
            manifest_hash=(
                transaction.selected_manifest_hash
                if manifest_hash is None
                else manifest_hash
            ),
            committed_item_hashes=hashes,
            datasets=manifest.datasets,
            integrity_verified=integrity_verified,
            synchronization_state="repair-required",
            eligible=False,
            eligibility_reasons=("recovery-required",),
            reported_at=NOW,
        )
        assessment = self.control.submit_storage_replica_report(
            report,
            actor_node_id="node-old",
        )
        assert assessment.accepted
        return report

    def planner(
        self,
        *,
        fence_store: ProviderFenceStore | None = None,
        control: PhaseDControlPlane | None = None,
    ) -> FormerPrimaryRecoveryPlanner:
        return FormerPrimaryRecoveryPlanner(
            control=control or self.control,
            returning_provider=self.returning,
            former_provider_fence_store=fence_store or self.old_fence,
            clock=lambda: NOW,
        )


# F3-007, F3-010, F4-004: persist a complete plan before transfer.
def test_e71_plan_persists_missing_items_and_replays_after_restart(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request = runtime.ingest()
    runtime.report()

    first = runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)
    restarted = PhaseDControlPlane(runtime.coordinator_path)
    second = runtime.planner(control=restarted).plan(
        SESSION_ID,
        GROUP_ID,
        PROMOTION_ID,
    )

    assert first == second
    assert first.record.state is FormerPrimaryRecoveryState.PLANNED
    assert first.record.missing_count == 1
    assert first.record.present_count == 0
    assert first.record.conflict_count == 0
    assert tuple(item.item_id for item in first.items) == (request.batch_id,)
    assert first.items[0].status is FormerPrimaryItemStatus.MISSING
    with restarted._connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM storage_former_primary_recoveries"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM storage_former_primary_recovery_items"
        ).fetchone()[0] == 1


# F4-004: exact identity is present, never inferred from row count.
def test_e71_exact_target_identity_is_present(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    request = runtime.ingest()
    runtime.returning.ingest(request)
    manifest = runtime.control.manifest(SESSION_ID, GROUP_ID)
    runtime.report(
        hashes=(request.content_hash,),
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
    )

    plan = runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)

    assert plan.record.present_count == 1
    assert plan.record.missing_count == 0
    assert plan.items[0].status is FormerPrimaryItemStatus.PRESENT


# F0-015 and F3-007: recovery requires the durable old-authority fence.
def test_e71_missing_former_primary_fence_fails_closed(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    runtime.ingest()
    runtime.report()
    empty_fence = ProviderFenceStore(
        tmp_path / "empty-fence.sqlite3",
        session_id=SESSION_ID,
        provider_id=OLD_PROVIDER,
    )

    with pytest.raises(FederationValidationError) as error:
        runtime.planner(fence_store=empty_fence).plan(
            SESSION_ID,
            GROUP_ID,
            PROMOTION_ID,
        )

    assert error.value.code == "former-primary-not-fenced"
    assert (
        FormerPrimaryRecoveryStore(runtime.control.database).recoveries(
            session_id=SESSION_ID,
            group_id=GROUP_ID,
            returning_provider_id=OLD_PROVIDER,
        )
        == ()
    )


# F3-008/F3-009: bind planning to published assignment and current grant.
def test_e71_changed_assignment_or_authority_fails_before_persistence(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    runtime.ingest()
    runtime.report()
    runtime.control.change_assignment(
        SESSION_ID,
        "coordinator",
        GROUP_ID,
        NEW_PROVIDER,
        (),
    )

    with pytest.raises(FederationValidationError) as error:
        runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)

    assert error.value.code == "former-primary-authority-mismatch"


# F4-007: a persisted plan is never reused after the manifest advances.
def test_e71_manifest_change_requires_replan(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    runtime.ingest("1")
    runtime.report(report_revision=2)
    plan = runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)

    runtime.ingest("2")

    with pytest.raises(FederationValidationError) as error:
        runtime.planner().validate_plan(plan.record.recovery_id)

    assert error.value.code == "former-primary-replan-required"


# F4-003: conflicting immutable target state remains operator-attention.
def test_e71_conflicting_target_identity_is_never_overwritten(
    tmp_path: Path,
) -> None:
    runtime = Runtime(tmp_path)
    request = runtime.ingest()
    conflicting_content = {"conflict": True}
    conflicting = replace(
        request,
        content=conflicting_content,
        content_hash=BatchIngestRequest.calculate_content_hash(
            conflicting_content
        ),
    )
    runtime.returning.ingest(conflicting)
    runtime.report(hashes=(conflicting.content_hash,))

    plan = runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)

    assert plan.record.state is FormerPrimaryRecoveryState.OPERATOR_ATTENTION
    assert plan.record.latest_error_code == "returning-provider-extra-hash"
    assert plan.record.conflict_count == 1
    assert plan.items[0].status is FormerPrimaryItemStatus.CONFLICT


# F4-003: unknown extra hashes cannot map to authoritative identities.
def test_e71_unknown_extra_hash_fails_closed(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    runtime.ingest()
    extra_hash = "sha256:" + "f" * 64
    runtime.report(hashes=(extra_hash,))

    plan = runtime.planner().plan(SESSION_ID, GROUP_ID, PROMOTION_ID)

    assert plan.record.state is FormerPrimaryRecoveryState.OPERATOR_ATTENTION
    assert plan.record.latest_error_code == "returning-provider-extra-hash"
    assert plan.record.missing_count == 1


# F4-008: durable plans are isolated by storage group and provider.
def test_e71_recovery_store_isolated_per_group(tmp_path: Path) -> None:
    store = FormerPrimaryRecoveryStore(tmp_path / "isolated.sqlite3")
    now = NOW

    def plan_for(group_id: str) -> tuple[
        FormerPrimaryRecoveryRecord,
        tuple[FormerPrimaryRecoveryItem, ...],
    ]:
        recovery_id = f"recovery-{group_id}"
        item = FormerPrimaryRecoveryItem(
            recovery_id=recovery_id,
            session_id=SESSION_ID,
            group_id=group_id,
            returning_provider_id=OLD_PROVIDER,
            item_id=f"item-{group_id}",
            kind="batch",
            dataset_id="telemetry",
            schema_name="msh.telemetry.observations",
            schema_version=3,
            idempotency_key=f"idem-{group_id}",
            content_hash="sha256:" + ("a" if group_id == "group-a" else "b") * 64,
            size_bytes=1,
            source_id=None,
            first_sequence=None,
            last_sequence=None,
            commit_state="committed",
            status="missing",
            delivery_id=f"delivery-{group_id}",
            attempt_count=0,
            last_error_code=None,
            last_error_reason=None,
            updated_at=now,
        )
        record = FormerPrimaryRecoveryRecord(
            schema=FORMER_PRIMARY_RECOVERY_SCHEMA,
            recovery_id=recovery_id,
            session_id=SESSION_ID,
            group_id=group_id,
            returning_provider_id=OLD_PROVIDER,
            returning_node_id="node-old",
            source_provider_id=NEW_PROVIDER,
            source_node_id="node-new",
            promotion_id=f"promotion-{group_id}",
            final_authority_identity=f"authority-{group_id}",
            publication_control_revision=10,
            manifest_revision=1,
            manifest_hash="sha256:" + ("c" if group_id == "group-a" else "d") * 64,
            grant_id=f"grant-{group_id}",
            term=2,
            fencing_token=3,
            lease_expires_at=NOW.replace(hour=23),
            report_revision=1,
            report_hash="sha256:" + ("e" if group_id == "group-a" else "f") * 64,
            state="planned",
            item_count=1,
            present_count=0,
            missing_count=1,
            conflict_count=0,
            latest_error_code=None,
            latest_error_reason=None,
            created_at=now,
            updated_at=now,
        )
        return record, (item,)

    record_a, items_a = plan_for("group-a")
    record_b, items_b = plan_for("group-b")
    stored_a = store.create_plan(record_a, items_a)
    stored_b = store.create_plan(record_b, items_b)

    assert store.recoveries(
        session_id=SESSION_ID,
        group_id="group-a",
        returning_provider_id=OLD_PROVIDER,
    ) == (stored_a,)
    assert store.recoveries(
        session_id=SESSION_ID,
        group_id="group-b",
        returning_provider_id=OLD_PROVIDER,
    ) == (stored_b,)

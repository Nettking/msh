from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.control_sync import (
    STORAGE_CONTROL_PLAN_SCHEMA,
    StorageControlPlan,
)
from catalog.federation.live_failover import (
    LIVE_FAILOVER_SCHEMA,
    LiveFailoverRecord,
    LiveFailoverStore,
)
from catalog.federation.reporting import (
    REPORT_PROTOCOL_VERSION,
    REPORT_SCHEMA,
    StorageReplicaAssessment,
    StorageReplicaReport,
)
from catalog.federation.verified_live_failover import (
    VerifiedStorageFailoverCoordinator,
)

NOW = datetime(2026, 7, 31, 9, 0, tzinfo=timezone.utc)
MANIFEST_HASH = "sha256:" + "1" * 64
REPORT_HASH = "sha256:" + "2" * 64
FAILOVER_ID = "sha256:" + "3" * 64


class _FreshReportChannel:
    def __init__(self, report: StorageReplicaReport) -> None:
        self.report = report
        self.requests: list[dict[str, object]] = []
        self.refresh_handler = None

    def set_refresh_handler(self, handler) -> None:
        self.refresh_handler = handler

    async def request_replica_report(self, **arguments):
        self.requests.append(arguments)
        return self.report


class _FreshReportControl:
    def __init__(
        self,
        previous: StorageReplicaAssessment,
        fresh: StorageReplicaAssessment,
    ) -> None:
        self.previous = previous
        self.fresh = fresh
        self.submissions: list[tuple[StorageReplicaReport, str]] = []

    def latest_storage_replica_assessment(
        self,
        session_id: str,
        group_id: str,
        provider_id: str,
    ) -> StorageReplicaAssessment:
        assert (session_id, group_id, provider_id) == (
            "session-1",
            "storage-main",
            "provider-replica",
        )
        return self.previous

    def submit_storage_replica_report(
        self,
        report: StorageReplicaReport,
        *,
        actor_node_id: str,
    ) -> StorageReplicaAssessment:
        self.submissions.append((report, actor_node_id))
        assert report is self.fresh.report
        return self.fresh


class _PublicationChannel:
    def __init__(self, *, fail: bool) -> None:
        self.fail = fail
        self.plans: list[StorageControlPlan] = []
        self.targets: list[tuple[str, ...]] = []
        self.refresh_handler = None

    def set_refresh_handler(self, handler) -> None:
        self.refresh_handler = handler

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def publish(
        self,
        plan: StorageControlPlan,
        target_node_ids: tuple[str, ...],
    ) -> tuple[str, ...]:
        self.plans.append(plan)
        self.targets.append(target_node_ids)
        if self.fail:
            raise TimeoutError("simulated coordinator interruption")
        return target_node_ids


class _StatusCoordinator:
    def status(self, *, actor_node_id: str, cursor=None):
        assert actor_node_id == "authority-node"
        assert cursor is None
        return {
            "sessions": [],
            "nodes": [
                {
                    "node_id": "replica-node",
                    "connection_state": "connected",
                    "revoked": False,
                }
            ],
            "capabilities": [
                {
                    "node_id": "replica-node",
                    "type": "storage-provider",
                    "status": "ready",
                    "properties": {"provider_id": "provider-replica"},
                }
            ],
            "pagination": {"has_more": False, "next_cursor": None},
        }


class _CommittedControl:
    def snapshot(self, session_id: str):
        assert session_id == "session-1"
        return SimpleNamespace(
            groups={"storage-main": SimpleNamespace()},
            providers={
                "provider-replica": SimpleNamespace(node_id="replica-node")
            },
        )


def _report(revision: int) -> StorageReplicaReport:
    return StorageReplicaReport(
        schema=REPORT_SCHEMA,
        protocol_version=REPORT_PROTOCOL_VERSION,
        session_id="session-1",
        group_id="storage-main",
        provider_id="provider-replica",
        report_revision=revision,
        manifest_revision=4,
        manifest_hash=MANIFEST_HASH,
        committed_item_hashes=(),
        datasets=(),
        integrity_verified=True,
        synchronization_state="synchronized",
        eligible=True,
        eligibility_reasons=("eligible",),
        reported_at=NOW,
    )


def _assessment(report: StorageReplicaReport) -> StorageReplicaAssessment:
    return StorageReplicaAssessment(
        session_id=report.session_id,
        group_id=report.group_id,
        provider_id=report.provider_id,
        report_revision=report.report_revision,
        report_hash=report.report_hash(),
        accepted=True,
        eligibility=True,
        eligibility_reason="eligible",
        assessed_at=NOW,
        report=report,
    )


def _plan() -> StorageControlPlan:
    return StorageControlPlan(
        schema=STORAGE_CONTROL_PLAN_SCHEMA,
        publication_id="publication-2",
        publication_revision=2,
        source_control_revision=12,
        session_id="session-1",
        authority_node_id="authority-node",
        authority_public_key="test-public-key",
        issued_at=NOW,
        providers=({"provider_id": "provider-replica"},),
        groups=({"group_id": "storage-main"},),
        content_hash="sha256:" + "4" * 64,
        signature="test-signature",
    )


def _record(plan: StorageControlPlan) -> LiveFailoverRecord:
    return LiveFailoverRecord(
        schema=LIVE_FAILOVER_SCHEMA,
        failover_id=FAILOVER_ID,
        session_id="session-1",
        group_id="storage-main",
        failed_provider_id="provider-primary",
        failed_node_id="primary-node",
        source_grant_id="grant-1",
        source_term=1,
        source_fencing_token=1,
        promoted_provider_id="provider-replica",
        promoted_node_id="replica-node",
        selected_report_revision=8,
        selected_report_hash=REPORT_HASH,
        retained_replica_provider_ids=(),
        previous_acknowledgement_mode=AcknowledgementMode.ONE_REPLICA,
        effective_acknowledgement_mode=AcknowledgementMode.PRIMARY,
        source_control_revision=8,
        term=2,
        fencing_token=2,
        grant_id="grant-2",
        state="control-committed",
        publication=plan,
        reason_code="primary-relay-unavailable",
        reason_detail="primary is unavailable",
        detected_at=NOW,
        updated_at=NOW,
    )


def _coordinator(
    *,
    control,
    channel,
    store: LiveFailoverStore,
) -> VerifiedStorageFailoverCoordinator:
    return VerifiedStorageFailoverCoordinator(
        session_coordinator=_StatusCoordinator(),
        control_plane=control,
        publication_store=SimpleNamespace(),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="authority-node")
        ),
        channel=channel,
        failover_store=store,
        session_id="session-1",
        clock=lambda: NOW,
    )


def test_verified_policy_never_reuses_an_older_accepted_report(tmp_path) -> None:
    async def scenario() -> None:
        previous_report = _report(7)
        fresh_report = _report(8)
        previous = _assessment(previous_report)
        fresh = _assessment(fresh_report)
        channel = _FreshReportChannel(fresh_report)
        control = _FreshReportControl(previous, fresh)
        coordinator = _coordinator(
            control=control,
            channel=channel,
            store=LiveFailoverStore(tmp_path / "failover.sqlite3"),
        )
        manifest = SimpleNamespace(group_id="storage-main")
        status = {
            "nodes": [
                {
                    "node_id": "replica-node",
                    "connection_state": "connected",
                    "revoked": False,
                }
            ],
            "capabilities": [
                {
                    "node_id": "replica-node",
                    "type": "storage-provider",
                    "status": "ready",
                    "properties": {"provider_id": "provider-replica"},
                }
            ],
        }
        results = await coordinator._candidate_assessments(
            "observation-1",
            ("provider-replica",),
            {
                "provider-replica": SimpleNamespace(
                    assignable=True,
                    node_id="replica-node",
                )
            },
            status,
            manifest,
        )
        assert results == (fresh,)
        assert len(channel.requests) == 1
        assert channel.requests[0]["report_revision"] == 8
        assert channel.requests[0]["failover_id"] == "observation-1"
        assert control.submissions == [(fresh_report, "replica-node")]

    asyncio.run(scenario())


def test_interrupted_publication_resumes_same_durable_authority(tmp_path) -> None:
    async def scenario() -> None:
        plan = _plan()
        store = LiveFailoverStore(tmp_path / "failover.sqlite3")
        store.save(_record(plan))

        failing_channel = _PublicationChannel(fail=True)
        first = _coordinator(
            control=_CommittedControl(),
            channel=failing_channel,
            store=store,
        )
        await first.start()
        with pytest.raises(TimeoutError, match="simulated coordinator"):
            await first.scan_once()
        await first.close()

        pending = store.active("session-1", "storage-main")
        assert pending is not None
        assert pending.state == "control-committed"
        assert pending.term == 2
        assert pending.fencing_token == 2
        assert pending.publication is not None
        assert pending.publication.publication_id == "publication-2"

        succeeding_channel = _PublicationChannel(fail=False)
        restarted = _coordinator(
            control=_CommittedControl(),
            channel=succeeding_channel,
            store=store,
        )
        await restarted.start()
        results = await restarted.scan_once()
        await restarted.close()

        assert len(results) == 1
        assert results[0].status == "promoted"
        assert results[0].term == 2
        assert results[0].fencing_token == 2
        assert results[0].publication_revision == 2
        assert succeeding_channel.plans == [plan]
        assert succeeding_channel.targets == [("replica-node",)]

        completed = store.get("session-1", "storage-main", FAILOVER_ID)
        assert completed is not None
        assert completed.state == "published"
        assert completed.term == 2
        assert completed.fencing_token == 2
        assert completed.publication is not None
        assert completed.publication.publication_id == "publication-2"

    asyncio.run(scenario())

"""Two real nodes analyse JSONL over the authenticated federation relay.

This exercises the whole chain end to end with nothing mocked but the analysis
executor itself:

    discovery → durable job → F8.2 provider report → select_provider
      → F7.5 ownership + lease → F7.6 artifact grant
      → ResilientDispatchCoordinator → RelayLifecycleEndpoint → relay
      → RelayLifecycleEndpoint → activated trusted CancellableCapabilityWorker
      → F6 authorized artifact transfer → analysis
      → bounded result reference → F7.5 single result commit
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from catalog.capabilities.analysis import (
    AnalysisArtifactGateway,
    AnalysisExecutionReport,
    AnalysisJobRegistry,
    AnalysisWorkService,
    AnalysisWorkSlice,
    FederatedAnalysisHandler,
    FederatedAnalysisScheduler,
    FederatedProviderReportSource,
    LocalArtifactContentStore,
)
from catalog.capabilities.analysis.artifact_carrier import RelayAnalysisArtifactEndpoint
from catalog.capabilities.analysis.contracts import (
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.analysis.provisioning import (
    AnalysisProviderProvisioner,
    analysis_capability_id,
    analysis_handler_descriptor,
    lifecycle_worker_factory,
)
from catalog.capabilities.artifact_secure_runtime import (
    SQLiteCapabilityArtifactAuthority,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.lifecycle_worker import SQLiteLifecycleDispatchInbox
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.capabilities.worker_activation import (
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)
TIMEOUT = 15.0


class _Executor:
    """Stand in for the runner pipeline, and record what actually arrived."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute(self, *, plan, data_dir: Path, workspace: Path):
        files = {
            item.name: item.read_text(encoding="utf-8")
            for item in sorted(data_dir.rglob("*"))
            if item.is_file()
        }
        self.calls.append({"plan": plan, "files": files})
        return AnalysisExecutionReport(
            succeeded=True,
            processed_dates=plan.target_dates,
            analysis_session_ids=("auto_default_20260813_20260813",),
        )


async def _start_relay(database: Path, current: list[datetime]) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: current[0]),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=TIMEOUT,
        send_timeout_seconds=TIMEOUT,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def _client(path: Path, relay: RelayServer, name: str, current) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=path,
        relay_url=relay.url,
        display_name=name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=TIMEOUT,
        clock=lambda: current[0],
    )


async def _enroll(relay: RelayServer, node: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    await node.connect(enrollment_token=token)


def test_two_nodes_analyse_jsonl_over_the_relay(tmp_path: Path) -> None:
    async def scenario() -> None:
        current = [NOW]
        relay = owner = provider_node = None
        endpoints: list = []
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3", current)
            owner = _client(tmp_path / "owner", relay, "Data owner", current)
            provider_node = _client(tmp_path / "provider", relay, "Compute", current)
            await _enroll(relay, owner)
            await _enroll(relay, provider_node)
            session = await owner.create_session("Federated JSONL analysis")
            session_id = str(session["session_id"])
            invitation = await owner.create_invitation(
                session_id, ttl_seconds=60, max_uses=1
            )
            await provider_node.join_session(str(invitation["token"]))
            await owner.request_replay(session_id)

            # --- the provider node installs and enrols its trusted handler ----
            descriptor = analysis_handler_descriptor()
            capability_id = analysis_capability_id(provider_node.node_id)
            relay.coordinator.announce_capability(
                CapabilityAnnouncement(
                    capability_id=capability_id,
                    node_id=provider_node.node_id,
                    session_id=session_id,
                    type=descriptor.capability_type,
                    protocol=descriptor.protocol,
                    protocol_version=descriptor.protocol_version,
                    status=CapabilityStatus.READY,
                    properties=descriptor.attributes,
                    announced_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                request_id="announce-analysis",
            )
            enrollments = FederatedProviderEnrollmentService(
                relay.coordinator,
                SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
                clock=lambda: current[0],
            )
            requested = enrollments.request(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=provider_node.node_id,
                command_id="request-analysis",
            )
            # The session owner approves; the provider cannot approve itself.
            enrollments.approve(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=owner.node_id,
                command_id="approve-analysis",
                expected_revision=requested.revision,
            )
            health = FederatedProviderHealthService(
                enrollments,
                SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
                clock=lambda: current[0],
            )
            inventory = LocalComputeHandlerInventory()
            binder = TrustedComputeWorkerBinder(
                ComputeWorkerActivationAuthority(
                    health,
                    inventory,
                    session_id=session_id,
                    local_node_id=provider_node.node_id,
                    clock=lambda: current[0],
                ),
                lambda provider_id: SQLiteLifecycleDispatchInbox(
                    tmp_path / f"inbox-{provider_id}.sqlite3"
                ),
                clock=lambda: current[0],
                worker_factory=lifecycle_worker_factory,
            )

            # --- data owner: durable stores, artifacts, and the relay carrier -
            store = SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")
            authority = SQLiteCapabilityArtifactAuthority(store)
            content_store = LocalArtifactContentStore(tmp_path / "artifacts")
            gateway = AnalysisArtifactGateway(authority, content_store)
            owner_lifecycle = RelayLifecycleEndpoint(owner, request_timeout=TIMEOUT)
            provider_lifecycle = RelayLifecycleEndpoint(
                provider_node, request_timeout=TIMEOUT
            )
            # Each node has one inbound relay stream: the lifecycle endpoint
            # reads it and forwards everything else to the artifact endpoint.
            owner_artifacts = RelayAnalysisArtifactEndpoint(
                owner,
                gateway,
                clock=lambda: current[0],
                message_source=owner_lifecycle,
            )
            provider_artifacts = RelayAnalysisArtifactEndpoint(
                provider_node,
                None,
                clock=lambda: current[0],
                message_source=provider_lifecycle,
            )

            executor = _Executor()
            handler = FederatedAnalysisHandler(
                session_id=session_id,
                node_id=provider_node.node_id,
                provider_id=capability_id,
                artifact_transport=provider_artifacts,
                executor=executor,
                workspace_root=tmp_path / "workspaces",
                content_store=LocalArtifactContentStore(tmp_path / "worker-artifacts"),
                clock=lambda: current[0],
                data_owner_node_id=lambda _job: owner.node_id,
            )
            provisioner = AnalysisProviderProvisioner(
                coordinator=relay.coordinator,
                enrollments=enrollments,
                health=health,
                inventory=inventory,
                binder=binder,
                session_id=session_id,
                node_id=provider_node.node_id,
                clock=lambda: current[0],
                max_concurrent_jobs=2,
            )
            endpoints.extend(
                (
                    owner_artifacts,
                    provider_artifacts,
                    owner_lifecycle,
                    provider_lifecycle,
                )
            )
            outcome = provisioner.provision(handler, provider_lifecycle)
            assert outcome.reason == "provider-active", outcome.reason
            await asyncio.gather(
                owner_lifecycle.start(),
                provider_lifecycle.start(),
                owner_artifacts.start(),
                provider_artifacts.start(),
            )

            scheduler = FederatedAnalysisScheduler(
                store=store,
                gateway=gateway,
                transport=owner_lifecycle,
                report_source=FederatedProviderReportSource(
                    health, actor_node_id=owner.node_id
                ),
                coordinator_node_id=owner.node_id,
                clock=lambda: current[0],
            )
            service = AnalysisWorkService(
                scheduler=scheduler,
                registry=AnalysisJobRegistry(tmp_path / "jobs.sqlite3"),
                session_id=session_id,
            )

            # --- the data owner discovers a slice and submits durable work ----
            data_dir = tmp_path / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            source = data_dir / "2026-08-13.jsonl"
            source.write_text(
                '{"timestamp":"2026-08-13T10:00:00Z","machine":"A"}\n', encoding="utf-8"
            )
            work = AnalysisWorkSlice(
                session_id=session_id,
                slice_kind=SLICE_KIND_DATE,
                slice_key="2026-08-13",
                target_dates=("2026-08-13",),
                script_keys=("data_pr_day",),
                runtime_namespace="default",
                source_signature="e" * 64,
                origin=ORIGIN_AUTOMATIC_DISCOVERY,
            )
            submission = service.submit_analysis_work(
                work, slice_files=(source,), slice_root=data_dir
            )
            assert submission.created is True

            result = await scheduler.schedule(submission.job_id)

            # --- the whole chain must have run on the *other* node -----------
            assert result.decision == "dispatched", result.reason
            assert result.provider_id == capability_id
            assert result.node_id == provider_node.node_id
            assert result.node_id != owner.node_id

            snapshot = store.snapshot(submission.job_id)
            assert snapshot.job.status.value == "succeeded"
            committed = store.result_commit(submission.job_id)
            assert committed is not None
            assert committed.provider_id == capability_id
            assert (
                committed.reference.reference_id == snapshot.job.outputs[0].reference_id
            )

            # The JSONL really crossed the relay as an authorized artifact.
            assert len(executor.calls) == 1
            assert executor.calls[0]["files"] == {
                "2026-08-13.jsonl": '{"timestamp":"2026-08-13T10:00:00Z","machine":"A"}\n'
            }
            assert executor.calls[0]["plan"].job_id == submission.job_id

            # Re-scheduling the same job must not execute a second time.
            replay = await scheduler.schedule(submission.job_id)
            assert replay.decision == "already-terminal"
            assert len(executor.calls) == 1
            assert store.result_commit(submission.job_id) == committed
        finally:
            for endpoint in endpoints:
                await endpoint.close()
            for node in (owner, provider_node):
                if node is not None:
                    await node.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(asyncio.wait_for(scenario(), timeout=90))

"""Shared fixtures for the federated analysis job tests.

The harness wires the *real* stack: a device-local ``SessionCoordinator``,
F8.1 enrollment, F8.2 health, F8.4 trusted activation, the F7.5 lifecycle store
and coordinator, F7.6 artifact authority, and the F6 object-transfer contract.
Only the analysis executor is a stand-in, so tests exercise production
scheduling, authorization and dispatch rather than mocks around them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.analysis import (
    AnalysisArtifactGateway,
    AnalysisExecutionReport,
    AnalysisJobRegistry,
    AnalysisPlan,
    AnalysisWorkService,
    AnalysisWorkSlice,
    FederatedAnalysisHandler,
    FederatedAnalysisScheduler,
    FederatedProviderReportSource,
    LocalArtifactContentStore,
)
from catalog.capabilities.analysis.artifact_carrier import LocalAnalysisArtifactCarrier
from catalog.capabilities.analysis.contracts import (
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.analysis.provisioning import (
    AnalysisProviderProvisioner,
    analysis_capability_id,
)
from catalog.capabilities.artifact_secure_runtime import (
    SQLiteCapabilityArtifactAuthority,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.local_lifecycle import LocalLifecycleTransport
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.node.identity import IdentityStore
from catalog.orchestrator.analysis_federation import DeviceFederationAuthority

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)


class Clock:
    """A movable clock so lease, grant and report expiry are directly testable."""

    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> datetime:
        self.now = self.now + timedelta(**delta)
        return self.now


class RecordingExecutor:
    """Stand in for the runner pipeline and capture what the worker received."""

    def __init__(self, *, succeed: bool = True) -> None:
        self.succeed = succeed
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        *,
        plan: AnalysisPlan,
        data_dir: Path,
        workspace: Path,
    ) -> AnalysisExecutionReport:
        contents = {
            item.name: item.read_text(encoding="utf-8")
            for item in sorted(data_dir.rglob("*"))
            if item.is_file()
        }
        self.calls.append(
            {"plan": plan, "data_dir": data_dir, "workspace": workspace, "files": contents}
        )
        return AnalysisExecutionReport(
            succeeded=self.succeed,
            analysis_session_ids=("auto_default_20260813_20260813",),
            processed_dates=plan.target_dates,
            failed_scripts=() if self.succeed else ("data_pr_day",),
            script_results=({"script": "data_pr_day", "state": "done"},),
            reason_code=None if self.succeed else "analysis-script-failed",
        )


class FailingLifecycleTransport:
    """Carrier that cannot reach the selected worker."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    async def request(self, *, target_node_id, request):
        raise self.error

    async def cancel(self, *, target_node_id, request):
        raise self.error


def work_slice(
    *,
    session_id: str,
    target_date: str = "2026-08-13",
    source_signature: str = "a" * 64,
    script_keys: tuple[str, ...] = ("data_pr_day",),
    runtime_namespace: str = "default",
    origin: str = ORIGIN_AUTOMATIC_DISCOVERY,
) -> AnalysisWorkSlice:
    return AnalysisWorkSlice(
        session_id=session_id,
        slice_kind=SLICE_KIND_DATE,
        slice_key=target_date,
        target_dates=(target_date,),
        script_keys=script_keys,
        runtime_namespace=runtime_namespace,
        source_signature=source_signature,
        origin=origin,
    )


def source_files(root: Path, *, name: str = "day.jsonl", body: str | None = None):
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(
        body or '{"timestamp":"2026-08-13T10:00:00Z","machine":"A"}\n',
        encoding="utf-8",
    )
    return tuple(sorted(root.glob("*.jsonl")))


def provider_report(
    *,
    capability_id: str,
    node_id: str,
    session_id: str,
    descriptor,
    now: datetime,
    capability_type: str | None = None,
    protocol: str | None = None,
    protocol_version: str | None = None,
    attributes: dict[str, Any] | None = None,
    status: ProviderStatus = ProviderStatus.READY,
    max_concurrent_jobs: int = 2,
    active_jobs: int = 0,
) -> ProviderResourceReport:
    """Build a provider report the way the provisioner does, for negative cases."""

    from catalog.capabilities.worker_activation import ComputeHandlerActivationReference

    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type=capability_type or descriptor.capability_type,
        protocol=protocol or descriptor.protocol,
        protocol_version=protocol_version or descriptor.protocol_version,
        status=status,
        report_revision=1,
        max_concurrent_jobs=max_concurrent_jobs,
        active_jobs=active_jobs,
        queue_depth=0,
        utilization_millis=0,
        attributes=(
            attributes
            if attributes is not None
            else {
                **descriptor.attributes,
                "activation": ComputeHandlerActivationReference(
                    descriptor.handler_id, descriptor.descriptor_fingerprint
                ).to_dict(),
            }
        ),
        reported_at=now,
        expires_at=now + timedelta(minutes=5),
    )


class StaticReportSource:
    """Report source under direct test control, for negative selection cases."""

    def __init__(self, reports: tuple[ProviderResourceReport, ...] = ()) -> None:
        self.reports_value = reports

    def reports(
        self, *, session_id: str, capability_type: str
    ) -> tuple[ProviderResourceReport, ...]:
        # Filtered only by session so provider selection, not the source, is the
        # component under test for capability mismatches.
        return tuple(
            report for report in self.reports_value if report.session_id == session_id
        )


@dataclass
class AnalysisStack:
    """One device running the whole durable analysis architecture."""

    tmp_path: Path
    clock: Clock
    federation: DeviceFederationAuthority
    store: SQLiteJobLifecycleStore
    authority: SQLiteCapabilityArtifactAuthority
    content_store: LocalArtifactContentStore
    gateway: AnalysisArtifactGateway
    scheduler: FederatedAnalysisScheduler
    service: AnalysisWorkService
    executor: RecordingExecutor
    transport: LocalLifecycleTransport
    provisioner: AnalysisProviderProvisioner
    data_dir: Path
    session_id: str
    node_id: str
    provider_id: str
    worker: Any = None
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def coordinator_node_id(self) -> str:
        return self.node_id

    @property
    def worker_node_id(self) -> str:
        return self.node_id

    def submit(self, work: AnalysisWorkSlice | None = None, **kwargs):
        work = work or work_slice(session_id=self.session_id, **kwargs)
        return self.service.submit_analysis_work(
            work,
            slice_files=source_files(self.data_dir),
            slice_root=self.data_dir,
        )

    def use_report_source(self, source) -> None:
        """Swap in a controlled report source for negative selection tests."""

        self.scheduler.report_source = source


def build_stack(
    tmp_path: Path,
    *,
    clock: Clock | None = None,
    succeed: bool = True,
    activate_provider: bool = True,
) -> AnalysisStack:
    """Build a device with the real F8 trust chain and F7.5 lifecycle stack."""

    clock = clock or Clock()
    capability_root = tmp_path / "capabilities"
    capability_root.mkdir(parents=True, exist_ok=True)
    credentials = IdentityStore(
        capability_root / "standalone_identity", display_name="Harness device"
    ).load_or_create(now=clock.now)
    node_id = credentials.identity.node_id
    session_id = "session-analysis-harness"

    @dataclass(frozen=True)
    class _Identity:
        session_id: str
        node_id: str

    federation = DeviceFederationAuthority.for_device(
        capability_root=capability_root,
        identity=_Identity(session_id, node_id),
        clock=clock,
    )
    store = SQLiteJobLifecycleStore(capability_root / "analysis_jobs.sqlite3")
    authority = SQLiteCapabilityArtifactAuthority(store)
    content_store = LocalArtifactContentStore(capability_root / "artifacts")
    gateway = AnalysisArtifactGateway(authority, content_store)
    transport = federation.lifecycle_transport()
    carrier = LocalAnalysisArtifactCarrier(gateway, local_node_id=node_id, clock=clock)
    executor = RecordingExecutor(succeed=succeed)
    provider_id = analysis_capability_id(node_id)

    handler = FederatedAnalysisHandler(
        session_id=session_id,
        node_id=node_id,
        provider_id=provider_id,
        artifact_transport=carrier,
        executor=executor,
        workspace_root=capability_root / "workspaces",
        content_store=content_store,
        clock=clock,
        data_owner_node_id=lambda _job: node_id,
    )
    provisioner = AnalysisProviderProvisioner(
        coordinator=federation.coordinator,
        enrollments=federation.enrollments,
        health=federation.health,
        inventory=federation.inventory,
        binder=federation.binder,
        session_id=session_id,
        node_id=node_id,
        clock=clock,
        max_concurrent_jobs=2,
    )
    worker = None
    if activate_provider:
        worker = provisioner.provision(handler, transport).worker

    scheduler = FederatedAnalysisScheduler(
        store=store,
        gateway=gateway,
        transport=transport,
        report_source=FederatedProviderReportSource(
            federation.health, actor_node_id=node_id
        ),
        coordinator_node_id=node_id,
        clock=clock,
    )
    service = AnalysisWorkService(
        scheduler=scheduler,
        registry=AnalysisJobRegistry(capability_root / "analysis_jobs.sqlite3"),
        session_id=session_id,
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return AnalysisStack(
        tmp_path=tmp_path,
        clock=clock,
        federation=federation,
        store=store,
        authority=authority,
        content_store=content_store,
        gateway=gateway,
        scheduler=scheduler,
        service=service,
        executor=executor,
        transport=transport,
        provisioner=provisioner,
        data_dir=data_dir,
        session_id=session_id,
        node_id=node_id,
        provider_id=provider_id,
        worker=worker,
        extras={"handler": handler, "carrier": carrier},
    )


__all__ = [
    "NOW",
    "AnalysisStack",
    "Clock",
    "FailingLifecycleTransport",
    "RecordingExecutor",
    "StaticReportSource",
    "build_stack",
    "provider_report",
    "source_files",
    "work_slice",
]

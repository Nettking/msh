"""Shared fixtures for the federated analysis job tests.

The harness wires the real capability stack (durable job store, artifact
authority, provider selection, dispatch, lifecycle) around a fake analysis
executor, so tests exercise the production scheduling and authorization paths.
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
    LocalAnalysisProviderSource,
    LocalArtifactContentStore,
    NodeRoutedDispatchTransport,
    analysis_provider_attributes,
)
from catalog.capabilities.analysis.contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_PROTOCOL,
    ANALYSIS_PROTOCOL_VERSION,
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.artifact_secure_runtime import (
    SQLiteCapabilityArtifactAuthority,
)
from catalog.capabilities.dispatch import (
    CapabilityWorker,
    DispatchRequest,
    DispatchResponse,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)
SESSION = "session-analysis-tests"
COORDINATOR = "node-coordinator"
WORKER_NODE = "node-worker"
PROVIDER = "provider-analysis-worker"


class Clock:
    """A movable clock so lease and grant expiry can be exercised directly."""

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


class StaticReportSource:
    """A provider report source under direct test control."""

    def __init__(self, reports: tuple[ProviderResourceReport, ...] = ()) -> None:
        self.reports_value = reports

    def reports(
        self, *, session_id: str, capability_type: str
    ) -> tuple[ProviderResourceReport, ...]:
        # Deliberately filtered only by session so provider selection, not the
        # report source, is the component under test for capability mismatches.
        return tuple(
            report
            for report in self.reports_value
            if report.session_id == session_id
        )


class DirectRelayTransport:
    """Authenticated carrier double for dispatch to another federation node."""

    def __init__(self, workers: dict[str, CapabilityWorker] | None = None) -> None:
        self.workers = dict(workers or {})
        self.delivered: list[DispatchRequest] = []
        self.failure: Exception | None = None

    async def request(
        self, *, target_node_id: str, request: DispatchRequest
    ) -> DispatchResponse:
        if self.failure is not None:
            raise self.failure
        self.delivered.append(request)
        worker = self.workers[target_node_id]
        return await worker.handle(
            request,
            authenticated_actor=request.coordinator_node_id,
            authenticated_session=request.session_id,
        )


def provider_report(
    *,
    capability_id: str = PROVIDER,
    node_id: str = WORKER_NODE,
    session_id: str = SESSION,
    now: datetime = NOW,
    capability_type: str = ANALYSIS_CAPABILITY_TYPE,
    protocol: str = ANALYSIS_PROTOCOL,
    protocol_version: str = ANALYSIS_PROTOCOL_VERSION,
    attributes: dict[str, Any] | None = None,
    status: ProviderStatus = ProviderStatus.READY,
    max_concurrent_jobs: int = 2,
    active_jobs: int = 0,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type=capability_type,
        protocol=protocol,
        protocol_version=protocol_version,
        status=status,
        report_revision=1,
        max_concurrent_jobs=max_concurrent_jobs,
        active_jobs=active_jobs,
        queue_depth=0,
        utilization_millis=0,
        attributes=(
            analysis_provider_attributes() if attributes is None else attributes
        ),
        reported_at=now,
        expires_at=now + timedelta(minutes=5),
    )


def work_slice(
    *,
    session_id: str = SESSION,
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
    return (path,)


@dataclass
class AnalysisStack:
    """Everything one federation node needs to schedule and run analysis."""

    tmp_path: Path
    clock: Clock
    store: SQLiteJobLifecycleStore
    authority: SQLiteCapabilityArtifactAuthority
    content_store: LocalArtifactContentStore
    gateway: AnalysisArtifactGateway
    report_source: StaticReportSource
    scheduler: FederatedAnalysisScheduler
    service: AnalysisWorkService
    executor: RecordingExecutor
    worker: CapabilityWorker
    relay: DirectRelayTransport
    data_dir: Path
    session_id: str = SESSION
    coordinator_node_id: str = COORDINATOR
    worker_node_id: str = WORKER_NODE
    provider_id: str = PROVIDER
    handlers: dict[str, FederatedAnalysisHandler] = field(default_factory=dict)

    def submit(self, work: AnalysisWorkSlice | None = None, **kwargs):
        work = work or work_slice(**kwargs)
        return self.service.submit_analysis_work(
            work,
            slice_files=source_files(self.data_dir),
            slice_root=self.data_dir,
        )


def build_stack(
    tmp_path: Path,
    *,
    session_id: str = SESSION,
    coordinator_node_id: str = COORDINATOR,
    worker_node_id: str = WORKER_NODE,
    provider_id: str = PROVIDER,
    clock: Clock | None = None,
    succeed: bool = True,
    with_provider_report: bool = True,
    local_worker: bool = False,
) -> AnalysisStack:
    """Build a coordinator with one worker, either remote or on its own node."""

    clock = clock or Clock()
    database = tmp_path / "capabilities" / "analysis_jobs.sqlite3"
    store = SQLiteJobLifecycleStore(database)
    authority = SQLiteCapabilityArtifactAuthority(store)
    content_store = LocalArtifactContentStore(tmp_path / "artifacts")
    gateway = AnalysisArtifactGateway(authority, content_store)
    executor = RecordingExecutor(succeed=succeed)
    node_id = coordinator_node_id if local_worker else worker_node_id

    handler = FederatedAnalysisHandler(
        session_id=session_id,
        node_id=node_id,
        provider_id=provider_id,
        input_transport=gateway,
        executor=executor,
        workspace_root=tmp_path / "workspaces",
        content_store=content_store,
        clock=clock,
    )
    worker = CapabilityWorker(
        WorkerRegistration(
            session_id=session_id,
            node_id=node_id,
            provider_id=provider_id,
            capability_type=ANALYSIS_CAPABILITY_TYPE,
            protocol=ANALYSIS_PROTOCOL,
            protocol_version=ANALYSIS_PROTOCOL_VERSION,
            attributes=analysis_provider_attributes(),
        ),
        handler,
        SQLiteDispatchInbox(tmp_path / "inbox.sqlite3"),
        clock=clock,
    )
    relay = DirectRelayTransport({node_id: worker})
    transport = NodeRoutedDispatchTransport(
        local_node_id=coordinator_node_id,
        local_workers={provider_id: worker} if local_worker else {},
        remote=relay,
    )
    report_source = StaticReportSource(
        (provider_report(capability_id=provider_id, node_id=node_id, now=clock.now),)
        if with_provider_report
        else ()
    )
    scheduler = FederatedAnalysisScheduler(
        store=store,
        gateway=gateway,
        transport=transport,
        report_source=report_source,
        coordinator_node_id=coordinator_node_id,
        clock=clock,
    )
    service = AnalysisWorkService(
        scheduler=scheduler,
        registry=AnalysisJobRegistry(database),
        session_id=session_id,
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return AnalysisStack(
        tmp_path=tmp_path,
        clock=clock,
        store=store,
        authority=authority,
        content_store=content_store,
        gateway=gateway,
        report_source=report_source,
        scheduler=scheduler,
        service=service,
        executor=executor,
        worker=worker,
        relay=relay,
        data_dir=data_dir,
        session_id=session_id,
        coordinator_node_id=coordinator_node_id,
        worker_node_id=node_id,
        provider_id=provider_id,
        handlers={provider_id: handler},
    )


__all__ = [
    "COORDINATOR",
    "NOW",
    "PROVIDER",
    "SESSION",
    "WORKER_NODE",
    "AnalysisStack",
    "Clock",
    "DirectRelayTransport",
    "LocalAnalysisProviderSource",
    "RecordingExecutor",
    "StaticReportSource",
    "build_stack",
    "provider_report",
    "source_files",
    "work_slice",
]

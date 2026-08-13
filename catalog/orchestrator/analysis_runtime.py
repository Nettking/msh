"""Compose the federated analysis job runtime for this device.

This module is the only place that knows how the generic capability machinery is
wired on a running MSH node:

* which durable stores back jobs, artifacts, and the job index,
* which identity this node uses as coordinator, data owner, and provider,
* which local handler executes analysis (the existing runner pipeline),
* how dispatch reaches a local worker or a remote federation node.

Nothing here re-implements analysis, scheduling, ownership, or authorization.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.analysis import (
    AnalysisArtifactGateway,
    AnalysisExecutionReport,
    AnalysisJobRegistry,
    AnalysisPlan,
    AnalysisWorkService,
    AnalysisWorkSlice,
    CompositeProviderReportSource,
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
    DEFAULT_MAX_SLICE_BYTES,
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.analysis.scheduler import SubmissionOutcome
from catalog.capabilities.artifact_secure_runtime import (
    SQLiteCapabilityArtifactAuthority,
)
from catalog.capabilities.dispatch import (
    CapabilityWorker,
    SQLiteDispatchInbox,
    WorkerRegistration,
)
from catalog.capabilities.jobs import JobStatus
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.retry_claim import attempt_owner
from catalog.runner.data_filtering import source_files_for_dates
from catalog.runner.script_catalog import discover_runnable_scripts, repo_root

from .pipeline import StatusPrinter, _run_for_date_slice

#: The supplier that binds this runtime to a real federation identity. The Flask
#: application registers one during startup; without it the device runs the same
#: architecture inside its own single-node federation session. One slot, so a
#: restarted application replaces the binding instead of accumulating closures.
_IDENTITY_SUPPLIER: Callable[[], tuple[str, str] | None] | None = None
_IDENTITY_LOCK = threading.Lock()

_RUNTIME: AnalysisRuntime | None = None
_RUNTIME_LOCK = threading.Lock()


def register_identity_supplier(
    supplier: Callable[[], tuple[str, str] | None],
) -> None:
    """Register the ``() -> (session_id, node_id)`` federation identity supplier."""

    global _IDENTITY_SUPPLIER
    with _IDENTITY_LOCK:
        _IDENTITY_SUPPLIER = supplier


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class AnalysisIdentity:
    """Who this node is when it schedules, owns, and executes analysis."""

    session_id: str
    node_id: str
    provider_id: str
    standalone: bool

    @property
    def coordinator_node_id(self) -> str:
        return self.node_id


def _provider_id(node_id: str) -> str:
    """Derive this node's analysis provider identity.

    Ownership may never be granted by the provider that receives it, so the
    provider identity is deliberately distinct from the coordinator node ID.
    """

    digest = hashlib.sha256(f"analysis-provider\0{node_id}".encode()).hexdigest()
    return f"analysis-provider-{digest[:24]}"


def _standalone_identity(state_path: Path) -> tuple[str, str]:
    """Return the persisted single-node federation identity, creating it once.

    Standalone installations are a supported product mode. They are modelled as a
    federation of one: a real session, a real coordinator node, and a real
    provider. Authority checks are enforced exactly as in a multi-node
    federation; nothing is bypassed to make this mode work.
    """

    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            session_id = str(payload["session_id"])
            node_id = str(payload["node_id"])
            if session_id and node_id:
                return session_id, node_id
        except (OSError, ValueError, KeyError):
            pass
    session_id = f"session-standalone-{uuid.uuid4().hex}"
    node_id = f"node-standalone-{uuid.uuid4().hex}"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {"session_id": session_id, "node_id": node_id, "mode": "standalone"},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return session_id, node_id


def resolve_analysis_identity(state_path: Path) -> AnalysisIdentity:
    """Resolve the federation identity this runtime binds to."""

    session_id = str(os.getenv("FCP_ANALYSIS_SESSION_ID", "")).strip()
    node_id = str(os.getenv("FCP_ANALYSIS_NODE_ID", "")).strip()
    if session_id and node_id:
        return AnalysisIdentity(session_id, node_id, _provider_id(node_id), False)
    with _IDENTITY_LOCK:
        supplier = _IDENTITY_SUPPLIER
    if supplier is not None:
        try:
            resolved = supplier()
        except Exception:  # noqa: BLE001 - a failing supplier must not break startup
            resolved = None
        if resolved is not None:
            session_id, node_id = (str(item).strip() for item in resolved)
            if session_id and node_id:
                return AnalysisIdentity(
                    session_id, node_id, _provider_id(node_id), False
                )
    session_id, node_id = _standalone_identity(state_path)
    return AnalysisIdentity(session_id, node_id, _provider_id(node_id), True)


class RunnerSliceAnalysisExecutor:
    """Execute an authorized slice with the existing runner analysis pipeline."""

    def __init__(
        self,
        *,
        workflows_root: Path,
        catalog_root: Path,
        status: StatusPrinter | None = None,
    ) -> None:
        self.workflows_root = Path(workflows_root)
        self.catalog_root = Path(catalog_root)
        self.status = status or StatusPrinter()

    def execute(
        self,
        *,
        plan: AnalysisPlan,
        data_dir: Path,
        workspace: Path,
    ) -> AnalysisExecutionReport:
        script_options = discover_runnable_scripts(self.catalog_root)
        discovered = {item.key for item in script_options}
        # Only locally discovered scripts may run. A plan can never name an
        # executable, a path, or a command; unknown keys are simply skipped.
        script_keys = tuple(key for key in plan.script_keys if key in discovered)
        if not script_keys:
            return AnalysisExecutionReport(
                succeeded=False, reason_code="analysis-scripts-unavailable"
            )
        self.workflows_root.mkdir(parents=True, exist_ok=True)
        sessions: list[str] = []
        processed: list[str] = []
        failed: list[str] = []
        results: list[dict[str, Any]] = []
        total = len(plan.target_dates)
        for index, iso_date in enumerate(plan.target_dates):
            outcome = _run_for_date_slice(
                status=self.status,
                workflows_root=self.workflows_root,
                data_dir=data_dir,
                script_options=script_options,
                target_day=date.fromisoformat(iso_date),
                script_keys=script_keys,
                run_label="federated_analysis_job",
                runtime_namespace=plan.runtime_namespace,
                active_slice=iso_date,
                remaining_slices=max(0, total - index - 1),
            )
            sessions.append(outcome.session_id)
            processed.append(iso_date)
            failed.extend(outcome.failed_scripts)
            results.extend(outcome.script_results)
        return AnalysisExecutionReport(
            succeeded=not failed,
            analysis_session_ids=tuple(dict.fromkeys(sessions)),
            processed_dates=tuple(processed),
            failed_scripts=tuple(dict.fromkeys(failed)),
            script_results=tuple(results),
            reason_code=None if not failed else "analysis-script-failed",
            details={"workspace_kind": "isolated-job-workspace"},
        )


class AnalysisRuntime:
    """Durable stores, identity, local provider, and the shared work service."""

    def __init__(
        self,
        *,
        root: Path | None = None,
        identity: AnalysisIdentity | None = None,
        clock: Callable[[], datetime] = _utc_now,
        enable_local_provider: bool = True,
        max_slice_bytes: int | None = None,
    ) -> None:
        self.root = Path(root) if root is not None else repo_root()
        self.capability_root = self.root / "results" / "capabilities"
        self.capability_root.mkdir(parents=True, exist_ok=True)
        self.identity = identity or resolve_analysis_identity(
            self.capability_root / "analysis_identity.json"
        )
        self.clock = clock
        self.max_slice_bytes = int(
            max_slice_bytes
            if max_slice_bytes is not None
            else os.getenv("FCP_ANALYSIS_MAX_SLICE_BYTES", DEFAULT_MAX_SLICE_BYTES)
        )

        self.store = SQLiteJobLifecycleStore(self.capability_root / "analysis_jobs.sqlite3")
        self.artifact_authority = SQLiteCapabilityArtifactAuthority(self.store)
        self.content_store = LocalArtifactContentStore(
            self.capability_root / "artifacts", max_bytes=self.max_slice_bytes
        )
        self.gateway = AnalysisArtifactGateway(self.artifact_authority, self.content_store)
        self.transport = NodeRoutedDispatchTransport(local_node_id=self.identity.node_id)
        self._report_sources: list[object] = []
        self.service: AnalysisWorkService | None = None
        self.max_concurrent_jobs = max(
            int(os.getenv("FCP_ANALYSIS_MAX_CONCURRENT_JOBS", "1")), 1
        )

        if enable_local_provider:
            self._install_local_provider()

        self.scheduler = FederatedAnalysisScheduler(
            store=self.store,
            gateway=self.gateway,
            transport=self.transport,
            report_source=CompositeProviderReportSource(self._report_sources),
            coordinator_node_id=self.identity.coordinator_node_id,
            clock=self.clock,
        )
        self.service = AnalysisWorkService(
            scheduler=self.scheduler,
            registry=AnalysisJobRegistry(
                self.capability_root / "analysis_jobs.sqlite3"
            ),
            session_id=self.identity.session_id,
        )

    # ------------------------------------------------------------------

    def register_report_source(self, source: object) -> None:
        """Add another provider report source (for example provider health)."""

        self._report_sources.append(source)

    def _local_active_jobs(self) -> int:
        """Count jobs this node's provider currently owns, for honest capacity."""

        service = self.service
        if service is None:
            return 0
        active = 0
        for job_id in service.pending_job_ids():
            try:
                snapshot = self.store.snapshot(job_id)
            except Exception:  # noqa: BLE001,S112 - one unreadable job must not hide load
                continue
            ownership = snapshot.ownership
            if ownership is not None and (
                ownership.owner_provider_id == self.identity.provider_id
            ):
                active += 1
        return active

    def _install_local_provider(self) -> None:
        """Offer this node's own compute as an ordinary federation provider."""

        handler = FederatedAnalysisHandler(
            session_id=self.identity.session_id,
            node_id=self.identity.node_id,
            provider_id=self.identity.provider_id,
            input_transport=self.gateway,
            executor=RunnerSliceAnalysisExecutor(
                workflows_root=self.root / "results" / "workflows",
                catalog_root=self.root / "catalog",
            ),
            workspace_root=self.capability_root / "workspaces",
            content_store=self.content_store,
            clock=self.clock,
            max_slice_bytes=self.max_slice_bytes,
        )
        worker = CapabilityWorker(
            WorkerRegistration(
                session_id=self.identity.session_id,
                node_id=self.identity.node_id,
                provider_id=self.identity.provider_id,
                capability_type=ANALYSIS_CAPABILITY_TYPE,
                protocol=ANALYSIS_PROTOCOL,
                protocol_version=ANALYSIS_PROTOCOL_VERSION,
                attributes=analysis_provider_attributes(),
            ),
            handler,
            SQLiteDispatchInbox(self.capability_root / "analysis_dispatch_inbox.sqlite3"),
            clock=self.clock,
        )
        self.transport.register_worker(self.identity.provider_id, worker)
        self._report_sources.append(
            LocalAnalysisProviderSource(
                session_id=self.identity.session_id,
                node_id=self.identity.node_id,
                provider_id=self.identity.provider_id,
                clock=self.clock,
                max_concurrent_jobs=self.max_concurrent_jobs,
                active_jobs=self._local_active_jobs,
            )
        )


class DiscoveryAnalysisGateway:
    """Turn discovered or uploaded data slices into durable federation jobs.

    Automatic discovery and the manual upload workflow both call this gateway, so
    there is exactly one path from "new JSONL exists" to "a durable job exists".
    """

    def __init__(self, runtime: AnalysisRuntime | None = None) -> None:
        self._runtime = runtime

    @property
    def runtime(self) -> AnalysisRuntime:
        return self._runtime if self._runtime is not None else get_analysis_runtime()

    # ------------------------------------------------------------------

    def submit_slice(
        self,
        *,
        data_dir: Path,
        target_dates: Sequence[date | str],
        script_keys: Sequence[str],
        runtime_namespace: str,
        source_signature: str,
        slice_kind: str,
        slice_key: str,
        origin: str,
    ) -> SubmissionOutcome | None:
        runtime = self.runtime
        isolated = tuple(
            item.isoformat() if isinstance(item, date) else str(item)
            for item in target_dates
        )
        files = source_files_for_dates(data_dir, isolated)
        if not files:
            return None
        work = AnalysisWorkSlice(
            session_id=runtime.identity.session_id,
            slice_kind=slice_kind,
            slice_key=slice_key,
            target_dates=isolated,
            script_keys=tuple(script_keys),
            runtime_namespace=runtime_namespace,
            source_signature=source_signature,
            origin=origin,
        )
        service = runtime.service
        if service is None:  # pragma: no cover - constructed with the runtime
            return None
        return service.submit_analysis_work(
            work, slice_files=files, slice_root=data_dir
        )

    def submit_date_slice(
        self,
        *,
        data_dir: Path,
        target_day: date,
        script_keys: Sequence[str],
        runtime_namespace: str,
        source_signature: str,
        origin: str = ORIGIN_AUTOMATIC_DISCOVERY,
    ) -> SubmissionOutcome | None:
        return self.submit_slice(
            data_dir=data_dir,
            target_dates=(target_day,),
            script_keys=script_keys,
            runtime_namespace=runtime_namespace,
            source_signature=source_signature,
            slice_kind=SLICE_KIND_DATE,
            slice_key=target_day.isoformat(),
            origin=origin,
        )

    def request_scheduling_pass(self) -> bool:
        service = self.runtime.service
        return False if service is None else service.request_scheduling_pass()

    def run_scheduling_pass(self):
        service = self.runtime.service
        return () if service is None else service.run_scheduling_pass()

    # ------------------------------------------------------------------

    def job_view(self, job_id: str) -> dict[str, Any] | None:
        """Return the durable job facts product views need, or ``None``."""

        runtime = self.runtime
        try:
            snapshot = runtime.store.snapshot(job_id)
        except Exception:  # noqa: BLE001 - a missing job is not a runtime failure
            return None
        job = snapshot.job
        attempts = job.attempts
        provider_id: str | None = None
        if snapshot.ownership is not None:
            provider_id = snapshot.ownership.owner_provider_id
        elif attempts:
            try:
                provider_id = attempt_owner(runtime.store, job_id, len(attempts))
            except Exception:  # noqa: BLE001 - provider history is best effort
                provider_id = None
        record = None
        service = runtime.service
        if service is not None:
            record = service.registry.record_for(job_id)
        try:
            committed = runtime.store.result_commit(job_id)
        except Exception:  # noqa: BLE001 - result history is best effort
            committed = None
        return {
            "job_id": job_id,
            "session_id": job.session_id,
            "status": job.status.value,
            "terminal": job.terminal,
            "succeeded": job.status is JobStatus.SUCCEEDED,
            "attempt_count": len(attempts),
            "attempt_status": attempts[-1].status.value if attempts else None,
            "error_code": attempts[-1].error_code if attempts else None,
            "provider_id": provider_id,
            "node_id": self._node_for_provider(provider_id),
            "lease_expires_at": (
                None
                if snapshot.ownership is None
                else snapshot.ownership.lease_expires_at.isoformat().replace(
                    "+00:00", "Z"
                )
            ),
            "input_artifacts": [item.reference_id for item in job.inputs],
            "output_artifact": (
                None if committed is None else committed.reference.reference_id
            ),
            "slice_kind": None if record is None else record.slice_kind,
            "slice_key": None if record is None else record.slice_key,
            "origin": None if record is None else record.origin,
            "source_signature": None if record is None else record.source_signature,
            "target_dates": [] if record is None else list(record.target_dates),
        }

    def _node_for_provider(self, provider_id: str | None) -> str | None:
        if provider_id is None:
            return None
        runtime = self.runtime
        for source in runtime._report_sources:
            for report in source.reports(  # type: ignore[attr-defined]
                session_id=runtime.identity.session_id,
                capability_type=ANALYSIS_CAPABILITY_TYPE,
            ):
                if report.capability_id == provider_id:
                    return report.node_id
        return None


def get_analysis_runtime() -> AnalysisRuntime:
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is None:
            _RUNTIME = AnalysisRuntime()
        return _RUNTIME


def reset_analysis_runtime() -> None:
    """Drop the cached runtime (used by tests and identity rebinding)."""

    global _RUNTIME
    with _RUNTIME_LOCK:
        _RUNTIME = None


__all__ = [
    "AnalysisIdentity",
    "AnalysisRuntime",
    "DiscoveryAnalysisGateway",
    "RunnerSliceAnalysisExecutor",
    "get_analysis_runtime",
    "register_identity_supplier",
    "reset_analysis_runtime",
    "resolve_analysis_identity",
]

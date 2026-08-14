"""Compose the federated analysis job runtime for this device.

This module is the only place that knows how the generic capability machinery is
wired on a running FCP node: durable stores, identity, trusted providers,
dispatch transport, and the existing analysis executor. Execution-efficiency
learning is composed here as an observer/ranker around those existing paths; it
does not create a second scheduler, worker, or transport protocol.
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
    AnalysisProviderProvisioner,
    AnalysisWorkService,
    AnalysisWorkSlice,
    FederatedAnalysisHandler,
    FederatedAnalysisScheduler,
    FederatedProviderReportSource,
    LocalArtifactContentStore,
    analysis_capability_id,
)
from catalog.capabilities.analysis.contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    DEFAULT_MAX_SLICE_BYTES,
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
)
from catalog.capabilities.analysis.provisioning import dispatched_data_owner_node_id
from catalog.capabilities.analysis.scheduler import SubmissionOutcome
from catalog.capabilities.artifact_secure_runtime import SQLiteCapabilityArtifactAuthority
from catalog.capabilities.efficiency import ExecutionEfficiencyRuntime
from catalog.capabilities.jobs import JobStatus
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.capabilities.retry_claim import attempt_owner
from catalog.node.identity import IdentityStore
from catalog.orchestrator.analysis_federation import DeviceFederationAuthority
from catalog.runner.data_filtering import source_files_for_dates
from catalog.runner.script_catalog import discover_runnable_scripts, repo_root

from .pipeline import StatusPrinter, _run_for_date_slice

_IDENTITY_SUPPLIER: Callable[[], tuple[str, str] | None] | None = None
_IDENTITY_LOCK = threading.Lock()

_FEDERATION_SUPPLIER: (
    Callable[
        ["AnalysisIdentity", Path, Callable[[], datetime]],
        DeviceFederationAuthority | None,
    ]
    | None
) = None
_FEDERATION_GENERATION_SUPPLIER: Callable[[], int] | None = None
_FEDERATION_LOCK = threading.Lock()

_RUNTIME: AnalysisRuntime | None = None
_RUNTIME_LOCK = threading.Lock()


def register_identity_supplier(
    supplier: Callable[[], tuple[str, str] | None],
) -> None:
    """Register the ``() -> (session_id, node_id)`` federation identity supplier."""

    global _IDENTITY_SUPPLIER
    with _IDENTITY_LOCK:
        _IDENTITY_SUPPLIER = supplier


def register_federation_supplier(
    supplier: Callable[
        ["AnalysisIdentity", Path, Callable[[], datetime]],
        DeviceFederationAuthority | None,
    ],
    *,
    generation_supplier: Callable[[], int] | None = None,
) -> None:
    """Register the authenticated federation path and its relay binding generation."""

    global _FEDERATION_SUPPLIER, _FEDERATION_GENERATION_SUPPLIER
    with _FEDERATION_LOCK:
        _FEDERATION_SUPPLIER = supplier
        _FEDERATION_GENERATION_SUPPLIER = generation_supplier


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
    """Derive this node's analysis provider identity."""

    return analysis_capability_id(node_id)


def _standalone_identity(state_path: Path) -> tuple[str, str]:
    """Return the persisted single-node federation identity, creating it once."""

    credentials = IdentityStore(
        state_path.parent / "standalone_identity",
        display_name="This device",
    ).load_or_create()
    node_id = credentials.identity.node_id
    if state_path.exists():
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            session_id = str(payload["session_id"])
            if session_id and str(payload.get("node_id")) == node_id:
                return session_id, node_id
        except (OSError, ValueError, KeyError):
            pass
    session_id = f"session-standalone-{uuid.uuid4().hex}"
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
                return AnalysisIdentity(session_id, node_id, _provider_id(node_id), False)
    session_id, node_id = _standalone_identity(state_path)
    return AnalysisIdentity(session_id, node_id, _provider_id(node_id), True)


def _federation_generation(identity: AnalysisIdentity) -> int:
    """Return the current authenticated relay binding generation, fail-closed."""

    if identity.standalone:
        return 0
    with _FEDERATION_LOCK:
        supplier = _FEDERATION_GENERATION_SUPPLIER
    if supplier is None:
        return 0
    try:
        generation = supplier()
    except Exception:  # noqa: BLE001 - connectivity must not break app startup
        return 0
    if isinstance(generation, bool) or not isinstance(generation, int):
        return 0
    return max(generation, 0)


def _efficiency_database(capability_root: Path, session_id: str) -> Path:
    """Keep learned profiles isolated between federation sessions."""

    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:20]
    return capability_root / "efficiency" / f"analysis-{digest}.sqlite3"


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
        federation: DeviceFederationAuthority | None = None,
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
        self.max_concurrent_jobs = max(
            int(os.getenv("FCP_ANALYSIS_MAX_CONCURRENT_JOBS", "1")), 1
        )
        self.service: AnalysisWorkService | None = None
        self.provisioner: AnalysisProviderProvisioner | None = None
        self.worker: object | None = None

        self.federation = federation
        self.provisioning_reason = "provider-active"
        if self.federation is None and not self.identity.standalone:
            with _FEDERATION_LOCK:
                federation_supplier = _FEDERATION_SUPPLIER
            if federation_supplier is not None:
                try:
                    self.federation = federation_supplier(
                        self.identity, self.capability_root, self.clock
                    )
                except Exception as exc:  # noqa: BLE001 - startup stays available
                    self.provisioning_reason = str(
                        getattr(exc, "code", "federation-authority-unavailable")
                    )
        if self.federation is None and self.identity.standalone:
            try:
                self.federation = DeviceFederationAuthority.for_device(
                    capability_root=self.capability_root,
                    identity=self.identity,
                    clock=self.clock,
                )
            except Exception as exc:  # noqa: BLE001 - startup stays available
                self.provisioning_reason = str(
                    getattr(exc, "code", "federation-authority-unavailable")
                )
        if self.federation is None:
            self.federation = DeviceFederationAuthority.without_authority(
                capability_root=self.capability_root,
                node_id=self.identity.node_id,
                clock=self.clock,
            )
            enable_local_provider = False
            if self.provisioning_reason == "provider-active":
                self.provisioning_reason = "federation-transport-unavailable"

        # The raw transport is retained for F8 provider activation. The
        # scheduler receives a thin observing wrapper; no second transport or
        # reader is created.
        self.transport = self.federation.lifecycle_transport()
        self.artifact_carrier = self.federation.artifact_carrier(self.gateway)
        self.report_source = FederatedProviderReportSource(
            self.federation.health, actor_node_id=self.identity.node_id
        )
        self.efficiency = ExecutionEfficiencyRuntime(
            _efficiency_database(self.capability_root, self.identity.session_id),
            node_id=self.identity.node_id,
        )
        self.scheduler_transport = self.efficiency.wrap_transport(
            self.transport,
            report_source=self.report_source,
        )

        if enable_local_provider:
            self._provision_local_provider()

        self.scheduler = FederatedAnalysisScheduler(
            store=self.store,
            gateway=self.gateway,
            transport=self.scheduler_transport,
            report_source=self.report_source,
            coordinator_node_id=self.identity.coordinator_node_id,
            clock=self.clock,
            ranker=self.efficiency.ranker,
        )
        self.service = AnalysisWorkService(
            scheduler=self.scheduler,
            registry=AnalysisJobRegistry(self.capability_root / "analysis_jobs.sqlite3"),
            session_id=self.identity.session_id,
        )
        self.federation_generation = _federation_generation(self.identity)

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
            if ownership is not None and ownership.owner_provider_id == self.identity.provider_id:
                active += 1
        return active

    def _provision_local_provider(self) -> None:
        """Offer this node's compute through the real F8 trust chain."""

        handler = FederatedAnalysisHandler(
            session_id=self.identity.session_id,
            node_id=self.identity.node_id,
            provider_id=analysis_capability_id(self.identity.node_id),
            artifact_transport=self.artifact_carrier,
            executor=RunnerSliceAnalysisExecutor(
                workflows_root=self.root / "results" / "workflows",
                catalog_root=self.root / "catalog",
            ),
            workspace_root=self.capability_root / "workspaces",
            content_store=self.content_store,
            clock=self.clock,
            data_owner_node_id=dispatched_data_owner_node_id,
            max_slice_bytes=self.max_slice_bytes,
        )
        self.provisioner = AnalysisProviderProvisioner(
            coordinator=self.federation.coordinator,
            enrollments=self.federation.enrollments,
            health=self.federation.health,
            inventory=self.federation.inventory,
            binder=self.federation.binder,
            session_id=self.identity.session_id,
            node_id=self.identity.node_id,
            clock=self.clock,
            max_concurrent_jobs=self.max_concurrent_jobs,
            active_jobs=self._local_active_jobs,
            provider_generation=self.federation.provider_generation,
        )
        outcome = self.provisioner.provision(handler, self.transport)
        self.worker = outcome.worker
        self.provisioning_reason = outcome.reason

    def refresh_provider_health(self) -> bool:
        """Keep this node's provider report and F8.4 activation in sync."""

        if self.provisioner is None:
            return False
        refreshed = self.provisioner.refresh()
        provisioned = getattr(self.provisioner, "_worker", None)
        self.worker = provisioned
        self.provisioning_reason = (
            "provider-active" if provisioned is not None else "activation-unavailable"
        )
        return refreshed

    def stop(self) -> None:
        """Fence background scheduling and release a superseded relay reader."""

        service = self.service
        if service is not None:
            service.stop_scheduling()
        close_transport = getattr(self.transport, "close", None)
        if callable(close_transport):
            try:
                close_transport()
            except Exception:  # noqa: BLE001 - replacement must remain fail-safe
                pass


class DiscoveryAnalysisGateway:
    """Turn discovered or uploaded data slices into durable federation jobs."""

    def __init__(self, runtime: AnalysisRuntime | None = None) -> None:
        self._runtime = runtime

    @property
    def runtime(self) -> AnalysisRuntime:
        return self._runtime if self._runtime is not None else get_analysis_runtime()

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
        return service.submit_analysis_work(work, slice_files=files, slice_root=data_dir)

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
        runtime = self.runtime
        runtime.refresh_provider_health()
        service = runtime.service
        return False if service is None else service.request_scheduling_pass()

    def run_scheduling_pass(self):
        runtime = self.runtime
        runtime.refresh_provider_health()
        service = runtime.service
        return () if service is None else service.run_scheduling_pass()

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
        for report in runtime.report_source.reports(
            session_id=runtime.identity.session_id,
            capability_type=ANALYSIS_CAPABILITY_TYPE,
        ):
            if report.capability_id == provider_id:
                return report.node_id
        return None


def _identity_key(identity: AnalysisIdentity) -> tuple[str, str, bool]:
    return identity.session_id, identity.node_id, identity.standalone


def get_analysis_runtime() -> AnalysisRuntime:
    """Return the singleton, rebuilding on identity or relay-binding changes."""

    global _RUNTIME
    root = repo_root()
    state_path = root / "results" / "capabilities" / "analysis_identity.json"
    desired = resolve_analysis_identity(state_path)
    desired_generation = _federation_generation(desired)
    superseded: AnalysisRuntime | None = None
    with _RUNTIME_LOCK:
        if (
            _RUNTIME is None
            or _identity_key(_RUNTIME.identity) != _identity_key(desired)
            or _RUNTIME.federation_generation != desired_generation
        ):
            replacement = AnalysisRuntime(root=root, identity=desired)
            superseded = _RUNTIME
            _RUNTIME = replacement
        current = _RUNTIME
    if superseded is not None:
        superseded.stop()
    return current


def reset_analysis_runtime() -> None:
    """Drop the cached runtime and fence any background scheduling it owned."""

    global _RUNTIME
    with _RUNTIME_LOCK:
        superseded = _RUNTIME
        _RUNTIME = None
    if superseded is not None:
        superseded.stop()


__all__ = [
    "AnalysisIdentity",
    "AnalysisRuntime",
    "DiscoveryAnalysisGateway",
    "RunnerSliceAnalysisExecutor",
    "get_analysis_runtime",
    "register_federation_supplier",
    "register_identity_supplier",
    "reset_analysis_runtime",
    "resolve_analysis_identity",
]

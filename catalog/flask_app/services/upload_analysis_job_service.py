"""Durable job lifecycle for user-requested JSONL background analysis."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.capabilities.job_store import DurableJobSnapshot, SQLiteJobStore
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.script_catalog import repo_root

_LOCAL_ANALYSIS_PROTOCOL = "msh-background-analysis"
_LOCAL_ANALYSIS_PROTOCOL_VERSION = "1.0"
_LEASE_SECONDS = 45 * 60
_RENEW_AFTER_SECONDS = 20 * 60
_DEFAULT_MONITOR_SECONDS = 7 * 24 * 60 * 60


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class UploadAnalysisJobError(RuntimeError):
    """Safe failure while creating or tracking an upload analysis job."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class UploadAnalysisJobService:
    """Bind local runtime refreshes to the existing durable job contracts."""

    def __init__(
        self,
        *,
        database: Path | str,
        runtime_manager: object,
        context_supplier: Callable[[], tuple[str, str]],
        clock: Callable[[], datetime] = _now,
        poll_seconds: float = 0.25,
        monitor_timeout_seconds: int = _DEFAULT_MONITOR_SECONDS,
    ) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.store = SQLiteJobStore(self.database)
        self.runtime_manager = runtime_manager
        self.context_supplier = context_supplier
        self.clock = clock
        self.poll_seconds = max(float(poll_seconds), 0.01)
        self.monitor_timeout_seconds = max(int(monitor_timeout_seconds), 1)
        self._lock = threading.Lock()
        self._initialize_links()
        self._recover_interrupted_jobs()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize_links(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("""
                CREATE TABLE IF NOT EXISTS data_upload_analysis_jobs (
                    job_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    coordinator_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    execution_id TEXT,
                    baseline_update_at TEXT,
                    created_at TEXT NOT NULL
                )
                """)
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(data_upload_analysis_jobs)"
                )
            }
            if "execution_id" not in columns:
                connection.execute(
                    "ALTER TABLE data_upload_analysis_jobs ADD COLUMN execution_id TEXT"
                )

    @staticmethod
    def _worker_id(coordinator_id: str) -> str:
        digest = hashlib.sha256(coordinator_id.encode("utf-8")).hexdigest()[:20]
        return f"analysis-worker-{digest}"

    @staticmethod
    def _job_id(batch_id: str) -> str:
        return f"analysis-{batch_id}"

    def _runtime_snapshot(self) -> dict[str, Any]:
        supplier = getattr(self.runtime_manager, "state_snapshot", None)
        if not callable(supplier):
            return {}
        value = supplier()
        return dict(value) if isinstance(value, Mapping) else {}

    def submit_batch(self, batch: Mapping[str, Any]) -> str:
        batch_id = str(batch.get("batch_id") or "").strip()
        if not batch_id:
            raise UploadAnalysisJobError(
                "analysis-batch-required",
                "The upload batch could not be identified safely.",
            )
        try:
            session_id, coordinator_id = self.context_supplier()
        except UploadAnalysisJobError:
            raise
        except Exception as exc:
            raise UploadAnalysisJobError(
                "analysis-federation-required",
                "Connect this device to its Federation before starting analysis.",
            ) from exc
        if not session_id or not coordinator_id:
            raise UploadAnalysisJobError(
                "analysis-federation-required",
                "Connect this device to its Federation before starting analysis.",
            )

        provider_id = self._worker_id(coordinator_id)
        job_id = self._job_id(batch_id)
        request_id = f"request-{batch_id}"
        identity = hashlib.sha256(f"{session_id}\0{batch_id}".encode()).hexdigest()
        job = JobContract(
            job_id=job_id,
            session_id=session_id,
            request_id=request_id,
            idempotency_key=f"upload-analysis:{identity}",
            capability=CapabilityRequirement(
                capability_type="background-analysis",
                protocol=_LOCAL_ANALYSIS_PROTOCOL,
                protocol_version=_LOCAL_ANALYSIS_PROTOCOL_VERSION,
                requirements={
                    "batch_id": batch_id,
                    "file_count": int(batch.get("file_count") or 0),
                    "record_count": int(batch.get("imported_records") or 0),
                    "total_bytes": int(batch.get("total_bytes") or 0),
                },
            ),
            inputs=(
                ArtifactReference(
                    reference_id=f"input-{batch_id}",
                    session_id=session_id,
                    schema_name="msh.jsonl-upload-batch.v1",
                    media_type="application/x-ndjson",
                ),
            ),
            outputs=(
                ArtifactReference(
                    reference_id=f"output-{batch_id}",
                    session_id=session_id,
                    schema_name="msh.analysis-workflow.v1",
                    media_type="application/json",
                ),
            ),
            retry_policy=RetryPolicy(
                max_attempts=1,
                backoff_seconds=(),
                retryable_error_codes=(),
            ),
            timeout_policy=TimeoutPolicy(
                overall_timeout_seconds=_DEFAULT_MONITOR_SECONDS,
                queue_timeout_seconds=60 * 60,
                start_timeout_seconds=60 * 60,
                run_timeout_seconds=_DEFAULT_MONITOR_SECONDS,
                cancellation_grace_seconds=60,
            ),
        )
        now = self.clock()
        submitted = self.store.submit(job, coordinator_id=coordinator_id, now=now)
        snapshot = submitted.snapshot
        if snapshot.job.status is JobStatus.SUBMITTED:
            snapshot = self.store.queue(
                job_id,
                coordinator_id=coordinator_id,
                command_id=f"queue-{batch_id}",
                expected_revision=snapshot.revision,
                now=now,
            ).snapshot
        baseline = self._runtime_snapshot().get("last_update_check_at")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO data_upload_analysis_jobs(
                    job_id,batch_id,session_id,coordinator_id,provider_id,
                    baseline_update_at,created_at,execution_id
                ) VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(job_id) DO NOTHING
                """,
                (
                    job_id,
                    batch_id,
                    session_id,
                    coordinator_id,
                    provider_id,
                    None if baseline is None else str(baseline),
                    _stamp(now),
                    job_id,
                ),
            )
        return job_id

    def _activate(self, job_id: str) -> DurableJobSnapshot:
        with self._lock:
            snapshot = self.store.snapshot(job_id)
            if snapshot.job.terminal:
                return snapshot
            link = self._link(job_id)
            now = self.clock()
            if snapshot.job.status is JobStatus.SUBMITTED:
                snapshot = self.store.queue(
                    job_id,
                    coordinator_id=link["coordinator_id"],
                    command_id=f"queue-{link['batch_id']}",
                    expected_revision=snapshot.revision,
                    now=now,
                ).snapshot
            if snapshot.job.status is JobStatus.QUEUED:
                snapshot = self.store.claim(
                    job_id,
                    coordinator_id=link["coordinator_id"],
                    owner_provider_id=link["provider_id"],
                    attempt_id=f"attempt-{link['batch_id']}",
                    lease_id=f"lease-{link['batch_id']}",
                    command_id=f"claim-{link['batch_id']}",
                    expected_revision=snapshot.revision,
                    lease_expires_at=now + timedelta(seconds=_LEASE_SECONDS),
                    now=now,
                ).snapshot
            ownership = snapshot.ownership
            if ownership is None:
                return snapshot
            if snapshot.job.attempts[-1].status is AttemptStatus.ASSIGNED:
                snapshot = self.store.record_attempt_status(
                    job_id,
                    coordinator_id=link["coordinator_id"],
                    owner_provider_id=link["provider_id"],
                    attempt_id=ownership.attempt_id,
                    lease_id=ownership.lease_id,
                    command_id=f"accept-{link['batch_id']}",
                    expected_revision=snapshot.revision,
                    target_status=AttemptStatus.ACCEPTED,
                    now=now,
                ).snapshot
                ownership = snapshot.ownership
            if (
                ownership is not None
                and snapshot.job.attempts[-1].status is AttemptStatus.ACCEPTED
            ):
                snapshot = self.store.record_attempt_status(
                    job_id,
                    coordinator_id=link["coordinator_id"],
                    owner_provider_id=link["provider_id"],
                    attempt_id=ownership.attempt_id,
                    lease_id=ownership.lease_id,
                    command_id=f"run-{link['batch_id']}",
                    expected_revision=snapshot.revision,
                    target_status=AttemptStatus.RUNNING,
                    now=now,
                ).snapshot
            return snapshot

    def start_tracking(self, job_id: str) -> None:
        snapshot = self._activate(job_id)
        if snapshot.job.terminal:
            return
        threading.Thread(
            target=self._monitor,
            args=(job_id,),
            name=f"msh-upload-analysis-{job_id[-8:]}",
            daemon=True,
        ).start()

    def fail(self, job_id: str, error_code: str) -> None:
        try:
            snapshot = self._activate(job_id)
            if snapshot.job.terminal:
                return
            self._complete(snapshot, success=False, error_code=error_code)
        except Exception:  # noqa: BLE001 - original safe route error remains primary
            return

    def _link(self, job_id: str) -> sqlite3.Row:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM data_upload_analysis_jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise UploadAnalysisJobError(
                "analysis-job-not-found",
                "The durable analysis job could not be found.",
            )
        return row

    def _complete(
        self,
        snapshot: DurableJobSnapshot,
        *,
        success: bool,
        error_code: str | None = None,
    ) -> None:
        ownership = snapshot.ownership
        if ownership is None or snapshot.job.terminal:
            return
        link = self._link(snapshot.job.job_id)
        self.store.complete(
            snapshot.job.job_id,
            coordinator_id=link["coordinator_id"],
            owner_provider_id=ownership.owner_provider_id,
            attempt_id=ownership.attempt_id,
            lease_id=ownership.lease_id,
            command_id=f"complete-{link['batch_id']}",
            expected_revision=snapshot.revision,
            terminal_status=(
                AttemptStatus.SUCCEEDED if success else AttemptStatus.FAILED
            ),
            error_code=None if success else (error_code or "analysis-runtime-failed"),
            now=self.clock(),
        )

    def _monitor(self, job_id: str) -> None:
        link = self._link(job_id)
        deadline = time.monotonic() + self.monitor_timeout_seconds
        last_renewal = time.monotonic()
        while time.monotonic() < deadline:
            state = self._runtime_snapshot()
            if state.get("completed_execution_id") == link["execution_id"]:
                snapshot = self.store.snapshot(job_id)
                failed = not bool(state.get("completed_execution_succeeded"))
                self._complete(
                    snapshot,
                    success=not failed,
                    error_code="analysis-step-failed" if failed else None,
                )
                return

            if time.monotonic() - last_renewal >= _RENEW_AFTER_SECONDS:
                snapshot = self.store.snapshot(job_id)
                ownership = snapshot.ownership
                if ownership is None or snapshot.job.terminal:
                    return
                renewal_now = self.clock()
                self.store.renew(
                    job_id,
                    coordinator_id=link["coordinator_id"],
                    owner_provider_id=ownership.owner_provider_id,
                    attempt_id=ownership.attempt_id,
                    lease_id=ownership.lease_id,
                    command_id=f"renew-{link['batch_id']}-{snapshot.revision}",
                    expected_revision=snapshot.revision,
                    lease_expires_at=renewal_now + timedelta(seconds=_LEASE_SECONDS),
                    now=renewal_now,
                )
                last_renewal = time.monotonic()
            time.sleep(self.poll_seconds)

        self.fail(job_id, "analysis-monitor-timeout")

    def snapshots(self, session_id: str) -> tuple[DurableJobSnapshot, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_id FROM data_upload_analysis_jobs
                WHERE session_id=? ORDER BY created_at DESC,job_id DESC
                """,
                (session_id,),
            ).fetchall()
        snapshots: list[DurableJobSnapshot] = []
        for row in rows:
            try:
                snapshots.append(self.store.snapshot(str(row["job_id"])))
            except Exception:  # noqa: BLE001,S112 - one damaged job must not hide others
                continue
        return tuple(snapshots)

    def _recover_interrupted_jobs(self) -> None:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT job_id FROM data_upload_analysis_jobs"
            ).fetchall()
        for row in rows:
            job_id = str(row["job_id"])
            try:
                snapshot = self.store.snapshot(job_id)
                if snapshot.job.terminal:
                    continue
                self.fail(job_id, "analysis-interrupted")
            except Exception:  # noqa: BLE001,S112 - startup remains available and fails closed
                continue


def _configured_database() -> Path:
    value = Path(
        str(
            current_app.config.get(
                "DATA_UPLOAD_DATABASE",
                "data/imports/uploads.sqlite3",
            )
        )
    )
    return value if value.is_absolute() else repo_root() / value


def _authorized_context() -> tuple[str, str]:
    from .capability_onboarding_service import get_capability_onboarding_service

    context = get_capability_onboarding_service().authorized_context()
    if context is None:
        raise UploadAnalysisJobError(
            "analysis-federation-required",
            "Connect this device to its Federation before starting analysis.",
        )
    return (
        context.binding.internal_session_id,
        context.credentials.identity.node_id,
    )


def get_upload_analysis_job_service() -> UploadAnalysisJobService:
    existing = current_app.extensions.get("upload_analysis_job_service")
    if isinstance(existing, UploadAnalysisJobService):
        return existing
    service = UploadAnalysisJobService(
        database=_configured_database(),
        runtime_manager=get_runtime_manager(),
        context_supplier=_authorized_context,
    )
    current_app.extensions["upload_analysis_job_service"] = service
    return service


__all__ = [
    "UploadAnalysisJobError",
    "UploadAnalysisJobService",
    "get_upload_analysis_job_service",
]

"""Durable, federation-dispatched analysis for user-uploaded JSONL.

The upload workflow used to create a durable job and then immediately claim it
for a synthetic worker derived from the coordinator, which made the job a wrapper
around local execution. It now submits the *same* work slices automatic discovery
submits, through the same service, so an uploaded batch is scheduled onto
whichever federation provider the scheduler selects.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.capabilities.analysis.contracts import (
    ORIGIN_MANUAL_UPLOAD,
    SLICE_KIND_DATE,
)
from catalog.capabilities.job_store import DurableJobSnapshot
from catalog.orchestrator.analysis_runtime import DiscoveryAnalysisGateway
from catalog.runner.data_filtering import (
    date_range_source_signature,
    discover_available_dates,
    source_date_signatures,
)
from catalog.runner.script_catalog import repo_root
from catalog.runner.session_store import AUTOMATIC_RUNTIME_SCRIPT_KEYS


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
    """Submit uploaded batches as federation analysis jobs and track them."""

    def __init__(
        self,
        *,
        database: Path | str,
        gateway: DiscoveryAnalysisGateway | None = None,
        data_dir: Path | str | None = None,
        clock: Callable[[], datetime] = _now,
        script_keys: tuple[str, ...] = AUTOMATIC_RUNTIME_SCRIPT_KEYS,
        runtime_namespace: str = "default",
    ) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.gateway = gateway if gateway is not None else DiscoveryAnalysisGateway()
        self.data_dir = Path(data_dir) if data_dir is not None else repo_root() / "data"
        self.clock = clock
        self.script_keys = tuple(script_keys)
        self.runtime_namespace = runtime_namespace
        self._lock = threading.Lock()
        self._initialize_links()

    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _create_links_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS data_upload_analysis_jobs (
                job_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                coordinator_id TEXT NOT NULL,
                provider_id TEXT,
                execution_id TEXT,
                baseline_update_at TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY(job_id, batch_id)
            )
            """
        )

    @staticmethod
    def _links_schema_is_current(connection: sqlite3.Connection) -> bool:
        rows = connection.execute(
            "PRAGMA table_info(data_upload_analysis_jobs)"
        ).fetchall()
        if not rows:
            return False
        by_name = {str(row["name"]): row for row in rows}
        required = {
            "job_id",
            "batch_id",
            "session_id",
            "coordinator_id",
            "provider_id",
            "execution_id",
            "baseline_update_at",
            "created_at",
        }
        if not required.issubset(by_name):
            return False
        return (
            int(by_name["job_id"]["pk"]) == 1
            and int(by_name["batch_id"]["pk"]) == 2
            and int(by_name["provider_id"]["notnull"]) == 0
        )

    def _migrate_legacy_links(self, connection: sqlite3.Connection) -> None:
        """Replace the pre-federation one-job-per-batch link schema atomically."""

        legacy = "data_upload_analysis_jobs_legacy"
        connection.execute(f"DROP TABLE IF EXISTS {legacy}")
        connection.execute(
            f"ALTER TABLE data_upload_analysis_jobs RENAME TO {legacy}"
        )
        self._create_links_table(connection)
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({legacy})").fetchall()
        }

        def value(name: str, fallback: str = "NULL") -> str:
            return name if name in columns else fallback

        # Every released legacy schema had the first five identity columns and
        # created_at. Optional tracking columns are copied when present. The old
        # provider value remains useful history, while new rows may leave it null
        # until F7 selection chooses an actual provider.
        required = {
            "job_id",
            "batch_id",
            "session_id",
            "coordinator_id",
            "provider_id",
            "created_at",
        }
        if not required.issubset(columns):
            raise sqlite3.OperationalError(
                "legacy data_upload_analysis_jobs schema is missing required columns"
            )
        connection.execute(
            f"""
            INSERT OR IGNORE INTO data_upload_analysis_jobs(
                job_id,batch_id,session_id,coordinator_id,provider_id,
                execution_id,baseline_update_at,created_at
            )
            SELECT
                job_id,batch_id,session_id,coordinator_id,provider_id,
                {value('execution_id')},{value('baseline_update_at')},created_at
            FROM {legacy}
            """
        )
        connection.execute(f"DROP TABLE {legacy}")

    def _initialize_links(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            exists = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='data_upload_analysis_jobs'
                """
            ).fetchone()
            if exists is None:
                self._create_links_table(connection)
                return
            if self._links_schema_is_current(connection):
                return
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._migrate_legacy_links(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    # ------------------------------------------------------------------

    def _batch_dates(self, batch: Mapping[str, Any]) -> tuple[str, ...]:
        published = batch.get("published_path")
        if not published:
            raise UploadAnalysisJobError(
                "analysis-batch-not-published",
                "The upload batch has not finished importing yet.",
            )
        directory = Path(str(published))
        if not directory.is_dir():
            raise UploadAnalysisJobError(
                "analysis-batch-not-published",
                "The imported upload files could not be located safely.",
            )
        dates = tuple(item.isoformat() for item in discover_available_dates(directory))
        if not dates:
            raise UploadAnalysisJobError(
                "analysis-batch-undated",
                "No analysable dates were found in the uploaded JSONL.",
            )
        return dates

    def submit_batch(self, batch: Mapping[str, Any]) -> str:
        """Submit one job per uploaded date slice and return the primary job ID.

        The identity of each job is derived from the data, not from the upload,
        so an uploaded slice that automatic discovery already queued resolves to
        the existing durable job instead of a duplicate.
        """

        batch_id = str(batch.get("batch_id") or "").strip()
        if not batch_id:
            raise UploadAnalysisJobError(
                "analysis-batch-required",
                "The upload batch could not be identified safely.",
            )
        dates = self._batch_dates(batch)
        signatures = source_date_signatures(self.data_dir)
        gateway = self.gateway
        submitted: list[str] = []
        with self._lock:
            for iso_date in dates:
                if iso_date not in signatures:
                    continue
                # The identical signature automatic discovery would compute, so
                # both triggers resolve to one durable job for the same slice.
                day = date.fromisoformat(iso_date)
                signature = date_range_source_signature(signatures, day, day)
                outcome = gateway.submit_slice(
                    data_dir=self.data_dir,
                    target_dates=(iso_date,),
                    script_keys=self.script_keys,
                    runtime_namespace=self.runtime_namespace,
                    source_signature=signature,
                    slice_kind=SLICE_KIND_DATE,
                    slice_key=iso_date,
                    origin=ORIGIN_MANUAL_UPLOAD,
                )
                if outcome is not None:
                    submitted.append(outcome.job_id)
        if not submitted:
            raise UploadAnalysisJobError(
                "analysis-batch-unschedulable",
                "The uploaded JSONL could not be turned into analysis work.",
            )
        identity = gateway.runtime.identity
        now = self.clock()
        with self._connect() as connection:
            for job_id in submitted:
                connection.execute(
                    """
                    INSERT INTO data_upload_analysis_jobs(
                        job_id,batch_id,session_id,coordinator_id,provider_id,
                        baseline_update_at,created_at,execution_id
                    ) VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(job_id,batch_id) DO NOTHING
                    """,
                    (
                        job_id,
                        batch_id,
                        identity.session_id,
                        identity.coordinator_node_id,
                        None,
                        None,
                        _stamp(now),
                        job_id,
                    ),
                )
        return submitted[0]

    def start_tracking(self, job_id: str) -> None:
        """Ask the federation scheduler to place the queued work.

        The pass runs on its own thread: the upload request returns immediately
        and the job progresses through the durable lifecycle.
        """

        self.gateway.request_scheduling_pass()

    def fail(self, job_id: str, error_code: str) -> None:
        """Record that the *upload route* could not complete after submission.

        The durable job is owned by the federation lifecycle: a browser request
        that fails afterwards must not terminalize work a selected provider may
        already be executing. Timeouts, loss, and retries are decided by the
        existing lifecycle coordinator instead.
        """

        return

    # ------------------------------------------------------------------

    def job_ids(self, session_id: str) -> tuple[str, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT job_id FROM data_upload_analysis_jobs
                WHERE session_id=? ORDER BY job_id
                """,
                (session_id,),
            ).fetchall()
        return tuple(str(row["job_id"]) for row in rows)

    def snapshots(self, session_id: str) -> tuple[DurableJobSnapshot, ...]:
        """Return durable snapshots for every analysis job in this session."""

        service = self.gateway.runtime.service
        if service is None:  # pragma: no cover - constructed with the runtime
            return ()
        return service.snapshots(session_id)

    @property
    def store(self):
        return self.gateway.runtime.store


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


def federation_analysis_identity() -> tuple[str, str] | None:
    """Return this device's federation ``(session_id, node_id)`` when connected.

    Registered with the analysis runtime during application startup so durable
    analysis jobs are coordinated under the real federation session whenever one
    exists. Without a binding the runtime keeps its own single-node session and
    the identical authority checks still apply.
    """

    from .capability_onboarding_service import get_capability_onboarding_service

    try:
        context = get_capability_onboarding_service().authorized_context()
    except Exception:  # noqa: BLE001 - identity resolution must never break startup
        return None
    if context is None:
        return None
    return (
        context.binding.internal_session_id,
        context.credentials.identity.node_id,
    )


def get_upload_analysis_job_service() -> UploadAnalysisJobService:
    existing = current_app.extensions.get("upload_analysis_job_service")
    if isinstance(existing, UploadAnalysisJobService):
        return existing
    service = UploadAnalysisJobService(database=_configured_database())
    current_app.extensions["upload_analysis_job_service"] = service
    return service


__all__ = [
    "UploadAnalysisJobError",
    "UploadAnalysisJobService",
    "federation_analysis_identity",
    "get_upload_analysis_job_service",
]
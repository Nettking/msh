"""The single entry point both analysis triggers converge on.

Automatic JSONL discovery and the manual upload workflow call
:meth:`AnalysisWorkService.submit_analysis_work`. Neither decides where the work
runs; both create a durable job and let federation scheduling place it.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

from ..job_store import DurableJobSnapshot
from ..jobs import JobStatus
from .contracts import AnalysisWorkSlice
from .scheduler import (
    FederatedAnalysisScheduler,
    SchedulingOutcome,
    SubmissionOutcome,
)

DECISION_SCHEDULING_FAILED = "scheduling-failed"
DEFAULT_SCHEDULER_POLL_SECONDS = 5.0

_SCHEDULING_ERRORS = (
    FederationValidationError,
    FederationOperationError,
    ProtocolCompatibilityError,
    OSError,
    TimeoutError,
)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class AnalysisJobRecord:
    """Index entry describing one submitted analysis job.

    This is an index for enumeration and product views only. Duplicate
    suppression is owned by the job store's ``idempotency_key`` semantics.
    """

    job_id: str
    session_id: str
    slice_kind: str
    slice_key: str
    origin: str
    identity_digest: str
    source_signature: str
    target_dates: tuple[str, ...]
    created_at: str


class AnalysisJobRegistry:
    """Durable index of analysis jobs submitted from this device."""

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS federated_analysis_jobs (
                    job_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    slice_kind TEXT NOT NULL,
                    slice_key TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    identity_digest TEXT NOT NULL,
                    source_signature TEXT NOT NULL,
                    target_dates TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_federated_analysis_jobs_session
                    ON federated_analysis_jobs(session_id, created_at DESC)
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def record(self, work: AnalysisWorkSlice, *, created_at: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO federated_analysis_jobs(
                    job_id, session_id, slice_kind, slice_key, origin,
                    identity_digest, source_signature, target_dates, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(job_id) DO NOTHING
                """,
                (
                    work.job_id,
                    work.session_id,
                    work.slice_kind,
                    work.slice_key,
                    work.origin,
                    work.identity_digest,
                    work.source_signature,
                    json.dumps(list(work.target_dates), separators=(",", ":")),
                    _stamp(created_at),
                ),
            )

    def _record_from_row(self, row: sqlite3.Row) -> AnalysisJobRecord:
        dates = json.loads(str(row["target_dates"]))
        return AnalysisJobRecord(
            job_id=str(row["job_id"]),
            session_id=str(row["session_id"]),
            slice_kind=str(row["slice_kind"]),
            slice_key=str(row["slice_key"]),
            origin=str(row["origin"]),
            identity_digest=str(row["identity_digest"]),
            source_signature=str(row["source_signature"]),
            target_dates=tuple(str(item) for item in dates),
            created_at=str(row["created_at"]),
        )

    def records(
        self,
        *,
        session_id: str | None = None,
        limit: int = 200,
    ) -> tuple[AnalysisJobRecord, ...]:
        query = "SELECT * FROM federated_analysis_jobs"
        parameters: list[Any] = []
        if session_id is not None:
            query += " WHERE session_id=?"
            parameters.append(session_id)
        query += " ORDER BY created_at DESC, job_id DESC LIMIT ?"
        parameters.append(int(limit))
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def scan_job_ids(self, *, session_id: str | None = None) -> tuple[str, ...]:
        """Return every recorded job id, oldest first.

        :meth:`records` is a bounded product view. Lifecycle scanning must not
        inherit that bound: a device that keeps receiving data keeps creating
        jobs, and a job that scrolled off the newest page still has to be driven
        to a terminal state.
        """

        query = "SELECT job_id FROM federated_analysis_jobs"
        parameters: list[Any] = []
        if session_id is not None:
            query += " WHERE session_id=?"
            parameters.append(session_id)
        query += " ORDER BY created_at ASC, job_id ASC"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return tuple(str(row["job_id"]) for row in rows)

    def record_for(self, job_id: str) -> AnalysisJobRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM federated_analysis_jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
        return None if row is None else self._record_from_row(row)


class AnalysisWorkService:
    """Submit analysis work and continuously drive its durable lifecycle."""

    def __init__(
        self,
        *,
        scheduler: FederatedAnalysisScheduler,
        registry: AnalysisJobRegistry,
        session_id: str,
        scheduler_poll_seconds: float = DEFAULT_SCHEDULER_POLL_SECONDS,
    ) -> None:
        self.scheduler = scheduler
        self.registry = registry
        self.session_id = session_id
        self.scheduler_poll_seconds = max(float(scheduler_poll_seconds), 0.05)
        # Terminal is final, so a job that reached it never has to be read from
        # the durable store again while this runtime lives. Without that the
        # lifecycle scan would keep growing on a device that keeps discovering
        # data, even though almost all of those jobs are long finished.
        self._settled_job_ids: set[str] = set()
        self._lock = threading.Lock()
        self._scheduling = threading.Event()
        self._scheduler_wake = threading.Event()
        self._scheduler_stop = threading.Event()

    # ------------------------------------------------------------------

    def submit_analysis_work(
        self,
        work: AnalysisWorkSlice,
        *,
        slice_files: Sequence[Path],
        slice_root: Path,
    ) -> SubmissionOutcome:
        if work.session_id != self.session_id:
            raise FederationValidationError(
                "analysis-session-mismatch",
                "session_id",
                "work belongs to another federation session",
            )
        with self._lock:
            outcome = self.scheduler.submit(
                work, slice_files=slice_files, slice_root=slice_root
            )
            self.registry.record(work, created_at=self.scheduler.clock())
        self._scheduler_wake.set()
        return outcome

    # ------------------------------------------------------------------

    def pending_job_ids(self) -> tuple[str, ...]:
        """Return every non-terminal job this session owns, oldest first.

        The driver stops when this is empty, so it has to see all of the work:
        scanning only the newest page of the index would strand older pending
        jobs on a device that keeps discovering new data.
        """

        pending: list[str] = []
        for job_id in self.registry.scan_job_ids(session_id=self.session_id):
            if job_id in self._settled_job_ids:
                continue
            try:
                snapshot = self.scheduler.store.snapshot(job_id)
            except _SCHEDULING_ERRORS:
                continue
            if snapshot.job.terminal:
                self._settled_job_ids.add(job_id)
                continue
            pending.append(job_id)
        return tuple(pending)

    async def schedule_pending(self) -> tuple[SchedulingOutcome, ...]:
        outcomes: list[SchedulingOutcome] = []
        for job_id in self.pending_job_ids():
            try:
                outcomes.append(await self.scheduler.schedule(job_id))
            except _SCHEDULING_ERRORS as exc:
                outcomes.append(
                    SchedulingOutcome(
                        job_id,
                        DECISION_SCHEDULING_FAILED,
                        str(getattr(exc, "code", exc.__class__.__name__)),
                    )
                )
        return tuple(outcomes)

    def run_scheduling_pass(self) -> tuple[SchedulingOutcome, ...]:
        """Run one scheduling pass synchronously (used by tests and CLI paths)."""

        return asyncio.run(self.schedule_pending())

    def request_scheduling_pass(self) -> bool:
        """Ensure the background lifecycle driver is running.

        The driver stays alive while durable work is pending. This is essential
        for queue timeouts, provider appearance, retry backoff and lease-loss
        evaluation: none of those transitions are guaranteed to coincide with a
        new discovery or browser request.
        """

        if self._scheduler_stop.is_set():
            return False
        with self._lock:
            if self._scheduling.is_set():
                self._scheduler_wake.set()
                return False
            self._scheduling.set()
            self._scheduler_wake.set()
        thread = threading.Thread(
            target=self._background_pass,
            name="fcp-analysis-scheduler",
            daemon=True,
        )
        try:
            thread.start()
        except Exception:
            self._scheduling.clear()
            raise
        return True

    def stop_scheduling(self) -> None:
        """Fence a superseded runtime's background driver without touching jobs."""

        self._scheduler_stop.set()
        self._scheduler_wake.set()

    def _background_pass(self) -> None:
        try:
            while not self._scheduler_stop.is_set():
                self._scheduler_wake.clear()
                try:
                    asyncio.run(self.schedule_pending())
                except _SCHEDULING_ERRORS:
                    # A transient scheduling/control failure is exactly the case
                    # the persistent driver exists to revisit.
                    pass
                if self._scheduler_stop.is_set() or not self.pending_job_ids():
                    return
                self._scheduler_wake.wait(self.scheduler_poll_seconds)
        finally:
            self._scheduling.clear()
            # Close the race where work was submitted after the final pending
            # check but before the scheduling flag cleared. A deliberately
            # stopped/superseded runtime must never resurrect its driver.
            if (
                not self._scheduler_stop.is_set()
                and self.pending_job_ids()
                and self._scheduler_wake.is_set()
            ):
                self.request_scheduling_pass()

    # ------------------------------------------------------------------

    def snapshots(self, session_id: str) -> tuple[DurableJobSnapshot, ...]:
        snapshots: list[DurableJobSnapshot] = []
        for record in self.registry.records(session_id=session_id):
            try:
                snapshots.append(self.scheduler.store.snapshot(record.job_id))
            except _SCHEDULING_ERRORS:
                continue
        return tuple(snapshots)

    def job_status(self, job_id: str) -> JobStatus | None:
        try:
            return self.scheduler.store.snapshot(job_id).job.status
        except _SCHEDULING_ERRORS:
            return None


__all__ = [
    "AnalysisJobRecord",
    "AnalysisJobRegistry",
    "AnalysisWorkService",
]
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

#: Failures the persistent lifecycle driver must survive rather than die on.
#
# The driver is the only thing that advances queue timeouts, provider
# appearance, retry backoff and lease loss for durable work, and none of those
# transitions is guaranteed to coincide with a new discovery or browser
# request. Its durable stores are SQLite, so ``sqlite3.Error`` -- a locked
# database, a transient disk I/O error -- is an ordinary condition for this
# loop, not an unexpected one. It is deliberately absent from
# ``_SCHEDULING_ERRORS`` above: there those errors would silently drop a job
# from a read/projection surface, which is a different and worse trade.
_DRIVER_RETRY_ERRORS = (*_SCHEDULING_ERRORS, sqlite3.Error)


@dataclass(frozen=True)
class AnalysisDriverHealth:
    """What the persistent analysis lifecycle driver last did.

    A required loop that dies leaves durable work stranded until an unrelated
    external trigger happens to restart it. Recording why it stopped is what
    separates "no analysis work is pending" from "nothing is driving the work
    that is pending".
    """

    running: bool
    stopped: bool
    consecutive_failures: int
    last_error_code: str | None
    last_failure_at: datetime | None

    @property
    def failing(self) -> bool:
        return self.consecutive_failures > 0


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
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(federated_analysis_jobs)"
                ).fetchall()
            }
            if "settled_at" not in columns:
                connection.execute(
                    "ALTER TABLE federated_analysis_jobs ADD COLUMN settled_at TEXT"
                )
            # Lifecycle scanning only cares about work that has not finished, and
            # on a device that keeps discovering data the finished rows dominate.
            # A partial index keeps that scan proportional to unsettled work
            # rather than to the whole history.
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_federated_analysis_jobs_unsettled
                    ON federated_analysis_jobs(session_id, created_at)
                    WHERE settled_at IS NULL
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

    def unsettled_job_ids(self, *, session_id: str | None = None) -> tuple[str, ...]:
        """Return recorded job ids not yet known to be finished, oldest first.

        :meth:`records` is a bounded product view. Lifecycle scanning must not
        inherit that bound: a device that keeps receiving data keeps creating
        jobs, and a job that scrolled off the newest page still has to be driven
        to a terminal state. It must not inherit the whole history either, so
        jobs already observed in a terminal state are excluded here rather than
        re-read on every pass.
        """

        query = "SELECT job_id FROM federated_analysis_jobs WHERE settled_at IS NULL"
        parameters: list[Any] = []
        if session_id is not None:
            query += " AND session_id=?"
            parameters.append(session_id)
        query += " ORDER BY created_at ASC, job_id ASC"
        with self._connect() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return tuple(str(row["job_id"]) for row in rows)

    def mark_settled(self, job_ids: Sequence[str], *, settled_at: datetime) -> None:
        """Record that these jobs reached a terminal state.

        Terminal job states are absorbing, so this is durable rather than a
        per-process memo: a restart must not have to re-read finished history to
        rediscover what it already established.
        """

        if not job_ids:
            return
        stamp = _stamp(settled_at)
        with self._connect() as connection:
            connection.executemany(
                """
                UPDATE federated_analysis_jobs SET settled_at=?
                WHERE job_id=? AND settled_at IS NULL
                """,
                [(stamp, str(job_id)) for job_id in job_ids],
            )

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
        self._lock = threading.Lock()
        self._scheduling = threading.Event()
        self._scheduler_wake = threading.Event()
        self._scheduler_stop = threading.Event()
        self._driver_lock = threading.Lock()
        self._driver_failures = 0
        self._driver_last_error: str | None = None
        self._driver_last_failure_at: datetime | None = None

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
        jobs on a device that keeps discovering new data. Terminal is final, so
        a job seen finished here is recorded as settled and drops out of every
        later scan, which keeps a pass proportional to unfinished work.
        """

        pending: list[str] = []
        settled: list[str] = []
        for job_id in self.registry.unsettled_job_ids(session_id=self.session_id):
            try:
                snapshot = self.scheduler.store.snapshot(job_id)
            except _SCHEDULING_ERRORS:
                # An unreadable job is not evidence of anything. Leave it
                # unsettled so a later pass reads it again.
                continue
            if snapshot.job.terminal:
                settled.append(job_id)
                continue
            pending.append(job_id)
        self.registry.mark_settled(settled, settled_at=self.scheduler.clock())
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

    # ------------------------------------------------------------------
    # Required-loop health
    # ------------------------------------------------------------------

    def _record_driver_failure(self, exc: BaseException) -> None:
        code = str(getattr(exc, "code", None) or type(exc).__name__)
        try:
            failed_at: datetime | None = self.scheduler.clock()
        except Exception:  # noqa: BLE001 - never replace the failure being recorded
            failed_at = None
        with self._driver_lock:
            self._driver_failures += 1
            self._driver_last_error = code
            self._driver_last_failure_at = failed_at

    def _record_driver_success(self) -> None:
        with self._driver_lock:
            self._driver_failures = 0
            self._driver_last_error = None
            self._driver_last_failure_at = None

    def driver_health(self) -> AnalysisDriverHealth:
        """Report whether the persistent lifecycle driver is actually driving."""

        with self._driver_lock:
            return AnalysisDriverHealth(
                running=self._scheduling.is_set(),
                stopped=self._scheduler_stop.is_set(),
                consecutive_failures=self._driver_failures,
                last_error_code=self._driver_last_error,
                last_failure_at=self._driver_last_failure_at,
            )

    def _pending_job_ids_or_none(self) -> tuple[str, ...] | None:
        """Read pending work without letting a store failure escape cleanup."""

        try:
            return self.pending_job_ids()
        except Exception:  # noqa: BLE001 - cleanup must not mask the real failure
            return None

    def _background_pass(self) -> None:
        try:
            while not self._scheduler_stop.is_set():
                self._scheduler_wake.clear()
                try:
                    asyncio.run(self.schedule_pending())
                    # Reading the remaining work is part of the pass, not of
                    # the loop's scaffolding: a durable-store failure here used
                    # to escape the guard below and kill the driver just as
                    # surely as a failed scheduling attempt.
                    pending = self.pending_job_ids()
                except _DRIVER_RETRY_ERRORS as exc:
                    # A transient scheduling/control/durable-store failure is
                    # exactly the case the persistent driver exists to revisit.
                    # It is retried, but it is no longer invisible: a loop that
                    # keeps failing must not look like an idle healthy one.
                    self._record_driver_failure(exc)
                    # A failed pass must not be read as "there is nothing left
                    # to drive", but a driver whose work has genuinely drained
                    # must still be able to stop. Only a successful read of an
                    # empty backlog ends the loop; an unreadable one keeps it.
                    if self._pending_job_ids_or_none() == ():
                        return
                except BaseException as exc:
                    # Unexpected: this driver stops. Record why, so the exit is
                    # an observable degraded state rather than a stderr
                    # traceback and silently stranded durable work. Re-arming
                    # here instead would turn one deterministic fault into an
                    # unbounded restart loop.
                    self._record_driver_failure(exc)
                    raise
                else:
                    self._record_driver_success()
                    if self._scheduler_stop.is_set() or not pending:
                        return
                self._scheduler_wake.wait(self.scheduler_poll_seconds)
        finally:
            self._scheduling.clear()
            # Close the race where work was submitted after the final pending
            # check but before the scheduling flag cleared. A deliberately
            # stopped/superseded runtime must never resurrect its driver.
            if (
                not self._scheduler_stop.is_set()
                and self._scheduler_wake.is_set()
                and self._pending_job_ids_or_none()
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
    "AnalysisDriverHealth",
    "AnalysisJobRecord",
    "AnalysisJobRegistry",
    "AnalysisWorkService",
]
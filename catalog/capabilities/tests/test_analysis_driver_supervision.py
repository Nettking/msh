"""B06: the analysis lifecycle driver is a required loop, not a best-effort one.

``AnalysisWorkService`` starts one persistent background driver and documents
why it must stay alive: queue timeouts, provider appearance, retry backoff and
lease-loss evaluation are the transitions it owns, and none of them is
guaranteed to coincide with a new discovery pass or a browser request.

Its durable stores are SQLite. A locked database or a transient disk I/O error
raises ``sqlite3.Error``, which is not an ``OSError`` -- so it used to walk
straight out of the loop's guard, kill the thread with a bare stderr traceback,
and leave every queued job pending with nothing driving it and no health
surface saying so.

These tests drive the real stack: the real lifecycle store, the real scheduler
and the real background thread.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest

from catalog.capabilities.jobs import JobStatus
from catalog.capabilities.tests.analysis_harness import NOW, build_stack
from catalog.federation.host_resources import FilesystemMeasurement
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)

DEADLINE_SECONDS = 20.0


def _wait_until(predicate, *, timeout: float = DEADLINE_SECONDS) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


@contextmanager
def _swallowed_thread_exceptions():
    """Keep a deliberately fatal driver fault out of the suite's warning stream."""

    previous = threading.excepthook
    threading.excepthook = lambda args: None
    try:
        yield
    finally:
        threading.excepthook = previous


def _failing_passes(service, *, failures: int, error: BaseException):
    """Replace the pass with one that fails ``failures`` times, then behaves."""

    real = service.schedule_pending
    attempts: list[int] = []

    async def pass_once():
        attempts.append(1)
        if len(attempts) <= failures:
            raise error
        return await real()

    service.schedule_pending = pass_once
    return attempts


def test_a_locked_durable_store_does_not_strand_queued_analysis_work(
    tmp_path,
) -> None:
    """The consequence: one sqlite error used to leave the job queued forever."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    outcome = stack.submit()
    assert stack.store.snapshot(outcome.job_id).job.status is JobStatus.QUEUED

    attempts = _failing_passes(
        stack.service,
        failures=1,
        error=sqlite3.OperationalError("database is locked"),
    )
    try:
        assert stack.service.request_scheduling_pass() is True
        assert _wait_until(
            lambda: stack.store.snapshot(outcome.job_id).job.terminal
        ), "the driver never drove the queued job to a terminal state"
    finally:
        stack.service.stop_scheduling()

    assert len(attempts) >= 2, "the driver did not retry after the store failure"
    assert stack.store.snapshot(outcome.job_id).job.status is JobStatus.SUCCEEDED
    assert stack.service.pending_job_ids() == ()


def test_a_repeating_store_failure_is_visible_instead_of_silent(tmp_path) -> None:
    """A required loop that keeps failing must not look like an idle healthy one."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    stack.submit()

    _failing_passes(
        stack.service,
        failures=10_000,
        error=sqlite3.OperationalError("disk I/O error"),
    )
    try:
        assert stack.service.request_scheduling_pass() is True
        assert _wait_until(lambda: stack.service.driver_health().failing)
        health = stack.service.driver_health()
        assert health.running is True
        assert health.stopped is False
        assert health.consecutive_failures >= 1
        assert health.last_error_code == "OperationalError"
        assert health.last_failure_at is not None
        # It is still driving: the loop retries rather than abandoning the work.
        assert _wait_until(
            lambda: stack.service.driver_health().consecutive_failures >= 2
        )
        assert stack.service.driver_health().running is True
    finally:
        stack.service.stop_scheduling()


def test_a_recovered_pass_clears_the_driver_failure_state(tmp_path) -> None:
    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    outcome = stack.submit()

    _failing_passes(
        stack.service,
        failures=2,
        error=sqlite3.OperationalError("database is locked"),
    )
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(
            lambda: stack.store.snapshot(outcome.job_id).job.terminal
        )
        # The clean pass is what clears the record, so wait for that, not for
        # the job state that merely precedes it.
        assert _wait_until(lambda: not stack.service.driver_health().failing)
    finally:
        stack.service.stop_scheduling()

    health = stack.service.driver_health()
    assert health.failing is False
    assert health.consecutive_failures == 0
    assert health.last_error_code is None


def test_an_unexpected_driver_failure_is_recorded_before_the_thread_exits(
    tmp_path,
) -> None:
    """An unknown fault stops this driver, but never invisibly."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    stack.submit()

    class _Unexpected(Exception):
        pass

    _failing_passes(stack.service, failures=10_000, error=_Unexpected("boom"))
    try:
        with _swallowed_thread_exceptions():
            stack.service.request_scheduling_pass()
            assert _wait_until(lambda: stack.service.driver_health().failing)
            health = stack.service.driver_health()
            assert health.last_error_code == "_Unexpected"
            assert health.consecutive_failures == 1
            # Recorded, and deliberately not re-armed into a restart loop.
            assert _wait_until(
                lambda: not stack.service.driver_health().running
            )
            assert stack.service.driver_health().consecutive_failures == 1
    finally:
        stack.service.stop_scheduling()


def test_a_store_failure_while_reading_pending_work_does_not_kill_the_driver(
    tmp_path,
) -> None:
    """``pending_job_ids`` is part of the pass, not unguarded loop scaffolding."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    outcome = stack.submit()

    real_pending = stack.service.pending_job_ids
    raised: list[int] = []

    def pending_job_ids():
        if not raised:
            raised.append(1)
            raise sqlite3.OperationalError("database is locked")
        return real_pending()

    stack.service.pending_job_ids = pending_job_ids
    try:
        assert stack.service.request_scheduling_pass() is True
        assert _wait_until(
            lambda: stack.store.snapshot(outcome.job_id).job.terminal
        ), "a store failure reading pending work killed the driver"
    finally:
        stack.service.stop_scheduling()

    assert raised == [1]
    assert stack.store.snapshot(outcome.job_id).job.status is JobStatus.SUCCEEDED


def test_a_drained_backlog_still_stops_the_driver_after_a_failed_pass(
    tmp_path,
) -> None:
    """Retrying a failure must not become an unbounded loop over no work."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    # No work was ever submitted, so the backlog is readable and empty.
    _failing_passes(
        stack.service,
        failures=10_000,
        error=sqlite3.OperationalError("database is locked"),
    )
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: not stack.service.driver_health().running), (
            "the driver kept retrying with no durable work left to drive"
        )
    finally:
        stack.service.stop_scheduling()

    assert stack.service.driver_health().failing is True


def test_operator_stop_still_fences_a_superseded_driver(tmp_path) -> None:
    """Preserved behavior: a deliberately stopped runtime never resurrects."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    stack.submit()
    stack.service.stop_scheduling()

    assert stack.service.request_scheduling_pass() is False
    assert _wait_until(lambda: not stack.service.driver_health().running)
    assert stack.service.driver_health().stopped is True


def test_an_ordinary_pass_drives_work_and_reports_a_healthy_driver(
    tmp_path,
) -> None:
    """Preserved behavior: the unmodified path still drains and stays clean."""

    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    outcome = stack.submit()
    try:
        assert stack.service.request_scheduling_pass() is True
        assert _wait_until(
            lambda: stack.store.snapshot(outcome.job_id).job.terminal
        )
    finally:
        stack.service.stop_scheduling()

    assert stack.store.snapshot(outcome.job_id).job.status is JobStatus.SUCCEEDED
    assert stack.service.driver_health().failing is False


def _controlled_admission(pressure):
    """Exercise real admission/refusal without depending on the CI host's disk."""

    return SerializedProcessResourceAdmission(
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="supervision-test-volume",
            observed_at=NOW,
            total_bytes=1024**4,
            free_bytes=(11 if pressure.is_set() else 512) * 1024**3,
            total_inodes=None,
            free_inodes=None,
            available=True,
        ),
        clock=lambda: NOW,
    )


def _stop_driver(service):
    service.stop_scheduling()
    assert _wait_until(lambda: not service.driver_health().running)


def test_host_refusal_before_claim_keeps_work_queued_then_executes_once(tmp_path):
    pressure = threading.Event()
    stack = build_stack(tmp_path, resource_admission=_controlled_admission(pressure))
    stack.service.scheduler_poll_seconds = 0.05
    job_id = stack.submit().job_id
    pressure.set()
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: stack.service.driver_health().failing)
        health = stack.service.driver_health()
        assert health.running and health.last_error_code == "resource_pressure"
        snapshot = stack.store.snapshot(job_id)
        assert snapshot.job.status is JobStatus.QUEUED
        assert snapshot.ownership is None and snapshot.attempt_generation == 0
        assert stack.service.registry.unsettled_job_ids() == (job_id,)
        assert stack.executor.calls == []

        pressure.clear()  # No new submission or scheduling request wakes recovery.
        assert _wait_until(lambda: stack.store.snapshot(job_id).job.terminal)
        assert _wait_until(lambda: not stack.service.driver_health().failing)
    finally:
        _stop_driver(stack.service)
    snapshot = stack.store.snapshot(job_id)
    assert snapshot.job.status is JobStatus.SUCCEEDED
    assert snapshot.attempt_generation == 1 and len(snapshot.job.attempts) == 1
    assert len(stack.executor.calls) == 1


def test_repeated_refusal_waits_despite_wake_requests_and_stop_interrupts_it(tmp_path):
    pressure = threading.Event()
    stack = build_stack(tmp_path, resource_admission=_controlled_admission(pressure))
    stack.service.scheduler_poll_seconds = 0.25
    job_id = stack.submit().job_id
    pressure.set()
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: stack.service.driver_health().failing)
        initial = stack.service.driver_health().consecutive_failures
        started = time.monotonic()
        while time.monotonic() - started < 0.6:
            assert stack.service.request_scheduling_pass() is False
            time.sleep(0.005)
        elapsed = time.monotonic() - started
        health = stack.service.driver_health()
        assert health.running and health.consecutive_failures > initial
        assert health.consecutive_failures - initial <= int(elapsed / 0.25) + 1
        assert stack.store.snapshot(job_id).job.status is JobStatus.QUEUED
        assert stack.executor.calls == []
        # A long existing poll interval must not delay operator shutdown.
        stack.service.scheduler_poll_seconds = 30.0
        previous = health.consecutive_failures
        assert _wait_until(
            lambda: stack.service.driver_health().consecutive_failures > previous
        )
    finally:
        stack.service.stop_scheduling()
        assert _wait_until(lambda: not stack.service.driver_health().running, timeout=2)
    pressure.clear()
    assert stack.service.request_scheduling_pass() is False
    assert stack.store.snapshot(job_id).job.status is JobStatus.QUEUED


def test_refusal_after_claim_preserves_the_existing_attempt(tmp_path, monkeypatch):
    pressure = threading.Event()
    admission = _controlled_admission(pressure)
    stack = build_stack(tmp_path, resource_admission=admission)
    stack.service.scheduler_poll_seconds = 0.05
    job_id = stack.submit().job_id
    grant = stack.scheduler._grant_input_access
    calls = []

    def refuse_first_grant(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            pressure.set()
        return grant(*args, **kwargs)

    monkeypatch.setattr(stack.scheduler, "_grant_input_access", refuse_first_grant)
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: stack.service.driver_health().failing)
        assert stack.service.driver_health().running
        owned = stack.store.snapshot(job_id)
        assert owned.ownership is not None and owned.attempt_generation == 1
        assert stack.executor.calls == []
        pressure.clear()
        assert _wait_until(lambda: stack.store.snapshot(job_id).job.terminal)
    finally:
        _stop_driver(stack.service)
    completed = stack.store.snapshot(job_id)
    assert completed.job.status is JobStatus.SUCCEEDED
    assert completed.attempt_generation == 1
    assert completed.job.attempts[0].attempt_id == owned.ownership.attempt_id
    assert len(stack.executor.calls) == 1


@pytest.mark.parametrize("error_type", [sqlite3.OperationalError, OSError])
def test_transient_snapshot_read_is_not_an_empty_backlog(tmp_path, monkeypatch, error_type):
    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    job_id = stack.submit().job_id
    snapshot = stack.store.snapshot
    failed = []

    def read(job):
        if threading.current_thread().name == "fcp-analysis-scheduler" and not failed:
            failed.append(1)
            raise error_type("temporary read failure")
        return snapshot(job)

    monkeypatch.setattr(stack.store, "snapshot", read)
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: snapshot(job_id).job.terminal)
    finally:
        _stop_driver(stack.service)
    assert failed == [1]
    assert snapshot(job_id).job.status is JobStatus.SUCCEEDED
    assert len(stack.executor.calls) == 1


def test_concurrent_submissions_during_claim_refusal_strand_nothing(tmp_path, monkeypatch):
    pressure = threading.Event()
    admission = _controlled_admission(pressure)
    # Fault injection at claim admission allows concurrent submission to commit;
    # the real job/ownership stores, admission policy and worker remain in use.
    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    claim = stack.store.claim

    def admitted_claim(*args, **kwargs):
        with admission.reserve(stack.store.database, bytes_required=1):
            return claim(*args, **kwargs)

    monkeypatch.setattr(stack.store, "claim", admitted_claim)
    first = stack.submit().job_id
    pressure.set()

    def submit(day):
        outcome = stack.submit(target_date=day)
        stack.service.request_scheduling_pass()
        return outcome.job_id

    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(lambda: stack.service.driver_health().failing)
        with ThreadPoolExecutor(max_workers=2) as pool:
            submitted = list(pool.map(submit, ["2026-08-14", "2026-08-15"] * 2))
        jobs = {first, *submitted}
        assert len(jobs) == 3
        assert stack.service.driver_health().running
        assert set(stack.service.registry.unsettled_job_ids()) == jobs
        assert all(stack.store.snapshot(job).ownership is None for job in jobs)
        pressure.clear()
        assert _wait_until(lambda: all(stack.store.snapshot(job).job.terminal for job in jobs))
    finally:
        _stop_driver(stack.service)
    assert len(stack.executor.calls) == 3
    assert all(stack.store.snapshot(job).attempt_generation == 1 for job in jobs)
    assert all(stack.store.snapshot(job).job.status is JobStatus.SUCCEEDED for job in jobs)


def test_fatal_error_does_not_rearm_itself_when_a_wakeup_is_pending(tmp_path):
    stack = build_stack(tmp_path)
    stack.submit()
    calls = []

    async def fatal_pass():
        calls.append(1)
        stack.service.request_scheduling_pass()
        raise RuntimeError("unrecoverable driver fault")

    stack.service.schedule_pending = fatal_pass
    try:
        with _swallowed_thread_exceptions():
            stack.service.request_scheduling_pass()
            assert _wait_until(lambda: stack.service.driver_health().failing)
            assert _wait_until(lambda: not stack.service.driver_health().running)
            assert calls == [1]
            assert stack.service.driver_health().last_error_code == "RuntimeError"
    finally:
        _stop_driver(stack.service)


def test_submission_during_driver_exit_survives_the_next_store_read_failure(
    tmp_path, monkeypatch,
):
    stack = build_stack(tmp_path)
    stack.service.scheduler_poll_seconds = 0.05
    first = stack.submit().job_id
    pending = stack.service.pending_job_ids
    submitted = []
    read_failed = []

    def read_pending():
        if submitted and not read_failed:
            read_failed.append(1)
            raise sqlite3.OperationalError("temporary read failure")
        result = pending()
        if not result and not submitted:
            # Commit new work after the empty read but before the driver exits.
            submitted.append(stack.submit(target_date="2026-08-14").job_id)
            stack.service.request_scheduling_pass()
        return result

    monkeypatch.setattr(stack.service, "pending_job_ids", read_pending)
    try:
        stack.service.request_scheduling_pass()
        assert _wait_until(
            lambda: submitted and stack.store.snapshot(submitted[0]).job.terminal
        )
    finally:
        _stop_driver(stack.service)
    assert read_failed == [1]
    assert len(stack.executor.calls) == 2
    assert all(
        stack.store.snapshot(job).job.status is JobStatus.SUCCEEDED
        and stack.store.snapshot(job).attempt_generation == 1
        for job in (first, submitted[0])
    )

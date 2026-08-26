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
from contextlib import contextmanager

from catalog.capabilities.jobs import JobStatus
from catalog.capabilities.tests.analysis_harness import build_stack

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

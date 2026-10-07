from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.federation.errors import FederationOperationError
from catalog.federation.outbox import RetiredDatasetCount, RetiredSummary
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_delivery import RecorderDeliveryRunResult
from catalog.federation.recorder_publication import (
    RecorderPublicationCycleReport,
    RecorderWorkerCycleResult,
)
from catalog.flask_app.services import (
    recorder_federation_publication_install as install_module,
)
from catalog.flask_app.services.recorder_federation_publication_install import (
    install_recorder_federation_publication,
)


class _Onboarding:
    def __init__(self, context: object | None) -> None:
        self.context = context

    def authorized_context(self):
        return self.context


class _Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(dict(kwargs))
        return PhaseDIngestOutcome(committed=True)


def _context():
    return SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session-1"),
        credentials=SimpleNamespace(
            identity=SimpleNamespace(node_id="recorder-node-1")
        ),
    )


def _configured_app(tmp_path, *, context=None):
    app = Flask(__name__)
    onboarding = _Onboarding(_context() if context is None else context)
    monitor = install_recorder_federation_publication(
        app,
        onboarding_service=onboarding,
    )
    app.config.update(
        RECORDER_FEDERATION_PUBLICATION_ENABLED=True,
        RECORDER_FEDERATION_STORAGE_GROUP_ID="telemetry-storage",
        RECORDER_FEDERATION_DATA_DIRECTORY=str(tmp_path / "data"),
        RECORDER_FEDERATION_CHECKPOINT_FILE=str(
            tmp_path / "data" / "source_state" / "mtconnect_recorder_state.json"
        ),
        RECORDER_FEDERATION_OUTBOX_DATABASE=str(tmp_path / "publication.sqlite3"),
        RECORDER_FEDERATION_POLL_INTERVAL_SECONDS=0.2,
        RECORDER_FEDERATION_DELIVERY_LIMIT=10,
    )
    return app, monitor


def test_publication_is_disabled_without_explicit_consent(tmp_path):
    app = Flask(__name__)
    monitor = install_recorder_federation_publication(
        app,
        onboarding_service=_Onboarding(_context()),
    )

    monitor.start()

    snapshot = monitor.snapshot()
    assert snapshot.status == "disabled"
    assert snapshot.enabled is False
    assert app.extensions["recorder_federation_publication"] is monitor


def test_enabled_publication_requires_explicit_logical_storage_target(tmp_path):
    app, monitor = _configured_app(tmp_path)
    app.config["RECORDER_FEDERATION_STORAGE_GROUP_ID"] = ""
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = lambda _context: _Client()

    monitor.start()

    snapshot = monitor.snapshot()
    assert snapshot.status == "not-configured"
    assert snapshot.enabled is True
    assert snapshot.last_error_code == "recorder-publication-target-required"


def test_worker_composition_uses_authorized_session_node_and_client_factory(tmp_path):
    context = _context()
    app, monitor = _configured_app(tmp_path, context=context)
    client = _Client()
    seen_contexts: list[object] = []

    def factory(value):
        seen_contexts.append(value)
        return client

    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = factory

    with app.app_context():
        worker = monitor.build_worker()

    assert seen_contexts == [context]
    assert worker.reconciler.target.session_id == "session-1"
    assert worker.reconciler.target.recorder_node_id == "recorder-node-1"
    assert worker.reconciler.target.group_id == "telemetry-storage"
    assert worker.reconciler.target.dataset_id("Mazak") == (
        "mtconnect:recorder-node-1:Mazak"
    )

    result = monitor.run_once()
    assert result.checkpoint_changed is True
    assert result.reconcile is not None
    assert result.reconcile.enqueued == 0
    assert result.delivery.attempted == 0


def test_worker_does_not_compose_without_trusted_federation_context(tmp_path):
    app, monitor = _configured_app(tmp_path, context=False)
    monitor.onboarding_service = _Onboarding(None)
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = lambda _context: _Client()

    with app.app_context(), pytest.raises(FederationOperationError) as excinfo:
        monitor.build_worker()

    assert excinfo.value.code == "recorder-publication-federation-required"


# --------------------------------------------------------------------------
# B03: the required-thread boundary must surface degraded publication health
# --------------------------------------------------------------------------


def _run_worker_briefly(monitor, worker, *, until) -> None:
    """Drive the monitor's own required-thread coroutine to a condition."""

    async def _drive() -> None:
        task = asyncio.create_task(monitor._run_worker(worker))
        deadline = asyncio.get_running_loop().time() + 5.0
        while not until() and not task.done():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("the publication loop never reached the condition")
            await asyncio.sleep(0.001)
        stop = monitor._async_stop
        if stop is not None:
            stop.set()
        # Surfaces any exception that escaped the required loop, which must not
        # happen: a cycle failure has to be reported and retried, not fatal.
        await task

    asyncio.run(_drive())


def test_a_failing_publication_loop_does_not_keep_reporting_running(tmp_path):
    """The regression this delivery closes at the Flask boundary.

    ``run_forever`` caught every bounded cycle failure and discarded it, so
    this snapshot stayed on "running" indefinitely while no recorder evidence
    moved at all. Against the previous implementation the status assertion
    below fails: the monitor reports a healthy running publisher forever.
    """

    app, monitor = _configured_app(tmp_path)
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = (
        lambda _context: _Client()
    )
    with app.app_context():
        worker = monitor.build_worker()
    worker.poll_interval_seconds = 0.01
    cycles = {"count": 0}

    async def _failing_cycle(*, force_reconcile: bool = False):
        cycles["count"] += 1
        raise OSError("recorder publication outbox is unreadable")

    worker.run_cycle = _failing_cycle

    _run_worker_briefly(monitor, worker, until=lambda: cycles["count"] >= 3)

    snapshot = monitor.snapshot()
    assert snapshot.status == "failing"
    assert snapshot.last_error_code == "OSError"
    assert snapshot.consecutive_failures >= 3
    # The loop kept retrying durable work rather than exiting on the failure.
    assert cycles["count"] >= 3


def test_a_recovering_publication_loop_reports_running_again(tmp_path):
    app, monitor = _configured_app(tmp_path)
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = (
        lambda _context: _Client()
    )
    with app.app_context():
        worker = monitor.build_worker()
    worker.poll_interval_seconds = 0.01
    original = worker.run_cycle
    cycles = {"count": 0}

    async def _flaky_cycle(*, force_reconcile: bool = False):
        cycles["count"] += 1
        if cycles["count"] == 1:
            raise OSError("transient")
        return await original(force_reconcile=force_reconcile)

    worker.run_cycle = _flaky_cycle

    _run_worker_briefly(monitor, worker, until=lambda: cycles["count"] >= 2)

    snapshot = monitor.snapshot()
    assert snapshot.status == "running"
    assert snapshot.last_error_code is None
    assert snapshot.consecutive_failures == 0


def test_degraded_publication_is_visible_in_the_monitor_snapshot(tmp_path):
    _app, monitor = _configured_app(tmp_path)

    def _report(*, retired: int, blocked: tuple[str, ...] = ()):
        return RecorderPublicationCycleReport(
            result=RecorderWorkerCycleResult(
                checkpoint_changed=False,
                reconcile=None,
                delivery=RecorderDeliveryRunResult(
                    attempted=1,
                    committed=1,
                    pending=0,
                    blocked_datasets=blocked,
                ),
                retirement=RetiredSummary(
                    total=retired,
                    datasets=(
                        (RetiredDatasetCount("mtconnect:r:Mazak", retired),)
                        if retired
                        else ()
                    ),
                    truncated=False,
                ),
            )
        )

    monitor._observe_cycle(_report(retired=2))
    degraded = monitor.snapshot()
    assert degraded.status == "degraded"
    assert degraded.last_error_code == "recorder-delivery-retired"
    assert degraded.retired_batches == 2

    # A retryable fence is a lesser state and stays "running".
    monitor._observe_cycle(_report(retired=0, blocked=("mtconnect:r:Mazak",)))
    blocked = monitor.snapshot()
    assert blocked.status == "running"
    assert blocked.last_error_code == "recorder-delivery-pending"

    # And an ordinary cycle clears both, because the snapshot is replaced from
    # the cycle that just ran rather than merged with what came before.
    monitor._observe_cycle(_report(retired=0))
    healthy = monitor.snapshot()
    assert healthy.status == "running"
    assert healthy.last_error_code is None
    assert healthy.retired_batches == 0


# --------------------------------------------------------------------------
# B06: publication driver failure must be observable and bounded, not a
#      once-per-second rebuild of the most expensive part of the driver
# --------------------------------------------------------------------------


def test_a_durable_store_failure_stays_inside_the_publication_loop(tmp_path):
    """The outbox behind this loop is SQLite, so this is an ordinary condition.

    A ``sqlite3.Error`` used to escape ``run_forever`` entirely. It was not
    fatal -- the supervisor above rebuilt the worker -- but rebuilding discards
    this loop's own consecutive-failure count and its poll interval, so a store
    that had been unreadable for hours was presented as a first retry, and each
    retry reloaded the Federation context and reconstructed an authenticated
    storage client.
    """

    app, monitor = _configured_app(tmp_path)
    app.config["RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY"] = (
        lambda _context: _Client()
    )
    with app.app_context():
        worker = monitor.build_worker()
    worker.poll_interval_seconds = 0.01
    cycles = {"count": 0}

    async def _locked_store(*, force_reconcile: bool = False):
        cycles["count"] += 1
        raise sqlite3.OperationalError("database is locked")

    worker.run_cycle = _locked_store

    # ``_run_worker_briefly`` awaits the loop task, so an escape fails here.
    _run_worker_briefly(monitor, worker, until=lambda: cycles["count"] >= 3)

    snapshot = monitor.snapshot()
    assert snapshot.status == "failing"
    assert snapshot.last_error_code == "OperationalError"
    assert snapshot.consecutive_failures >= 3
    assert cycles["count"] >= 3


def test_a_repeated_restart_is_counted_and_backed_off(tmp_path, monkeypatch):
    """A supervisor that never counts its restarts cannot report a stuck driver.

    Every pass through this path reloads the authorized Federation context and
    constructs an authenticated logical-storage client. At a fixed one-second
    wait that is once per second, indefinitely, on a device whose Federation or
    durable store is already failing.
    """

    _app, monitor = _configured_app(tmp_path)
    waits: list[float] = []

    def _never_builds():
        raise FederationOperationError(
            "recorder-publication-federation-required",
            "a trusted Federation connection is required",
        )

    monkeypatch.setattr(monitor, "build_worker", _never_builds)

    def _stop_after_five(delay: float) -> bool:
        waits.append(delay)
        return len(waits) >= 5

    monkeypatch.setattr(monitor._stop, "wait", _stop_after_five)

    monitor._run()

    snapshot = monitor.snapshot()
    assert snapshot.status == "waiting"
    assert snapshot.last_error_code == "recorder-publication-federation-required"
    assert snapshot.consecutive_failures == 5
    # The first wait is unchanged, then the ladder grows and stays bounded.
    assert waits == [1.0, 2.0, 4.0, 8.0, 16.0]
    assert all(delay <= install_module._MAX_RETRY_SECONDS for delay in waits)


def test_the_restart_ladder_is_bounded_and_reset_only_by_publishing(tmp_path):
    """Constructing a worker is not evidence; a published cycle is."""

    _app, monitor = _configured_app(tmp_path)

    assert install_module._restart_delay_seconds(1) == 1.0
    assert install_module._restart_delay_seconds(100) == (
        install_module._MAX_RETRY_SECONDS
    )

    for _ in range(4):
        monitor._record_restart_failure()
    assert monitor._restart_count() == 4

    # A cycle that failed leaves the ladder where it is: the worker built, but
    # nothing was published, which is exactly the loop this bound exists for.
    monitor._observe_cycle(
        RecorderPublicationCycleReport(result=None, error_code="OperationalError")
    )
    assert monitor._restart_count() == 4

    monitor._observe_cycle(
        RecorderPublicationCycleReport(
            result=RecorderWorkerCycleResult(
                checkpoint_changed=False,
                reconcile=None,
                delivery=RecorderDeliveryRunResult(
                    attempted=0,
                    committed=0,
                    pending=0,
                    blocked_datasets=(),
                ),
                retirement=RetiredSummary(total=0, datasets=(), truncated=False),
            )
        )
    )
    assert monitor._restart_count() == 0


def test_restart_delay_saturates_before_large_exponentiation():
    assert install_module._restart_delay_seconds(10**6) == (
        install_module._MAX_RETRY_SECONDS
    )

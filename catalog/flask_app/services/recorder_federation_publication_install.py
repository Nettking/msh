"""Flask lifecycle integration for recorder-to-Federation publication.

Publication is intentionally opt-in. The app owns the lifecycle, but an existing
authenticated logical-storage client factory and an explicit logical storage
group are required before any recorder data may leave the device.
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.outbox import SQLiteOutbox
from catalog.federation.recorder_delivery import DurableRecorderDeliveryQueue
from catalog.federation.recorder_publication import (
    RecorderArchiveReconciler,
    RecorderFederationDeliveryWorker,
    RecorderPublicationCycleReport,
    RecorderPublicationTarget,
    RecorderWorkerCycleResult,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore

_EXTENSION_KEY = "recorder_federation_publication"
_DEFAULT_RETRY_SECONDS = 1.0
#: Ceiling for the supervisor's own restart wait.
#
# Rebuilding this worker is not free: it reloads the authorized Federation
# context, constructs an authenticated logical-storage client and reopens the
# durable outbox. A fixed one-second retry turned an unreachable Federation or
# an unopenable store into that work once per second, indefinitely, on the same
# device the failure is already about. The first wait is unchanged so ordinary
# recovery stays immediate; only a condition that keeps failing backs off.
_MAX_RETRY_SECONDS = 60.0


def _restart_delay_seconds(consecutive_failures: int) -> float:
    """Bounded exponential wait before rebuilding the publication worker."""

    exponent = max(0, consecutive_failures - 1)
    return min(_DEFAULT_RETRY_SECONDS * 2.0**exponent, _MAX_RETRY_SECONDS)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _required_group(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise FederationValidationError(
            "recorder-publication-target-required",
            "group_id",
            "an explicit logical Federation storage group is required",
        )
    group_id = value.strip()
    if len(group_id.encode("utf-8")) > 512 or any(
        ord(char) < 32 for char in group_id
    ):
        raise FederationValidationError(
            "invalid-recorder-publication-target",
            "group_id",
            "must be bounded printable text",
        )
    return group_id


@dataclass(frozen=True)
class RecorderFederationPublicationSnapshot:
    status: str
    enabled: bool
    last_error_code: str | None = None
    # Durable tombstones owned by this session/group, re-read from the outbox
    # on every cycle. Never accumulated here, so a restart cannot forget it and
    # an operator repair clears it without anything having to invalidate it.
    retired_batches: int = 0
    # Consecutive failed worker cycles. Zero whenever the last cycle completed.
    consecutive_failures: int = 0
    # Sources fenced behind an archive item no retry can turn into a
    # publication. Re-derived from the cycle that just ran, never accumulated.
    quarantined_sources: int = 0


class RecorderFederationPublicationMonitor:
    """Own one background publication worker without owning capture authority."""

    def __init__(self, app: Flask, onboarding_service: object) -> None:
        self.app = app
        self.onboarding_service = onboarding_service
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._async_stop: asyncio.Event | None = None
        self._snapshot = RecorderFederationPublicationSnapshot(
            status="not-started",
            enabled=False,
        )
        # Consecutive supervisor-level restarts: builds that could not produce
        # a worker, and workers whose loop escaped. Cleared by a cycle that
        # actually published, never by merely managing to construct a worker.
        self._restart_failures = 0

    def snapshot(self) -> RecorderFederationPublicationSnapshot:
        with self._lock:
            return self._snapshot

    def _set_snapshot(
        self,
        status: str,
        *,
        enabled: bool,
        error_code: str | None = None,
        retired_batches: int = 0,
        consecutive_failures: int = 0,
        quarantined_sources: int = 0,
    ) -> None:
        with self._lock:
            self._snapshot = RecorderFederationPublicationSnapshot(
                status=status,
                enabled=enabled,
                last_error_code=error_code,
                retired_batches=retired_batches,
                consecutive_failures=consecutive_failures,
                quarantined_sources=quarantined_sources,
            )

    def _observe_cycle(self, report: RecorderPublicationCycleReport) -> None:
        """Publish one worker cycle outcome into the app-visible snapshot.

        This is the required-thread boundary the publication loop reports
        through. Before it existed the loop caught every cycle failure and
        discarded it, so a recorder whose outbox had become unreadable kept
        this snapshot on "running" indefinitely while no evidence moved at all.

        Every value here is derived from the cycle that just ran, so the
        snapshot is replaced rather than merged: a state that has been repaired
        cannot survive in it, and a state that is still true is re-asserted on
        the very next cycle.
        """

        state = report.state
        result = report.result
        retired = 0 if result is None else result.retirement.total
        if state != "failing":
            # The worker is not merely constructible, it is publishing. That is
            # the only evidence that clears the restart backoff; a worker that
            # builds and then fails its first cycle every time must keep
            # escalating rather than reset the ladder on each attempt.
            with self._lock:
                self._restart_failures = 0
        if state == "failing":
            self._set_snapshot(
                "failing",
                enabled=True,
                error_code=report.error_code,
                consecutive_failures=report.consecutive_failures,
            )
            return
        if state == "degraded":
            quarantined = report.quarantined_sources
            # Retirement means evidence was permanently withdrawn; quarantine
            # means a source is fenced behind an item an operator has to
            # resolve. Both need an operator, and they are different work, so
            # the snapshot names which one it is rather than merging them.
            self._set_snapshot(
                "degraded",
                enabled=True,
                error_code=(
                    "recorder-delivery-retired"
                    if retired
                    else "recorder-archive-quarantined"
                ),
                retired_batches=retired,
                quarantined_sources=quarantined,
            )
            return
        if state == "blocked":
            self._set_snapshot(
                "running",
                enabled=True,
                error_code="recorder-delivery-pending",
            )
            return
        self._set_snapshot("running", enabled=True)

    def _record_restart_failure(self) -> float:
        """Count one supervisor-level restart and report how long to wait.

        A restart that is never counted cannot be told apart from a first
        blip, and a wait that never grows answers a persistent failure by
        repeating its most expensive part once per second.
        """

        with self._lock:
            self._restart_failures += 1
            failures = self._restart_failures
        return _restart_delay_seconds(failures)

    def _restart_count(self) -> int:
        with self._lock:
            return self._restart_failures

    def _enabled(self) -> bool:
        return bool(
            self.app.config.get("RECORDER_FEDERATION_PUBLICATION_ENABLED", False)
        )

    def _client_factory(self):
        factory = self.app.config.get("RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY")
        if not callable(factory):
            raise FederationValidationError(
                "recorder-publication-client-required",
                "storage_client_factory",
                "an authenticated logical-storage client factory is required",
            )
        return factory

    def build_worker(self) -> RecorderFederationDeliveryWorker:
        """Compose one worker from current authenticated app configuration."""

        if not self._enabled():
            raise FederationOperationError(
                "recorder-publication-disabled",
                "recorder Federation publication is not enabled",
            )
        group_id = _required_group(
            self.app.config.get("RECORDER_FEDERATION_STORAGE_GROUP_ID", "")
        )
        context_loader = getattr(self.onboarding_service, "authorized_context", None)
        context = context_loader() if callable(context_loader) else None
        if context is None:
            raise FederationOperationError(
                "recorder-publication-federation-required",
                "a trusted Federation connection is required before recorder publication",
                "binding",
            )
        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        session_id = getattr(binding, "internal_session_id", None)
        node_id = getattr(identity, "node_id", None)
        if not isinstance(session_id, str) or not session_id:
            raise FederationValidationError(
                "invalid-recorder-publication-context",
                "session_id",
                "the authorized Federation session is missing",
            )
        if not isinstance(node_id, str) or not node_id:
            raise FederationValidationError(
                "invalid-recorder-publication-context",
                "node_id",
                "the authenticated recorder node identity is missing",
            )

        client = self._client_factory()(context)
        if not callable(getattr(client, "ingest_batch", None)):
            raise FederationValidationError(
                "invalid-recorder-publication-client",
                "storage_client",
                "client must provide ingest_batch()",
            )

        data_dir = Path(str(self.app.config["RECORDER_FEDERATION_DATA_DIRECTORY"]))
        checkpoint_file = Path(
            str(self.app.config["RECORDER_FEDERATION_CHECKPOINT_FILE"])
        )
        outbox_path = Path(
            str(self.app.config["RECORDER_FEDERATION_OUTBOX_DATABASE"])
        )
        outbox = SQLiteOutbox(outbox_path)
        queue = DurableRecorderDeliveryQueue(
            outbox=outbox,
            client=client,
            session_id=session_id,
            destination_id=group_id,
        )
        reconciler = RecorderArchiveReconciler(
            store=DurableRecorderStore(data_dir),
            checkpoint_file=checkpoint_file,
            queue=queue,
            target=RecorderPublicationTarget(
                session_id=session_id,
                group_id=group_id,
                recorder_node_id=node_id,
            ),
        )
        return RecorderFederationDeliveryWorker(
            reconciler=reconciler,
            queue=queue,
            poll_interval_seconds=float(
                self.app.config["RECORDER_FEDERATION_POLL_INTERVAL_SECONDS"]
            ),
            delivery_limit=int(
                self.app.config["RECORDER_FEDERATION_DELIVERY_LIMIT"]
            ),
            cycle_observer=self._observe_cycle,
        )

    async def _run_worker(self, worker: RecorderFederationDeliveryWorker) -> None:
        stop = asyncio.Event()
        with self._lock:
            self._async_stop = stop
        self._set_snapshot("running", enabled=True)
        await worker.run_forever(stop)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with self.app.app_context():
                    worker = self.build_worker()
            except Exception as exc:  # noqa: BLE001 - retry boundary is deliberate
                code = str(getattr(exc, "code", type(exc).__name__))
                delay = self._record_restart_failure()
                self._set_snapshot(
                    "waiting",
                    enabled=self._enabled(),
                    error_code=code,
                    consecutive_failures=self._restart_count(),
                )
                if self._stop.wait(delay):
                    return
                continue

            loop = asyncio.new_event_loop()
            with self._lock:
                self._loop = loop
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._run_worker(worker))
            except Exception as exc:  # noqa: BLE001 - worker must remain restartable
                code = str(getattr(exc, "code", type(exc).__name__))
                delay = self._record_restart_failure()
                self._set_snapshot(
                    "retrying",
                    enabled=True,
                    error_code=code,
                    consecutive_failures=self._restart_count(),
                )
                if self._stop.wait(delay):
                    return
            finally:
                with self._lock:
                    self._async_stop = None
                    self._loop = None
                loop.close()
                asyncio.set_event_loop(None)
        self._set_snapshot("stopped", enabled=self._enabled())

    def start(self) -> None:
        if not self._enabled():
            self._set_snapshot("disabled", enabled=False)
            return
        # Validate the non-network authority seams before allocating a thread.
        try:
            _required_group(
                self.app.config.get("RECORDER_FEDERATION_STORAGE_GROUP_ID", "")
            )
            self._client_factory()
        except FederationValidationError as exc:
            self._set_snapshot(
                "not-configured",
                enabled=True,
                error_code=exc.code,
            )
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="fcp-recorder-federation-publication",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            loop = self._loop
            async_stop = self._async_stop
        if loop is not None and async_stop is not None:
            loop.call_soon_threadsafe(async_stop.set)

    def run_once(self) -> RecorderWorkerCycleResult:
        """Run one fully authorized cycle for diagnostics and deterministic tests."""

        with self.app.app_context():
            worker = self.build_worker()
            return asyncio.run(worker.run_cycle(force_reconcile=True))


def install_recorder_federation_publication(
    app: Flask,
    *,
    onboarding_service: object,
) -> RecorderFederationPublicationMonitor:
    """Install opt-in near-live recorder publication into the Flask lifecycle."""

    app.config.setdefault(
        "RECORDER_FEDERATION_PUBLICATION_ENABLED",
        _env_bool("FCP_RECORDER_FEDERATION_ENABLED", False),
    )
    app.config.setdefault(
        "RECORDER_FEDERATION_STORAGE_GROUP_ID",
        os.getenv("FCP_RECORDER_FEDERATION_STORAGE_GROUP", ""),
    )
    app.config.setdefault("RECORDER_FEDERATION_STORAGE_CLIENT_FACTORY", None)
    app.config.setdefault(
        "RECORDER_FEDERATION_DATA_DIRECTORY",
        os.getenv("FCP_RECORDER_DATA_DIR", "data"),
    )
    app.config.setdefault(
        "RECORDER_FEDERATION_CHECKPOINT_FILE",
        os.getenv(
            "FCP_RECORDER_STATE_FILE",
            "data/source_state/mtconnect_recorder_state.json",
        ),
    )
    app.config.setdefault(
        "RECORDER_FEDERATION_OUTBOX_DATABASE",
        os.getenv(
            "FCP_RECORDER_FEDERATION_OUTBOX_DATABASE",
            "data/federation/recorder_publication/outbox.sqlite3",
        ),
    )
    app.config.setdefault(
        "RECORDER_FEDERATION_POLL_INTERVAL_SECONDS",
        float(os.getenv("FCP_RECORDER_FEDERATION_POLL_INTERVAL", "0.2")),
    )
    app.config.setdefault(
        "RECORDER_FEDERATION_DELIVERY_LIMIT",
        int(os.getenv("FCP_RECORDER_FEDERATION_DELIVERY_LIMIT", "100")),
    )

    monitor = RecorderFederationPublicationMonitor(app, onboarding_service)
    app.extensions[_EXTENSION_KEY] = monitor

    @app.before_request
    def _start_recorder_federation_publication() -> None:
        monitor.start()

    return monitor


__all__ = [
    "RecorderFederationPublicationMonitor",
    "RecorderFederationPublicationSnapshot",
    "install_recorder_federation_publication",
]

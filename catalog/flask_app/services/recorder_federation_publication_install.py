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
    RecorderPublicationTarget,
    RecorderWorkerCycleResult,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore

_EXTENSION_KEY = "recorder_federation_publication"
_DEFAULT_RETRY_SECONDS = 1.0


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

    def snapshot(self) -> RecorderFederationPublicationSnapshot:
        with self._lock:
            return self._snapshot

    def _set_snapshot(
        self,
        status: str,
        *,
        enabled: bool,
        error_code: str | None = None,
    ) -> None:
        with self._lock:
            self._snapshot = RecorderFederationPublicationSnapshot(
                status=status,
                enabled=enabled,
                last_error_code=error_code,
            )

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
                self._set_snapshot(
                    "waiting",
                    enabled=self._enabled(),
                    error_code=code,
                )
                if self._stop.wait(_DEFAULT_RETRY_SECONDS):
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
                self._set_snapshot("retrying", enabled=True, error_code=code)
                if self._stop.wait(_DEFAULT_RETRY_SECONDS):
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

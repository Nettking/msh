"""Refresh the Flask artifact catalog after durable recorder progress.

The recorder and Flask normally run in separate containers.  The recorder's
checkpoint is therefore the small, durable notification boundary: watching its
metadata lets Flask request a background catalog scan without walking data
directories on a request thread.
"""

from __future__ import annotations

import math
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from flask import Flask

from .catalog_service import ArtifactCatalog

RECORDER_ARTIFACT_REFRESH_EXTENSION = "recorder_artifact_refresh_monitor"
DEFAULT_RECORDER_CHECKPOINT = "data/source_state/mtconnect_recorder_state.json"
DEFAULT_POLL_INTERVAL_SECONDS = 1.0
MIN_POLL_INTERVAL_SECONDS = 0.05
MAX_POLL_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class RecorderArtifactRefreshSnapshot:
    status: str
    running: bool
    checkpoint_file: str
    observed_mtime_ns: int | None
    observed_size: int | None
    refresh_requests: int
    last_error: str | None = None


class RecorderArtifactRefreshMonitor:
    """Watch only recorder checkpoint metadata and request catalog refreshes."""

    def __init__(
        self,
        *,
        catalog: ArtifactCatalog,
        checkpoint_file: str | Path,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        interval = float(poll_interval_seconds)
        if not math.isfinite(interval):
            interval = DEFAULT_POLL_INTERVAL_SECONDS
        self.catalog = catalog
        self.checkpoint_file = Path(checkpoint_file)
        self.poll_interval_seconds = min(
            max(interval, MIN_POLL_INTERVAL_SECONDS),
            MAX_POLL_INTERVAL_SECONDS,
        )
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._observed_signature: tuple[int, int] | None = None
        self._refresh_pending_reason: str | None = None
        self._snapshot = RecorderArtifactRefreshSnapshot(
            status="not-started",
            running=False,
            checkpoint_file=str(self.checkpoint_file),
            observed_mtime_ns=None,
            observed_size=None,
            refresh_requests=0,
        )

    def snapshot(self) -> RecorderArtifactRefreshSnapshot:
        with self._lock:
            return self._snapshot

    def start(self) -> bool:
        """Start once and reconcile an existing checkpoint in the background."""

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            signature, error = self._read_signature()
            self._observed_signature = signature
            # The application's startup scan and this lazy first-request start
            # can race a recorder commit.  Queue one bounded baseline refresh
            # when a checkpoint already exists so that a final recorder batch
            # cannot remain invisible until some later checkpoint change.
            self._refresh_pending_reason = (
                "recorder_checkpoint_baseline"
                if signature is not None and error is None
                else None
            )
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._run,
                args=(stop_event,),
                name="fcp-recorder-artifact-refresh",
                daemon=True,
            )
            self._stop_event = stop_event
            self._thread = thread
            self._replace_snapshot(
                status=(
                    "error"
                    if error is not None
                    else "running"
                    if signature is not None
                    else "waiting-for-checkpoint"
                ),
                running=True,
                signature=signature,
                error=error,
            )
            thread.start()
            return True

    def stop(self) -> None:
        """Stop the current watcher safely; a later ``start`` creates a baseline."""

        with self._lock:
            thread = self._thread
            stop_event = self._stop_event
            if stop_event is not None:
                stop_event.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        with self._lock:
            running = bool(thread is not None and thread.is_alive())
            if not running and self._thread is thread:
                self._thread = None
                self._stop_event = None
            self._replace_snapshot(
                status="stopping" if running else "stopped",
                running=running,
                signature=self._observed_signature,
                error=self._snapshot.last_error,
            )

    def _run(self, stop_event: threading.Event) -> None:
        try:
            while not stop_event.wait(self.poll_interval_seconds):
                self._poll_once()
        finally:
            with self._lock:
                if self._stop_event is stop_event:
                    self._thread = None
                    self._stop_event = None
                    self._replace_snapshot(
                        status="stopped",
                        running=False,
                        signature=self._observed_signature,
                        error=self._snapshot.last_error,
                    )

    def _poll_once(self) -> None:
        signature, error = self._read_signature()
        with self._lock:
            previous = self._observed_signature
            if error is not None:
                self._replace_snapshot(
                    status="error",
                    running=True,
                    signature=previous,
                    error=error,
                )
                return
            if signature != previous:
                # Advance before notifying.  A failed request is retained as one
                # pending refresh instead of creating an unbounded request pile.
                self._observed_signature = signature
                self._refresh_pending_reason = (
                    "recorder_checkpoint_changed"
                    if signature is not None
                    else "recorder_checkpoint_removed"
                )
            reason = self._refresh_pending_reason
            if reason is None:
                # Clear a transient stat error even when the checkpoint did not
                # change while access to it was unavailable.
                if self._snapshot.last_error is not None:
                    self._replace_snapshot(
                        status=(
                            "running"
                            if signature is not None
                            else "waiting-for-checkpoint"
                        ),
                        running=True,
                        signature=signature,
                        error=None,
                    )
                return

        try:
            accepted = bool(self.catalog.start_background_rescan_if_idle(reason=reason))
        except Exception as exc:  # noqa: BLE001 - monitor remains restartable
            with self._lock:
                self._replace_snapshot(
                    status="error",
                    running=True,
                    signature=signature,
                    error=f"{type(exc).__name__}: {exc}",
                )
            return

        with self._lock:
            # A test double or future scheduler may explicitly decline work.
            # Keep one pending request so a later poll can retry it safely.
            if not accepted:
                self._replace_snapshot(
                    status=(
                        "running" if signature is not None else "waiting-for-checkpoint"
                    ),
                    running=True,
                    signature=signature,
                    error=None,
                )
                return
            if self._refresh_pending_reason == reason:
                self._refresh_pending_reason = None
            self._replace_snapshot(
                status=(
                    "running" if signature is not None else "waiting-for-checkpoint"
                ),
                running=True,
                signature=signature,
                error=None,
                refresh_requests=self._snapshot.refresh_requests + 1,
            )

    def _read_signature(self) -> tuple[tuple[int, int] | None, str | None]:
        try:
            stat = self.checkpoint_file.stat()
        except FileNotFoundError:
            return None, None
        except OSError as exc:
            return None, f"{type(exc).__name__}: {exc}"
        if not self.checkpoint_file.is_file():
            return None, None
        return (stat.st_mtime_ns, stat.st_size), None

    def _replace_snapshot(
        self,
        *,
        status: str,
        running: bool,
        signature: tuple[int, int] | None,
        error: str | None,
        refresh_requests: int | None = None,
    ) -> None:
        mtime_ns, size = signature if signature is not None else (None, None)
        self._snapshot = RecorderArtifactRefreshSnapshot(
            status=status,
            running=running,
            checkpoint_file=str(self.checkpoint_file),
            observed_mtime_ns=mtime_ns,
            observed_size=size,
            refresh_requests=(
                self._snapshot.refresh_requests
                if refresh_requests is None
                else refresh_requests
            ),
            last_error=error,
        )


def install_recorder_artifact_refresh(
    app: Flask,
    *,
    catalog: ArtifactCatalog,
) -> RecorderArtifactRefreshMonitor:
    """Install the recorder checkpoint watcher into Flask's lazy lifecycle."""

    existing = app.extensions.get(RECORDER_ARTIFACT_REFRESH_EXTENSION)
    if isinstance(existing, RecorderArtifactRefreshMonitor):
        return existing
    app.config.setdefault(
        "RECORDER_ARTIFACT_REFRESH_CHECKPOINT_FILE",
        os.getenv("FCP_RECORDER_STATE_FILE", DEFAULT_RECORDER_CHECKPOINT),
    )
    app.config.setdefault(
        "RECORDER_ARTIFACT_REFRESH_POLL_INTERVAL_SECONDS",
        DEFAULT_POLL_INTERVAL_SECONDS,
    )
    monitor = RecorderArtifactRefreshMonitor(
        catalog=catalog,
        checkpoint_file=app.config["RECORDER_ARTIFACT_REFRESH_CHECKPOINT_FILE"],
        poll_interval_seconds=app.config[
            "RECORDER_ARTIFACT_REFRESH_POLL_INTERVAL_SECONDS"
        ],
    )
    app.extensions[RECORDER_ARTIFACT_REFRESH_EXTENSION] = monitor

    @app.before_request
    def _start_recorder_artifact_refresh() -> None:
        monitor.start()

    return monitor


__all__ = [
    "DEFAULT_RECORDER_CHECKPOINT",
    "RECORDER_ARTIFACT_REFRESH_EXTENSION",
    "RecorderArtifactRefreshMonitor",
    "RecorderArtifactRefreshSnapshot",
    "install_recorder_artifact_refresh",
]

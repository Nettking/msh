from __future__ import annotations

import os
import threading
from pathlib import Path

from flask import Flask

from catalog.flask_app.services.recorder_artifact_refresh import (
    RECORDER_ARTIFACT_REFRESH_EXTENSION,
    RecorderArtifactRefreshMonitor,
    install_recorder_artifact_refresh,
)


def _write_checkpoint(path: Path, payload: str) -> None:
    """Replace the checkpoint the way the recorder actually writes it.

    ``Path.write_text`` truncates before writing, so a poll that lands inside
    the write observes an intermediate state and reports two changes for one
    logical update. The recorder writes checkpoints atomically
    (``start_recorder.py`` ``_write_json_atomic``), so the tests must too.
    """

    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(payload, encoding="utf-8")
    os.replace(temporary, path)


class _RecordingCatalog:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.changed = threading.Event()
        self.calls: list[str] = []

    def start_background_rescan_if_idle(self, *, reason: str) -> bool:
        with self._lock:
            self.calls.append(reason)
        self.changed.set()
        return True

    def call_count(self) -> int:
        with self._lock:
            return len(self.calls)


def _monitor(
    catalog: _RecordingCatalog,
    checkpoint: Path,
) -> RecorderArtifactRefreshMonitor:
    return RecorderArtifactRefreshMonitor(
        catalog=catalog,  # type: ignore[arg-type]
        checkpoint_file=checkpoint,
        poll_interval_seconds=0.05,
    )


def test_existing_checkpoint_reconciles_baseline_then_refreshes_one_change(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    checkpoint.parent.mkdir(parents=True)
    _write_checkpoint(checkpoint, '{"revision":0}')
    catalog = _RecordingCatalog()
    monitor = _monitor(catalog, checkpoint)

    try:
        assert monitor.start() is True
        assert monitor.start() is False
        assert catalog.changed.wait(1.0) is True
        assert catalog.calls == ["recorder_checkpoint_baseline"]
        catalog.changed.clear()
        assert catalog.changed.wait(0.15) is False

        _write_checkpoint(
            checkpoint,
            '{"revision":1,"new_records":3}',
        )
        assert catalog.changed.wait(1.0) is True
        catalog.changed.clear()
        assert catalog.changed.wait(0.15) is False

        assert catalog.calls == [
            "recorder_checkpoint_baseline",
            "recorder_checkpoint_changed",
        ]
        snapshot = monitor.snapshot()
        assert snapshot.running is True
        assert snapshot.status == "running"
        assert snapshot.observed_size == checkpoint.stat().st_size
        assert snapshot.refresh_requests == 2
        assert snapshot.last_error is None
    finally:
        monitor.stop()

    assert monitor.snapshot().running is False
    assert monitor.snapshot().status == "stopped"


def test_missing_checkpoint_is_watched_and_restart_takes_a_fresh_baseline(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    catalog = _RecordingCatalog()
    monitor = _monitor(catalog, checkpoint)

    try:
        assert monitor.start() is True
        assert monitor.snapshot().status == "waiting-for-checkpoint"
        assert catalog.changed.wait(0.15) is False

        checkpoint.parent.mkdir(parents=True)
        _write_checkpoint(checkpoint, '{"revision":1}')
        assert catalog.changed.wait(1.0) is True
        assert catalog.call_count() == 1
        monitor.stop()

        catalog.changed.clear()
        assert monitor.start() is True
        assert monitor.start() is False
        assert catalog.changed.wait(1.0) is True
        assert catalog.calls == [
            "recorder_checkpoint_changed",
            "recorder_checkpoint_baseline",
        ]
        catalog.changed.clear()
        assert catalog.changed.wait(0.15) is False

        _write_checkpoint(
            checkpoint,
            '{"revision":2,"new_records":10}',
        )
        assert catalog.changed.wait(1.0) is True
        assert catalog.calls == [
            "recorder_checkpoint_changed",
            "recorder_checkpoint_baseline",
            "recorder_checkpoint_changed",
        ]
        assert monitor.snapshot().refresh_requests == 3
    finally:
        monitor.stop()


def test_removing_existing_checkpoint_requests_catalog_refresh(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    checkpoint.parent.mkdir(parents=True)
    _write_checkpoint(checkpoint, '{"revision":1}')
    catalog = _RecordingCatalog()
    monitor = _monitor(catalog, checkpoint)

    try:
        monitor.start()
        assert catalog.changed.wait(1.0) is True
        catalog.changed.clear()
        checkpoint.unlink()

        assert catalog.changed.wait(1.0) is True
        assert catalog.calls == [
            "recorder_checkpoint_baseline",
            "recorder_checkpoint_removed",
        ]
        snapshot = monitor.snapshot()
        assert snapshot.status == "waiting-for-checkpoint"
        assert snapshot.observed_mtime_ns is None
        assert snapshot.observed_size is None
        assert snapshot.refresh_requests == 2
    finally:
        monitor.stop()


def test_transient_stat_error_clears_without_requiring_checkpoint_change(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    checkpoint.parent.mkdir(parents=True)
    _write_checkpoint(checkpoint, '{"revision":1}')
    catalog = _RecordingCatalog()
    monitor = _monitor(catalog, checkpoint)
    signature = (checkpoint.stat().st_mtime_ns, checkpoint.stat().st_size)
    responses = iter(
        [
            (None, "PermissionError: temporarily unavailable"),
            (signature, None),
        ]
    )
    monkeypatch.setattr(monitor, "_read_signature", lambda: next(responses))
    monitor._observed_signature = signature

    monitor._poll_once()
    assert monitor.snapshot().status == "error"
    monitor._poll_once()

    snapshot = monitor.snapshot()
    assert snapshot.status == "running"
    assert snapshot.last_error is None
    assert catalog.calls == []


def test_declined_catalog_refresh_is_retained_as_one_pending_retry(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    checkpoint.parent.mkdir(parents=True)
    _write_checkpoint(checkpoint, '{"revision":1}')

    class DecliningCatalog(_RecordingCatalog):
        def __init__(self) -> None:
            super().__init__()
            self.accept = False

        def start_background_rescan_if_idle(self, *, reason: str) -> bool:
            with self._lock:
                self.calls.append(reason)
            return self.accept

    catalog = DecliningCatalog()
    monitor = _monitor(catalog, checkpoint)

    monitor._poll_once()
    assert catalog.calls == ["recorder_checkpoint_changed"]
    assert monitor.snapshot().refresh_requests == 0

    catalog.accept = True
    monitor._poll_once()
    assert catalog.calls == [
        "recorder_checkpoint_changed",
        "recorder_checkpoint_changed",
    ]
    assert monitor.snapshot().refresh_requests == 1


def test_install_registers_extension_and_starts_lazily_once(
    tmp_path: Path,
) -> None:
    app = Flask(__name__)
    checkpoint = tmp_path / "source_state" / "mtconnect_recorder_state.json"
    app.config.update(
        RECORDER_ARTIFACT_REFRESH_CHECKPOINT_FILE=str(checkpoint),
        RECORDER_ARTIFACT_REFRESH_POLL_INTERVAL_SECONDS=0.05,
    )
    catalog = _RecordingCatalog()
    monitor = install_recorder_artifact_refresh(
        app,
        catalog=catalog,  # type: ignore[arg-type]
    )
    assert (
        install_recorder_artifact_refresh(
            app,
            catalog=catalog,  # type: ignore[arg-type]
        )
        is monitor
    )

    @app.get("/health")
    def health() -> str:
        return "ok"

    try:
        assert app.extensions[RECORDER_ARTIFACT_REFRESH_EXTENSION] is monitor
        assert monitor.snapshot().status == "not-started"
        client = app.test_client()
        assert client.get("/health").status_code == 200
        assert client.get("/health").status_code == 200
        assert monitor.snapshot().running is True
        assert monitor.start() is False
        assert catalog.call_count() == 0
    finally:
        monitor.stop()

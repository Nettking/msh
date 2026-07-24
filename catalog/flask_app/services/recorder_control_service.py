from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from .server_setup_service import ServerSetupSettings


class RecorderControlError(RuntimeError):
    """Raised when the recorder cannot be started or stopped safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _tail_text(path: Path, maximum_characters: int = 3000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-maximum_characters:]


class RecorderProcessManager:
    """Start and stop the existing MTConnect recorder inside the Flask container.

    This deliberately reuses ``standalone-recorder_v2.py`` instead of maintaining
    a second recorder implementation. The child process receives the recorder
    settings saved by the browser setup and writes into the same mounted ``data``
    directory as Flask.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._process: subprocess.Popen[str] | None = None
        self._log_handle: TextIO | None = None
        self._started_at: str | None = None
        self._sources = ""
        self._last_message = "Recorder has not been started from this web process."

        self.repo_root = Path(__file__).resolve().parents[3]
        self.script_path = (
            self.repo_root
            / "catalog"
            / "standalone-recorder_v2"
            / "standalone-recorder_v2.py"
        )
        self.log_path = (
            self.repo_root
            / "data"
            / "source_state"
            / "mtconnect_recorder_process.log"
        )

    def _is_running_unlocked(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def status(self) -> dict[str, Any]:
        with self._lock:
            running = self._is_running_unlocked()
            exit_code = None
            if self._process is not None and not running:
                exit_code = self._process.poll()
                if exit_code is not None and self._last_message.startswith("Recorder started"):
                    self._last_message = f"Recorder stopped unexpectedly with exit code {exit_code}."
            return {
                "running": running,
                "pid": self._process.pid if running and self._process is not None else None,
                "started_at": self._started_at,
                "sources": self._sources,
                "log_path": str(self.log_path.relative_to(self.repo_root)),
                "last_message": self._last_message,
                "exit_code": exit_code,
                "log_tail": _tail_text(self.log_path),
            }

    def start(self, settings: ServerSetupSettings) -> tuple[bool, str]:
        with self._lock:
            if self._is_running_unlocked():
                return True, "Recording is already on."

            if settings.deployment_mode not in {"full-server", "recorder-only"}:
                raise RecorderControlError(
                    "This device must use the Full server or Recorder only role before recording can start."
                )

            sources = settings.recorder_sources.strip()
            if not sources:
                raise RecorderControlError(
                    "No MTConnect source is configured. Add at least one source in device setup first."
                )

            if not self.script_path.exists():
                raise RecorderControlError(
                    f"Recorder script was not found: {self.script_path}"
                )

            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            self._close_log_unlocked()
            self._log_handle = self.log_path.open("a", encoding="utf-8", buffering=1)
            self._log_handle.write(f"\n=== Recorder requested from web at {_utc_now()} ===\n")

            environment = os.environ.copy()
            environment.update(
                {
                    "MSH_RECORDER_SOURCES": sources,
                    "MSH_RECORDER_DATA_DIR": "data",
                    "MSH_RECORDER_STATE_FILE": "data/source_state/mtconnect_recorder_state.json",
                    "MSH_RECORDER_POLL_INTERVAL": settings.recorder_poll_interval or "0.2",
                    "MSH_RECORDER_FLUSH_INTERVAL": "1.0",
                    "MSH_RECORDER_REQUEST_TIMEOUT": "1.0",
                    "MSH_RECORDER_INCLUDE_CONDITION": (
                        "true" if settings.recorder_include_condition else "false"
                    ),
                }
            )

            try:
                self._process = subprocess.Popen(
                    [sys.executable, str(self.script_path)],
                    cwd=self.repo_root,
                    env=environment,
                    stdout=self._log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
            except OSError as exc:
                self._process = None
                self._close_log_unlocked()
                raise RecorderControlError(f"Could not launch recorder: {exc}") from exc

            self._started_at = _utc_now()
            self._sources = sources
            time.sleep(0.6)

            if not self._is_running_unlocked():
                exit_code = self._process.poll() if self._process is not None else None
                details = _tail_text(self.log_path)
                self._last_message = f"Recorder failed to start with exit code {exit_code}."
                self._close_log_unlocked()
                return False, self._last_message + (f" Log: {details}" if details else "")

            self._last_message = "Recorder started and is polling the configured MTConnect sources."
            return True, self._last_message

    def stop(self) -> tuple[bool, str]:
        with self._lock:
            if not self._is_running_unlocked():
                self._close_log_unlocked()
                self._last_message = "Recording is already off."
                return True, self._last_message

            assert self._process is not None
            self._process.terminate()
            try:
                self._process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=3)

            exit_code = self._process.returncode
            self._close_log_unlocked()
            self._last_message = f"Recording stopped. Recorder exit code: {exit_code}."
            return True, self._last_message

    def _close_log_unlocked(self) -> None:
        if self._log_handle is not None:
            try:
                self._log_handle.flush()
                self._log_handle.close()
            except OSError:
                pass
        self._log_handle = None


_RECORDER_MANAGER: RecorderProcessManager | None = None


def get_recorder_process_manager() -> RecorderProcessManager:
    global _RECORDER_MANAGER
    if _RECORDER_MANAGER is None:
        _RECORDER_MANAGER = RecorderProcessManager()
    return _RECORDER_MANAGER

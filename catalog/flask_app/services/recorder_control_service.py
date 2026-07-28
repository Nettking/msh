from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .server_setup_service import ServerSetupSettings


CONTROL_PATH = Path("data") / "source_state" / "mtconnect_recorder_control.json"
STATUS_PATH = Path("data") / "source_state" / "mtconnect_recorder_status.json"
LOG_PATH = Path("data") / "source_state" / "mtconnect_recorder.log"
HEARTBEAT_TIMEOUT_SECONDS = 10


class RecorderControlError(RuntimeError):
    """Raised when recording cannot be enabled safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tail_text(path: Path, maximum_characters: int = 4000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return text[-maximum_characters:]


class RecorderControlService:
    """Control the independent recorder service through durable shared files.

    Flask never owns the recorder process. The browser writes the desired state to
    ``CONTROL_PATH``. The separate Docker recorder service watches that file,
    records while enabled, and publishes a heartbeat and diagnostics to
    ``STATUS_PATH``. This survives Flask restarts and avoids duplicate child
    processes when multiple web workers are used.
    """

    def __init__(
        self,
        *,
        control_path: Path | str = CONTROL_PATH,
        status_path: Path | str = STATUS_PATH,
        log_path: Path | str = LOG_PATH,
    ) -> None:
        self.control_path = Path(control_path)
        self.status_path = Path(status_path)
        self.log_path = Path(log_path)

    @staticmethod
    def ready(settings: ServerSetupSettings | None) -> bool:
        return bool(
            settings
            and settings.configured
            and settings.user_setup_complete
            and settings.deployment_mode in {"full-server", "recorder-only"}
            and settings.recorder_sources.strip()
        )

    def set_enabled(
        self,
        enabled: bool,
        settings: ServerSetupSettings,
    ) -> tuple[bool, str]:
        if enabled and not self.ready(settings):
            if settings.deployment_mode not in {"full-server", "recorder-only"}:
                raise RecorderControlError(
                    "Choose the Full server or Recorder station role before starting recording."
                )
            raise RecorderControlError(
                "Add at least one MTConnect source in device setup before starting recording."
            )

        payload = {
            "schema": "msh.mtconnect_recorder.control.v1",
            "enabled": bool(enabled),
            "updated_at": _utc_now(),
            "requested_by": "web",
        }
        _write_json_atomic(self.control_path, payload)
        if enabled:
            return True, "Recording requested. The recorder service will start polling within a few seconds."
        return True, "Recording stopped. The recorder service will flush buffered rows and remain on standby."

    def status(self, settings: ServerSetupSettings | None) -> dict[str, Any]:
        control = _read_json(self.control_path)
        runtime = _read_json(self.status_path)
        requested_enabled = bool(control.get("enabled", False))

        heartbeat = _parse_utc(runtime.get("heartbeat_at"))
        heartbeat_age = None
        worker_alive = False
        if heartbeat is not None:
            heartbeat_age = max(
                0.0,
                (datetime.now(timezone.utc) - heartbeat).total_seconds(),
            )
            worker_alive = heartbeat_age <= HEARTBEAT_TIMEOUT_SECONDS

        runtime_state = str(runtime.get("state") or "offline")
        recorder_ready = self.ready(settings)
        running = bool(
            recorder_ready
            and requested_enabled
            and worker_alive
            and runtime_state == "recording"
        )

        if not recorder_ready:
            state = "not_configured"
            message = "Choose a recorder role and configure an MTConnect source."
        elif not worker_alive:
            state = "offline"
            message = (
                "Recorder service is not reporting. Rebuild/restart the Docker services, "
                "then try again."
            )
        elif not requested_enabled:
            state = "stopped"
            message = "Recorder service is healthy and waiting. Recording is off."
        elif runtime_state == "recording":
            state = "recording"
            message = str(runtime.get("message") or "Recording from configured MTConnect sources.")
        elif runtime_state == "error":
            state = "error"
            message = str(runtime.get("last_error") or runtime.get("message") or "Recorder reported an error.")
        else:
            state = "starting"
            message = str(runtime.get("message") or "Recording was requested and is starting.")

        return {
            "ready": recorder_ready,
            "requested_enabled": requested_enabled,
            "running": running,
            "worker_alive": worker_alive,
            "state": state,
            "message": message,
            "last_message": message,
            "heartbeat_at": runtime.get("heartbeat_at"),
            "heartbeat_age_seconds": heartbeat_age,
            "started_at": runtime.get("recording_started_at"),
            "sources": runtime.get("sources") or [],
            "source_status": runtime.get("source_status") or {},
            "records_written": int(runtime.get("records_written") or 0),
            "records_buffered": int(runtime.get("records_buffered") or 0),
            "last_flush_at": runtime.get("last_flush_at"),
            "last_error": runtime.get("last_error") or "",
            "control_path": str(self.control_path),
            "status_path": str(self.status_path),
            "log_path": str(self.log_path),
            "log_tail": _tail_text(self.log_path),
        }


_RECORDER_CONTROL_SERVICE: RecorderControlService | None = None


def get_recorder_control_service() -> RecorderControlService:
    global _RECORDER_CONTROL_SERVICE
    if _RECORDER_CONTROL_SERVICE is None:
        _RECORDER_CONTROL_SERVICE = RecorderControlService()
    return _RECORDER_CONTROL_SERVICE

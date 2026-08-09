from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .server_setup_service import ServerSetupError, parse_recorder_sources

CONTROL_PATH = Path("data") / "source_state" / "mtconnect_recorder_control.json"
STATUS_PATH = Path("data") / "source_state" / "mtconnect_recorder_status.json"
LOG_PATH = Path("data") / "source_state" / "mtconnect_recorder.log"
HEARTBEAT_TIMEOUT_SECONDS = 10


class RecorderConfiguration(Protocol):
    """Minimal recorder configuration shape; it carries no runtime authority."""

    recorder_sources: str


class RecorderControlError(RuntimeError):
    """Raised when recording cannot be enabled safely."""


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


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


def _safe_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: object) -> str:
    return str(value or "").strip()


def _configured_sources(
    config: RecorderConfiguration | object | None,
) -> dict[str, str]:
    if config is None:
        return {}
    try:
        return parse_recorder_sources(
            str(getattr(config, "recorder_sources", "") or "")
        )
    except (ServerSetupError, TypeError, ValueError):
        return {}


class RecorderControlService:
    """Control the independent recorder service through durable shared files.

    Flask never owns the recorder process. The browser writes the desired state to
    ``CONTROL_PATH``. The separate Docker recorder service watches that file,
    records while enabled, and publishes a heartbeat and diagnostics to
    ``STATUS_PATH``. This survives Flask restarts and avoids duplicate child
    processes when multiple web workers are used.

    This service validates recorder *configuration* only. Whether a caller has
    authority to start recording is decided by capability/contribution state
    before calling ``set_enabled``; legacy device roles are not authority here.
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
    def ready(config: RecorderConfiguration | object | None) -> bool:
        return bool(_configured_sources(config))

    def set_enabled(
        self,
        enabled: bool,
        config: RecorderConfiguration | object,
    ) -> tuple[bool, str]:
        if enabled and not self.ready(config):
            raise RecorderControlError(
                "Add at least one MTConnect source before starting recording."
            )

        payload = {
            "schema": "msh.mtconnect_recorder.control.v1",
            "enabled": bool(enabled),
            "updated_at": _utc_now(),
            "requested_by": "web",
        }
        _write_json_atomic(self.control_path, payload)
        if enabled:
            return (
                True,
                "Recording requested. The recorder service will start polling "
                "within a few seconds.",
            )
        return (
            True,
            "Recording stopped. The recorder service will flush buffered rows "
            "and remain on standby.",
        )

    def status(
        self,
        config: RecorderConfiguration | object | None,
        *,
        include_diagnostics: bool = False,
    ) -> dict[str, Any]:
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
        recorder_ready = self.ready(config)
        running = bool(
            recorder_ready
            and requested_enabled
            and worker_alive
            and runtime_state == "recording"
        )

        if not recorder_ready:
            state = "not_configured"
            message = "Configure at least one MTConnect source."
        elif not worker_alive:
            state = "offline"
            message = (
                "Recorder service is not reporting. Rebuild/restart the Docker "
                "services, then try again."
            )
        elif not requested_enabled:
            state = "stopped"
            message = "Recorder service is healthy and waiting. Recording is off."
        elif runtime_state == "recording":
            state = "recording"
            message = str(
                runtime.get("message")
                or "Recording from configured MTConnect sources."
            )
        elif runtime_state == "error":
            state = "error"
            message = str(
                runtime.get("last_error")
                or runtime.get("message")
                or "Recorder reported an error."
            )
        else:
            state = "starting"
            message = str(
                runtime.get("message")
                or "Recording was requested and is starting."
            )

        configured_sources = _configured_sources(config)
        raw_source_status = runtime.get("source_status")
        if not isinstance(raw_source_status, Mapping):
            raw_source_status = {}
        source_names = (
            set(configured_sources)
            if config is not None
            else {str(name) for name in raw_source_status}
        )
        source_status: dict[str, dict[str, Any]] = {}
        for source_name in sorted(source_names, key=str.casefold):
            raw_source = raw_source_status.get(source_name)
            if not isinstance(raw_source, Mapping):
                raw_source = {}
            last_error = _text(raw_source.get("last_error"))
            caught_up = bool(raw_source.get("caught_up", False))
            if not worker_alive:
                source_state = "offline"
            elif not requested_enabled:
                source_state = "stopped"
            elif last_error:
                source_state = "error"
            elif running and caught_up:
                source_state = "caught_up"
            elif running:
                source_state = "polling"
            else:
                source_state = "waiting"
            source_status[source_name] = {
                "machine_id": _text(raw_source.get("machine_id")),
                "base_url": (
                    _text(raw_source.get("base_url"))
                    or configured_sources.get(source_name, "")
                ),
                "last_success_at": _text(raw_source.get("last_success_at")),
                "next_sequence": _safe_optional_int(
                    raw_source.get("next_sequence")
                ),
                "agent_last_sequence": _safe_optional_int(
                    raw_source.get("agent_last_sequence")
                ),
                "caught_up": caught_up,
                "last_error": last_error,
                "state": source_state,
            }

        payload = {
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
            "sources": sorted(source_status, key=str.casefold),
            "source_status": source_status,
            "records_written": max(
                0,
                _safe_int(runtime.get("records_written")),
            ),
            "records_buffered": max(
                0,
                _safe_int(runtime.get("records_buffered")),
            ),
            "last_flush_at": runtime.get("last_flush_at"),
            "last_error": runtime.get("last_error") or "",
        }
        if include_diagnostics:
            payload.update(
                {
                    "control_path": str(self.control_path),
                    "status_path": str(self.status_path),
                    "log_path": str(self.log_path),
                    "log_tail": _tail_text(self.log_path),
                }
            )
        return payload

    def web_status(
        self,
        config: RecorderConfiguration | object | None,
    ) -> dict[str, Any]:
        """Return the small, debug-free status contract used by live polling."""

        status = self.status(config)
        sources = [
            {
                "source_name": source_name,
                "machine_id": source.get("machine_id", ""),
                "base_url": source.get("base_url", ""),
                "last_success_at": source.get("last_success_at", ""),
                "next_sequence": source.get("next_sequence"),
                "agent_last_sequence": source.get("agent_last_sequence"),
                "caught_up": bool(source.get("caught_up", False)),
                "last_error": source.get("last_error", ""),
                "state": source.get("state", "waiting"),
            }
            for source_name, source in status["source_status"].items()
        ]
        return {
            "schema": "msh.recorder.web_status.v1",
            "generated_at": _utc_now(),
            "poll_after_ms": 2000,
            "ready": status["ready"],
            "requested_enabled": status["requested_enabled"],
            "running": status["running"],
            "worker_alive": status["worker_alive"],
            "state": status["state"],
            "message": status["message"],
            "heartbeat_at": status["heartbeat_at"],
            "records_written": status["records_written"],
            "records_buffered": status["records_buffered"],
            "last_flush_at": status["last_flush_at"],
            "sources": sources,
        }


_RECORDER_CONTROL_SERVICE: RecorderControlService | None = None


def get_recorder_control_service() -> RecorderControlService:
    global _RECORDER_CONTROL_SERVICE
    if _RECORDER_CONTROL_SERVICE is None:
        _RECORDER_CONTROL_SERVICE = RecorderControlService()
    return _RECORDER_CONTROL_SERVICE

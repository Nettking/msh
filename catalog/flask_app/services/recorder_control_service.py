from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .capability_config_service import (
    CapabilityConfig,
    CapabilityConfigError,
    parse_recorder_sources,
)

CONTROL_PATH = Path("data") / "source_state" / "mtconnect_recorder_control.json"
STATUS_PATH = Path("data") / "source_state" / "mtconnect_recorder_status.json"
LOG_PATH = Path("data") / "source_state" / "mtconnect_recorder.log"
HEARTBEAT_TIMEOUT_SECONDS = 10
PAUSE_GUARD_MAX_SECONDS = 20 * 60
_OPERATION_ID_PATTERN = re.compile(r"\A[a-f0-9]{32}\Z")
_SHA256_PATTERN = re.compile(r"\A[a-f0-9]{64}\Z")


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
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


@contextmanager
def _control_file_lock(path: Path):
    """Serialize supported control mutations across Flask worker processes."""

    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as stream:
        if os.name == "nt" and lock_path.stat().st_size == 0:
            stream.write(b"\0")
            stream.flush()
        deadline = time.monotonic() + 5.0
        while True:
            stream.seek(0)
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise RecorderControlError(
                        "Recorder control is busy; no state change was made."
                    ) from exc
                time.sleep(0.05)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


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


def _capture_pause_proven(
    control: Mapping[str, Any], runtime: Mapping[str, Any]
) -> bool:
    """Require a worker acknowledgement for this exact control operation."""

    operation_id = control.get("operation_id")
    capture = runtime.get("capture_control")
    if (
        control.get("enabled") is not False
        or not isinstance(operation_id, str)
        or not operation_id.strip()
        or not isinstance(capture, Mapping)
    ):
        return False
    return bool(
        capture.get("operation_id") == operation_id
        and capture.get("requested_enabled") is False
        and capture.get("capture_scheduling") is False
        and type(capture.get("inflight_capture_tasks")) is int
        and capture.get("inflight_capture_tasks") == 0
        and capture.get("acknowledged_operation_id") == operation_id
        and _parse_utc(capture.get("acknowledged_at")) is not None
        and capture.get("durable_boundary") is True
    )


def _configured_sources(
    config: CapabilityConfig | None,
) -> dict[str, str]:
    if config is None:
        return {}
    try:
        return parse_recorder_sources(config.recorder_sources)
    except (CapabilityConfigError, TypeError, ValueError):
        return {}


class RecorderControlService:
    """Control the independent recorder service through durable shared files.

    Flask never owns the recorder process. The browser writes the desired state to
    ``CONTROL_PATH``. The separate Docker recorder service watches that file,
    records while enabled, and publishes a heartbeat and diagnostics to
    ``STATUS_PATH``. This survives Flask restarts and avoids duplicate child
    processes when multiple web workers are used.

    This service validates recorder configuration only. Whether a caller has
    authority to start recording is decided by capability/contribution state
    before calling ``set_enabled``.
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
    def ready(config: CapabilityConfig | None) -> bool:
        return bool(_configured_sources(config))

    def set_enabled(
        self,
        enabled: bool,
        config: CapabilityConfig,
        *,
        operation_id: str | None = None,
        expected_current_operation_id: str | None = None,
        expected_pause_operation_id: str | None = None,
        resume_guard: Mapping[str, Any] | None = None,
        resume_guard_token: str | None = None,
        runtime_binding_sha256: str | None = None,
    ) -> tuple[bool, str]:
        if enabled and not self.ready(config):
            raise RecorderControlError(
                "Add at least one MTConnect source before starting recording."
            )

        if expected_pause_operation_id is not None:
            if not enabled or not _OPERATION_ID_PATTERN.fullmatch(
                expected_pause_operation_id
            ):
                raise RecorderControlError("Invalid expected pause operation identity.")
            if not isinstance(resume_guard_token, str) or not resume_guard_token:
                raise RecorderControlError("Automatic resume authorization is missing.")
            if not isinstance(runtime_binding_sha256, str) or not _SHA256_PATTERN.fullmatch(
                runtime_binding_sha256
            ):
                raise RecorderControlError("Runtime binding identity is invalid.")

        if expected_current_operation_id is not None and (
            enabled
            or not _OPERATION_ID_PATTERN.fullmatch(expected_current_operation_id)
        ):
            raise RecorderControlError("Invalid current control operation identity.")

        new_operation_id = operation_id or uuid4().hex
        if not _OPERATION_ID_PATTERN.fullmatch(new_operation_id):
            raise RecorderControlError("Invalid recorder control operation identity.")
        if resume_guard is not None:
            if enabled or not isinstance(resume_guard, Mapping):
                raise RecorderControlError("Resume guard may only accompany a stop request.")
            deadline = _parse_utc(resume_guard.get("deadline_utc"))
            now = datetime.now(timezone.utc)
            if (
                resume_guard.get("operation_id") != new_operation_id
                or resume_guard.get("prior_control_operation_id")
                != expected_current_operation_id
                or expected_current_operation_id is None
                or not isinstance(resume_guard.get("token_sha256"), str)
                or not _SHA256_PATTERN.fullmatch(resume_guard["token_sha256"])
                or not isinstance(resume_guard.get("runtime_binding_sha256"), str)
                or not _SHA256_PATTERN.fullmatch(
                    resume_guard["runtime_binding_sha256"]
                )
                or deadline is None
                or deadline <= now
                or (deadline - now).total_seconds() > PAUSE_GUARD_MAX_SECONDS
            ):
                raise RecorderControlError("Resume guard binding or deadline is invalid.")

        payload = {
            "schema": "fcp.mtconnect_recorder.control.v1",
            "enabled": bool(enabled),
            "operation_id": new_operation_id,
            "updated_at": _utc_now(),
            "requested_by": (
                "automatic-resume-guard"
                if expected_pause_operation_id is not None
                else "web"
            ),
        }
        if resume_guard is not None:
            payload["resume_guard"] = dict(resume_guard)

        with _control_file_lock(self.control_path):
            current = _read_json(self.control_path)
            if current.get("operation_id") == new_operation_id:
                raise RecorderControlError(
                    "Recorder control operation identities must be unique."
                )
            if expected_current_operation_id is not None and (
                current.get("enabled") is not True
                or current.get("operation_id") != expected_current_operation_id
            ):
                raise RecorderControlError(
                    "Control ownership changed; refusing stale pause request."
                )
            if expected_pause_operation_id is not None:
                guard = current.get("resume_guard")
                token_digest = hashlib.sha256(
                    str(resume_guard_token).encode("utf-8")
                ).hexdigest()
                if (
                    current.get("enabled") is not False
                    or current.get("operation_id") != expected_pause_operation_id
                    or not isinstance(guard, Mapping)
                    or guard.get("operation_id") != expected_pause_operation_id
                    or guard.get("runtime_binding_sha256") != runtime_binding_sha256
                    or not isinstance(guard.get("token_sha256"), str)
                    or not hmac.compare_digest(guard["token_sha256"], token_digest)
                ):
                    raise RecorderControlError(
                        "Pause ownership changed; refusing stale automatic resume."
                    )
            _write_json_atomic(self.control_path, payload)
        if enabled:
            return True, (
                "Recording requested. The recorder service will start polling "
                "within a few seconds."
            )
        return True, (
            "Recording stop requested. The recorder service will stop scheduling "
            "capture work, drain in-flight work, and remain on standby."
        )

    def validate_resume_guard(
        self,
        *,
        token: str,
        operation_id: str,
        runtime_binding_sha256: str,
    ) -> bool:
        """Validate a scoped local auto-resume credential without consuming it."""

        if (
            not _OPERATION_ID_PATTERN.fullmatch(operation_id)
            or not _SHA256_PATTERN.fullmatch(runtime_binding_sha256)
            or not token
        ):
            return False
        control = _read_json(self.control_path)
        guard = control.get("resume_guard")
        if (
            control.get("enabled") is not False
            or control.get("operation_id") != operation_id
            or not isinstance(guard, Mapping)
            or guard.get("operation_id") != operation_id
            or guard.get("runtime_binding_sha256") != runtime_binding_sha256
            or not isinstance(guard.get("token_sha256"), str)
        ):
            return False
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        return hmac.compare_digest(guard["token_sha256"], digest)

    def status(
        self,
        config: CapabilityConfig | None,
        *,
        include_diagnostics: bool = False,
    ) -> dict[str, Any]:
        control = _read_json(self.control_path)
        runtime = _read_json(self.status_path)
        requested_enabled = control.get("enabled") is True
        control_operation_id = _text(control.get("operation_id"))
        pause_proven = _capture_pause_proven(control, runtime)
        runtime_capture_control = runtime.get("capture_control")
        if not isinstance(runtime_capture_control, Mapping):
            runtime_capture_control = {}

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
            if pause_proven:
                state = "stopped"
                message = "Recorder acknowledged this pause after capture work drained."
            else:
                state = "draining"
                message = (
                    "Recording is disabled; the worker has not yet proven that "
                    "this pause is fully drained."
                )
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
            "capture_schedule_count": _safe_optional_int(
                runtime.get("capture_schedule_count")
            ),
            "last_capture_scheduled_at": (
                runtime.get("last_capture_scheduled_at")
                if isinstance(runtime.get("last_capture_scheduled_at"), str)
                else None
            ),
            # Preserve unknown: the Recorder can have an active capture/store
            # future without knowing its eventual observation count yet.
            "records_buffered": _safe_optional_int(runtime.get("records_buffered")),
            "last_flush_at": runtime.get("last_flush_at"),
            "last_commit_at": runtime.get("last_commit_at"),
            "control_operation_id": control_operation_id,
            "pause_proven": pause_proven,
            "capture_control": dict(runtime_capture_control),
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
        config: CapabilityConfig | None,
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
            "schema": "fcp.recorder.web_status.v1",
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
            "capture_schedule_count": status["capture_schedule_count"],
            "last_capture_scheduled_at": status["last_capture_scheduled_at"],
            "records_buffered": status["records_buffered"],
            "last_flush_at": status["last_flush_at"],
            "last_commit_at": status["last_commit_at"],
            "control_operation_id": status["control_operation_id"],
            "pause_proven": status["pause_proven"],
            "capture_control": status["capture_control"],
            "sources": sources,
        }


_RECORDER_CONTROL_SERVICE: RecorderControlService | None = None


def get_recorder_control_service() -> RecorderControlService:
    global _RECORDER_CONTROL_SERVICE
    if _RECORDER_CONTROL_SERVICE is None:
        _RECORDER_CONTROL_SERVICE = RecorderControlService()
    return _RECORDER_CONTROL_SERVICE

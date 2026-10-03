"""Host-managed, runtime-bound guard for a temporary Recorder capture pause.

The helper is armed only after a live identity read and holds the same named
host-mutation mutex used by the Windows build/update path. It never starts a
container or selects a deployment; recovery is a conditional request to the
existing Flask Start route.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GUARD_DIR = Path(__file__).resolve().parent
if str(GUARD_DIR) not in sys.path:
    sys.path.insert(0, str(GUARD_DIR))

from recorder_pause_resume_guard_logic import (
    BoundedPauseResumeGuard,
    PauseGuardAction,
    PauseGuardObservation,
)

_OPERATION_ID = re.compile(r"\A[a-f0-9]{32}\Z")
_SHA256 = re.compile(r"\A[a-f0-9]{64}\Z")
_COMMIT = re.compile(r"\A[a-f0-9]{40}\Z")
_RUNTIME_GENERATION = re.compile(r"\A[a-f0-9]{32}\Z")
_MAX_PAUSE_SECONDS = 20 * 60
_HEARTBEAT_MAX_AGE_SECONDS = 10.0
_POLL_SECONDS = 1.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except (OSError, ValueError):
        return False
    return True


def validate_spec(
    spec: dict[str, Any], *, now: datetime | None = None, allow_expired: bool = False
) -> None:
    runtime = spec.get("runtime_binding")
    if not isinstance(runtime, dict):
        raise TypeError("runtime_binding must be an object")
    for name in (
        "repo_root",
        "data_root",
        "config_path",
        "control_path",
        "status_path",
        "container_id",
        "image_id",
        "candidate_commit",
        "native_runtime",
        "runtime_generation",
        "mount_destination",
        "docker_exe",
    ):
        if name not in runtime:
            raise ValueError(f"runtime_binding is missing {name}")
    if not _OPERATION_ID.fullmatch(str(spec.get("prior_control_operation_id", ""))):
        raise ValueError("prior_control_operation_id is invalid")
    if not _COMMIT.fullmatch(str(runtime["candidate_commit"])):
        raise ValueError("candidate_commit must be a full Git SHA")
    native_runtime = runtime.get("native_runtime")
    if (
        not isinstance(native_runtime, dict)
        or native_runtime.get("schema") != "fcp.recorder-native-runtime.v1"
        or native_runtime.get("runtime_type") != "native-python"
        or type(native_runtime.get("pid")) is not int
        or native_runtime["pid"] <= 0
    ):
        raise ValueError("native_runtime must identify the Recorder process")
    if not _RUNTIME_GENERATION.fullmatch(str(runtime["runtime_generation"])):
        raise ValueError("runtime_generation must be a process-bound 32-character identity")
    if not re.fullmatch(r"[a-f0-9]{64}", str(runtime["container_id"]).casefold()):
        raise ValueError("container_id must be a full Docker container ID")
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", str(runtime["image_id"]).casefold()):
        raise ValueError("image_id must be a full Docker image ID")
    data_root = Path(str(runtime["data_root"]))
    for name in ("config_path", "control_path", "status_path"):
        if not _path_within(Path(str(runtime[name])), data_root):
            raise ValueError(f"{name} must remain under data_root")
    deadline = _parse_utc(spec.get("hard_deadline_utc"))
    now = now or _utc_now()
    if deadline is None:
        raise ValueError("hard_deadline_utc must be a UTC timestamp")
    if allow_expired:
        prepared = _parse_utc(spec.get("guard_prepared_at_utc"))
        if prepared is None or deadline <= prepared:
            raise ValueError("guard preparation time and hard deadline are inconsistent")
        if (deadline - prepared).total_seconds() > _MAX_PAUSE_SECONDS:
            raise ValueError("hard pause exceeds the 20-minute maximum")
    else:
        if deadline <= now:
            raise ValueError("hard_deadline_utc must be a future UTC timestamp")
        if (deadline - now).total_seconds() > _MAX_PAUSE_SECONDS:
            raise ValueError("hard pause exceeds the 20-minute maximum")
    for name in ("controller_heartbeat_path", "copy_outcome_path", "copy_processes_path"):
        if not isinstance(spec.get(name), str) or not spec[name]:
            raise ValueError(f"{name} is required")
    if not isinstance(spec.get("baseline_capture_schedule_count"), int) or isinstance(
        spec["baseline_capture_schedule_count"], bool
    ):
        raise TypeError("baseline_capture_schedule_count must be an integer")
    if not isinstance(spec.get("flask_url"), str) or not spec["flask_url"].startswith(
        "http://127.0.0.1:"
    ):
        raise ValueError("flask_url must use the host-local Recorder API")


def prepare_config(spec_path: Path, config_root: Path) -> dict[str, Any]:
    spec = _read_json(spec_path)
    if spec is None:
        raise ValueError("pause guard specification is unreadable")
    validate_spec(spec)
    runtime = spec["runtime_binding"]
    operation_id = uuid.uuid4().hex
    program_data = os.environ.get("ProgramData")
    if not program_data:
        raise RuntimeError("Windows ProgramData is unavailable")
    expected_config_root = Path(program_data) / "FCP" / "RecorderPauseGuards"
    if Path(config_root).resolve(strict=False) != expected_config_root.resolve(strict=False):
        raise ValueError("guard config root must be the protected ProgramData directory")
    private_dir = expected_config_root / operation_id
    config_path = private_dir / "guard.json"
    _protect_private_directory(private_dir)
    evidence_dir = private_dir / "evidence"
    prepared_at = _utc_now()
    token = secrets.token_urlsafe(32)
    binding_sha256 = _sha256(_canonical_bytes(runtime))
    script_path = Path(__file__).resolve()
    logic_path = GUARD_DIR / "recorder_pause_resume_guard_logic.py"
    register_path = GUARD_DIR / "register_recorder_pause_resume_guard.ps1"
    script_sha256 = _sha256(script_path.read_bytes())
    logic_sha256 = _sha256(logic_path.read_bytes())
    register_sha256 = _sha256(register_path.read_bytes())
    payload = {
        "schema": "fcp.recorder.pause-resume-guard.host.v1",
        **spec,
        "operation_id": operation_id,
        "guard_prepared_at_utc": _iso(prepared_at),
        "resume_token": token,
        "resume_token_sha256": _sha256(token.encode("utf-8")),
        "runtime_binding_sha256": binding_sha256,
        "guard_script": str(script_path),
        "guard_script_sha256": script_sha256,
        "guard_logic": str(logic_path.resolve()),
        "guard_logic_sha256": logic_sha256,
        "register_script": str(register_path.resolve()),
        "register_script_sha256": register_sha256,
        "task_name": f"FCP-Recorder-PauseResume-{operation_id}",
        "guard_state_path": str(evidence_dir / "guard-state.json"),
        "guard_events_path": str(evidence_dir / "guard-events.jsonl"),
    }
    _write_json_atomic(config_path, payload)
    _protect_config(config_path)
    return {
        "operation_id": operation_id,
        "resume_token_sha256": payload["resume_token_sha256"],
        "runtime_binding_sha256": binding_sha256,
        "hard_deadline_utc": payload["hard_deadline_utc"],
        "task_name": payload["task_name"],
        "config_path": str(config_path),
        "stop_form_fields": {
            "operation_id": operation_id,
            "expected_control_operation_id": spec["prior_control_operation_id"],
            "resume_guard_token_sha256": payload["resume_token_sha256"],
            "resume_guard_runtime_binding_sha256": binding_sha256,
            "resume_guard_deadline_utc": payload["hard_deadline_utc"],
        },
    }


def _protect_config(path: Path) -> None:
    if os.name != "nt":
        raise RuntimeError("host guard secrets may only be prepared on Windows")
    completed = subprocess.run(
        [
            "icacls.exe",
            str(path),
            "/inheritance:r",
            "/grant:r",
            "*S-1-5-18:F",
            "*S-1-5-32-544:F",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        path.unlink(missing_ok=True)
        raise RuntimeError("could not restrict the pause guard secret file ACL")


def _protect_private_directory(path: Path) -> None:
    if os.name != "nt":
        raise RuntimeError("host guard secrets may only be prepared on Windows")
    path.mkdir(parents=True, exist_ok=False)
    completed = subprocess.run(
        [
            "icacls.exe",
            str(path),
            "/inheritance:r",
            "/grant:r",
            "*S-1-5-18:(OI)(CI)F",
            "*S-1-5-32-544:(OI)(CI)F",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("could not protect the pause guard operation directory")


def _load_config(path: Path) -> dict[str, Any]:
    config = _read_json(path)
    if config is None or config.get("schema") != "fcp.recorder.pause-resume-guard.host.v1":
        raise ValueError("pause guard config is missing or has an unsupported schema")
    program_data = os.environ.get("ProgramData")
    if not program_data:
        raise RuntimeError("Windows ProgramData is unavailable")
    expected_config = (
        Path(program_data)
        / "FCP"
        / "RecorderPauseGuards"
        / str(config.get("operation_id"))
        / "guard.json"
    )
    if path.resolve(strict=False) != expected_config.resolve(strict=False):
        raise ValueError("pause guard config is outside its protected operation directory")
    for path_key, hash_key, description in (
        ("guard_script", "guard_script_sha256", "supervisor script"),
        ("guard_logic", "guard_logic_sha256", "guard decision logic"),
        ("register_script", "register_script_sha256", "task registration script"),
    ):
        if _sha256(Path(str(config[path_key])).read_bytes()) != config.get(hash_key):
            raise ValueError(f"pause guard {description} hash changed after preparation")
    token = config.get("resume_token")
    if not isinstance(token, str) or _sha256(token.encode()) != config.get(
        "resume_token_sha256"
    ):
        raise ValueError("pause guard secret file failed its integrity check")
    validate_spec(config, allow_expired=True)
    runtime = config["runtime_binding"]
    if _sha256(_canonical_bytes(runtime)) != config.get("runtime_binding_sha256"):
        raise ValueError("runtime binding hash does not match the protected config")
    return config


def _control_has_owned_pause(control: dict[str, Any] | None, config: dict[str, Any]) -> bool:
    """Return true only for this operation's exact, still-owned paused state."""

    if (
        control is None
        or control.get("enabled") is not False
        or control.get("operation_id") != config.get("operation_id")
    ):
        return False
    guard = control.get("resume_guard")
    return bool(
        isinstance(guard, dict)
        and guard.get("operation_id") == config.get("operation_id")
        and guard.get("prior_control_operation_id")
        == config.get("prior_control_operation_id")
        and guard.get("runtime_binding_sha256")
        == config.get("runtime_binding_sha256")
        and guard.get("token_sha256") == config.get("resume_token_sha256")
        and guard.get("deadline_utc") == config.get("hard_deadline_utc")
    )


def _docker_observation(runtime: dict[str, Any], timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        completed = subprocess.run(
            [str(runtime["docker_exe"]), "inspect", str(runtime["container_id"])],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    try:
        values = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], dict):
        return None
    return values[0]


def _status_process_binding_matches(
    status: dict[str, Any], runtime: dict[str, Any]
) -> bool | None:
    native = status.get("native_runtime")
    observability = status.get("acceptance_observability")
    provenance = observability.get("provenance") if isinstance(observability, dict) else None
    if not isinstance(native, dict) or not isinstance(provenance, dict):
        return None
    pid = provenance.get("pid")
    generation = provenance.get("runtime_generation")
    candidate = provenance.get("candidate_sha")
    if (
        type(pid) is not int
        or not isinstance(generation, str)
        or not isinstance(candidate, str)
    ):
        return None
    return bool(
        native == runtime.get("native_runtime")
        and pid == native.get("pid")
        and generation == runtime.get("runtime_generation")
        and candidate == runtime.get("candidate_commit")
    )


def _runtime_binding_match(config: dict[str, Any]) -> tuple[bool | None, dict[str, Any] | None]:
    runtime = config["runtime_binding"]
    actual_container = _docker_observation(runtime)
    if actual_container is None:
        return None, None
    if actual_container.get("State", {}).get("Running") is not True:
        return False, None
    labels = actual_container.get("Config", {}).get("Labels") or {}
    if (
        str(actual_container.get("Id", "")).casefold()
        != str(runtime["container_id"]).casefold()
        or actual_container.get("Image") != runtime["image_id"]
        or labels.get("no.fcp.build_commit") != runtime["candidate_commit"]
    ):
        return False, None
    expected_mount = (
        os.path.normcase(os.path.abspath(str(runtime["data_root"]))),
        str(runtime["mount_destination"]).replace("\\", "/").rstrip("/"),
    )
    mounts = actual_container.get("Mounts")
    if not isinstance(mounts, list) or not any(
        (
            os.path.normcase(os.path.abspath(str(item.get("Source", "")))),
            str(item.get("Destination", "")).replace("\\", "/").rstrip("/"),
        )
        == expected_mount
        for item in mounts
        if isinstance(item, dict)
    ):
        return False, None
    try:
        current_config_sha256 = _sha256(Path(str(runtime["config_path"])).read_bytes())
    except OSError:
        return None, None
    if current_config_sha256 != runtime.get("config_sha256"):
        return False, None
    status = _read_json(Path(str(runtime["status_path"])))
    heartbeat = _parse_utc(status.get("heartbeat_at") if status else None)
    if status is None or heartbeat is None:
        return None, status
    age = max(0.0, (_utc_now() - heartbeat).total_seconds())
    if age > _HEARTBEAT_MAX_AGE_SECONDS:
        return None, status
    if status.get("native_runtime") != runtime.get("native_runtime"):
        return False, status
    process_match = _status_process_binding_matches(status, runtime)
    if process_match is not True:
        return process_match, status
    return True, status


def _controller_age(config: dict[str, Any]) -> float:
    payload = _read_json(Path(str(config["controller_heartbeat_path"])))
    if payload is None or payload.get("operation_id") != config["operation_id"]:
        return float("inf")
    observed = _parse_utc(payload.get("observed_at_utc"))
    if observed is None:
        return float("inf")
    return max(0.0, (_utc_now() - observed).total_seconds())


def _copy_outcome(config: dict[str, Any]) -> str | None:
    payload = _read_json(Path(str(config["copy_outcome_path"])))
    if payload is None or payload.get("operation_id") != config["operation_id"]:
        return None
    outcome = payload.get("outcome")
    return outcome if outcome in {"complete", "failed"} else None


def _observation(config: dict[str, Any]) -> tuple[PauseGuardObservation, dict[str, Any] | None]:
    matches, status = _runtime_binding_match(config)
    control = _read_json(Path(str(config["runtime_binding"]["control_path"])))
    capture = status.get("capture_control") if isinstance(status, dict) else None
    if not isinstance(capture, dict):
        capture = {}
    observation = PauseGuardObservation(
        runtime_binding_matches=matches,
        control_operation_id=(
            str(control.get("operation_id")) if control and isinstance(control.get("operation_id"), str) else None
        ),
        control_enabled=(
            control.get("enabled") if control and type(control.get("enabled")) is bool else None
        ),
        pause_acknowledged_operation_id=(
            capture.get("acknowledged_operation_id")
            if isinstance(capture.get("acknowledged_operation_id"), str)
            else None
        ),
        capture_scheduling=(
            capture.get("capture_scheduling")
            if type(capture.get("capture_scheduling")) is bool
            else None
        ),
        inflight_capture_tasks=(
            capture.get("inflight_capture_tasks")
            if type(capture.get("inflight_capture_tasks")) is int
            else None
        ),
        durable_boundary=(
            capture.get("durable_boundary")
            if type(capture.get("durable_boundary")) is bool
            else None
        ),
        controller_heartbeat_age_seconds=_controller_age(config),
        copy_outcome=_copy_outcome(config),
        capture_schedule_count=(
            status.get("capture_schedule_count")
            if isinstance(status, dict)
            and type(status.get("capture_schedule_count")) is int
            else None
        ),
    )
    return observation, control


def _host_mutex(config: dict[str, Any], timeout_seconds: float = 10.0):
    if os.name != "nt":
        raise RuntimeError("the host mutation lease is only implemented for Windows")
    from ctypes import wintypes

    repo_root = os.path.normpath(os.path.abspath(config["runtime_binding"]["repo_root"]))
    path_hash = hashlib.sha256(repo_root.lower().encode("utf-8")).hexdigest()[:24].upper()
    name = f"Global\\FCPHostMutation-{path_hash}"
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = [wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR]
    kernel32.CreateMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.ReleaseMutex.argtypes = [wintypes.HANDLE]
    kernel32.ReleaseMutex.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.CreateMutexW(None, False, name)
    if not handle:
        raise OSError(ctypes.get_last_error(), "CreateMutexW failed")
    wait_result = kernel32.WaitForSingleObject(handle, int(timeout_seconds * 1000))
    if wait_result not in (0x00000000, 0x00000080):
        kernel32.CloseHandle(handle)
        raise RuntimeError("FCPHostMutation lease is busy")
    return kernel32, handle


def _release_mutex(kernel32, handle) -> None:
    try:
        kernel32.ReleaseMutex(handle)
    finally:
        kernel32.CloseHandle(handle)


def _event(config: dict[str, Any], state: str, **values: Any) -> None:
    event = {
        "schema": "fcp.recorder.pause-guard.event.v1",
        "operation_id": config["operation_id"],
        "observed_at_utc": _iso(_utc_now()),
        "state": state,
        **values,
    }
    _write_json_atomic(Path(str(config["guard_state_path"])), event)
    events_path = Path(str(config["guard_events_path"]))
    events_path.parent.mkdir(parents=True, exist_ok=True)
    with events_path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(event, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _request_resume(
    config: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    token = str(config["resume_token"])
    binding_sha256 = str(config["runtime_binding_sha256"])
    request = urllib.request.Request(
        str(config["flask_url"]).rstrip("/") + "/server-setup/recording/start",
        data=b"",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-FCP-Recorder-Resume-Token": token,
            "X-FCP-Recorder-Resume-Operation": str(config["operation_id"]),
            "X-FCP-Recorder-Runtime-Binding-SHA256": binding_sha256,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            status_code = response.status
            payload = json.loads(response.read(32_768).decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return None, {"http_status": exc.code, "failure": "http-error"}
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return None, {"failure": "transport-error", "error_type": type(exc).__name__}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, {"http_status": status_code, "failure": "invalid-json", "error_type": type(exc).__name__}
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return None, {"http_status": status_code, "failure": "route-refused"}
    operation_id = payload.get("operation_id")
    if not isinstance(operation_id, str) or not _OPERATION_ID.fullmatch(operation_id):
        return None, {"http_status": status_code, "failure": "invalid-operation-identity"}
    return operation_id, {"http_status": status_code, "result": "accepted"}


def _cancel_final_sync(config: dict[str, Any]) -> list[dict[str, Any]]:
    control_path = str(config["runtime_binding"]["control_path"])
    if not _control_has_owned_pause(_read_json(Path(control_path)), config):
        return [{"result": "control-owner-changed"}]
    payload = _read_json(Path(str(config["copy_processes_path"])))
    if payload is None or payload.get("operation_id") != config["operation_id"]:
        return [{"result": "no-verified-final-sync-process-record"}]
    processes = payload.get("processes")
    if not isinstance(processes, list):
        return [{"result": "invalid-final-sync-process-record"}]
    results: list[dict[str, Any]] = []
    for item in processes:
        if not isinstance(item, dict):
            results.append({"result": "invalid-final-sync-process-entry"})
            continue
        pid = item.get("pid")
        command_hash = item.get("command_line_sha256")
        creation_utc = item.get("creation_utc")
        if (
            type(pid) is not int
            or pid <= 0
            or not isinstance(command_hash, str)
            or not _SHA256.fullmatch(command_hash)
            or _parse_utc(creation_utc) is None
        ):
            results.append({"pid": pid if type(pid) is int else None, "result": "invalid-process-binding"})
            continue
        payload = json.dumps(
            {
                "pid": pid,
                "command_line_sha256": command_hash,
                "creation_utc": creation_utc,
                "operation_id": config["operation_id"],
                "control_path": control_path,
                "prior_control_operation_id": config["prior_control_operation_id"],
                "runtime_binding_sha256": config["runtime_binding_sha256"],
                "resume_token_sha256": config["resume_token_sha256"],
                "hard_deadline_utc": config["hard_deadline_utc"],
            },
            separators=(",", ":"),
        ).encode("utf-8")
        encoded_payload = base64.b64encode(payload).decode("ascii")
        script = r'''
$ErrorActionPreference='Stop'
$payload=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('__PAYLOAD_BASE64__')) | ConvertFrom-Json
$pidValue=[int]$payload.pid
$expectedHash=[string]$payload.command_line_sha256
$expectedCreated=[string]$payload.creation_utc
$operation=[string]$payload.operation_id
$controlPath=[string]$payload.control_path
$priorOperation=[string]$payload.prior_control_operation_id
$runtimeBinding=[string]$payload.runtime_binding_sha256
$tokenHash=[string]$payload.resume_token_sha256
$deadline=[string]$payload.hard_deadline_utc
function Test-OwnedPause {
  try {
    # Match RecorderControlService's byte-range lock while checking and terminating.
    $script:controlLockHandle=[IO.File]::Open(($controlPath+'.lock'),[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::ReadWrite)
    if($script:controlLockHandle.Length -eq 0){ $script:controlLockHandle.WriteByte(0); $script:controlLockHandle.Flush() }
    $locked=$false
    for($attempt=0;$attempt -lt 100;$attempt++){
      try { $script:controlLockHandle.Lock(0,1); $locked=$true; break }
      catch { Start-Sleep -Milliseconds 50 }
    }
    if(-not $locked){ Release-ControlOwnership; return $false }
    $script:controlHandle=[IO.File]::Open($controlPath,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)
    $reader=[IO.StreamReader]::new($script:controlHandle,[Text.Encoding]::UTF8,$true,1024,$true)
    try { $control=ConvertFrom-Json $reader.ReadToEnd() } finally { $reader.Dispose() }
    $guard=$control.resume_guard
    $matches=($control.enabled -eq $false -and
      [string]$control.operation_id -ceq $operation -and
      [string]$guard.operation_id -ceq $operation -and
      [string]$guard.prior_control_operation_id -ceq $priorOperation -and
      [string]$guard.runtime_binding_sha256 -ceq $runtimeBinding -and
      [string]$guard.token_sha256 -ceq $tokenHash -and
      [string]$guard.deadline_utc -ceq $deadline)
    if($matches){ return $true }
    Release-ControlOwnership
    return $false
  } catch {
    Release-ControlOwnership
    return $false
  }
}
function Release-ControlOwnership {
  if($script:controlHandle){ $script:controlHandle.Dispose(); $script:controlHandle=$null }
  if($script:controlLockHandle){
    try { $script:controlLockHandle.Unlock(0,1) } catch {}
    $script:controlLockHandle.Dispose(); $script:controlLockHandle=$null
  }
}
if(-not (Test-OwnedPause)){ Write-Output 'control-owner-changed'; exit 5 }
# Look up and validate the PID only after acquiring the same lock Recorder uses
# for control changes. This closes the PID-reuse window while lock acquisition waits.
$item=Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
if($null -eq $item){ Write-Output 'absent'; exit 0 }
$commandLine=[string]$item.CommandLine
if([string]::IsNullOrWhiteSpace($commandLine) -or -not $commandLine.Contains($operation)){ Write-Output 'tag-mismatch'; exit 2 }
$bytes=[Text.Encoding]::UTF8.GetBytes($commandLine)
$hasher=[Security.Cryptography.SHA256]::Create()
try { $sha=[BitConverter]::ToString($hasher.ComputeHash($bytes)).Replace('-','').ToLowerInvariant() }
finally { $hasher.Dispose() }
if($sha -ne $expectedHash){ Write-Output 'command-hash-mismatch'; exit 3 }
$created=$item.CreationDate.ToUniversalTime().ToString('o')
if([Math]::Abs(([DateTime]::Parse($created)-[DateTime]::Parse($expectedCreated)).TotalSeconds) -gt 2){ Write-Output 'creation-time-mismatch'; exit 4 }
try {
  # Keep helper termination bounded separately from lock acquisition. If the
  # copy still exists afterward, the caller records an unconfirmed cancellation
  # and must not request Recorder resume until a later check proves it is gone.
  $killer=Start-Process -FilePath (Join-Path $env:SystemRoot 'System32\taskkill.exe') -ArgumentList @('/PID',[string]$pidValue,'/T','/F') -PassThru -NoNewWindow
  if(-not $killer.WaitForExit(10000)){
    try { $killer.Kill() } catch {}
    if(-not $killer.WaitForExit(2000)){ Write-Output 'taskkill-helper-did-not-stop'; exit 7 }
    Write-Output 'taskkill-timeout'; exit 6
  }
  $killer.Refresh()
  $remaining=Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
  if($null -ne $remaining){
    $remainingCommand=[string]$remaining.CommandLine
    $remainingBytes=[Text.Encoding]::UTF8.GetBytes($remainingCommand)
    $remainingHasher=[Security.Cryptography.SHA256]::Create()
    try { $remainingHash=([BitConverter]::ToString($remainingHasher.ComputeHash($remainingBytes))).Replace('-','').ToLowerInvariant() }
    finally { $remainingHasher.Dispose() }
    $remainingCreated=$remaining.CreationDate.ToUniversalTime().ToString('o')
    if($remainingHash -eq $expectedHash -and [Math]::Abs(([DateTime]::Parse($remainingCreated)-[DateTime]::Parse($expectedCreated)).TotalSeconds) -le 2){
      Write-Output 'bound-process-still-running'; exit 8
    }
  }
  Write-Output 'terminated-bound-final-sync-process-tree'
} finally { Release-ControlOwnership }
'''
        script = script.replace("__PAYLOAD_BASE64__", encoded_payload)
        encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
        try:
            completed = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-EncodedCommand",
                    encoded,
                ],
                capture_output=True,
                text=True,
                # The PowerShell side has a 5s lock budget and a separate 10s
                # termination budget (plus bounded helper cleanup).
                timeout=25,
                check=False,
            )
            result = completed.stdout.strip().splitlines()[-1] if completed.stdout.strip() else "no-result"
            results.append({"pid": pid, "result": result, "exit_code": completed.returncode})
        except (OSError, subprocess.TimeoutExpired):
            results.append({"pid": pid, "result": "process-check-failed"})
    return results


def _final_sync_cancellation_confirmed(results: list[dict[str, Any]]) -> bool:
    """Return true only when every operation-bound copy is proven absent."""
    return all(
        result.get("result") == "absent"
        or (
            result.get("result") == "terminated-bound-final-sync-process-tree"
            and result.get("exit_code") == 0
        )
        for result in results
    )


def watch(config_path: Path) -> int:
    config = _load_config(config_path)
    kernel32, mutex = _host_mutex(config)
    try:
        observation, control = _observation(config)
        if observation.runtime_binding_matches is not True:
            owned_pause = _control_has_owned_pause(control, config)
            canceled = _cancel_final_sync(config) if owned_pause else None
            _event(
                config,
                "identity-unverified-before-pause",
                binding=observation.runtime_binding_matches,
                capture_was_paused=owned_pause,
                canceled_final_sync=canceled,
            )
            return 2
        fresh_owner = bool(
            control is not None
            and control.get("enabled") is True
            and control.get("operation_id") == config["prior_control_operation_id"]
            and control.get("resume_guard") is None
        )
        recovering_owned_pause = _control_has_owned_pause(control, config)
        if not fresh_owner and not recovering_owned_pause:
            _event(config, "control-owner-changed-before-pause")
            return 2
        _event(
            config,
            "armed" if fresh_owner else "guard-restarted-for-owned-pause",
            mutation_lease="Global FCPHostMutation held",
        )
        deadline = _parse_utc(config["hard_deadline_utc"])
        if deadline is None:
            _event(config, "invalid-hard-deadline")
            return 2
        machine = BoundedPauseResumeGuard(
            operation_id=str(config["operation_id"]),
            prior_control_operation_id=str(config["prior_control_operation_id"]),
            hard_deadline=deadline,
            drain_timeout_seconds=float(config.get("drain_timeout_seconds", 30)),
            controller_stale_after_seconds=float(
                config.get("controller_stale_after_seconds", 10)
            ),
            baseline_capture_schedule_count=int(config["baseline_capture_schedule_count"]),
        )
        prior_state = _read_json(Path(str(config["guard_state_path"]))) or {}
        resume_operation_id = prior_state.get("resume_operation_id")
        resume_requested_at = _parse_utc(prior_state.get("resume_requested_at_utc"))
        if isinstance(resume_operation_id, str) and _OPERATION_ID.fullmatch(
            resume_operation_id
        ):
            machine.note_resume_requested(resume_operation_id)
        # A restarted controller must revalidate cancellation from the current
        # process binding. Older guard state may have recorded this flag before
        # cancellation was positively confirmed.
        recovery_triggered = False
        next_resume_attempt_at = 0.0
        while True:
            now = _utc_now()
            observation, control = _observation(config)
            if (
                control is not None
                and control.get("operation_id") == config["operation_id"]
                and control.get("enabled") is False
                and not _control_has_owned_pause(control, config)
            ):
                _event(config, "resume-guard-control-binding-mismatch")
                return 4
            action = machine.decide(observation, now=now)
            if action is PauseGuardAction.IDENTITY_UNVERIFIED:
                owned_pause = _control_has_owned_pause(control, config)
                canceled = _cancel_final_sync(config) if owned_pause else None
                _event(
                    config,
                    action.value,
                    binding=observation.runtime_binding_matches,
                    capture_was_paused=owned_pause,
                    canceled_final_sync=canceled,
                )
                return 3
            if action is PauseGuardAction.WAIT_FOR_PAUSE:
                _event(config, action.value)
            elif action is PauseGuardAction.WAIT_FOR_DRAIN:
                _event(
                    config,
                    action.value,
                    inflight_capture_tasks=observation.inflight_capture_tasks,
                )
            elif action is PauseGuardAction.COPY_MAY_CONTINUE:
                _event(config, action.value)
            elif action is PauseGuardAction.REQUEST_RESUME:
                if not recovery_triggered:
                    canceled = _cancel_final_sync(config)
                    if _final_sync_cancellation_confirmed(canceled):
                        recovery_triggered = True
                        _event(
                            config,
                            "resume-requested",
                            canceled_final_sync=canceled,
                            recovery_triggered=True,
                        )
                    else:
                        _event(
                            config,
                            "final-sync-cancellation-unconfirmed",
                            canceled_final_sync=canceled,
                            recovery_triggered=False,
                        )
                if recovery_triggered and time.monotonic() >= next_resume_attempt_at:
                    resume_id, resume_result = _request_resume(config)
                    if resume_id:
                        machine.note_resume_requested(resume_id)
                        resume_requested_at = now
                        _event(
                            config,
                            "resume-route-accepted",
                            resume_operation_id=resume_id,
                            resume_requested_at_utc=_iso(now),
                            recovery_triggered=True,
                            **resume_result,
                        )
                    else:
                        next_resume_attempt_at = time.monotonic() + 5.0
                        _event(
                            config,
                            "resume-route-not-confirmed",
                            recovery_triggered=True,
                            **resume_result,
                        )
            elif action is PauseGuardAction.VERIFY_RESUME:
                _event(
                    config,
                    action.value,
                    capture_schedule_count=observation.capture_schedule_count,
                )
                if (
                    resume_requested_at is not None
                    and (now - resume_requested_at).total_seconds() > 90
                ):
                    _event(config, "resume-activity-verification-timeout")
                    return 4
            elif action is PauseGuardAction.RESUMED:
                _event(
                    config,
                    "resume-verified",
                    capture_schedule_count=observation.capture_schedule_count,
                    capture_control_operation_id=observation.control_operation_id,
                    recovery_triggered=recovery_triggered,
                )
                return 0
            elif action is PauseGuardAction.SUPERSEDED:
                _event(config, action.value, control_operation_id=observation.control_operation_id)
                return 0
            elif action is PauseGuardAction.PAUSE_NEVER_STARTED:
                _event(config, action.value)
                return 0
            time.sleep(_POLL_SECONDS)
    finally:
        _release_mutex(kernel32, mutex)


def arm(config_path: Path, register_script: Path) -> dict[str, Any]:
    config = _load_config(config_path)
    if register_script.resolve() != Path(str(config["register_script"])).resolve():
        raise RuntimeError("task registration script differs from the prepared binding")
    deadline = _parse_utc(config.get("hard_deadline_utc"))
    if deadline is None or (deadline - _utc_now()).total_seconds() < 30:
        raise RuntimeError("hard pause deadline is too close to arm this guard")
    observation, control = _observation(config)
    if observation.runtime_binding_matches is not True:
        raise RuntimeError("live runtime identity could not be verified")
    if (
        control is None
        or control.get("enabled") is not True
        or control.get("operation_id") != config["prior_control_operation_id"]
        or control.get("resume_guard") is not None
    ):
        raise RuntimeError("capture control changed before guard activation")
    registration = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(register_script),
            "-OperationId",
            str(config["operation_id"]),
            "-PythonExe",
            sys.executable,
            "-GuardScript",
            str(Path(__file__).resolve()),
            "-ConfigPath",
            str(config_path.resolve()),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if registration.returncode != 0:
        raise RuntimeError("Task Scheduler rejected the guard registration")
    state_path = Path(str(config["guard_state_path"]))
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        state = _read_json(state_path)
        if state and state.get("operation_id") == config["operation_id"]:
            if state.get("state") in {
                "armed",
                PauseGuardAction.WAIT_FOR_PAUSE.value,
            }:
                query = subprocess.run(
                    [
                        "powershell.exe",
                        "-NoProfile",
                        "-NonInteractive",
                        "-Command",
                        f"(Get-ScheduledTask -TaskName '{config['task_name']}').State",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=8,
                    check=False,
                )
                if query.returncode != 0 or query.stdout.strip() != "Running":
                    raise RuntimeError("Task Scheduler task is not actively running")
                return {
                    "armed": True,
                    "operation_id": config["operation_id"],
                    "task_name": config["task_name"],
                    "state_path": str(state_path),
                    "hard_deadline_utc": config["hard_deadline_utc"],
                }
            if state.get("state") not in {
                "resume-route-not-confirmed",
                "resume-route-accepted",
            }:
                raise RuntimeError("guard task did not reach an active state")
        time.sleep(0.5)
    raise RuntimeError("Task Scheduler did not start the guard within 20 seconds")


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--spec", type=Path, required=True)
    prepare_parser.add_argument("--config-root", type=Path, required=True)
    arm_parser = commands.add_parser("arm")
    arm_parser.add_argument("--config", type=Path, required=True)
    arm_parser.add_argument(
        "--register-script",
        type=Path,
        default=GUARD_DIR / "register_recorder_pause_resume_guard.ps1",
    )
    watch_parser = commands.add_parser("watch")
    watch_parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            print(json.dumps(prepare_config(args.spec, args.config_root), sort_keys=True))
            return 0
        if args.command == "arm":
            print(json.dumps(arm(args.config, args.register_script), sort_keys=True))
            return 0
        if args.command == "watch":
            return watch(args.config)
    except Exception as exc:  # noqa: BLE001 - outer task records only redacted errors
        if args.command == "watch":
            try:
                config = _load_config(args.config)
                _event(config, "guard-failed", error_type=type(exc).__name__)
            except (OSError, ValueError, RuntimeError, KeyError, TypeError) as record_exc:
                print(
                    "recorder-pause-guard failure record unavailable: "
                    f"{type(record_exc).__name__}",
                    file=sys.stderr,
                )
        print(f"recorder-pause-guard failed: {type(exc).__name__}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

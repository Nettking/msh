"""Host-managed, runtime-bound guard for a temporary Recorder capture pause.

The helper is armed only after a live identity read and holds the same named
host-mutation mutex used by the Windows build/update path. It never starts a
container or selects a deployment; recovery is a conditional request to the
existing Flask Start route.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import csv
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

from recorder_pause_job import JobObjectError, OperationJob
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


def _current_windows_sid() -> str:
    if os.name != "nt":
        raise RuntimeError("Windows identity is unavailable")
    result = subprocess.run(
        ["whoami.exe", "/user", "/fo", "csv", "/nh"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("could not determine the copy-controller Windows identity")
    try:
        fields = next(csv.reader([result.stdout.strip()]))
    except (StopIteration, csv.Error) as exc:
        raise RuntimeError("copy-controller Windows identity output is invalid") from exc
    sid = fields[-1].strip() if fields else ""
    if not re.fullmatch(r"S-1-(?:[0-9]+-){1,14}[0-9]+", sid):
        raise RuntimeError("copy-controller SID is invalid")
    return sid


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


@contextlib.contextmanager
def _control_file_lock(control_path: Path, timeout_seconds: float = 5.0):
    """Share Recorder's byte-range lock while starting or canceling a copy."""

    if os.name != "nt":
        raise RuntimeError("Recorder control locking is only implemented for Windows")
    import msvcrt

    lock_path = control_path.with_name(control_path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        deadline = time.monotonic() + timeout_seconds
        acquired = False
        while time.monotonic() < deadline:
            try:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                acquired = True
                break
            except OSError:
                time.sleep(0.05)
        if not acquired:
            raise TimeoutError("Recorder control ownership lock is busy")
        try:
            yield
        finally:
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)


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
        "config_sha256",
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
    if not _SHA256.fullmatch(str(runtime["config_sha256"])):
        raise ValueError("config_sha256 must identify the bound Recorder config bytes")
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
    if not _SHA256.fullmatch(str(spec.get("final_sync_command_sha256", ""))):
        raise ValueError("final_sync_command_sha256 must bind the approved argv")
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
    bundle_dir = private_dir / "code"
    bundle_dir.mkdir()
    sources = {
        "guard_script": Path(__file__).resolve(),
        "guard_logic": GUARD_DIR / "recorder_pause_resume_guard_logic.py",
        "job_script": GUARD_DIR / "recorder_pause_job.py",
        "register_script": GUARD_DIR / "register_recorder_pause_resume_guard.ps1",
    }
    bundled: dict[str, tuple[str, str]] = {}
    for key, source in sources.items():
        contents = source.read_bytes()
        destination = bundle_dir / source.name
        with destination.open("xb") as output:
            output.write(contents)
            output.flush()
            os.fsync(output.fileno())
        if _sha256(destination.read_bytes()) != _sha256(contents):
            raise RuntimeError(f"protected guard bundle verification failed for {key}")
        bundled[key] = (str(destination), _sha256(contents))
    copy_controller_sid = _current_windows_sid()
    payload = {
        "schema": "fcp.recorder.pause-resume-guard.host.v1",
        **spec,
        "operation_id": operation_id,
        "guard_prepared_at_utc": _iso(prepared_at),
        "resume_token": token,
        "resume_token_sha256": _sha256(token.encode("utf-8")),
        "runtime_binding_sha256": binding_sha256,
        "copy_controller_sid": copy_controller_sid,
        "final_sync_process_supervision": "windows-job-object.v1",
        "guard_script": bundled["guard_script"][0],
        "guard_script_sha256": bundled["guard_script"][1],
        "guard_logic": bundled["guard_logic"][0],
        "guard_logic_sha256": bundled["guard_logic"][1],
        "job_script": bundled["job_script"][0],
        "job_script_sha256": bundled["job_script"][1],
        "register_script": bundled["register_script"][0],
        "register_script_sha256": bundled["register_script"][1],
        "controller_heartbeat_path": str(evidence_dir / "controller-heartbeat.json"),
        "copy_outcome_path": str(evidence_dir / "copy-outcome.json"),
        "copy_processes_path": str(evidence_dir / "copy-processes.json"),
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
        "evidence_dir": str(evidence_dir),
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
    protected_code = path.parent / "code"
    for path_key, hash_key, description in (
        ("guard_script", "guard_script_sha256", "supervisor script"),
        ("guard_logic", "guard_logic_sha256", "guard decision logic"),
        ("job_script", "job_script_sha256", "Job Object helper"),
        ("register_script", "register_script_sha256", "task registration script"),
    ):
        code_path = Path(str(config[path_key]))
        if not _path_within(code_path, protected_code):
            raise ValueError(f"protected guard {description} is outside its code bundle")
        if _sha256(code_path.read_bytes()) != config.get(hash_key):
            raise ValueError(f"pause guard {description} hash changed after preparation")
    token = config.get("resume_token")
    if not isinstance(token, str) or _sha256(token.encode()) != config.get(
        "resume_token_sha256"
    ):
        raise ValueError("pause guard secret file failed its integrity check")
    validate_spec(config, allow_expired=True)
    if not re.fullmatch(r"S-1-(?:[0-9]+-){1,14}[0-9]+", str(config.get("copy_controller_sid", ""))):
        raise ValueError("protected guard config has an invalid copy-controller SID")
    if config.get("final_sync_process_supervision") != "windows-job-object.v1":
        raise ValueError("protected guard config has no supported final-sync supervisor")
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
        or pid <= 0
        or not isinstance(generation, str)
        or not _RUNTIME_GENERATION.fullmatch(generation)
        or not isinstance(candidate, str)
    ):
        return None
    if (
        not isinstance(native, dict)
        or native.get("schema") != "fcp.recorder-native-runtime.v1"
        or native.get("runtime_type") != "native-python"
        or type(native.get("pid")) is not int
        or native.get("pid") != pid
        or candidate != runtime.get("candidate_commit")
    ):
        return False
    same_process = native == runtime.get("native_runtime")
    expected_generation = runtime.get("runtime_generation")
    if same_process and generation != expected_generation:
        return False
    if not same_process and generation == expected_generation:
        return False
    build_commit = native.get("build_commit")
    if build_commit is not None and str(build_commit).casefold() != str(
        runtime.get("candidate_commit")
    ).casefold():
        return False
    supervisor = native.get("supervisor_session")
    supervisor_generation = provenance.get("supervisor_generation")
    if supervisor is None:
        return supervisor_generation is None
    return bool(
        isinstance(supervisor, str)
        and _RUNTIME_GENERATION.fullmatch(supervisor)
        and supervisor_generation == supervisor
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


def _write_copy_heartbeat(config: dict[str, Any]) -> None:
    _write_json_atomic(
        Path(str(config["controller_heartbeat_path"])),
        {
            "schema": "fcp.recorder.pause-copy-controller-heartbeat.v1",
            "operation_id": config["operation_id"],
            "pid": os.getpid(),
            "observed_at_utc": _iso(_utc_now()),
        },
    )


def _write_copy_result(
    config: dict[str, Any], *, outcome: str, result: dict[str, Any]
) -> None:
    existing = _read_json(Path(str(config["copy_processes_path"]))) or {}
    process_payload = dict(existing)
    process_payload.update(
        {
        "schema": "fcp.recorder.pause-copy-processes.v1",
        "operation_id": config["operation_id"],
        "state": outcome,
        "outcome": outcome,
        "observed_at_utc": _iso(_utc_now()),
        **result,
        }
    )
    _write_json_atomic(Path(str(config["copy_processes_path"])), process_payload)
    payload = {
        "schema": "fcp.recorder.pause-copy-outcome.v1",
        "operation_id": config["operation_id"],
        "outcome": outcome,
        "observed_at_utc": process_payload["observed_at_utc"],
        **result,
    }
    _write_json_atomic(Path(str(config["copy_outcome_path"])), payload)


def run_copy(config_path: Path, command: list[str]) -> dict[str, Any]:
    """Run one bounded final-sync command inside the independent guard's job."""

    config = _load_config(config_path)
    if config.get("final_sync_process_supervision") != "windows-job-object.v1":
        raise RuntimeError("copy operation is not bound to Windows Job Object supervision")
    if _current_windows_sid() != config.get("copy_controller_sid"):
        raise RuntimeError("copy controller does not match the protected Windows SID")
    if not command or any(not isinstance(part, str) or "\0" in part for part in command):
        raise ValueError("final-sync command must be a nonempty argument list")
    command_hash = _sha256(_canonical_bytes(command))
    if command_hash != config.get("final_sync_command_sha256"):
        raise RuntimeError("final-sync argv differs from the protected operation binding")
    deadline = _parse_utc(config.get("hard_deadline_utc"))
    if deadline is None or (_utc_now() >= deadline):
        raise RuntimeError("final-sync hard deadline has elapsed")
    control_path = Path(str(config["runtime_binding"]["control_path"]))
    processes_path = Path(str(config["copy_processes_path"]))
    outcome_path = Path(str(config["copy_outcome_path"]))
    child = None
    job: OperationJob | None = None
    pid: int | None = None
    process_creation_utc: str | None = None
    try:
        _write_copy_heartbeat(config)
        with _control_file_lock(control_path):
            observation, control = _observation(config)
            if observation.runtime_binding_matches is not True:
                raise RuntimeError("runtime binding is not verified for final sync")
            if not _control_has_owned_pause(control, config):
                raise RuntimeError("this operation does not own the paused Recorder")
            if not (
                observation.pause_acknowledged_operation_id == config["operation_id"]
                and observation.pause_acknowledged_at is not None
                and observation.capture_scheduling is False
                and observation.inflight_capture_tasks == 0
                and observation.durable_boundary is True
            ):
                raise RuntimeError("Recorder has not acknowledged a durable drained pause")
            if _utc_now() >= deadline:
                raise RuntimeError("final-sync hard deadline elapsed before process launch")
            if processes_path.exists() or outcome_path.exists():
                raise RuntimeError("final-sync evidence already exists for this operation")
            job = OperationJob.open_existing(str(config["operation_id"]))
            if job.active_process_count() != 0:
                raise RuntimeError("operation already has an active final-sync process")
            child = job.create_suspended(command, cwd=str(config["runtime_binding"]["repo_root"]))
            pid = child.pid
            process_creation_utc = child.creation_time_utc()
            receipt = {
                "schema": "fcp.recorder.pause-copy-processes.v1",
                "operation_id": config["operation_id"],
                "job_object_name": job.name,
                "state": "starting",
                "command_line_sha256": command_hash,
                "processes": [
                    {"pid": pid, "creation_utc": process_creation_utc}
                ],
                "started_at_utc": _iso(_utc_now()),
            }
            _write_json_atomic(processes_path, receipt)
            child.resume()
            receipt["state"] = "running"
            receipt["resumed_at_utc"] = _iso(_utc_now())
            _write_json_atomic(processes_path, receipt)
            # The independent Scheduled Task is the durable owner. Closing this
            # handle means its unexpected exit kills the whole process tree.
            job.close()
            job = None

        root_exit_code: int | None = None
        while True:
            _write_copy_heartbeat(config)
            if child is not None and root_exit_code is None:
                root_exit_code = child.wait(0.5)
            try:
                with OperationJob.open_existing(str(config["operation_id"])) as probe_job:
                    active_count = probe_job.active_process_count()
            except JobObjectError:
                active_count = -1
            if root_exit_code is not None and root_exit_code != 0:
                outcome = "failed"
                result = {"return_code": root_exit_code, "failure": "copy-command-failed"}
                break
            if root_exit_code == 0 and active_count == 0:
                outcome = "complete"
                result = {"return_code": 0, "process_tree_empty": True}
                break
            if _utc_now() >= deadline:
                outcome = "failed"
                result = {
                    "failure": "hard-deadline-reached",
                    "root_return_code": root_exit_code,
                    "active_processes": active_count,
                }
                break
            time.sleep(0.5)
        _write_copy_result(config, outcome=outcome, result=result)
        return {
            "operation_id": config["operation_id"],
            "outcome": outcome,
            **result,
        }
    except Exception as exc:
        if child is not None:
            try:
                child.close()
            except (OSError, JobObjectError):
                pass
        if not outcome_path.exists():
            try:
                failure_result = {
                    "failure": "copy-controller-error",
                    "error_type": type(exc).__name__,
                    "command_line_sha256": command_hash,
                }
                if pid is not None:
                    failure_result["pid"] = pid
                _write_copy_result(
                    config,
                    outcome="failed",
                    result=failure_result,
                )
            except OSError:
                pass
        raise
    finally:
        if child is not None:
            child.close()
        if job is not None:
            job.close()


def _observation(config: dict[str, Any]) -> tuple[PauseGuardObservation, dict[str, Any] | None]:
    matches, status = _runtime_binding_match(config)
    control = _read_json(Path(str(config["runtime_binding"]["control_path"])))
    capture = status.get("capture_control") if isinstance(status, dict) else None
    if not isinstance(capture, dict):
        capture = {}
    observation = PauseGuardObservation(
        runtime_binding_matches=matches,
        runtime_restart_detected=bool(
            matches is True
            and isinstance(status, dict)
            and status.get("native_runtime") != config["runtime_binding"].get("native_runtime")
        ),
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
        pause_acknowledged_at=_parse_utc(capture.get("acknowledged_at")),
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
    if config.get("final_sync_process_supervision") == "windows-job-object.v1":
        operation_id = str(config["operation_id"])
        try:
            with _control_file_lock(Path(control_path)):
                if not _control_has_owned_pause(
                    _read_json(Path(control_path)), config
                ):
                    return [{"result": "control-owner-changed"}]
                with OperationJob.open_existing(operation_id) as job:
                    active_before = job.active_process_count()
                    if active_before == 0:
                        return [
                            {
                                "result": "absent",
                                "exit_code": 0,
                                "job_object_name": job.name,
                                "active_processes_before": 0,
                                "active_processes_after": 0,
                            }
                        ]
                    terminated = job.terminate_and_wait(10.0)
                    active_after = job.active_process_count()
                    return [
                        {
                            "result": (
                                "terminated-operation-job"
                                if terminated and active_after == 0
                                else "operation-job-cancellation-unconfirmed"
                            ),
                            "exit_code": 0 if terminated and active_after == 0 else 1,
                            "job_object_name": job.name,
                            "active_processes_before": active_before,
                            "active_processes_after": active_after,
                        }
                    ]
        except (JobObjectError, OSError, TimeoutError, RuntimeError) as exc:
            return [
                {
                    "result": "operation-job-cancellation-failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:240],
                }
            ]
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
                "tree_snapshot_path": str(
                    Path(str(config["guard_state_path"])).with_name(
                        f"final-sync-tree-{pid}.json"
                    )
                ),
            },
            separators=(",", ":"),
        ).encode("utf-8")
        encoded_payload = base64.b64encode(payload).decode("ascii")
        script = r'''
$ErrorActionPreference='Stop'
$nativeSource=@'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
public static class FcpPauseProcessNative {
  [StructLayout(LayoutKind.Sequential)] private struct NativeFileTime { public uint Low; public uint High; }
  private const uint PROCESS_TERMINATE=0x0001;
  private const uint PROCESS_QUERY_LIMITED_INFORMATION=0x1000;
  private const uint SYNCHRONIZE=0x00100000;
  private const uint WAIT_OBJECT_0=0x00000000;
  private const uint WAIT_TIMEOUT=0x00000102;
  [DllImport("kernel32.dll", SetLastError=true)] private static extern SafeProcessHandle OpenProcess(uint access,bool inherit,int processId);
  [DllImport("kernel32.dll", SetLastError=true)] private static extern bool GetProcessTimes(SafeProcessHandle process,out NativeFileTime creation,out NativeFileTime exit,out NativeFileTime kernel,out NativeFileTime user);
  [DllImport("kernel32.dll", SetLastError=true)] private static extern bool TerminateProcess(SafeProcessHandle process,uint exitCode);
  [DllImport("kernel32.dll", SetLastError=true)] private static extern uint WaitForSingleObject(SafeProcessHandle process,uint milliseconds);
  private static long ToFileTime(NativeFileTime value) { return unchecked((long)(((ulong)value.High << 32) | value.Low)); }
  public static SafeProcessHandle OpenBoundHandle(int processId) {
    SafeProcessHandle handle=OpenProcess(PROCESS_TERMINATE|PROCESS_QUERY_LIMITED_INFORMATION|SYNCHRONIZE,false,processId);
    if(handle==null || handle.IsInvalid) { int error=Marshal.GetLastWin32Error(); if(handle!=null) handle.Dispose(); throw new Win32Exception(error); }
    return handle;
  }
  public static long GetCreationFileTimeUtc(SafeProcessHandle process) {
    NativeFileTime creation,exit,kernel,user;
    if(!GetProcessTimes(process,out creation,out exit,out kernel,out user)) throw new Win32Exception(Marshal.GetLastWin32Error());
    return ToFileTime(creation);
  }
  public static long GetExitFileTimeUtc(SafeProcessHandle process) {
    NativeFileTime creation,exit,kernel,user;
    if(!GetProcessTimes(process,out creation,out exit,out kernel,out user)) throw new Win32Exception(Marshal.GetLastWin32Error());
    return ToFileTime(exit);
  }
  public static void TerminateBoundHandle(SafeProcessHandle process) {
    if(!TerminateProcess(process,0)) throw new Win32Exception(Marshal.GetLastWin32Error());
  }
  public static bool WaitBoundHandle(SafeProcessHandle process,uint milliseconds) {
    uint result=WaitForSingleObject(process,milliseconds);
    if(result==WAIT_OBJECT_0) return true;
    if(result==WAIT_TIMEOUT) return false;
    throw new Win32Exception(Marshal.GetLastWin32Error());
  }
}
'@
try { Add-Type -TypeDefinition $nativeSource -ErrorAction Stop }
catch { Write-Output ('native-process-helper-load-failed-'+$_.Exception.GetType().Name); exit 11 }
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
$treeSnapshotPath=[string]$payload.tree_snapshot_path
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
function Normalize-CreationTicks([long]$ticks) { return ($ticks-($ticks % 10)) }
function Get-CreationTicks([string]$createdUtc) {
  return (Normalize-CreationTicks ([DateTime]::Parse($createdUtc).ToUniversalTime().ToFileTimeUtc()))
}
function Add-TreeProcess([int]$processId,[string]$createdUtc,[string]$imageName) {
  $identity=[string]$processId+'|'+[string](Get-CreationTicks $createdUtc)
  if(-not $script:treeIdentitySet.ContainsKey($identity)){
    [void]$script:processTree.Add([pscustomobject]@{pid=$processId;creation_utc=$createdUtc;exit_filetime_utc=$null;image_name=$imageName})
    $script:treeIdentitySet[$identity]=$true
    $script:treePidSet[[string]$processId]=$true
    return $true
  }
  foreach($member in $script:processTree){
    if([int]$member.pid -eq $processId -and
       (Get-CreationTicks ([string]$member.creation_utc)) -eq (Get-CreationTicks $createdUtc) -and
       [string]::IsNullOrWhiteSpace([string]$member.image_name)){
      $member.image_name=$imageName
      break
    }
  }
  return $false
}
function Add-ObservedDescendants($processes) {
  $currentByPid=@{}
  foreach($candidate in $processes){ $currentByPid[[string][int]$candidate.ProcessId]=$candidate }
  do {
    $changed=$false
    foreach($candidate in $processes){
      $parentKey=[string][int]$candidate.ParentProcessId
      if($script:treePidSet.ContainsKey($parentKey)){
        $candidatePid=[int]$candidate.ProcessId
        $candidateCreated=$candidate.CreationDate.ToUniversalTime().ToString('o')
        if([string]::IsNullOrWhiteSpace($candidateCreated)){ throw 'descendant identity missing' }
        $candidateTicks=Get-CreationTicks $candidateCreated
        $candidateIdentity=[string]$candidatePid+'|'+[string]$candidateTicks
        $parent=$currentByPid[$parentKey]
        $parentIdentity=''
        if($null -ne $parent){
          $parentIdentity=$parentKey+'|'+[string](Get-CreationTicks ($parent.CreationDate.ToUniversalTime().ToString('o')))
        }
        if($script:treeIdentitySet.ContainsKey($candidateIdentity)){
          continue
        }
        if($script:treeIdentitySet.ContainsKey($parentIdentity)){
          if(Add-TreeProcess -processId $candidatePid -createdUtc $candidateCreated -imageName ([string]$candidate.Name)){ $changed=$true }
        } else {
          # Adopt a late child only when it was created inside the exact lifetime
          # of a previously bound parent. A reused parent PID's later children
          # remain unbound and cannot be terminated by this operation.
          $boundToExitedParent=$false
          foreach($knownParent in $script:processTree.ToArray()){
            if([int]$knownParent.pid -ne [int]$parentKey -or $null -eq $knownParent.exit_filetime_utc){ continue }
            $parentStart=Get-CreationTicks ([string]$knownParent.creation_utc)
            $parentExit=Normalize-CreationTicks ([long]$knownParent.exit_filetime_utc)
            if($candidateTicks -gt $parentStart -and $candidateTicks -lt $parentExit){
              if(Add-TreeProcess -processId $candidatePid -createdUtc $candidateCreated -imageName ([string]$candidate.Name)){ $changed=$true }
              $boundToExitedParent=$true
              break
            }
          }
          if(-not $boundToExitedParent){ $script:unknownDescendant=$true }
        }
      }
    }
  } while($changed)
}
function Save-TreeSnapshot {
  $tempPath=$treeSnapshotPath+'.'+[guid]::NewGuid().ToString('N')+'.tmp'
  try {
    $snapshot=[ordered]@{
      schema='fcp.recorder.pause-guard.process-tree.v3'
      operation_id=$operation
      root_pid=$pidValue
      root_creation_utc=$expectedCreated
      processes=@($script:processTree.ToArray())
    }
    $json=ConvertTo-Json -InputObject $snapshot -Depth 5 -Compress
    [IO.File]::WriteAllText($tempPath,$json,[Text.UTF8Encoding]::new($false))
    if(Test-Path -LiteralPath $treeSnapshotPath){
      $backupPath=$tempPath+'.previous'
      [IO.File]::Replace($tempPath,$treeSnapshotPath,$backupPath)
      try { [IO.File]::Delete($backupPath) } catch {}
    } else {
      [IO.File]::Move($tempPath,$treeSnapshotPath)
    }
    return $true
  } catch {
    try { if(Test-Path -LiteralPath $tempPath){ [IO.File]::Delete($tempPath) } } catch {}
    return ($_.Exception.GetType().Name+':'+$_.Exception.Message)
  }
}
function Test-TreeHasLiveMember($processes) {
  foreach($member in @($script:processTree.ToArray())){
    $memberPid=[int]$member.pid
    $memberCreated=[string]$member.creation_utc
    $remaining=$processes | Where-Object { [int]$_.ProcessId -eq $memberPid } | Select-Object -First 1
    if($null -ne $remaining -and (Get-CreationTicks ($remaining.CreationDate.ToUniversalTime().ToString('o'))) -eq (Get-CreationTicks $memberCreated)){
      return $true
    }
  }
  return $false
}
function Stop-BoundProcessTree($members,[datetime]$deadline,[switch]$RequireRoot) {
  # Retain a native process handle and compare its normalized creation FILETIME
  # before TerminateProcess uses that same handle. PID reuse cannot redirect it.
  $handles=New-Object 'System.Collections.Generic.List[object]'
  $rootHandleSeen=$false
  $terminatedAny=$false
  try {
    $ordered=@($members.ToArray())
    [array]::Reverse($ordered)
    foreach($member in $ordered){
      $memberPid=[int]$member.pid
      $memberCreated=[string]$member.creation_utc
      if($memberPid -ne $pidValue -and [string]$member.image_name -ieq 'conhost.exe'){
        # Windows owns console hosts and may deny PROCESS_TERMINATE. The host
        # exits with its bound console process; verify that through the later tree scan.
        continue
      }
      $safeHandle=$null
      try { $safeHandle=[FcpPauseProcessNative]::OpenBoundHandle($memberPid) }
      catch [ComponentModel.Win32Exception] {
        if($_.Exception.NativeErrorCode -eq 87){ continue }
        return ('process-termination-failed-open-win32-'+[string]$_.Exception.NativeErrorCode)
      }
      catch {
        $cause=$_.Exception.GetBaseException()
        if($cause -is [ComponentModel.Win32Exception]){ return ('process-termination-failed-open-win32-'+[string]$cause.NativeErrorCode) }
        return ('process-termination-failed-open-'+$cause.GetType().Name)
      }
      try {
        $actualCreated=[FcpPauseProcessNative]::GetCreationFileTimeUtc($safeHandle)
        if((Normalize-CreationTicks $actualCreated) -ne (Get-CreationTicks $memberCreated)){
          $safeHandle.Dispose(); $safeHandle=$null
          if($memberPid -eq $pidValue -and $RequireRoot){ return 'root-identity-changed-before-cancel' }
          continue
        }
        if($memberPid -eq $pidValue){ $rootHandleSeen=$true }
        $handles.Add([pscustomobject]@{member=$member;handle=$safeHandle})
        $safeHandle=$null
      } catch {
        $cause=$_.Exception.GetBaseException()
        if($cause -is [ComponentModel.Win32Exception]){ return ('process-termination-failed-identity-check-win32-'+[string]$cause.NativeErrorCode) }
        return ('process-termination-failed-identity-check-'+$cause.GetType().Name)
      }
      finally { if($null -ne $safeHandle){ $safeHandle.Dispose() } }
    }
    if($RequireRoot -and -not $rootHandleSeen){ return 'root-identity-absent-before-cancel' }
    foreach($entry in $handles){
      $remaining=[int]([Math]::Max(0,($deadline-[DateTime]::UtcNow).TotalMilliseconds))
      if($remaining -le 0){ return 'process-tree-cancellation-timeout' }
      $terminationStage='terminate'
      try {
        # GetProcessTimes leaves lpExitTime undefined for a live process. Test
        # the retained handle's signaled state first; only read its exit time
        # after Windows confirms that this exact process has exited.
        $alreadyExited=[FcpPauseProcessNative]::WaitBoundHandle($entry.handle,0)
        if($alreadyExited){
          $exitTicks=[FcpPauseProcessNative]::GetExitFileTimeUtc($entry.handle)
          if($exitTicks -le (Get-CreationTicks ([string]$entry.member.creation_utc))){ return 'process-exit-identity-invalid' }
          $entry.member.exit_filetime_utc=$exitTicks
          $saveResult=Save-TreeSnapshot
          if($saveResult -ne $true){ return ('process-tree-snapshot-write-failed-'+[string]$saveResult) }
          continue
        }
        [FcpPauseProcessNative]::TerminateBoundHandle($entry.handle)
        $terminatedAny=$true
        $terminationStage='wait'
        if(-not [FcpPauseProcessNative]::WaitBoundHandle($entry.handle,[uint32]$remaining)){ return 'process-tree-cancellation-timeout' }
        $terminationStage='read-exit-time'
        $exitTicks=[FcpPauseProcessNative]::GetExitFileTimeUtc($entry.handle)
        if($exitTicks -le (Get-CreationTicks ([string]$entry.member.creation_utc))){ return 'process-exit-identity-invalid' }
        $entry.member.exit_filetime_utc=$exitTicks
        $terminationStage='save-tree-snapshot'
        $saveResult=Save-TreeSnapshot
        if($saveResult -ne $true){ return ('process-tree-snapshot-write-failed-'+[string]$saveResult) }
      } catch {
        $cause=$_.Exception.GetBaseException()
        if($cause -is [ComponentModel.Win32Exception]){ return ('process-termination-failed-'+$terminationStage+'-pid-'+[string]$entry.member.pid+'-win32-'+[string]$cause.NativeErrorCode) }
        return ('process-termination-failed-'+$terminationStage+'-pid-'+[string]$entry.member.pid+'-'+$cause.GetType().Name)
      }
    }
    if($terminatedAny){ return 'terminated' }
    return 'absent'
  } finally { foreach($entry in $handles){ $entry.handle.Dispose() } }
}
function Load-TreeSnapshot {
  if(-not (Test-Path -LiteralPath $treeSnapshotPath)){ return $false }
  try {
    $snapshot=ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($treeSnapshotPath))
    if($snapshot.schema -cne 'fcp.recorder.pause-guard.process-tree.v3' -or
       [string]$snapshot.operation_id -cne $operation -or
       [int]$snapshot.root_pid -ne $pidValue -or
       (Get-CreationTicks ([string]$snapshot.root_creation_utc)) -ne (Get-CreationTicks $expectedCreated)){
      return $false
    }
    foreach($member in @($snapshot.processes)){
      if($null -eq $member -or [int]$member.pid -le 0 -or [string]::IsNullOrWhiteSpace([string]$member.creation_utc) -or [string]::IsNullOrWhiteSpace([string]$member.image_name)){
        return $false
      }
      $exitFiletime=$null
      if($null -ne $member.exit_filetime_utc){
        $exitFiletime=[long]$member.exit_filetime_utc
        if((Normalize-CreationTicks $exitFiletime) -le (Get-CreationTicks ([string]$member.creation_utc))){ return $false }
      }
      $null=Add-TreeProcess -processId ([int]$member.pid) -createdUtc ([string]$member.creation_utc) -imageName ([string]$member.image_name)
      foreach($savedMember in $script:processTree){
        if([int]$savedMember.pid -eq [int]$member.pid -and (Get-CreationTicks ([string]$savedMember.creation_utc)) -eq (Get-CreationTicks ([string]$member.creation_utc))){
          $savedMember.exit_filetime_utc=$exitFiletime
          break
        }
      }
    }
    foreach($member in $script:processTree){
      if([int]$member.pid -eq $pidValue -and
         (Get-CreationTicks ([string]$member.creation_utc)) -eq (Get-CreationTicks $expectedCreated)){ return $true }
    }
    return $false
  } catch { return $false }
}
if(-not (Test-OwnedPause)){ Write-Output 'control-owner-changed'; exit 5 }
# Look up and validate the PID only after acquiring the same lock Recorder uses
# for control changes. This closes the PID-reuse window while lock acquisition waits.
$script:processTree=New-Object 'System.Collections.Generic.List[object]'
$script:treePidSet=@{}
$script:treeIdentitySet=@{}
$script:unknownDescendant=$false
$hasSnapshot=Load-TreeSnapshot
if((Test-Path -LiteralPath $treeSnapshotPath) -and -not $hasSnapshot){ Write-Output 'process-tree-snapshot-invalid'; exit 9 }
$item=Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
if($null -eq $item){
  if(-not $hasSnapshot){ Write-Output 'process-absent-without-tree-snapshot'; exit 10 }
  try { $remainingProcesses=@(Get-CimInstance Win32_Process); Add-ObservedDescendants $remainingProcesses }
  catch { Write-Output 'process-tree-verification-failed'; exit 9 }
  $saveResult=Save-TreeSnapshot
  if($saveResult -ne $true){ Write-Output ('process-tree-snapshot-write-failed-'+[string]$saveResult); exit 9 }
  if($script:unknownDescendant){ Write-Output 'unbound-descendant-under-stale-parent-pid'; exit 10 }
  $cancellationDeadline=[DateTime]::UtcNow.AddSeconds(8)
  $cancelResult=Stop-BoundProcessTree -members $script:processTree -deadline $cancellationDeadline
  if($cancelResult -like 'process-tree-cancellation-timeout'){ Write-Output $cancelResult; exit 6 }
  if($cancelResult -like 'process-termination-failed-*'){ Write-Output $cancelResult; exit 9 }
  try { $remainingProcesses=@(Get-CimInstance Win32_Process); Add-ObservedDescendants $remainingProcesses }
  catch { Write-Output 'process-tree-verification-failed'; exit 9 }
  $saveResult=Save-TreeSnapshot
  if($saveResult -ne $true){ Write-Output ('process-tree-snapshot-write-failed-'+[string]$saveResult); exit 9 }
  if($script:unknownDescendant){ Write-Output 'unbound-descendant-under-stale-parent-pid'; exit 10 }
  try { $remainingProcesses=@(Get-CimInstance Win32_Process); Add-ObservedDescendants $remainingProcesses }
  catch { Write-Output 'process-tree-verification-failed'; exit 9 }
    $saveResult=Save-TreeSnapshot
    if($saveResult -ne $true){ Write-Output ('process-tree-snapshot-write-failed-'+[string]$saveResult); exit 9 }
  if($script:unknownDescendant){ Write-Output 'unbound-descendant-under-stale-parent-pid'; exit 10 }
  if(Test-TreeHasLiveMember $remainingProcesses){ Write-Output 'bound-process-tree-still-running'; exit 8 }
  if($cancelResult -eq 'terminated'){ Write-Output 'terminated-bound-final-sync-process-tree' }
  else { Write-Output 'absent' }
  exit 0
}
$commandLine=[string]$item.CommandLine
if([string]::IsNullOrWhiteSpace($commandLine) -or -not $commandLine.Contains($operation)){ Write-Output 'tag-mismatch'; exit 2 }
$bytes=[Text.Encoding]::UTF8.GetBytes($commandLine)
$hasher=[Security.Cryptography.SHA256]::Create()
try { $sha=[BitConverter]::ToString($hasher.ComputeHash($bytes)).Replace('-','').ToLowerInvariant() }
finally { $hasher.Dispose() }
if($sha -ne $expectedHash){ Write-Output 'command-hash-mismatch'; exit 3 }
$created=$item.CreationDate.ToUniversalTime().ToString('o')
if((Get-CreationTicks $created) -ne (Get-CreationTicks $expectedCreated)){ Write-Output 'creation-time-mismatch'; exit 4 }
try {
  # Snapshot the bound process and its current descendants before termination.
  # A successful root-PID lookup alone cannot prove taskkill /T drained the tree.
  try { $treeProcesses=@(Get-CimInstance Win32_Process) }
  catch { Write-Output 'process-tree-snapshot-failed'; exit 9 }
  if($hasSnapshot -and -not $script:treeIdentitySet.ContainsKey([string]$pidValue+'|'+[string](Get-CreationTicks $created))){
    Write-Output 'process-tree-root-identity-mismatch'; exit 9
  }
  $null=Add-TreeProcess -processId $pidValue -createdUtc $created -imageName ([string]$item.Name)
  try { Add-ObservedDescendants $treeProcesses }
  catch { Write-Output 'process-tree-identity-incomplete'; exit 9 }
  $saveResult=Save-TreeSnapshot
  if($saveResult -ne $true){ Write-Output ('process-tree-snapshot-write-failed-'+[string]$saveResult); exit 9 }
  # The native process handle is identity-bound; PID reuse cannot redirect
  # TerminateProcess after creation identity is verified.
  $cancellationDeadline=[DateTime]::UtcNow.AddSeconds(10)
  $cancelResult=Stop-BoundProcessTree -members $script:processTree -deadline $cancellationDeadline -RequireRoot
  if($cancelResult -ne 'terminated'){
    if($cancelResult -eq 'process-tree-cancellation-timeout'){ Write-Output $cancelResult; exit 6 }
    Write-Output $cancelResult; exit 9
  }
  try { $remainingProcesses=@(Get-CimInstance Win32_Process) }
  catch { Write-Output 'process-tree-verification-failed'; exit 9 }
  try { Add-ObservedDescendants $remainingProcesses }
  catch { Write-Output 'process-tree-identity-incomplete'; exit 9 }
  $saveResult=Save-TreeSnapshot
  if($saveResult -ne $true){ Write-Output ('process-tree-snapshot-write-failed-'+[string]$saveResult); exit 9 }
  if($script:unknownDescendant){ Write-Output 'unbound-descendant-under-stale-parent-pid'; exit 10 }
  if(Test-TreeHasLiveMember $remainingProcesses){ Write-Output 'bound-process-tree-still-running'; exit 8 }
  Write-Output 'terminated-bound-final-sync-process-tree'
} finally { Release-ControlOwnership }
'''
        script = script.replace("__PAYLOAD_BASE64__", encoded_payload)
        script_path = Path(str(config["guard_state_path"])).with_name(
            f"final-sync-cancel-{pid}-{uuid.uuid4().hex}.ps1"
        )
        try:
            with script_path.open("x", encoding="utf-8", newline="\n") as script_file:
                script_file.write(script)
                script_file.flush()
                os.fsync(script_file.fileno())
            encoded_path = base64.b64encode(str(script_path).encode("utf-16-le")).decode(
                "ascii"
            )
            bootstrap = (
                "$ErrorActionPreference='Stop';"
                "$p=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String('"
                + encoded_path
                + "'));$body=[IO.File]::ReadAllText($p);"
                "& ([ScriptBlock]::Create($body))"
            )
            encoded_bootstrap = base64.b64encode(bootstrap.encode("utf-16-le")).decode(
                "ascii"
            )
            completed = subprocess.run(
                [
                    "powershell.exe",
                    "-NoProfile",
                    "-NonInteractive",
                    "-EncodedCommand",
                    encoded_bootstrap,
                ],
                capture_output=True,
                text=True,
                # The PowerShell side has a 5s lock budget and a separate 10s
                # termination budget (plus bounded helper cleanup). Keep the
                # full generated script in a protected file and use a short
                # encoded command to evaluate its contents. Execution policy
                # applies to script files, not command input; this remains
                # usable when Group Policy enforces AllSigned or Restricted.
                timeout=25,
                check=False,
            )
            output_lines = completed.stdout.strip().splitlines()
            result = output_lines[-1] if output_lines else "no-result"
            result_record = {"pid": pid, "result": result, "exit_code": completed.returncode}
            if not output_lines and completed.stderr.strip():
                result_record["stderr_excerpt"] = completed.stderr.strip()[-500:]
            results.append(result_record)
        except subprocess.TimeoutExpired as exc:
            results.append(
                {
                    "pid": pid,
                    "result": "process-check-timeout",
                    "timeout_seconds": exc.timeout,
                    "stdout_excerpt": str(exc.stdout or "")[-500:],
                    "stderr_excerpt": str(exc.stderr or "")[-500:],
                }
            )
        except OSError as exc:
            results.append(
                {
                    "pid": pid,
                    "result": "process-launch-failed",
                    "error_type": type(exc).__name__,
                    "errno": exc.errno,
                    "error": str(exc)[:240],
                }
            )
        finally:
            try:
                script_path.unlink(missing_ok=True)
            except OSError:
                pass
    return results


def _final_sync_cancellation_confirmed(results: list[dict[str, Any]]) -> bool:
    """Return true only when every operation-bound copy is proven absent."""
    return all(
        (result.get("result") == "absent" and result.get("exit_code") == 0)
        or (
            result.get("result") == "terminated-operation-job"
            and result.get("exit_code") == 0
            and result.get("active_processes_after") == 0
        )
        or (
            result.get("result") == "terminated-bound-final-sync-process-tree"
            and result.get("exit_code") == 0
        )
        for result in results
    )


def watch(config_path: Path) -> int:
    config = _load_config(config_path)
    kernel32, mutex = _host_mutex(config)
    final_sync_job: OperationJob | None = None
    try:
        if config.get("final_sync_process_supervision") == "windows-job-object.v1":
            # This independent Scheduled Task holds one Job Object handle for
            # the entire pause operation. If the task dies, KILL_ON_JOB_CLOSE
            # terminates every assigned copy process and descendant.
            final_sync_job = OperationJob.open_or_create(
                str(config["operation_id"]),
                copy_controller_sid=str(config["copy_controller_sid"]),
            )
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
                            runtime_restart_detected=(
                                observation.runtime_restart_detected
                            ),
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
        try:
            if final_sync_job is not None:
                final_sync_job.close()
        finally:
            _release_mutex(kernel32, mutex)


def arm(config_path: Path, register_script: Path | None = None) -> dict[str, Any]:
    config = _load_config(config_path)
    register_script = register_script or Path(str(config["register_script"]))
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
        default=None,
    )
    watch_parser = commands.add_parser("watch")
    watch_parser.add_argument("--config", type=Path, required=True)
    copy_parser = commands.add_parser("run-copy")
    copy_parser.add_argument("--config", type=Path, required=True)
    copy_parser.add_argument("argv", nargs=argparse.REMAINDER)
    hash_parser = commands.add_parser("hash-command")
    hash_parser.add_argument("argv", nargs=argparse.REMAINDER)
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
        if args.command == "run-copy":
            command = list(args.argv[1:] if args.argv[:1] == ["--"] else args.argv)
            result = run_copy(args.config, command)
            print(json.dumps(result, sort_keys=True))
            return 0 if result.get("outcome") == "complete" else 1
        if args.command == "hash-command":
            command = list(args.argv[1:] if args.argv[:1] == ["--"] else args.argv)
            if not command or any(not isinstance(part, str) or "\0" in part for part in command):
                raise ValueError("final-sync command must be a nonempty argument list")
            print(_sha256(_canonical_bytes(command)))
            return 0
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

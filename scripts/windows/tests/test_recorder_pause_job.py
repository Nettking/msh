from __future__ import annotations

import ctypes
import json
import os
import sys
import time
import uuid
from ctypes import wintypes
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.windows import recorder_pause_resume_guard as guard
from scripts.windows.recorder_pause_job import OperationJob
from scripts.windows.recorder_pause_resume_guard_logic import PauseGuardObservation

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows Job Objects are Windows-only")


def _process_handle(pid: int):
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    handle = kernel32.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
    if not handle:
        raise OSError(ctypes.get_last_error(), "OpenProcess failed")
    return kernel32, handle


def _wait_for_path(path: Path, timeout: float = 5.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            value = path.read_text(encoding="ascii").strip()
            if value:
                return value
        time.sleep(0.025)
    raise AssertionError("test child did not publish its child PID")


def _copy_child_command(pid_path: Path) -> list[str]:
    child_code = "import time; time.sleep(120)"
    parent_code = (
        "import pathlib,subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c',sys.argv[2]]); "
        "pathlib.Path(sys.argv[1]).write_text(str(p.pid),encoding='ascii'); "
        "time.sleep(120)"
    )
    return [sys.executable, "-c", parent_code, str(pid_path), child_code]


def test_kill_on_guard_handle_close_terminates_unseen_grandchild(tmp_path: Path) -> None:
    operation_id = uuid.uuid4().hex
    sid = guard._current_windows_sid()
    pid_file = tmp_path / "grandchild.pid"
    guardian = OperationJob.open_or_create(operation_id, copy_controller_sid=sid)
    launcher = OperationJob.open_existing(operation_id)
    process = launcher.create_suspended(_copy_child_command(pid_file))
    process.resume()
    launcher.close()

    grandchild_pid = int(_wait_for_path(pid_file))
    assert guardian.active_process_count() >= 2
    child_kernel32, child_handle = _process_handle(process.pid)
    grand_kernel32, grand_handle = _process_handle(grandchild_pid)

    # This models the independent Scheduled Task losing its process handle.
    # No parent-PID sampling is involved; Windows kills all job members.
    guardian.close()
    try:
        assert child_kernel32.WaitForSingleObject(child_handle, 5_000) == 0
        assert grand_kernel32.WaitForSingleObject(grand_handle, 5_000) == 0
        assert process.wait(0) is not None
    finally:
        child_kernel32.CloseHandle(child_handle)
        grand_kernel32.CloseHandle(grand_handle)
        process.close()


def test_failed_suspended_launch_cleanup_does_not_leave_a_job_member() -> None:
    operation_id = uuid.uuid4().hex
    with OperationJob.open_or_create(
        operation_id, copy_controller_sid=guard._current_windows_sid()
    ) as job:
        process = job.create_suspended([sys.executable, "-c", "import time; time.sleep(60)"])
        assert job.active_process_count() == 1
        process.close()
        assert job.active_process_count() == 0


def test_cancel_operation_job_rechecks_pause_owner_and_verifies_empty_job(
    tmp_path: Path,
) -> None:
    operation_id = uuid.uuid4().hex
    sid = guard._current_windows_sid()
    deadline = guard._iso(datetime.now(timezone.utc) + timedelta(minutes=5))
    control_path = tmp_path / "control.json"
    config = {
        "operation_id": operation_id,
        "prior_control_operation_id": "b" * 32,
        "hard_deadline_utc": deadline,
        "runtime_binding_sha256": "c" * 64,
        "resume_token_sha256": "d" * 64,
        "copy_controller_sid": sid,
        "final_sync_process_supervision": "windows-job-object.v1",
        "runtime_binding": {"control_path": str(control_path)},
    }
    control_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": config["runtime_binding_sha256"],
                    "token_sha256": config["resume_token_sha256"],
                    "deadline_utc": deadline,
                },
            }
        ),
        encoding="utf-8",
    )
    guardian = OperationJob.open_or_create(operation_id, copy_controller_sid=sid)
    launcher = OperationJob.open_existing(operation_id)
    process = launcher.create_suspended(_copy_child_command(tmp_path / "child.pid"))
    process.resume()
    launcher.close()
    _wait_for_path(tmp_path / "child.pid")
    try:
        result = guard._cancel_final_sync(config)
        assert len(result) == 1
        assert result[0]["result"] == "terminated-operation-job"
        assert result[0]["exit_code"] == 0
        assert result[0]["job_object_name"] == guardian.name
        assert result[0]["active_processes_before"] >= 2
        assert result[0]["active_processes_after"] == 0
        assert guard._final_sync_cancellation_confirmed(result)

        control = json.loads(control_path.read_text(encoding="utf-8"))
        control["operation_id"] = "e" * 32
        control["enabled"] = True
        control.pop("resume_guard", None)
        control_path.write_text(json.dumps(control), encoding="utf-8")

        # A newer operator Start supersedes the pause but does not transfer
        # ownership of this operation's Job Object. The old copy must still
        # be stopped before the caller verifies resumed capture.
        next_launcher = OperationJob.open_existing(operation_id)
        next_process = next_launcher.create_suspended(
            _copy_child_command(tmp_path / "newer-start-child.pid")
        )
        next_process.resume()
        next_launcher.close()
        _wait_for_path(tmp_path / "newer-start-child.pid")
        superseded = guard._cancel_final_sync(config)
        assert len(superseded) == 1
        assert superseded[0]["result"] == "terminated-operation-job"
        assert superseded[0]["exit_code"] == 0
        assert superseded[0]["active_processes_after"] == 0
        assert guard._final_sync_cancellation_confirmed(superseded)
        next_process.close()
    finally:
        guardian.close()
        process.close()


def test_run_copy_uses_acknowledged_pause_and_preserves_receipt(tmp_path: Path, monkeypatch) -> None:
    operation_id = uuid.uuid4().hex
    sid = guard._current_windows_sid()
    deadline = guard._iso(datetime.now(timezone.utc) + timedelta(minutes=2))
    args_path = tmp_path / "child output with spaces & symbols.json"
    arg_values = ["two words", 'quote"value', "å & 100%"]
    child_code = (
        "import json,pathlib,sys; "
        "pathlib.Path(sys.argv[1]).write_text(json.dumps(sys.argv[2:]),encoding='utf-8')"
    )
    command = [sys.executable, "-c", child_code, str(args_path), *arg_values]
    control_path = tmp_path / "control.json"
    runtime_binding_sha256 = "c" * 64
    token_sha256 = "d" * 64
    config = {
        "schema": "fcp.recorder.pause-resume-guard.host.v1",
        "operation_id": operation_id,
        "prior_control_operation_id": "b" * 32,
        "hard_deadline_utc": deadline,
        "copy_controller_sid": sid,
        "final_sync_process_supervision": "windows-job-object.v1",
        "runtime_binding_sha256": runtime_binding_sha256,
        "resume_token_sha256": token_sha256,
        "final_sync_command_sha256": guard._sha256(guard._canonical_bytes(command)),
        "runtime_binding": {"repo_root": str(tmp_path), "control_path": str(control_path)},
        "controller_heartbeat_path": str(tmp_path / "heartbeat.json"),
        "copy_outcome_path": str(tmp_path / "outcome.json"),
        "copy_processes_path": str(tmp_path / "processes.json"),
    }
    control_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": runtime_binding_sha256,
                    "token_sha256": token_sha256,
                    "deadline_utc": deadline,
                },
            }
        ),
        encoding="utf-8",
    )
    guardian = OperationJob.open_or_create(operation_id, copy_controller_sid=sid)
    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_current_windows_sid", lambda: sid)
    monkeypatch.setattr(
        guard,
        "_observation",
        lambda _config: (
            PauseGuardObservation(
                runtime_binding_matches=True,
                control_operation_id=operation_id,
                control_enabled=False,
                pause_acknowledged_operation_id=operation_id,
                pause_acknowledged_at=datetime.now(timezone.utc),
                capture_scheduling=False,
                inflight_capture_tasks=0,
                durable_boundary=True,
            ),
            json.loads(control_path.read_text(encoding="utf-8")),
        ),
    )
    try:
        result = guard.run_copy(
            tmp_path / "unused-config.json",
            command,
        )
        assert result["outcome"] == "complete"
        assert json.loads(args_path.read_text(encoding="utf-8")) == arg_values
        receipt = json.loads((tmp_path / "processes.json").read_text(encoding="utf-8"))
        assert receipt["schema"] == "fcp.recorder.pause-copy-processes.v1"
        assert receipt["operation_id"] == operation_id
        assert receipt["job_object_name"] == guardian.name
        assert receipt["command_line_sha256"]
        assert receipt["processes"][0]["pid"] > 0
        assert receipt["outcome"] == "complete"
        assert json.loads((tmp_path / "outcome.json").read_text(encoding="utf-8"))["outcome"] == "complete"
        assert guardian.active_process_count() == 0
    finally:
        guardian.close()


def test_run_copy_refuses_unproven_drain_without_spawning_a_process(
    tmp_path: Path, monkeypatch
) -> None:
    operation_id = uuid.uuid4().hex
    sid = guard._current_windows_sid()
    deadline = guard._iso(datetime.now(timezone.utc) + timedelta(minutes=2))
    command = [sys.executable, "-c", "raise SystemExit(0)"]
    control_path = tmp_path / "control.json"
    config = {
        "operation_id": operation_id,
        "prior_control_operation_id": "b" * 32,
        "hard_deadline_utc": deadline,
        "copy_controller_sid": sid,
        "final_sync_process_supervision": "windows-job-object.v1",
        "runtime_binding_sha256": "c" * 64,
        "resume_token_sha256": "d" * 64,
        "final_sync_command_sha256": guard._sha256(guard._canonical_bytes(command)),
        "runtime_binding": {"repo_root": str(tmp_path), "control_path": str(control_path)},
        "controller_heartbeat_path": str(tmp_path / "heartbeat.json"),
        "copy_outcome_path": str(tmp_path / "outcome.json"),
        "copy_processes_path": str(tmp_path / "processes.json"),
    }
    control_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": config["runtime_binding_sha256"],
                    "token_sha256": config["resume_token_sha256"],
                    "deadline_utc": deadline,
                },
            }
        ),
        encoding="utf-8",
    )
    guardian = OperationJob.open_or_create(operation_id, copy_controller_sid=sid)
    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_current_windows_sid", lambda: sid)
    monkeypatch.setattr(
        guard,
        "_observation",
        lambda _config: (
            PauseGuardObservation(
                runtime_binding_matches=True,
                control_operation_id=operation_id,
                control_enabled=False,
                pause_acknowledged_operation_id=None,
                pause_acknowledged_at=None,
                capture_scheduling=False,
                inflight_capture_tasks=1,
                durable_boundary=False,
            ),
            json.loads(control_path.read_text(encoding="utf-8")),
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="durable drained pause"):
            guard.run_copy(
                tmp_path / "unused-config.json",
                command,
            )
        assert guardian.active_process_count() == 0
        result = json.loads((tmp_path / "outcome.json").read_text(encoding="utf-8"))
        assert result["outcome"] == "failed"
        assert result["failure"] == "copy-controller-error"
        assert result["error_type"] == "RuntimeError"
        receipt = json.loads((tmp_path / "processes.json").read_text(encoding="utf-8"))
        assert "pid" not in receipt
        assert "processes" not in receipt
    finally:
        guardian.close()

from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.windows import recorder_pause_resume_guard as guard
from scripts.windows.recorder_pause_resume_guard_logic import PauseGuardObservation


class _FakeWinApiFunction:
    def __init__(self, result):
        self.result = result
        self.argtypes = None
        self.restype = None
        self.calls = []

    def __call__(self, *args):
        self.calls.append(args)
        return self.result


class _FakeKernel32:
    def __init__(self):
        self.CreateMutexW = _FakeWinApiFunction(1234)
        self.WaitForSingleObject = _FakeWinApiFunction(0)
        self.ReleaseMutex = _FakeWinApiFunction(True)
        self.CloseHandle = _FakeWinApiFunction(True)


def test_final_sync_termination_command_binds_payload_to_protected_script(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    command_hash = hashlib.sha256(b"python copy.py --operation " + operation_id.encode()).hexdigest()
    creation_utc = "2026-10-03T06:00:00Z"
    process_path = Path(config["copy_processes_path"])
    process_path.write_text(
        json.dumps(
            {
                "operation_id": operation_id,
                "processes": [
                    {
                        "pid": 1234,
                        "command_line_sha256": command_hash,
                        "creation_utc": creation_utc,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    Path(config["runtime_binding"]["control_path"]).write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": config["runtime_binding_sha256"],
                    "token_sha256": config["resume_token_sha256"],
                    "deadline_utc": config["hard_deadline_utc"],
                },
            }
        ),
        encoding="utf-8",
    )
    calls: list[list[str]] = []
    call_kwargs: list[dict] = []
    scripts: list[str] = []

    def fake_run(args, **kwargs):
        calls.append(args)
        call_kwargs.append(kwargs)
        scripts.append(Path(args[-1]).read_text(encoding="utf-8"))
        return SimpleNamespace(returncode=0, stdout="terminated-bound-final-sync-process-tree\n")

    monkeypatch.setattr(guard.subprocess, "run", fake_run)
    result = guard._cancel_final_sync(config)

    assert result == [
        {
            "pid": 1234,
            "result": "terminated-bound-final-sync-process-tree",
            "exit_code": 0,
        }
    ]
    assert len(calls) == 1
    assert call_kwargs[0]["timeout"] == 25
    assert calls[0][-2] == "-File"
    assert calls[0][1:6] == [
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
    ]
    assert len(calls[0]) == 7
    script = scripts[0]
    encoded_payload = script.split("FromBase64String('")[1].split("')")[0]
    payload = json.loads(base64.b64decode(encoded_payload).decode("utf-8"))
    assert payload == {
        "pid": 1234,
        "command_line_sha256": command_hash,
        "creation_utc": creation_utc,
        "operation_id": operation_id,
        "control_path": str(Path(config["runtime_binding"]["control_path"])),
        "prior_control_operation_id": config["prior_control_operation_id"],
        "runtime_binding_sha256": config["runtime_binding_sha256"],
        "resume_token_sha256": config["resume_token_sha256"],
        "hard_deadline_utc": config["hard_deadline_utc"],
        "tree_snapshot_path": str(
            Path(config["guard_state_path"]).with_name(
                f"final-sync-tree-{1234}.json"
            )
        ),
    }
    assert operation_id not in script
    assert "[Convert]::ToHexString" not in script
    assert "[Security.Cryptography.SHA256]::Create()" in script
    assert "[IO.File]::Open($controlPath,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::Read)" in script
    assert "[IO.FileShare]::Read" in script
    assert "($controlPath+'.lock')" in script
    assert "$script:controlLockHandle.Lock(0,1)" in script
    assert "$script:controlLockHandle.Unlock(0,1)" in script
    assert "if(-not (Test-OwnedPause)){ Write-Output 'control-owner-changed'; exit 5 }" in script
    assert script.index("Test-OwnedPause)){ Write-Output 'control-owner-changed'") < script.index(
        'Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"'
    )
    assert "[Diagnostics.Process]::GetProcessById($memberPid)" in script
    assert script.index("$bound.StartTime.ToUniversalTime()") < script.index("$bound.Kill()")
    assert "taskkill.exe" not in script
    assert "$script:controlLockHandle.Lock(0,1)" in script
    assert "$attempt -lt 100" in script
    assert "$treeProcesses=@(Get-CimInstance Win32_Process)" in script
    assert "root-identity-changed-before-cancel" in script
    assert "process-termination-failed-" in script
    assert "$remainingProcesses=@(Get-CimInstance Win32_Process)" in script
    assert "bound-process-tree-still-running" in script
    assert "process-absent-without-tree-snapshot" in script
    assert "unbound-descendant-under-stale-parent-pid" in script


def test_final_sync_cancellation_rejects_malformed_process_entry(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    Path(config["copy_processes_path"]).write_text(
        json.dumps({"operation_id": operation_id, "processes": [None]}),
        encoding="utf-8",
    )
    Path(config["runtime_binding"]["control_path"]).write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": operation_id,
                "resume_guard": {
                    "operation_id": operation_id,
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": config["runtime_binding_sha256"],
                    "token_sha256": config["resume_token_sha256"],
                    "deadline_utc": config["hard_deadline_utc"],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    assert guard._cancel_final_sync(config) == [
        {"result": "invalid-final-sync-process-entry"}
    ]


@pytest.mark.parametrize(
    ("results", "confirmed"),
    [
        ([], True),
        ([{"result": "absent", "exit_code": 0}], True),
        ([{"result": "absent"}], False),
        (
            [
                {
                    "result": "terminated-bound-final-sync-process-tree",
                    "exit_code": 0,
                }
            ],
            True,
        ),
        ([{"result": "process-check-failed"}], False),
        ([{"result": "taskkill-timeout", "exit_code": 6}], False),
        ([{"result": "terminated-bound-final-sync-process-tree"}], False),
        ([{"result": "absent"}, {"result": "bound-process-still-running"}], False),
    ],
)
def test_final_sync_cancellation_requires_positive_confirmation(results, confirmed) -> None:
    assert guard._final_sync_cancellation_confirmed(results) is confirmed


def test_final_sync_termination_does_not_run_for_another_operation(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    process_path = Path(config["copy_processes_path"])
    process_path.write_text(
        json.dumps({"operation_id": "b" * 32, "processes": []}),
        encoding="utf-8",
    )
    Path(config["runtime_binding"]["control_path"]).write_text(
        json.dumps(
            {
                "enabled": False,
                "operation_id": config["operation_id"],
                "resume_guard": {
                    "operation_id": config["operation_id"],
                    "prior_control_operation_id": config["prior_control_operation_id"],
                    "runtime_binding_sha256": config["runtime_binding_sha256"],
                    "token_sha256": config["resume_token_sha256"],
                    "deadline_utc": config["hard_deadline_utc"],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    assert guard._cancel_final_sync(config) == [
        {"result": "no-verified-final-sync-process-record"}
    ]


def test_final_sync_is_not_terminated_after_operator_changes_control_owner(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    command_hash = hashlib.sha256(b"copy --operation " + operation_id.encode()).hexdigest()
    Path(config["copy_processes_path"]).write_text(
        json.dumps(
            {
                "operation_id": operation_id,
                "processes": [
                    {
                        "pid": 1234,
                        "command_line_sha256": command_hash,
                        "creation_utc": "2026-10-03T06:00:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    Path(config["runtime_binding"]["control_path"]).write_text(
        json.dumps({"enabled": True, "operation_id": "e" * 32}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    assert guard._cancel_final_sync(config) == [{"result": "control-owner-changed"}]


@pytest.mark.skipif(os.name != "nt", reason="exercise the generated Windows termination script")
def test_termination_script_rechecks_owner_after_initial_guard_observation(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    command_line = "python copy.py --operation " + operation_id
    command_hash = hashlib.sha256(command_line.encode("utf-8")).hexdigest()
    creation_utc = "2026-10-03T06:00:00Z"
    Path(config["copy_processes_path"]).write_text(
        json.dumps(
            {
                "operation_id": operation_id,
                "processes": [
                    {
                        "pid": 1234,
                        "command_line_sha256": command_hash,
                        "creation_utc": creation_utc,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    control_path = Path(config["runtime_binding"]["control_path"])
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
                    "deadline_utc": config["hard_deadline_utc"],
                },
            }
        ),
        encoding="utf-8",
    )
    original_run = guard.subprocess.run

    def change_owner_then_run_script(args, **kwargs):
        script_path = Path(args[-1])
        script = script_path.read_text(encoding="utf-8")
        script = script.replace(
            '$item=Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"',
            f"$item=[pscustomobject]@{{CommandLine='{command_line}';CreationDate=[datetime]::Parse('{creation_utc}')}}",
        )
        script = script.replace(
            "taskkill.exe /PID $pidValue /T /F | Out-Null",
            "Write-Output 'unexpected-termination'; exit 99",
        )
        control_path.write_text(
            json.dumps({"enabled": True, "operation_id": "e" * 32}),
            encoding="utf-8",
        )
        script_path.write_text(script, encoding="utf-8")
        kwargs["timeout"] = 10
        return original_run(args, **kwargs)

    monkeypatch.setattr(guard.subprocess, "run", change_owner_then_run_script)

    assert guard._cancel_final_sync(config) == [
        {"pid": 1234, "result": "control-owner-changed", "exit_code": 5}
    ]


@pytest.mark.skipif(os.name != "nt", reason="exercise generated termination against a disposable process")
def test_termination_script_stops_only_the_bound_disposable_copy_process(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    child_pid_path = tmp_path / "copy-child.pid"
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import subprocess,sys,time; child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); open(sys.argv[1],'w').write(str(child.pid)); time.sleep(60)",
            str(child_pid_path),
            "--operation",
            operation_id,
        ],
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        child_deadline = time.monotonic() + 5
        while not child_pid_path.exists() and time.monotonic() < child_deadline:
            time.sleep(0.05)
        assert child_pid_path.exists(), "disposable copy child did not start"
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        metadata_script = (
            "$ProgressPreference='SilentlyContinue';"
            f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={process.pid}';"
            "$o=[ordered]@{command_line=[string]$p.CommandLine;"
            "creation_utc=$p.CreationDate.ToUniversalTime().ToString('o')};"
            "$o|ConvertTo-Json -Compress"
        )
        metadata_result = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                metadata_script,
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        metadata_line = next(
            line for line in reversed(metadata_result.stdout.splitlines()) if line.startswith("{")
        )
        metadata = json.loads(metadata_line)
        process_path = Path(config["copy_processes_path"])
        process_path.write_text(
            json.dumps(
                {
                    "operation_id": operation_id,
                    "processes": [
                        {
                            "pid": process.pid,
                            "command_line_sha256": hashlib.sha256(
                                metadata["command_line"].encode("utf-8")
                            ).hexdigest(),
                            "creation_utc": metadata["creation_utc"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        Path(config["runtime_binding"]["control_path"]).write_text(
            json.dumps(
                {
                    "enabled": False,
                    "operation_id": operation_id,
                    "resume_guard": {
                        "operation_id": operation_id,
                        "prior_control_operation_id": config["prior_control_operation_id"],
                        "runtime_binding_sha256": config["runtime_binding_sha256"],
                        "token_sha256": config["resume_token_sha256"],
                        "deadline_utc": config["hard_deadline_utc"],
                    },
                }
            ),
            encoding="utf-8",
        )

        original_run = guard.subprocess.run

        def kill_root_without_tree(args, **kwargs):
            script_path = Path(args[-1])
            script = script_path.read_text(encoding="utf-8")
            script = script.replace(
                "$ordered=@($members.ToArray())\n    [array]::Reverse($ordered)",
                "$ordered=@($members.ToArray() | Where-Object { [int]$_.pid -eq $pidValue })\n    [array]::Reverse($ordered)",
            )
            script_path.write_text(script, encoding="utf-8")
            return original_run(args, **kwargs)

        monkeypatch.setattr(guard.subprocess, "run", kill_root_without_tree)
        first_result = guard._cancel_final_sync(config)

        assert first_result == [
            {"pid": process.pid, "result": "bound-process-tree-still-running", "exit_code": 8}
        ]
        assert guard._final_sync_cancellation_confirmed(first_result) is False
        assert process.wait(timeout=5) is not None
        monkeypatch.setattr(guard.subprocess, "run", original_run)
        child_still_running = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={child_pid}'; if($null -ne $p){{exit 0}}else{{exit 1}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert child_still_running.returncode == 0, child_still_running.stderr

        result = guard._cancel_final_sync(config)

        assert result == [
            {
                "pid": process.pid,
                "result": "terminated-bound-final-sync-process-tree",
                "exit_code": 0,
            }
        ]
        assert guard._final_sync_cancellation_confirmed(result) is True
        snapshot_path = Path(config["guard_state_path"]).with_name(
            f"final-sync-tree-{process.pid}.json"
        )
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert {
            process.pid,
            child_pid,
        }.issubset({entry["pid"] for entry in snapshot["processes"]})
        child_absent = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={child_pid}'; if($null -eq $p){{exit 0}}else{{exit 1}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert child_absent.returncode == 0, child_absent.stderr
    finally:
        monkeypatch.setattr(guard.subprocess, "run", original_run)
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if child_pid_path.exists():
            try:
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                subprocess.run(
                    ["taskkill.exe", "/PID", str(child_pid), "/T", "/F"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass


@pytest.mark.skipif(os.name != "nt", reason="exercise PID reuse guard against a disposable process")
def test_guard_refuses_pid_reuse_between_snapshot_and_handle_open(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", "--operation", operation_id],
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    original_run = guard.subprocess.run
    try:
        metadata_script = (
            "$ProgressPreference='SilentlyContinue';"
            f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={process.pid}';"
            "$o=[ordered]@{command_line=[string]$p.CommandLine;"
            "creation_utc=$p.CreationDate.ToUniversalTime().ToString('o')};"
            "$o|ConvertTo-Json -Compress"
        )
        metadata_result = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", metadata_script],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        metadata_line = next(
            line for line in reversed(metadata_result.stdout.splitlines()) if line.startswith("{")
        )
        metadata = json.loads(metadata_line)
        Path(config["copy_processes_path"]).write_text(
            json.dumps(
                {
                    "operation_id": operation_id,
                    "processes": [
                        {
                            "pid": process.pid,
                            "command_line_sha256": hashlib.sha256(
                                metadata["command_line"].encode("utf-8")
                            ).hexdigest(),
                            "creation_utc": metadata["creation_utc"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        Path(config["runtime_binding"]["control_path"]).write_text(
            json.dumps(
                {
                    "enabled": False,
                    "operation_id": operation_id,
                    "resume_guard": {
                        "operation_id": operation_id,
                        "prior_control_operation_id": config["prior_control_operation_id"],
                        "runtime_binding_sha256": config["runtime_binding_sha256"],
                        "token_sha256": config["resume_token_sha256"],
                        "deadline_utc": config["hard_deadline_utc"],
                    },
                }
            ),
            encoding="utf-8",
        )

        def replace_bound_creation_before_cancel(args, **kwargs):
            script_path = Path(args[-1])
            script = script_path.read_text(encoding="utf-8")
            script = script.replace(
                "$cancelResult=Stop-BoundProcessTree -members $script:processTree -deadline $cancellationDeadline -RequireRoot",
                "foreach($member in $script:processTree){if([int]$member.pid -eq $pidValue){$member.creation_utc='2000-01-01T00:00:00Z'}}\n"
                "$cancelResult=Stop-BoundProcessTree -members $script:processTree -deadline $cancellationDeadline -RequireRoot",
            )
            script_path.write_text(script, encoding="utf-8")
            return original_run(args, **kwargs)

        monkeypatch.setattr(guard.subprocess, "run", replace_bound_creation_before_cancel)
        result = guard._cancel_final_sync(config)

        assert result == [
            {
                "pid": process.pid,
                "result": "root-identity-changed-before-cancel",
                "exit_code": 9,
            }
        ]
        assert process.poll() is None
    finally:
        monkeypatch.setattr(guard.subprocess, "run", original_run)
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


@pytest.mark.skipif(os.name != "nt", reason="exercise generated termination against a disposable process")
def test_termination_script_fails_closed_when_bound_process_termination_fails(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    operation_id = config["operation_id"]
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", "--operation", operation_id],
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    original_run = guard.subprocess.run
    try:
        metadata_script = (
            "$ProgressPreference='SilentlyContinue';"
            f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={process.pid}';"
            "$o=[ordered]@{command_line=[string]$p.CommandLine;"
            "creation_utc=$p.CreationDate.ToUniversalTime().ToString('o')};"
            "$o|ConvertTo-Json -Compress"
        )
        metadata_result = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", metadata_script],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        metadata_line = next(
            line for line in reversed(metadata_result.stdout.splitlines()) if line.startswith("{")
        )
        metadata = json.loads(metadata_line)
        Path(config["copy_processes_path"]).write_text(
            json.dumps(
                {
                    "operation_id": operation_id,
                    "processes": [
                        {
                            "pid": process.pid,
                            "command_line_sha256": hashlib.sha256(
                                metadata["command_line"].encode("utf-8")
                            ).hexdigest(),
                            "creation_utc": metadata["creation_utc"],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        Path(config["runtime_binding"]["control_path"]).write_text(
            json.dumps(
                {
                    "enabled": False,
                    "operation_id": operation_id,
                    "resume_guard": {
                        "operation_id": operation_id,
                        "prior_control_operation_id": config["prior_control_operation_id"],
                        "runtime_binding_sha256": config["runtime_binding_sha256"],
                        "token_sha256": config["resume_token_sha256"],
                        "deadline_utc": config["hard_deadline_utc"],
                    },
                }
            ),
            encoding="utf-8",
        )

        def run_with_failing_termination(args, **kwargs):
            script_path = Path(args[-1])
            script = script_path.read_text(encoding="utf-8")
            script = script.replace(
                "$bound.Kill()",
                "throw [InvalidOperationException]::new('simulated process termination failure')",
            )
            script_path.write_text(script, encoding="utf-8")
            return original_run(args, **kwargs)

        monkeypatch.setattr(guard.subprocess, "run", run_with_failing_termination)
        result = guard._cancel_final_sync(config)

        assert result == [
            {
                "pid": process.pid,
                "result": "process-termination-failed-InvalidOperationException",
                "exit_code": 9,
            }
        ]
        assert guard._final_sync_cancellation_confirmed(result) is False
        assert process.poll() is None
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_ps_registration_script_passes_its_operation_id_validator() -> None:
    script = Path(guard.__file__).with_name("register_recorder_pause_resume_guard.ps1")
    text = script.read_text(encoding="utf-8")
    assert "[ValidatePattern('\\A[a-f0-9]{32}\\z')]" in text
    assert "-UserId 'SYSTEM'" in text
    assert "-LogonType ServiceAccount" in text
    assert "-StartWhenAvailable" in text
    assert "-RestartCount 3" in text
    assert "-ExecutionTimeLimit (New-TimeSpan -Minutes 30)" in text
    assert "Start-ScheduledTask -TaskName $taskName" in text


@pytest.mark.skipif(os.name != "nt", reason="Windows named mutex API")
def test_host_mutex_uses_pointer_sized_windows_signatures(monkeypatch) -> None:
    fake = _FakeKernel32()
    monkeypatch.setattr(guard.ctypes, "WinDLL", lambda *_args, **_kwargs: fake)
    kernel32, handle = guard._host_mutex(
        {"runtime_binding": {"repo_root": str(Path.cwd())}},
        timeout_seconds=0,
    )
    try:
        assert handle == 1234
        assert fake.CreateMutexW.calls[0][2].startswith("Global\\FCPHostMutation-")
        assert fake.WaitForSingleObject.calls == [(1234, 0)]
        assert fake.WaitForSingleObject.argtypes == [ctypes.wintypes.HANDLE, ctypes.wintypes.DWORD]
    finally:
        guard._release_mutex(kernel32, handle)


@pytest.mark.skipif(os.name != "nt", reason="Windows ProgramData ACLs")
def test_prepare_keeps_secret_config_under_system_protected_programdata(
    tmp_path: Path, monkeypatch
) -> None:
    program_data = tmp_path / "ProgramData"
    expected_root = program_data / "FCP" / "RecorderPauseGuards"
    monkeypatch.setenv("ProgramData", str(program_data))
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(
        json.dumps(
            {
                "prior_control_operation_id": "d" * 32,
                "hard_deadline_utc": guard._iso(guard._utc_now() + timedelta(minutes=10)),
                "runtime_binding": {
                    "repo_root": r"C:\msh\git",
                    "data_root": r"C:\msh\git\data",
                    "config_path": r"C:\msh\git\data\capabilities\config.json",
                    "control_path": r"C:\msh\git\data\source_state\mtconnect_recorder_control.json",
                    "status_path": r"C:\msh\git\data\source_state\mtconnect_recorder_status.json",
                    "container_id": "a" * 64,
                    "image_id": "sha256:" + "b" * 64,
                    "candidate_commit": "c" * 40,
                    "native_runtime": {
                        "schema": "fcp.recorder-native-runtime.v1",
                        "runtime_type": "native-python",
                        "pid": 1,
                        "process_nonce": None,
                        "supervisor_session": None,
                        "build_commit": None,
                    },
                    "runtime_generation": "e" * 32,
                    "mount_destination": "/app/data",
                    "docker_exe": r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
                },
                "controller_heartbeat_path": str(tmp_path / "controller.json"),
                "copy_outcome_path": str(tmp_path / "copy.json"),
                "copy_processes_path": str(tmp_path / "copy-processes.json"),
                "baseline_capture_schedule_count": 42,
                "flask_url": "http://127.0.0.1:55000",
            }
        ),
        encoding="utf-8",
    )

    prepared = guard.prepare_config(spec_path, expected_root)
    config_path = Path(prepared["config_path"])
    config = json.loads(config_path.read_text(encoding="utf-8"))

    assert config_path == expected_root / prepared["operation_id"] / "guard.json"
    assert config["guard_state_path"].startswith(str(config_path.parent / "evidence"))
    assert "resume_token" not in prepared
    assert "resume_token_sha256" in prepared
    assert Path(config["guard_logic"]).exists()
    assert Path(config["register_script"]).exists()
    loaded = guard._load_config(config_path)
    assert loaded["runtime_binding"]["runtime_generation"] == "e" * 32


def test_runtime_binding_requires_candidate_and_process_generation_match() -> None:
    native = {
        "schema": "fcp.recorder-native-runtime.v1",
        "runtime_type": "native-python",
        "pid": 1,
        "process_nonce": None,
        "supervisor_session": None,
        "build_commit": None,
    }
    runtime = {
        "candidate_commit": "c" * 40,
        "native_runtime": native,
        "runtime_generation": "e" * 32,
    }
    status = {
        "native_runtime": native,
        "acceptance_observability": {
            "provenance": {
                "pid": 1,
                "runtime_generation": "e" * 32,
                "candidate_sha": "c" * 40,
            }
        },
    }
    assert guard._status_process_binding_matches(status, runtime) is True

    changed_process = json.loads(json.dumps(status))
    changed_process["acceptance_observability"]["provenance"]["runtime_generation"] = "f" * 32
    assert guard._status_process_binding_matches(changed_process, runtime) is False

    missing_provenance = {"native_runtime": native}
    assert guard._status_process_binding_matches(missing_provenance, runtime) is None

    changed_candidate = json.loads(json.dumps(status))
    changed_candidate["acceptance_observability"]["provenance"]["candidate_sha"] = "d" * 40
    assert guard._status_process_binding_matches(changed_candidate, runtime) is False


def _watch_config(tmp_path: Path) -> dict:
    operation_id = "a" * 32
    token = "guard-token-for-test"
    deadline = datetime.now(timezone.utc) + timedelta(minutes=10)
    return {
        "operation_id": operation_id,
        "prior_control_operation_id": "b" * 32,
        "hard_deadline_utc": guard._iso(deadline),
        "drain_timeout_seconds": 30,
        "controller_stale_after_seconds": 10,
        "baseline_capture_schedule_count": 100,
        "resume_token": token,
        "resume_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "runtime_binding_sha256": "c" * 64,
        "guard_state_path": str(tmp_path / "guard-state.json"),
        "guard_events_path": str(tmp_path / "guard-events.jsonl"),
        "controller_heartbeat_path": str(tmp_path / "controller.json"),
        "copy_outcome_path": str(tmp_path / "copy.json"),
        "copy_processes_path": str(tmp_path / "copy-processes.json"),
        "runtime_binding": {
            "repo_root": str(tmp_path),
            "control_path": str(tmp_path / "control.json"),
        },
    }


def _arm_test_config(tmp_path: Path) -> dict:
    config = _watch_config(tmp_path)
    config.update(
        {
            "register_script": str(
                Path(guard.__file__).with_name("register_recorder_pause_resume_guard.ps1")
            ),
            "task_name": "FCP-Recorder-PauseResume-" + "a" * 32,
        }
    )
    return config


def test_arm_requires_guard_heartbeat_and_running_host_task(
    tmp_path: Path, monkeypatch
) -> None:
    config = _arm_test_config(tmp_path)
    observation, control = _watch_observation(
        operation_id="b" * 32,
        enabled=True,
        acknowledged=None,
        scheduling=True,
        durable=None,
    )
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        if "-File" in command:
            Path(config["guard_state_path"]).write_text(
                json.dumps(
                    {"operation_id": config["operation_id"], "state": "wait-for-pause"}
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="Running\n", stderr="")

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_observation", lambda _config: (observation, control))
    monkeypatch.setattr(guard.subprocess, "run", fake_run)

    armed = guard.arm(
        tmp_path / "guard.json",
        Path(config["register_script"]),
    )

    assert armed["armed"] is True
    assert armed["operation_id"] == config["operation_id"]
    assert armed["task_name"] == config["task_name"]
    assert len(calls) == 2
    assert calls[1][-1] == (
        f"(Get-ScheduledTask -TaskName '{config['task_name']}').State"
    )


def test_arm_refuses_task_that_is_registered_but_not_running(
    tmp_path: Path, monkeypatch
) -> None:
    config = _arm_test_config(tmp_path)
    observation, control = _watch_observation(
        operation_id="b" * 32,
        enabled=True,
        acknowledged=None,
        scheduling=True,
        durable=None,
    )

    def fake_run(command, **_kwargs):
        if "-File" in command:
            Path(config["guard_state_path"]).write_text(
                json.dumps(
                    {"operation_id": config["operation_id"], "state": "wait-for-pause"}
                ),
                encoding="utf-8",
            )
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="Ready\n", stderr="")

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_observation", lambda _config: (observation, control))
    monkeypatch.setattr(guard.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="not actively running"):
        guard.arm(
            tmp_path / "guard.json",
            Path(config["register_script"]),
        )


def _watch_observation(
    *,
    runtime_matches: bool = True,
    operation_id: str | None = "a" * 32,
    enabled: bool | None = False,
    acknowledged: str | None = "a" * 32,
    scheduling: bool | None = False,
    inflight: int | None = 0,
    durable: bool | None = True,
    heartbeat_age: float | None = 1.0,
    schedule_count: int | None = 100,
) -> tuple[PauseGuardObservation, dict | None]:
    control = (
        {
            "enabled": enabled,
            "operation_id": operation_id,
            "resume_guard": {
                "operation_id": "a" * 32,
                "prior_control_operation_id": "b" * 32,
                "runtime_binding_sha256": "c" * 64,
                "token_sha256": hashlib.sha256(
                    b"guard-token-for-test"
                ).hexdigest(),
                "deadline_utc": None,
            },
        }
        if operation_id == "a" * 32 and enabled is False
        else {"enabled": enabled, "operation_id": operation_id}
    )
    return (
        PauseGuardObservation(
            runtime_binding_matches=runtime_matches,
            control_operation_id=operation_id,
            control_enabled=enabled,
            pause_acknowledged_operation_id=acknowledged,
            capture_scheduling=scheduling,
            inflight_capture_tasks=inflight,
            durable_boundary=durable,
            controller_heartbeat_age_seconds=heartbeat_age,
            copy_outcome=None,
            capture_schedule_count=schedule_count,
        ),
        control,
    )


def test_watch_cancels_final_sync_then_resumes_and_verifies_capture(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    resume_operation_id = "d" * 32
    pause_control = _watch_observation(heartbeat_age=11.0)[1]
    pause_control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    observations = iter(
        [
            _watch_observation(
                operation_id="b" * 32,
                enabled=True,
                acknowledged=None,
                scheduling=True,
                durable=None,
            ),
            (_watch_observation(heartbeat_age=11.0)[0], pause_control),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=False,
                durable=False,
            ),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=True,
                schedule_count=101,
                durable=False,
            ),
        ]
    )
    actions: list[str] = []
    events: list[dict] = []

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: next(observations))
    monkeypatch.setattr(guard, "_read_json", lambda _path: None)
    monkeypatch.setattr(
        guard,
        "_cancel_final_sync",
        lambda _config: actions.append("cancel")
        or [
            {
                "result": "terminated-bound-final-sync-process-tree",
                "exit_code": 0,
            }
        ],
    )
    monkeypatch.setattr(
        guard,
        "_request_resume",
        lambda _config: actions.append("resume")
        or (resume_operation_id, {"result": "accepted"}),
    )
    monkeypatch.setattr(guard, "_event", lambda _config, state, **kw: events.append({"state": state, **kw}))
    monkeypatch.setattr(guard.time, "sleep", lambda _seconds: None)

    assert guard.watch(tmp_path / "guard.json") == 0
    assert actions == ["cancel", "resume"]
    assert [event["state"] for event in events][-2:] == [
        "verify-resume",
        "resume-verified",
    ]
    resume_event = next(event for event in events if event["state"] == "resume-requested")
    assert resume_event["canceled_final_sync"] == [
        {"result": "terminated-bound-final-sync-process-tree", "exit_code": 0}
    ]


def test_watch_retries_unconfirmed_cancellation_before_requesting_resume(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    resume_operation_id = "d" * 32
    pause_control = _watch_observation(heartbeat_age=11.0)[1]
    pause_control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    observations = iter(
        [
            _watch_observation(operation_id="b" * 32, enabled=True),
            (_watch_observation(heartbeat_age=11.0)[0], pause_control),
            (_watch_observation(heartbeat_age=11.0)[0], pause_control),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=False,
                durable=False,
            ),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=True,
                schedule_count=101,
                durable=False,
            ),
        ]
    )
    actions: list[str] = []
    events: list[dict] = []
    cancellation_results = iter(
        [
            [{"result": "process-check-failed"}],
            [
                {
                    "result": "terminated-bound-final-sync-process-tree",
                    "exit_code": 0,
                }
            ],
        ]
    )

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: next(observations))
    monkeypatch.setattr(guard, "_read_json", lambda _path: None)
    monkeypatch.setattr(
        guard,
        "_cancel_final_sync",
        lambda _config: actions.append("cancel") or next(cancellation_results),
    )
    monkeypatch.setattr(
        guard,
        "_request_resume",
        lambda _config: actions.append("resume")
        or (resume_operation_id, {"result": "accepted"}),
    )
    monkeypatch.setattr(
        guard, "_event", lambda _config, state, **kw: events.append({"state": state, **kw})
    )
    monkeypatch.setattr(guard.time, "sleep", lambda _seconds: None)

    assert guard.watch(tmp_path / "guard.json") == 0
    assert actions == ["cancel", "cancel", "resume"]
    assert [event["state"] for event in events].count(
        "final-sync-cancellation-unconfirmed"
    ) == 1
    assert events[0]["state"] == "armed"
    assert next(
        index for index, event in enumerate(events) if event["state"] == "resume-requested"
    ) > next(
        index
        for index, event in enumerate(events)
        if event["state"] == "final-sync-cancellation-unconfirmed"
    )


def test_restarted_watch_revalidates_copy_before_resume(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    resume_operation_id = "d" * 32
    pause_observation, pause_control = _watch_observation(heartbeat_age=11.0)
    assert pause_control is not None
    pause_control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    observations = iter(
        [
            (pause_observation, pause_control),
            (pause_observation, pause_control),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=False,
                durable=False,
            ),
            _watch_observation(
                operation_id=resume_operation_id,
                enabled=True,
                acknowledged=None,
                scheduling=True,
                schedule_count=101,
                durable=False,
            ),
        ]
    )
    actions: list[str] = []
    events: list[dict] = []

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: next(observations))
    monkeypatch.setattr(
        guard,
        "_read_json",
        lambda path: (
            {"recovery_triggered": True}
            if Path(path) == Path(config["guard_state_path"])
            else None
        ),
    )
    monkeypatch.setattr(
        guard,
        "_cancel_final_sync",
        lambda _config: actions.append("cancel")
        or [
            {
                "result": "terminated-bound-final-sync-process-tree",
                "exit_code": 0,
            }
        ],
    )
    monkeypatch.setattr(
        guard,
        "_request_resume",
        lambda _config: actions.append("resume")
        or (resume_operation_id, {"result": "accepted"}),
    )
    monkeypatch.setattr(
        guard,
        "_event",
        lambda _config, state, **kw: events.append({"state": state, **kw}),
    )
    monkeypatch.setattr(guard.time, "sleep", lambda _seconds: None)

    assert guard.watch(tmp_path / "guard.json") == 0
    assert actions == ["cancel", "resume"]
    assert events[0]["state"] == "guard-restarted-for-owned-pause"
    assert events[-1]["state"] == "resume-verified"


def test_restarted_watch_refuses_paused_control_with_different_guard_binding(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    observation, control = _watch_observation()
    assert control is not None
    control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    control["resume_guard"]["runtime_binding_sha256"] = "e" * 64
    events: list[dict] = []
    actions: list[str] = []

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: (observation, control))
    monkeypatch.setattr(
        guard,
        "_request_resume",
        lambda _config: actions.append("resume"),
    )
    monkeypatch.setattr(
        guard,
        "_event",
        lambda _config, state, **kw: events.append({"state": state, **kw}),
    )

    assert guard.watch(tmp_path / "guard.json") == 2
    assert actions == []
    assert events[-1]["state"] == "control-owner-changed-before-pause"


def test_watch_failure_retains_redacted_reason_when_event_file_is_unavailable(
    monkeypatch, capsys
) -> None:
    def _raise_watch_error(_path: Path) -> int:
        raise RuntimeError("secret-token-value")

    def _raise_config_error(_path: Path) -> dict:
        raise OSError("protected config unavailable")

    monkeypatch.setattr(
        guard.sys,
        "argv",
        ["recorder_pause_resume_guard.py", "watch", "--config", "guard.json"],
    )
    monkeypatch.setattr(guard, "watch", _raise_watch_error)
    monkeypatch.setattr(guard, "_load_config", _raise_config_error)

    assert guard.main() == 2
    captured = capsys.readouterr()
    assert "recorder-pause-guard failed: RuntimeError" in captured.err
    assert "failure record unavailable: OSError" in captured.err
    assert "secret-token-value" not in captured.err


def test_watch_records_runtime_identity_mismatch_without_stale_resume(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    pause_control = _watch_observation()[1]
    pause_control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    observations = iter(
        [
                _watch_observation(
                    operation_id="b" * 32,
                    enabled=True,
                    acknowledged=None,
                    scheduling=True,
                    durable=None,
                ),
                (_watch_observation(runtime_matches=False)[0], pause_control),
            ]
        )
    actions: list[str] = []
    events: list[dict] = []

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: next(observations))
    monkeypatch.setattr(guard, "_read_json", lambda _path: None)
    monkeypatch.setattr(guard, "_request_resume", lambda _config: actions.append("resume"))
    monkeypatch.setattr(guard, "_cancel_final_sync", lambda _config: actions.append("cancel"))
    monkeypatch.setattr(guard, "_event", lambda _config, state, **kw: events.append({"state": state, **kw}))

    assert guard.watch(tmp_path / "guard.json") == 3
    assert actions == ["cancel"]
    assert events[-1]["state"] == "identity-unverified"
    assert events[-1]["capture_was_paused"] is True
    assert events[-1]["canceled_final_sync"] is None


def test_restarted_guard_cancels_owned_copy_but_never_resumes_on_identity_mismatch(
    tmp_path: Path, monkeypatch
) -> None:
    config = _watch_config(tmp_path)
    observation, control = _watch_observation(runtime_matches=False)
    assert control is not None
    control["resume_guard"]["deadline_utc"] = config["hard_deadline_utc"]
    actions: list[str] = []
    events: list[dict] = []

    monkeypatch.setattr(guard, "_load_config", lambda _path: config)
    monkeypatch.setattr(guard, "_host_mutex", lambda _config: (object(), 1234))
    monkeypatch.setattr(guard, "_release_mutex", lambda *_args: None)
    monkeypatch.setattr(guard, "_observation", lambda _config: (observation, control))
    monkeypatch.setattr(
        guard,
        "_cancel_final_sync",
        lambda _config: actions.append("cancel") or [{"result": "terminated"}],
    )
    monkeypatch.setattr(
        guard,
        "_request_resume",
        lambda _config: actions.append("resume") or ("d" * 32, {"result": "accepted"}),
    )
    monkeypatch.setattr(
        guard,
        "_event",
        lambda _config, state, **values: events.append({"state": state, **values}),
    )

    assert guard.watch(tmp_path / "guard.json") == 2
    assert actions == ["cancel"]
    assert events == [
        {
            "state": "identity-unverified-before-pause",
            "binding": False,
            "capture_was_paused": True,
            "canceled_final_sync": [{"result": "terminated"}],
        }
    ]

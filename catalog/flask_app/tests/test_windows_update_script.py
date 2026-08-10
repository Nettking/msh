from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _update_script() -> str:
    return (_repository_root() / "update.cmd").read_text(encoding="utf-8")


def _git(*args: str, cwd: Path) -> None:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_update_cmd_selects_relay_state_outside_parse_time_block() -> None:
    script = _update_script()

    assert "call :select_relay_volume" in script
    assert ":select_relay_volume" in script
    assert 'set "FCP_RELAY_SELECTION_FILE=%TEMP%' in script
    assert '-OutputFile "%FCP_RELAY_SELECTION_FILE%"' in script
    assert "setlocal EnableDelayedExpansion" not in script
    assert "!FCP_RELAY_SELECTION_FILE!" not in script


@pytest.mark.skipif(os.name != "nt", reason="Windows batch execution regression")
def test_update_cmd_passes_nonempty_relay_selection_output_file(
    tmp_path: Path,
) -> None:
    root = tmp_path / "fcp update fixture"
    remote = tmp_path / "fixture remote.git"
    scripts = root / "scripts" / "windows"
    temp_dir = root / "temp files"
    scripts.mkdir(parents=True)
    temp_dir.mkdir()

    (root / "update.cmd").write_text(_update_script(), encoding="utf-8")
    (root / "start.cmd").write_text(
        "@echo off\n"
        'if /I not "%~1"=="--resume" exit /b 9\n'
        "echo START_RESUME_OK\n"
        "exit /b 0\n",
        encoding="utf-8",
    )
    (scripts / "select_fcp_relay_volume.ps1").write_text(
        "[CmdletBinding()]\n"
        "param(\n"
        "    [Parameter(Mandatory = $true)]\n"
        "    [ValidateNotNullOrEmpty()]\n"
        "    [string]$DataDirectory,\n"
        "    [Parameter(Mandatory = $true)]\n"
        "    [ValidateNotNullOrEmpty()]\n"
        "    [string]$OutputFile\n"
        ")\n"
        "if ([string]::IsNullOrWhiteSpace($OutputFile)) {\n"
        '    [Console]::Error.WriteLine("EMPTY_OUTPUT_FILE")\n'
        "    exit 41\n"
        "}\n"
        "$parent = [System.IO.Path]::GetDirectoryName($OutputFile)\n"
        "[System.IO.Directory]::CreateDirectory($parent) | Out-Null\n"
        "$encoding = New-Object System.Text.UTF8Encoding($false)\n"
        "[System.IO.File]::WriteAllText(\n"
        '    $OutputFile, "fcp_relay_state", $encoding\n'
        ")\n"
        "exit 0\n",
        encoding="utf-8",
    )

    remote.mkdir()
    _git("init", "--bare", cwd=remote)
    _git("init", "-b", "main", cwd=root)
    _git("config", "user.email", "windows-update-test@example.invalid", cwd=root)
    _git("config", "user.name", "Windows Update Test", cwd=root)
    _git("add", "update.cmd", "start.cmd", "scripts/windows", cwd=root)
    _git("commit", "-m", "fixture", cwd=root)
    _git("remote", "add", "origin", str(remote), cwd=root)
    _git("push", "-u", "origin", "main", cwd=root)

    env = os.environ.copy()
    env.pop("FCP_RELAY_VOLUME_NAME", None)
    env["TEMP"] = str(temp_dir)
    env["TMP"] = str(temp_dir)

    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "update.cmd"],
        cwd=root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    output = completed.stdout + completed.stderr

    assert completed.returncode == 0, output
    assert "Federation state selected: fcp_relay_state" in output
    assert "START_RESUME_OK" in output
    assert "EMPTY_OUTPUT_FILE" not in output
    assert "Cannot bind argument to parameter 'OutputFile'" not in output

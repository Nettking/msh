from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = ROOT / "start.cmd"
HELPER = ROOT / "scripts" / "windows" / "stop_fcp_for_fresh_reset.ps1"


def test_fresh_reset_routes_shutdown_through_bounded_helper() -> None:
    launcher = LAUNCHER.read_text(encoding="utf-8")
    reset_section = launcher.split(":reset_device_state", 1)[1].split(":show_help", 1)[0]
    helper = HELPER.read_text(encoding="utf-8")

    assert "stop_fcp_for_fresh_reset.ps1" in reset_section
    assert "docker compose down --remove-orphans" not in reset_section

    assert "WaitForExit" in helper
    assert "taskkill.exe /PID $process.Id /T /F" in helper
    assert '@("compose", "kill")' in helper
    assert '@("compose", "down", "--remove-orphans", "--timeout", "5")' in helper

    # Recovery must stay scoped to the current Compose project and must never
    # broaden a fresh-device reset into Docker-wide or volume deletion.
    lowered = helper.lower()
    assert "system prune" not in lowered
    assert "container prune" not in lowered
    assert "volume prune" not in lowered
    assert '"--volumes"' not in lowered


@pytest.mark.skipif(sys.platform != "win32", reason="Windows process-tree recovery contract")
def test_shutdown_timeout_recovers_without_hanging(tmp_path: Path) -> None:
    docker = tmp_path / "docker.cmd"
    log = tmp_path / "docker.log"
    marker = tmp_path / "first-down.marker"
    docker.write_text(
        "\n".join(
            [
                "@echo off",
                'echo %*>>"%FCP_TEST_DOCKER_LOG%"',
                'if /I "%1 %2"=="compose down" if not exist "%FCP_TEST_DOCKER_MARKER%" (',
                '  type nul > "%FCP_TEST_DOCKER_MARKER%"',
                '  powershell -NoProfile -Command "Start-Sleep -Seconds 30"',
                ")",
                "exit /b 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}{os.pathsep}{env.get('PATH', '')}"
    env["FCP_TEST_DOCKER_LOG"] = str(log)
    env["FCP_TEST_DOCKER_MARKER"] = str(marker)

    started = time.monotonic()
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(HELPER),
            "-PrimaryTimeoutSeconds",
            "5",
            "-RecoveryTimeoutSeconds",
            "5",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 0, result.stdout + result.stderr
    assert elapsed < 15
    calls = log.read_text(encoding="utf-8").lower()
    assert calls.count("compose down") == 2
    assert "compose kill" in calls

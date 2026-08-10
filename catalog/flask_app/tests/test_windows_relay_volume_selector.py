from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _selector_script() -> Path:
    return _repository_root() / "scripts" / "windows" / "select_fcp_relay_volume.ps1"


def test_selector_uses_exit_codes_and_existing_relay_mounts() -> None:
    script = _selector_script().read_text(encoding="utf-8")

    assert "function Invoke-Docker" in script
    assert '$ErrorActionPreference = "Continue"' in script
    assert '"image", "inspect", $image' in script
    assert "Get-RelayContainerInfo" in script
    assert '$_ .Destination' not in script
    assert '$_ .Type' not in script
    assert '"/var/lib/fcp-relay"' in script
    assert "Multiple running relay containers use different state volumes" in script
    assert "Multiple relay-state volumes exist but none can be identified safely" in script


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell regression")
def test_selector_survives_missing_current_images_and_reuses_existing_relay_volume(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_file = tmp_path / "selected.txt"
    command_log = tmp_path / "docker-commands.txt"

    docker_cmd = fake_bin / "docker.cmd"
    docker_cmd.write_text(
        "@echo off\n"
        "setlocal EnableExtensions\n"
        "echo docker %*>>\"%FCP_FAKE_DOCKER_LOG%\"\n"
        "if /I \"%1\"==\"ps\" (echo relay123&exit /b 0)\n"
        "if /I \"%1\"==\"inspect\" if /I \"%2\"==\"relay123\" (\n"
        "  echo [{\"Mounts\":[{\"Destination\":\"/var/lib/fcp-relay\",\"Type\":\"volume\",\"Name\":\"retained_relay_state\"}],\"Image\":\"sha256:retainedprobe\",\"State\":{\"Running\":true}}]\n"
        "  exit /b 0\n"
        ")\n"
        "if /I \"%1\"==\"volume\" if /I \"%2\"==\"ls\" (echo retained_relay_state&exit /b 0)\n"
        "if /I \"%1\"==\"image\" if /I \"%2\"==\"inspect\" (\n"
        "  if /I \"%3\"==\"sha256:retainedprobe\" exit /b 0\n"
        "  1>&2 echo Error response from daemon: No such image: %3\n"
        "  exit /b 1\n"
        ")\n"
        "if /I \"%1\"==\"run\" (\n"
        "  echo {\"exists\":1,\"size\":128,\"nodes\":1,\"sessions\":1,\"memberships\":0}\n"
        "  exit /b 0\n"
        ")\n"
        "exit /b 9\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PATH"] = str(fake_bin) + os.pathsep + env["PATH"]
    env["FCP_FAKE_DOCKER_LOG"] = str(command_log)

    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_selector_script()),
            "-DataDirectory",
            str(data_dir),
            "-OutputFile",
            str(output_file),
        ],
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert output_file.read_text(encoding="utf-8") == "retained_relay_state"

    log = command_log.read_text(encoding="utf-8").casefold()
    assert "image inspect fcp-relay:latest" in log
    assert "image inspect sha256:retainedprobe" in log
    assert "docker run --rm --network none" in log

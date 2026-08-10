from __future__ import annotations

import json
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


def test_selector_has_read_only_probe_fallback_for_ambiguous_legacy_volumes() -> None:
    script = _selector_script().read_text(encoding="utf-8")

    assert '$FallbackProbeImage = "python:3.12-slim"' in script
    assert "function Ensure-FallbackProbeImage" in script
    assert 'Invoke-Docker -Arguments @("pull", $FallbackProbeImage)' in script
    assert '$candidates.Count -gt 1 -and $images.Count -eq 0' in script
    assert '"-v", $mount' in script
    assert '"${VolumeName}:/state:ro"' in script
    assert "tempfile.TemporaryDirectory" in script
    assert "shutil.copy2(path, probe_path)" in script
    assert 'for suffix in ("-wal", "-journal")' in script
    assert 'sqlite3.connect(probe_path)' in script
    assert 'f"file:{path}?mode=ro"' not in script
    assert 'result["identity_matches"]' in script
    assert "Saved device identity is active in multiple relay-state volumes" in script
    assert "Saved device identity appears in multiple relay-state volumes" in script
    assert "Multiple populated relay-state volumes exist without a unique saved-device membership" in script
    assert "docker volume rm" not in script.casefold()
    assert "docker compose down" not in script.casefold()


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
        "  echo {\"exists\":1,\"size\":128,\"nodes\":1,\"sessions\":1,\"memberships\":0,\"identity_matches\":0}\n"
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


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell regression")
def test_selector_pulls_python_probe_and_identifies_pc3_identity(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    data_dir = tmp_path / "data"
    identity_dir = data_dir / "federation" / "device"
    identity_dir.mkdir(parents=True)
    (identity_dir / "identity.json").write_text(
        json.dumps({"node_id": "node-pc3"}),
        encoding="utf-8",
    )

    output_file = tmp_path / "selected.txt"
    command_log = tmp_path / "docker-commands.txt"
    pull_marker = tmp_path / "python-probe-pulled.txt"

    docker_cmd = fake_bin / "docker.cmd"
    docker_cmd.write_text(
        "@echo off\n"
        "setlocal EnableExtensions\n"
        "echo docker %*>>\"%FCP_FAKE_DOCKER_LOG%\"\n"
        "if /I \"%1\"==\"ps\" exit /b 0\n"
        "if /I \"%1\"==\"volume\" if /I \"%2\"==\"ls\" (\n"
        "  echo old_a_relay_state\n"
        "  echo old_b_relay_state\n"
        "  exit /b 0\n"
        ")\n"
        "if /I \"%1\"==\"image\" if /I \"%2\"==\"inspect\" (\n"
        "  if /I \"%3\"==\"python:3.12-slim\" if exist \"%FCP_FAKE_PULL_MARKER%\" exit /b 0\n"
        "  1>&2 echo Error response from daemon: No such image: %3\n"
        "  exit /b 1\n"
        ")\n"
        "if /I \"%1\"==\"pull\" if /I \"%2\"==\"python:3.12-slim\" (\n"
        "  echo pulled>\"%FCP_FAKE_PULL_MARKER%\"\n"
        "  echo Status: Downloaded newer image for python:3.12-slim\n"
        "  exit /b 0\n"
        ")\n"
        "if /I \"%1\"==\"run\" (\n"
        "  echo %*|findstr /I /C:\"old_b_relay_state:/state:ro\" >nul\n"
        "  if not errorlevel 1 (\n"
        "    echo {\"exists\":1,\"size\":256,\"nodes\":3,\"sessions\":1,\"memberships\":0,\"identity_matches\":1}\n"
        "  ) else (\n"
        "    echo {\"exists\":1,\"size\":128,\"nodes\":0,\"sessions\":0,\"memberships\":0,\"identity_matches\":0}\n"
        "  )\n"
        "  exit /b 0\n"
        ")\n"
        "exit /b 9\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PATH"] = str(fake_bin) + os.pathsep + env["PATH"]
    env["FCP_FAKE_DOCKER_LOG"] = str(command_log)
    env["FCP_FAKE_PULL_MARKER"] = str(pull_marker)

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
    assert output_file.read_text(encoding="utf-8") == "old_b_relay_state"
    assert pull_marker.exists()

    log = command_log.read_text(encoding="utf-8").casefold()
    assert "docker pull python:3.12-slim" in log
    assert "old_a_relay_state:/state:ro" in log
    assert "old_b_relay_state:/state:ro" in log

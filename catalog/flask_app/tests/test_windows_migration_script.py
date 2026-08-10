from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _migration_script() -> Path:
    return _repository_root() / "scripts" / "windows" / "migrate_existing_fcp.ps1"


def _migration_launcher() -> str:
    return (_repository_root() / "migrate.cmd").read_text(encoding="utf-8")


def test_migration_bootstrap_is_strict_fast_forward_only() -> None:
    script = _migration_script().read_text(encoding="utf-8")

    assert '$ApprovedRepository = "Nettking/msh"' in script
    assert '$ApprovedBranch = "main"' in script
    assert '$ApprovedRemote = "origin"' in script
    assert '"remote", "get-url", $ApprovedRemote' in script
    assert "Test-ApprovedRemote" in script
    assert '"symbolic-ref", "--quiet", "--short", "HEAD"' in script
    assert '"status", "--porcelain=v1", "--untracked-files=all"' in script
    assert '"fetch", "--no-tags", $ApprovedRemote, $ApprovedBranch' in script
    assert '"merge-base", "--is-ancestor"' in script
    assert '"merge", "--ff-only", $target' in script
    assert '"cat-file", "-e", "${target}:$path"' in script

    forbidden = (
        "git reset",
        "git clean",
        "git stash",
        "git rebase",
        "git checkout",
        "docker volume rm",
        "docker volume prune",
        "docker compose down -v",
        "Remove-Item -Recurse",
    )
    for value in forbidden:
        assert value.casefold() not in script.casefold()


def test_migration_preflights_runtime_and_preserves_federation_state() -> None:
    script = _migration_script().read_text(encoding="utf-8")

    docker_info = 'Invoke-Native -FilePath $docker -Arguments @("info")'
    docker_compose = 'Invoke-Native -FilePath $docker -Arguments @("compose", "version")'
    fast_forward = '"merge", "--ff-only", $target'
    assert docker_info in script
    assert docker_compose in script
    assert script.index(docker_info) < script.index(fast_forward)
    assert script.index(docker_compose) < script.index(fast_forward)

    assert '$env:COMPOSE_PROJECT_NAME = "fcp"' in script
    assert "Select-RetainedRelayVolume" in script
    assert "select_fcp_relay_volume.ps1" in script
    assert "resolve_fcp_web_port.ps1" in script
    assert "fcp_update_agent.ps1" in script
    assert '& $startScript --resume' in script
    assert script.index("Select-RetainedRelayVolume -Root $root") < script.index(
        '& $startScript --resume'
    )
    assert "Saved identity, Federation membership, evidence, data, models, and Docker volumes are retained." in script
    assert "can participate in Federation -> Update all devices" in script


def test_migrate_cmd_delegates_to_powershell_bootstrap() -> None:
    launcher = _migration_launcher()

    assert "migrate_existing_fcp.ps1" in launcher
    assert '-RepoRoot "%~dp0"' in launcher
    assert "start.cmd --fresh" not in launcher
    assert "git reset" not in launcher
    assert "git clean" not in launcher
    assert "docker compose down" not in launcher


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser check")
def test_windows_migration_script_has_valid_powershell_syntax() -> None:
    path = _migration_script()
    escaped_path = str(path).replace("'", "''")
    command = (
        "$errors = $null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_path}', [ref]$null, [ref]$errors) | Out-Null; "
        "if ($errors.Count -gt 0) { "
        "$errors | ForEach-Object { Write-Error $_.Message }; exit 1 }"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.skipif(os.name != "nt", reason="Windows migration smoke")
def test_windows_migration_fast_forwards_fake_legacy_checkout_and_resumes(
    tmp_path: Path,
) -> None:
    fake_root = tmp_path / "legacy checkout"
    fake_root.mkdir()
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    merge_marker = tmp_path / "merged.txt"
    start_marker = tmp_path / "started.txt"
    command_log = tmp_path / "commands.txt"

    current = "1" * 40
    target = "2" * 40

    git_cmd = fake_bin / "git.cmd"
    git_cmd.write_text(
        "@echo off\n"
        "setlocal EnableExtensions\n"
        "echo git %*>>\"%FCP_FAKE_LOG%\"\n"
        "if /I \"%1\"==\"-C\" (shift&shift)\n"
        "if /I \"%1\"==\"rev-parse\" if /I \"%2\"==\"--show-toplevel\" (echo %FCP_FAKE_ROOT%&exit /b 0)\n"
        "if /I \"%1\"==\"remote\" if /I \"%2\"==\"get-url\" (echo https://github.com/Nettking/msh.git&exit /b 0)\n"
        "if /I \"%1\"==\"symbolic-ref\" (echo main&exit /b 0)\n"
        "if /I \"%1\"==\"status\" (exit /b 0)\n"
        "if /I \"%1\"==\"fetch\" (exit /b 0)\n"
        "if /I \"%1\"==\"cat-file\" (exit /b 0)\n"
        "if /I \"%1\"==\"merge-base\" (exit /b 0)\n"
        "if /I \"%1\"==\"merge\" (echo merged>\"%FCP_FAKE_MERGED%\"&exit /b 0)\n"
        "if /I \"%1\"==\"rev-parse\" if /I \"%2\"==\"--verify\" (\n"
        "  echo %3|findstr /I \"FETCH_HEAD\" >nul && (echo %FCP_FAKE_TARGET%&exit /b 0)\n"
        "  if exist \"%FCP_FAKE_MERGED%\" (echo %FCP_FAKE_TARGET%) else (echo %FCP_FAKE_CURRENT%)\n"
        "  exit /b 0\n"
        ")\n"
        "exit /b 9\n",
        encoding="utf-8",
    )

    docker_cmd = fake_bin / "docker.cmd"
    docker_cmd.write_text(
        "@echo off\n"
        "echo docker %*>>\"%FCP_FAKE_LOG%\"\n"
        "if /I \"%1\"==\"info\" exit /b 0\n"
        "if /I \"%1\"==\"compose\" if /I \"%2\"==\"version\" exit /b 0\n"
        "exit /b 9\n",
        encoding="utf-8",
    )

    start_cmd = fake_root / "start.cmd"
    start_cmd.write_text(
        "@echo off\n"
        "echo start %*>>\"%FCP_FAKE_LOG%\"\n"
        "echo started>\"%FCP_FAKE_STARTED%\"\n"
        "exit /b 0\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PATH"] = str(fake_bin) + os.pathsep + env["PATH"]
    env["FCP_FAKE_ROOT"] = str(fake_root)
    env["FCP_FAKE_CURRENT"] = current
    env["FCP_FAKE_TARGET"] = target
    env["FCP_FAKE_MERGED"] = str(merge_marker)
    env["FCP_FAKE_STARTED"] = str(start_marker)
    env["FCP_FAKE_LOG"] = str(command_log)
    env["FCP_RELAY_VOLUME_NAME"] = "retained_relay_state"

    completed = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_migration_script()),
            "-RepoRoot",
            str(fake_root),
        ],
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert merge_marker.exists()
    assert start_marker.exists()
    assert "Migration complete." in completed.stdout

    log = command_log.read_text(encoding="utf-8").casefold()
    assert "docker info" in log
    assert "docker compose version" in log
    assert "git fetch --no-tags origin main" in log
    assert f"git merge --ff-only {target}" in log
    assert "start --resume" in log
    assert log.index("docker info") < log.index("git merge --ff-only")
    assert "git reset" not in log
    assert "git clean" not in log
    assert "git stash" not in log

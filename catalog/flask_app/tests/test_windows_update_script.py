from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _update_script() -> str:
    return (_repository_root() / "update.cmd").read_text(encoding="utf-8")


def test_update_cmd_is_a_non_mutating_retirement_shim() -> None:
    script = _update_script()
    lowered = script.lower()

    assert "update.cmd is retired" in lowered
    assert "check for updates" in lowered
    assert "update all devices" in lowered
    assert "exit /b 2" in lowered

    forbidden_commands = (
        "\ngit pull",
        "\ngit fetch",
        "\ngit merge",
        "\ngit reset",
        "\ngit clean",
        "\ndocker ",
        "\npowershell ",
        "\ncall start.cmd",
    )
    for command in forbidden_commands:
        assert command not in lowered


def test_runtime_recovery_never_directs_to_retired_update_cmd() -> None:
    resolver = (
        _repository_root() / "scripts" / "windows" / "resolve_fcp_web_port.ps1"
    ).read_text(encoding="utf-8")

    assert "run update.cmd" not in resolver.lower()
    assert "set FCP_RELAY_VOLUME_NAME" in resolver
    assert "run start.cmd again" in resolver


@pytest.mark.skipif(os.name != "nt", reason="Windows batch execution regression")
def test_update_cmd_exits_without_external_dependencies(tmp_path: Path) -> None:
    root = tmp_path / "retired update fixture"
    root.mkdir()
    (root / "update.cmd").write_text(_update_script(), encoding="utf-8")

    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "update.cmd"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = completed.stdout + completed.stderr

    assert completed.returncode == 2, output
    assert "update.cmd is retired" in output
    assert "Check for updates" in output
    assert "Update all devices" in output

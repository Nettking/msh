from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _runtime_resolver() -> str:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "resolve_msh_web_port.ps1"
    ).read_text(encoding="utf-8")


def _relay_selector() -> str:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "select_msh_relay_volume.ps1"
    ).read_text(encoding="utf-8")


def test_runtime_resolver_preserves_single_project_volume_as_array() -> None:
    script = _runtime_resolver()

    assert "$values = @(\n        @(" in script
    assert "return [string]($values[0])" in script
    assert "return [string]$values[0]" not in script


def test_relay_selector_wraps_function_output_before_indexing() -> None:
    script = _relay_selector()

    assert "$candidates = @(Get-RelayCandidates)" in script
    assert "$selected = [string]($candidates[0])" in script


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell scalar/array semantics")
def test_windows_powershell_keeps_complete_single_volume_name() -> None:
    command = (
        "$values = @(@('msh_ollama_models') | Where-Object { $_ }); "
        "$selected = [string]($values[0]); "
        "if ($selected -ne 'msh_ollama_models') { "
        "Write-Error \"Selected '$selected'\"; exit 1 }; "
        "Write-Output $selected"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert completed.stdout.strip() == "msh_ollama_models"

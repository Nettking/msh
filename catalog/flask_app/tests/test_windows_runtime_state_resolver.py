from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolver_script() -> str:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "resolve_msh_web_port.ps1"
    ).read_text(encoding="utf-8")


def test_relay_probe_uses_encoded_python_across_native_boundary() -> None:
    script = _resolver_script()

    assert "PSNativeCommandUseErrorActionPreference" in script
    assert "[Convert]::ToBase64String" in script
    assert "[System.Text.Encoding]::UTF8.GetBytes($probeCode)" in script
    assert "import base64;exec(base64.b64decode('$encoded'))" in script
    assert '$arguments = @(' in script
    assert '& docker @arguments 2>$null' in script
    assert "-c $probeCode" not in script


def test_relay_selection_does_not_guess_between_ambiguous_volumes() -> None:
    script = _resolver_script()

    assert "Recovered populated Federation coordinator volume" in script
    assert "$relayCandidates.Count -eq 1" in script
    assert "$relayCandidates.Count -gt 1" in script
    assert "none could be selected safely" in script
    assert "No state was changed" in script


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell native argument check")
def test_encoded_python_survives_windows_powershell_native_argument_forwarding(
    tmp_path: Path,
) -> None:
    probe = tmp_path / "probe.ps1"
    probe.write_text(
        """$ErrorActionPreference = "Stop"
$probeCode = @'
import json
import sys
print(json.dumps({"line": 7, "node": sys.argv[1]}, sort_keys=True))
'@
$encoded = [Convert]::ToBase64String(
    [System.Text.Encoding]::UTF8.GetBytes($probeCode)
)
$launcher = "import base64;exec(base64.b64decode('$encoded'))"
$output = (& python -c $launcher "node with spaces" 2>&1) -join "`n"
if ($LASTEXITCODE -ne 0) { Write-Error $output; exit $LASTEXITCODE }
Write-Output $output
""",
        encoding="utf-8",
    )

    completed = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(probe)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    payload = json.loads(completed.stdout.strip())
    assert payload == {"line": 7, "node": "node with spaces"}

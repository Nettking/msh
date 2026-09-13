from __future__ import annotations

import json
import os
import socket
import subprocess
from pathlib import Path

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolver_script() -> str:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "resolve_fcp_web_port.ps1"
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


def test_probe_image_lookup_does_not_fail_when_images_are_absent() -> None:
    script = _resolver_script()
    section = script.split("function Find-ProbeImage", 1)[1].split(
        "function Get-RelayVolumeProbe", 1
    )[0]

    assert "docker image inspect" not in section
    assert 'docker image ls --quiet --filter "reference=$image"' in section
    assert "$LASTEXITCODE -eq 0" in section
    assert "@($matches).Count -gt 0" in section


def test_relay_selection_does_not_guess_between_ambiguous_volumes() -> None:
    script = _resolver_script()

    assert "Recovered populated Federation coordinator volume" in script
    assert "$relayCandidates.Count -eq 1" in script
    assert "$relayCandidates.Count -gt 1" in script
    assert "none could be selected safely" in script
    assert "No state was changed" in script


def test_project_volume_lookup_accepts_empty_project_name() -> None:
    script = _resolver_script()

    assert (
        '[Parameter(Mandatory = $true)][AllowEmptyString()][string]$ProjectName'
        in script
    )
    assert 'if ([string]::IsNullOrWhiteSpace($ProjectName)) {' in script


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


def test_missing_docker_volume_is_treated_as_absent_state() -> None:
    script = _resolver_script()
    section = script.split("function Get-VolumeInspection", 1)[1].split(
        "function Find-ProjectVolume", 1
    )[0]

    inspect = section.index("$raw = (& docker volume inspect $VolumeName 2>$null)")
    try_start = section.rfind("try {", 0, inspect)
    catch_start = section.index("catch {", inspect)
    null_return = section.index("return $null", catch_start)

    assert try_start != -1
    assert try_start < inspect < catch_start < null_return


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell runtime resolver")
@pytest.mark.parametrize(
    ("binding_address", "matching_port", "variant", "expected_exit"),
    [("127.0.0.1", True, "normal", 0), ("0.0.0.0", True, "normal", 0),
     ("::ffff:127.0.0.1", True, "normal", 0),
     ("127.0.0.1", True, "request-any", 0),
     ("127.0.0.2", True, "normal", 2), ("127.0.0.1", False, "normal", 2),
     ("127.0.0.1", True, "udp", 2), ("127.0.0.1", True, "non-fcp", 2),
     ("127.0.0.1", True, "ambiguous", 10)],
)
def test_existing_custom_project_is_identified_by_published_host_binding(
    tmp_path: Path, binding_address: str, matching_port: bool, variant: str,
    expected_exit: int,
) -> None:
    """The actual resolver must preserve an existing owner and reject other ports/IPs."""
    script = _repository_root() / "scripts/windows/resolve_fcp_web_port.ps1"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        inspection = [{
            "Config": {"Labels": {"com.docker.compose.service": "flask",
                "com.docker.compose.project": "fcp-custom"},
                "Env": ["FCP_SCAN_DIRS=/app/data"]},
            "Mounts": [],
            "NetworkSettings": {"Ports": {("5000/udp" if variant == "udp" else "5000/tcp"): [{
                "HostIp": binding_address, "HostPort": str(port if matching_port else 1),
            }]}},
        }]
        if variant == "non-fcp":
            inspection[0]["Config"]["Env"] = []
        inspect_path = tmp_path / "inspection.json"
        inspect_path.write_text(json.dumps(inspection), encoding="utf-8")
        mutations = tmp_path / "mutations.txt"
        output = tmp_path / "resolved.txt"
        wrapper = tmp_path / "run.ps1"
        def quote(path: Path) -> str:
            return str(path).replace("'", "''")
        wrapper.write_text(f"""$ErrorActionPreference = 'Stop'
function docker {{
    $global:LASTEXITCODE = 0
    if ($args[0] -eq 'ps') {{
        # Docker's publish filter selects container ports, not mapped host ports.
        if (($args -join ' ') -notmatch 'publish=') {{
            'owned-flask'
            {"'second-flask'" if variant == "ambiguous" else ""}
        }}
        return
    }}
    if ($args[0] -eq 'inspect') {{ Get-Content -LiteralPath '{quote(inspect_path)}' -Raw; return }}
    if ($args[0] -eq 'volume' -and $args[1] -eq 'inspect') {{
        $global:LASTEXITCODE = 1; return
    }}
    if ($args[0] -eq 'image' -and $args[1] -eq 'ls') {{ return }}
    Add-Content -LiteralPath '{quote(mutations)}' -Value ($args -join ' ')
    throw 'Unexpected Docker operation'
}}
$env:FCP_DATA_DIR = '{quote(tmp_path / 'data')}'
$env:FCP_RESULTS_DIR = '{quote(tmp_path / 'results')}'
$env:FCP_RELAY_VOLUME_NAME = 'owned-relay'
$env:FCP_OLLAMA_VOLUME_NAME = 'owned-model'
$env:FCP_MODEL_PROVIDER_VOLUME_NAME = 'owned-provider'
& '{quote(script)}' -BindAddress {"0.0.0.0" if variant == "request-any" else "127.0.0.1"} -PreferredPort {port} -CurrentProjectName fcp-custom -OutputFile '{quote(output)}'
exit $LASTEXITCODE
""", encoding="utf-8")
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(wrapper)],
            capture_output=True, text=True, timeout=30, check=False,
        )
    assert completed.returncode == expected_exit, completed.stderr or completed.stdout
    assert not mutations.exists(), "Resolving an existing installation must not stop or remove it"
    if expected_exit == 0:
        assert f"FCP_WEB_PORT={port}" in output.read_text(encoding="utf-8")
        assert "FCP_RELAY_VOLUME_NAME=owned-relay" in output.read_text(encoding="utf-8")
    else:
        assert not output.exists()


def test_start_passes_selected_project_to_both_runtime_resolver_branches() -> None:
    script = (_repository_root() / "start.cmd").read_text(encoding="utf-8")
    calls = [line for line in script.splitlines() if 'powershell ' in line and 'resolve_fcp_web_port.ps1' in line]
    assert len(calls) == 2
    assert all('-CurrentProjectName "%COMPOSE_PROJECT_NAME%"' in line for line in calls)

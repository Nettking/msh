from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _start_script() -> str:
    return (_repository_root() / "start.cmd").read_text(encoding="utf-8")


def test_windows_start_uses_bound_address_for_local_readiness() -> None:
    script = _start_script()

    assert 'set "MSH_WEB_CLIENT_HOST=%MSH_WEB_BIND%"' in script
    assert (
        'if "%MSH_WEB_CLIENT_HOST%"=="0.0.0.0" '
        'set "MSH_WEB_CLIENT_HOST=127.0.0.1"'
    ) in script
    assert (
        'set "MSH_BASE_URL=http://%MSH_WEB_CLIENT_HOST%:%MSH_WEB_PORT_RESOLVED%"'
        in script
    )
    assert 'set "MSH_BASE_URL=http://localhost:%MSH_WEB_PORT_RESOLVED%"' not in script
    assert "-Uri '%MSH_BASE_URL%/onboarding'" in script


@pytest.mark.skipif(os.name != "nt", reason="Windows cmd expansion regression")
@pytest.mark.parametrize(
    ("bind_address", "expected_host"),
    [("127.0.0.1", "127.0.0.1"), ("0.0.0.0", "127.0.0.1")],
)
def test_windows_start_expands_reachable_client_base_url(
    tmp_path: Path,
    bind_address: str,
    expected_host: str,
) -> None:
    script = _start_script()
    lines = script.splitlines()
    start = lines.index('set "MSH_WEB_CLIENT_HOST=%MSH_WEB_BIND%"')
    base_url_lines = lines[start : start + 3]

    batch_file = tmp_path / "resolve-client-base.cmd"
    batch_file.write_text(
        "@echo off\n"
        f'set "MSH_WEB_BIND={bind_address}"\n'
        'set "MSH_WEB_PORT_RESOLVED=5004"\n'
        + "\n".join(base_url_lines)
        + "\n"
        + "echo %MSH_BASE_URL%\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", str(batch_file)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert completed.stdout.strip() == f"http://{expected_host}:5004"

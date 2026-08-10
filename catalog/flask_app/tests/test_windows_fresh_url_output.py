from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _onboarding_status_line() -> str:
    script = (_repository_root() / "start.cmd").read_text(encoding="utf-8")
    return next(
        line for line in script.splitlines() if line.startswith("echo Onboarding:")
    )


def test_start_cmd_quotes_onboarding_url_in_status_output() -> None:
    assert _onboarding_status_line() == (
        'echo Onboarding:            "%FCP_ONBOARDING_URL%"'
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows cmd metacharacter regression")
def test_fresh_onboarding_query_is_printed_without_executing_reset_fragment(
    tmp_path: Path,
) -> None:
    onboarding_url = "http://localhost:5004/onboarding?fresh=1&reset=12345"
    batch_file = tmp_path / "print-onboarding-url.cmd"
    batch_file.write_text(
        "@echo off\n"
        f'set "FCP_ONBOARDING_URL={onboarding_url}"\n'
        f"{_onboarding_status_line()}\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", str(batch_file)],
        check=False,
        capture_output=True,
        text=True,
    )
    combined = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode == 0, combined
    assert onboarding_url in completed.stdout
    assert "is not recognized as an internal or external command" not in combined

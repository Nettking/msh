from __future__ import annotations

import ipaddress
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import start_tailscale_recorder as launcher


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _powershell_script() -> Path:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "start_tailscale_recorder.ps1"
    )


def test_tailscale_recorder_command_uses_same_process_secret_prompt() -> None:
    root = _repository_root()
    command = (root / "start-tailscale-recorder.cmd").read_text(encoding="utf-8")
    powershell = _powershell_script().read_text(encoding="utf-8")
    python = (root / "scripts" / "start_tailscale_recorder.py").read_text(
        encoding="utf-8"
    )

    assert "start_tailscale_recorder.ps1" in command
    assert "scripts.start_tailscale_recorder" in powershell
    assert "import start_recorder" in powershell
    assert "FCP_RECORDER_FEDERATION_KEY" not in powershell
    assert '"--require-federation"' in python
    assert '"--require-data-sharing"' in python
    assert "100.64.0.0/10" in python
    assert "getpass.getpass" in python
    assert "Machine 4 recorder - Mekanisk Service Halden" in python
    assert "tailscale up" in python
    assert "start-tailscale.cmd" not in command


def test_tailscale_preflight_requires_a_signed_in_tailnet_address(monkeypatch) -> None:
    monkeypatch.setattr(launcher.shutil, "which", lambda _name: "tailscale")
    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=["tailscale", "ip", "-4"],
            returncode=0,
            stdout="100.100.0.44\n",
            stderr="",
        ),
    )

    assert launcher._tailscale_ipv4() == ipaddress.ip_address("100.100.0.44")

    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=["tailscale", "ip", "-4"],
            returncode=0,
            stdout="192.168.1.44\n",
            stderr="",
        ),
    )
    with pytest.raises(RuntimeError, match="100.64.0.0/10"):
        launcher._tailscale_ipv4()


def test_first_start_keeps_pairing_key_out_of_arguments_and_clears_it(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.delenv("FCP_RECORDER_FEDERATION_KEY", raising=False)
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: ipaddress.ip_address("100.100.0.44"),
    )
    monkeypatch.setattr(
        launcher.sys,
        "stdin",
        SimpleNamespace(isatty=lambda: True),
    )
    monkeypatch.setattr(
        launcher.getpass,
        "getpass",
        lambda _prompt: "FCP1-private-test-key",
    )

    def fake_start(arguments: list[str]) -> int:
        captured["arguments"] = list(arguments)
        captured["key"] = os.environ.get("FCP_RECORDER_FEDERATION_KEY")
        return 0

    monkeypatch.setattr(launcher.start_recorder, "main", fake_start)

    result = launcher.main(
        [
            "--data-dir",
            str(tmp_path / "recorder-data"),
            "--storage-group",
            "fcp-local-storage",
        ]
    )

    assert result == 0
    arguments = captured["arguments"]
    assert isinstance(arguments, list)
    assert "--require-federation" in arguments
    assert "--require-data-sharing" in arguments
    assert "Machine 4 recorder - Mekanisk Service Halden" in arguments
    assert "FCP1-private-test-key" not in arguments
    assert captured["key"] == "FCP1-private-test-key"
    assert "FCP_RECORDER_FEDERATION_KEY" not in os.environ


def test_saved_membership_restarts_without_pairing_prompt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    membership = data_dir / launcher.PAIRING_STATE_RELATIVE
    membership.parent.mkdir(parents=True)
    membership.write_text("{}", encoding="utf-8")
    monkeypatch.delenv("FCP_RECORDER_FEDERATION_KEY", raising=False)
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: ipaddress.ip_address("100.100.0.44"),
    )
    monkeypatch.setattr(
        launcher.getpass,
        "getpass",
        lambda _prompt: pytest.fail("saved membership must not prompt"),
    )
    monkeypatch.setattr(launcher.start_recorder, "main", lambda _args: 0)

    assert launcher.main(["--data-dir", str(data_dir)]) == 0


def test_pairing_code_is_rejected_on_the_command_line(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: ipaddress.ip_address("100.100.0.44"),
    )

    assert launcher.main(["FCP1-do-not-put-me-in-argv"]) == 2
    assert "hidden prompt" in capsys.readouterr().err


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher execution")
def test_windows_wrapper_validates_python_and_forwards_recorder_options(
    tmp_path: Path,
) -> None:
    fake_python = tmp_path / "python.cmd"
    log_path = tmp_path / "python-invocation.txt"
    fake_python.write_text(
        "@echo off\n"
        'if "%1"=="-c" exit /b 0\n'
        '> "%FCP_TEST_LAUNCH_LOG%" echo ARGS=%*\n'
        "exit /b 0\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.update(
        {
            "FCP_RECORDER_PYTHON": str(fake_python),
            "FCP_TEST_LAUNCH_LOG": str(log_path),
        }
    )

    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(_powershell_script()),
            "--storage-group",
            "fcp-local-storage",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    logged = log_path.read_text(encoding="utf-8")
    assert "-m scripts.start_tailscale_recorder" in logged
    assert "--storage-group fcp-local-storage" in logged
    assert "FCP1-" not in logged

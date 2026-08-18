from __future__ import annotations

import ipaddress
import os
import subprocess
import sys
from pathlib import Path

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


def _command_script() -> Path:
    return _repository_root() / "start-tailscale-recorder.cmd"


def test_tailscale_recorder_command_is_zero_touch_and_keeps_secrets_out_of_shell() -> None:
    root = _repository_root()
    command = _command_script().read_text(encoding="utf-8")
    powershell = _powershell_script().read_text(encoding="utf-8")
    python = (root / "scripts" / "start_tailscale_recorder.py").read_text(
        encoding="utf-8"
    )

    assert "start_tailscale_recorder.ps1" in command
    assert "scripts.start_tailscale_recorder" in powershell
    assert "import scripts.start_tailscale_recorder as launcher" in powershell
    assert "import catalog.mtconnect_recorder as recorder" in powershell
    assert "import catalog.mtconnect_recorder.runtime" in powershell
    assert "import requests" in powershell
    assert "Remove-Item Env:FCP_RECORDER_FEDERATION_KEY" in powershell
    assert "$PairingKey" not in powershell
    assert '"--require-federation"' in python
    assert '"--require-data-sharing"' in python
    assert "100.64.0.0/10" in python
    assert "load_host_module" in python
    assert "tailnet_join_client" in python
    assert "getpass" not in python
    assert "FCP MTConnect recorder" in python
    assert "tailscale up" in python
    assert "start-tailscale.cmd" not in command
    assert "WaitForExit(10000)" in powershell
    assert "Stop-Process" in powershell
    assert "-WindowStyle Hidden" in powershell


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


def _auto_join_modules(*, code: str = "FCP1-private-test-key", joined: bool = True):
    class Discovery:
        @staticmethod
        def discover(*, web_ports):
            assert web_ports == (5000,)
            return {"federations": [{"federation_id": "fed-test"}]}

        @staticmethod
        def write_snapshot(path, payload):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text("{}", encoding="utf-8")
            assert payload["federations"]

    class Client:
        @staticmethod
        def join_discovered_federation(*, snapshot_path, grant_file):
            assert Path(snapshot_path).name == "tailscale_discovery.json"
            assert str(grant_file).endswith("auto_join_grant.json")
            return joined, "authorized" if joined else "peer-owned-by-another-user"

    class Bridge:
        GRANT_RELATIVE = "federation/onboarding/auto_join_grant.json"

        @staticmethod
        def load_grant(_path):
            return code

        @staticmethod
        def clear_grant(_path):
            return None

    return {"tailscale_host_discovery": Discovery, "tailnet_join_client": Client, "tailnet_join_bridge": Bridge}


def test_first_start_discovers_and_auto_joins_without_human_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    modules = _auto_join_modules()
    monkeypatch.delenv("FCP_RECORDER_FEDERATION_KEY", raising=False)
    monkeypatch.setattr(launcher, "load_host_module", lambda name: modules[name])

    key = launcher._pairing_key(["--data-dir", str(tmp_path / "recorder-data")])

    assert key == "FCP1-private-test-key"
    assert "FCP_RECORDER_FEDERATION_KEY" not in os.environ


def test_first_start_keeps_auto_join_key_out_of_arguments_and_clears_it(
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
    monkeypatch.setattr(launcher, "_pairing_key", lambda _args: "FCP1-private-test-key")
    monkeypatch.setattr(launcher, "_relay_url", lambda _args, _key: "ws://tail:8765")
    monkeypatch.setattr(launcher, "_require_tailnet_relay", lambda _url: None)

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
    assert "FCP MTConnect recorder" in arguments
    assert "FCP1-private-test-key" not in arguments
    assert captured["key"] == "FCP1-private-test-key"
    assert "FCP_RECORDER_FEDERATION_KEY" not in os.environ


def test_no_discovered_federation_fails_instead_of_prompting_or_starting_local(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class Discovery:
        @staticmethod
        def discover(*, web_ports):
            return {"federations": []}

        @staticmethod
        def write_snapshot(_path, _payload):
            return None

    monkeypatch.setattr(launcher, "load_host_module", lambda _name: Discovery)

    with pytest.raises(RuntimeError, match="No FCP Federation was discovered"):
        launcher._pairing_key(["--data-dir", str(tmp_path)])


def test_ambiguous_discovery_fails_closed(tmp_path: Path, monkeypatch) -> None:
    class Discovery:
        @staticmethod
        def discover(*, web_ports):
            return {"federations": [{"id": "one"}, {"id": "two"}]}

        @staticmethod
        def write_snapshot(_path, _payload):
            return None

    monkeypatch.setattr(launcher, "load_host_module", lambda _name: Discovery)

    with pytest.raises(RuntimeError, match="More than one"):
        launcher._pairing_key(["--data-dir", str(tmp_path)])


def test_identity_refusal_is_not_replaced_by_manual_pairing_prompt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    modules = _auto_join_modules(joined=False)
    monkeypatch.setattr(launcher, "load_host_module", lambda name: modules[name])

    with pytest.raises(RuntimeError, match="peer-owned-by-another-user"):
        launcher._pairing_key(["--data-dir", str(tmp_path)])


def test_saved_membership_restarts_without_discovery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    membership = data_dir / launcher.PAIRING_STATE_RELATIVE
    membership.parent.mkdir(parents=True)
    membership.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("FCP_RECORDER_FEDERATION_KEY", "FCP1-stale")
    monkeypatch.setattr(
        launcher,
        "load_host_module",
        lambda _name: pytest.fail("saved membership must not rediscover or pair"),
    )

    assert launcher._pairing_key(["--data-dir", str(data_dir)]) is None
    assert "FCP_RECORDER_FEDERATION_KEY" not in os.environ


@pytest.mark.parametrize(
    "arguments",
    [
        ["FCP1-do-not-put-me-in-argv"],
        ["--federation-key", "FCP1-do-not-put-me-in-argv"],
        ["--federation-key=FCP1-do-not-put-me-in-argv"],
        ["--federation-k=FCP1-do-not-put-me-in-argv"],
        ["  FCP1-do-not-put-me-in-argv"],
    ],
)
def test_pairing_code_is_rejected_on_the_command_line(
    arguments: list[str],
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: ipaddress.ip_address("100.100.0.44"),
    )

    assert launcher.main(arguments) == 2
    assert "joins automatically" in capsys.readouterr().err


def test_help_needs_no_tailscale_or_enrollment(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: pytest.fail("help must not inspect Tailscale"),
    )
    monkeypatch.setattr(
        launcher,
        "_pairing_key",
        lambda _args: pytest.fail("help must not request enrollment"),
    )

    assert launcher.main(["--help"]) == 0
    assert "Start the loss-aware FCP MTConnect recorder" in capsys.readouterr().out


def test_environment_data_directory_is_forwarded_consistently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path / "custom-data"))

    arguments = launcher._recorder_arguments(["--storage-group", "telemetry"])

    index = arguments.index("--data-dir")
    assert arguments[index + 1] == str(tmp_path / "custom-data")
    assert launcher._data_directory(arguments) == (tmp_path / "custom-data").resolve()


def test_last_data_directory_option_controls_membership_and_startup(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    incoming = ["--data-dir", str(first), f"--data-dir={second}"]

    arguments = launcher._recorder_arguments(incoming)

    assert launcher._data_directory(arguments) == second.resolve()
    assert arguments[-1] == f"--data-dir={second}"


@pytest.mark.parametrize("arguments", [["--data-dir"], ["--data-dir="]])
def test_invalid_data_directory_fails_before_tailscale_preflight(
    arguments: list[str],
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: pytest.fail("invalid arguments must fail before Tailscale"),
    )

    assert launcher.main(arguments) == 2
    assert "--data-dir requires" in capsys.readouterr().err


def test_abbreviated_data_directory_is_rejected_before_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        launcher,
        "_tailscale_ipv4",
        lambda: pytest.fail("invalid arguments must fail before Tailscale"),
    )

    assert launcher.main(["--data-d", "other-data"]) == 2


def test_relay_preflight_requires_literal_tailnet_ip_and_tcp_reachability(monkeypatch) -> None:
    connected: list[tuple[tuple[str, int], float]] = []
    peer_checks: list[list[str]] = []

    class Connection:
        def close(self) -> None:
            pass

    monkeypatch.setattr(launcher.shutil, "which", lambda _name: "tailscale")
    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda arguments, **_kwargs: (
            peer_checks.append(arguments)
            or subprocess.CompletedProcess(arguments, 0, "pong\n", "")
        ),
    )
    monkeypatch.setattr(
        launcher.socket,
        "create_connection",
        lambda address, timeout: (connected.append((address, timeout)) or Connection()),
    )

    launcher._require_tailnet_relay("ws://100.90.1.2:8765")
    assert connected == [(('100.90.1.2', 8765), 5.0)]
    assert peer_checks == [
        ["tailscale", "ping", "--timeout=5s", "--c=1", "100.90.1.2"]
    ]

    with pytest.raises(RuntimeError, match="literal Tailscale"):
        launcher._require_tailnet_relay("ws://leader.tailnet.ts.net:8765")
    with pytest.raises(RuntimeError, match="not a Tailscale"):
        launcher._require_tailnet_relay("ws://192.168.1.2:8765")

    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda arguments, **_kwargs: subprocess.CompletedProcess(
            arguments, 1, "", "no reply"
        ),
    )
    with pytest.raises(RuntimeError, match="not a reachable peer"):
        launcher._require_tailnet_relay("ws://100.90.1.3:8765")


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher execution")
def test_windows_probe_rejects_runtime_that_cannot_import_exact_entrypoint(
    tmp_path: Path,
) -> None:
    blocker_directory = tmp_path / "blocked-runtime"
    blocker_directory.mkdir()
    (blocker_directory / "sitecustomize.py").write_text(
        "import importlib.abc\n"
        "import sys\n\n"
        "class ExactEntrypointBlocker(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path, target=None):\n"
        "        if fullname == 'scripts.start_tailscale_recorder':\n"
        "            raise ModuleNotFoundError('blocked exact recorder entrypoint')\n"
        "        return None\n\n"
        "sys.meta_path.insert(0, ExactEntrypointBlocker())\n",
        encoding="utf-8",
    )
    blocked_python = tmp_path / "blocked-python.cmd"
    invocation_log = tmp_path / "blocked-python-invocations.txt"
    blocked_python.write_text(
        "@echo off\n"
        "setlocal\n"
        '>> "%FCP_TEST_BLOCKED_RUNTIME_LOG%" echo ARGS=%*\n'
        'set "PYTHONPATH=%FCP_TEST_BLOCKER_DIRECTORY%"\n'
        '"%FCP_TEST_REAL_PYTHON%" %*\n'
        "exit /b %ERRORLEVEL%\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.pop("FCP_RECORDER_FEDERATION_KEY", None)
    environment.update(
        {
            "FCP_RECORDER_PYTHON": str(blocked_python),
            "FCP_TEST_BLOCKED_RUNTIME_LOG": str(invocation_log),
            "FCP_TEST_BLOCKER_DIRECTORY": str(blocker_directory),
            "FCP_TEST_REAL_PYTHON": sys.executable,
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
            "--help",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    invocations = invocation_log.read_text(encoding="utf-8").splitlines()
    assert len(invocations) == 1
    assert "ARGS=-c" in invocations[0]
    assert "import scripts.start_tailscale_recorder as launcher" in invocations[0]
    assert "-m scripts.start_tailscale_recorder" not in invocations[0]


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher execution")
def test_windows_wrapper_scrubs_legacy_secret_and_forwards_recorder_options(
    tmp_path: Path,
) -> None:
    fake_python = tmp_path / "python.cmd"
    log_path = tmp_path / "python-invocation.txt"
    probe_key_path = tmp_path / "probe-key.txt"
    final_key_path = tmp_path / "final-key.txt"
    fake_python.write_text(
        "@echo off\n"
        'if "%1"=="-c" (\n'
        '  > "%FCP_TEST_PROBE_KEY_LOG%" echo KEY=%FCP_RECORDER_FEDERATION_KEY%\n'
        "  exit /b 0\n"
        ")\n"
        '> "%FCP_TEST_LAUNCH_LOG%" echo ARGS=%*\n'
        '> "%FCP_TEST_FINAL_KEY_LOG%" echo KEY=%FCP_RECORDER_FEDERATION_KEY%\n'
        "exit /b 0\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.update(
        {
            "FCP_RECORDER_PYTHON": str(fake_python),
            "FCP_TEST_LAUNCH_LOG": str(log_path),
            "FCP_TEST_PROBE_KEY_LOG": str(probe_key_path),
            "FCP_TEST_FINAL_KEY_LOG": str(final_key_path),
            "FCP_RECORDER_FEDERATION_KEY": "FCP1-private-probe-test",
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
    assert probe_key_path.read_text(encoding="utf-8").strip() == "KEY="
    assert final_key_path.read_text(encoding="utf-8").strip() == "KEY="


@pytest.mark.skipif(os.name != "nt", reason="Windows launcher execution")
def test_root_cmd_preserves_spaced_argument_and_nonzero_exit(tmp_path: Path) -> None:
    fake_directory = tmp_path / "python with spaces"
    fake_directory.mkdir()
    fake_python = fake_directory / "python.cmd"
    log_path = tmp_path / "cmd-invocation.txt"
    fake_python.write_text(
        "@echo off\n"
        'if "%1"=="-c" exit /b 0\n'
        '> "%FCP_TEST_LAUNCH_LOG%" echo ARGS=%*\n'
        "exit /b 7\n",
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.pop("FCP_RECORDER_FEDERATION_KEY", None)
    environment.update(
        {
            "FCP_RECORDER_PYTHON": str(fake_python),
            "FCP_TEST_LAUNCH_LOG": str(log_path),
        }
    )

    completed = subprocess.run(
        [
            "cmd.exe",
            "/d",
            "/c",
            "call",
            str(_command_script()),
            "--device-name",
            "Maskin fire test",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert completed.returncode == 7, completed.stderr or completed.stdout
    logged = log_path.read_text(encoding="utf-8")
    assert "--device-name" in logged
    assert "Maskin fire test" in logged
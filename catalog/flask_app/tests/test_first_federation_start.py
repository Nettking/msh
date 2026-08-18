from __future__ import annotations

import builtins
from pathlib import Path
from types import SimpleNamespace

from scripts import first_federation_start as first
from scripts import zero_touch_federation_start as zero_touch


ROOT = Path(__file__).resolve().parents[3]


def _completed_result() -> dict[str, object]:
    return {
        "federation_id": "fed-a",
        "node_id": "node-a",
        "capability": {
            "startup_completed": True,
            "benchmarks_run": 2,
            "contributions_enabled": 1,
        },
    }


def test_first_device_collects_reset_and_admin_before_long_startup(monkeypatch) -> None:
    events: list[object] = []
    answers = iter(["RESET", "admin@example.com"])

    monkeypatch.setattr(first.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(
        builtins,
        "input",
        lambda prompt: events.append(("input", prompt)) or next(answers),
    )
    monkeypatch.setattr(
        first.getpass,
        "getpass",
        lambda prompt: events.append(("password", prompt)) or "private-password-123",
    )
    for name, value in {
        "FCP_TAILSCALE_IP": "100.80.20.75",
        "FCP_TAILSCALE_DISCOVERY_PORT": "5000",
        "FCP_AUTO_JOIN_PORT": "5151",
        "FCP_AUTO_JOIN_APP_URL": "http://100.80.20.75:5000",
    }.items():
        monkeypatch.setenv(name, value)

    monkeypatch.setattr(first, "_run_fresh_reset", lambda: events.append("fresh-start"))
    monkeypatch.setattr(first, "_check_responder", lambda: events.append("preflight"))
    monkeypatch.setattr(
        first,
        "_ensure_windows_firewall",
        lambda port: events.append(("firewall", port)),
    )
    monkeypatch.setattr(
        first,
        "_start_responder",
        lambda **kwargs: events.append(("responder", kwargs)),
    )

    def bootstrap(**kwargs):
        events.append(("initialize", kwargs))
        return _completed_result()

    monkeypatch.setattr(first.zero_touch, "zero_touch_start", bootstrap)

    result = first.first_federation_start(open_browser=False)

    assert result["federation_id"] == "fed-a"
    fresh_index = events.index("fresh-start")
    assert events.index(("input", "Type RESET to continue: ")) < fresh_index
    assert events.index(("input", "Federation administrator email: ")) < fresh_index
    assert events.index(("password", "Federation administrator password: ")) < fresh_index
    initialize = next(value for value in events if isinstance(value, tuple) and value[0] == "initialize")
    assert initialize[1]["first_admin_credentials"] == (
        "admin@example.com",
        "private-password-123",
    )


def test_fresh_start_subprocess_receives_only_reset_not_admin_credentials(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(first.subprocess, "run", fake_run)

    first._run_fresh_reset()

    assert len(calls) == 1
    assert calls[0]["input"] == "RESET\n"
    encoded = repr(calls[0])
    assert "admin@example.com" not in encoded
    assert "private-password" not in encoded


def test_windows_fresh_start_uses_cmd_call_without_nested_quote_string(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(first.os, "name", "nt")
    monkeypatch.setenv("COMSPEC", r"C:\Windows\System32\cmd.exe")
    monkeypatch.setattr(first.subprocess, "run", fake_run)

    first._run_fresh_reset()

    assert len(calls) == 1
    command = calls[0]["command"]
    assert command == [
        r"C:\Windows\System32\cmd.exe",
        "/d",
        "/c",
        "call",
        str(first.ROOT / "start.cmd"),
        "--fresh",
    ]
    assert "/s" not in command
    assert not str(command[4]).startswith('"')
    assert calls[0]["input"] == "RESET\n"


def test_zero_touch_initializer_passes_both_human_credentials_only_on_stdin(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def inside(module, *arguments, input_text=None):
        captured["module"] = module
        captured["arguments"] = arguments
        captured["input_text"] = input_text
        return "{}"

    monkeypatch.setattr(zero_touch, "_inside_flask", inside)

    zero_touch._initialize_first_federation(("admin@example.com", "private-password-123"))

    assert captured["arguments"] == ("--json", "initialize")
    assert captured["input_text"] == "admin@example.com\nprivate-password-123\n"
    assert "--email" not in captured["arguments"]
    assert "--password" not in captured["arguments"]


def test_windows_fresh_start_repairs_only_missing_git_tracked_scaffolding() -> None:
    script = (ROOT / "start.cmd").read_text(encoding="utf-8")
    main_body = script.split("\n:repair_checkout_scaffolding", maxsplit=1)[0]
    repair = script.split("\n:repair_checkout_scaffolding", maxsplit=1)[1].split(
        "\n:resolve_build_commit", maxsplit=1
    )[0]

    assert main_body.count("call :repair_checkout_scaffolding") == 2
    assert main_body.index("call :repair_checkout_scaffolding") < main_body.index(
        "call :resolve_runtime_state"
    )
    assert main_body.rindex("call :repair_checkout_scaffolding") > main_body.index(
        "call :reset_device_state"
    )
    for path in (
        "data\\.gitkeep",
        "data\\README.md",
        "results\\.gitkeep",
        "results\\README.md",
    ):
        assert path in repair
    assert 'git restore --source=HEAD --worktree -- "%%~F"' in repair
    assert "git reset --hard" not in repair
    assert "git clean" not in repair


def test_first_device_launchers_delegate_to_memory_only_host_orchestrator() -> None:
    windows = (ROOT / "start-tailscale.cmd").read_text(encoding="utf-8")
    posix = (ROOT / "start-tailscale.sh").read_text(encoding="utf-8")
    orchestrator = (ROOT / "scripts" / "first_federation_start.py").read_text(
        encoding="utf-8"
    )

    assert "scripts\\first_federation_start.py" in windows
    assert "scripts/first_federation_start.py" in posix
    assert "getpass.getpass" in orchestrator
    assert 'input="RESET\\n"' in orchestrator
    assert "first_admin_credentials=(email, password)" in orchestrator
    assert "--password" not in orchestrator
    assert "--email" not in orchestrator
    assert "FCP_ADMIN_PASSWORD" not in orchestrator
    assert "write_text" not in orchestrator

from __future__ import annotations

import builtins
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import first_federation_start as first


ROOT = Path(__file__).resolve().parents[3]


def _completed_result() -> dict[str, object]:
    return {
        "federation_id": "fed-a",
        "node_id": "node-a",
        "capability": {
            "startup_completed": True,
            "benchmarks_run": 1,
            "contributions_enabled": 1,
        },
    }


def test_internal_fresh_start_suppresses_browser_without_mutating_parent_env(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.delenv("FCP_SUPPRESS_BROWSER", raising=False)

    def fake_run(command, **kwargs):
        calls.append({"command": command, **kwargs})
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(first.subprocess, "run", fake_run)

    first._run_fresh_reset()

    assert len(calls) == 1
    child_env = calls[0]["env"]
    assert isinstance(child_env, dict)
    assert child_env["FCP_SUPPRESS_BROWSER"] == "1"
    assert "FCP_SUPPRESS_BROWSER" not in first.os.environ
    assert calls[0]["input"] == "RESET\n"


def test_final_browser_open_happens_after_admin_and_federation_initialization(monkeypatch) -> None:
    events: list[object] = []
    answers = iter(["RESET", "admin@example.com"])

    monkeypatch.setattr(first.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(builtins, "input", lambda prompt: next(answers))
    monkeypatch.setattr(first.getpass, "getpass", lambda prompt: "private-password-123")
    for name, value in {
        "FCP_TAILSCALE_IP": "100.80.20.75",
        "FCP_TAILSCALE_DISCOVERY_PORT": "5000",
        "FCP_AUTO_JOIN_PORT": "5151",
        "FCP_AUTO_JOIN_APP_URL": "http://100.80.20.75:5000",
    }.items():
        monkeypatch.setenv(name, value)

    monkeypatch.setattr(first, "_run_fresh_reset", lambda: events.append("fresh-start"))
    monkeypatch.setattr(first, "_check_responder", lambda: events.append("preflight"))
    monkeypatch.setattr(first, "_ensure_windows_firewall", lambda port: None)
    monkeypatch.setattr(first, "_start_responder", lambda **kwargs: None)

    def bootstrap(**kwargs):
        events.append(("initialize", kwargs["first_admin_credentials"]))
        return _completed_result()

    monkeypatch.setattr(first.zero_touch, "zero_touch_start", bootstrap)
    monkeypatch.setattr(first.webbrowser, "open", lambda url: events.append(("browser", url)))

    first.first_federation_start(open_browser=True)

    initialize_index = events.index(("initialize", ("admin@example.com", "private-password-123")))
    browser_event = ("browser", "http://100.80.20.75:5000/federation")
    assert browser_event in events
    assert initialize_index < events.index(browser_event)


def test_windows_start_has_explicit_internal_browser_suppression_contract() -> None:
    script = (ROOT / "start.cmd").read_text(encoding="utf-8")
    condition = 'if /I "%FCP_SUPPRESS_BROWSER%"=="1" ('
    deferred = "Browser opening deferred to the calling startup orchestrator."
    browser = 'start "" "%FCP_OPEN_URL%"'

    assert condition in script
    assert deferred in script
    assert browser in script
    assert script.index(condition) < script.index(browser)


@pytest.mark.skipif(first.os.name != "nt", reason="requires real Windows cmd.exe")
def test_windows_suppression_block_does_not_launch_premature_target(tmp_path: Path) -> None:
    source = (ROOT / "start.cmd").read_text(encoding="utf-8")
    start_marker = 'if /I "%FCP_SUPPRESS_BROWSER%"=="1" ('
    tail_marker = "\nexit /b 0\n\n:repair_checkout_scaffolding"
    decision = source.split(start_marker, maxsplit=1)[1].split(tail_marker, maxsplit=1)[0]
    decision = start_marker + decision

    launched = tmp_path / "premature-browser-launched.txt"
    trap = tmp_path / "premature-browser-target.cmd"
    trap.write_text(
        "@echo off\n"
        f'>"{launched}" echo launched\n'
        "exit /b 0\n",
        encoding="utf-8",
    )
    wrapper = tmp_path / "browser-decision.cmd"
    wrapper.write_text(
        "@echo off\n"
        "setlocal EnableExtensions\n"
        'set "FCP_SUPPRESS_BROWSER=1"\n'
        f'set "FCP_OPEN_URL={trap}"\n'
        + decision
        + "\nexit /b 0\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [first.os.environ.get("COMSPEC") or "cmd.exe", "/d", "/c", "call", str(wrapper)],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    time.sleep(0.5)

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "Browser opening deferred to the calling startup orchestrator." in completed.stdout
    assert not launched.exists()

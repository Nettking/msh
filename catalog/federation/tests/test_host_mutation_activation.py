from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_build, host_mutation

ROOT = Path(__file__).resolve().parents[3]


def test_posix_launcher_enters_one_lease_before_build_and_activation() -> None:
    text = (ROOT / "start.sh").read_text(encoding="utf-8")

    handoff = text.index(
        'if [ "${FCP_HOST_MUTATION_LEASE_ACTIVE:-}" != "1" ]; then'
    )
    leased_body = text.index('if [ "$MODE" = fresh ]; then', handoff)
    build = text.index("build_core_images", leased_body)
    required = text.index("docker compose up -d relay recorder", leased_body)
    flask = text.index("docker compose up -d flask", leased_body)
    ollama = text.index("docker compose up -d ollama", leased_body)
    readiness = text.index("docker compose exec -T flask", leased_body)
    compose_ps = text.index("docker compose ps relay ollama flask recorder", leased_body)
    agent = text.index("scripts/posix/fcp_update_agent.py", leased_body)

    assert handoff < leased_body <= build < required < flask < ollama
    assert ollama < readiness < compose_ps < agent
    assert "--lease-already-held" in text
    assert "exec python3 -m catalog.federation.host_mutation" in text
    assert "FCP_HOST_MUTATION_LEASE_ACTIVE= FCP_HOST_MUTATION_LEASE_OWNER_PID=" in text


def test_windows_launcher_enters_same_mutex_before_checkout_and_compose_reads() -> None:
    start = (ROOT / "start.cmd").read_text(encoding="utf-8")
    lease = (ROOT / "scripts/windows/fcp_host_activation_lease.ps1").read_text(
        encoding="utf-8"
    )
    build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(
        encoding="utf-8"
    )

    handoff = start.index(
        'if "%FCP_HOST_MUTATION_LEASE_ACTIVE%"=="1" goto :host_mutation_lease_ready'
    )
    leased_body = start.index("\n:host_mutation_lease_ready\n", handoff)
    repair = start.index("call :repair_checkout_scaffolding", leased_body)
    resolve = start.index("call :resolve_runtime_state", leased_body)
    host_build = start.index("call :resolve_build_commit", leased_body)
    required = start.index("docker compose up -d relay recorder", leased_body)
    readiness = start.index("Invoke-WebRequest", leased_body)
    agent = start.index("call :start_update_agent", leased_body)

    assert handoff < leased_body < repair < resolve < host_build < required < readiness < agent
    assert "fcp_host_activation_lease.ps1" in start
    assert "-LeaseAlreadyHeld" in start
    assert "[switch]$LeaseAlreadyHeld" in build
    assert "host_mutation_lease_missing" in build

    mutex = "$mutexName = 'Global\\FCPHostMutation-' + (Get-PathHash $RepoRoot)"
    assert mutex in lease
    assert lease.index("WaitOne") < lease.index("& $env:ComSpec") < lease.index(
        "ReleaseMutex"
    )


def test_launcher_owned_build_mode_fails_closed_without_the_lease_marker(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="host_mutation_lease_missing"):
        host_build.build_core_images(
            tmp_path,
            env={},
            lease_already_held=True,
        )


def test_launcher_owned_build_reuses_parent_lease_without_reacquiring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    @contextmanager
    def forbidden_lock(*_args, **_kwargs):
        raise AssertionError("the child build must not deadlock on its parent lease")
        yield  # pragma: no cover

    monkeypatch.setattr(host_build, "host_mutation_lock", forbidden_lock)
    monkeypatch.setattr(
        host_build,
        "build_core_images_locked",
        lambda *_args, **_kwargs: calls.append("build") or "a" * 40,
    )

    commit = host_build.build_core_images(
        tmp_path,
        env={host_build.HOST_MUTATION_LEASE_ENV: "1"},
        lease_already_held=True,
    )

    assert commit == "a" * 40
    assert calls == ["build"]


def test_posix_lease_process_holds_lock_until_child_launcher_returns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def lock(root: Path, *, timeout_seconds: float):
        assert root == tmp_path.resolve()
        assert timeout_seconds == host_mutation.HOST_MUTATION_LOCK_TIMEOUT_SECONDS
        events.append("lock_enter")
        try:
            yield
        finally:
            events.append("lock_exit")

    def run(command, *, cwd, env, shell, check):  # type: ignore[no-untyped-def]
        assert command == ["sh", "start.sh"]
        assert cwd == tmp_path.resolve()
        assert env[host_mutation.HOST_MUTATION_LEASE_ENV] == "1"
        assert env[host_mutation.HOST_MUTATION_LEASE_OWNER_ENV] == str(os.getpid())
        assert shell is False
        assert check is False
        events.append("child")
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(host_mutation, "host_mutation_lock", lock)
    monkeypatch.setattr(host_mutation.subprocess, "run", run)

    result = host_mutation.run_command_under_host_mutation_lock(
        tmp_path,
        ["sh", "start.sh"],
    )

    assert result == 7
    assert events == ["lock_enter", "child", "lock_exit"]


def test_background_update_agents_do_not_inherit_the_internal_lease_marker() -> None:
    posix = (ROOT / "start.sh").read_text(encoding="utf-8")
    windows = (ROOT / "start.cmd").read_text(encoding="utf-8")

    assert (
        "FCP_HOST_MUTATION_LEASE_ACTIVE= FCP_HOST_MUTATION_LEASE_OWNER_PID=" in posix
    )
    start_agent = windows.split("\n:start_update_agent", maxsplit=1)[1].split(
        "\n:resolve_runtime_state", maxsplit=1
    )[0]
    assert 'set "FCP_HOST_MUTATION_LEASE_ACTIVE="' in start_agent
    assert 'set "FCP_HOST_MUTATION_LEASE_OWNER_PID="' in start_agent
    assert start_agent.index('set "FCP_HOST_MUTATION_LEASE_ACTIVE="') < start_agent.index(
        'start "FCP Update Agent"'
    )
    assert start_agent.index('start "FCP Update Agent"') < start_agent.index(
        'set "FCP_HOST_MUTATION_LEASE_ACTIVE=%FCP_LEASE_ACTIVE_SAVED%"'
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser check")
def test_windows_activation_lease_has_valid_powershell_syntax() -> None:
    path = ROOT / "scripts/windows/fcp_host_activation_lease.ps1"
    escaped_path = str(path).replace("'", "''")
    command = (
        "$errors = $null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_path}', [ref]$null, [ref]$errors) | Out-Null; "
        "if ($errors.Count -gt 0) { "
        "$errors | ForEach-Object { Write-Error $_.Message }; exit 1 }"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout

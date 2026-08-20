"""Fixed safety boundaries of the native recorder supervisor and host agent.

The Windows supervisor cannot be executed on a Linux CI runner, and the parts of
it that matter most are absences: no force-kill, no Compose, no history rewrite,
no peer-supplied process shape. This repository already pins host-updater
boundaries by inspecting their source, and the same approach is used here so a
future edit that reintroduces one of those is caught on every platform.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

SUPERVISOR = ROOT / "scripts/windows/fcp_recorder_supervisor.ps1"
LAUNCHER = ROOT / "scripts/windows/start_tailscale_recorder.ps1"
COMMAND = ROOT / "start-tailscale-recorder.cmd"
AGENT = ROOT / "scripts/fcp_native_recorder_update_agent.py"
LIFECYCLE = ROOT / "catalog/mtconnect_recorder/native_update_agent.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# the supported operator command still reaches a supervised recorder
# --------------------------------------------------------------------------


def test_the_operator_command_is_unchanged_and_reaches_the_supervisor() -> None:
    """Operators must not have to start a separate updater after bootstrap."""

    command = _text(COMMAND)
    assert "scripts\\windows\\start_tailscale_recorder.ps1" in command

    launcher = _text(LAUNCHER)
    assert "fcp_recorder_supervisor.ps1" in launcher
    assert "-PythonExecutable $PythonCommand.Executable" in launcher
    assert "-PythonPrefix $PythonCommand.Prefix" in launcher
    # The interpreter is still resolved and probed locally before anything runs.
    assert "Resolve-RecorderPython" in launcher
    assert "Test-RecorderPython" in launcher


def test_the_launcher_refuses_to_run_without_the_supervisor() -> None:
    launcher = _text(LAUNCHER)
    index = launcher.index("$SupervisorScript")
    assert "Stop-RecorderLaunch" in launcher[index : index + 800]
    assert launcher.index("$SupervisorScript") < launcher.index("Push-Location")


# --------------------------------------------------------------------------
# process lifecycle
# --------------------------------------------------------------------------


def test_the_supervisor_never_force_kills_the_recorder() -> None:
    supervisor = _text(SUPERVISOR)

    assert "Stop-Process" not in supervisor
    assert "-Force -ErrorAction SilentlyContinue" not in supervisor.replace(
        "Remove-Item -LiteralPath $AgentStopFile -Force -ErrorAction SilentlyContinue",
        "",
    )
    assert "taskkill" not in supervisor.lower()
    assert "Kill()" not in supervisor
    assert ".Terminate" not in supervisor
    # The agent is asked to stop through a file and is waited for, never killed.
    assert "New-Item -ItemType File -Path $AgentStopFile" in supervisor
    assert "$Agent.WaitForExit(" in supervisor


def test_the_supervisor_guarantees_one_recorder_per_checkout() -> None:
    supervisor = _text(SUPERVISOR)

    assert "'Global\\FCPNativeRecorderSupervisor-' + (Get-PathHash $RepoRoot)" in supervisor
    assert "if (-not $createdNew)" in supervisor
    assert "exit 3" in supervisor
    # Exactly one child is started per loop iteration.
    assert supervisor.count("Start-Recorder $nonce $buildCommit") == 1


def test_every_child_gets_a_fresh_process_instance_nonce() -> None:
    supervisor = _text(SUPERVISOR)

    assert "$nonce = New-Nonce" in supervisor
    assert "$SupervisorSession = New-Nonce" in supervisor
    # The session is generated once; the nonce is generated inside the loop.
    assert supervisor.index("$SupervisorSession = New-Nonce") < supervisor.index(
        "while ($true)"
    )
    assert supervisor.index("while ($true)") < supervisor.index("$nonce = New-Nonce")


def test_only_local_supervisor_identity_crosses_into_the_child() -> None:
    supervisor = _text(SUPERVISOR)

    assert "$env:FCP_RECORDER_SUPERVISOR_SESSION = $SupervisorSession" in supervisor
    assert "$env:FCP_RECORDER_PROCESS_NONCE = $Nonce" in supervisor
    assert "$env:FCP_RECORDER_BUILD_COMMIT = $BuildCommit" in supervisor
    # Nothing a peer could name is ever turned into a process shape.
    for forbidden in (
        "Invoke-Expression",
        "iex ",
        "$request.",
        "-EncodedCommand",
        "FromBase64String",
        "Start-Job",
    ):
        assert forbidden not in supervisor


def test_recorder_arguments_are_never_re_parsed_by_the_supervisor() -> None:
    """A second PowerShell binding layer could silently rewrite ``--data-dir``."""

    supervisor = _text(SUPERVISOR)
    launcher = _text(LAUNCHER)

    # The launcher hands the arguments over as one explicit array value.
    assert "-RecorderArguments $RecorderArguments" in launcher
    # The supervisor accepts them as a plain named parameter. Remaining-argument
    # binding here would re-parse every "--option" the operator passed.
    parameters = supervisor[: supervisor.index(")\n")]
    assert "[string[]]$RecorderArguments" in parameters
    assert "ValueFromRemainingArguments" not in parameters
    # Quoting is applied only where Start-Process needs it; the call operator
    # quotes for itself, so double-quoting there would corrupt the arguments.
    assert "ConvertTo-ProcessArgument" in supervisor
    assert supervisor.count("ConvertTo-ProcessArgument $RecorderArguments") == 0


def test_the_relaunch_profile_is_the_one_the_operator_started_with() -> None:
    supervisor = _text(SUPERVISOR)

    assert "-m scripts.start_tailscale_recorder @RecorderArguments" in supervisor
    # The interpreter and arguments are parameters resolved by the launcher, and
    # are reused verbatim; the supervisor never re-resolves or rewrites them.
    assert "[string]$PythonExecutable" in supervisor
    assert "[string[]]$RecorderArguments" in supervisor
    assert "Get-Command python" not in supervisor
    assert "$env:PATH" not in supervisor


# --------------------------------------------------------------------------
# stop before mutate
# --------------------------------------------------------------------------


def test_the_supervisor_updates_source_only_after_the_child_exits() -> None:
    supervisor = _text(SUPERVISOR)

    exited = supervisor.index("$exitCode = Start-Recorder $nonce $buildCommit")
    finalize = supervisor.index("$finalize = Invoke-Finalize")
    relaunch = supervisor.index("$replacementPending = $true")
    assert exited < finalize < relaunch
    # The supervisor itself owns no Git mutation; finalize is the only path.
    assert "merge" not in supervisor
    assert "Invoke-Git" not in supervisor


def test_the_supervisor_relaunches_only_for_the_approved_exit_code() -> None:
    supervisor = _text(SUPERVISOR)

    assert "$ApprovedUpdateRestartExitCode = 75" in supervisor
    assert "if ($exitCode -ne $ApprovedUpdateRestartExitCode) {" in supervisor
    # Any other exit code, including 0 and Ctrl+C, ends supervision.
    assert "exit $exitCode" in supervisor


def test_a_failed_finalize_never_relaunches() -> None:
    supervisor = _text(SUPERVISOR)

    failure = supervisor.index("The approved recorder update did not complete")
    relaunch = supervisor.index("$replacementPending = $true")
    assert failure < relaunch
    assert "exit 5" in supervisor
    # An agent that did not finish is also a refusal, not a relaunch.
    assert "if (-not $agentStopped) {" in supervisor
    assert "exit 4" in supervisor


def test_the_supervisor_records_the_replacement_before_starting_it() -> None:
    supervisor = _text(SUPERVISOR)

    marked = supervisor.index("Set-RelaunchedNonce $nonce")
    started = supervisor.index("$exitCode = Start-Recorder $nonce $buildCommit")
    assert marked < started


# --------------------------------------------------------------------------
# the native path is never the Compose path
# --------------------------------------------------------------------------


def test_no_native_recorder_component_builds_or_starts_compose() -> None:
    for path in (SUPERVISOR, AGENT, LIFECYCLE, LAUNCHER):
        text = _text(path)
        for forbidden in (
            "docker",
            "compose",
            "FCP_BUILD_COMMIT",
            "COMPOSE_PROJECT_NAME",
        ):
            assert forbidden not in text, f"{path.name} must not reference {forbidden}"


def test_no_native_recorder_component_rewrites_history() -> None:
    for path in (SUPERVISOR, AGENT, LIFECYCLE):
        text = _text(path)
        for forbidden in (
            "reset --hard",
            "git clean",
            "git stash",
            "--force",
            "push",
            "rebase",
        ):
            assert forbidden not in text, f"{path.name} must not use {forbidden}"


def test_the_lifecycle_keeps_its_fixed_source_and_mutation_boundary() -> None:
    text = _text(LIFECYCLE)

    assert "APPROVED_REPOSITORY" in text
    assert "APPROVED_BRANCH" in text
    assert 'raise UpdateRefused("unapproved_source"' in text
    # The single mutation is delegated to the shared adapter's ff-only apply.
    assert "self.adapter.apply(target)" in text
    assert text.count("self.adapter.apply(") == 1
    assert "recorder_still_running" in text
    assert "dependency_change_requires_manual_update" in text
    assert "activation_superseded" in text


def test_the_agent_entry_point_takes_no_peer_supplied_process_shape() -> None:
    text = _text(AGENT)

    assert "--repo-root" in text
    assert "--supervisor-session" in text
    for forbidden in (
        "subprocess",
        "os.system",
        "shell=True",
        "Popen",
        "os.execv",
        "eval(",
        "exec(",
    ):
        assert forbidden not in text


def test_the_agent_allows_one_instance_per_recorder() -> None:
    text = _text(AGENT)

    assert "_claim_single_instance" in text
    assert "process_is_running(holder)" in text
    assert "agent.lock" in text


def test_the_windows_process_probe_never_signals() -> None:
    """``os.kill(pid, 0)`` is not a supported Windows existence check."""

    text = _text(ROOT / "catalog/mtconnect_recorder/native_update.py")

    assert "OpenProcess" in text
    assert "GetExitCodeProcess" in text
    assert "CloseHandle" in text
    windows_branch = text[text.index('if os.name == "nt"') : text.index("try:\n        os.kill")]
    assert "os.kill" not in windows_branch
    assert "PROCESS_TERMINATE" not in text
    assert "TerminateProcess" not in text

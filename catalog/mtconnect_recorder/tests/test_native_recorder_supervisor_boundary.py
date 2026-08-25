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
    assert "taskkill" not in supervisor.lower()
    assert "Kill()" not in supervisor
    assert ".Terminate" not in supervisor
    # The recorder is run synchronously and simply allowed to finish.
    assert "$exitCode = Start-Recorder $nonce $buildCommit" in supervisor


def test_the_supervisor_starts_nothing_before_the_recorder() -> None:
    """The supported startup path must stay exactly one child process.

    An earlier revision started a background agent and ran a Python probe to
    resolve the data directory before launching the recorder. Both collided
    with the launcher's own interpreter contract -- the probe shares the ``-c``
    import-probe shape the launcher already uses -- and the existing Windows
    launcher regressions caught it. Nothing may run before the child again.
    """

    supervisor = _text(SUPERVISOR)

    assert "Start-Job" not in supervisor
    # No import-probe shape: the launcher already owns the only "-c" call, and a
    # second one collides with the fake interpreters the launcher tests use.
    assert "'-c'" not in supervisor

    # The one background process this file may start is the branch-trial
    # watchdog, and it is unreachable on the supported startup path: both flags
    # guarding it initialize false, so the first child is still the first thing
    # started.
    assert supervisor.count("Start-Process") == 1
    watchdog = supervisor.index("Start-Process")
    assert supervisor.rindex("function Start-TrialWatchdog {") < watchdog
    assert "$replacementPending = $false\n" in supervisor
    assert "$childOwner = $OrdinaryChild\n" in supervisor

    loop = supervisor[supervisor.index("while ($true) {") :]
    launch = loop.index("$exitCode = Start-Recorder")
    # Inside the loop, the only interpreter call reachable before the child is
    # the relaunch bookkeeping, and that runs solely after an approved update
    # or a trial the agent planned.
    assert loop.index("Invoke-Finalize") > launch
    assert loop.count("Set-RelaunchedNonce $nonce") == 1
    assert loop.index("Set-RelaunchedNonce $nonce") < launch
    assert "if ($replacementPending) {" in loop
    guarded = loop[loop.index("if ($replacementPending) {") : launch]
    # The watchdog is still started for a trial child and nothing else -- a
    # rollback child is transition-owned but gets no trial watchdog.
    assert "if ($childOwner -eq $TrialChild) {" in guarded
    assert guarded.index("if ($childOwner -eq $TrialChild) {") < guarded.index(
        "Start-TrialWatchdog"
    )


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
    # They reach the replacement verbatim, and reach the agent only as opaque
    # values it uses to resolve the data directory the launcher would.
    assert "-m scripts.start_tailscale_recorder @arguments" in supervisor
    assert "$arguments = @($RecorderArguments)" in supervisor
    # The only thing ever added to them is a host-resolved data directory for a
    # trial child launched from another root -- and it is the *same* directory
    # the launcher would have resolved, appended rather than re-parsed.
    added = [
        line.strip() for line in supervisor.splitlines() if "$arguments +=" in line
    ]
    assert added == ["$arguments += @('--data-dir', $DataDirectory)"]
    assert "('--recorder-arg=' + $argument)" in supervisor


def test_the_relaunch_profile_is_the_one_the_operator_started_with() -> None:
    supervisor = _text(SUPERVISOR)

    assert "-m scripts.start_tailscale_recorder @arguments" in supervisor
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
    """The *update* relaunch stays keyed to exactly one exit code.

    Ordinary restart is a separate decision with its own state machine below.
    This test pins the boundary between them: reaching the update path still
    requires the approved code or an active trial, and nothing else.
    """

    supervisor = _text(SUPERVISOR)

    assert "$ApprovedUpdateRestartExitCode = 75" in supervisor
    assert (
        "if ($exitCode -ne $ApprovedUpdateRestartExitCode -and $childOwner -eq $OrdinaryChild) {"
        in supervisor
    )
    # Everything that is neither the approved code nor a trial is handled inside
    # that branch, so the update path below it is unreachable for an ordinary
    # exit -- the finalize/plan sequence still runs only for 75 or a trial.
    loop = supervisor[supervisor.index("while ($true) {") :]
    ordinary = loop.index(
        "if ($exitCode -ne $ApprovedUpdateRestartExitCode -and $childOwner -eq $OrdinaryChild) {"
    )
    assert ordinary < loop.index("$finalize = Invoke-Finalize")
    # And a child is only ever transition-owned because the host agent said the
    # child it just planned is one. The supervisor never decides that itself.
    assert "if ([string]$plan.mode -eq $TrialChild) {" in supervisor
    assert "elseif ([string]$plan.mode -eq $RollbackChild) {" in supervisor


# --------------------------------------------------------------------------
# ordinary-failure supervision: restart, bounded backoff, deterministic fence
# --------------------------------------------------------------------------


def _ordinary_exit_branch() -> str:
    """The body that decides what an ordinary (non-update, non-trial) exit means."""

    supervisor = _text(SUPERVISOR)
    loop = supervisor[supervisor.index("while ($true) {") :]
    start = loop.index(
        "if ($exitCode -ne $ApprovedUpdateRestartExitCode -and $childOwner -eq $OrdinaryChild) {"
    )
    return loop[start : loop.index("$finalize = Invoke-Finalize")]


def test_an_unexpected_ordinary_failure_is_restarted_rather_than_ending_supervision() -> None:
    """The defect this delivery closes.

    The supervisor used to ``exit $exitCode`` for *every* ordinary child exit,
    so a single unhandled recorder exception ended supervision and left the
    host with no recorder until an operator noticed. Restart is now the
    supervisor's own decision, which is what makes it the child's owner.
    """

    branch = _ordinary_exit_branch()

    # The branch no longer ends supervision unconditionally: it can loop.
    assert "continue" in branch
    # And the restart re-enters the same loop, so the child is started by the
    # one existing launch site rather than a second one.
    supervisor = _text(SUPERVISOR)
    assert supervisor.count("Start-Recorder $nonce $buildCommit") == 1


def test_an_operator_stop_is_never_restarted() -> None:
    branch = _ordinary_exit_branch()
    supervisor = _text(SUPERVISOR)

    # The intentional-stop check is the first thing the branch does, before any
    # counter moves, so an operator stop can never contribute to a crash streak.
    assert branch.index("Test-IntentionalStop $exitCode") < branch.index(
        "$rapidFailureStreak += 1"
    )
    assert "exit $exitCode" in branch[: branch.index("$rapidFailureStreak += 1")]

    # A graceful zero is the recorder's own Ctrl+C path; a console interrupt
    # that bypassed its handler arrives as STATUS_CONTROL_C_EXIT instead.
    assert "$WindowsControlCExitCode = -1073741510" in supervisor
    stop = supervisor[supervisor.index("function Test-IntentionalStop") :]
    stop = stop.split("\n}\n", 1)[0]
    assert "if ($ExitCode -eq 0) { return $true }" in stop
    assert "if ($ExitCode -eq $WindowsControlCExitCode) { return $true }" in stop


def test_repeated_rapid_failures_fence_supervision_instead_of_looping() -> None:
    branch = _ordinary_exit_branch()
    supervisor = _text(SUPERVISOR)

    # The fence is checked before the backoff wait, so the fencing failure does
    # not first sleep for a restart it will never make.
    assert branch.index("if ($rapidFailureStreak -ge $MaxRapidRestarts) {") < branch.index(
        "Start-Sleep"
    )
    # It stops with a code of its own rather than the child's, so an external
    # service manager can tell a fenced host from a normal recorder stop.
    assert "$CrashFenceExitCode = 6" in supervisor
    assert "exit $CrashFenceExitCode" in branch
    # The fence lives in memory for the life of this supervisor. A state file
    # would be one more thing a crash could leave behind and go stale.
    assert "$rapidFailureStreak = 0" in supervisor
    for persisted in ("Out-File", "Set-Content", "Export-Clixml", "New-Item"):
        assert persisted not in supervisor


def test_the_restart_backoff_is_bounded_by_construction() -> None:
    supervisor = _text(SUPERVISOR)

    # A finite ladder whose last entry is the ceiling, not a doubling that a
    # cap has to catch: there is no expression here that can grow at all.
    assert "[int[]]$RestartBackoffLadderSeconds = @(5, 15, 45, 120)" in supervisor
    delay = supervisor[supervisor.index("function Get-RestartDelaySeconds") :]
    delay = delay.split("\n}\n", 1)[0]
    # The index is clamped to the last rung in both directions.
    assert "if ($index -lt 0) { $index = 0 }" in delay
    assert "$last = $RestartBackoffLadderSeconds.Count - 1" in delay
    assert "if ($index -gt $last) { $index = $last }" in delay
    # No arithmetic that could compound the delay.
    for growth in ("*", "[math]::Pow", "-shl"):
        assert growth not in delay


def test_a_healthy_runtime_decays_the_crash_fence() -> None:
    branch = _ordinary_exit_branch()
    supervisor = _text(SUPERVISOR)

    assert "[int]$HealthyRuntimeSeconds = 120" in supervisor
    # Measured across the child, not guessed from the exit code.
    assert "$childClock = [System.Diagnostics.Stopwatch]::StartNew()" in supervisor
    assert "$childRuntimeSeconds = $childClock.Elapsed.TotalSeconds" in supervisor
    # The reset happens before this failure is counted, so a healthy runtime
    # followed by a crash starts a fresh streak at one rather than continuing.
    reset = branch.index("if ($childRuntimeSeconds -ge $HealthyRuntimeSeconds) {")
    assert reset < branch.index("$rapidFailureStreak += 1")
    # An approved update or trial verdict is not a failure either.
    loop = supervisor[supervisor.index("while ($true) {") :]
    assert loop.count("$rapidFailureStreak = 0") == 2


def test_an_ordinary_restart_adds_no_update_or_trial_bookkeeping() -> None:
    """A crash restart is not an update, and must not borrow its authority."""

    branch = _ordinary_exit_branch()

    # No relaunch journal entry, no watchdog, no plan, no Git.
    for forbidden in (
        "Set-RelaunchedNonce",
        "Start-TrialWatchdog",
        "Invoke-Finalize",
        "Read-LaunchPlan",
        "$replacementPending = $true",
        "$childOwner = $TrialChild",
        "$childOwner = $RollbackChild",
        "$launchRoot =",
        "$launchBuildCommit =",
    ):
        assert forbidden not in branch, f"an ordinary restart must not do {forbidden}"


def test_the_supervision_policy_is_never_supplied_by_the_launcher() -> None:
    """Production always runs the defaults; the parameters exist for tests.

    They are range-validated too, so even a hand-run supervisor cannot be given
    a value that removes the fence or unbounds the wait.
    """

    supervisor = _text(SUPERVISOR)
    launcher = _text(LAUNCHER)

    for policy in (
        "StartedRuntimeSeconds",
        "HealthyRuntimeSeconds",
        "RestartBackoffLadderSeconds",
        "MaxRapidRestarts",
    ):
        assert f"-{policy}" not in launcher, f"the launcher must not pass {policy}"

    assert "[ValidateRange(1, 3600)]\n    [int]$HealthyRuntimeSeconds" in supervisor
    assert "[ValidateCount(1, 16)]" in supervisor
    assert "[ValidateRange(2, 100)]\n    [int]$MaxRapidRestarts" in supervisor


def test_a_recorder_that_never_started_is_not_restarted() -> None:
    """Restart is for a recorder that has been seen working.

    A checkout whose recorder has never got past startup -- a bad interpreter,
    an unimportable entry point, a refused preflight -- fails exactly the same
    way every time. Retrying it four more times only delays the operator and
    replaces the child's own exit code with the fence code, so the first
    failure is propagated unchanged instead.
    """

    branch = _ordinary_exit_branch()
    supervisor = _text(SUPERVISOR)

    assert "[int]$StartedRuntimeSeconds = 10" in supervisor
    assert "$everStarted = $false" in supervisor
    # The gate is evaluated before any streak arithmetic, so a never-started
    # failure cannot contribute to the fence either.
    gate = branch.index("if (-not $everStarted) {")
    assert gate < branch.index("$rapidFailureStreak += 1")
    # It exits with the child's own code, not the fence code.
    refusal = branch[gate : gate + 600]
    assert "exit $exitCode" in refusal
    assert "$CrashFenceExitCode" not in refusal
    # And the flag is only ever set by a child that actually ran that long.
    assert "if ($childRuntimeSeconds -ge $StartedRuntimeSeconds) {" in branch
    assert branch.count("$everStarted = $true") == 1


def test_the_child_exit_code_is_an_integer_and_never_the_child_output() -> None:
    """``Start-Recorder`` must return the exit code alone.

    A native command's stdout inside a PowerShell function becomes part of that
    function's return value, so without this the caller receives the recorder's
    printed output *and* the exit code as one ``Object[]``. That silently
    swallowed everything the recorder printed, and left every exit-code
    comparison reading an array rather than a number.
    """

    supervisor = _text(SUPERVISOR)

    launch = supervisor.index("function Start-Recorder(")
    body = supervisor[launch:].split("\n}\n", 1)[0]
    assert (
        "& $PythonExecutable @PythonPrefix -m scripts.start_tailscale_recorder "
        "@arguments | Out-Host" in body
    )
    assert "return [int]$LASTEXITCODE" in body
    # The decision helper binds a real integer, which is what makes an array
    # here a hard failure rather than a silently wrong comparison.
    assert "function Test-IntentionalStop([int]$ExitCode)" in supervisor


def test_a_rollback_child_is_transition_owned_and_never_ordinarily_restarted() -> None:
    """Rollback is the agent's verdict to make, not the restart machine's.

    ``NativeRecorderTrialAgent`` answers a failed trial with a launch plan whose
    mode is ``rollback``. Keying the supervisor's transition state off ``trial``
    alone left that child looking ordinary, so a rollback that also failed could
    be restarted and fenced instead of handed back to the agent -- and whether
    it was depended on unrelated earlier history, because only a previous
    ordinary failure sets ``$everStarted``. That breaks the branch-trial
    invariant that a safe version which also fails is reported once, never
    looped, and it means ``ROLLBACK_STARTING``/``ROLLBACK_VERIFYING`` never
    reaches ``ROLLBACK_FAILED``.
    """

    supervisor = _text(SUPERVISOR)
    branch = _ordinary_exit_branch()

    # The three owners are named, not inferred from a single boolean.
    assert "$OrdinaryChild = 'ordinary'" in supervisor
    assert "$TrialChild = 'trial'" in supervisor
    assert "$RollbackChild = 'rollback'" in supervisor

    # The restart machine is entered only by an ordinary child, so a trial or
    # rollback child cannot reach the streak, the backoff or the fence.
    assert "$childOwner -eq $OrdinaryChild) {" in supervisor
    for owned in ("$everStarted", "$rapidFailureStreak", "Start-Sleep"):
        assert owned in branch, f"{owned} must live inside the ordinary branch"

    # Both transition modes come from the agent's plan.
    loop = supervisor[supervisor.index("while ($true) {") :]
    assert "if ([string]$plan.mode -eq $TrialChild) {" in loop
    assert "elseif ([string]$plan.mode -eq $RollbackChild) {" in loop

    # A rollback child's exit therefore falls through to the agent's finalize,
    # which is where ROLLBACK_VERIFYING -> ROLLBACK_FAILED is recorded.
    assert loop.index("$childOwner -eq $OrdinaryChild) {") < loop.index(
        "$finalize = Invoke-Finalize"
    )


def test_the_rollback_mode_the_supervisor_reads_is_the_one_the_agent_writes() -> None:
    """The two sides of this contract must not drift apart again."""

    supervisor = _text(SUPERVISOR)
    agent = _text(ROOT / "catalog/mtconnect_recorder/native_trial_agent.py")

    # The agent plans a rollback launch, and answers a rollback child's exit
    # with the terminal rollback failure rather than another launch.
    assert 'TrialPlan(\n            True,\n            "rollback",' in agent
    assert 'code="rollback_launch",' in agent
    assert "if stage in {ROLLBACK_STARTING, ROLLBACK_VERIFYING}:" in agent
    assert "_finish_rollback_failure(" in agent
    assert 'TrialPlan(False, "rollback", code="rollback_failed")' in agent

    # The supervisor recognises exactly that mode string.
    assert "$RollbackChild = 'rollback'" in supervisor


def test_the_restart_state_machine_never_signals_or_waits_on_a_process() -> None:
    """Restart is reached only because the child already exited on its own."""

    branch = _ordinary_exit_branch()

    for forbidden in (
        "Stop-Process",
        "Wait-Process",
        "Get-Process",
        "taskkill",
        ".Kill",
        "CloseMainWindow",
    ):
        assert forbidden not in branch
    # The only wait is the backoff itself.
    assert branch.count("Start-Sleep") == 1


def test_a_failed_finalize_never_relaunches() -> None:
    supervisor = _text(SUPERVISOR)

    failure = supervisor.index("The approved recorder update did not complete")
    relaunch = supervisor.index("$replacementPending = $true")
    assert failure < relaunch
    assert "exit 5" in supervisor
    # An unreadable checkout is a refusal too, not a relaunch.
    assert "exit 2" in supervisor


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
    """One recorder process means one agent, with no lock file to go stale."""

    agent = _text(AGENT)
    worker = _text(ROOT / "catalog/mtconnect_recorder/federation_update.py")

    # The polling agent lives in the recorder process, so it is single-instance
    # by construction rather than by a lock that a crash could leave behind.
    assert "class RecorderHostUpdateAgentWorker" in worker
    assert "NativeRecorderUpdateAgent(" in worker
    # The script keeps only the steps that cannot run inside the recorder.
    assert "--finalize" in agent
    assert "--mark-relaunched" in agent
    assert "while True" not in agent
    assert "--stop-file" not in agent


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


# --------------------------------------------------------------------------
# branch trials add a destination, never an authority
# --------------------------------------------------------------------------


def test_the_supervisor_launches_only_what_the_host_agent_planned() -> None:
    """The supervisor never invents a root, a data directory or a commit."""

    supervisor = _text(SUPERVISOR)

    # Every launch value is read from the agent's plan, and nothing else.
    assert "$launchRoot = Normalize-DirectoryPath ([string]$plan.launch_root)" in supervisor
    assert (
        "$launchDataDirectory = Normalize-DirectoryPath ([string]$plan.data_directory)"
        in supervisor
    )
    assert "$launchBuildCommit = ([string]$plan.build_commit).Trim().ToLowerInvariant()" in supervisor
    # A plan that names anything but an exact commit is refused, not launched.
    assert "if ($launchBuildCommit -notmatch '^[0-9a-f]{40}$') {" in supervisor
    # Defaults are reset before each plan is read, so a stale trial root can
    # never leak into a later launch.
    reset = supervisor.index("$launchRoot = $RepoRoot\n            $launchDataDirectory")
    assert reset < supervisor.index("$plan.launch_root")


def test_a_missing_or_unreadable_plan_never_relaunches() -> None:
    supervisor = _text(SUPERVISOR)

    assert (
        "if ($finalize.ExitCode -ne 0 -or $null -eq $plan -or -not $plan.relaunch) {"
        in supervisor
    )
    failure = supervisor.index("The approved recorder update did not complete")
    relaunch = supervisor.rindex("$replacementPending = $true")
    assert failure < relaunch


def test_the_supervisor_owns_no_git_for_a_trial() -> None:
    """Rolling back is a relaunch, not a repair: no Git belongs here at all."""

    supervisor = _text(SUPERVISOR)
    invocations = [
        line.strip()
        for line in supervisor.splitlines()
        if not line.lstrip().startswith("#") and "git " in line
    ]

    # Exactly two: one availability probe and one read-only commit read. No
    # worktree, merge, reset, clean, stash or checkout is invoked here at all,
    # which is why a rollback is a relaunch rather than a repair.
    assert invocations == [
        "if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) { return '' }",
        "$output = & git -C $Root rev-parse --verify 'HEAD^{commit}' 2>&1",
    ]


def test_a_trial_child_is_told_the_permanent_checkout_it_belongs_to() -> None:
    supervisor = _text(SUPERVISOR)

    assert "$env:FCP_RECORDER_PRODUCTION_ROOT = $RepoRoot" in supervisor
    # Locally resolved, and cleared again with the rest of the child identity.
    assert "Remove-Item Env:FCP_RECORDER_PRODUCTION_ROOT" in supervisor


def test_an_operator_stop_during_a_trial_still_ends_supervision() -> None:
    supervisor = _text(SUPERVISOR)

    index = supervisor.index("$plan.code -eq 'trial_operator_stopped'")
    assert "exit $exitCode" in supervisor[index : index + 400]


def test_the_trial_lifecycle_is_documented_where_it_is_implemented() -> None:
    lifecycle = _text(LIFECYCLE)

    # The agent still never kills, never rewrites history, never runs a
    # destructive Git recovery, and never takes a path from a peer.
    for forbidden in ("reset --hard", "git clean", "git stash", "taskkill", "SIGKILL"):
        assert forbidden not in lifecycle


def test_the_trial_watchdog_runs_from_the_permanent_checkout() -> None:
    """The verdict on a trial must not come from the branch being tested.

    A check living in the trial worktree is one that branch could omit, break
    or simply predate -- and a trial that never fails itself would keep an
    unproven recorder running indefinitely. So the watchdog is started from the
    permanent checkout, with the permanent checkout's own agent.
    """

    supervisor = _text(SUPERVISOR)
    body = supervisor[
        supervisor.index("function Start-TrialWatchdog {") :
    ].split("\n}\n", 1)[0]

    assert "Get-AgentArguments @('--watch-trial')" in body
    assert "-WorkingDirectory $RepoRoot" in body
    # Never blocking: the supervisor still waits on the child, not on this.
    assert "-Wait" not in body
    # And the agent it runs is resolved from $RepoRoot, never a launch root.
    assert "$launchRoot" not in body
    assert "'--repo-root', $RepoRoot" in _text(SUPERVISOR)


def test_the_supervisor_still_never_terminates_a_process() -> None:
    """Including a trial child, which writes to the real data directory."""

    supervisor = _text(SUPERVISOR)

    for forbidden in ("Stop-Process", "taskkill", "Kill()", ".Terminate", "-Force"):
        assert forbidden not in supervisor

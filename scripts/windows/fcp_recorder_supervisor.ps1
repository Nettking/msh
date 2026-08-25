[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$PythonExecutable,
    [string[]]$PythonPrefix = @(),
    # Passed as one explicit array, never as remaining arguments: re-parsing
    # "--data-dir" and friends through a second PowerShell binding layer would
    # be a silent way to launch the replacement with different arguments than
    # the operator started with.
    [string[]]$RecorderArguments = @(),
    # Ordinary-failure supervision policy. The launcher passes none of these, so
    # a supervised recorder always runs the production defaults below; they are
    # parameters only so the restart state machine can be driven end-to-end by a
    # test instead of being asserted at a distance. Every one is range-validated,
    # so no value that would defeat the fence or unbound the delay can be bound.
    # The child got past startup. A first failure below this never enters the
    # restart machine at all: see $everStarted below.
    [ValidateRange(1, 3600)]
    [int]$StartedRuntimeSeconds = 10,
    [ValidateRange(1, 3600)]
    [int]$HealthyRuntimeSeconds = 120,
    [ValidateCount(1, 16)]
    [ValidateRange(0, 3600)]
    [int[]]$RestartBackoffLadderSeconds = @(5, 15, 45, 120),
    [ValidateRange(2, 100)]
    [int]$MaxRapidRestarts = 5
)

# Native supervision for the standalone MTConnect recorder.
#
# The recorder is a native Python process, not a container, so "Update all
# devices" cannot recreate it. This supervisor is the piece that can: it owns
# the child process lifecycle, and it is the only component allowed to relaunch
# the recorder.
#
# It never invents anything a Federation peer could influence. The interpreter,
# its arguments and the repository are resolved locally before the loop starts
# and are reused verbatim for every replacement. The supervisor's whole
# contribution to an update is:
#
#   * generate a supervisor session, and a fresh process-instance nonce per child
#   * notice the approved-update exit code
#   * fast-forward the checkout only after the child has actually exited
#   * start exactly one replacement, and record which one it started
#
# It also owns ordinary child restart, which is a separate job from the update
# path and shares none of its authority. An operator stop still ends
# supervision; an unexpected failure is restarted after a bounded wait; and a
# failure that keeps repeating without the recorder ever staying up stops
# supervision with a distinct code rather than looping forever. That fence is
# per supervisor process, held in memory and never on disk, so there is no
# state file for a crash to leave behind and go stale.
#
# An external service manager may own this supervisor process. It must not own
# the child: the mutex below keeps one recorder per checkout whatever restarts
# the supervisor itself.
#
# Branch trials add exactly one idea and no new authority: the agent answers
# *which prepared root* to launch next. For an ordinary update that answer is
# "the same one as always", byte for byte. For a trial it is a separate
# worktree the agent already prepared and verified, and for a fallback it is
# the production checkout that was never moved -- which is why restoring the
# known-good version needs no Git operation at all.
#
# A trial child also gets one bounded companion: a watchdog run from *this*
# permanent checkout, never from the branch under test. It owns the verdict on
# the trial, because a check living in the trial worktree is one the tested
# branch could omit, break or simply predate. It exits on its own, is started
# only for a trial child, and is never started on the ordinary startup path.
#
# The recorder runs the host update agent inside its own process, so the
# supported startup path stays exactly one child and this supervisor starts
# nothing before it.
#
# It never force-kills the recorder, never runs reset/clean/stash, and never
# builds or starts a Compose service.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ApprovedUpdateRestartExitCode = 75
$UpdateAgentScript = 'scripts/fcp_native_recorder_update_agent.py'

# Which transition owns the child that is about to start. A trial or rollback
# child belongs to the branch-trial transition: its exit is the host agent's
# verdict to make, and the agent's own finalize path already answers for it --
# ROLLBACK_STARTING/ROLLBACK_VERIFYING records ROLLBACK_FAILED, which is how a
# safe version that also fails is reported once instead of retried. Only an
# ordinary child -- the one the operator started, or the replacement after an
# approved update -- is the restart machine's business. The agent names these
# in its launch plan: "update" for the ordinary path, "trial", "rollback".
$OrdinaryChild = 'ordinary'
$TrialChild = 'trial'
$RollbackChild = 'rollback'

# A console Ctrl+C that never reached the recorder's own handler surfaces as
# STATUS_CONTROL_C_EXIT (0xC000013A) rather than a graceful zero.
$WindowsControlCExitCode = -1073741510
# Supervision stopped itself after repeated rapid failures. This is deliberately
# distinct from every child code and from the existing refusal codes, so an
# external service manager can tell "this host needs attention" apart from "the
# recorder stopped normally" without having to own the child itself.
$CrashFenceExitCode = 6

function Normalize-DirectoryPath([string]$Value) {
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw 'recorder_supervisor_path_missing'
    }
    $full = [System.IO.Path]::GetFullPath($Value)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Length -le $root.Length) { return $full }
    $separators = [char[]]@(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    return $full.TrimEnd($separators)
}

$RepoRoot = Normalize-DirectoryPath $RepoRoot

function New-Nonce {
    return [guid]::NewGuid().ToString('N')
}

function Get-PathHash([string]$Value) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value.ToLowerInvariant())
        return ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').Substring(0, 24)
    }
    finally {
        $sha.Dispose()
    }
}

function Last-Text([object[]]$Values) {
    $last = $Values | Select-Object -Last 1
    if ($null -eq $last) { return '' }
    return [string]$last
}

function Invoke-Python([string[]]$Arguments) {
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $PythonExecutable @PythonPrefix @Arguments 2>&1
        $exit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{
        Output = @($output | ForEach-Object { [string]$_ })
        ExitCode = [int]$exit
    }
}

function Get-HeadCommit([string]$Root = $RepoRoot) {
    if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) { return '' }
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & git -C $Root rev-parse --verify 'HEAD^{commit}' 2>&1
        $exit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }
    if ($exit -ne 0) { return '' }
    $value = (Last-Text @($output)).Trim().ToLowerInvariant()
    if ($value -match '^[0-9a-f]{40}$') { return $value }
    return ''
}

function Test-IntentionalStop([int]$ExitCode) {
    # The recorder installs its own signal handler, so an operator Ctrl+C stops
    # capture and returns a graceful zero; "--once" finishes the same way. A
    # console interrupt that bypassed that handler arrives as
    # STATUS_CONTROL_C_EXIT instead. Both are the operator ending supervision,
    # and neither has ever been restarted -- that stays true here.
    if ($ExitCode -eq 0) { return $true }
    if ($ExitCode -eq $WindowsControlCExitCode) { return $true }
    return $false
}

function Get-RestartDelaySeconds([int]$Streak) {
    # Clamped to the ladder, so a long streak repeats the ceiling instead of
    # extending the delay.
    $index = $Streak - 1
    if ($index -lt 0) { $index = 0 }
    $last = $RestartBackoffLadderSeconds.Count - 1
    if ($index -gt $last) { $index = $last }
    return [int]$RestartBackoffLadderSeconds[$index]
}

# Exactly one supervisor -- and therefore exactly one recorder -- per checkout.
$mutexName = 'Global\FCPNativeRecorderSupervisor-' + (Get-PathHash $RepoRoot)
$createdNew = $false
$mutex = [System.Threading.Mutex]::new($true, $mutexName, [ref]$createdNew)
if (-not $createdNew) {
    $mutex.Dispose()
    [Console]::Error.WriteLine(
        'An FCP recorder supervisor is already running for this checkout. ' +
        'Stop it before starting another one.'
    )
    exit 3
}

$SupervisorSession = New-Nonce

function Get-AgentArguments([string[]]$Extra) {
    # The agent resolves the recorder data directory from these arguments with
    # the launcher's own rule, so the supervisor never has to compute it -- and
    # never runs an extra interpreter on the supported startup path to do so.
    # One token per argument. A recorder argument starts with "--", so a
    # separate value token would be parsed as an option name instead.
    $forwarded = @()
    foreach ($argument in $RecorderArguments) {
        $forwarded += ('--recorder-arg=' + $argument)
    }
    return @(
        $UpdateAgentScript,
        '--repo-root', $RepoRoot,
        '--supervisor-session', $SupervisorSession
    ) + $forwarded + $Extra
}

function Invoke-Finalize {
    return Invoke-Python (Get-AgentArguments @('--finalize'))
}

function Start-TrialWatchdog {
    # Started from $RepoRoot with the permanent checkout's own agent, so the
    # branch under test cannot influence, disable or outlive the verdict.
    $arguments = @($PythonPrefix) + (Get-AgentArguments @('--watch-trial'))
    Start-Process `
        -FilePath $PythonExecutable `
        -ArgumentList $arguments `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden | Out-Null
}

function Read-LaunchPlan([object[]]$Output) {
    # The agent answers with one JSON document. Anything else -- an empty
    # result, a traceback, a partial line -- is treated as "no plan", which
    # keeps the supervisor on its existing refuse-and-stop path.
    $lines = @($Output)
    for ($index = $lines.Count - 1; $index -ge 0; $index--) {
        $text = ([string]$lines[$index]).Trim()
        if (-not $text.StartsWith('{')) { continue }
        try { $plan = $text | ConvertFrom-Json } catch { continue }
        if ($null -eq $plan) { continue }
        $complete = $true
        foreach ($required in @(
            'relaunch', 'mode', 'code', 'launch_root', 'data_directory', 'build_commit'
        )) {
            if (-not ($plan.PSObject.Properties.Name -contains $required)) {
                $complete = $false
                break
            }
        }
        if ($complete) { return $plan }
    }
    return $null
}

function Set-RelaunchedNonce([string]$Nonce) {
    Invoke-Python (
        Get-AgentArguments @('--mark-relaunched', '--process-nonce', $Nonce)
    ) | Out-Null
}

function Start-Recorder(
    [string]$Nonce,
    [string]$BuildCommit,
    [string]$LaunchRoot,
    [string]$DataDirectory
) {
    # Only locally generated supervisor identity crosses into the child. No
    # value here has ever been supplied by, or seen by, a Federation peer.
    $env:FCP_RECORDER_SUPERVISOR_SESSION = $SupervisorSession
    $env:FCP_RECORDER_PROCESS_NONCE = $Nonce
    $env:FCP_RECORDER_BUILD_COMMIT = $BuildCommit
    # The permanent checkout, stated rather than inferred: a trial child runs
    # from a worktree, so it cannot work this out from where its own code is.
    $env:FCP_RECORDER_PRODUCTION_ROOT = $RepoRoot
    # A trial child is launched from a different root, so its data directory is
    # named explicitly. It is the *same* directory the running recorder already
    # uses: identity, pairing, checkpoints, recordings and backlog never move.
    $arguments = @($RecorderArguments)
    if (-not [string]::IsNullOrWhiteSpace($DataDirectory)) {
        $arguments += @('--data-dir', $DataDirectory)
    }
    Push-Location $LaunchRoot
    try {
        # Out-Host keeps the recorder's own output on the operator's console
        # instead of collecting it as this function's return value. Without it
        # the caller receives the child's stdout *and* the exit code as one
        # Object[], which silently swallows everything the recorder printed and
        # leaves every later exit-code decision reading an array.
        & $PythonExecutable @PythonPrefix -m scripts.start_tailscale_recorder @arguments | Out-Host
        return [int]$LASTEXITCODE
    }
    finally {
        Pop-Location
        Remove-Item Env:FCP_RECORDER_PROCESS_NONCE -ErrorAction SilentlyContinue
        Remove-Item Env:FCP_RECORDER_BUILD_COMMIT -ErrorAction SilentlyContinue
        Remove-Item Env:FCP_RECORDER_PRODUCTION_ROOT -ErrorAction SilentlyContinue
    }
}

$mutexReleased = $false
$replacementPending = $false
$launchRoot = $RepoRoot
$launchDataDirectory = ''
$launchBuildCommit = ''
$childOwner = $OrdinaryChild
# Consecutive ordinary failures that did not reach a healthy runtime. Reset by a
# healthy child and by the update path; only this counter can reach the fence.
$rapidFailureStreak = 0
# Whether a recorder has ever got past startup under this supervisor. Restart is
# for a recorder that has been seen working; a checkout that has never run one is
# a startup or configuration failure the operator needs to see directly.
$everStarted = $false
try {
    Push-Location $RepoRoot
    try {
        while ($true) {
            $nonce = New-Nonce
            if ([string]::IsNullOrWhiteSpace($launchBuildCommit)) {
                $buildCommit = Get-HeadCommit $launchRoot
            }
            else {
                $buildCommit = $launchBuildCommit
            }
            if ([string]::IsNullOrWhiteSpace($buildCommit)) {
                [Console]::Error.WriteLine(
                    'The recorder checkout has no readable commit. Federation ' +
                    'updates need a supported FCP Git checkout.'
                )
                exit 2
            }

            if ($replacementPending) {
                # Recorded before the child starts, so the replacement must be
                # proven to be exactly this process and not an earlier survivor.
                Set-RelaunchedNonce $nonce
                $replacementPending = $false
                if ($childOwner -eq $TrialChild) {
                    # Only for a trial child, and only after the journal names
                    # the instance the watchdog has to judge.
                    Start-TrialWatchdog
                }
            }

            # Nothing else is started here. The recorder runs the host update
            # agent inside its own process, so the supported startup path is
            # exactly one child and the launcher contract is unchanged.
            $childClock = [System.Diagnostics.Stopwatch]::StartNew()
            $exitCode = Start-Recorder $nonce $buildCommit $launchRoot $launchDataDirectory
            $childRuntimeSeconds = $childClock.Elapsed.TotalSeconds

            # A trial or rollback child that exits for *any* reason is asked
            # about, because "the branch crashed on startup" is exactly the case
            # the pinned fallback exists for and it never reaches the approved
            # exit code -- and a restored safe version that also fails is the
            # case the agent reports once rather than retrying.
            if ($exitCode -ne $ApprovedUpdateRestartExitCode -and $childOwner -eq $OrdinaryChild) {
                # An ordinary child exit, and the one decision this supervisor
                # owns outright. Three outcomes, in this order:
                #
                #   * the operator stopped it        -> end supervision
                #   * it failed unexpectedly         -> restart after a bounded wait
                #   * it keeps failing rapidly       -> fence instead of looping
                #
                # The child is already gone in every case, so nothing is ever
                # signalled or terminated to reach any of them.
                if (Test-IntentionalStop $exitCode) {
                    exit $exitCode
                }
                if ($childRuntimeSeconds -ge $StartedRuntimeSeconds) {
                    $everStarted = $true
                }
                if (-not $everStarted) {
                    # No recorder has run here yet, so there is nothing to
                    # restore and nothing to learn from trying again: a bad
                    # interpreter, an unimportable entry point or a refused
                    # preflight fails exactly this way every time. The operator
                    # gets the child's own exit code, immediately and unchanged,
                    # rather than the same failure four more times and a code
                    # that hides it.
                    exit $exitCode
                }
                if ($childRuntimeSeconds -ge $HealthyRuntimeSeconds) {
                    # The runtime proved itself between failures, so the earlier
                    # streak is spent and this failure starts a new one.
                    $rapidFailureStreak = 0
                }
                $rapidFailureStreak += 1
                if ($rapidFailureStreak -ge $MaxRapidRestarts) {
                    [Console]::Error.WriteLine(
                        'The recorder failed ' + $rapidFailureStreak +
                        ' times without staying up for ' + $HealthyRuntimeSeconds +
                        ' seconds (last exit code ' + $exitCode + '). This is a ' +
                        'deterministic failure, not a transient one, so ' +
                        'supervision has stopped instead of restarting it again.'
                    )
                    exit $CrashFenceExitCode
                }
                $restartDelaySeconds = Get-RestartDelaySeconds $rapidFailureStreak
                [Console]::Error.WriteLine(
                    'The recorder exited unexpectedly with code ' + $exitCode +
                    '. Restarting in ' + $restartDelaySeconds + ' seconds (failure ' +
                    $rapidFailureStreak + ' of ' + $MaxRapidRestarts + ').'
                )
                Start-Sleep -Seconds $restartDelaySeconds
                # Same root, same arguments, fresh nonce, still exactly one
                # child: an ordinary restart adds no update bookkeeping and
                # starts no watchdog, so both flags stay false.
                continue
            }

            # An approved update or a trial verdict, which is not a failure: the
            # streak an earlier crash left behind does not survive into it.
            $rapidFailureStreak = 0

            # Reached only after the recorder process has exited.
            $finalize = Invoke-Finalize
            $plan = Read-LaunchPlan $finalize.Output
            if ($null -ne $plan -and -not $plan.relaunch -and $plan.code -eq 'trial_operator_stopped') {
                # An operator ended the trial themselves. Nothing is relaunched
                # behind them, exactly as an operator stop has always behaved.
                exit $exitCode
            }
            if ($finalize.ExitCode -ne 0 -or $null -eq $plan -or -not $plan.relaunch) {
                [Console]::Error.WriteLine(
                    'The approved recorder update did not complete: ' +
                    (($finalize.Output) -join ' ')
                )
                exit 5
            }

            $launchRoot = $RepoRoot
            $launchDataDirectory = ''
            $launchBuildCommit = ''
            $childOwner = $OrdinaryChild
            if (-not [string]::IsNullOrWhiteSpace([string]$plan.launch_root)) {
                $launchRoot = Normalize-DirectoryPath ([string]$plan.launch_root)
            }
            if (-not [string]::IsNullOrWhiteSpace([string]$plan.data_directory)) {
                $launchDataDirectory = Normalize-DirectoryPath ([string]$plan.data_directory)
            }
            if (-not [string]::IsNullOrWhiteSpace([string]$plan.build_commit)) {
                $launchBuildCommit = ([string]$plan.build_commit).Trim().ToLowerInvariant()
                if ($launchBuildCommit -notmatch '^[0-9a-f]{40}$') {
                    [Console]::Error.WriteLine(
                        'The recorder update agent named an unusable commit.'
                    )
                    exit 5
                }
            }
            # Both transition modes are read from the plan the agent returned.
            # Anything else -- "update", or a mode this supervisor predates --
            # is an ordinary child, which is the pre-branch-trial behavior.
            if ([string]$plan.mode -eq $TrialChild) {
                $childOwner = $TrialChild
            }
            elseif ([string]$plan.mode -eq $RollbackChild) {
                $childOwner = $RollbackChild
            }
            $replacementPending = $true
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    Remove-Item Env:FCP_RECORDER_SUPERVISOR_SESSION -ErrorAction SilentlyContinue
    if (-not $mutexReleased) {
        $mutex.ReleaseMutex() | Out-Null
        $mutex.Dispose()
    }
}

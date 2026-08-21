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
    [string[]]$RecorderArguments = @()
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
# Branch trials add exactly one idea and no new authority: the agent answers
# *which prepared root* to launch next. For an ordinary update that answer is
# "the same one as always", byte for byte. For a trial it is a separate
# worktree the agent already prepared and verified, and for a fallback it is
# the production checkout that was never moved -- which is why restoring the
# known-good version needs no Git operation at all.
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
        & $PythonExecutable @PythonPrefix -m scripts.start_tailscale_recorder @arguments
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
$trialActive = $false
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
            }

            # Nothing else is started here. The recorder runs the host update
            # agent inside its own process, so the supported startup path is
            # exactly one child and the launcher contract is unchanged.
            $exitCode = Start-Recorder $nonce $buildCommit $launchRoot $launchDataDirectory

            # A trial child that exits for *any* reason is asked about, because
            # "the branch crashed on startup" is exactly the case the pinned
            # fallback exists for and it never reaches the approved exit code.
            if ($exitCode -ne $ApprovedUpdateRestartExitCode -and -not $trialActive) {
                exit $exitCode
            }

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
            $trialActive = $false
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
            $trialActive = ([string]$plan.mode -eq 'trial')
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

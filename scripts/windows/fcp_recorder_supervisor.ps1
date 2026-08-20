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
#   * run the host update agent alongside the recorder
#   * notice the approved-update exit code
#   * fast-forward the checkout only after the child has actually exited
#   * start exactly one replacement, and record which one it started
#
# It never force-kills the recorder, never runs reset/clean/stash, and never
# builds or starts a Compose service.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ApprovedUpdateRestartExitCode = 75
$AgentShutdownTimeoutSeconds = 60
$UpdateAgentScript = 'scripts/fcp_native_recorder_update_agent.py'

function Normalize-DirectoryPath([string]$Value) {
    $full = [System.IO.Path]::GetFullPath($Value)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Length -le $root.Length) { return $full }
    $separators = [char[]]@(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    return $full.TrimEnd($separators)
}

function ConvertTo-ProcessArgument([string]$Value) {
    # Windows PowerShell 5.1 does not quote Start-Process arguments for us, and
    # a physical recorder is routinely installed under a path with spaces.
    if ($Value -match '^[A-Za-z0-9._:\\/=-]+$') { return $Value }
    return '"' + $Value.Replace('"', '\"') + '"'
}

if ($null -eq $PythonPrefix) { $PythonPrefix = @() }
if ($null -eq $RecorderArguments) { $RecorderArguments = @() }

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

function Get-HeadCommit {
    if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) { return '' }
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & git -C $RepoRoot rev-parse --verify 'HEAD^{commit}' 2>&1
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

function Resolve-DataDirectory {
    # Resolved by the launcher's own rule rather than re-implemented here. A
    # supervisor watching a different directory than the recorder writes to
    # would verify the wrong process.
    $probe = (
        'import sys; ' +
        'from scripts.start_tailscale_recorder import recorder_data_directory; ' +
        'print(recorder_data_directory(sys.argv[1:]))'
    )
    $result = Invoke-Python (@('-c', $probe) + $RecorderArguments)
    if ($result.ExitCode -ne 0) {
        throw 'recorder_data_directory_unavailable'
    }
    return Normalize-DirectoryPath ((Last-Text $result.Output).Trim())
}

$DataDirectory = Resolve-DataDirectory
$AgentDirectory = Join-Path $DataDirectory 'federation\recorder-update-agent'
$AgentStopFile = Join-Path $AgentDirectory 'agent-stop'
New-Item -ItemType Directory -Path $AgentDirectory -Force | Out-Null

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
    return @($PythonPrefix) + @(
        (ConvertTo-ProcessArgument $UpdateAgentScript),
        '--repo-root', (ConvertTo-ProcessArgument $RepoRoot),
        '--data-directory', (ConvertTo-ProcessArgument $DataDirectory),
        '--supervisor-session', $SupervisorSession
    ) + $Extra
}

function Start-UpdateAgent {
    Remove-Item -LiteralPath $AgentStopFile -Force -ErrorAction SilentlyContinue
    return Start-Process `
        -FilePath $PythonExecutable `
        -ArgumentList (Get-AgentArguments @(
            '--stop-file', (ConvertTo-ProcessArgument $AgentStopFile)
        )) `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -PassThru
}

function Stop-UpdateAgent([object]$Agent) {
    if ($null -eq $Agent) { return $true }
    # Cooperative shutdown only. The agent finishes whatever bounded step it is
    # in and exits by itself; nothing here terminates it mid-operation.
    New-Item -ItemType File -Path $AgentStopFile -Force | Out-Null
    $stopped = $Agent.WaitForExit($AgentShutdownTimeoutSeconds * 1000)
    if ($stopped) { $Agent.Dispose() }
    return $stopped
}

function Invoke-Finalize {
    return Invoke-Python (@(
        $UpdateAgentScript,
        '--repo-root', $RepoRoot,
        '--data-directory', $DataDirectory,
        '--supervisor-session', $SupervisorSession,
        '--finalize'
    ))
}

function Set-RelaunchedNonce([string]$Nonce) {
    Invoke-Python (@(
        $UpdateAgentScript,
        '--repo-root', $RepoRoot,
        '--data-directory', $DataDirectory,
        '--supervisor-session', $SupervisorSession,
        '--mark-relaunched',
        '--process-nonce', $Nonce
    )) | Out-Null
}

function Start-Recorder([string]$Nonce, [string]$BuildCommit) {
    # Only locally generated supervisor identity crosses into the child. No
    # value here has ever been supplied by, or seen by, a Federation peer.
    $env:FCP_RECORDER_SUPERVISOR_SESSION = $SupervisorSession
    $env:FCP_RECORDER_PROCESS_NONCE = $Nonce
    $env:FCP_RECORDER_BUILD_COMMIT = $BuildCommit
    try {
        & $PythonExecutable @PythonPrefix -m scripts.start_tailscale_recorder @RecorderArguments
        return [int]$LASTEXITCODE
    }
    finally {
        Remove-Item Env:FCP_RECORDER_PROCESS_NONCE -ErrorAction SilentlyContinue
        Remove-Item Env:FCP_RECORDER_BUILD_COMMIT -ErrorAction SilentlyContinue
    }
}

$mutexReleased = $false
$replacementPending = $false
try {
    Push-Location $RepoRoot
    try {
        while ($true) {
            $nonce = New-Nonce
            $buildCommit = Get-HeadCommit
            if ([string]::IsNullOrWhiteSpace($buildCommit)) {
                [Console]::Error.WriteLine(
                    'The recorder checkout has no readable commit. Federation ' +
                    'updates need a supported Git checkout of Nettking/msh.'
                )
                exit 2
            }

            if ($replacementPending) {
                # Recorded before the child starts, so the replacement must be
                # proven to be exactly this process and not an earlier survivor.
                Set-RelaunchedNonce $nonce
                $replacementPending = $false
            }

            $agent = $null
            try {
                $agent = Start-UpdateAgent
            }
            catch {
                # Capture is the product. A recorder that cannot participate in
                # updates still records; it simply reports no update result.
                Write-Warning "FCP recorder update agent did not start: $($_.Exception.Message)"
            }

            $exitCode = Start-Recorder $nonce $buildCommit
            $agentStopped = Stop-UpdateAgent $agent

            if ($exitCode -ne $ApprovedUpdateRestartExitCode) {
                exit $exitCode
            }
            if (-not $agentStopped) {
                [Console]::Error.WriteLine(
                    'The recorder update agent did not finish; the checkout was ' +
                    'left untouched and the recorder was not restarted.'
                )
                exit 4
            }

            $finalize = Invoke-Finalize
            if ($finalize.ExitCode -ne 0) {
                [Console]::Error.WriteLine(
                    'The approved recorder update did not complete: ' +
                    (($finalize.Output) -join ' ')
                )
                exit 5
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

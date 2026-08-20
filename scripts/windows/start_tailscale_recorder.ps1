[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RecorderArguments
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path

function Stop-RecorderLaunch {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,
        [int]$ExitCode = 2
    )

    [Console]::Error.WriteLine($Message)
    exit $ExitCode
}

function Test-RecorderPython {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Candidate
    )

    $ProbeCode = (
        "import scripts.start_tailscale_recorder as launcher; " +
        "import catalog.mtconnect_recorder as recorder; " +
        "import catalog.mtconnect_recorder.runtime; " +
        "import requests; " +
        "raise SystemExit(0 if callable(launcher.main) and " +
        "callable(recorder.run) else 1)"
    )
    $ProbeArguments = @($Candidate.Prefix) + @("-c", ('"{0}"' -f $ProbeCode))
    $Process = $null
    try {
        $Process = Start-Process `
            -FilePath $Candidate.Executable `
            -ArgumentList $ProbeArguments `
            -WorkingDirectory $RepositoryRoot `
            -WindowStyle Hidden `
            -PassThru
        if (-not $Process.WaitForExit(10000)) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
            return $false
        }
        return $Process.ExitCode -eq 0
    }
    catch {
        return $false
    }
    finally {
        if ($null -ne $Process) {
            $Process.Dispose()
        }
    }
}

function Resolve-RecorderPython {
    $Candidates = [Collections.Generic.List[object]]::new()
    if (-not [string]::IsNullOrWhiteSpace($env:FCP_RECORDER_PYTHON)) {
        $Candidates.Add([pscustomobject]@{
            Executable = $env:FCP_RECORDER_PYTHON
            Prefix = @()
        })
    }

    $VirtualEnvironmentPython = Join-Path $RepositoryRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $VirtualEnvironmentPython -PathType Leaf) {
        $Candidates.Add([pscustomobject]@{
            Executable = $VirtualEnvironmentPython
            Prefix = @()
        })
    }

    $Python = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($null -ne $Python) {
        $Candidates.Add([pscustomobject]@{
            Executable = $Python.Source
            Prefix = @()
        })
    }

    $PyLauncher = Get-Command py.exe -ErrorAction SilentlyContinue
    if ($null -ne $PyLauncher) {
        $Candidates.Add([pscustomobject]@{
            Executable = $PyLauncher.Source
            Prefix = @("-3")
        })
    }

    $Seen = @{}
    foreach ($Candidate in $Candidates) {
        $Identity = "$($Candidate.Executable)|$($Candidate.Prefix -join ' ')"
        if ($Seen.ContainsKey($Identity)) {
            continue
        }
        $Seen[$Identity] = $true
        if (Test-RecorderPython $Candidate) {
            return $Candidate
        }
    }

    Stop-RecorderLaunch (
        "No Python 3 interpreter can load the FCP recorder. Install the " +
        "repository requirements, then run this command again."
    )
}

# The zero-touch Tailscale path never consumes a pairing code from the host
# environment. Remove any inherited legacy value before both the import probe
# and the real process so it cannot become an accidental enrollment bypass.
Remove-Item Env:FCP_RECORDER_FEDERATION_KEY -ErrorAction SilentlyContinue
$PythonCommand = Resolve-RecorderPython

# The recorder runs under the native supervisor so "Check for updates ->
# Update all devices" can actually replace this process. The supervisor keeps
# the operator command unchanged: it resolves nothing a Federation peer can
# influence, and reuses exactly the interpreter and arguments resolved above.
$SupervisorScript = Join-Path $PSScriptRoot 'fcp_recorder_supervisor.ps1'
if (-not (Test-Path -LiteralPath $SupervisorScript -PathType Leaf)) {
    Stop-RecorderLaunch (
        "The FCP recorder supervisor is missing from this checkout. Update " +
        "this checkout to current main, then run this command again."
    )
}

Push-Location $RepositoryRoot
try {
    & $SupervisorScript `
        -RepoRoot $RepositoryRoot `
        -PythonExecutable $PythonCommand.Executable `
        -PythonPrefix $PythonCommand.Prefix `
        -RecorderArguments $RecorderArguments
    $LauncherExitCode = $LASTEXITCODE
}
finally {
    Remove-Item Env:FCP_RECORDER_FEDERATION_KEY -ErrorAction SilentlyContinue
    Pop-Location
}
exit $LauncherExitCode

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

    $ProbeArguments = @($Candidate.Prefix) + @("-c", '"import start_recorder"')
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

$PairingKey = $env:FCP_RECORDER_FEDERATION_KEY
Remove-Item Env:FCP_RECORDER_FEDERATION_KEY -ErrorAction SilentlyContinue
$PythonCommand = Resolve-RecorderPython

Push-Location $RepositoryRoot
try {
    if (-not [string]::IsNullOrWhiteSpace($PairingKey)) {
        $env:FCP_RECORDER_FEDERATION_KEY = $PairingKey
    }
    & $PythonCommand.Executable @($PythonCommand.Prefix) -m scripts.start_tailscale_recorder @RecorderArguments
    $LauncherExitCode = $LASTEXITCODE
}
finally {
    Remove-Item Env:FCP_RECORDER_FEDERATION_KEY -ErrorAction SilentlyContinue
    $PairingKey = $null
    Pop-Location
}
exit $LauncherExitCode

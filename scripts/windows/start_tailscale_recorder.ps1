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

    Push-Location $RepositoryRoot
    try {
        & $Candidate.Executable @($Candidate.Prefix) -c "import start_recorder" 2>$null
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
    finally {
        Pop-Location
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

$PythonCommand = Resolve-RecorderPython

Push-Location $RepositoryRoot
try {
    & $PythonCommand.Executable @($PythonCommand.Prefix) -m scripts.start_tailscale_recorder @RecorderArguments
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

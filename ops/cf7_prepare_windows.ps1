param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("local-ai", "cnc-recorder", "school-control")]
    [string]$Machine,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-f]{40}$")]
    [string]$Commit,

    [string]$Operator = "Martin",

    [ValidateSet("init", "preflight", "gate", "all")]
    [string]$Action = "all"
)

$ErrorActionPreference = "Stop"
$Checkout = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Checkout

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python is not available on PATH. Install Python 3.11 or newer."
}

function Invoke-Cf7Readiness {
    param([string]$Subcommand)

    $Arguments = @(
        "-m", "ops.cf7_physical_readiness",
        "--checkout", $Checkout,
        "--evidence-root", "evidence",
        $Subcommand,
        "--machine", $Machine,
        "--commit", $Commit
    )
    if ($Subcommand -eq "init") {
        $Arguments += @("--operator", $Operator)
    }
    & python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "CF7 $Subcommand failed for $Machine."
    }
}

switch ($Action) {
    "init" { Invoke-Cf7Readiness "init" }
    "preflight" { Invoke-Cf7Readiness "preflight" }
    "gate" { Invoke-Cf7Readiness "gate" }
    "all" {
        Invoke-Cf7Readiness "init"
        Invoke-Cf7Readiness "preflight"
        Invoke-Cf7Readiness "gate"
    }
}

Write-Host "CF7 readiness completed for $Machine. This is not final physical acceptance."

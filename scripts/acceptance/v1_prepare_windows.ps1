param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-f]{40}$")]
    [string]$Commit,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$")]
    [string]$HostId,

    [string]$Role = "physical-test-host",
    [string]$Operator = "Martin",

    [ValidateSet("prepare", "status", "privacy", "validate")]
    [string]$Action = "prepare"
)

$ErrorActionPreference = "Stop"
$Checkout = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Checkout

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python is not available on PATH. Install Python 3.11 or newer."
}

function Invoke-Campaign {
    param([string[]]$Arguments)
    & python -m scripts.acceptance.v1_physical_campaign --checkout $Checkout --evidence-root "evidence/v1-physical" @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Federation v1 physical campaign command failed."
    }
}

switch ($Action) {
    "prepare" {
        Invoke-Campaign @("init", "--commit", $Commit, "--operator", $Operator)
        Invoke-Campaign @("host", "--commit", $Commit, "--host", $HostId, "--role", $Role)
        Invoke-Campaign @("sample", "--commit", $Commit, "--host", $HostId, "--scenario", "P01", "--label", "pre-campaign-baseline")
        Invoke-Campaign @("status", "--commit", $Commit)
        Write-Host "Windows host prepared. Evidence is under evidence/v1-physical/."
    }
    "status" { Invoke-Campaign @("status", "--commit", $Commit) }
    "privacy" { Invoke-Campaign @("privacy", "--commit", $Commit) }
    "validate" { Invoke-Campaign @("validate", "--commit", $Commit) }
}

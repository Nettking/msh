param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-f]{40}$")]
    [string]$Commit,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$")]
    [string]$HostId,

    [Parameter(Mandatory = $true)]
    [ValidateSet("local-ai", "cnc-recorder", "school-control")]
    [string]$HostProfile,

    [string]$Operator = "Martin",

    [ValidateSet("prepare", "automate", "report", "sample", "status", "privacy", "validate")]
    [string]$Action = "prepare"
)

$ErrorActionPreference = "Stop"
$Checkout = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Checkout
$RuntimeBinding = $env:FCP_RUNTIME_BINDING

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python is not available on PATH. Install Python 3.11 or newer."
}

# Each helper writes the harness JSON straight to the host so an operator sees
# it, and leaves $LASTEXITCODE for the caller. AllowFailure is used only where a
# non-zero exit is the normal "physical work still outstanding" answer.
function Invoke-Module {
    param(
        [Parameter(Mandatory = $true)][string]$Module,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )
    & python -m $Module --checkout $Checkout --evidence-root "evidence/v1-physical" @Arguments
    if ($LASTEXITCODE -ne 0 -and -not $AllowFailure) {
        throw "Federation v1 physical campaign command failed: $Module $($Arguments -join ' ')"
    }
}

function Invoke-Campaign {
    param([Parameter(Mandatory = $true)][string[]]$Arguments, [switch]$AllowFailure)
    Invoke-Module -Module "scripts.acceptance.v1_physical_campaign" -Arguments $Arguments -AllowFailure:$AllowFailure
}

function Invoke-Runner {
    param([Parameter(Mandatory = $true)][string[]]$Arguments, [switch]$AllowFailure)
    $runnerArguments = @()
    if ($RuntimeBinding) {
        $runnerArguments += @("--runtime-binding", $RuntimeBinding)
    }
    $runnerArguments += $Arguments
    Invoke-Module -Module "scripts.acceptance.v1_physical_runner" -Arguments $runnerArguments -AllowFailure:$AllowFailure
}

function Invoke-Strict {
    param([Parameter(Mandatory = $true)][string[]]$Arguments, [switch]$AllowFailure)
    Invoke-Module -Module "scripts.acceptance.v1_physical_campaign_strict" -Arguments $Arguments -AllowFailure:$AllowFailure
}

# Non-timed scenarios only. P07 and P12 evidence must be bound to an explicit
# begin/finish run id, so a wrapper never starts one implicitly. Set
# FCP_RUNTIME_BINDING when collecting P12 resource samples.
$UntimedScenarios = @("P01", "P02", "P03", "P04", "P05", "P06", "P08", "P09", "P10", "P11")

switch ($Action) {
    "prepare" {
        Invoke-Campaign @("init", "--commit", $Commit, "--operator", $Operator)
        Invoke-Campaign @("host", "--commit", $Commit, "--host", $HostId, "--role", $HostProfile, "--profile", $HostProfile)
        Invoke-Runner @("sample", "--commit", $Commit, "--host", $HostId, "--scenario", "P01", "--label", "pre-campaign-baseline")
        Invoke-Runner @("report", "--commit", $Commit, "--host", $HostId) -AllowFailure
        Write-Host "Windows host prepared. Evidence is under evidence/v1-physical/."
    }
    "automate" {
        $failed = 0
        foreach ($scenario in $UntimedScenarios) {
            Invoke-Runner @("scenario", "--commit", $Commit, "--host", $HostId, "--scenario", $scenario) -AllowFailure
            if ($LASTEXITCODE -ne 0) { $failed = 1 }
        }
        Invoke-Runner @("report", "--commit", $Commit, "--host", $HostId) -AllowFailure
        exit $failed
    }
    "report" {
        Invoke-Runner @("report", "--commit", $Commit, "--host", $HostId) -AllowFailure
    }
    "sample" {
        Invoke-Runner @("sample", "--commit", $Commit, "--host", $HostId, "--scenario", "P01", "--label", "operator-sample")
    }
    "status" {
        Invoke-Campaign @("status", "--commit", $Commit)
    }
    "privacy" {
        Invoke-Campaign @("privacy", "--commit", $Commit)
    }
    "validate" {
        Invoke-Strict @("validate", "--commit", $Commit)
    }
}

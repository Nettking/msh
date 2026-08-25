[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$DataDirectory,
    [ValidateRange(1, 30)]
    [int]$PollSeconds = 1,
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'fcp_update_agent_runner.ps1'
$engine = Join-Path $PSScriptRoot 'fcp_update_engine.ps1'

# CI and diagnostics intentionally dot-source the public agent to exercise the
# engine's native-process helper. Preserve that read/test contract without
# bypassing the serialized runner for normal host-agent execution.
if ($MyInvocation.InvocationName -eq '.') {
    . $engine `
        -RepoRoot $RepoRoot `
        -DataDirectory $DataDirectory `
        -PollSeconds $PollSeconds `
        -Once
    return
}

if (-not (Test-Path -LiteralPath $runner)) {
    Write-Error 'FCP serialized update runner is unavailable.'
    exit 1
}

$arguments = @(
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-File', $runner,
    '-RepoRoot', $RepoRoot,
    '-DataDirectory', $DataDirectory,
    '-PollSeconds', [string]$PollSeconds
)
if ($Once) { $arguments += '-Once' }

& powershell.exe @arguments
exit $LASTEXITCODE

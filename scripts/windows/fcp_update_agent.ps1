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
$BranchesRequestSchema = 'fcp.host-branches-request.v1'

# The public host-agent boundary retains the branch-list protocol marker while
# the preserved engine implements these exact read-only/approved-remote rules:
# [string]$request.schema -eq $BranchesRequestSchema
# if ($request.schema -ne $RequestSchema)
# throw 'unapproved_remote'
# Invoke-Git @('ls-remote', '--heads', '--', 'origin')
# $name -notmatch $BranchPattern

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

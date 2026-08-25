[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [object[]]$RemainingArguments
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'fcp_update_agent_runner.ps1'
if (-not (Test-Path -LiteralPath $runner)) {
    Write-Error 'FCP serialized update runner is unavailable.'
    exit 1
}

# Preserve every named argument supplied by start.cmd or by the previous
# release's self-reload process. The runner intentionally keeps the public
# agent CLI unchanged.
$forward = @($RemainingArguments | ForEach-Object { [string]$_ })
& powershell.exe `
    -NoProfile `
    -ExecutionPolicy Bypass `
    -File $runner `
    @forward
exit $LASTEXITCODE

$ErrorActionPreference = 'Stop'

$state = Join-Path $env:RUNNER_TEMP 'fcp-update-agent-activation-smoke'
. .\scripts\windows\fcp_update_agent.ps1 `
    -RepoRoot (Resolve-Path '.').Path `
    -DataDirectory $state `
    -Once

$stderrSuccess = Invoke-ExternalResult 'powershell.exe' @(
    '-NoProfile',
    '-Command',
    "[Console]::Error.WriteLine('docker-progress'); exit 0"
)
if ($stderrSuccess.ExitCode -ne 0) {
    throw 'stderr_success_exit_changed'
}
if (($stderrSuccess.Output -join "`n") -notmatch 'docker-progress') {
    throw 'stderr_success_output_missing'
}

$resumePartial = Invoke-ExternalResult 'powershell.exe' @(
    '-NoProfile',
    '-Command',
    "[Console]::Error.WriteLine('resume-partial'); exit 4"
)
if ($resumePartial.ExitCode -ne 4) {
    throw 'resume_partial_exit_changed'
}
if (($resumePartial.Output -join "`n") -notmatch 'resume-partial') {
    throw 'resume_partial_output_missing'
}

$global:LASTEXITCODE = 0

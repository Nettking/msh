[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$Model,
    [ValidateSet('ollama', 'model-provider')]
    [string]$Target = 'ollama',
    [ValidateRange(1, 7200)]
    [int]$TimeoutSeconds = 3600
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Same shared host-resource thresholds as catalog/federation/host_resources.py.
# Model pulls are optional, unknown-size writes: they may start only above
# PRESSURE and are stopped if the host reaches PRESSURE while downloading.
$CriticalFreeBytes = [int64]10737418240
$PressureFreeBytes = [int64]12884901888
$PollMilliseconds = 250
$ModelPattern = '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$'

function Normalize-DirectoryPath([string]$Value) {
    return [System.IO.Path]::GetFullPath($Value)
}

function Invoke-NativeResult {
    param([string]$FilePath, [string[]]$Arguments)
    $previous = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $FilePath @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{
        Output = @($output | ForEach-Object { [string]$_ })
        ExitCode = [int]$exitCode
    }
}

function Get-FreeBytes {
    # Supported Windows layout: Docker Desktop's growing data image and this
    # checkout consume the same host system volume. A different Docker backing
    # resource is outside the v1 supported-layout evidence boundary.
    $drive = [System.IO.Path]::GetPathRoot((Resolve-Path $RepoRoot).Path)
    return [int64]([System.IO.DriveInfo]::New($drive)).AvailableFreeSpace
}

$RepoRoot = Normalize-DirectoryPath $RepoRoot
if ($Model -notmatch $ModelPattern) {
    Write-Error 'Invalid model identifier.'
    exit 1
}

if ($Target -eq 'model-provider') {
    $service = 'model-provider'
    $profile = 'provider'
    $installer = 'model-provider-install'
}
else {
    $service = 'ollama'
    $profile = 'model-install'
    $installer = 'ollama-pull'
}

Set-Location -LiteralPath $RepoRoot
$existing = Invoke-NativeResult 'docker' @(
    'compose', 'exec', '-T', $service, 'ollama', 'show', $Model
)
if ($existing.ExitCode -eq 0) {
    Write-Output "Ollama model is already installed: $Model"
    exit 0
}

$before = Get-FreeBytes
if ($before -le $PressureFreeBytes) {
    Write-Warning (
        "Model installation was not started because the Docker backing " +
        "resource is under pressure. Core FCP remains available."
    )
    exit 2
}

$containerName = 'fcp-model-pull-' + [guid]::NewGuid().ToString('N').Substring(0, 20)
$arguments = @(
    'compose', '--profile', $profile, 'run', '--rm', '--name', $containerName,
    '--entrypoint', '/bin/ollama', $installer, 'pull', $Model
)
$process = Start-Process -FilePath 'docker' -ArgumentList $arguments -NoNewWindow -PassThru
$deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)

while (-not $process.HasExited) {
    Start-Sleep -Milliseconds $PollMilliseconds
    $free = Get-FreeBytes
    if ($free -le $PressureFreeBytes) {
        # Disconnecting only the CLI does not prove the Ollama daemon stopped
        # writing. Stop the optional writer itself before releasing this pull.
        Invoke-NativeResult 'docker' @(
            'compose', 'stop', '--timeout', '5', $service
        ) | Out-Null
        Invoke-NativeResult 'docker' @('rm', '-f', $containerName) | Out-Null
        if (-not $process.WaitForExit(30000)) {
            $process.Kill()
            $process.WaitForExit()
        }
        Write-Warning (
            "Model installation stopped at host resource pressure. " +
            "The $CriticalFreeBytes-byte emergency floor remains reserved for core recovery."
        )
        exit 2
    }
    if ([DateTimeOffset]::UtcNow -ge $deadline) {
        Invoke-NativeResult 'docker' @(
            'compose', 'stop', '--timeout', '5', $service
        ) | Out-Null
        Invoke-NativeResult 'docker' @('rm', '-f', $containerName) | Out-Null
        if (-not $process.WaitForExit(30000)) {
            $process.Kill()
            $process.WaitForExit()
        }
        Write-Error 'Model installation exceeded its bounded host-side deadline.'
        exit 1
    }
}

if ($process.ExitCode -ne 0) {
    Write-Error "Model installation exited with code $($process.ExitCode)."
    exit 1
}

$verified = Invoke-NativeResult 'docker' @(
    'compose', 'exec', '-T', $service, 'ollama', 'show', $Model
)
if ($verified.ExitCode -ne 0) {
    Write-Error 'Model installation finished but the selected model could not be verified.'
    exit 1
}

Write-Output "Ollama model is installed and verified: $Model"
exit 0

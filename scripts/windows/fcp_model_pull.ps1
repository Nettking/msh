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

# Same host-resource thresholds as catalog/federation/host_resources.py.
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

function Get-DockerBackingPath {
    # Docker Desktop persists Linux-container volumes in a host VHDX. Measure
    # the Windows volume containing that file, not the checkout drive. Support
    # both current and legacy default Docker Desktop locations. A custom or
    # otherwise unprovable location fails closed for a new model download.
    if (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
        $candidates = @(
            (Join-Path $env:LOCALAPPDATA 'Docker\wsl\disk\docker_data.vhdx'),
            (Join-Path $env:LOCALAPPDATA 'Docker\wsl\data\ext4.vhdx')
        )
        foreach ($candidate in $candidates) {
            if (Test-Path -LiteralPath $candidate -PathType Leaf) {
                return (Get-Item -LiteralPath $candidate).Directory.FullName
            }
        }
    }

    # Native Windows Docker engines can expose their data root as an ordinary
    # host directory. This is intentionally a fallback after Docker Desktop.
    if (-not [string]::IsNullOrWhiteSpace($env:PROGRAMDATA)) {
        $nativeRoot = Join-Path $env:PROGRAMDATA 'docker'
        if (Test-Path -LiteralPath $nativeRoot -PathType Container) {
            return (Resolve-Path -LiteralPath $nativeRoot).Path
        }
    }
    return $null
}

function Get-FreeBytes([string]$BackingPath) {
    $drive = [System.IO.Path]::GetPathRoot((Resolve-Path -LiteralPath $BackingPath).Path)
    return [int64]([System.IO.DriveInfo]::New($drive)).AvailableFreeSpace
}

function Test-ModelWriterStopped([string]$Service, [string]$ContainerName) {
    $serviceProbe = Invoke-NativeResult 'docker' @(
        'compose', 'ps', '--status', 'running', '-q', $Service
    )
    $pullProbe = Invoke-NativeResult 'docker' @(
        'ps', '-q', '--filter', "name=^/$ContainerName$"
    )
    if ($serviceProbe.ExitCode -ne 0 -or $pullProbe.ExitCode -ne 0) {
        return $false
    }
    return (
        [string]::IsNullOrWhiteSpace(($serviceProbe.Output -join '')) -and
        [string]::IsNullOrWhiteSpace(($pullProbe.Output -join ''))
    )
}

function Stop-ModelWriter([string]$Service, [string]$ContainerName) {
    Invoke-NativeResult 'docker' @(
        'compose', 'stop', '--timeout', '5', $Service
    ) | Out-Null
    Invoke-NativeResult 'docker' @('rm', '-f', $ContainerName) | Out-Null
    if (Test-ModelWriterStopped $Service $ContainerName) {
        return $true
    }

    # Escalate once, then verify again. Docker command failure must never be
    # translated into a false claim that the unknown-size writer stopped.
    Invoke-NativeResult 'docker' @('compose', 'kill', $Service) | Out-Null
    Invoke-NativeResult 'docker' @('rm', '-f', $ContainerName) | Out-Null
    return [bool](Test-ModelWriterStopped $Service $ContainerName)
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

$backingPath = Get-DockerBackingPath
if ([string]::IsNullOrWhiteSpace($backingPath)) {
    Write-Error (
        'Model installation was not started because FCP could not prove the ' +
        'Windows host resource backing Docker model storage.'
    )
    exit 3
}

$before = Get-FreeBytes $backingPath
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
    $free = Get-FreeBytes $backingPath
    if ($free -le $PressureFreeBytes) {
        $stopped = Stop-ModelWriter $service $containerName
        if (-not $process.WaitForExit(30000)) {
            $process.Kill()
            $process.WaitForExit()
        }
        if (-not $stopped) {
            Write-Error (
                'Docker reached host resource pressure and FCP could not prove ' +
                'that the optional model writer stopped.'
            )
            exit 3
        }
        Write-Warning (
            "Model installation stopped when free space reached the shared " +
            "pressure threshold ($PressureFreeBytes bytes; critical threshold " +
            "is $CriticalFreeBytes bytes). Core FCP can continue without AI."
        )
        exit 2
    }
    if ([DateTimeOffset]::UtcNow -ge $deadline) {
        $stopped = Stop-ModelWriter $service $containerName
        if (-not $process.WaitForExit(30000)) {
            $process.Kill()
            $process.WaitForExit()
        }
        if (-not $stopped) {
            Write-Error (
                'The model pull deadline expired and FCP could not prove that ' +
                'the optional model writer stopped.'
            )
            exit 3
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

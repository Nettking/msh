Set-StrictMode -Version Latest

# Keep these byte thresholds aligned with catalog/federation/host_resources.py.
$script:FcpCriticalFreeBytes = [int64]10737418240
$script:FcpPressureFreeBytes = [int64]12884901888
$script:FcpWarningFreeBytes = [int64]17179869184

function Invoke-FcpDockerNativeResult {
    param([string]$FilePath, [string[]]$Arguments)
    $previous = $ErrorActionPreference
    $output = @()
    $exitCode = 127
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $FilePath @Arguments 2>&1
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
    }
    catch {
        $output = @($_.Exception.Message)
        $exitCode = 127
    }
    finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{
        Output = @($output | ForEach-Object { [string]$_ })
        ExitCode = [int]$exitCode
    }
}

function Get-FcpDockerBackingPath {
    param([Parameter(Mandatory = $true)][string]$RepoRoot)

    # Docker Desktop persists Linux-container storage in a host VHDX. Measure
    # the Windows volume containing that file, never the checkout drive merely
    # because the checkout happens to be there.
    if (-not [string]::IsNullOrWhiteSpace([string]$env:LOCALAPPDATA)) {
        $candidates = @(
            (Join-Path $env:LOCALAPPDATA 'Docker\wsl\disk\docker_data.vhdx'),
            (Join-Path $env:LOCALAPPDATA 'Docker\wsl\data\ext4.vhdx')
        )
        foreach ($candidate in $candidates) {
            try {
                if (Test-Path -LiteralPath $candidate -PathType Leaf) {
                    return (Get-Item -LiteralPath $candidate).Directory.FullName
                }
            }
            catch {}
        }
    }

    # Native Windows Docker is accepted only when the active engine itself
    # reports a concrete absolute Windows DockerRootDir. A Linux DockerRootDir
    # returned through Docker Desktop is not a host measurement path.
    $info = Invoke-FcpDockerNativeResult 'docker' @(
        'info', '--format', '{{.OSType}}|{{.DockerRootDir}}'
    )
    if ($info.ExitCode -ne 0) { return $null }
    $parts = (($info.Output -join '').Trim()).Split('|', 2)
    if ($parts.Count -ne 2 -or $parts[0].Trim().ToLowerInvariant() -ne 'windows') {
        return $null
    }
    $nativeRoot = $parts[1].Trim()
    if (-not [System.IO.Path]::IsPathRooted($nativeRoot)) { return $null }
    try {
        if (Test-Path -LiteralPath $nativeRoot -PathType Container) {
            return (Resolve-Path -LiteralPath $nativeRoot).Path
        }
    }
    catch {}
    return $null
}

function Get-FcpResourceFreeBytes {
    param([Parameter(Mandatory = $true)][string]$BackingPath)
    try {
        $resolved = (Resolve-Path -LiteralPath $BackingPath).Path
        $drive = [System.IO.Path]::GetPathRoot($resolved)
        return [int64]([System.IO.DriveInfo]::New($drive)).AvailableFreeSpace
    }
    catch {
        return [int64]-1
    }
}

function Get-FcpResourcePressureLevel {
    param([Parameter(Mandatory = $true)][int64]$FreeBytes)
    if ($FreeBytes -lt 0 -or $FreeBytes -le $script:FcpCriticalFreeBytes) {
        return 'critical'
    }
    if ($FreeBytes -le $script:FcpPressureFreeBytes) { return 'pressure' }
    if ($FreeBytes -le $script:FcpWarningFreeBytes) { return 'warning' }
    return 'normal'
}

function Get-FcpDockerResourceAssessment {
    param([Parameter(Mandatory = $true)][string]$RepoRoot)
    $backingPath = Get-FcpDockerBackingPath -RepoRoot $RepoRoot
    if ([string]::IsNullOrWhiteSpace([string]$backingPath)) {
        return [pscustomobject]@{
            BackingPath = $null
            FreeBytes = [int64]-1
            Level = 'critical'
            Proven = $false
        }
    }
    $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
    return [pscustomobject]@{
        BackingPath = [string]$backingPath
        FreeBytes = [int64]$freeBytes
        Level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
        Proven = ($freeBytes -ge 0)
    }
}

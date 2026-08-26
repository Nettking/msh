[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputFile,
    [ValidateRange(0, 600)]
    [int]$LockTimeoutSeconds = 30,
    [AllowNull()][string]$ExpectedCommit = $null,
    [switch]$LeaseAlreadyHeld
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$UpdateRequiredFreeBytes = 10737418240
$BuildCacheKeepBytes = 8589934592
$OidPattern = '^[0-9a-f]{40}$'
$DockerResourceHelper = Join-Path $PSScriptRoot 'fcp_docker_resource.ps1'
if (-not (Test-Path -LiteralPath $DockerResourceHelper -PathType Leaf)) {
    throw 'docker_resource_helper_unavailable'
}
. $DockerResourceHelper

function Normalize-DirectoryPath([string]$Value) {
    $full = [System.IO.Path]::GetFullPath($Value)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ($full.Length -le $root.Length) { return $full }
    $separators = [char[]]@(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    return $full.TrimEnd($separators)
}

function Get-PathHash([string]$Value) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value.ToLowerInvariant())
        return ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').Substring(0, 24)
    }
    finally {
        $sha.Dispose()
    }
}

function Last-Text([object[]]$Values) {
    $last = $Values | Select-Object -Last 1
    if ($null -eq $last) { return '' }
    return [string]$last
}

function Invoke-Git([string[]]$Arguments) {
    $output = & git -C $RepoRoot @Arguments 2>&1
    $exit = $LASTEXITCODE
    if ($exit -ne 0) {
        throw "git_failed:$($Arguments[0]):$exit"
    }
    return @($output | ForEach-Object { [string]$_ })
}

function Get-CleanCommit {
    $commit = (Last-Text (Invoke-Git @('rev-parse', '--verify', 'HEAD^{commit}'))).Trim().ToLowerInvariant()
    if ($commit -notmatch $OidPattern) { throw 'unsupported_checkout' }
    $status = Invoke-Git @('status', '--porcelain=v1', '--untracked-files=all')
    if (($status -join '').Length -gt 0) { throw 'dirty_build_context' }
    return $commit
}

function Invoke-BuildCachePrune {
    & docker builder prune --force "--keep-storage=$BuildCacheKeepBytes"
    return ($LASTEXITCODE -eq 0)
}

function Assert-DiskPreflight {
    $backingPath = Get-FcpDockerBackingPath -RepoRoot $RepoRoot
    if ([string]::IsNullOrWhiteSpace([string]$backingPath)) {
        throw 'docker_backing_resource_unproven'
    }
    $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
    $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
    if ($level -in @('normal', 'warning')) { return }

    Invoke-BuildCachePrune | Out-Null
    $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
    $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
    if ($level -in @('normal', 'warning')) { return }
    throw 'insufficient_disk_for_update'
}

function Write-AtomicText([string]$Path, [string]$Value) {
    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent)) {
        New-Item -ItemType Directory -Path $parent -Force | Out-Null
    }
    $temporary = "$Path.tmp-$PID-$([guid]::NewGuid().ToString('N'))"
    [System.IO.File]::WriteAllText(
        $temporary,
        $Value + [Environment]::NewLine,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Move-Item -LiteralPath $temporary -Destination $Path -Force
}

$RepoRoot = Normalize-DirectoryPath $RepoRoot
$OutputFile = [System.IO.Path]::GetFullPath($OutputFile)
Set-Location -LiteralPath $RepoRoot

$mutationMutex = $null
$acquired = $false
$ownsMutationMutex = $false
try {
    if ($LeaseAlreadyHeld) {
        if ($env:FCP_HOST_MUTATION_LEASE_ACTIVE -ne '1') {
            throw 'host_mutation_lease_missing'
        }
    }
    else {
        $mutexName = 'Global\FCPHostMutation-' + (Get-PathHash $RepoRoot)
        $mutationMutex = [System.Threading.Mutex]::new($false, $mutexName)
        try {
            $acquired = $mutationMutex.WaitOne([TimeSpan]::FromSeconds($LockTimeoutSeconds))
        }
        catch [System.Threading.AbandonedMutexException] {
            $acquired = $true
        }
        if (-not $acquired) { throw 'host_mutation_busy' }
        $ownsMutationMutex = $true
    }

    $commit = Get-CleanCommit
    if (-not [string]::IsNullOrWhiteSpace($ExpectedCommit)) {
        $expected = $ExpectedCommit.Trim().ToLowerInvariant()
        if ($expected -notmatch $OidPattern -or $commit -ne $expected) {
            throw 'source_verification_failed'
        }
    }

    $env:FCP_BUILD_COMMIT = $commit
    Assert-DiskPreflight

    $buildExit = 0
    & docker compose build relay flask recorder
    $buildExit = $LASTEXITCODE

    $pruneOk = Invoke-BuildCachePrune
    if (-not $pruneOk) {
        if ($buildExit -ne 0) { throw 'build_failed_and_cache_prune_failed' }
        throw 'build_cache_prune_failed'
    }
    if ($buildExit -ne 0) { throw "core_image_build_failed:$buildExit" }

    $after = Get-CleanCommit
    if ($after -ne $commit) { throw 'build_context_changed' }

    Write-AtomicText $OutputFile $commit
    exit 0
}
catch {
    Write-Error "FCP host build refused: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($ownsMutationMutex -and $acquired -and $null -ne $mutationMutex) {
        try { $mutationMutex.ReleaseMutex() | Out-Null } catch {}
    }
    if ($null -ne $mutationMutex) {
        $mutationMutex.Dispose()
    }
}

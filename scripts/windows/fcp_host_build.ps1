[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [AllowNull()][string]$OutputFile = $null,
    [ValidateRange(0, 600)]
    [int]$LockTimeoutSeconds = 30,
    [AllowNull()][string]$ExpectedCommit = $null,
    [switch]$LeaseAlreadyHeld,
    [switch]$CacheCleanupOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$UpdateRequiredFreeBytes = 10737418240
$BuildCacheKeepBytes = 8589934592
$BuildTimeoutSeconds = 900
$BuildPollMilliseconds = 250
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

$script:DockerExe = $null

function Resolve-DockerExecutable {
    $inherited = [string]$env:FCP_REAL_DOCKER_EXE
    if (-not [string]::IsNullOrWhiteSpace($inherited)) {
        $candidate = [System.IO.Path]::GetFullPath($inherited)
        if (Test-Path -LiteralPath $candidate -PathType Leaf) {
            $script:DockerExe = $candidate
            return
        }
        throw 'docker_executable_unavailable'
    }
    $command = Get-Command docker -CommandType Application -ErrorAction Stop | Select-Object -First 1
    if ($null -eq $command -or [string]::IsNullOrWhiteSpace([string]$command.Source)) {
        throw 'docker_executable_unavailable'
    }
    $script:DockerExe = [string]$command.Source
}

function Invoke-DockerResult([string[]]$Arguments) {
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $script:DockerExe @Arguments 2>&1
        $exit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    return [pscustomobject]@{
        Output = @($output | ForEach-Object { [string]$_ })
        ExitCode = [int]$exit
    }
}

function Get-FcpBuilderName {
    return 'fcp-build-' + (Get-PathHash $RepoRoot)
}

function Get-FcpBuilderInspection([string]$Name) {
    return Invoke-DockerResult @('buildx', 'inspect', $Name)
}

function Get-FcpBuilderDriver([object]$Inspection) {
    foreach ($line in @($Inspection.Output)) {
        $text = ([string]$line).Trim()
        if ($text -match '^Driver:\s*(?<driver>\S+)\s*$') {
            return [string]$Matches.driver
        }
    }
    return ''
}

function Test-FcpBuilderStopped([string]$Name) {
    $inspection = Get-FcpBuilderInspection $Name
    if ($inspection.ExitCode -ne 0) {
        # A removed builder cannot own a live BuildKit node.
        return $true
    }
    $statuses = @()
    foreach ($line in @($inspection.Output)) {
        $text = ([string]$line).Trim()
        if ($text -match '^Status:\s*(?<status>\S+)\s*$') {
            $statuses += ([string]$Matches.status).ToLowerInvariant()
        }
    }
    if ($statuses.Count -eq 0) { return $false }
    foreach ($status in $statuses) {
        if ($status -in @('running', 'starting')) { return $false }
    }
    return $true
}

function Remove-FcpBuilder([string]$Name) {
    $inspection = Get-FcpBuilderInspection $Name
    if ($inspection.ExitCode -ne 0) { return $true }
    $removed = Invoke-DockerResult @(
        'buildx', 'rm', '--force', '--timeout', '20s', $Name
    )
    if ($removed.ExitCode -ne 0) { return $false }
    return (Get-FcpBuilderInspection $Name).ExitCode -ne 0
}

function Stop-FcpBuildWriter([string]$Name, [switch]$DiscardCache) {
    $stopped = Invoke-DockerResult @('buildx', 'stop', $Name)
    if ($stopped.ExitCode -eq 0 -and (Test-FcpBuilderStopped $Name)) {
        if ($DiscardCache) {
            # Once the BuildKit writer is positively stopped, cache deletion is
            # reconstructible cleanup. A deletion failure does not make it live.
            Remove-FcpBuilder $Name | Out-Null
        }
        return $true
    }
    return Remove-FcpBuilder $Name
}

function Ensure-FcpControllableBuilder {
    $name = Get-FcpBuilderName
    $inspection = Get-FcpBuilderInspection $name
    if ($inspection.ExitCode -eq 0) {
        if ((Get-FcpBuilderDriver $inspection) -ne 'docker-container') {
            throw 'controllable_builder_conflict'
        }
        return $name
    }

    $created = Invoke-DockerResult @(
        'buildx', 'create',
        '--name', $name,
        '--driver', 'docker-container',
        '--driver-opt', 'default-load=true'
    )
    if ($created.ExitCode -ne 0) { throw 'controllable_builder_unavailable' }
    $inspection = Get-FcpBuilderInspection $name
    if (
        $inspection.ExitCode -ne 0 -or
        (Get-FcpBuilderDriver $inspection) -ne 'docker-container'
    ) {
        throw 'controllable_builder_unavailable'
    }
    return $name
}

function Invoke-BuildCachePrune {
    $name = Get-FcpBuilderName
    $inspection = Get-FcpBuilderInspection $name
    if ($inspection.ExitCode -ne 0) { return $true }

    $pruned = Invoke-DockerResult @(
        'buildx', 'prune',
        '--builder', $name,
        '--force',
        "--keep-storage=$BuildCacheKeepBytes"
    )
    if ($pruned.ExitCode -ne 0) { return $false }
    return Stop-FcpBuildWriter $name
}

function Stop-BuildClient([System.Diagnostics.Process]$Process) {
    if ($Process.HasExited) { return }
    try {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
    }
    catch {}
    try { $Process.WaitForExit(5000) | Out-Null } catch {}
}

function Invoke-ControlledCoreBuild([string]$BackingPath) {
    $name = Ensure-FcpControllableBuilder
    $docker = $script:DockerExe
    $arguments = @(
        'compose', 'build',
        '--builder', $name,
        'relay', 'flask', 'recorder'
    )

    try {
        $process = Start-Process `
            -FilePath $docker `
            -ArgumentList $arguments `
            -NoNewWindow `
            -PassThru
    }
    catch {
        throw 'core_image_build_failed'
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($BuildTimeoutSeconds)
    while (-not $process.HasExited) {
        $freeBytes = Get-FcpResourceFreeBytes -BackingPath $BackingPath
        $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
        if ($level -in @('pressure', 'critical')) {
            Stop-BuildClient $process
            if (-not (Stop-FcpBuildWriter $name -DiscardCache)) {
                throw 'build_writer_stop_unverified'
            }
            throw 'build_resource_pressure'
        }
        if ([DateTimeOffset]::UtcNow -ge $deadline) {
            Stop-BuildClient $process
            if (-not (Stop-FcpBuildWriter $name -DiscardCache)) {
                throw 'build_writer_stop_unverified'
            }
            throw 'core_image_build_timeout'
        }
        Start-Sleep -Milliseconds $BuildPollMilliseconds
    }

    $exit = [int]$process.ExitCode
    if ($exit -ne 0) {
        if (-not (Stop-FcpBuildWriter $name)) {
            throw 'build_writer_stop_unverified'
        }
        throw "core_image_build_failed:$exit"
    }
    if (-not (Invoke-BuildCachePrune)) {
        if (-not (Stop-FcpBuildWriter $name)) {
            throw 'build_writer_stop_unverified'
        }
        throw 'build_cache_prune_failed'
    }
    if (-not (Stop-FcpBuildWriter $name)) {
        throw 'build_writer_stop_unverified'
    }
}

function Assert-CoreImageCommits([string]$Commit) {
    foreach ($service in @('relay', 'flask', 'recorder')) {
        $image = Invoke-DockerResult @('compose', 'images', '-q', $service)
        $imageId = (Last-Text $image.Output).Trim()
        if ($image.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($imageId)) {
            throw 'built_image_identity_unavailable'
        }
        $label = Invoke-DockerResult @(
            'image', 'inspect',
            '--format', '{{ index .Config.Labels "no.fcp.build_commit" }}',
            $imageId
        )
        if (
            $label.ExitCode -ne 0 -or
            (Last-Text $label.Output).Trim().ToLowerInvariant() -ne $Commit
        ) {
            throw 'built_image_identity_mismatch'
        }
    }
}

function Assert-DiskPreflight {
    $backingPath = Get-FcpDockerBackingPath -RepoRoot $RepoRoot
    if ([string]::IsNullOrWhiteSpace([string]$backingPath)) {
        throw 'docker_backing_resource_unproven'
    }
    $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
    $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
    if ($level -in @('normal', 'warning')) { return $backingPath }

    Invoke-BuildCachePrune | Out-Null
    $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
    $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
    if ($level -in @('normal', 'warning')) { return $backingPath }
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
if (-not $CacheCleanupOnly) {
    if ([string]::IsNullOrWhiteSpace([string]$OutputFile)) {
        throw 'build_output_file_required'
    }
    $OutputFile = [System.IO.Path]::GetFullPath([string]$OutputFile)
}
Set-Location -LiteralPath $RepoRoot
Resolve-DockerExecutable

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

    if ($CacheCleanupOnly) {
        $backingPath = Get-FcpDockerBackingPath -RepoRoot $RepoRoot
        if ([string]::IsNullOrWhiteSpace([string]$backingPath)) {
            throw 'docker_backing_resource_unproven'
        }
        $freeBytes = Get-FcpResourceFreeBytes -BackingPath $backingPath
        $level = Get-FcpResourcePressureLevel -FreeBytes $freeBytes
        if ($level -in @('pressure', 'critical')) {
            if (-not (Invoke-BuildCachePrune)) {
                throw 'build_cache_prune_failed'
            }
        }
        else {
            $name = Get-FcpBuilderName
            if (-not (Test-FcpBuilderStopped $name) -and -not (Stop-FcpBuildWriter $name)) {
                throw 'build_writer_stop_unverified'
            }
        }
        exit 0
    }

    $commit = Get-CleanCommit
    if (-not [string]::IsNullOrWhiteSpace($ExpectedCommit)) {
        $expected = $ExpectedCommit.Trim().ToLowerInvariant()
        if ($expected -notmatch $OidPattern -or $commit -ne $expected) {
            throw 'source_verification_failed'
        }
    }

    $env:FCP_BUILD_COMMIT = $commit
    $backingPath = Assert-DiskPreflight
    Invoke-ControlledCoreBuild $backingPath
    Assert-CoreImageCommits $commit

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

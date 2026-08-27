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
$DockerLifecycleTimeoutSeconds = 30
$OidPattern = '^[0-9a-f]{40}$'
# Only these Buildx node states positively establish a stopped writer. Anything
# else, including an unparseable or newly introduced state, is refused.
$BuilderQuiescentStates = @('inactive', 'stopped')
# The docker-container driver backs a builder with a container named after it.
$BuildKitContainerPrefix = 'buildx_buildkit_'
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
    if (
        $env:FCP_CONTROLLED_BUILD_ACTIVE -eq '1' -and
        -not [string]::IsNullOrWhiteSpace($inherited)
    ) {
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

function Invoke-BoundedDockerResult(
    [string[]]$Arguments,
    [int]$TimeoutSeconds = $DockerLifecycleTimeoutSeconds
) {
    if ($TimeoutSeconds -le 0) { throw 'docker_lifecycle_timeout_invalid' }
    $stdoutPath = $null
    $stderrPath = $null
    $process = $null
    $exitCode = 127
    $failure = $null
    try {
        # Created inside the guarded region: a temp-file failure out here would
        # escape as an unhandled exception and skip required stop/prune work,
        # instead of being reported as the ordinary bounded failure that every
        # caller already treats fail-closed.
        $stdoutPath = [System.IO.Path]::GetTempFileName()
        $stderrPath = [System.IO.Path]::GetTempFileName()
        $process = Start-Process `
            -FilePath $script:DockerExe `
            -ArgumentList $Arguments `
            -NoNewWindow `
            -PassThru `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            # Kill the tree, not just the CLI: "docker buildx" runs the Buildx
            # plugin as a child, and a surviving plugin would keep mutating
            # builder state and cache after the mutation lock is released.
            try { & taskkill.exe /PID $process.Id /T /F 2>&1 | Out-Null } catch {}
            try { $process.WaitForExit(2000) | Out-Null } catch {}
            if (-not $process.HasExited) {
                try { $process.Kill() } catch {}
                try { $process.WaitForExit(2000) | Out-Null } catch {}
            }
            $exitCode = 124
        }
        else {
            $exitCode = [int]$process.ExitCode
        }
    }
    catch {
        $failure = $_.Exception.Message
        $exitCode = 127
    }
    finally {
        $output = @()
        try {
            if (
                -not [string]::IsNullOrWhiteSpace([string]$stdoutPath) -and
                (Test-Path -LiteralPath $stdoutPath)
            ) {
                $output += @(Get-Content -LiteralPath $stdoutPath -ErrorAction SilentlyContinue)
            }
            if (
                -not [string]::IsNullOrWhiteSpace([string]$stderrPath) -and
                (Test-Path -LiteralPath $stderrPath)
            ) {
                $output += @(Get-Content -LiteralPath $stderrPath -ErrorAction SilentlyContinue)
            }
        }
        catch {}
        if (-not [string]::IsNullOrWhiteSpace([string]$failure)) {
            $output += [string]$failure
        }
        if (-not [string]::IsNullOrWhiteSpace([string]$stdoutPath)) {
            Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
        }
        if (-not [string]::IsNullOrWhiteSpace([string]$stderrPath)) {
            Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
        }
    }
    return [pscustomobject]@{
        Output = @($output | ForEach-Object { [string]$_ })
        ExitCode = [int]$exitCode
    }
}

function Get-FcpBuilderName {
    return 'fcp-build-' + (Get-PathHash $RepoRoot)
}

function Get-FcpBuilderInspection([string]$Name) {
    return Invoke-BoundedDockerResult @('buildx', 'inspect', $Name)
}

function Test-FcpBuildKitContainerRunning([string]$Name) {
    # Fails closed: if the running set cannot be established the writer is
    # treated as possibly live rather than assumed gone.
    $listed = Invoke-BoundedDockerResult @('ps', '--format', '{{.Names}}')
    if ($listed.ExitCode -ne 0) { return $true }
    $prefix = $BuildKitContainerPrefix + $Name
    foreach ($line in @($listed.Output)) {
        if (([string]$line).Trim().StartsWith($prefix)) { return $true }
    }
    return $false
}

function Test-FcpBuilderAbsent([string]$Name) {
    $listed = Invoke-BoundedDockerResult @('buildx', 'ls', '--format', '{{.Name}}')
    if ($listed.ExitCode -ne 0) { return $false }
    foreach ($line in @($listed.Output)) {
        if (([string]$line).Trim() -eq $Name) { return $false }
    }
    # Buildx can drop its own store entry while the docker-container driver's
    # BuildKit container keeps running, so disappearing from enumeration does
    # not by itself prove the writer is gone.
    return -not (Test-FcpBuildKitContainerRunning $Name)
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
        return Test-FcpBuilderAbsent $Name
    }
    $statuses = @()
    foreach ($line in @($inspection.Output)) {
        $text = ([string]$line).Trim()
        if ($text -match '^Status:\s*(?<status>\S+)\s*$') {
            $statuses += ([string]$Matches.status).ToLowerInvariant()
        }
    }
    if ($statuses.Count -eq 0) { return $false }
    # Whitelist, not blacklist: an unrecognised or newly introduced Buildx state
    # is refused rather than read as safe, because cache is discarded on the
    # strength of this reading.
    foreach ($status in $statuses) {
        if ($status -notin $BuilderQuiescentStates) { return $false }
    }
    # The builder record saying "stopped" describes the record, not the process
    # holding the cache open, so the driver container is checked as well.
    return -not (Test-FcpBuildKitContainerRunning $Name)
}

function Remove-FcpBuilder([string]$Name) {
    $inspection = Get-FcpBuilderInspection $Name
    if ($inspection.ExitCode -ne 0) { return Test-FcpBuilderAbsent $Name }
    $removed = Invoke-BoundedDockerResult @(
        'buildx', 'rm', '--force', '--timeout', '20s', $Name
    )
    if ($removed.ExitCode -ne 0) { return $false }
    return Test-FcpBuilderAbsent $Name
}

function Settle-FcpBuildWriter([string]$Name, [switch]$DiscardCache) {
    # Quiescence and cache discard are separate claims. A caller that promises
    # the cache was bounded must not be handed a result that only proves the
    # writer stopped, so both facts are reported rather than collapsed.
    $stopped = Invoke-BoundedDockerResult @('buildx', 'stop', $Name)
    if ($stopped.ExitCode -eq 0 -and (Test-FcpBuilderStopped $Name)) {
        if (-not $DiscardCache) {
            return [pscustomobject]@{ Quiescent = $true; CacheDiscarded = $false }
        }
        return [pscustomobject]@{
            Quiescent = $true
            CacheDiscarded = [bool](Remove-FcpBuilder $Name)
        }
    }
    $removed = [bool](Remove-FcpBuilder $Name)
    return [pscustomobject]@{ Quiescent = $removed; CacheDiscarded = $removed }
}

function Stop-FcpBuildWriter([string]$Name, [switch]$DiscardCache) {
    return (Settle-FcpBuildWriter $Name -DiscardCache:$DiscardCache).Quiescent
}

function Ensure-FcpControllableBuilder {
    $composeHelp = Invoke-DockerResult @('compose', 'build', '--help')
    if (
        $composeHelp.ExitCode -ne 0 -or
        -not ((@($composeHelp.Output) -join "`n") -match '(?m)^\s*--builder(?:\s|$)')
    ) {
        throw 'controllable_builder_unavailable'
    }

    $name = Get-FcpBuilderName
    $inspection = Get-FcpBuilderInspection $name
    if ($inspection.ExitCode -eq 0) {
        if ((Get-FcpBuilderDriver $inspection) -ne 'docker-container') {
            throw 'controllable_builder_conflict'
        }
        if (-not (Test-FcpBuilderStopped $name) -and -not (Stop-FcpBuildWriter $name)) {
            throw 'build_writer_stop_unverified'
        }
        return $name
    }

    $created = Invoke-BoundedDockerResult @(
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
    if ($inspection.ExitCode -ne 0) { return Test-FcpBuilderAbsent $name }

    $pruned = Invoke-BoundedDockerResult @(
        'buildx', 'prune',
        '--builder', $name,
        '--force',
        "--keep-storage=$BuildCacheKeepBytes"
    )
    if ($pruned.ExitCode -ne 0) {
        Stop-FcpBuildWriter $name | Out-Null
        return $false
    }
    return Stop-FcpBuildWriter $name
}

function Stop-BuildClient([System.Diagnostics.Process]$Process) {
    if ($Process.HasExited) { return $true }
    try {
        & taskkill.exe /PID $Process.Id /T /F 2>&1 | Out-Null
    }
    catch {}
    try { $Process.WaitForExit(5000) | Out-Null } catch {}
    if (-not $Process.HasExited) {
        try {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        }
        catch {}
        try { $Process.WaitForExit(2000) | Out-Null } catch {}
    }
    return [bool]$Process.HasExited
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
            $clientStopped = Stop-BuildClient $process
            $settled = Settle-FcpBuildWriter $name -DiscardCache
            if (-not $clientStopped -or -not $settled.Quiescent) {
                throw 'build_writer_stop_unverified'
            }
            if (-not $settled.CacheDiscarded) {
                throw 'build_cache_discard_failed'
            }
            throw 'build_resource_pressure'
        }
        if ([DateTimeOffset]::UtcNow -ge $deadline) {
            $clientStopped = Stop-BuildClient $process
            $settled = Settle-FcpBuildWriter $name -DiscardCache
            if (-not $clientStopped -or -not $settled.Quiescent) {
                throw 'build_writer_stop_unverified'
            }
            if (-not $settled.CacheDiscarded) {
                throw 'build_cache_discard_failed'
            }
            throw 'core_image_build_timeout'
        }
        Start-Sleep -Milliseconds $BuildPollMilliseconds
    }

    $exit = [int]$process.ExitCode
    if ($exit -ne 0) {
        $cleanupOk = Invoke-BuildCachePrune
        if (-not $cleanupOk) {
            if (-not (Stop-FcpBuildWriter $name)) {
                throw 'build_writer_stop_unverified'
            }
            throw 'build_failed_and_cache_prune_failed'
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
    $name = Get-FcpBuilderName
    if ($level -in @('normal', 'warning')) {
        if (-not (Test-FcpBuilderStopped $name) -and -not (Stop-FcpBuildWriter $name)) {
            throw 'build_writer_stop_unverified'
        }
        return $backingPath
    }

    $settled = Settle-FcpBuildWriter $name -DiscardCache
    if (-not $settled.Quiescent) {
        throw 'build_writer_stop_unverified'
    }
    if (-not $settled.CacheDiscarded) {
        throw 'build_cache_discard_failed'
    }
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
        # Quiescence is proved on every branch before anything is discarded.
        # Pruning is the branch that destroys cache, so it is the branch that
        # most needs the writer proven dead first: an abandoned checkout-scoped
        # BuildKit daemon left by an earlier crash would otherwise still be
        # writing into the cache being pruned.
        $name = Get-FcpBuilderName
        if (-not (Test-FcpBuilderStopped $name) -and -not (Stop-FcpBuildWriter $name)) {
            throw 'build_writer_stop_unverified'
        }
        if ($level -in @('pressure', 'critical')) {
            if (-not (Invoke-BuildCachePrune)) {
                throw 'build_cache_prune_failed'
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
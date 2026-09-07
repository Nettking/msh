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
$BuilderQuiescentStates = @('inactive', 'stopped')
$BuildKitContainerPrefix = 'buildx_buildkit_'
$BuilderPrefix = 'fcp-build-'
$BuilderRootEnv = 'FCP_BUILDER_ROOT_HEX'
$MaxListedBuildersPerPass = 32
$MaxExaminedBuildersPerPass = 16
$MaxRemovalAttemptsPerPass = 8
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

function ConvertTo-WindowsProcessArgument([AllowNull()][string]$Value) {
    if ($null -eq $Value) { return '""' }

    $builder = New-Object System.Text.StringBuilder
    [void]$builder.Append('"')
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') {
            $backslashes++
            continue
        }
        if ($character -eq '"') {
            for ($index = 0; $index -lt (2 * $backslashes + 1); $index++) {
                [void]$builder.Append('\')
            }
            [void]$builder.Append('"')
            $backslashes = 0
            continue
        }
        for ($index = 0; $index -lt $backslashes; $index++) {
            [void]$builder.Append('\')
        }
        $backslashes = 0
        [void]$builder.Append($character)
    }
    for ($index = 0; $index -lt (2 * $backslashes); $index++) {
        [void]$builder.Append('\')
    }
    [void]$builder.Append('"')
    return $builder.ToString()
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
        $stdoutPath = [System.IO.Path]::GetTempFileName()
        $stderrPath = [System.IO.Path]::GetTempFileName()
        $startInfo = New-Object System.Diagnostics.ProcessStartInfo
        $startInfo.FileName = $script:DockerExe
        $startInfo.Arguments = (($Arguments | ForEach-Object {
            ConvertTo-WindowsProcessArgument $_
        }) -join ' ')
        $startInfo.UseShellExecute = $false
        $startInfo.CreateNoWindow = $true
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $startInfo
        if (-not $process.Start()) { throw 'docker_process_start_failed' }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
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
        $stdoutText = $stdoutTask.GetAwaiter().GetResult()
        $stderrText = $stderrTask.GetAwaiter().GetResult()
        [System.IO.File]::WriteAllText($stdoutPath, $stdoutText)
        [System.IO.File]::WriteAllText($stderrPath, $stderrText)
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
    return $BuilderPrefix + (Get-PathHash $RepoRoot)
}

function Get-FcpBuilderInspection([string]$Name) {
    return Invoke-BoundedDockerResult @('buildx', 'inspect', $Name)
}

function Test-FcpBuildKitContainerRunning([string]$Name) {
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
    foreach ($status in $statuses) {
        if ($status -notin $BuilderQuiescentStates) { return $false }
    }
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

function Get-FcpBuilderRootDriverOpt {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes((Normalize-DirectoryPath $RepoRoot))
    $hex = -join ($bytes | ForEach-Object { $_.ToString('x2') })
    return 'env.' + $BuilderRootEnv + '=' + $hex
}

function Get-FcpBuilderStampedRoot([string]$Name) {
    $inspected = Invoke-BoundedDockerResult @(
        'inspect',
        '--format', '{{range .Config.Env}}{{println .}}{{end}}',
        ($BuildKitContainerPrefix + $Name + '0')
    )
    if ($inspected.ExitCode -ne 0) { return '' }
    $prefix = $BuilderRootEnv + '='
    foreach ($line in @($inspected.Output)) {
        $text = ([string]$line).Trim()
        if (-not $text.StartsWith($prefix)) { continue }
        $hex = $text.Substring($prefix.Length)
        if ($hex.Length -eq 0 -or ($hex.Length % 2) -ne 0) { return '' }
        if ($hex -notmatch '^[0-9a-fA-F]+$') { return '' }
        try {
            $bytes = New-Object byte[] ([int]($hex.Length / 2))
            for ($index = 0; $index -lt $bytes.Length; $index++) {
                $bytes[$index] = [Convert]::ToByte($hex.Substring($index * 2, 2), 16)
            }
            $strict = New-Object System.Text.UTF8Encoding($false, $true)
            return $strict.GetString($bytes)
        }
        catch { return '' }
    }
    return ''
}

function Get-FcpBuilderRetirementCursorPath {
    try {
        $raw = (Last-Text (Invoke-Git @('rev-parse', '--git-path', 'fcp-builder-retirement.cursor'))).Trim()
        if ([string]::IsNullOrWhiteSpace($raw)) { return '' }
        if (-not [System.IO.Path]::IsPathRooted($raw)) {
            $raw = Join-Path $RepoRoot $raw
        }
        return [System.IO.Path]::GetFullPath($raw)
    }
    catch { return '' }
}

function Get-FcpBuilderRetirementCursor {
    $path = Get-FcpBuilderRetirementCursorPath
    if ([string]::IsNullOrWhiteSpace($path)) { return '' }
    try {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return '' }
        $value = ([System.IO.File]::ReadAllText($path)).Trim().ToLowerInvariant()
        if ($value -match '^[0-9a-f]{12,64}$') { return $value }
    }
    catch {}
    return ''
}

function Set-FcpBuilderRetirementCursor([AllowNull()][string]$Value) {
    $path = Get-FcpBuilderRetirementCursorPath
    if ([string]::IsNullOrWhiteSpace($path)) { return }
    try {
        if ([string]::IsNullOrWhiteSpace([string]$Value)) {
            Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
            return
        }
        $text = ([string]$Value).Trim().ToLowerInvariant()
        if ($text -notmatch '^[0-9a-f]{12,64}$') { return }
        $parent = Split-Path -Parent $path
        if (-not [string]::IsNullOrWhiteSpace($parent)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }
        $temporary = "$path.tmp-$PID-$([guid]::NewGuid().ToString('N'))"
        [System.IO.File]::WriteAllText(
            $temporary,
            $text + [Environment]::NewLine,
            (New-Object System.Text.UTF8Encoding($false))
        )
        Move-Item -LiteralPath $temporary -Destination $path -Force
    }
    catch {}
}

function Invoke-FcpStrandedBuilderRetirement {
    $current = Get-FcpBuilderName
    $cursor = Get-FcpBuilderRetirementCursor
    $arguments = @(
        'ps', '--all', '--no-trunc', '--last', [string]$MaxListedBuildersPerPass,
        '--filter', ('name=' + $BuildKitContainerPrefix + $BuilderPrefix)
    )
    if (-not [string]::IsNullOrWhiteSpace($cursor)) {
        $arguments += @('--filter', ('before=' + $cursor))
    }
    $arguments += @('--format', '{{.ID}} {{.Names}}')
    $listed = Invoke-BoundedDockerResult $arguments
    if ($listed.ExitCode -ne 0) {
        if (-not [string]::IsNullOrWhiteSpace($cursor)) {
            Set-FcpBuilderRetirementCursor $null
        }
        return @()
    }

    $retired = @()
    $names = @()
    $seen = 0
    $lastId = ''
    $exactPrefix = $BuildKitContainerPrefix + $BuilderPrefix
    foreach ($line in @($listed.Output)) {
        $text = ([string]$line).Trim()
        if ([string]::IsNullOrWhiteSpace($text)) { continue }
        if ($text -notmatch '^(?<id>[0-9a-fA-F]{12,64})\s+(?<container>\S+)$') {
            return @()
        }
        $seen++
        $lastId = ([string]$Matches.id).ToLowerInvariant()
        $container = [string]$Matches.container
        if (-not $container.StartsWith($exactPrefix)) { continue }
        if (-not $container.EndsWith('0')) { continue }
        $name = $container.Substring($BuildKitContainerPrefix.Length, $container.Length - $BuildKitContainerPrefix.Length - 1)
        if (-not $name.StartsWith($BuilderPrefix)) { continue }
        if ($name -eq $current) { continue }
        $names += $name
    }
    if ($seen -ge $MaxListedBuildersPerPass -and -not [string]::IsNullOrWhiteSpace($lastId)) {
        Set-FcpBuilderRetirementCursor $lastId
    }
    else {
        Set-FcpBuilderRetirementCursor $null
    }

    $examined = 0
    $attempted = 0
    foreach ($name in $names) {
        if ($examined -ge $MaxExaminedBuildersPerPass) { break }
        $examined++
        $owner = Get-FcpBuilderStampedRoot $name
        if ([string]::IsNullOrWhiteSpace($owner)) { continue }
        $ownerExists = $true
        try { $ownerExists = Test-Path -LiteralPath $owner -ErrorAction Stop }
        catch { $ownerExists = $true }
        if ($ownerExists) { continue }
        if (Test-FcpBuildKitContainerRunning $name) { continue }
        if ($attempted -ge $MaxRemovalAttemptsPerPass) { break }
        $attempted++
        if (Remove-FcpBuilder $name) { $retired += $name }
    }
    return $retired
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
        '--driver-opt', 'default-load=true',
        '--driver-opt', (Get-FcpBuilderRootDriverOpt)
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
    Invoke-FcpStrandedBuilderRetirement | Out-Null
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

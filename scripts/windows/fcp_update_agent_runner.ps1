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
Set-StrictMode -Version Latest

$RequestSchema = 'fcp.host-update-request.v1'
$BuildCacheKeepBytes = 8589934592
$MaxBytes = 8192

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

function Test-ApplyRequest([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    try {
        $item = Get-Item -LiteralPath $Path
        if ($item.Length -gt $MaxBytes) { return $false }
        $request = [System.IO.File]::ReadAllText($Path) | ConvertFrom-Json
        return (
            [string]$request.schema -eq $RequestSchema -and
            [string]$request.action -eq 'apply'
        )
    }
    catch { return $false }
}

function Invoke-PostBuildCachePrune {
    # During an apply, PATH points at the private docker proxy. The legacy
    # command shape is intentionally retained so the mature engine/runner
    # contract stays stable, but the proxy maps it to checkout-scoped Buildx
    # cleanup and never to Docker's global default-builder cache.
    try {
        & docker builder prune --force "--keep-storage=$BuildCacheKeepBytes" | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "FCP update cache cleanup returned exit code $LASTEXITCODE."
            return $false
        }
        return $true
    }
    catch {
        Write-Warning "FCP update cache cleanup failed: $($_.Exception.Message)"
        return $false
    }
}

function Start-ReplacementRunner {
    $scriptLiteral = "'" + $RunnerPath.Replace("'", "''") + "'"
    $rootLiteral = "'" + $RepoRoot.Replace("'", "''") + "'"
    $dataLiteral = "'" + $DataDirectory.Replace("'", "''") + "'"
    $command = (
        "Start-Sleep -Milliseconds 500; & $scriptLiteral " +
        "-RepoRoot $rootLiteral -DataDirectory $dataLiteral -PollSeconds $PollSeconds"
    )
    $encoded = [Convert]::ToBase64String(
        [System.Text.Encoding]::Unicode.GetBytes($command)
    )
    Start-Process `
        -FilePath 'powershell.exe' `
        -ArgumentList @(
            '-NoProfile', '-WindowStyle', 'Hidden', '-ExecutionPolicy', 'Bypass',
            '-EncodedCommand', $encoded
        ) `
        -WindowStyle Hidden | Out-Null
}

function Restore-ProcessEnvironment([string]$Name, [AllowNull()][string]$Value) {
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

$RepoRoot = Normalize-DirectoryPath $RepoRoot
$DataDirectory = Normalize-DirectoryPath $DataDirectory
$AgentDirectory = Join-Path $DataDirectory 'federation\update-agent'
$RequestFile = Join-Path $AgentDirectory 'request.json'
New-Item -ItemType Directory -Path $AgentDirectory -Force | Out-Null
$RunnerPath = [System.IO.Path]::GetFullPath($PSCommandPath)
$EnginePath = Join-Path (Split-Path -Parent $RunnerPath) 'fcp_update_engine.ps1'
$ProxySource = Join-Path (Split-Path -Parent $RunnerPath) 'fcp_docker_build_proxy.cmd'
if (-not (Test-Path -LiteralPath $EnginePath -PathType Leaf)) {
    throw 'update_engine_unavailable'
}
if (-not (Test-Path -LiteralPath $ProxySource -PathType Leaf)) {
    throw 'controlled_build_proxy_unavailable'
}
$InitialRunnerHash = (Get-FileHash -LiteralPath $RunnerPath -Algorithm SHA256).Hash
$InitialEngineHash = (Get-FileHash -LiteralPath $EnginePath -Algorithm SHA256).Hash
$InitialProxyHash = (Get-FileHash -LiteralPath $ProxySource -Algorithm SHA256).Hash

$pathHash = Get-PathHash $RepoRoot
$runnerMutex = [System.Threading.Mutex]::new($false, "Global\FCPUpdateAgentRunner-$pathHash")
$runnerAcquired = $false
try {
    try { $runnerAcquired = $runnerMutex.WaitOne(0) }
    catch [System.Threading.AbandonedMutexException] { $runnerAcquired = $true }
    if (-not $runnerAcquired) { exit 0 }

    Set-Location -LiteralPath $RepoRoot
    while ($true) {
        if (-not (Test-Path -LiteralPath $RequestFile)) {
            if ($Once) { break }
            Start-Sleep -Seconds $PollSeconds
            continue
        }

        $isApply = Test-ApplyRequest $RequestFile
        $mutationMutex = $null
        $mutationAcquired = $false
        $proxyDirectory = $null
        $buildOutput = $null
        $previousPath = $null
        $previousRealDocker = $null
        $previousControlledActive = $null
        $previousControlledRoot = $null
        $previousControlledOutput = $null
        $previousLeaseMarker = $null
        try {
            if ($isApply) {
                $mutationMutex = [System.Threading.Mutex]::new(
                    $false,
                    "Global\FCPHostMutation-$pathHash"
                )
                try {
                    $mutationAcquired = $mutationMutex.WaitOne([TimeSpan]::FromSeconds(30))
                }
                catch [System.Threading.AbandonedMutexException] {
                    $mutationAcquired = $true
                }
                if (-not $mutationAcquired) {
                    if ($Once) { exit 1 }
                    Start-Sleep -Milliseconds 250
                    continue
                }

                # Keep the mature engine unchanged. During an apply, a private
                # docker.cmd shim intercepts only its exact core-build and build-
                # cache cleanup commands; every other Docker command is forwarded
                # to the already-resolved real executable.
                $dockerCommand = Get-Command docker -CommandType Application -ErrorAction Stop |
                    Select-Object -First 1
                if (
                    $null -eq $dockerCommand -or
                    [string]::IsNullOrWhiteSpace([string]$dockerCommand.Source)
                ) {
                    throw 'docker_executable_unavailable'
                }
                $previousPath = $env:PATH
                $previousRealDocker = $env:FCP_REAL_DOCKER_EXE
                $previousControlledActive = $env:FCP_CONTROLLED_BUILD_ACTIVE
                $previousControlledRoot = $env:FCP_CONTROLLED_BUILD_REPO_ROOT
                $previousControlledOutput = $env:FCP_CONTROLLED_BUILD_OUTPUT
                $previousLeaseMarker = $env:FCP_HOST_MUTATION_LEASE_ACTIVE

                $proxyDirectory = Join-Path $AgentDirectory (
                    "docker-proxy-$PID-$([guid]::NewGuid().ToString('N'))"
                )
                New-Item -ItemType Directory -Path $proxyDirectory -Force | Out-Null
                Copy-Item -LiteralPath $ProxySource -Destination (
                    Join-Path $proxyDirectory 'docker.cmd'
                ) -Force
                Copy-Item -LiteralPath (
                    Join-Path (Split-Path -Parent $RunnerPath) 'fcp_host_build.ps1'
                ) -Destination (Join-Path $proxyDirectory 'fcp_host_build.ps1') -Force
                Copy-Item -LiteralPath (
                    Join-Path (Split-Path -Parent $RunnerPath) 'fcp_docker_resource.ps1'
                ) -Destination (Join-Path $proxyDirectory 'fcp_docker_resource.ps1') -Force

                $buildOutput = Join-Path $AgentDirectory (
                    "controlled-build-$PID-$([guid]::NewGuid().ToString('N')).txt"
                )
                $env:FCP_REAL_DOCKER_EXE = [string]$dockerCommand.Source
                $env:FCP_CONTROLLED_BUILD_ACTIVE = '1'
                $env:FCP_CONTROLLED_BUILD_REPO_ROOT = $RepoRoot
                $env:FCP_CONTROLLED_BUILD_OUTPUT = $buildOutput
                $env:FCP_HOST_MUTATION_LEASE_ACTIVE = '1'
                $env:PATH = $proxyDirectory + [System.IO.Path]::PathSeparator + $previousPath
            }

            & powershell.exe `
                -NoProfile `
                -ExecutionPolicy Bypass `
                -File $EnginePath `
                -RepoRoot $RepoRoot `
                -DataDirectory $DataDirectory `
                -PollSeconds $PollSeconds `
                -Once
            $engineExit = $LASTEXITCODE

            # Keep the old runner cleanup point, but while the private docker
            # proxy is still active so this can only touch the FCP Buildx builder.
            if ($isApply) {
                Invoke-PostBuildCachePrune | Out-Null
            }
            if ($engineExit -ne 0) {
                Write-Warning "FCP update engine exited with code $engineExit."
            }
        }
        finally {
            if ($isApply) {
                Restore-ProcessEnvironment 'PATH' $previousPath
                Restore-ProcessEnvironment 'FCP_REAL_DOCKER_EXE' $previousRealDocker
                Restore-ProcessEnvironment 'FCP_CONTROLLED_BUILD_ACTIVE' $previousControlledActive
                Restore-ProcessEnvironment 'FCP_CONTROLLED_BUILD_REPO_ROOT' $previousControlledRoot
                Restore-ProcessEnvironment 'FCP_CONTROLLED_BUILD_OUTPUT' $previousControlledOutput
                Restore-ProcessEnvironment 'FCP_HOST_MUTATION_LEASE_ACTIVE' $previousLeaseMarker
                if (-not [string]::IsNullOrWhiteSpace([string]$buildOutput)) {
                    Remove-Item -LiteralPath $buildOutput -Force -ErrorAction SilentlyContinue
                }
                if (-not [string]::IsNullOrWhiteSpace([string]$proxyDirectory)) {
                    Remove-Item -LiteralPath $proxyDirectory -Recurse -Force -ErrorAction SilentlyContinue
                }
            }
            if ($mutationAcquired -and $null -ne $mutationMutex) {
                try { $mutationMutex.ReleaseMutex() | Out-Null } catch {}
            }
            if ($null -ne $mutationMutex) { $mutationMutex.Dispose() }
        }

        if ($Once) { break }
        if (
            (Get-FileHash -LiteralPath $RunnerPath -Algorithm SHA256).Hash -ne $InitialRunnerHash -or
            (Get-FileHash -LiteralPath $EnginePath -Algorithm SHA256).Hash -ne $InitialEngineHash -or
            (Get-FileHash -LiteralPath $ProxySource -Algorithm SHA256).Hash -ne $InitialProxyHash
        ) {
            Start-ReplacementRunner
            break
        }
    }
}
finally {
    if ($runnerAcquired) {
        try { $runnerMutex.ReleaseMutex() | Out-Null } catch {}
    }
    $runnerMutex.Dispose()
}

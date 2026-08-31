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
$ResultSchema = 'fcp.host-update-result.v1'
$BuildCacheKeepBytes = 8589934592
$MaxBytes = 8192
$OidPattern = '^[0-9a-f]{40}$'
$RequestIdPattern = '^[A-Za-z0-9._:-]{1,128}$'

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

function Get-RequestResultFile([string]$RequestId) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($RequestId)
        $digest = ([System.BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
        return Join-Path $AgentDirectory "result-$digest.json"
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

function Get-ApplyRequestIdentity([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $null }
    try {
        $item = Get-Item -LiteralPath $Path
        if ($item.Length -gt $MaxBytes) { return $null }
        $request = [System.IO.File]::ReadAllText($Path) | ConvertFrom-Json
        $requestId = [string]$request.request_id
        $target = ([string]$request.target_commit).ToLowerInvariant()
        if (
            [string]$request.schema -ne $RequestSchema -or
            [string]$request.action -ne 'apply' -or
            $requestId -notmatch $RequestIdPattern -or
            $target -notmatch $OidPattern
        ) { return $null }
        return [pscustomobject]@{
            RequestId = $requestId
            TargetCommit = $target
        }
    }
    catch { return $null }
}

function Invoke-PostBuildCachePrune {
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

function Invoke-DockerResult([string]$DockerExe, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $output = @()
    $exitCode = 127
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $DockerExe @Arguments 2>&1
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
    }
    catch {
        $output = @($_.Exception.Message)
        $exitCode = 127
    }
    finally {
        $ErrorActionPreference = $previous
    }
    return [pscustomobject]@{ Output = @($output); ExitCode = $exitCode }
}

function Get-RunningFlaskCommit([string]$DockerExe) {
    $probe = Invoke-DockerResult $DockerExe @(
        'compose', 'exec', '-T', 'flask', 'python', '-c',
        "import os; print(os.environ.get('FCP_BUILD_COMMIT',''))"
    )
    if ($probe.ExitCode -ne 0) { return $null }
    $lines = @($probe.Output | ForEach-Object { ([string]$_).Trim() } | Where-Object { $_ })
    if ($lines.Count -eq 0) { return $null }
    $value = ([string]$lines[-1]).ToLowerInvariant()
    if ($value -notmatch $OidPattern) { return $null }
    return $value
}

function Write-JsonAtomic([string]$Path, [object]$Value) {
    $json = $Value | ConvertTo-Json -Compress -Depth 8
    if ([System.Text.Encoding]::UTF8.GetByteCount($json) -gt $MaxBytes) {
        throw 'result_too_large'
    }
    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    $temp = Join-Path $directory ('.' + [System.IO.Path]::GetFileName($Path) + '.' + [guid]::NewGuid().ToString('N'))
    try {
        [System.IO.File]::WriteAllText($temp, $json, [System.Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $temp -Destination $Path -Force
    }
    finally {
        Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue
    }
}

function Get-UpdateResult {
    $path = Join-Path $AgentDirectory 'result.json'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $null }
    try {
        $item = Get-Item -LiteralPath $path
        if ($item.Length -gt $MaxBytes) { return $null }
        return [System.IO.File]::ReadAllText($path) | ConvertFrom-Json
    }
    catch { return $null }
}

function Write-ActivationRecovery(
    [object]$Result,
    [string]$State,
    [AllowNull()][string]$RunningCommit,
    [string]$Code,
    [string]$Message
) {
    $requestId = [string]$Result.request_id
    if ($requestId -notmatch $RequestIdPattern) { return }
    $value = [ordered]@{
        schema = $ResultSchema
        request_id = $requestId
        action = 'apply'
        state = $State
        current_commit = $Result.current_commit
        target_commit = $Result.target_commit
        running_commit = $RunningCommit
        code = $Code
        message = $Message
        completed_at = [DateTimeOffset]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffffffZ')
    }
    Write-JsonAtomic (Get-RequestResultFile $requestId) $value
    Write-JsonAtomic (Join-Path $AgentDirectory 'result.json') $value
}

function Restore-PreviousFlaskRuntime([string]$DockerExe, [string]$PreviousCommit) {
    if ([string]::IsNullOrWhiteSpace($PreviousCommit)) { return $false }
    $started = Invoke-DockerResult $DockerExe @('compose', 'start', 'flask')
    if ($started.ExitCode -ne 0) { return $false }
    return (Get-RunningFlaskCommit $DockerExe) -eq $PreviousCommit
}

function Reconcile-FailedActivation(
    [string]$DockerExe,
    [AllowNull()][string]$PreviousCommit,
    [string]$PhaseFile,
    [string]$ExpectedRequestId,
    [string]$ExpectedTargetCommit
) {
    $result = Get-UpdateResult
    if (
        $null -eq $result -or
        [string]$result.action -ne 'apply' -or
        [string]$result.code -ne 'host_update_failed' -or
        [string]$result.request_id -ne $ExpectedRequestId -or
        ([string]$result.target_commit).ToLowerInvariant() -ne $ExpectedTargetCommit
    ) { return }

    $phase = ''
    try {
        if (Test-Path -LiteralPath $PhaseFile -PathType Leaf) {
            $phase = ([System.IO.File]::ReadAllText($PhaseFile)).Trim()
        }
    }
    catch { $phase = '' }

    if ($phase -eq 'flask-stopped' -and -not [string]::IsNullOrWhiteSpace($PreviousCommit)) {
        if (Restore-PreviousFlaskRuntime $DockerExe $PreviousCommit) {
            Write-ActivationRecovery `
                -Result $result `
                -State 'error' `
                -RunningCommit $PreviousCommit `
                -Code 'activation_recovered' `
                -Message 'Target activation failed before Flask replacement. The previous core runtime was restarted and verified; retry the approved apply.'
            return
        }
    }

    if ($phase -eq 'target-started') {
        Write-ActivationRecovery `
            -Result $result `
            -State 'activation_required' `
            -RunningCommit (Get-RunningFlaskCommit $DockerExe) `
            -Code 'activation_required' `
            -Message 'Target activation began but runtime verification did not complete. Source remains on the approved target; retry the same apply to resume activation.'
    }
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
        $applyIdentity = if ($isApply) { Get-ApplyRequestIdentity $RequestFile } else { $null }
        $mutationMutex = $null
        $mutationAcquired = $false
        $proxyDirectory = $null
        $buildOutput = $null
        $activationPhaseFile = $null
        $previousRunning = $null
        $realDocker = $null
        $previousPath = $null
        $previousRealDocker = $null
        $previousControlledActive = $null
        $previousControlledRoot = $null
        $previousControlledOutput = $null
        $previousLeaseMarker = $null
        $previousActivationPhase = $null
        try {
            if ($isApply) {
                if ($null -eq $applyIdentity) { throw 'malformed_apply_identity' }
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

                $dockerCommand = Get-Command docker -CommandType Application -ErrorAction Stop |
                    Select-Object -First 1
                if (
                    $null -eq $dockerCommand -or
                    [string]::IsNullOrWhiteSpace([string]$dockerCommand.Source)
                ) {
                    throw 'docker_executable_unavailable'
                }
                $realDocker = [string]$dockerCommand.Source
                $previousRunning = Get-RunningFlaskCommit $realDocker
                $previousPath = $env:PATH
                $previousRealDocker = $env:FCP_REAL_DOCKER_EXE
                $previousControlledActive = $env:FCP_CONTROLLED_BUILD_ACTIVE
                $previousControlledRoot = $env:FCP_CONTROLLED_BUILD_REPO_ROOT
                $previousControlledOutput = $env:FCP_CONTROLLED_BUILD_OUTPUT
                $previousLeaseMarker = $env:FCP_HOST_MUTATION_LEASE_ACTIVE
                $previousActivationPhase = $env:FCP_ACTIVATION_PHASE_FILE

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
                $activationPhaseFile = Join-Path $AgentDirectory (
                    "activation-phase-$PID-$([guid]::NewGuid().ToString('N')).txt"
                )
                $env:FCP_REAL_DOCKER_EXE = $realDocker
                $env:FCP_CONTROLLED_BUILD_ACTIVE = '1'
                $env:FCP_CONTROLLED_BUILD_REPO_ROOT = $RepoRoot
                $env:FCP_CONTROLLED_BUILD_OUTPUT = $buildOutput
                $env:FCP_HOST_MUTATION_LEASE_ACTIVE = '1'
                $env:FCP_ACTIVATION_PHASE_FILE = $activationPhaseFile
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

            if ($isApply) {
                Invoke-PostBuildCachePrune | Out-Null
                Reconcile-FailedActivation `
                    -DockerExe $realDocker `
                    -PreviousCommit $previousRunning `
                    -PhaseFile $activationPhaseFile `
                    -ExpectedRequestId ([string]$applyIdentity.RequestId) `
                    -ExpectedTargetCommit ([string]$applyIdentity.TargetCommit)
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
                Restore-ProcessEnvironment 'FCP_ACTIVATION_PHASE_FILE' $previousActivationPhase
                if (-not [string]::IsNullOrWhiteSpace([string]$activationPhaseFile)) {
                    Remove-Item -LiteralPath $activationPhaseFile -Force -ErrorAction SilentlyContinue
                }
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

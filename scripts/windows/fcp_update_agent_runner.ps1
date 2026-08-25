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

$RepoRoot = Normalize-DirectoryPath $RepoRoot
$DataDirectory = Normalize-DirectoryPath $DataDirectory
$AgentDirectory = Join-Path $DataDirectory 'federation\update-agent'
$RequestFile = Join-Path $AgentDirectory 'request.json'
New-Item -ItemType Directory -Path $AgentDirectory -Force | Out-Null
$RunnerPath = [System.IO.Path]::GetFullPath($PSCommandPath)
$EnginePath = Join-Path (Split-Path -Parent $RunnerPath) 'fcp_update_engine.ps1'
if (-not (Test-Path -LiteralPath $EnginePath)) { throw 'update_engine_unavailable' }
$InitialRunnerHash = (Get-FileHash -LiteralPath $RunnerPath -Algorithm SHA256).Hash
$InitialEngineHash = (Get-FileHash -LiteralPath $EnginePath -Algorithm SHA256).Hash

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

            # The preserved engine already prunes after a successful build.
            # An apply whose build raised before that point still gets the same
            # bounded cache cleanup attempt here before the shared lock is released.
            if ($isApply) {
                Invoke-PostBuildCachePrune | Out-Null
            }
            if ($engineExit -ne 0) {
                Write-Warning "FCP update engine exited with code $engineExit."
            }
        }
        finally {
            if ($mutationAcquired -and $null -ne $mutationMutex) {
                try { $mutationMutex.ReleaseMutex() | Out-Null } catch {}
            }
            if ($null -ne $mutationMutex) { $mutationMutex.Dispose() }
        }

        if ($Once) { break }
        if (
            (Get-FileHash -LiteralPath $RunnerPath -Algorithm SHA256).Hash -ne $InitialRunnerHash -or
            (Get-FileHash -LiteralPath $EnginePath -Algorithm SHA256).Hash -ne $InitialEngineHash
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

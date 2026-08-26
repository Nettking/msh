[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [ValidateSet('normal', 'fresh', 'resume')]
    [string]$Mode = 'normal',
    [ValidateRange(0, 600)]
    [int]$LockTimeoutSeconds = 30
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

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

$RepoRoot = Normalize-DirectoryPath $RepoRoot
$startCommand = Join-Path $RepoRoot 'start.cmd'
if (-not (Test-Path -LiteralPath $startCommand -PathType Leaf)) {
    Write-Error 'FCP launcher lease refused: start.cmd is missing.'
    exit 1
}

$mutexName = 'Global\FCPHostMutation-' + (Get-PathHash $RepoRoot)
$mutationMutex = [System.Threading.Mutex]::new($false, $mutexName)
$acquired = $false
$previousLease = $env:FCP_HOST_MUTATION_LEASE_ACTIVE
$previousOwner = $env:FCP_HOST_MUTATION_LEASE_OWNER_PID
try {
    try {
        $acquired = $mutationMutex.WaitOne([TimeSpan]::FromSeconds($LockTimeoutSeconds))
    }
    catch [System.Threading.AbandonedMutexException] {
        $acquired = $true
    }
    if (-not $acquired) {
        Write-Error 'FCP launcher lease refused: host_mutation_busy'
        exit 1
    }

    $env:FCP_HOST_MUTATION_LEASE_ACTIVE = '1'
    $env:FCP_HOST_MUTATION_LEASE_OWNER_PID = [string]$PID

    $modeArgument = switch ($Mode) {
        'fresh' { ' --fresh' }
        'resume' { ' --resume' }
        default { '' }
    }
    $commandLine = '"' + $startCommand + '"' + $modeArgument
    & $env:ComSpec /d /c $commandLine
    exit $LASTEXITCODE
}
finally {
    if ($null -eq $previousLease) {
        Remove-Item Env:FCP_HOST_MUTATION_LEASE_ACTIVE -ErrorAction SilentlyContinue
    }
    else {
        $env:FCP_HOST_MUTATION_LEASE_ACTIVE = $previousLease
    }
    if ($null -eq $previousOwner) {
        Remove-Item Env:FCP_HOST_MUTATION_LEASE_OWNER_PID -ErrorAction SilentlyContinue
    }
    else {
        $env:FCP_HOST_MUTATION_LEASE_OWNER_PID = $previousOwner
    }
    if ($acquired) {
        try { $mutationMutex.ReleaseMutex() | Out-Null } catch {}
    }
    $mutationMutex.Dispose()
}

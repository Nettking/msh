[CmdletBinding()]
param(
    [string]$VpnReconnectScript = "ops/vpn/reconnect-vpn.ps1",
    [string]$StartupExecutable = "docker",
    [string[]]$StartupArguments = @("compose", "up", "--build", "webapp")
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[start-system] $Message"
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

Write-Step "Repository root: $repoRoot"

$vpnScriptPath = Join-Path $repoRoot $VpnReconnectScript
if (-not (Test-Path -LiteralPath $vpnScriptPath)) {
    throw "Required VPN reconnect script not found: $vpnScriptPath"
}

if (-not (Get-Command $StartupExecutable -ErrorAction SilentlyContinue)) {
    throw "Startup executable not found on PATH: $StartupExecutable"
}

$vpnScriptRegex = [Regex]::Escape($vpnScriptPath)
$existingVpnMonitors = Get-CimInstance Win32_Process | Where-Object {
    ($_.Name -match "^(powershell|pwsh)(\.exe)?$") -and
    ($_.CommandLine -match $vpnScriptRegex)
}

if ($existingVpnMonitors) {
    $existingPids = ($existingVpnMonitors | Select-Object -ExpandProperty ProcessId) -join ", "
    Write-Step "VPN reconnect monitor already running. Reusing existing process(es): $existingPids"
}
else {
    Write-Step "Starting VPN reconnect monitor in a separate PowerShell process..."
    $vpnProcess = Start-Process -FilePath "powershell.exe" `
        -ArgumentList @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", "`"$vpnScriptPath`""
        ) `
        -WorkingDirectory $repoRoot `
        -PassThru

    Write-Step "VPN reconnect monitor launched (PID: $($vpnProcess.Id))."
}

Write-Step "Starting existing system startup command: $StartupExecutable $($StartupArguments -join ' ')"
& $StartupExecutable @StartupArguments

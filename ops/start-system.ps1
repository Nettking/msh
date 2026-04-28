[CmdletBinding()]
param(
    [string]$VpnReconnectScript = "",
    [string]$StartupExecutable = "docker",
    [string[]]$StartupArguments = @("compose", "up", "--build", "flask")
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[start-system] $Message"
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

Write-Step "Repository root: $repoRoot"

if (-not (Get-Command $StartupExecutable -ErrorAction SilentlyContinue)) {
    throw "Startup executable not found on PATH: $StartupExecutable"
}

if ($VpnReconnectScript) {
    $vpnScriptPath = Join-Path $repoRoot $VpnReconnectScript
    if (-not (Test-Path -LiteralPath $vpnScriptPath)) {
        throw "VPN reconnect script not found: $vpnScriptPath"
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
}
else {
    Write-Step "No VPN reconnect script configured; skipping VPN monitor startup."
}

Write-Step "Starting existing system startup command: $StartupExecutable $($StartupArguments -join ' ')"
& $StartupExecutable @StartupArguments

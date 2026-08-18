[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot
)

$ErrorActionPreference = "Stop"

$root = [System.IO.Path]::GetFullPath($RepoRoot)
$startScript = Join-Path $root "start.cmd"
$hostRunner = Join-Path $root "scripts\federation_host_runner.py"
$zeroTouch = Join-Path $root "scripts\zero_touch_federation_start.py"

if (-not (Test-Path -LiteralPath $startScript -PathType Leaf)) {
    Write-Error "FCP start script was not found: $startScript"
    exit 2
}
if (-not $env:FCP_TAILSCALE_IP -or -not $env:FCP_TAILSCALE_DISCOVERY_PORT) {
    Write-Error "Tailscale startup environment is incomplete."
    exit 2
}

Write-Host ""
Write-Host "FIRST FEDERATION INITIALIZATION"
Write-Host "Enter the reset confirmation and first administrator credentials now."
Write-Host "The password is held only in this host process until FCP is ready; it is not"
Write-Host "placed in command-line arguments, environment variables, or files."
Write-Host ""

$reset = Read-Host "Type RESET to continue"
if ($reset -cne "RESET") {
    Write-Host "Fresh install cancelled. No state was removed."
    exit 2
}

$email = (Read-Host "Federation administrator email").Trim()
if ([string]::IsNullOrWhiteSpace($email)) {
    Write-Error "Federation administrator email is required."
    exit 2
}
$password = Read-Host "Federation administrator password" -AsSecureString
if ($password.Length -eq 0) {
    Write-Error "Federation administrator password is required."
    exit 2
}

# Run the destructive reset/build with only the reset confirmation on stdin.
# The administrator credential never traverses Docker or any startup subprocess.
$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $env:ComSpec
$escapedStart = '"' + $startScript + '" --fresh'
$startInfo.Arguments = "/d /s /c `"$escapedStart`""
$startInfo.WorkingDirectory = $root
$startInfo.UseShellExecute = $false
$startInfo.RedirectStandardInput = $true
$startProcess = New-Object System.Diagnostics.Process
$startProcess.StartInfo = $startInfo
if (-not $startProcess.Start()) {
    Write-Error "Could not start FCP services."
    exit 2
}
$startProcess.StandardInput.WriteLine("RESET")
$startProcess.StandardInput.Close()
$startProcess.WaitForExit()
if ($startProcess.ExitCode -ne 0) {
    Write-Error "FCP service startup failed with exit code $($startProcess.ExitCode)."
    exit $startProcess.ExitCode
}

# Verify the host responder implementation before consuming the administrator
# password. Failure here leaves the credential only in SecureString memory.
& python $hostRunner tailnet_join_responder --check
if ($LASTEXITCODE -ne 0) {
    Write-Error "Automatic Federation joining is unavailable; refusing zero-touch startup."
    exit 2
}

netsh advfirewall firewall show rule name="FCP tailnet join responder" *> $null
if ($LASTEXITCODE -ne 0) {
    netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=$env:FCP_AUTO_JOIN_PORT *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Could not open port $env:FCP_AUTO_JOIN_PORT in Windows Firewall automatically."
        Write-Host "Run this once in an elevated prompt if another trusted device cannot join:"
        Write-Host "  netsh advfirewall firewall add rule name=`"FCP tailnet join responder`" dir=in action=allow protocol=TCP localport=$env:FCP_AUTO_JOIN_PORT"
    }
}

# Convert the SecureString only for the bounded stdin handoff to the local
# zero-touch initializer. The plaintext value is cleared immediately afterwards.
$bstr = [IntPtr]::Zero
$plainPassword = $null
try {
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($password)
    $plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    @($email, $plainPassword) | & python $zeroTouch `
        --initialize-federation `
        --first-admin-stdin `
        --web-port $env:FCP_TAILSCALE_DISCOVERY_PORT
    if ($LASTEXITCODE -ne 0) {
        Write-Error "FCP services are running, but zero-touch Federation setup did not complete."
        exit 2
    }
}
finally {
    $plainPassword = $null
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
    $password.Dispose()
}

# The first Federation now exists. Start the host responder for subsequent
# trusted devices only after the credential-bearing initializer has finished.
$pythonPath = (Get-Command python -ErrorAction Stop).Source
$responderArguments = @(
    $hostRunner,
    "tailnet_join_responder",
    "--bind", $env:FCP_TAILSCALE_IP,
    "--port", $env:FCP_AUTO_JOIN_PORT,
    "--app-url", $env:FCP_AUTO_JOIN_APP_URL
)
Start-Process -FilePath $pythonPath -ArgumentList $responderArguments -NoNewWindow | Out-Null

Write-Host ""
Write-Host "FCP Tailscale address: $env:FCP_TAILSCALE_IP"
Write-Host "Zero-touch startup is complete."
Start-Process "http://$($env:FCP_TAILSCALE_IP):$($env:FCP_TAILSCALE_DISCOVERY_PORT)/federation"
exit 0

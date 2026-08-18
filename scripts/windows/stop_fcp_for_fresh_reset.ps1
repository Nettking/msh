param(
    [ValidateRange(5, 300)]
    [int]$PrimaryTimeoutSeconds = 45,
    [ValidateRange(5, 120)]
    [int]$RecoveryTimeoutSeconds = 20,
    [string]$DockerExecutable = ""
)

$ErrorActionPreference = "Stop"

function Resolve-DockerExecutable {
    if (-not [string]::IsNullOrWhiteSpace($DockerExecutable)) {
        if (-not (Test-Path -LiteralPath $DockerExecutable -PathType Leaf)) {
            throw "Configured Docker executable does not exist: $DockerExecutable"
        }
        return (Resolve-Path -LiteralPath $DockerExecutable).Path
    }

    $docker = Get-Command docker -ErrorAction Stop
    return $docker.Source
}

function Invoke-BoundedDocker {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    Write-Host $Description

    # Use System.Diagnostics.Process directly rather than Start-Process. During
    # physical acceptance, Windows PowerShell returned a blank ExitCode from the
    # Start-Process object after a completed Docker invocation. Casting that blank
    # value to [int] silently produced 0 and could falsely authorize a fresh reset.
    $process = New-Object System.Diagnostics.Process
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $Executable
    # All arguments used by this helper are fixed tokens without whitespace.
    $startInfo.Arguments = ($Arguments -join " ")
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $process.StartInfo = $startInfo

    try {
        $started = $process.Start()
        if (-not $started) {
            Write-Warning "$Description could not start Docker. Failing closed."
            return 125
        }

        $completed = $process.WaitForExit($TimeoutSeconds * 1000)
        if ($completed) {
            # Complete the wait before reading ExitCode. If Windows cannot provide
            # a trustworthy code, never coerce null/blank to zero.
            $process.WaitForExit()
            if (-not $process.HasExited) {
                Write-Warning "$Description reported completion but the process is still running. Failing closed."
                return 125
            }

            try {
                $exitCode = $process.ExitCode
            }
            catch {
                Write-Warning "$Description completed but its exit code could not be read. Failing closed."
                return 125
            }

            if ($null -eq $exitCode) {
                Write-Warning "$Description completed but returned no exit code. Failing closed."
                return 125
            }
            return [int]$exitCode
        }

        Write-Warning "$Description timed out after $TimeoutSeconds seconds. Terminating only that Docker CLI process tree."
        & taskkill.exe /PID $process.Id /T /F 2>$null | Out-Null
        try {
            [void]$process.WaitForExit(5000)
        }
        catch {
            # The process is already being force-terminated. Recovery below is bounded too.
        }
        return 124
    }
    finally {
        $process.Dispose()
    }
}

try {
    $dockerPath = Resolve-DockerExecutable
    $primaryExit = Invoke-BoundedDocker -Executable $dockerPath -Arguments @("compose", "down", "--remove-orphans", "--timeout", "10") -TimeoutSeconds $PrimaryTimeoutSeconds -Description "Stopping the current FCP Compose project..."

    if ($primaryExit -eq 0) {
        exit 0
    }

    Write-Warning "Normal Compose shutdown did not complete cleanly (exit $primaryExit). Attempting bounded project-scoped recovery."

    $killExit = Invoke-BoundedDocker -Executable $dockerPath -Arguments @("compose", "kill") -TimeoutSeconds $RecoveryTimeoutSeconds -Description "Force-stopping containers in this FCP Compose project..."

    if ($killExit -ne 0) {
        Write-Warning "Project-scoped docker compose kill exited with code $killExit. Cleanup will still be attempted."
    }

    $cleanupExit = Invoke-BoundedDocker -Executable $dockerPath -Arguments @("compose", "down", "--remove-orphans", "--timeout", "5") -TimeoutSeconds $RecoveryTimeoutSeconds -Description "Removing stopped FCP containers and project network..."

    if ($cleanupExit -eq 0) {
        Write-Host "FCP shutdown recovered successfully."
        exit 0
    }

    Write-Error "FCP could not be stopped safely after bounded recovery (cleanup exit $cleanupExit). Device state was not reset."
    exit 1
}
catch {
    Write-Error "FCP shutdown helper failed safely: $($_.Exception.Message)"
    exit 1
}

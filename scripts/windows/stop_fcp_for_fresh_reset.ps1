param(
    [ValidateRange(5, 300)]
    [int]$PrimaryTimeoutSeconds = 45,
    [ValidateRange(5, 120)]
    [int]$RecoveryTimeoutSeconds = 20
)

$ErrorActionPreference = "Stop"

function Invoke-BoundedDocker {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    $docker = Get-Command docker -ErrorAction Stop
    Write-Host $Description
    $process = Start-Process -FilePath $docker.Source -ArgumentList $Arguments -NoNewWindow -PassThru

    try {
        if ($process.WaitForExit($TimeoutSeconds * 1000)) {
            # On Windows/PowerShell, reading ExitCode immediately after the
            # timeout overload of WaitForExit() can yield an unset value even
            # though the process has exited.  Complete the parameterless wait
            # and refresh the Process object before reading ExitCode.
            $process.WaitForExit()
            $process.Refresh()
            $exitCode = $process.ExitCode
            if ($null -eq $exitCode) {
                throw "$Description completed but did not expose an exit code."
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
    $primaryExit = Invoke-BoundedDocker -Arguments @("compose", "down", "--remove-orphans", "--timeout", "10") -TimeoutSeconds $PrimaryTimeoutSeconds -Description "Stopping the current FCP Compose project..."

    if ($primaryExit -eq 0) {
        exit 0
    }

    Write-Warning "Normal Compose shutdown did not complete cleanly (exit $primaryExit). Attempting bounded project-scoped recovery."

    $killExit = Invoke-BoundedDocker -Arguments @("compose", "kill") -TimeoutSeconds $RecoveryTimeoutSeconds -Description "Force-stopping containers in this FCP Compose project..."

    if ($killExit -ne 0) {
        Write-Warning "Project-scoped docker compose kill exited with code $killExit. Cleanup will still be attempted."
    }

    $cleanupExit = Invoke-BoundedDocker -Arguments @("compose", "down", "--remove-orphans", "--timeout", "5") -TimeoutSeconds $RecoveryTimeoutSeconds -Description "Removing stopped FCP containers and project network..."

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

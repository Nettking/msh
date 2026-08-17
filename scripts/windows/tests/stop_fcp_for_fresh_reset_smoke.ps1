$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$helper = Join-Path $repoRoot 'scripts\windows\stop_fcp_for_fresh_reset.ps1'
$baseTemp = if ([string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) { [IO.Path]::GetTempPath() } else { $env:RUNNER_TEMP }
$tempRoot = Join-Path $baseTemp ('fcp-fresh-shutdown-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

try {
    # Use a native executable rather than a .cmd shim. The production helper
    # launches Docker Desktop's native docker.exe, and Start-Process can expose
    # different process/exit-code semantics for batch files through cmd.exe.
    # FAIL-1 is specifically about native Windows process exit-state handling.
    $fakeDocker = Join-Path $tempRoot 'docker.exe'
    $fakeDockerSource = @'
using System;
using System.IO;
using System.Threading;

public static class FakeDocker
{
    private static int ReadExit(string name, int fallback)
    {
        int value;
        return Int32.TryParse(Environment.GetEnvironmentVariable(name), out value) ? value : fallback;
    }

    public static int Main(string[] args)
    {
        string log = Environment.GetEnvironmentVariable("FCP_FAKE_DOCKER_LOG");
        if (String.IsNullOrWhiteSpace(log))
        {
            return 90;
        }

        string command = String.Join(" ", args);
        File.AppendAllText(log, command + Environment.NewLine);

        if (command == "compose down --remove-orphans --timeout 10")
        {
            if (Environment.GetEnvironmentVariable("FCP_FAKE_DOCKER_PRIMARY_DELAY") == "1")
            {
                Thread.Sleep(8000);
            }
            return ReadExit("FCP_FAKE_DOCKER_PRIMARY_EXIT", 92);
        }
        if (command == "compose kill")
        {
            return ReadExit("FCP_FAKE_DOCKER_KILL_EXIT", 93);
        }
        if (command == "compose down --remove-orphans --timeout 5")
        {
            return ReadExit("FCP_FAKE_DOCKER_CLEANUP_EXIT", 94);
        }
        return 91;
    }
}
'@
    Add-Type -TypeDefinition $fakeDockerSource -OutputAssembly $fakeDocker -OutputType ConsoleApplication

    function Invoke-Case {
        param(
            [string]$Name,
            [int]$PrimaryExit,
            [int]$KillExit,
            [int]$CleanupExit,
            [bool]$DelayPrimary,
            [int]$ExpectedExit,
            [string[]]$ExpectedCommands
        )

        $log = Join-Path $tempRoot ($Name + '.log')
        Remove-Item -LiteralPath $log -Force -ErrorAction SilentlyContinue
        $env:FCP_FAKE_DOCKER_LOG = $log
        $env:FCP_FAKE_DOCKER_PRIMARY_EXIT = [string]$PrimaryExit
        $env:FCP_FAKE_DOCKER_KILL_EXIT = [string]$KillExit
        $env:FCP_FAKE_DOCKER_CLEANUP_EXIT = [string]$CleanupExit
        $env:FCP_FAKE_DOCKER_PRIMARY_DELAY = $(if ($DelayPrimary) { '1' } else { '0' })

        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $helper `
            -DockerExecutable $fakeDocker `
            -PrimaryTimeoutSeconds 5 `
            -RecoveryTimeoutSeconds 5
        $actualExit = $LASTEXITCODE
        $global:LASTEXITCODE = 0
        if ($actualExit -ne $ExpectedExit) {
            throw "$Name expected exit $ExpectedExit but got $actualExit"
        }

        $commands = @()
        if (Test-Path -LiteralPath $log) {
            $commands = @(Get-Content -LiteralPath $log)
        }
        if ($commands.Count -ne $ExpectedCommands.Count) {
            throw "$Name expected $($ExpectedCommands.Count) Docker calls but got $($commands.Count): $($commands -join '; ')"
        }
        for ($i = 0; $i -lt $ExpectedCommands.Count; $i++) {
            if ($commands[$i].Trim() -ne $ExpectedCommands[$i]) {
                throw "$Name command $i was '$($commands[$i])', expected '$($ExpectedCommands[$i])'"
            }
        }
    }

    Invoke-Case `
        -Name 'primary-success' `
        -PrimaryExit 0 -KillExit 99 -CleanupExit 99 -DelayPrimary $false `
        -ExpectedExit 0 `
        -ExpectedCommands @('compose down --remove-orphans --timeout 10')

    Invoke-Case `
        -Name 'real-failure-recovers' `
        -PrimaryExit 7 -KillExit 0 -CleanupExit 0 -DelayPrimary $false `
        -ExpectedExit 0 `
        -ExpectedCommands @(
            'compose down --remove-orphans --timeout 10',
            'compose kill',
            'compose down --remove-orphans --timeout 5'
        )

    Invoke-Case `
        -Name 'recovery-fails-closed' `
        -PrimaryExit 7 -KillExit 8 -CleanupExit 9 -DelayPrimary $false `
        -ExpectedExit 1 `
        -ExpectedCommands @(
            'compose down --remove-orphans --timeout 10',
            'compose kill',
            'compose down --remove-orphans --timeout 5'
        )

    # A hung primary command must be terminated at the configured bound. The
    # helper may then recover, but a failed cleanup still blocks fresh reset.
    Invoke-Case `
        -Name 'timeout-fails-closed' `
        -PrimaryExit 0 -KillExit 0 -CleanupExit 9 -DelayPrimary $true `
        -ExpectedExit 1 `
        -ExpectedCommands @(
            'compose down --remove-orphans --timeout 10',
            'compose kill',
            'compose down --remove-orphans --timeout 5'
        )

    Write-Host 'Windows fresh shutdown smoke passed.'
}
finally {
    Remove-Item Env:FCP_FAKE_DOCKER_LOG -ErrorAction SilentlyContinue
    Remove-Item Env:FCP_FAKE_DOCKER_PRIMARY_EXIT -ErrorAction SilentlyContinue
    Remove-Item Env:FCP_FAKE_DOCKER_KILL_EXIT -ErrorAction SilentlyContinue
    Remove-Item Env:FCP_FAKE_DOCKER_CLEANUP_EXIT -ErrorAction SilentlyContinue
    Remove-Item Env:FCP_FAKE_DOCKER_PRIMARY_DELAY -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
}

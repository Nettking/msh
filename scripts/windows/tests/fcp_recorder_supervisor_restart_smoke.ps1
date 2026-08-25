$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Execution-level coverage of the native recorder supervisor's ordinary-failure
# restart state machine.
#
# The boundary tests in
# catalog/mtconnect_recorder/tests/test_native_recorder_supervisor_boundary.py
# pin the shape of this policy on every platform. They cannot run it. Two of
# its properties are only true of a real Windows native process:
#
#   * a console Ctrl+C that bypassed the recorder's own handler exits with
#     STATUS_CONTROL_C_EXIT (0xC000013A), which no POSIX child can return --
#     an 8-bit POSIX status cannot express it; and
#   * PowerShell's $LASTEXITCODE handling of a native child is what the
#     supervisor actually branches on.
#
# So this smoke drives the real supervisor with a compiled fake child, the same
# approach stop_fcp_for_fresh_reset_smoke.ps1 uses for docker.exe.

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$supervisor = Join-Path $repoRoot 'scripts\windows\fcp_recorder_supervisor.ps1'
$baseTemp = if ([string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) { [IO.Path]::GetTempPath() } else { $env:RUNNER_TEMP }
$tempRoot = Join-Path $baseTemp ('fcp-recorder-supervisor-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tempRoot | Out-Null

$failures = New-Object System.Collections.Generic.List[string]

function Assert-Equal([string]$What, $Expected, $Actual) {
    if ($Expected -ne $Actual) {
        $script:failures.Add("${What}: expected '$Expected', got '$Actual'")
        Write-Host "  FAIL ${What}: expected '$Expected', got '$Actual'"
    }
    else {
        Write-Host "  ok   ${What} = $Actual"
    }
}

try {
    # A native executable, not a .cmd shim: exit-code semantics for batch files
    # go through cmd.exe and cannot carry STATUS_CONTROL_C_EXIT.
    $fakeChild = Join-Path $tempRoot 'fakerecorder.exe'
    $fakeSource = @'
using System;
using System.IO;
using System.Threading;

public static class FakeRecorder
{
    private static int Nth(string name, int index, int fallback)
    {
        string raw = Environment.GetEnvironmentVariable(name);
        if (String.IsNullOrWhiteSpace(raw)) { return fallback; }
        string[] parts = raw.Split(',');
        string pick = index <= parts.Length ? parts[index - 1] : parts[parts.Length - 1];
        int value;
        return Int32.TryParse(pick.Trim(), out value) ? value : fallback;
    }

    public static int Main(string[] args)
    {
        string log = Environment.GetEnvironmentVariable("FCP_FAKE_RECORDER_LOG");
        if (String.IsNullOrWhiteSpace(log)) { return 90; }
        string joined = String.Join(" ", args);

        // Host-agent calls are answered without consuming a child slot, so the
        // start count stays a count of recorder children only.
        if (joined.Contains("fcp_native_recorder_update_agent.py"))
        {
            File.AppendAllText(log + ".agent", joined + Environment.NewLine);
            return 1;
        }

        int n = 0;
        string countPath = log + ".count";
        if (File.Exists(countPath)) { Int32.TryParse(File.ReadAllText(countPath).Trim(), out n); }
        n++;
        File.WriteAllText(countPath, n.ToString());
        File.AppendAllText(log, "start " + n + ": " + joined + Environment.NewLine);

        int runtime = Nth("FCP_FAKE_RECORDER_RUNTIME", n, 0);
        if (runtime > 0) { Thread.Sleep(runtime * 1000); }
        return Nth("FCP_FAKE_RECORDER_EXITS", n, 0);
    }
}
'@
    Add-Type -TypeDefinition $fakeSource -OutputAssembly $fakeChild -OutputType ConsoleApplication

    $scenario = 0
    function Invoke-Supervisor(
        [string]$Exits,
        [string]$Runtime,
        [int[]]$Ladder,
        [int]$Max,
        [int]$Healthy,
        [int]$Started
    ) {
        $script:scenario++
        $log = Join-Path $tempRoot ('run-' + $script:scenario + '.log')
        $env:FCP_FAKE_RECORDER_LOG = $log
        $env:FCP_FAKE_RECORDER_EXITS = $Exits
        $env:FCP_FAKE_RECORDER_RUNTIME = $Runtime
        try {
            & $supervisor `
                -RepoRoot $repoRoot `
                -PythonExecutable $fakeChild `
                -RestartBackoffLadderSeconds $Ladder `
                -MaxRapidRestarts $Max `
                -HealthyRuntimeSeconds $Healthy `
                -StartedRuntimeSeconds $Started 2>&1 | Out-Null
            $code = $LASTEXITCODE
        }
        finally {
            Remove-Item Env:FCP_FAKE_RECORDER_LOG -ErrorAction SilentlyContinue
            Remove-Item Env:FCP_FAKE_RECORDER_EXITS -ErrorAction SilentlyContinue
            Remove-Item Env:FCP_FAKE_RECORDER_RUNTIME -ErrorAction SilentlyContinue
        }
        $starts = 0
        if (Test-Path -LiteralPath ($log + '.count')) {
            $starts = [int](Get-Content -LiteralPath ($log + '.count') -Raw).Trim()
        }
        return [pscustomobject]@{ ExitCode = $code; Starts = $starts }
    }

    Write-Host 'Scenario 1: a clean operator stop is never restarted'
    $r = Invoke-Supervisor '0' '0' @(0) 5 120 1
    Assert-Equal 'clean stop exit code' 0 $r.ExitCode
    Assert-Equal 'clean stop child starts' 1 $r.Starts

    Write-Host 'Scenario 2: Ctrl+C that bypassed the handler is never restarted'
    # -1073741510 is STATUS_CONTROL_C_EXIT (0xC000013A) as a signed int. No
    # POSIX child can return this, which is why this case is Windows-only.
    $r = Invoke-Supervisor '-1073741510' '0' @(0) 5 120 1
    Assert-Equal 'ctrl+c exit code' -1073741510 $r.ExitCode
    Assert-Equal 'ctrl+c child starts' 1 $r.Starts

    Write-Host 'Scenario 3: a recorder that never started propagates its own code'
    # Startup failure: the child never reaches the started threshold, so the
    # operator gets exit code 7 at once rather than the same failure four more
    # times and a fence code that hides it.
    $r = Invoke-Supervisor '7' '0' @(5, 15, 45, 120) 5 120 10
    Assert-Equal 'never-started exit code' 7 $r.ExitCode
    Assert-Equal 'never-started child starts' 1 $r.Starts

    Write-Host 'Scenario 4: a started recorder that fails is restarted'
    $r = Invoke-Supervisor '1,0' '2,0' @(0) 5 120 1
    Assert-Equal 'restart-then-stop exit code' 0 $r.ExitCode
    Assert-Equal 'restart-then-stop child starts' 2 $r.Starts

    Write-Host 'Scenario 5: repeated rapid failures fence supervision'
    $r = Invoke-Supervisor '1' '2,0,0,0,0' @(0) 5 120 1
    Assert-Equal 'fenced exit code' 6 $r.ExitCode
    Assert-Equal 'fenced child starts' 5 $r.Starts

    Write-Host 'Scenario 6: the fence honours its threshold exactly'
    $r = Invoke-Supervisor '1' '2,0,0' @(0) 3 120 1
    Assert-Equal 'fence-at-3 exit code' 6 $r.ExitCode
    Assert-Equal 'fence-at-3 child starts' 3 $r.Starts

    Write-Host 'Scenario 7: backoff is bounded -- a two-rung ladder clamps'
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $r = Invoke-Supervisor '1' '2,0,0,0,0' @(1, 2) 5 120 1
    $elapsed = $clock.Elapsed.TotalSeconds
    Assert-Equal 'bounded backoff exit code' 6 $r.ExitCode
    Assert-Equal 'bounded backoff child starts' 5 $r.Starts
    # Four waits of 1,2,2,2 = 7s, plus a 2s first child. Doubling without a
    # ceiling would be 1+2+4+8 = 15s.
    if ($elapsed -gt 14) {
        $failures.Add("backoff grew beyond the ladder ceiling: ${elapsed}s")
        Write-Host "  FAIL backoff grew beyond the ladder ceiling: ${elapsed}s"
    }
    else {
        Write-Host ("  ok   backoff stayed within the ladder ceiling: " + [int]$elapsed + "s")
    }

    Write-Host 'Scenario 8: a healthy runtime decays the crash fence'
    # Without decay the third rapid failure would fence at 3 starts. The third
    # child stays up past the healthy threshold, so the streak restarts there
    # and the fence is only reached two failures later.
    $r = Invoke-Supervisor '1,1,1,1,1' '2,0,3,0,0' @(0) 3 2 1
    Assert-Equal 'healthy-decay exit code' 6 $r.ExitCode
    Assert-Equal 'healthy-decay child starts' 5 $r.Starts

    Write-Host 'Scenario 9: the approved update path still runs finalize unchanged'
    $r = Invoke-Supervisor '75' '0' @(0) 5 120 1
    # The fake agent refuses, so the supervisor takes its existing refusal exit.
    Assert-Equal 'approved update refusal exit code' 5 $r.ExitCode
    Assert-Equal 'approved update child starts' 1 $r.Starts
    $agentLog = Join-Path $tempRoot ('run-' + $scenario + '.log.agent')
    if (-not (Test-Path -LiteralPath $agentLog)) {
        $failures.Add('the approved update path did not call the host agent')
        Write-Host '  FAIL the approved update path did not call the host agent'
    }
    else {
        $agentCall = (Get-Content -LiteralPath $agentLog -Raw)
        foreach ($expected in @('--repo-root', '--supervisor-session', '--finalize')) {
            if ($agentCall -notlike "*$expected*") {
                $failures.Add("the finalize call lost $expected")
                Write-Host "  FAIL the finalize call lost $expected"
            }
        }
        Write-Host '  ok   finalize was called with its existing arguments'
    }

    if ($failures.Count -gt 0) {
        Write-Host ''
        Write-Host ('FAILED: ' + $failures.Count + ' supervisor restart assertions')
        $failures | ForEach-Object { Write-Host ('  - ' + $_) }
        exit 1
    }
    Write-Host ''
    Write-Host 'All recorder supervisor restart scenarios passed.'
}
finally {
    Remove-Item -LiteralPath $tempRoot -Recurse -ErrorAction SilentlyContinue
}

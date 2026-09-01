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
    // Two fake recorders can be alive at once: the supervisor starts the trial
    // watchdog agent call asynchronously while a recorder child is still
    // running, which is exactly what scenario 10 drives. File.AppendAllText
    // opens with FileShare.Read, so the second writer takes a sharing violation
    // and the process dies with an unhandled IOException. The supervisor then
    // reads that as a failed agent call and the scenario's assertions collapse.
    // Serialize each append with an exclusive handle and retry instead.
    private const int ShareRetryMilliseconds = 5000;
    private const int ShareRetryStepMilliseconds = 25;

    private static void WithRetry(Action action)
    {
        int waited = 0;
        while (true)
        {
            try
            {
                action();
                return;
            }
            catch (IOException)
            {
                if (waited >= ShareRetryMilliseconds) { throw; }
                Thread.Sleep(ShareRetryStepMilliseconds);
                waited += ShareRetryStepMilliseconds;
            }
        }
    }

    private static void AppendShared(string path, string text)
    {
        WithRetry(delegate
        {
            using (FileStream stream = new FileStream(
                path, FileMode.Append, FileAccess.Write, FileShare.None))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                writer.Write(text);
            }
        });
    }

    // Read-modify-write of a counter is not atomic either, so two overlapping
    // processes could both claim the same ordinal and the scripted per-start
    // runtime/exit tables would be read off by one. One exclusive handle per
    // bump keeps the sequence a sequence.
    private static int NextCount(string path)
    {
        int value = 0;
        WithRetry(delegate
        {
            using (FileStream stream = new FileStream(
                path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
            {
                StreamReader reader = new StreamReader(stream);
                int parsed;
                value = Int32.TryParse(reader.ReadToEnd().Trim(), out parsed) ? parsed : 0;
                value++;
                stream.SetLength(0);
                stream.Position = 0;
                StreamWriter writer = new StreamWriter(stream);
                writer.Write(value.ToString());
                writer.Flush();
            }
        });
        return value;
    }

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
            AppendShared(log + ".agent", joined + Environment.NewLine);
            if (joined.Contains("--finalize"))
            {
                // A scripted sequence of launch plans, one per finalize, so the
                // real trial -> rollback transition can be driven end to end.
                string plans = Environment.GetEnvironmentVariable("FCP_FAKE_AGENT_PLANS");
                if (String.IsNullOrWhiteSpace(plans)) { return 1; }
                int f = NextCount(log + ".fin");
                string[] steps = plans.Split(';');
                string step = f <= steps.Length ? steps[f - 1] : steps[steps.Length - 1];
                string root = Environment.GetEnvironmentVariable("FCP_FAKE_LAUNCH_ROOT");
                string commit = Environment.GetEnvironmentVariable("FCP_FAKE_BUILD_COMMIT");
                if (step == "none") { return 1; }
                if (step == "rollback_failed")
                {
                    Console.WriteLine("{\"relaunch\": false, \"mode\": \"rollback\", " +
                        "\"code\": \"rollback_failed\", \"launch_root\": \"\", " +
                        "\"data_directory\": \"\", \"build_commit\": \"\"}");
                    return 0;
                }
                Console.WriteLine("{\"relaunch\": true, \"mode\": \"" + step + "\", " +
                    "\"code\": \"" + step + "_launch\", \"launch_root\": \"" +
                    root.Replace("\\", "\\\\") + "\", \"data_directory\": \"\", " +
                    "\"build_commit\": \"" + commit + "\"}");
                return 0;
            }
            return 1;
        }

        int n = NextCount(log + ".count");
        AppendShared(log, "start " + n + ": " + joined + Environment.NewLine);

        int runtime = Nth("FCP_FAKE_RECORDER_RUNTIME", n, 0);
        if (runtime > 0) { Thread.Sleep(runtime * 1000); }
        return Nth("FCP_FAKE_RECORDER_EXITS", n, 0);
    }
}
'@
    Add-Type -TypeDefinition $fakeSource -OutputAssembly $fakeChild -OutputType ConsoleApplication

    # The supervisor refuses a plan that does not name an exact commit, so the
    # scripted plans answer with this checkout's real head.
    $headCommit = (& git -C $repoRoot rev-parse --verify 'HEAD^{commit}').Trim()

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
        $env:FCP_FAKE_LAUNCH_ROOT = $repoRoot
        $env:FCP_FAKE_BUILD_COMMIT = $headCommit
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
            Remove-Item Env:FCP_FAKE_AGENT_PLANS -ErrorAction SilentlyContinue
            Remove-Item Env:FCP_FAKE_LAUNCH_ROOT -ErrorAction SilentlyContinue
            Remove-Item Env:FCP_FAKE_BUILD_COMMIT -ErrorAction SilentlyContinue
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

    Write-Host 'Scenario 10: a failed rollback goes back to the agent, never to ordinary restart'
    # The regression this scenario exists for. In order:
    #   child 1  ordinary, runs past the started threshold, then fails
    #            -> one ordinary restart, and $everStarted becomes true
    #   child 2  exits 75 -> finalize #1 plans a TRIAL
    #   child 3  the trial child fails -> finalize #2 plans a ROLLBACK
    #   child 4  the rollback child fails
    #
    # Child 4 must go back to the agent, not into the restart machine that
    # child 1 already armed. Keying transition state off "trial" alone made
    # child 4 look ordinary, so it was restarted to the fence and the agent
    # never got to record ROLLBACK_VERIFYING -> ROLLBACK_FAILED.
    $env:FCP_FAKE_AGENT_PLANS = 'trial;rollback;rollback_failed'
    $r = Invoke-Supervisor '1,75,1,1' '2,0,0,0' @(0) 5 120 1
    # The agent refused the last plan, so the supervisor takes its refusal exit.
    Assert-Equal 'rollback verdict exit code' 5 $r.ExitCode
    Assert-Equal 'children started through the transition' 4 $r.Starts

    $agentLog = Join-Path $tempRoot ('run-' + $scenario + '.log.agent')
    $agentCalls = @(Get-Content -LiteralPath $agentLog)
    $finalizes = @($agentCalls | Where-Object { $_ -like '*--finalize*' }).Count
    # Three finalizes: plan the trial, plan the rollback, and record the
    # rollback failure. The third is the one the regression lost entirely.
    Assert-Equal 'finalize calls (the third records the rollback verdict)' 3 $finalizes

    # Exactly one rollback launch: the agent is asked once and answers once.
    $marked = @($agentCalls | Where-Object { $_ -like '*--mark-relaunched*' }).Count
    Assert-Equal 'relaunches recorded (trial + rollback)' 2 $marked
    # And the watchdog is still trial-only: a rollback child never adds a second
    # one. This is a ceiling rather than an equality because the watchdog is the
    # one agent call the supervisor starts asynchronously, so its log line can
    # lag; two would prove a rollback started one, which is what matters here.
    # That it is started *only* for a trial child is pinned statically by
    # test_the_supervisor_starts_nothing_before_the_recorder.
    $watched = @($agentCalls | Where-Object { $_ -like '*--watch-trial*' }).Count
    if ($watched -gt 1) {
        $failures.Add("a rollback child started a trial watchdog: $watched")
        Write-Host "  FAIL a rollback child started a trial watchdog: $watched"
    }
    else {
        Write-Host "  ok   trial watchdogs started = $watched (never more than one)"
    }

    # The child count carries the restart evidence on its own, and it is exact
    # in both directions. Four children is only reachable if child 1 *was*
    # ordinarily restarted -- without that, supervision ends at one child and
    # $everStarted is never armed -- and only if child 4 was *not*, because an
    # ordinarily restarted rollback child would keep going to the fence. The
    # pre-fix supervisor ran eight children here and never reached finalize #3.

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

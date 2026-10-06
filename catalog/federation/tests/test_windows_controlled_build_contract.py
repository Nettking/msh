from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _controlled_build_functions() -> str:
    source = _read("scripts/windows/fcp_host_build.ps1")
    return (
        source[source.index("function ConvertTo-WindowsProcessArgument"):
               source.index("function Invoke-BoundedDockerResult")]
        + source[source.index("function Start-FcpBuildProcess"):
                 source.index("function Get-NonEmptyTextLines")]
    )


@pytest.mark.skipif(os.name != "nt", reason="native Windows process exit status")
@pytest.mark.parametrize("child_exit", [0, 7])
@pytest.mark.parametrize("observe_after_exit", [False, True])
def test_native_build_child_exit_controls_success_and_cleanup(
    tmp_path: Path, child_exit: int, observe_after_exit: bool,
) -> None:
    # Execute the real controller function with a real native child process.
    # Its Python script stands in for Docker; only disk/builder I/O is stubbed.
    function = _controlled_build_functions()
    (tmp_path / "compose").write_text(
        "import sys\nprint('native-build-stdout', flush=True)\n"
        "print('native-build-stderr', file=sys.stderr, flush=True)\n"
        f"raise SystemExit({child_exit})\n"
    )
    def quote(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    script = tmp_path / "native-exit.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        "Set-StrictMode -Version Latest\n"
        f"$RepoRoot = {quote(tmp_path)}\n"
        f"$script:DockerExe = {quote(sys.executable)}\n"
        "Set-Location -LiteralPath $RepoRoot\n"
        "[Environment]::CurrentDirectory = $RepoRoot\n"
        "$BuildTimeoutSeconds = 10\n$BuildPollMilliseconds = 10\n"
        "$script:prunes = 0\n$script:writerStopped = $false\n"
        "function Ensure-FcpControllableBuilder { return 'fcp-build-test' }\n"
        "function Get-FcpResourceFreeBytes { return 1099511627776 }\n"
        "function Get-FcpResourcePressureLevel { return 'normal' }\n"
        "function Invoke-BuildCachePrune { $script:prunes++; "
        "$script:writerStopped = $true; return $true }\n"
        "function Stop-FcpBuildWriter { $script:writerStopped = $true; "
        "return $true }\n"
        + function
        + (
            "\n$script:OwnedBuildProcessFactory = ${function:Start-FcpBuildProcess}\n"
            "function Start-FcpBuildProcess { param([string]$Executable,[string[]]$Arguments)\n"
            "  $child = & $script:OwnedBuildProcessFactory $Executable $Arguments\n"
            "  $child.WaitForExit()\n  return $child\n}\n"
            if observe_after_exit else ""
        )
        + "\n$failure = $null\n"
        "try { Invoke-ControlledCoreBuild 'unused' } "
        "catch { $failure = $_.Exception.Message }\n"
        "@{ failure=$failure; prunes=$script:prunes; "
        "writerStopped=$script:writerStopped } | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy",
         "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    assert "native-build-stdout" in result.stdout
    assert "native-build-stderr" in result.stderr
    outcome = json.loads(result.stdout.splitlines()[-1])
    expected = f"core_image_build_failed:{child_exit}" if child_exit else None
    assert outcome["failure"] == expected
    assert outcome["prunes"] == 1
    assert outcome["writerStopped"] is True


@pytest.mark.skipif(os.name != "nt", reason="native Windows process lifecycle")
@pytest.mark.parametrize("reason", ["timeout", "pressure"])
def test_native_build_failure_stops_the_owned_child_before_writer_cleanup(
    tmp_path: Path, reason: str,
) -> None:
    (tmp_path / "compose").write_text("import time\ntime.sleep(15)\n")
    script = tmp_path / "bounded-failure.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\nSet-StrictMode -Version Latest\n"
        f"$RepoRoot = '{str(tmp_path).replace(chr(39), chr(39) * 2)}'\n"
        f"$script:DockerExe = '{str(sys.executable).replace(chr(39), chr(39) * 2)}'\n"
        "$BuildTimeoutSeconds = 0\n$BuildPollMilliseconds = 10\n"
        "$script:childStopped = $false\n$script:writerSettled = $false\n"
        "function Ensure-FcpControllableBuilder { return 'fcp-build-test' }\n"
        "function Get-FcpResourceFreeBytes { return 1099511627776 }\n"
        f"function Get-FcpResourcePressureLevel {{ return '{'pressure' if reason == 'pressure' else 'normal'}' }}\n"
        "function Stop-BuildClient { param([System.Diagnostics.Process]$Process)\n"
        "  $Process.Kill(); $Process.WaitForExit(); $script:childStopped = $Process.HasExited\n"
        "  return $script:childStopped\n}\n"
        "function Settle-FcpBuildWriter { param($Name,[switch]$DiscardCache)\n"
        "  if (-not $script:childStopped) { throw 'cleanup_before_child_stopped' }\n"
        "  $script:writerSettled = $true\n"
        "  return [pscustomobject]@{ Quiescent = $true; CacheDiscarded = $true }\n}\n"
        + _controlled_build_functions()
        + "\n$failure = $null\ntry { Invoke-ControlledCoreBuild 'unused' } catch { $failure = $_.Exception.Message }\n"
        "@{ failure=$failure; childStopped=$script:childStopped; writerSettled=$script:writerSettled } | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout.splitlines()[-1])
    assert outcome == {
        "failure": "build_resource_pressure" if reason == "pressure" else "core_image_build_timeout",
        "childStopped": True, "writerSettled": True,
    }


@pytest.mark.skipif(os.name != "nt", reason="native Windows argument forwarding")
def test_owned_build_process_preserves_special_arguments_and_working_directory(tmp_path: Path) -> None:
    directory = tmp_path / "working directory & spaces"
    directory.mkdir()
    child = directory / "echo arguments.py"
    child.write_text("import json,os,sys\nprint(json.dumps({'argv':sys.argv[1:],'cwd':os.getcwd()}))\n")
    argument = 'quoted "value" with a trailing slash\\'
    def quote(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"
    script = tmp_path / "owned-arguments.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\nSet-StrictMode -Version Latest\n"
        f"$RepoRoot = {quote(directory)}\n"
        + _controlled_build_functions()
        + f"\n$process = Start-FcpBuildProcess {quote(sys.executable)} @({quote(child)}, {quote(argument)})\n"
        "try { $process.WaitForExit(); [System.Threading.Tasks.Task]::WaitAll([System.Threading.Tasks.Task[]]$process.FcpOutputTasks); if ($process.ExitCode -ne 0) { throw 'child_failed' } } finally { $process.Dispose() }\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"argv": [argument], "cwd": str(directory)}


@pytest.mark.skipif(os.name != "nt", reason="native Windows process startup")
def test_owned_build_process_start_failure_cannot_report_success(tmp_path: Path) -> None:
    script = tmp_path / "missing-child.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\nSet-StrictMode -Version Latest\n"
        f"$RepoRoot = '{str(tmp_path).replace(chr(39), chr(39) * 2)}'\n"
        + _controlled_build_functions()
        + "\n$failure = $null\ntry { $process = Start-FcpBuildProcess 'Z:\\absent-fcp-child.exe' @('compose') } catch { $failure = $_.Exception.Message }\n"
        "@{ failure=$failure } | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20,
        check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"failure": "core_image_build_failed"}


@pytest.mark.skipif(os.name != "nt", reason="native Windows output-drain lifecycle")
@pytest.mark.parametrize(
    "child_exit,output_state,prune_ok,writer_ok,expected",
    [
        (0, "faulted", True, True, "core_image_build_output_incomplete"),
        (0, "pending", True, True, "core_image_build_output_incomplete"),
        (7, "faulted", True, True, "core_image_build_failed:7"),
        (0, "faulted", False, True, "build_failed_and_cache_prune_failed"),
        (0, "faulted", False, False, "build_writer_stop_unverified"),
    ],
)
def test_native_build_output_drain_fails_closed_and_preserves_cleanup_precedence(
    tmp_path: Path, child_exit: int, output_state: str,
    prune_ok: bool, writer_ok: bool, expected: str,
) -> None:
    # The real owned child has finished; only its output-copy task outcome is
    # substituted to discriminate the bounded drain from process exit status.
    (tmp_path / "compose").write_text(f"raise SystemExit({child_exit})\n")

    def quote(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    script = tmp_path / "output-drain.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\nSet-StrictMode -Version Latest\n"
        f"$RepoRoot = {quote(tmp_path)}\n"
        f"$script:DockerExe = {quote(sys.executable)}\n"
        "$BuildTimeoutSeconds = 10\n$BuildPollMilliseconds = 10\n"
        "$script:prunes = 0\n$script:writerStops = 0\n"
        "function Ensure-FcpControllableBuilder { return 'fcp-build-test' }\n"
        "function Get-FcpResourceFreeBytes { return 1099511627776 }\n"
        "function Get-FcpResourcePressureLevel { return 'normal' }\n"
        "function Invoke-BuildCachePrune { $script:prunes++; "
        f"return ${str(prune_ok).lower()} }}\n"
        "function Stop-FcpBuildWriter { $script:writerStops++; "
        f"return ${str(writer_ok).lower()} }}\n"
        + _controlled_build_functions()
        + "\n$script:OwnedBuildProcessFactory = ${function:Start-FcpBuildProcess}\n"
        "function Start-FcpBuildProcess { param([string]$Executable,[string[]]$Arguments)\n"
        "  $child = & $script:OwnedBuildProcessFactory $Executable $Arguments\n"
        "  $child.WaitForExit()\n"
        "  $copy = New-Object 'System.Threading.Tasks.TaskCompletionSource[bool]'\n"
        + (
            "  $copy.SetException((New-Object System.InvalidOperationException 'fixture_copy_fault'))\n"
            if output_state == "faulted" else ""
        )
        + "  Add-Member -InputObject $child -NotePropertyName FcpOutputTasks -NotePropertyValue @($copy.Task) -Force\n"
        "  $script:ownedHandle = $child.SafeHandle\n  return $child\n}\n"
        "$timer = [System.Diagnostics.Stopwatch]::StartNew()\n"
        "$failure = $null\ntry { Invoke-ControlledCoreBuild 'unused' } catch { $failure = $_.Exception.Message }\n"
        "$timer.Stop()\n$disposed = $script:ownedHandle.IsClosed\n"
        "@{ failure=$failure; prunes=$script:prunes; writerStops=$script:writerStops; "
        "disposed=$disposed; elapsed=$timer.Elapsed.TotalSeconds } | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20, check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout.splitlines()[-1])
    assert outcome["failure"] == expected
    assert outcome["prunes"] == 1
    assert outcome["writerStops"] == (0 if prune_ok else 1)
    assert outcome["disposed"] is True
    assert outcome["elapsed"] < 12
    if output_state == "pending":
        assert outcome["elapsed"] >= 4.5


@pytest.mark.skipif(os.name != "nt", reason="native Windows process startup cleanup")
@pytest.mark.parametrize("stage", ["output-setup", "start"])
def test_failed_build_start_stops_its_child_and_settles_only_the_owned_writer(tmp_path: Path, stage: str) -> None:
    (tmp_path / "compose").write_text("import time\ntime.sleep(15)\n")
    source = _read("scripts/windows/fcp_host_build.ps1")
    stop_client = source[source.index("function Stop-BuildClient"):
                         source.index("function Start-FcpBuildProcess")]
    executable = str(sys.executable) if stage == "output-setup" else str(tmp_path / "absent-child.exe")
    script = tmp_path / "setup-failure.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\nSet-StrictMode -Version Latest\n"
        f"$RepoRoot = '{str(tmp_path).replace(chr(39), chr(39) * 2)}'\n"
        f"$script:DockerExe = '{executable.replace(chr(39), chr(39) * 2)}'\n"
        "$script:childId = $null\n$script:childStoppedBeforeWriter = $false\n$script:writerName = $null\n"
        "function Ensure-FcpControllableBuilder { return 'fcp-build-isolated' }\n"
        "function Add-Member { param($InputObject,$NotePropertyName,$NotePropertyValue)\n"
        "  $script:childId = $InputObject.Id; throw 'injected-output-setup-failure'\n}\n"
        "function Settle-FcpBuildWriter { param($Name,[switch]$DiscardCache)\n"
        "  $script:writerName = $Name\n"
        "  $alive = if ($null -ne $script:childId) { Get-Process -Id $script:childId -ErrorAction SilentlyContinue } else { $null }\n"
        "  $script:childStoppedBeforeWriter = ($null -eq $alive)\n"
        "  return [pscustomobject]@{ Quiescent = $true; CacheDiscarded = $true }\n}\n"
        + stop_client + _controlled_build_functions()
        + "\n$failure = $null\ntry { Invoke-ControlledCoreBuild 'unused' } catch { $failure = $_.Exception.Message }\n"
        "@{ failure=$failure; childStoppedBeforeWriter=$script:childStoppedBeforeWriter; writerName=$script:writerName } | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=20, check=False,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "failure": "core_image_build_failed", "childStoppedBeforeWriter": True, "writerName": "fcp-build-isolated",
    }


def test_windows_host_build_owns_a_checkout_scoped_buildkit_writer() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "function Get-FcpBuilderName" in text
    # The prefix is now a named constant because retirement has to recognise
    # FCP's own builders by it. The identity it composes is unchanged.
    assert "$BuilderPrefix = 'fcp-build-'" in text
    assert "$BuilderPrefix + (Get-PathHash $RepoRoot)" in text
    assert "'--driver', 'docker-container'" in text
    assert "'--driver-opt', 'default-load=true'" in text
    assert "'compose', 'build', '--help'" in text
    assert "controllable_builder_unavailable" in text
    assert "'compose', 'build'" in text
    assert "'--builder', $name" in text
    assert "'relay', 'flask', 'recorder'" in text
    assert "& docker compose build relay flask recorder" not in text


def test_windows_active_build_stops_client_tree_and_proves_writer_at_pressure() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    stop_client = text[text.index("function Stop-BuildClient") : text.index("function Invoke-ControlledCoreBuild")]
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]

    assert "$BuildPollMilliseconds = 250" in text
    assert "taskkill.exe /PID $Process.Id /T /F" in stop_client
    assert "while (-not $process.HasExited)" in build
    assert "Get-FcpResourceFreeBytes -BackingPath $BackingPath" in build
    assert "$level -in @('pressure', 'critical')" in build
    active_build = build[build.index("while (-not $process.HasExited)"):]
    assert active_build.index("Stop-BuildClient $process") < active_build.index(
        "Settle-FcpBuildWriter $name -DiscardCache"
    )
    assert "build_writer_stop_unverified" in build
    assert "build_resource_pressure" in build
    assert "core_image_build_timeout" in build


def test_windows_build_cache_is_scoped_and_writer_is_stopped_after_success() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    prune = text[text.index("function Invoke-BuildCachePrune") : text.index("function Stop-BuildClient")]
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]

    assert "'buildx', 'prune'" in prune
    assert "'--builder', $name" in prune
    assert "--keep-storage=$BuildCacheKeepBytes" in prune
    assert "'builder', 'prune'" not in prune
    assert prune.count("Stop-FcpBuildWriter $name") >= 2
    assert build.rindex("Stop-FcpBuildWriter $name") > build.index("Invoke-BuildCachePrune")


def test_windows_build_requires_exact_image_identity_before_success() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "function Assert-CoreImageCommits" in text
    assert "function Resolve-CoreImageReference" in text
    assert "'compose', 'config', '--images', $Service" in text
    assert "$references.Count -ne 1" in text
    assert "StdOut" in text
    assert 'no.fcp.build_commit' in text
    assert "built_image_identity_unavailable" in text
    assert "built_image_identity_mismatch" in text
    assert text.index("Invoke-ControlledCoreBuild $backingPath") < text.index(
        "Assert-CoreImageCommits $commit"
    ) < text.index("Write-AtomicText $OutputFile $commit")


def test_windows_image_identity_is_precontainer_and_service_scoped() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    resolver = text[text.index("function Resolve-CoreImageReference") : text.index("function Assert-CoreImageCommits")]
    identity = text[text.index("function Assert-CoreImageCommits") : text.index("function Assert-DiskPreflight")]

    assert "'compose', 'config', '--images', $Service" in resolver
    assert "'compose', 'images', '-q'" not in resolver
    assert "if ($resolved.ExitCode -ne 0)" in resolver
    assert "$references.Count -ne 1" in resolver
    assert "resolved.StdOut" in resolver
    assert "resolved.StdErr" not in resolver
    assert "Invoke-BoundedDockerResult" in identity
    assert "'image', 'inspect'" in identity
    assert "inspection.StdOut" in identity
    assert "inspection.StdErr" not in identity
    assert "Resolve-CoreImageReference $service" in identity
    assert "@('relay', 'flask', 'recorder')" in identity


def test_windows_image_identity_fails_closed_for_missing_or_wrong_artifacts() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    identity = text[text.index("function Assert-CoreImageCommits") : text.index("function Assert-DiskPreflight")]

    assert identity.count("throw 'built_image_identity_unavailable'") >= 4
    assert "throw 'built_image_identity_mismatch'" in identity
    assert "$inspection.ExitCode -ne 0" in identity
    assert "$lines.Count -ne 1" in identity
    assert "$label -ne $Commit" in identity


def test_windows_machine_values_never_promote_stderr_to_image_or_label() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    bounded = text[text.index("function Invoke-BoundedDockerResult") : text.index("function Get-FcpBuilderName")]
    resolver = text[text.index("function Resolve-CoreImageReference") : text.index("function Assert-CoreImageCommits")]
    identity = text[text.index("function Assert-CoreImageCommits") : text.index("function Assert-DiskPreflight")]

    assert "StdOut =" in bounded
    assert "StdErr =" in bounded
    assert "resolved.StdOut" in resolver
    assert "resolved.Output" not in resolver
    assert "inspection.StdOut" in identity
    assert "inspection.Output" not in identity


def test_windows_proxy_real_docker_override_is_private_to_controlled_build() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    resolve = text[text.index("function Resolve-DockerExecutable") : text.index("function Invoke-DockerResult")]

    assert "$env:FCP_CONTROLLED_BUILD_ACTIVE -eq '1'" in resolve
    assert "$env:FCP_REAL_DOCKER_EXE" in resolve
    assert "Get-Command docker -CommandType Application" in resolve


def test_windows_update_proxy_intercepts_only_fixed_build_shapes() -> None:
    proxy = _read("scripts/windows/fcp_docker_build_proxy.cmd")

    assert 'if not "%FCP_CONTROLLED_BUILD_ACTIVE%"=="1" goto :forward' in proxy
    assert '"%~1"=="compose"' in proxy
    assert '"%~2"=="build"' in proxy
    assert '"%~3"=="relay"' in proxy
    assert '"%~4"=="flask"' in proxy
    assert '"%~5"=="recorder"' in proxy
    assert 'if "%~6"=="" goto :controlled_build' in proxy
    assert '"%~1"=="builder"' in proxy and '"%~2"=="prune"' in proxy
    assert '"%FCP_REAL_DOCKER_EXE%" %*' in proxy
    assert "-LeaseAlreadyHeld" in proxy
    assert "-CacheCleanupOnly" in proxy
    assert "Invoke-Expression" not in proxy


def test_windows_update_runner_keeps_proxy_inside_host_mutation_lease() -> None:
    runner = _read("scripts/windows/fcp_update_agent_runner.ps1")

    assert "Global\\FCPHostMutation-$pathHash" in runner
    assert "fcp_docker_build_proxy.cmd" in runner
    assert "Join-Path $proxyDirectory 'docker.cmd'" in runner
    assert "$env:FCP_REAL_DOCKER_EXE" in runner
    assert "$env:FCP_CONTROLLED_BUILD_ACTIVE = '1'" in runner
    assert "$env:FCP_HOST_MUTATION_LEASE_ACTIVE = '1'" in runner
    assert "$env:PATH = $proxyDirectory" in runner
    assert "Restore-ProcessEnvironment 'PATH'" in runner
    assert "Invoke-PostBuildCachePrune" in runner
    assert runner.index("$mutationAcquired = $mutationMutex.WaitOne") < runner.index(
        "$env:FCP_CONTROLLED_BUILD_ACTIVE = '1'"
    ) < runner.index("& powershell.exe")

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_windows_builder_absence_requires_successful_enumeration() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "function Test-FcpBuilderAbsent" in text
    assert "'buildx', 'ls', '--format', '{{.Name}}'" in text
    assert "if ($inspection.ExitCode -ne 0) {\n        return Test-FcpBuilderAbsent $Name\n    }" in text
    assert "if ($inspection.ExitCode -ne 0) { return Test-FcpBuilderAbsent $Name }" in text
    assert "if ($inspection.ExitCode -ne 0) { return $true }" not in text


def test_windows_buildx_lifecycle_calls_are_bounded() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "$DockerLifecycleTimeoutSeconds = 30" in text
    assert "function Invoke-BoundedDockerResult" in text
    assert "$process.WaitForExit($TimeoutSeconds * 1000)" in text
    assert "$exitCode = 124" in text
    assert "return Invoke-BoundedDockerResult @('buildx', 'inspect', $Name)" in text
    assert "$stopped = Invoke-BoundedDockerResult @('buildx', 'stop', $Name)" in text
    assert "$pruned = Invoke-BoundedDockerResult @(" in text


@pytest.mark.skipif(os.name != "nt", reason="native Windows bounded Docker process")
@pytest.mark.parametrize(
    ("cli_exit", "absence_proven"), [(0, True), (7, True), (0, False)],
)
def test_windows_remove_builder_uses_supported_cli_and_keeps_outer_bound(
    tmp_path: Path, cli_exit: int, absence_proven: bool,
) -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    bounded = text[
        text.index("function ConvertTo-WindowsProcessArgument"):
        text.index("function Get-FcpBuilderName")
    ]
    remove = text[text.index("function Remove-FcpBuilder"):text.index("function Settle-FcpBuildWriter")]
    (tmp_path / "buildx").write_text(
        "import json, pathlib, sys\n"
        "args = sys.argv[1:]\n"
        "pathlib.Path('actual-argv.json').write_text(json.dumps(args))\n"
        "if '--timeout' in args:\n"
        "    sys.stderr.write('unknown flag: --timeout\\n')\n"
        "    raise SystemExit(2)\n"
        "assert args == ['rm', '--force', 'fcp-build-test']\n"
        f"raise SystemExit({cli_exit})\n",
    )

    def quote(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    script = tmp_path / "remove-builder.ps1"
    script.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        f"$RepoRoot = {quote(tmp_path)}\n"
        f"$script:DockerExe = {quote(sys.executable)}\n"
        "Set-Location -LiteralPath $RepoRoot\n"
        "[Environment]::CurrentDirectory = $RepoRoot\n"
        "$DockerLifecycleTimeoutSeconds = 30\n"
        + bounded
        + "\n$script:RealBoundedDocker = ${function:Invoke-BoundedDockerResult}\n"
        "function Invoke-BoundedDockerResult { param([string[]]$Arguments,"
        "[int]$TimeoutSeconds = $DockerLifecycleTimeoutSeconds)\n"
        " $script:ObservedTimeout = $TimeoutSeconds\n"
        " return & $script:RealBoundedDocker -Arguments $Arguments -TimeoutSeconds $TimeoutSeconds\n"
        "}\n"
        "function Get-FcpBuilderInspection { return [pscustomobject]@{ExitCode=0} }\n"
        "$script:AbsenceChecks = 0\n"
        "function Test-FcpBuilderAbsent { $script:AbsenceChecks++; return "
        + ("$true" if absence_proven else "$false")
        + " }\n"
        + remove
        + "\n$result = Remove-FcpBuilder 'fcp-build-test'\n"
        "[ordered]@{result=$result;timeout=$script:ObservedTimeout;"
        "absence_checks=$script:AbsenceChecks}|ConvertTo-Json -Compress\n",
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive",
         "-ExecutionPolicy", "Bypass", "-File", str(script)],
        cwd=tmp_path, capture_output=True, text=True, timeout=15, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    actual = json.loads(completed.stdout)
    assert actual["result"] is (cli_exit == 0 and absence_proven)
    assert actual["timeout"] == 30
    assert actual["absence_checks"] == (1 if cli_exit == 0 else 0)
    assert json.loads((tmp_path / "actual-argv.json").read_text()) == [
        "rm", "--force", "fcp-build-test",
    ]


def test_windows_bounded_docker_result_preserves_native_exit_codes() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    bounded = text[text.index("function Invoke-BoundedDockerResult") : text.index("function Get-FcpBuilderName")]

    assert "System.Diagnostics.ProcessStartInfo" in bounded
    assert "$startInfo.UseShellExecute = $false" in bounded
    assert "$process.Start()" in bounded
    assert "ReadToEndAsync()" in bounded
    assert "$exitCode = [int]$process.ExitCode" in bounded
    assert "$exitCode = 124" in bounded
    assert "$exitCode = 127" in bounded
    assert "taskkill.exe /PID $process.Id /T /F" in bounded
    assert "Start-Process" not in bounded


def test_windows_existing_builder_is_reproved_quiescent_before_reuse() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    ensure = text[text.index("function Ensure-FcpControllableBuilder") : text.index("function Invoke-BuildCachePrune")]

    assert "Test-FcpBuilderStopped $name" in ensure
    assert "Stop-FcpBuildWriter $name" in ensure
    assert "throw 'build_writer_stop_unverified'" in ensure
    assert ensure.index("Test-FcpBuilderStopped $name") < ensure.index("return $name")


def test_windows_pressure_requires_both_client_and_writer_quiescence() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    stop_client = text[text.index("function Stop-BuildClient") : text.index("function Invoke-ControlledCoreBuild")]
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]

    assert "return [bool]$Process.HasExited" in stop_client
    assert "$clientStopped = Stop-BuildClient $process" in build
    assert "$settled = Settle-FcpBuildWriter $name -DiscardCache" in build
    assert "if (-not $clientStopped -or -not $settled.Quiescent)" in build
    assert "build_writer_stop_unverified" in build
    # Discarding the cache is a separate claim from stopping the writer, so a
    # failed discard must not be reported through a quiescence-only success.
    assert "if (-not $settled.CacheDiscarded)" in build
    assert "build_cache_discard_failed" in build


def test_windows_preflight_reproves_quiescence_at_all_pressure_levels() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    preflight = text[text.index("function Assert-DiskPreflight") : text.index("function Write-AtomicText")]

    assert "$name = Get-FcpBuilderName" in preflight
    assert "Test-FcpBuilderStopped $name" in preflight
    assert "Stop-FcpBuildWriter $name" in preflight
    assert "Settle-FcpBuildWriter $name -DiscardCache" in preflight
    assert "throw 'build_writer_stop_unverified'" in preflight
    assert "throw 'build_cache_discard_failed'" in preflight
    assert "Invoke-BuildCachePrune" not in preflight


def test_windows_failed_build_attempt_still_bounds_its_fcp_cache() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]
    # Locate the failure branch by its cleanup action. Output collection is
    # now part of completion, so an old exact spelling of the exit-only guard
    # cannot identify every failed build. Retain both original failure cases
    # and require incomplete output to take the same bounded cleanup path.
    guards = list(re.finditer(
        r"if \(([^()\n]+)\) \{\s+\$cleanupOk = Invoke-BuildCachePrune", build
    ))
    assert len(guards) == 1
    guard = guards[0]
    assert set(guard.group(1).split(" -or ")) == {
        "$null -eq $exit", "$exit -ne 0", "-not $outputComplete"
    }
    failed = build[guard.start() : build.index("if (-not (Invoke-BuildCachePrune))")]

    assert "$cleanupOk = Invoke-BuildCachePrune" in failed
    assert "Stop-FcpBuildWriter $name" in failed
    assert "build_writer_stop_unverified" in failed
    assert "build_failed_and_cache_prune_failed" in failed
    assert failed.index("Invoke-BuildCachePrune") < failed.index("core_image_build_failed:$exit")


def test_windows_cache_only_cleanup_proves_quiescence_before_it_prunes() -> None:
    """The Update-All cache-only path reaches this branch through the private
    docker proxy. Pruning is what destroys cache, so it is the branch that most
    needs the writer proven dead first: an abandoned checkout-scoped BuildKit
    daemon left by an earlier crash would otherwise still be writing into the
    cache being pruned.
    """

    text = _read("scripts/windows/fcp_host_build.ps1")
    cleanup = text[
        text.index("if ($CacheCleanupOnly) {") : text.index("$commit = Get-CleanCommit")
    ]

    quiescence = cleanup.index("throw 'build_writer_stop_unverified'")
    prune = cleanup.index("Invoke-BuildCachePrune")
    assert quiescence < prune, (
        "cache-only cleanup pruned before proving the writer quiescent"
    )
    # Quiescence is proved unconditionally, not only on the branch that happens
    # not to prune.
    assert cleanup.count("Test-FcpBuilderStopped $name") == 1
    assert cleanup.index("Test-FcpBuilderStopped $name") < prune

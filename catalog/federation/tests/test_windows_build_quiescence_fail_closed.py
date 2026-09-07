from __future__ import annotations

from pathlib import Path

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
    failed = build[build.index("if ($exit -ne 0)") : build.index("if (-not (Invoke-BuildCachePrune))")]

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

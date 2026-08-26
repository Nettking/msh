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


def test_windows_pressure_requires_both_client_and_writer_quiescence() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    stop_client = text[text.index("function Stop-BuildClient") : text.index("function Invoke-ControlledCoreBuild")]
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]

    assert "return [bool]$Process.HasExited" in stop_client
    assert "$clientStopped = Stop-BuildClient $process" in build
    assert "$writerStopped = Stop-FcpBuildWriter $name -DiscardCache" in build
    assert "if (-not $clientStopped -or -not $writerStopped)" in build
    assert "build_writer_stop_unverified" in build


def test_windows_pressure_preflight_does_not_ignore_failed_cleanup() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    preflight = text[text.index("function Assert-DiskPreflight") : text.index("function Write-AtomicText")]

    assert "if (-not (Invoke-BuildCachePrune))" in preflight
    assert "throw 'build_cache_prune_failed'" in preflight

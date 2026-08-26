from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_windows_host_build_owns_a_checkout_scoped_buildkit_writer() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "function Get-FcpBuilderName" in text
    assert "'fcp-build-' + (Get-PathHash $RepoRoot)" in text
    assert "'--driver', 'docker-container'" in text
    assert "'--driver-opt', 'default-load=true'" in text
    assert "'compose', 'build'" in text
    assert "'--builder', $name" in text
    assert "'relay', 'flask', 'recorder'" in text
    assert "& docker compose build relay flask recorder" not in text


def test_windows_active_build_stops_and_proves_writer_at_pressure() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")
    build = text[text.index("function Invoke-ControlledCoreBuild") : text.index("function Assert-CoreImageCommits")]

    assert "$BuildPollMilliseconds = 250" in text
    assert "while (-not $process.HasExited)" in build
    assert "Get-FcpResourceFreeBytes -BackingPath $BackingPath" in build
    assert "$level -in @('pressure', 'critical')" in build
    assert build.index("Stop-BuildClient $process") < build.index(
        "Stop-FcpBuildWriter $name -DiscardCache"
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
    assert "Stop-FcpBuildWriter $name" in prune
    assert build.rindex("Stop-FcpBuildWriter $name") > build.index("Invoke-BuildCachePrune")


def test_windows_build_requires_exact_image_identity_before_success() -> None:
    text = _read("scripts/windows/fcp_host_build.ps1")

    assert "function Assert-CoreImageCommits" in text
    assert "'compose', 'images', '-q', $service" in text
    assert 'no.fcp.build_commit' in text
    assert "built_image_identity_unavailable" in text
    assert "built_image_identity_mismatch" in text
    assert text.index("Invoke-ControlledCoreBuild $backingPath") < text.index(
        "Assert-CoreImageCommits $commit"
    ) < text.index("Write-AtomicText $OutputFile $commit")


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

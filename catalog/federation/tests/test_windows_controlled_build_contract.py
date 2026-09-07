from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


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
    assert build.index("Stop-BuildClient $process") < build.index(
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

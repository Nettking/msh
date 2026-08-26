from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _function(text: str, name: str, next_name: str) -> str:
    start = text.index(f"function {name}")
    end = text.index(f"function {next_name}", start)
    return text[start:end]


def test_windows_docker_resource_helper_proves_host_backing_storage() -> None:
    helper = (ROOT / "scripts/windows/fcp_docker_resource.ps1").read_text(
        encoding="utf-8"
    )

    assert "Docker\\wsl\\disk\\docker_data.vhdx" in helper
    assert "Docker\\wsl\\data\\ext4.vhdx" in helper
    assert "{{.OSType}}|{{.DockerRootDir}}" in helper
    assert "ProgramData" not in helper
    assert "$script:FcpCriticalFreeBytes = [int64]10737418240" in helper
    assert "$script:FcpPressureFreeBytes = [int64]12884901888" in helper
    assert "$script:FcpWarningFreeBytes = [int64]17179869184" in helper
    assert "if ($FreeBytes -lt 0" in helper


def test_windows_build_update_and_model_paths_share_one_resource_helper() -> None:
    build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(encoding="utf-8")
    update = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )
    model = (ROOT / "scripts/windows/fcp_model_pull.ps1").read_text(encoding="utf-8")

    for text in (build, update, model):
        assert "fcp_docker_resource.ps1" in text
        assert "Get-FcpDockerBackingPath" in text
        assert "Get-FcpResourceFreeBytes" in text

    assert "function Get-DockerBackingPath" not in model
    assert "function Get-FreeBytes" not in model


def test_windows_build_preflights_never_use_checkout_free_space() -> None:
    build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(encoding="utf-8")
    update = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )

    build_preflight = _function(build, "Assert-DiskPreflight", "Write-AtomicText")
    update_preflight = _function(update, "Assert-DiskPreflight", "Invoke-Git")

    for preflight in (build_preflight, update_preflight):
        assert "Get-FcpDockerBackingPath" in preflight
        assert preflight.count("Get-FcpResourceFreeBytes") == 2
        assert "Get-FcpResourcePressureLevel" in preflight
        assert "Get-FreeBytes" not in preflight
        assert "Invoke-BuildCachePrune" in preflight
        assert "insufficient_disk_for_update" in preflight


def test_windows_update_refuses_resource_pressure_before_build_or_runtime_stop() -> None:
    update = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )

    assert (
        update.index("Assert-DiskPreflight")
        < update.index("'compose', 'build', 'relay', 'flask', 'recorder'")
        < update.index("'compose', 'stop', 'flask'")
    )

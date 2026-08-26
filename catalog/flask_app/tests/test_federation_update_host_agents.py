from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def test_windows_agent_has_fixed_safe_mutation_boundary() -> None:
    text = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )

    assert "$ApprovedRepository = 'Nettking/msh'" in text
    assert "$ApprovedBranch = 'main'" in text
    assert "merge', '--ff-only'" in text
    assert "'compose', 'build', 'relay', 'flask', 'recorder'" in text
    assert "FCP_BUILD_COMMIT" in text
    assert "runtime_verified" in text
    assert "Ensure-OllamaModel" in text
    assert "Start-ReplacementAgent" in text
    assert "Get-AgentHash" in text
    assert "Normalize-DirectoryPath" in text
    assert "Get-RequestResultFile" in text
    assert "dirty_build_context" in text
    assert "Invoke-ExternalResult" in text
    assert "$previousErrorActionPreference = $ErrorActionPreference" in text
    assert "$ErrorActionPreference = 'Continue'" in text
    assert "$ErrorActionPreference = $previousErrorActionPreference" in text
    assert "$exit = $LASTEXITCODE" in text
    assert "Preserve-RelayVolumeSelection" in text
    assert "Get-ServiceRelayVolume" in text
    assert "$env:FCP_RELAY_VOLUME_NAME" in text
    assert "$env:FCP_DATA_DIR = $DataDirectory" in text
    assert "$env:COMPOSE_PROJECT_NAME = 'fcp'" in text
    assert "'compose', 'stop', 'flask'" in text
    assert "'catalog.flask_app.services.existing_setup_resume'" in text
    assert "$resume.ExitCode -notin @(0, 4)" in text
    assert text.index("Preserve-RelayVolumeSelection | Out-Null") < text.index(
        "'compose', 'build', 'relay', 'flask', 'recorder'"
    )
    assert text.index("Move-Item -LiteralPath $RequestFile") < text.index(
        "[System.IO.File]::ReadAllText($processing)"
    )
    assert "& docker" not in text
    assert "reset --hard" not in text
    assert "git clean" not in text
    assert "git stash" not in text
    assert "Invoke-Expression" not in text
    assert "$request.command" not in text
    assert "$request.arguments" not in text
    assert "$request.remote" not in text
    assert "[string]$request.branch -ne $ApprovedBranch" in text


def test_posix_agent_never_executes_peer_supplied_process_shape() -> None:
    text = (ROOT / "scripts/posix/fcp_update_engine.py").read_text(encoding="utf-8")

    assert 'APPROVED_REPOSITORY = "Nettking/msh"' in text
    assert 'APPROVED_BRANCH = "main"' in text
    assert 'git(root, "merge", "--ff-only", target)' in text
    assert 'env["FCP_BUILD_COMMIT"] = target' in text
    assert 'state="runtime_verified"' in text
    assert "ensure_ollama_model(root, env)" in text
    assert "os.execv(" in text
    assert "initial_digest = _digest(script_path)" in text
    assert "_request_result_path" in text
    assert "dirty_build_context" in text
    assert text.index("os.replace(request_file, processing)") < text.index(
        'processing.read_text(encoding="utf-8")'
    )
    assert "shell=False" in text
    assert "reset --hard" not in text
    assert "git clean" not in text
    assert "git stash" not in text
    assert 'value.get("command")' not in text
    assert 'value.get("arguments")' not in text
    assert 'value.get("remote")' not in text
    assert 'value.get("branch") != APPROVED_BRANCH' in text


def test_public_agents_delegate_to_serialized_runners() -> None:
    windows = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(encoding="utf-8")
    windows_runner = (ROOT / "scripts/windows/fcp_update_agent_runner.ps1").read_text(
        encoding="utf-8"
    )
    posix_runner = (ROOT / "scripts/posix/fcp_update_agent_runner.py").read_text(
        encoding="utf-8"
    )
    windows_build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(
        encoding="utf-8"
    )
    posix_build = (ROOT / "catalog/federation/host_build.py").read_text(
        encoding="utf-8"
    )

    assert "fcp_update_agent_runner.ps1" in windows
    assert "fcp_update_agent_runner.py" in posix
    assert "fcp_update_engine.ps1" in windows_runner
    assert 'ENGINE_NAME = "fcp_update_engine.py"' in posix_runner
    assert "Global\\FCPHostMutation-" in windows_runner
    assert "Global\\FCPHostMutation-" in windows_build
    assert "host_build.host_mutation_lock(root)" in posix_runner
    assert "with host_mutation_lock(root, timeout_seconds=lock_timeout_seconds):" in posix_build
    assert "lease_already_held" in posix_build
    assert "host_mutation_lease_missing" in posix_build
    assert "Invoke-PostBuildCachePrune" in windows_runner
    assert "build_phase_entered and not prune_called" in posix_runner


def test_legacy_recorder_only_update_agents_are_retired() -> None:
    assert not (ROOT / "scripts/windows/fcp_recorder_update_agent.ps1").exists()
    assert not (ROOT / "scripts/posix/fcp_recorder_update_agent.py").exists()


def test_supported_launchers_use_serialized_host_build_before_agent() -> None:
    windows = (ROOT / "start.cmd").read_text(encoding="utf-8")
    posix = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert "fcp_host_build.ps1" in windows
    assert "FCP_BUILD_COMMIT" in windows
    assert "docker compose build relay flask recorder" not in windows
    assert windows.index("call :resolve_build_commit") < windows.index(
        "call :start_update_agent"
    )

    assert "catalog.federation.host_build" in posix
    assert "FCP_BUILD_COMMIT" in posix
    assert "docker compose build relay flask recorder" not in posix
    assert posix.index("build_core_images") < posix.index(
        'AGENT_DIR="$FCP_DATA_DIR/federation/update-agent"'
    )


def test_runtime_images_bake_build_identity() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    cli = (ROOT / "Dockerfile.cli").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for text in (dockerfile, cli):
        assert "ARG FCP_BUILD_COMMIT=unknown" in text
        assert "FCP_BUILD_COMMIT=${FCP_BUILD_COMMIT}" in text
        assert "no.fcp.build_commit=${FCP_BUILD_COMMIT}" in text
    assert compose.count("FCP_BUILD_COMMIT: ${FCP_BUILD_COMMIT:-unknown}") >= 3


# -- update disk lifecycle -----------------------------------------------


def test_the_build_commit_is_declared_below_the_dependency_install() -> None:
    for name in ("Dockerfile", "Dockerfile.cli"):
        text = (ROOT / name).read_text(encoding="utf-8")
        install = text.index("python -m pip install")
        declaration = text.index("ARG FCP_BUILD_COMMIT=unknown")
        assert declaration > install, (
            f"{name}: the build commit must be declared after the dependency "
            "install, or every update rebuilds and re-caches it"
        )


def test_update_engines_preflight_disk_before_building() -> None:
    windows = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_engine.py").read_text(
        encoding="utf-8"
    )

    assert "insufficient_disk_for_update" in windows
    assert "insufficient_disk_for_update" in posix
    assert (
        windows.index("Assert-DiskPreflight")
        < windows.index("'compose', 'build', 'relay', 'flask', 'recorder'")
        < windows.index("'compose', 'stop', 'flask'")
    )
    assert posix.index("preflight_disk(root, env)") < posix.index(
        '["docker", "compose", "build", "relay", "flask", "recorder"]'
    )


def test_launchers_and_update_engines_bound_build_cache() -> None:
    windows_engine = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )
    posix_engine = (ROOT / "scripts/posix/fcp_update_engine.py").read_text(
        encoding="utf-8"
    )
    windows_build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(
        encoding="utf-8"
    )
    posix_build = (ROOT / "catalog/federation/host_build.py").read_text(
        encoding="utf-8"
    )

    for text in (windows_engine, windows_build):
        assert "builder" in text and "prune" in text and "keep-storage" in text
    for text in (posix_engine, posix_build):
        assert '"builder",' in text and '"prune",' in text and "keep-storage" in text


def test_all_core_build_paths_agree_on_disk_figures() -> None:
    windows_engine = (ROOT / "scripts/windows/fcp_update_engine.ps1").read_text(
        encoding="utf-8"
    )
    posix_engine = (ROOT / "scripts/posix/fcp_update_engine.py").read_text(
        encoding="utf-8"
    )
    windows_build = (ROOT / "scripts/windows/fcp_host_build.ps1").read_text(
        encoding="utf-8"
    )
    posix_build = (ROOT / "catalog/federation/host_build.py").read_text(
        encoding="utf-8"
    )

    assert "UPDATE_REQUIRED_FREE_BYTES = 10 * 1024**3" in posix_engine
    assert "UPDATE_REQUIRED_FREE_BYTES = 10 * 1024**3" in posix_build
    assert "$UpdateRequiredFreeBytes = 10737418240" in windows_engine
    assert "$UpdateRequiredFreeBytes = 10737418240" in windows_build
    assert "BUILD_CACHE_KEEP_BYTES = 8 * 1024**3" in posix_engine
    assert "BUILD_CACHE_KEEP_BYTES = 8 * 1024**3" in posix_build
    assert "$BuildCacheKeepBytes = 8589934592" in windows_engine
    assert "$BuildCacheKeepBytes = 8589934592" in windows_build


_POSIX_AGENT_ONLY = pytest.mark.skipif(
    importlib.util.find_spec("fcntl") is None,
    reason="the POSIX update engine imports fcntl, which Windows does not provide",
)


def _load_posix_agent():
    spec = importlib.util.spec_from_file_location(
        "_fcp_update_engine_under_test",
        ROOT / "scripts/posix/fcp_update_engine.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@_POSIX_AGENT_ONLY
def test_preflight_passes_when_there_is_room(tmp_path, monkeypatch) -> None:
    agent = _load_posix_agent()
    monkeypatch.setattr(
        agent, "free_bytes", lambda _root: agent.UPDATE_REQUIRED_FREE_BYTES
    )
    pruned: list[bool] = []
    monkeypatch.setattr(
        agent, "prune_build_cache", lambda *_a: pruned.append(True) or True
    )

    agent.preflight_disk(tmp_path, {})

    assert pruned == [], "a host with room must not have its cache pruned"


@_POSIX_AGENT_ONLY
def test_preflight_recovers_from_its_own_build_cache(tmp_path, monkeypatch) -> None:
    agent = _load_posix_agent()
    readings = iter([0, agent.UPDATE_REQUIRED_FREE_BYTES])
    monkeypatch.setattr(agent, "free_bytes", lambda _root: next(readings))
    pruned: list[bool] = []
    monkeypatch.setattr(
        agent, "prune_build_cache", lambda *_a: pruned.append(True) or True
    )

    agent.preflight_disk(tmp_path, {})

    assert pruned == [True]


@_POSIX_AGENT_ONLY
def test_preflight_refuses_when_pruning_is_not_enough(tmp_path, monkeypatch) -> None:
    agent = _load_posix_agent()
    monkeypatch.setattr(agent, "free_bytes", lambda _root: 0)
    monkeypatch.setattr(agent, "prune_build_cache", lambda *_a: True)

    with pytest.raises(RuntimeError) as caught:
        agent.preflight_disk(tmp_path, {})

    assert str(caught.value) == "insufficient_disk_for_update"


@_POSIX_AGENT_ONLY
def test_a_failed_prune_never_becomes_an_update_failure(tmp_path, monkeypatch) -> None:
    agent = _load_posix_agent()

    def _explode(*_args, **_kwargs):
        raise OSError("docker unavailable")

    monkeypatch.setattr(agent.subprocess, "run", _explode)

    assert agent.prune_build_cache(tmp_path, {}) is False

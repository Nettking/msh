from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def test_windows_agent_has_fixed_safe_mutation_boundary() -> None:
    text = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
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
    # Repository/branch fields are read only to compare against local constants.
    assert "[string]$request.branch -ne $ApprovedBranch" in text


def test_posix_agent_never_executes_peer_supplied_process_shape() -> None:
    text = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(encoding="utf-8")

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


def test_recorder_only_host_agents_rebuild_only_the_recorder() -> None:
    windows = (ROOT / "scripts/windows/fcp_recorder_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_recorder_update_agent.py").read_text(
        encoding="utf-8"
    )

    assert "$ApprovedRepository = 'Nettking/msh'" in windows
    assert "$ApprovedBranch = 'main'" in windows
    assert "merge', '--ff-only'" in windows
    assert "'compose', 'build', 'recorder'" in windows
    assert "'--no-deps', '--force-recreate', 'recorder'" in windows
    assert "Wait-RecorderRuntime" in windows
    assert "mtconnect_recorder_status.json" in windows
    assert "FCP_BUILD_COMMIT" in windows
    assert "runtime_verified" in windows
    assert "Start-ReplacementAgent" in windows
    assert "Invoke-Expression" not in windows
    assert "reset --hard" not in windows
    assert "git clean" not in windows
    assert "git stash" not in windows
    assert "'compose', 'build', 'relay'" not in windows
    assert "Ensure-OllamaModel" not in windows

    assert 'APPROVED_REPOSITORY = "Nettking/msh"' in posix
    assert 'APPROVED_BRANCH = "main"' in posix
    assert 'git(root, env, "merge", "--ff-only", target)' in posix
    assert '["docker", "compose", "build", "recorder"]' in posix
    assert '"--no-deps",' in posix
    assert '"--force-recreate",' in posix
    assert '"recorder",' in posix
    assert "wait_recorder_runtime" in posix
    assert "mtconnect_recorder_status.json" in posix
    assert 'activation_env["FCP_BUILD_COMMIT"] = target' in posix
    assert 'state="runtime_verified"' in posix
    assert "os.execve(" in posix
    assert "shell=False" in posix
    assert "reset --hard" not in posix
    assert "git clean" not in posix
    assert "git stash" not in posix
    assert '"build", "relay"' not in posix
    assert "ensure_ollama_model" not in posix


def test_supported_launchers_start_agent_and_embed_build_commit() -> None:
    windows = (ROOT / "start.cmd").read_text(encoding="utf-8")
    posix = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert "fcp_update_agent.ps1" in windows
    assert "FCP_BUILD_COMMIT" in windows
    assert "docker compose build relay flask recorder" in windows
    assert "git status --porcelain=v1 --untracked-files=all" in windows
    assert "fcp_update_agent.py" in posix
    assert "FCP_BUILD_COMMIT" in posix
    assert "docker compose build relay flask recorder" in posix
    assert "git status --porcelain=v1 --untracked-files=all" in posix


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
#
# The physical host that filled its drive did it through repeated update
# builds: 21 GB of BuildKit cache in ~1 GB entries, while every FCP data path
# together was under 1% of the disk. These pin the three properties that stop
# it recurring.


def test_the_build_commit_is_declared_below_the_dependency_install() -> None:
    """This ordering is the whole fix; reversing it re-breaks the cache.

    A build argument invalidates the layer that consumes it and every layer
    after it. Declared above the install, each changed commit re-ran the whole
    dependency install and wrote roughly a gigabyte of fresh cache per image.
    """

    for name in ("Dockerfile", "Dockerfile.cli"):
        text = (ROOT / name).read_text(encoding="utf-8")
        install = text.index("python -m pip install")
        declaration = text.index("ARG FCP_BUILD_COMMIT=unknown")
        assert declaration > install, (
            f"{name}: the build commit must be declared after the dependency "
            "install, or every update rebuilds and re-caches it"
        )


def test_both_agents_preflight_disk_before_building() -> None:
    """Refusing after Flask is stopped is the state this must never reach."""

    windows = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(
        encoding="utf-8"
    )

    assert "insufficient_disk_for_update" in windows
    assert "insufficient_disk_for_update" in posix

    # The preflight must sit before the build, and the build before the stop.
    assert (
        windows.index("Assert-DiskPreflight")
        < windows.index("'compose', 'build', 'relay', 'flask', 'recorder'")
        < windows.index("'compose', 'stop', 'flask'")
    )
    assert posix.index("preflight_disk(root, env)") < posix.index(
        '["docker", "compose", "build", "relay", "flask", "recorder"]'
    )


def test_both_agents_bound_the_build_cache() -> None:
    windows = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(
        encoding="utf-8"
    )

    assert "'builder', 'prune', '--force'" in windows
    assert '"builder",' in posix and '"prune",' in posix
    assert "keep-storage" in windows
    assert "keep-storage" in posix


def test_the_two_agents_agree_on_the_disk_figures() -> None:
    """Two languages, one policy. A drift here is a silent inconsistency."""

    windows = (ROOT / "scripts/windows/fcp_update_agent.ps1").read_text(
        encoding="utf-8"
    )
    posix = (ROOT / "scripts/posix/fcp_update_agent.py").read_text(
        encoding="utf-8"
    )

    assert "UPDATE_REQUIRED_FREE_BYTES = 10 * 1024**3" in posix
    assert "$UpdateRequiredFreeBytes = 10737418240" in windows
    assert 10 * 1024**3 == 10737418240

    assert "BUILD_CACHE_KEEP_BYTES = 8 * 1024**3" in posix
    assert "$BuildCacheKeepBytes = 8589934592" in windows
    assert 8 * 1024**3 == 8589934592



def _load_posix_agent():
    """Import the standalone POSIX agent by path, as its launcher runs it."""

    spec = importlib.util.spec_from_file_location(
        "_fcp_update_agent_under_test",
        ROOT / "scripts/posix/fcp_update_agent.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


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


def test_preflight_recovers_from_its_own_build_cache(tmp_path, monkeypatch) -> None:
    """A cache past its bound is the usual reason the room went missing."""

    agent = _load_posix_agent()
    readings = iter([0, agent.UPDATE_REQUIRED_FREE_BYTES])
    monkeypatch.setattr(agent, "free_bytes", lambda _root: next(readings))
    pruned: list[bool] = []
    monkeypatch.setattr(
        agent, "prune_build_cache", lambda *_a: pruned.append(True) or True
    )

    agent.preflight_disk(tmp_path, {})

    assert pruned == [True]


def test_preflight_refuses_when_pruning_is_not_enough(tmp_path, monkeypatch) -> None:
    """The refusal must land before anything is stopped, not during."""

    agent = _load_posix_agent()
    monkeypatch.setattr(agent, "free_bytes", lambda _root: 0)
    monkeypatch.setattr(agent, "prune_build_cache", lambda *_a: True)

    with pytest.raises(RuntimeError) as caught:
        agent.preflight_disk(tmp_path, {})

    assert str(caught.value) == "insufficient_disk_for_update"


def test_a_failed_prune_never_becomes_an_update_failure(tmp_path, monkeypatch) -> None:
    """Cache is reconstructible; failing to prune it must not fail the update."""

    agent = _load_posix_agent()

    def _explode(*_args, **_kwargs):
        raise OSError("docker unavailable")

    monkeypatch.setattr(agent.subprocess, "run", _explode)

    assert agent.prune_build_cache(tmp_path, {}) is False

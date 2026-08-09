from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_windows_agent_has_fixed_safe_mutation_boundary() -> None:
    text = (ROOT / "scripts/windows/msh_update_agent.ps1").read_text(
        encoding="utf-8"
    )

    assert "$ApprovedRepository = 'Nettking/msh'" in text
    assert "$ApprovedBranch = 'main'" in text
    assert "merge', '--ff-only'" in text
    assert "docker' @('compose', 'build', 'relay', 'flask', 'recorder')" in text
    assert "MSH_BUILD_COMMIT" in text
    assert "runtime_verified" in text
    assert "reset --hard" not in text
    assert "git clean" not in text
    assert "git stash" not in text
    assert "Invoke-Expression" not in text
    assert "$request.command" not in text
    assert "$request.arguments" not in text
    assert "$request.remote" not in text
    assert "$request.branch" not in text


def test_posix_agent_never_executes_peer_supplied_process_shape() -> None:
    text = (ROOT / "scripts/posix/msh_update_agent.py").read_text(encoding="utf-8")

    assert 'APPROVED_REPOSITORY = "Nettking/msh"' in text
    assert 'APPROVED_BRANCH = "main"' in text
    assert 'git(root, "merge", "--ff-only", target)' in text
    assert 'env["MSH_BUILD_COMMIT"] = target' in text
    assert 'state="runtime_verified"' in text
    assert "shell=False" in text
    assert "reset --hard" not in text
    assert "git clean" not in text
    assert "git stash" not in text
    assert 'value.get("command")' not in text
    assert 'value.get("arguments")' not in text
    assert 'value.get("remote")' not in text


def test_supported_launchers_start_agent_and_embed_build_commit() -> None:
    windows = (ROOT / "start.cmd").read_text(encoding="utf-8")
    posix = (ROOT / "start.sh").read_text(encoding="utf-8")

    assert "msh_update_agent.ps1" in windows
    assert "MSH_BUILD_COMMIT" in windows
    assert "docker compose build relay flask recorder" in windows
    assert "msh_update_agent.py" in posix
    assert "MSH_BUILD_COMMIT" in posix
    assert "docker compose build relay flask recorder" in posix


def test_runtime_images_bake_build_identity() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    cli = (ROOT / "Dockerfile.cli").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    for text in (dockerfile, cli):
        assert "ARG MSH_BUILD_COMMIT=unknown" in text
        assert "MSH_BUILD_COMMIT=${MSH_BUILD_COMMIT}" in text
        assert "no.msh.build_commit=${MSH_BUILD_COMMIT}" in text
    assert compose.count("MSH_BUILD_COMMIT: ${MSH_BUILD_COMMIT:-unknown}") >= 3

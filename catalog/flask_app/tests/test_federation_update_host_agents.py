from __future__ import annotations

from pathlib import Path

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

from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _start_script() -> str:
    return (_repository_root() / "start.cmd").read_text(encoding="utf-8")


def _update_script() -> str:
    return (_repository_root() / "update.cmd").read_text(encoding="utf-8")


def _compose_file() -> str:
    return (_repository_root() / "docker-compose.yml").read_text(encoding="utf-8")


def _port_resolver_script() -> str:
    return (
        _repository_root()
        / "scripts"
        / "windows"
        / "resolve_msh_web_port.ps1"
    ).read_text(encoding="utf-8")


def test_start_cmd_builds_background_services_then_starts_web() -> None:
    script = _start_script()

    assert "docker compose build relay flask recorder" in script
    assert "docker compose up -d relay ollama recorder" in script
    assert "docker compose up -d flask" in script
    assert "call :ensure_ollama_model" in script
    assert "Ollama benchmark model is ready" in script
    assert "docker compose port flask 5000" in script
    assert 'set "MSH_BASE_URL=http://localhost:%MSH_WEB_PORT_RESOLVED%"' in script
    assert 'set "MSH_OPEN_URL=%MSH_BASE_URL%"' in script
    assert 'set "MSH_OPEN_URL=%MSH_ONBOARDING_URL%"' in script
    assert "-Uri '%MSH_BASE_URL%/onboarding'" in script
    assert 'start "" "%MSH_OPEN_URL%"' in script
    assert "%MSH_BASE_URL%/federation" in script
    assert "%MSH_BASE_URL%/status" in script
    assert "%MSH_BASE_URL%/docs" in script
    assert "docker compose ps relay ollama flask recorder" in script
    assert 'set "MSH_WEB_BIND=127.0.0.1"' in script
    assert 'set "COMPOSE_PROJECT_NAME=msh"' in script
    assert "Invoke-WebRequest" in script
    assert script.index("docker compose up -d relay ollama recorder") < script.index(
        "call :ensure_ollama_model"
    )
    assert script.index("call :ensure_ollama_model") < script.index(
        "docker compose up -d flask"
    )
    assert script.index("Invoke-WebRequest") < script.index(
        'start "" "%MSH_OPEN_URL%"'
    )
    assert "docker compose up --build" not in script


def test_start_cmd_recovers_runtime_state_before_compose_start() -> None:
    script = _start_script()
    resolver = _port_resolver_script()

    assert "call :resolve_runtime_state" in script
    assert "resolve_msh_web_port.ps1" in script
    assert 'set "MSH_WEB_PORT=5000"' in script
    assert 'set "MSH_WEB_PORT_EXPLICIT=0"' in script
    assert 'set "MSH_DATA_DIR_DEFAULTED=1"' in script
    assert 'set "MSH_RESULTS_DIR_DEFAULTED=1"' in script
    assert "-AllowFallback" in script
    assert '-OutputFile "%MSH_RUNTIME_FILE%"' in script
    assert '> "%MSH_RUNTIME_FILE%"' not in script
    assert 'if /I "%%A"=="MSH_RELAY_VOLUME_NAME"' in script
    assert 'if /I "%%A"=="MSH_DATA_DIR"' in script
    assert "Runtime-state resolver omitted MSH_WEB_PORT" in script
    assert "Resolver output was:" in script
    assert script.index("call :resolve_runtime_state") < script.index(
        "docker compose build relay flask recorder"
    )

    assert '[string]$CurrentProjectName = "msh"' in resolver
    assert '[string]$OutputFile = ""' in resolver
    assert "System.Text.UTF8Encoding($false)" in resolver
    assert "WriteAllLines" in resolver
    assert 'docker ps --filter "publish=$PreferredPort"' in resolver
    assert "Test-MshFlaskContainer" in resolver
    assert "Get-IdentityNodeId" in resolver
    assert "Get-RelayVolumeProbe" in resolver
    assert "session_memberships" in resolver
    assert "memberships -gt 0" in resolver
    assert "Recovered Federation coordinator volume" in resolver
    assert "MSH_RELAY_VOLUME_NAME=" in resolver
    assert "MSH_DATA_DIR=" in resolver
    assert "No Docker volume is deleted" in resolver
    assert "MSH will use http://localhost:$candidate" in resolver
    assert "docker volume rm" not in resolver
    assert "docker volume prune" not in resolver
    assert "Stop-Process" not in resolver
    assert "taskkill" not in resolver.casefold()


def test_compose_uses_selected_state_and_host_data_locations() -> None:
    compose = _compose_file()

    assert "source: ${MSH_DATA_DIR:-./data}" in compose
    assert "source: ${MSH_RESULTS_DIR:-./results}" in compose
    assert "name: ${MSH_RELAY_VOLUME_NAME:-msh_relay_state}" in compose
    assert "name: ${MSH_OLLAMA_VOLUME_NAME:-msh_ollama_models}" in compose
    assert (
        "name: ${MSH_MODEL_PROVIDER_VOLUME_NAME:-msh_model_provider_models}"
        in compose
    )


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parser check")
def test_windows_port_resolver_has_valid_powershell_syntax() -> None:
    path = (
        _repository_root()
        / "scripts"
        / "windows"
        / "resolve_msh_web_port.ps1"
    )
    escaped_path = str(path).replace("'", "''")
    command = (
        "$errors = $null; "
        "[System.Management.Automation.Language.Parser]::ParseFile("
        f"'{escaped_path}', [ref]$null, [ref]$errors) | Out-Null; "
        "if ($errors.Count -gt 0) { "
        "$errors | ForEach-Object { Write-Error $_.Message }; exit 1 }"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_start_cmd_verifies_the_exact_ollama_model_before_opening_browser() -> None:
    script = _start_script()

    assert ":ensure_ollama_model" in script
    assert "os.environ.get('MSH_AI_MODEL') or 'llama3.2:3b'" in script
    assert 'ollama show "%MSH_AI_MODEL_RESOLVED%"' in script
    assert (
        "docker compose --profile model-install run --rm --entrypoint "
        "/bin/ollama ollama-pull pull \"%MSH_AI_MODEL_RESOLVED%\""
        in script
    )
    assert "attempt %MSH_MODEL_ATTEMPT% of 3" in script
    assert "if %MSH_MODEL_ATTEMPT% GEQ 3" in script
    assert "Ollama does not contain the required model" in script
    assert "webapp will not be opened with a missing benchmark model" in script
    assert script.index("call :ensure_ollama_model") < script.index(
        'start "" "%MSH_OPEN_URL%"'
    )

    assert (
        "MSH will continue, but the language-model benchmark may be unavailable"
        not in script
    )


def test_start_cmd_resume_runs_before_long_running_flask_container() -> None:
    script = _start_script()

    assert 'if /I "%~1"=="--resume"' in script
    assert 'set "MSH_RESUME_EXISTING=1"' in script
    assert "call :run_existing_setup_resume" in script
    resume_command = (
        "docker compose run --rm --no-deps --entrypoint python flask -m "
        "catalog.flask_app.services.existing_setup_resume"
    )
    assert resume_command in script
    assert "docker compose stop flask" in script

    main_body = script.split("\n:resolve_runtime_state", maxsplit=1)[0]
    resume_block = script.split(
        "\n:run_existing_setup_resume",
        maxsplit=1,
    )[1].split(
        "\n:ensure_ollama_model",
        maxsplit=1,
    )[0]
    assert main_body.index("call :run_existing_setup_resume") < main_body.index(
        "docker compose up -d flask"
    )
    assert resume_block.index("docker compose stop flask") < resume_block.index(
        resume_command
    )

    assert (
        "docker compose exec -T flask python -m "
        "catalog.flask_app.services.existing_setup_resume"
        not in script
    )
    assert 'set "MSH_OPEN_URL=%MSH_BASE_URL%/federation"' in script
    assert 'set "MSH_OPEN_URL=%MSH_BASE_URL%/federation/benchmarks"' not in script
    assert 'set "MSH_OPEN_URL=%MSH_BASE_URL%/onboarding?repair=1"' in script
    assert "No identity or Federation was replaced" in script
    assert "start.cmd --resume" in script
    assert "reuse saved inspection and benchmark evidence without rerunning" in script
    assert "Resume mode never runs inspection or benchmarks" in script
    assert "run its benchmark plan" not in resume_block

    flask_start = script.index("docker compose up -d flask")
    assert flask_start < script.index("Waiting for the MSH webapp")
    assert script.index("Waiting for the MSH webapp") < script.index(
        'start "" "%MSH_OPEN_URL%"'
    )


def test_update_cmd_fast_forwards_then_resumes_without_resetting_state() -> None:
    script = _update_script()

    assert "git pull --ff-only" in script
    assert "chcp 65001 >nul" in script
    assert "call start.cmd --resume" in script
    assert script.index("git pull --ff-only") < script.index(
        "call start.cmd --resume"
    )
    assert "git reset --hard" not in script
    assert "git clean" not in script
    assert "start.cmd --fresh" not in script
    assert "docker compose down" not in script
    assert "docker compose down -v" not in script
    assert "Remove-Item" not in script
    assert "rmdir" not in script


def test_start_cmd_fresh_mode_resets_and_verifies_authoritative_state() -> None:
    script = _start_script()

    assert 'if /I "%~1"=="--fresh"' in script
    assert "Type RESET to continue" in script
    assert "docker compose down --remove-orphans" in script
    assert (
        "python flask -m catalog.flask_app.services.device_state_reset" in script
    )
    assert (
        "docker compose exec -T flask python -m "
        "catalog.flask_app.services.device_state_reset --verify-fresh"
        in script
    )
    assert "its authoritative setup state is not fresh" in script
    assert "?fresh=1&reset=%RANDOM%%RANDOM%" in script
    assert "start.cmd --fresh" in script

    assert "docker compose down -v" not in script
    assert "docker volume prune" not in script
    assert 'rmdir /s /q "data"' not in script
    assert "Remove-Item -LiteralPath 'data'" not in script
    assert "Remove-Item -LiteralPath 'results'" not in script
    assert "Remove-Item -LiteralPath 'data\\source_config'" not in script
    assert "Remove-Item -LiteralPath 'data\\source_state'" not in script


def test_fresh_launch_clears_only_msh_onboarding_browser_progress() -> None:
    script = (
        _repository_root()
        / "catalog"
        / "flask_app"
        / "static"
        / "js"
        / "onboarding.js"
    ).read_text(encoding="utf-8")

    assert 'url.searchParams.get("fresh") !== "1"' in script
    assert 'key.indexOf("msh.onboarding.") === 0' in script
    assert "window.localStorage.removeItem(key)" in script
    assert 'url.searchParams.delete("fresh")' in script
    assert 'url.searchParams.delete("reset")' in script
    assert "window.localStorage.clear()" not in script

from __future__ import annotations

from pathlib import Path


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _start_script() -> str:
    return (_repository_root() / "start.cmd").read_text(encoding="utf-8")


def test_start_cmd_runs_current_core_services_detached() -> None:
    script = _start_script()

    assert (
        "docker compose up -d --build relay ollama flask recorder" in script
    )
    assert "docker compose port flask 5000" in script
    assert 'set "MSH_BASE_URL=http://localhost:%MSH_WEB_PORT_RESOLVED%"' in script
    assert "-Uri '%MSH_BASE_URL%/onboarding'" in script
    assert 'start "" "%MSH_ONBOARDING_URL%"' in script
    assert "%MSH_BASE_URL%/federation" in script
    assert "%MSH_BASE_URL%/status" in script
    assert "%MSH_BASE_URL%/docs" in script
    assert "docker compose ps relay ollama flask recorder" in script
    assert 'set "MSH_WEB_BIND=127.0.0.1"' in script
    assert "Invoke-WebRequest" in script
    assert script.index("Invoke-WebRequest") < script.index(
        'start "" "%MSH_ONBOARDING_URL%"'
    )
    assert "docker compose up -d --build flask recorder" not in script
    assert "docker compose up --build" not in script


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

    # Fresh-device setup must not become a generic host-data or model wipe.
    assert "docker compose down -v" not in script
    assert "docker volume prune" not in script
    assert 'rmdir /s /q "data"' not in script
    assert "Remove-Item -LiteralPath 'data'" not in script
    assert "Remove-Item -LiteralPath 'results'" not in script
    assert "ollama_models" not in script
    assert "model_provider_models" not in script
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

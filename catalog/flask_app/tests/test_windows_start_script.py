from __future__ import annotations

from pathlib import Path


def _start_script() -> str:
    return (Path(__file__).resolve().parents[3] / "start.cmd").read_text(
        encoding="utf-8"
    )


def test_start_cmd_runs_current_core_services_detached() -> None:
    script = _start_script()

    assert (
        "docker compose up -d --build relay ollama flask recorder" in script
    )
    assert "-Uri 'http://localhost:5000/onboarding'" in script
    assert 'start "" "http://localhost:5000/onboarding"' in script
    assert "http://localhost:5000/federation" in script
    assert "http://localhost:5000/status" in script
    assert "http://localhost:5000/docs" in script
    assert "ws://localhost:8765" in script
    assert "docker compose ps relay ollama flask recorder" in script
    assert 'set "MSH_WEB_BIND=127.0.0.1"' in script
    assert "Invoke-WebRequest" in script
    assert script.index("Invoke-WebRequest") < script.index(
        'start "" "http://localhost:5000/onboarding"'
    )
    assert "docker compose up -d --build flask recorder" not in script
    assert "docker compose up --build" not in script


def test_start_cmd_fresh_mode_resets_only_device_and_federation_state() -> None:
    script = _start_script()

    assert 'if /I "%~1"=="--fresh"' in script
    assert "Type RESET to continue" in script
    assert "docker compose down --remove-orphans" in script
    assert "com.docker.compose.volume=relay_state" in script
    assert "Remove-Item -LiteralPath 'data\\federation'" in script
    assert (
        "Remove-Item -LiteralPath "
        "'data\\server_setup\\server_settings.json'" in script
    )
    assert "start.cmd --fresh" in script

    # Fresh-device setup must not become a generic data or volume wipe.
    assert "docker compose down -v" not in script
    assert "docker volume prune" not in script
    assert "Remove-Item -LiteralPath 'data'" not in script
    assert "ollama_models" not in script
    assert "model_provider_models" not in script
    assert "data\\source_config" not in script
    assert "data\\source_state" not in script
    assert "results" not in script.split(":reset_device_state", maxsplit=1)[1].split(
        ":show_help", maxsplit=1
    )[0].replace("analysis results", "")

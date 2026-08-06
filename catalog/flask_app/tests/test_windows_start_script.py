from __future__ import annotations

from pathlib import Path


def test_start_cmd_runs_current_core_services_detached() -> None:
    script = (Path(__file__).resolve().parents[3] / "start.cmd").read_text(
        encoding="utf-8"
    )

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

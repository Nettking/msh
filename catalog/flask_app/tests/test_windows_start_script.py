from __future__ import annotations

from pathlib import Path


def test_start_cmd_runs_app_and_managed_recorder_detached() -> None:
    script = (Path(__file__).resolve().parents[3] / "start.cmd").read_text(
        encoding="utf-8"
    )

    assert "docker compose up -d --build flask recorder" in script
    assert "-Uri 'http://localhost:5000/startup'" in script
    assert 'start "" "http://localhost:5000/startup"' in script
    assert "http://localhost:5000/status" in script
    assert "http://localhost:5000/docs" in script
    assert "docker compose ps flask recorder" in script
    assert 'set "MSH_WEB_BIND=127.0.0.1"' in script
    assert "Invoke-WebRequest" in script
    assert script.index("Invoke-WebRequest") < script.index(
        'start "" "http://localhost:5000/startup"'
    )
    assert "docker compose up --build flask recorder" not in script

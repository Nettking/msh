from __future__ import annotations

from pathlib import Path


def _script() -> str:
    return (Path(__file__).resolve().parents[3] / "start-tailscale.cmd").read_text(
        encoding="utf-8"
    )


def test_normal_tailscale_restart_releases_only_current_fcp_flask_port_owner() -> None:
    script = _script()

    project_filter = (
        'docker ps --filter "label=com.docker.compose.project=%COMPOSE_PROJECT_NAME%" '
        '--filter "label=com.docker.compose.service=flask"'
    )
    stop = "docker stop --time 20 %%C"
    start = 'call "%~dp0start.cmd"'

    assert project_filter in script
    assert stop in script
    assert script.index(project_filter) < script.index(stop) < script.index(start)

    # Restart may stop the verified current web container, but must not mutate
    # durable Federation state or terminate an arbitrary host process.
    assert "docker rm %%C" not in script
    assert "docker volume rm" not in script
    assert "docker compose down -v" not in script
    assert "taskkill" not in script.casefold()
    assert "Stop-Process" not in script

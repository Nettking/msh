"""Keep the container's discovery port aligned with the host responder."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from catalog.federation.tailnet_join_bridge import auto_join_port

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def compose_command() -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker Compose CLI is required; no daemon or containers are used")
    result = subprocess.run(
        [docker, "compose", "version"], capture_output=True, timeout=15, check=False
    )
    if result.returncode:
        pytest.skip("Docker Compose plugin is unavailable")
    return [
        docker,
        "compose",
        "--env-file",
        os.devnull,
        "--project-directory",
        str(ROOT),
        "-f",
        str(ROOT / "docker-compose.yml"),
        "config",
        "--format",
        "json",
    ]


@pytest.mark.parametrize("configured", [None, "", "5151", "5152", "65535"])
def test_advertised_join_port_matches_host_configuration(
    compose_command: list[str], configured: str | None
) -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("FCP_", "COMPOSE_"))
    }
    if configured is not None:
        env["FCP_AUTO_JOIN_PORT"] = configured
    result = subprocess.run(
        compose_command,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    flask_environment = json.loads(result.stdout)["services"]["flask"]["environment"]
    # This is the same resolver used by the public discovery advertisement.
    # Render real Compose interpolation so a host-only setting cannot pass.
    advertised_port = auto_join_port(flask_environment)
    host_port = auto_join_port(env)
    assert advertised_port == host_port

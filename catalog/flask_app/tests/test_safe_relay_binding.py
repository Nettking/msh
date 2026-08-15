from __future__ import annotations

from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (_root() / path).read_text(encoding="utf-8")


def test_relay_host_publish_is_loopback_by_default() -> None:
    compose = _read("docker-compose.yml")
    start_cmd = _read("start.cmd")
    start_sh = _read("start.sh")

    assert '${FCP_RELAY_BIND:-127.0.0.1}:${FCP_RELAY_PORT:-8765}:8765' in compose
    assert '${FCP_RELAY_BIND:-0.0.0.0}:${FCP_RELAY_PORT:-8765}:8765' not in compose
    # The relay still listens on all interfaces inside its container so Docker
    # can route the explicitly published host interface to it.
    assert "- --host\n      - 0.0.0.0" in compose
    assert 'if not defined FCP_RELAY_BIND set "FCP_RELAY_BIND=127.0.0.1"' in start_cmd
    assert ': "${FCP_RELAY_BIND:=127.0.0.1}"' in start_sh
    assert "export FCP_WEB_BIND FCP_RELAY_BIND FCP_WEB_PORT" in start_sh


def test_tailscale_launchers_bind_relay_only_to_tailscale_address_by_default() -> None:
    windows = _read("start-tailscale.cmd")
    posix = _read("start-tailscale.sh")

    assert 'if not defined FCP_RELAY_BIND set "FCP_RELAY_BIND=%FCP_TAILSCALE_IP%"' in windows
    assert ': "${FCP_RELAY_BIND:=$FCP_TAILSCALE_IP}"' in posix
    assert "export FCP_WEB_BIND FCP_RELAY_BIND FCP_DATA_DIR" in posix


def test_operator_configuration_documents_explicit_relay_exposure() -> None:
    env_example = _read(".env.example")
    quick_start = _read("docs/quick_start.md")
    operations = _read("docs/federation_operations.md")

    assert "FCP_RELAY_BIND=127.0.0.1" in env_example
    assert "FCP_RELAY_PORT=8765" in env_example
    assert "set FCP_RELAY_BIND=0.0.0.0" in quick_start
    assert "Setting only the web bind is not enough" in operations
    assert "Do not expose the Flask workbench, relay" in operations

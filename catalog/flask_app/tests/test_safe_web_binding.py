from __future__ import annotations

from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read(path: str) -> str:
    return (_root() / path).read_text(encoding="utf-8")


def test_web_host_publish_is_loopback_by_default() -> None:
    compose = _read("docker-compose.yml")
    env_example = _read(".env.example")
    setup = _read("setup_fcp.py")

    assert '${FCP_WEB_BIND:-127.0.0.1}:${FCP_WEB_PORT:-5000}:5000' in compose
    assert '${FCP_WEB_BIND:-0.0.0.0}:${FCP_WEB_PORT:-5000}:5000' not in compose
    assert "FCP_WEB_BIND=127.0.0.1" in env_example
    assert 'web_bind = "127.0.0.1"' in setup
    assert 'default="127.0.0.1"' in setup


def test_cross_device_web_exposure_remains_explicit() -> None:
    quick_start = _read("docs/quick_start.md")
    server_setup = _read("docs/server_setup.md")

    assert "set FCP_WEB_BIND=0.0.0.0" in quick_start
    assert "Direct Compose is also loopback-only by default" in quick_start
    assert "Direct Compose keeps both the Flask workbench and Federation relay on host loopback" in server_setup

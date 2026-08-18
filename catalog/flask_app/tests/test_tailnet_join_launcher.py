"""Contract tests for the supported zero-touch Tailscale launchers."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HOST_RUNNER = "scripts/federation_host_runner.py"
ZERO_TOUCH = "scripts/zero_touch_federation_start.py"


def _windows() -> str:
    return (ROOT / "start-tailscale.cmd").read_text(encoding="utf-8")


def _posix() -> str:
    return (ROOT / "start-tailscale.sh").read_text(encoding="utf-8")


def test_both_launchers_preflight_before_starting_the_responder() -> None:
    for script in (_windows(), _posix()):
        assert "tailnet_join_responder --check" in script
        assert "Automatic Federation joining is unavailable" in script
        assert "refusing zero-touch startup" in script


def test_both_launchers_start_the_responder_on_the_tailscale_address() -> None:
    windows = _windows()
    assert HOST_RUNNER.replace("/", "\\") in windows
    assert "tailnet_join_responder --bind %FCP_TAILSCALE_IP%" in windows
    assert "--port %FCP_AUTO_JOIN_PORT%" in windows

    posix = _posix()
    assert HOST_RUNNER in posix
    assert 'tailnet_join_responder \\\n  --bind "$FCP_TAILSCALE_IP"' in posix
    assert '--port "$FCP_AUTO_JOIN_PORT"' in posix


def test_fresh_reset_happens_before_discovery_and_enrollment() -> None:
    """A fresh reset deletes mutable data, so auto-join must be post-reset."""

    windows = _windows()
    posix = _posix()
    assert windows.index('call "%~dp0start.cmd"') < windows.index(
        "zero_touch_federation_start.py"
    )
    assert posix.index('sh "$ROOT/start.sh"') < posix.index(
        "zero_touch_federation_start.py"
    )
    for script in (windows, posix):
        assert "automatic federation joining runs on the next normal start" not in script.lower()


def test_first_federation_initialization_is_explicit_but_members_are_zero_touch() -> None:
    for script in (_windows(), _posix()):
        assert "--initialize-federation" in script
        assert "zero_touch_federation_start.py" in script
    # Human credentials belong only to first-Federation initialization; neither
    # launcher accepts email/password arguments for ordinary member enrollment.
    assert "--email" not in _windows()
    assert "--password" not in _windows()
    assert "--email" not in _posix()
    assert "--password" not in _posix()


def test_windows_opens_the_responder_port_in_the_firewall() -> None:
    windows = _windows()
    assert "netsh advfirewall firewall add rule" in windows
    assert "localport=%FCP_AUTO_JOIN_PORT%" in windows
    assert "elevated prompt" in windows


def test_host_helpers_do_not_import_the_server_federation_package_directly() -> None:
    runner = (ROOT / HOST_RUNNER).read_text(encoding="utf-8")
    assert '_HOST_PACKAGE = "_fcp_host_federation"' in runner
    assert "spec_from_file_location" in runner
    assert "import catalog.federation" not in runner


def test_full_tailscale_start_requests_creator_storage_authority() -> None:
    windows = _windows()
    posix = _posix()
    assert "FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED=1" in windows
    assert "FCP_FEDERATION_STORAGE_AUTHORITY_RELAY=ws://relay:8765" in windows
    assert "FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED:=1" in posix
    assert "FCP_FEDERATION_STORAGE_AUTHORITY_RELAY:=ws://relay:8765" in posix


def test_normal_start_delegates_idempotence_to_the_zero_touch_service() -> None:
    windows = _windows()
    posix = _posix()
    assert "automatic_capability_bootstrap" not in windows
    assert "automatic_capability_bootstrap" not in posix
    assert ZERO_TOUCH.replace("/", "\\") in windows
    assert ZERO_TOUCH in posix

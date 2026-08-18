"""Both supported launchers must actually start and use the host responder.

Automatic joining lives outside the application container, so nothing in the
normal test suite exercises it unless the launcher wiring itself is asserted.
A user must never have to start an undocumented second service by hand.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RESPONDER = "catalog/federation/tailnet_join_responder.py"
CLIENT = "catalog/federation/tailnet_join_client.py"


def _windows() -> str:
    return (ROOT / "start-tailscale.cmd").read_text(encoding="utf-8")


def _posix() -> str:
    return (ROOT / "start-tailscale.sh").read_text(encoding="utf-8")


def test_both_launchers_preflight_before_starting_the_responder() -> None:
    for script in (_windows(), _posix()):
        assert "--check" in script
        # The failure must be explicit rather than a later Federation timeout.
        assert "Automatic Federation joining is unavailable" in script
        assert "manual pairing codes still work" in script


def test_both_launchers_start_the_responder_on_the_tailscale_address() -> None:
    windows = _windows()
    assert RESPONDER.replace("/", "\\") in windows
    assert "--bind %FCP_TAILSCALE_IP%" in windows
    assert "--port %FCP_AUTO_JOIN_PORT%" in windows

    posix = _posix()
    assert RESPONDER in posix
    assert '--bind "$FCP_TAILSCALE_IP"' in posix
    assert '--port "$FCP_AUTO_JOIN_PORT"' in posix


def test_both_launchers_attempt_an_automatic_join_after_discovery() -> None:
    windows = _windows()
    posix = _posix()
    assert CLIENT.replace("/", "\\") in windows
    assert CLIENT in posix
    # The join attempt must read the snapshot discovery just produced.
    assert "--snapshot" in windows
    assert "--snapshot" in posix
    # Discovery must still run first.
    assert windows.index("tailscale_host_discovery.py") < windows.index(CLIENT.replace("/", "\\"))
    assert posix.index("tailscale_host_discovery.py") < posix.index(CLIENT)


def test_the_launcher_never_claims_the_responder_is_listening() -> None:
    """Only the responder knows whether its socket exists.

    The batch launcher cannot capture a pid from `start /b`, so its cleanup was
    dead code and it announced a listener that may have failed to bind. The
    responder now owns replacing the previous instance, its pid file, and the
    success line.
    """

    for script in (_windows(), _posix()):
        assert "Automatic Federation joining is listening" not in script
        assert "tailnet_join_responder.pid" not in script
        assert "taskkill /pid" not in script


def test_windows_opens_the_responder_port_in_the_firewall() -> None:
    """Docker publishes its own ports; a plain host listener gets no rule."""

    windows = _windows()
    assert "netsh advfirewall firewall add rule" in windows
    assert "localport=%FCP_AUTO_JOIN_PORT%" in windows
    # Elevation may be missing, so the fallback must be actionable, not silent.
    assert "elevated prompt" in windows


def test_the_responder_never_blocks_normal_startup() -> None:
    posix = _posix()
    # The join attempt must not abort the launcher under `set -e`.
    assert "tailnet_join_client.py" in posix
    assert "|| true" in posix
    # Startup still hands off to the normal launcher.
    assert posix.rstrip().endswith('exec sh "$ROOT/start.sh" "$@"')
    assert _windows().rstrip().endswith("exit /b %ERRORLEVEL%")


def test_a_fresh_launch_defers_the_join_instead_of_losing_the_grant() -> None:
    """--fresh wipes the data directory after these host steps run.

    A grant fetched before that reset would be deleted before it could ever be
    redeemed, so the join must be deferred to the next normal start rather than
    silently discarded.
    """

    for script in (_windows(), _posix()):
        assert "--fresh" in script
        assert "automatic federation joining runs on the next normal start" in script.lower()


def test_the_responder_is_started_even_on_a_fresh_launch() -> None:
    """The coordinator still answers joins after its own factory reset.

    The responder re-reads its secret per request, so the reset removing that
    file does not stop it serving; only the joining side has to wait.
    """

    windows = _windows()
    posix = _posix()
    # The responder start is not inside the fresh-mode branch.
    assert windows.index("--bind %FCP_TAILSCALE_IP%") < windows.index("FCP_FRESH_REQUESTED")
    assert posix.index('--bind "$FCP_TAILSCALE_IP"') < posix.index("FCP_FRESH_REQUESTED")

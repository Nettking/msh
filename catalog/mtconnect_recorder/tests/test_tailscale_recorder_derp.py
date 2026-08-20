from __future__ import annotations

import subprocess

import pytest

from scripts import start_tailscale_recorder as launcher


class _Connection:
    def close(self) -> None:
        pass


def test_relay_preflight_accepts_derp_pong_even_when_tailscale_ping_exits_one(
    monkeypatch,
) -> None:
    connected: list[tuple[tuple[str, int], float]] = []

    monkeypatch.setattr(launcher.shutil, "which", lambda _name: "tailscale")
    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda arguments, **_kwargs: subprocess.CompletedProcess(
            arguments,
            1,
            "pong from beast (100.85.20.75) via DERP(hel) in 34ms\n"
            "direct connection not established\n",
            "",
        ),
    )
    monkeypatch.setattr(
        launcher.socket,
        "create_connection",
        lambda address, timeout: (connected.append((address, timeout)) or _Connection()),
    )

    launcher._require_tailnet_relay("ws://100.85.20.75:8765")

    assert connected == [(("100.85.20.75", 8765), 5.0)]


def test_relay_preflight_still_rejects_unreachable_peer(monkeypatch) -> None:
    monkeypatch.setattr(launcher.shutil, "which", lambda _name: "tailscale")
    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda arguments, **_kwargs: subprocess.CompletedProcess(
            arguments,
            1,
            "",
            'ping "100.85.20.75" timed out\nno reply\n',
        ),
    )
    monkeypatch.setattr(
        launcher.socket,
        "create_connection",
        lambda *_args, **_kwargs: pytest.fail(
            "TCP relay check must not run when the peer was not reached"
        ),
    )

    with pytest.raises(RuntimeError, match="not a reachable peer"):
        launcher._require_tailnet_relay("ws://100.85.20.75:8765")

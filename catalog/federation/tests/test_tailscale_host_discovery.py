from __future__ import annotations

import json
import subprocess
from io import BytesIO
from types import SimpleNamespace

from catalog.federation.tailscale_host_discovery import (
    ADVERTISEMENT_SCHEMA,
    DISCOVERY_SCHEMA,
    discover,
)


def _status(*peers: dict[str, object]) -> str:
    return json.dumps({"Peer": {f"peer-{index}": peer for index, peer in enumerate(peers)}})


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))
        self.headers = {"Content-Type": "application/json; charset=utf-8"}

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)


def test_discovery_uses_existing_tailscale_login_without_credentials() -> None:
    calls: list[tuple[object, dict[str, object]]] = []

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_status(
                {
                    "Online": True,
                    "TailscaleIPs": ["100.64.0.10"],
                    "DNSName": "coordinator.example.ts.net.",
                    "HostName": "coordinator",
                }
            ),
            stderr="",
        )

    def opener(request, *, timeout):
        assert request.full_url == (
            "http://100.64.0.10:5000/onboarding/federation/discovery.json"
        )
        assert timeout == 0.75
        return _Response(
            {
                "schema": ADVERTISEMENT_SCHEMA,
                "federation_label": "Workshop Federation",
                "federation_fingerprint": "a" * 32,
                "device_name": "Coordinator PC",
                "relay_port": 8765,
                "pairing_required": True,
                "enrollment_token": "must-never-be-copied",
            }
        )

    result = discover(runner=runner, opener=opener)

    assert result["schema"] == DISCOVERY_SCHEMA
    assert result["tailscale_available"] is True
    assert result["federations"] == [
        {
            "federation_label": "Workshop Federation",
            "federation_fingerprint": "a" * 32,
            "device_name": "Coordinator PC",
            "relay_port": 8765,
            "pairing_required": True,
            "tailscale_ip": "100.64.0.10",
            "tailscale_dns_name": "coordinator.example.ts.net",
            "tailscale_host_name": "coordinator",
            "web_port": 5000,
        }
    ]
    command, kwargs = calls[0]
    assert command == ("tailscale", "status", "--json")
    assert "env" not in kwargs
    assert "auth" not in repr(calls).casefold()
    assert "api" not in repr(calls).casefold()
    assert "must-never-be-copied" not in json.dumps(result)


def test_offline_and_non_ipv4_peers_are_not_probed() -> None:
    probes: list[str] = []

    def runner(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_status(
                {"Online": False, "TailscaleIPs": ["100.64.0.11"]},
                {"Online": True, "TailscaleIPs": ["fd7a:115c:a1e0::1"]},
            ),
            stderr="",
        )

    def opener(request, **_kwargs):
        probes.append(request.full_url)
        raise AssertionError("offline/non-IPv4 peers must not be probed")

    result = discover(runner=runner, opener=opener)

    assert result["tailscale_available"] is True
    assert result["federations"] == []
    assert probes == []


def test_missing_tailscale_is_a_safe_empty_fallback() -> None:
    def runner(*_args, **_kwargs):
        raise FileNotFoundError("tailscale")

    result = discover(runner=runner)

    assert result == {
        "schema": DISCOVERY_SCHEMA,
        "tailscale_available": False,
        "federations": [],
    }


def test_malformed_tailscale_json_is_a_safe_empty_fallback() -> None:
    def runner(command, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="not-json", stderr="")

    result = discover(runner=runner)

    assert result["tailscale_available"] is False
    assert result["federations"] == []


def test_same_federation_advertised_by_multiple_peers_is_deduplicated() -> None:
    def runner(command, **_kwargs):
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_status(
                {"Online": True, "TailscaleIPs": ["100.64.0.20"]},
                {"Online": True, "TailscaleIPs": ["100.64.0.21"]},
            ),
            stderr="",
        )

    def opener(_request, **_kwargs):
        return _Response(
            {
                "schema": ADVERTISEMENT_SCHEMA,
                "federation_label": "One Federation",
                "federation_fingerprint": "b" * 32,
                "device_name": "Coordinator",
                "relay_port": 8765,
                "pairing_required": True,
            }
        )

    result = discover(runner=runner, opener=opener)

    assert len(result["federations"]) == 1

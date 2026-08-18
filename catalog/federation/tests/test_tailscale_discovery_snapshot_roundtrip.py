from __future__ import annotations

from catalog.federation.tailscale_host_discovery import (
    DEFAULT_AUTO_JOIN_PORT,
    DISCOVERY_SCHEMA,
    load_snapshot,
    write_snapshot,
)


def test_discovery_output_roundtrips_into_browser_snapshot(tmp_path) -> None:
    """Host discovery output intentionally omits the remote wire schema per item."""

    path = tmp_path / "tailscale_discovery.json"
    discovered = {
        "schema": DISCOVERY_SCHEMA,
        "tailscale_available": True,
        "federations": [
            {
                "federation_label": "Workshop Federation",
                "federation_fingerprint": "f" * 32,
                "device_name": "Coordinator PC",
                "relay_port": 8765,
                "pairing_required": True,
                "tailscale_ip": "100.90.80.70",
                "tailscale_dns_name": "coordinator.example.ts.net",
                "tailscale_host_name": "coordinator",
                "web_port": 5000,
            }
        ],
    }

    write_snapshot(path, discovered)
    loaded = load_snapshot(path)

    # A snapshot written before automatic joining existed still loads; the
    # responder port is filled in with its default rather than rejecting the
    # advertisement, so an older peer keeps being discoverable.
    expected = dict(discovered)
    expected["federations"] = [
        {**discovered["federations"][0], "auto_join_port": DEFAULT_AUTO_JOIN_PORT}
    ]
    assert loaded == expected

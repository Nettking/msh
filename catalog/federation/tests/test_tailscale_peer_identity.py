"""Auto-join must verify identity, never reachability.

Being able to reach this host over a tailnet says nothing about who is calling.
These tests pin every rejection path, because each one is a way an untrusted
device could otherwise become a fully trusted Federation member.
"""

from __future__ import annotations

import json
import subprocess

from catalog.federation.tailscale_peer_identity import (
    is_tailnet_address,
    local_tailnet_identity,
    verify_tailnet_peer,
)

LOCAL_ADDRESS = "100.90.80.70"
PEER_ADDRESS = "100.90.80.71"
TAILNET = "tail0abc.ts.net"


def _status(*, login: str = "owner@example.com", node: str = f"coordinator.{TAILNET}."):
    return {
        "Self": {
            "UserID": 4242,
            "DNSName": node,
            "TailscaleIPs": [LOCAL_ADDRESS],
        },
        "User": {"4242": {"LoginName": login}},
    }


def _whois(
    *,
    login: str = "owner@example.com",
    node: str = f"laptop.{TAILNET}.",
    sharer: object = None,
):
    payload = {
        "Node": {"Name": node},
        "UserProfile": {"LoginName": login},
    }
    if sharer is not None:
        payload["Node"]["Sharer"] = sharer
    return payload


def _runner(*, status=None, whois=None, whois_returncode: int = 0):
    def run(command, **_kwargs):
        if "status" in command:
            payload = _status() if status is None else status
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")
        if "whois" in command:
            payload = _whois() if whois is None else whois
            return subprocess.CompletedProcess(
                command,
                whois_returncode,
                json.dumps(payload),
                "",
            )
        raise AssertionError(f"unexpected command: {command}")

    return run


def test_same_tailnet_same_owner_peer_is_trusted() -> None:
    result = verify_tailnet_peer(PEER_ADDRESS, runner=_runner())

    assert result.trusted is True
    assert result.reason_code == "verified-same-tailnet-owner"
    assert result.peer is not None
    assert result.peer.login_name == "owner@example.com"
    assert result.peer.tailnet == TAILNET


def test_non_tailnet_address_is_rejected_without_asking_tailscale() -> None:
    def explode(command, **_kwargs):
        raise AssertionError("must not shell out for a non-tailnet address")

    for address in ("192.168.1.20", "10.0.0.5", "203.0.113.9", "", None, 100):
        result = verify_tailnet_peer(address, runner=explode)
        assert result.trusted is False
        assert result.reason_code == "not-a-tailnet-address"


def test_peer_owned_by_another_user_is_rejected() -> None:
    result = verify_tailnet_peer(
        PEER_ADDRESS,
        runner=_runner(whois=_whois(login="someone-else@example.com")),
    )

    assert result.trusted is False
    assert result.reason_code == "peer-owned-by-another-user"


def test_peer_in_another_tailnet_is_rejected() -> None:
    result = verify_tailnet_peer(
        PEER_ADDRESS,
        runner=_runner(whois=_whois(node="laptop.other9xyz.ts.net.")),
    )

    assert result.trusted is False
    assert result.reason_code == "peer-in-another-tailnet"


def test_node_shared_in_from_another_tailnet_is_rejected() -> None:
    # A shared node can be reachable and even appear to match; the sharer field
    # is what proves it belongs to someone else's tailnet.
    result = verify_tailnet_peer(
        PEER_ADDRESS,
        runner=_runner(whois=_whois(sharer=9931)),
    )

    assert result.trusted is False
    assert result.reason_code == "peer-shared-from-another-tailnet"


def test_tag_owned_node_is_rejected() -> None:
    result = verify_tailnet_peer(
        PEER_ADDRESS,
        runner=_runner(whois=_whois(login="tag:ci-runner")),
    )

    assert result.trusted is False
    assert result.reason_code == "peer-is-tag-owned"


def test_unresolvable_peer_fails_closed() -> None:
    result = verify_tailnet_peer(
        PEER_ADDRESS,
        runner=_runner(whois_returncode=1),
    )

    assert result.trusted is False
    assert result.reason_code == "peer-identity-unavailable"


def test_incomplete_whois_payload_fails_closed() -> None:
    for payload in ({}, {"Node": {}}, {"Node": {"Name": ""}, "UserProfile": {}}):
        result = verify_tailnet_peer(PEER_ADDRESS, runner=_runner(whois=payload))
        assert result.trusted is False
        assert result.reason_code in {
            "peer-identity-incomplete",
            "peer-in-another-tailnet",
        }


def test_missing_tailscale_cli_fails_closed() -> None:
    def missing(command, **_kwargs):
        raise FileNotFoundError("tailscale is not installed")

    result = verify_tailnet_peer(PEER_ADDRESS, runner=missing)

    assert result.trusted is False
    assert result.reason_code == "local-tailnet-identity-unavailable"


def test_tailscale_timeout_fails_closed() -> None:
    def slow(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, 2.0)

    result = verify_tailnet_peer(PEER_ADDRESS, runner=slow)

    assert result.trusted is False
    assert result.peer is None


def test_local_identity_reports_this_host_tailnet_and_owner() -> None:
    local = local_tailnet_identity(runner=_runner())

    assert local is not None
    assert local.login_name == "owner@example.com"
    assert local.tailnet == TAILNET
    assert local.node_name == f"coordinator.{TAILNET}"


def test_tailnet_address_range_covers_ipv4_and_ipv6() -> None:
    assert is_tailnet_address("100.64.0.1") is True
    assert is_tailnet_address("100.127.255.254") is True
    assert is_tailnet_address("fd7a:115c:a1e0::1") is True
    assert is_tailnet_address("[fd7a:115c:a1e0::1]") is True
    assert is_tailnet_address("100.128.0.1") is False
    assert is_tailnet_address("fd00::1") is False

"""Verify who a Tailscale peer actually is before granting Federation trust.

Reachability is not identity. A device can reach this host over a tailnet while
belonging to another tailnet owner, being shared in from a foreign tailnet, or
being a tag-owned server with no human owner at all. Auto-join therefore never
trusts the source address on its own: it asks the local ``tailscale`` CLI who
owns the connection, and accepts only a peer that

* resolves to a real tailnet node through ``tailscale whois``,
* belongs to this same tailnet,
* is owned by the same human login as this host,
* was not shared in from another tailnet, and
* is not tag-owned (a tagged node has no human owner to match).

Like :mod:`tailscale_host_discovery`, this module uses only the local CLI. It
never accepts, reads, stores, or forwards a Tailscale auth or API key.

It must therefore run **host-side**, in the same place discovery already runs.
The web container has no ``tailscale`` CLI and, because its port is published
through Docker, it does not observe the peer's tailnet address either -- both
checks below would fail closed inside the container. Automatic joining is not
wired to this module yet for exactly that reason: it needs a host-side responder
that can see the connecting peer, mint a pairing grant through the existing
authority, and hand it back over the tailnet.
"""

from __future__ import annotations

import ipaddress
import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass

WHOIS_TIMEOUT_SECONDS = 2.0
_TAILSCALE_IPV4_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")
_TAILSCALE_IPV6_NETWORK = ipaddress.IPv6Network("fd7a:115c:a1e0::/48")
_TAGGED_OWNER_PREFIX = "tag:"

Runner = Callable[..., "subprocess.CompletedProcess[str]"]


@dataclass(frozen=True)
class TailnetPeer:
    """One verified tailnet peer identity."""

    login_name: str
    tailnet: str
    node_name: str
    address: str


@dataclass(frozen=True)
class PeerVerification:
    """The outcome of one auto-join identity check."""

    peer: TailnetPeer | None
    reason_code: str

    @property
    def trusted(self) -> bool:
        return self.peer is not None


def is_tailnet_address(value: object) -> bool:
    """Return whether an address belongs to the Tailscale range."""

    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate:
        return False
    if candidate.startswith("[") and candidate.endswith("]"):
        candidate = candidate[1:-1]
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv4Address):
        return address in _TAILSCALE_IPV4_NETWORK
    return address in _TAILSCALE_IPV6_NETWORK


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _tailnet_of(node_name: str) -> str:
    """Return the tailnet portion of a MagicDNS name.

    ``machine.tail0abc.ts.net.`` belongs to tailnet ``tail0abc.ts.net``.
    """

    trimmed = node_name.strip().rstrip(".")
    _, _, domain = trimmed.partition(".")
    return domain.casefold()


def _run_whois(
    address: str,
    *,
    runner: Runner = subprocess.run,
) -> dict[str, object] | None:
    try:
        completed = runner(
            ("tailscale", "whois", "--json", address),
            capture_output=True,
            text=True,
            check=False,
            timeout=WHOIS_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _run_status(
    *,
    runner: Runner = subprocess.run,
) -> dict[str, object] | None:
    try:
        completed = runner(
            ("tailscale", "status", "--json"),
            capture_output=True,
            text=True,
            check=False,
            timeout=WHOIS_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def local_tailnet_identity(
    *,
    runner: Runner = subprocess.run,
) -> TailnetPeer | None:
    """Return this host's own tailnet identity, or ``None`` when unavailable."""

    payload = _run_status(runner=runner)
    if payload is None:
        return None
    profile = payload.get("User")
    self_node = payload.get("Self")
    if not isinstance(self_node, dict):
        return None
    login_name = ""
    user_id = self_node.get("UserID")
    if isinstance(profile, dict) and user_id is not None:
        entry = profile.get(str(user_id))
        if isinstance(entry, dict):
            login_name = _text(entry.get("LoginName"))
    node_name = _text(self_node.get("DNSName"))
    if not login_name or not node_name:
        return None
    addresses = self_node.get("TailscaleIPs")
    address = ""
    if isinstance(addresses, list) and addresses:
        address = _text(addresses[0])
    return TailnetPeer(
        login_name=login_name.casefold(),
        tailnet=_tailnet_of(node_name),
        node_name=node_name.rstrip("."),
        address=address,
    )


def verify_tailnet_peer(
    address: object,
    *,
    runner: Runner = subprocess.run,
) -> PeerVerification:
    """Verify that ``address`` is a same-tailnet, same-owner peer of this host.

    The returned reason code is safe to log and to expose as an error code; it
    never carries the peer's identity.
    """

    if not is_tailnet_address(address):
        return PeerVerification(None, "not-a-tailnet-address")
    remote = str(address).strip().strip("[]")

    local = local_tailnet_identity(runner=runner)
    if local is None:
        return PeerVerification(None, "local-tailnet-identity-unavailable")

    payload = _run_whois(remote, runner=runner)
    if payload is None:
        return PeerVerification(None, "peer-identity-unavailable")

    node = payload.get("Node")
    user = payload.get("UserProfile")
    if not isinstance(node, dict) or not isinstance(user, dict):
        return PeerVerification(None, "peer-identity-incomplete")

    # A node shared in from another tailnet carries a sharer; it is reachable
    # here but is not ours, so it can never auto-join.
    if node.get("Sharer"):
        return PeerVerification(None, "peer-shared-from-another-tailnet")

    node_name = _text(node.get("Name"))
    if not node_name:
        return PeerVerification(None, "peer-identity-incomplete")
    if _tailnet_of(node_name) != local.tailnet:
        return PeerVerification(None, "peer-in-another-tailnet")

    login_name = _text(user.get("LoginName")).casefold()
    if not login_name:
        return PeerVerification(None, "peer-identity-incomplete")
    # Tag-owned nodes have no human owner to compare against this host's owner.
    if login_name.startswith(_TAGGED_OWNER_PREFIX):
        return PeerVerification(None, "peer-is-tag-owned")
    if login_name != local.login_name:
        return PeerVerification(None, "peer-owned-by-another-user")

    return PeerVerification(
        TailnetPeer(
            login_name=login_name,
            tailnet=_tailnet_of(node_name),
            node_name=node_name.rstrip("."),
            address=remote,
        ),
        "verified-same-tailnet-owner",
    )


__all__ = [
    "PeerVerification",
    "TailnetPeer",
    "is_tailnet_address",
    "local_tailnet_identity",
    "verify_tailnet_peer",
]

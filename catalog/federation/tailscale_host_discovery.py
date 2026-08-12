"""Best-effort host-side Federation discovery through an existing Tailscale client.

This module intentionally uses only the local ``tailscale`` CLI.  It never accepts,
reads, stores, or forwards a Tailscale auth/API key.  Tailnet membership is only a
reachability/discovery signal: the returned descriptors contain no enrollment or
invitation material and therefore grant no Federation authority.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DISCOVERY_SCHEMA = "fcp.federation.tailscale-discovery.v1"
ADVERTISEMENT_SCHEMA = "fcp.federation.discovery-advertisement.v1"
DEFAULT_WEB_PORT = 5000
DEFAULT_TIMEOUT_SECONDS = 0.75
MAX_PEERS = 32
MAX_RESPONSE_BYTES = 16_384
_FINGERPRINT = re.compile(r"^[0-9a-f]{32}$")


def _safe_ipv4(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parts = value.strip().split(".")
    if len(parts) != 4:
        return None
    try:
        numbers = tuple(int(part, 10) for part in parts)
    except ValueError:
        return None
    if any(number < 0 or number > 255 for number in numbers):
        return None
    return ".".join(str(number) for number in numbers)


def _online_peers(payload: object) -> tuple[dict[str, str], ...]:
    if not isinstance(payload, dict):
        return ()
    raw_peers = payload.get("Peer")
    if not isinstance(raw_peers, dict):
        return ()
    peers: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in raw_peers.values():
        if not isinstance(raw, dict) or raw.get("Online") is not True:
            continue
        ips = raw.get("TailscaleIPs")
        if not isinstance(ips, list):
            continue
        address = next((_safe_ipv4(value) for value in ips if _safe_ipv4(value)), None)
        if address is None or address in seen:
            continue
        seen.add(address)
        dns_name = raw.get("DNSName")
        host_name = raw.get("HostName")
        peers.append(
            {
                "address": address,
                "dns_name": (
                    dns_name.rstrip(".")
                    if isinstance(dns_name, str) and dns_name.strip()
                    else ""
                ),
                "host_name": host_name if isinstance(host_name, str) else "",
            }
        )
        if len(peers) >= MAX_PEERS:
            break
    return tuple(peers)


def _run_tailscale_status(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[dict[str, str], ...] | None:
    try:
        completed = runner(
            ("tailscale", "status", "--json"),
            capture_output=True,
            text=True,
            check=False,
            timeout=2.0,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError):
        return None
    return _online_peers(payload)


def _validate_advertisement(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict) or payload.get("schema") != ADVERTISEMENT_SCHEMA:
        return None
    label = payload.get("federation_label")
    fingerprint = payload.get("federation_fingerprint")
    device_name = payload.get("device_name")
    relay_port = payload.get("relay_port")
    if (
        not isinstance(label, str)
        or not label.strip()
        or len(label.encode("utf-8")) > 256
        or not isinstance(fingerprint, str)
        or _FINGERPRINT.fullmatch(fingerprint) is None
        or not isinstance(device_name, str)
        or len(device_name.encode("utf-8")) > 256
        or isinstance(relay_port, bool)
        or not isinstance(relay_port, int)
        or not 1 <= relay_port <= 65_535
        or payload.get("pairing_required") is not True
    ):
        return None
    # Deliberately copy only the public-safe allowlist. Unknown fields from a
    # remote peer never enter the persisted snapshot.
    return {
        "federation_label": label.strip(),
        "federation_fingerprint": fingerprint,
        "device_name": device_name.strip(),
        "relay_port": relay_port,
        "pairing_required": True,
    }


def _probe_peer(
    peer: dict[str, str],
    *,
    web_ports: Iterable[int],
    timeout_seconds: float,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, object] | None:
    address = peer["address"]
    for port in web_ports:
        url = f"http://{address}:{port}/onboarding/federation/discovery.json"
        request = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "FCP-Tailscale-Discovery/1"},
            method="GET",
        )
        try:
            with opener(request, timeout=timeout_seconds) as response:
                content_type = str(response.headers.get("Content-Type", ""))
                if "application/json" not in content_type.casefold():
                    continue
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError):
            continue
        if len(raw) > MAX_RESPONSE_BYTES:
            continue
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        advertisement = _validate_advertisement(payload)
        if advertisement is None:
            continue
        advertisement.update(
            {
                "tailscale_ip": address,
                "tailscale_dns_name": peer.get("dns_name", ""),
                "tailscale_host_name": peer.get("host_name", ""),
                "web_port": port,
            }
        )
        return advertisement
    return None


def discover(
    *,
    web_ports: Iterable[int] = (DEFAULT_WEB_PORT,),
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, object]:
    """Return a bounded, public-safe snapshot of FCP Federations in the tailnet."""

    ports = tuple(
        port
        for port in web_ports
        if isinstance(port, int) and not isinstance(port, bool) and 1 <= port <= 65_535
    )
    if not ports:
        ports = (DEFAULT_WEB_PORT,)
    peers = _run_tailscale_status(runner=runner)
    if peers is None:
        return {
            "schema": DISCOVERY_SCHEMA,
            "tailscale_available": False,
            "federations": [],
        }

    federations: list[dict[str, object]] = []
    seen_fingerprints: set[str] = set()
    for peer in peers:
        advertisement = _probe_peer(
            peer,
            web_ports=ports,
            timeout_seconds=timeout_seconds,
            opener=opener,
        )
        if advertisement is None:
            continue
        fingerprint = str(advertisement["federation_fingerprint"])
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        federations.append(advertisement)

    federations.sort(
        key=lambda item: (
            str(item.get("federation_label", "")).casefold(),
            str(item.get("federation_fingerprint", "")),
        )
    )
    return {
        "schema": DISCOVERY_SCHEMA,
        "tailscale_available": True,
        "federations": federations,
    }


def write_snapshot(path: Path | str, payload: dict[str, object]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        dir=target.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            descriptor = -1
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        try:
            target.chmod(0o600)
        except OSError:
            pass
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover FCP Federations through the already logged-in Tailscale client.",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--web-port", type=int, action="append", dest="web_ports")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    timeout = args.timeout
    if not isinstance(timeout, float) or not 0.05 <= timeout <= 5.0:
        raise SystemExit("--timeout must be between 0.05 and 5 seconds")
    payload = discover(
        web_ports=tuple(args.web_ports or (DEFAULT_WEB_PORT,)),
        timeout_seconds=timeout,
    )
    write_snapshot(args.output, payload)
    count = len(payload["federations"])
    if payload["tailscale_available"]:
        print(f"Tailscale discovery found {count} FCP Federation(s).")
    else:
        print("Tailscale discovery unavailable; normal onboarding remains available.")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through launcher integration
    raise SystemExit(main())

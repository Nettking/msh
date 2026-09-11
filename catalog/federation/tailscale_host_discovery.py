"""Best-effort host-side Federation discovery through an existing Tailscale client.

This module intentionally uses only the local ``tailscale`` CLI. It never accepts,
reads, stores, or forwards a Tailscale auth/API key. Tailnet membership is only a
reachability/discovery signal: the returned descriptors contain no enrollment or
invitation material and therefore grant no Federation authority.
"""

from __future__ import annotations

import argparse
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request

DISCOVERY_SCHEMA = "fcp.federation.tailscale-discovery.v1"
ADVERTISEMENT_SCHEMA = "fcp.federation.discovery-advertisement.v1"
DEFAULT_WEB_PORT = 5000
DEFAULT_AUTO_JOIN_PORT = 5151
DEFAULT_TIMEOUT_SECONDS = 2.0
MAX_PEERS = 32
MAX_WEB_PORTS = 4
MAX_PROBE_WORKERS = 8
MAX_DISCOVERY_SECONDS = 10.0
MAX_RESPONSE_BYTES = 16_384
MAX_SNAPSHOT_BYTES = 256 * 1024
_FINGERPRINT = re.compile(r"^[0-9a-f]{32}$")
_TAILSCALE_IPV4_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")


class DiscoveryBudgetExceeded(RuntimeError):
    """An incomplete scan must not be mistaken for a unique/absent Federation."""


@contextmanager
def _open_probe(request: Request, *, timeout: float):
    """Bound the whole numeric-host HTTP exchange, including trickled replies.

    Socket inactivity timeouts alone restart as bytes arrive. The timer closes
    this one socket at its absolute deadline. No proxy, DNS or redirect can send
    a discovery probe beyond the already selected peer and port.
    """
    parsed = urlsplit(request.full_url)
    ipaddress.IPv4Address(parsed.hostname or "")
    if parsed.scheme != "http":
        raise ValueError("discovery requires an IPv4 HTTP endpoint")
    deadline = time.monotonic() + timeout
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    timer = None
    response = None
    try:
        connection.connect()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("discovery HTTP budget exhausted")
        peer_socket = connection.sock

        def interrupt():
            try:
                peer_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        timer = threading.Timer(remaining, interrupt)
        timer.daemon = True
        timer.start()
        connection.request("GET", parsed.path, headers=dict(request.header_items()))
        response = connection.getresponse()
        yield response
    finally:
        if timer is not None:
            timer.cancel()
            timer.join()
        if response is not None:
            response.close()
        connection.close()


def _empty_snapshot(*, tailscale_available: bool = False) -> dict[str, object]:
    return {
        "schema": DISCOVERY_SCHEMA,
        "tailscale_available": tailscale_available,
        "federations": [],
    }


def _safe_tailscale_ipv4(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        address = ipaddress.IPv4Address(value.strip())
    except ipaddress.AddressValueError:
        return None
    if address not in _TAILSCALE_IPV4_NETWORK:
        return None
    return str(address)


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
        address = None
        for value in ips:
            candidate = _safe_tailscale_ipv4(value)
            if candidate is not None:
                address = candidate
                break
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
    # The responder port is public-safe routing information, like relay_port:
    # it says where to ask, never who may join. A malformed or absent value
    # falls back to the default rather than rejecting the whole advertisement,
    # so a peer running an older build still discovers normally.
    raw_auto_join_port = payload.get("auto_join_port")
    auto_join_port = DEFAULT_AUTO_JOIN_PORT
    if (
        not isinstance(raw_auto_join_port, bool)
        and isinstance(raw_auto_join_port, int)
        and 1 <= raw_auto_join_port <= 65_535
    ):
        auto_join_port = raw_auto_join_port
    # Deliberately copy only the public-safe allowlist. Unknown fields from a
    # remote peer never enter the persisted snapshot.
    return {
        "federation_label": label.strip(),
        "federation_fingerprint": fingerprint,
        "device_name": device_name.strip(),
        "relay_port": relay_port,
        "pairing_required": True,
        "auto_join_port": auto_join_port,
    }


def _validated_snapshot_federation(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    raw_schema = payload.get("schema")
    if raw_schema not in (None, ADVERTISEMENT_SCHEMA):
        return None
    normalized = dict(payload)
    normalized["schema"] = ADVERTISEMENT_SCHEMA
    advertisement = _validate_advertisement(normalized)
    if advertisement is None:
        return None
    tailscale_ip = _safe_tailscale_ipv4(payload.get("tailscale_ip"))
    web_port = payload.get("web_port")
    if (
        tailscale_ip is None
        or isinstance(web_port, bool)
        or not isinstance(web_port, int)
        or not 1 <= web_port <= 65_535
    ):
        return None
    dns_name = payload.get("tailscale_dns_name")
    host_name = payload.get("tailscale_host_name")
    if not isinstance(dns_name, str) or len(dns_name.encode("utf-8")) > 256:
        dns_name = ""
    if not isinstance(host_name, str) or len(host_name.encode("utf-8")) > 256:
        host_name = ""
    advertisement.update(
        {
            "tailscale_ip": tailscale_ip,
            "tailscale_dns_name": dns_name.strip().rstrip("."),
            "tailscale_host_name": host_name.strip(),
            "web_port": web_port,
        }
    )
    return advertisement


def _probe_peer(
    peer: dict[str, str],
    *,
    web_ports: Iterable[int],
    timeout_seconds: float,
    opener: Callable[..., Any] = _open_probe,
    deadline: float | None = None,
) -> dict[str, object] | None:
    address = peer["address"]
    for port in web_ports:
        remaining = timeout_seconds if deadline is None else deadline - time.monotonic()
        if remaining <= 0:
            raise DiscoveryBudgetExceeded("Tailscale discovery scan budget exhausted")
        url = f"http://{address}:{port}/onboarding/federation/discovery.json"
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "FCP-Tailscale-Discovery/1",
            },
            method="GET",
        )
        try:
            with opener(request, timeout=min(timeout_seconds, remaining)) as response:
                if getattr(response, "status", 200) != 200:
                    continue
                content_type = str(response.headers.get("Content-Type", ""))
                if "application/json" not in content_type.casefold():
                    continue
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except (
            HTTPError, URLError, TimeoutError, OSError, ValueError,
            http.client.HTTPException,
        ):
            continue
        if deadline is not None and time.monotonic() >= deadline:
            raise DiscoveryBudgetExceeded("Tailscale discovery scan budget exhausted")
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
    total_timeout_seconds: float = MAX_DISCOVERY_SECONDS,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    opener: Callable[..., Any] = _open_probe,
) -> dict[str, object]:
    """Return a bounded, public-safe snapshot of FCP Federations in the tailnet."""

    for name, value, maximum in (
        ("timeout_seconds", timeout_seconds, 5.0),
        ("total_timeout_seconds", total_timeout_seconds, MAX_DISCOVERY_SECONDS),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0.05 <= value <= maximum
        ):
            raise ValueError(f"{name} must be between 0.05 and {maximum} seconds")
    configured_ports = tuple(islice(web_ports, MAX_WEB_PORTS + 1))
    if len(configured_ports) > MAX_WEB_PORTS:
        raise ValueError(f"discovery accepts at most {MAX_WEB_PORTS} web ports")
    ports = tuple(
        dict.fromkeys(
            port
            for port in configured_ports
            if isinstance(port, int) and not isinstance(port, bool)
            and 1 <= port <= 65_535
        )
    )
    if not ports:
        ports = (DEFAULT_WEB_PORT,)
    peers = _run_tailscale_status(runner=runner)
    if peers is None:
        return _empty_snapshot()

    deadline = time.monotonic() + total_timeout_seconds
    pool = ThreadPoolExecutor(max_workers=MAX_PROBE_WORKERS, thread_name_prefix="fcp-discovery")
    futures = []
    advertisements = {}
    try:
        futures = [
            pool.submit(
                _probe_peer, peer, web_ports=ports, timeout_seconds=timeout_seconds,
                opener=opener, deadline=deadline,
            )
            for peer in peers
        ]
        for future in as_completed(futures, timeout=max(0.0, deadline - time.monotonic())):
            advertisements[future] = future.result()
        if time.monotonic() >= deadline:
            raise DiscoveryBudgetExceeded("Tailscale discovery scan budget exhausted")
    except TimeoutError as exc:
        raise DiscoveryBudgetExceeded("Tailscale discovery scan budget exhausted") from exc
    finally:
        # Running probes share the deadline and close their own sockets. Queued
        # probes are cancelled; none may outlive this invocation's cleanup.
        pool.shutdown(wait=True, cancel_futures=True)

    federations: list[dict[str, object]] = []
    seen_fingerprints: set[str] = set()
    for future in futures:
        advertisement = advertisements[future]
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


def load_snapshot(path: Path | str) -> dict[str, object]:
    """Load a persisted host snapshot through the same bounded allowlist.

    The bind-mounted data directory is treated as untrusted input. Malformed,
    oversized, non-Tailscale, or unexpectedly shaped content collapses to an
    empty unavailable snapshot rather than reaching the browser view model.
    """

    target = Path(path)
    try:
        size = target.stat().st_size
        if size < 0 or size > MAX_SNAPSHOT_BYTES:
            return _empty_snapshot()
        raw = target.read_bytes()
    except OSError:
        return _empty_snapshot()
    if len(raw) > MAX_SNAPSHOT_BYTES:
        return _empty_snapshot()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _empty_snapshot()
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != DISCOVERY_SCHEMA
        or not isinstance(payload.get("tailscale_available"), bool)
        or not isinstance(payload.get("federations"), list)
    ):
        return _empty_snapshot()

    tailscale_available = payload["tailscale_available"] is True
    if not tailscale_available:
        return _empty_snapshot()

    federations: list[dict[str, object]] = []
    seen_fingerprints: set[str] = set()
    for raw_federation in payload["federations"][:MAX_PEERS]:
        federation = _validated_snapshot_federation(raw_federation)
        if federation is None:
            continue
        fingerprint = str(federation["federation_fingerprint"])
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        federations.append(federation)
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
        description=(
            "Discover FCP Federations through the already logged-in Tailscale client."
        ),
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
    try:
        payload = discover(
            web_ports=tuple(args.web_ports or (DEFAULT_WEB_PORT,)),
            timeout_seconds=timeout,
        )
    except (DiscoveryBudgetExceeded, ValueError) as exc:
        write_snapshot(args.output, _empty_snapshot())
        raise SystemExit(str(exc)) from exc
    write_snapshot(args.output, payload)
    count = len(payload["federations"])
    if payload["tailscale_available"]:
        print(f"Tailscale discovery found {count} FCP Federation(s).")
    else:
        print("Tailscale discovery unavailable; normal onboarding remains available.")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through launcher integration
    raise SystemExit(main())

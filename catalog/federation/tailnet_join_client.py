"""Host-side joining step: ask a discovered Federation to authorize this device.

This runs on the joining host, next to the launcher that already performs
Tailscale discovery. It reads the discovery snapshot, asks each discovered
coordinator's host responder for a grant, and hands the first grant it receives
to the local application through the mounted data directory.

The grant is never printed and never shown to a person. If no discovered
coordinator authorizes this device, nothing is written and the normal manual
pairing flow remains exactly as it was.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

from .tailnet_join_bridge import (
    DEFAULT_AUTO_JOIN_PORT,
    auto_join_port,
    grant_path,
    store_grant,
)
from .tailscale_host_discovery import load_snapshot
from .tailscale_peer_identity import is_tailnet_address

JOIN_PATH = "/fcp/federation/tailnet-join"
REQUEST_TIMEOUT_SECONDS = 10.0
MAX_RESPONSE_BYTES = 32_768
MAX_CANDIDATES = 8


def _candidates(snapshot: dict[str, object]) -> tuple[tuple[str, int], ...]:
    federations = snapshot.get("federations")
    if not isinstance(federations, list):
        return ()
    found: list[tuple[str, int]] = []
    for entry in federations[:MAX_CANDIDATES]:
        if not isinstance(entry, dict):
            continue
        address = entry.get("tailscale_ip")
        if not is_tailnet_address(address):
            continue
        raw_port = entry.get("auto_join_port", DEFAULT_AUTO_JOIN_PORT)
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            port = DEFAULT_AUTO_JOIN_PORT
        if not 1 <= port <= 65_535:
            port = DEFAULT_AUTO_JOIN_PORT
        found.append((str(address), port))
    return tuple(dict.fromkeys(found))


def request_join(
    address: str,
    port: int,
    *,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> tuple[str, str]:
    """Ask one coordinator to authorize this device. Returns ``(code, reason)``."""

    if not is_tailnet_address(address):
        return "", "not-a-tailnet-address"
    request = urllib.request.Request(
        f"http://{address}:{port}{JOIN_PATH}",
        data=b"{}",
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with opener(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read(MAX_RESPONSE_BYTES).decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = "refused"
        try:
            body = json.loads(error.read(MAX_RESPONSE_BYTES).decode("utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            # A refusal body is best effort; the status alone is enough.
            body = None
        if isinstance(body, dict) and isinstance(body.get("error"), str):
            detail = body["error"]
        return "", detail
    except (urllib.error.URLError, OSError, TimeoutError):
        return "", "responder-unreachable"
    except (ValueError, json.JSONDecodeError):
        return "", "responder-unreadable"
    if not isinstance(payload, dict) or payload.get("accepted") is not True:
        reason = "refused"
        if isinstance(payload, dict) and isinstance(payload.get("error"), str):
            reason = payload["error"]
        return "", reason
    code = payload.get("pairing_code")
    if not isinstance(code, str) or not code.strip():
        return "", "no-grant-returned"
    return code.strip(), "granted"


def join_discovered_federation(
    *,
    snapshot_path: Path | str,
    grant_file: Path | str,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> tuple[bool, str]:
    """Try every discovered coordinator. Returns ``(joined, reason)``."""

    snapshot = load_snapshot(snapshot_path)
    candidates = _candidates(snapshot)
    if not candidates:
        return False, "no-discovered-federation"
    reasons: list[str] = []
    for address, port in candidates:
        code, reason = request_join(address, port, opener=opener)
        if code:
            store_grant(grant_file, code)
            return True, "granted"
        reasons.append(reason)
    return False, reasons[0] if reasons else "no-discovered-federation"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ask a Tailscale-discovered Federation to authorize this device "
            "automatically, without a copied pairing code."
        )
    )
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--grant-file", default="")
    parser.add_argument("--port", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    grant_file = Path(arguments.grant_file) if arguments.grant_file else grant_path()
    joined, reason = join_discovered_federation(
        snapshot_path=arguments.snapshot,
        grant_file=grant_file,
    )
    if joined:
        print(
            "tailnet auto-join: this device was authorized by a discovered "
            "Federation and will connect on startup.",
            file=sys.stderr,
        )
        return 0
    print(
        f"tailnet auto-join: not authorized automatically ({reason}); "
        "use a pairing code from the other device if this is unexpected.",
        file=sys.stderr,
    )
    return 0 if reason == "no-discovered-federation" else 1


__all__ = [
    "auto_join_port",
    "join_discovered_federation",
    "main",
    "request_join",
]


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

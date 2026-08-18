"""Host orchestration for the supported zero-touch Tailscale startup.

The first device is explicit: ``--initialize-federation`` permits creation when
no existing Federation is discovered and asks for the sole initial human admin.
Every later device is join-only and refuses to create a split Federation when
discovery/authorization fails.

Device enrollment itself never uses human credentials. It uses the existing
same-tailnet/same-owner responder and ordinary one-use pairing grant, then runs
the installed capability bootstrap inside the Flask container.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

try:
    from scripts.federation_host_runner import load_host_module
except ModuleNotFoundError:  # direct execution from scripts/
    from federation_host_runner import load_host_module

ROOT = Path(__file__).resolve().parents[1]


class ZeroTouchStartError(RuntimeError):
    pass


def _run(
    command: list[str],
    *,
    input_text: str | None = None,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=os.environ.copy(),
        input=input_text,
        text=True,
        capture_output=capture,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise ZeroTouchStartError(
            f"command failed ({completed.returncode}): {' '.join(command)}"
            + (f"\n{detail}" if detail else "")
        )
    return completed


def _inside_flask(
    module: str,
    *arguments: str,
    input_text: str | None = None,
) -> str:
    completed = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "flask",
            "python",
            "-m",
            module,
            *arguments,
        ],
        input_text=input_text,
    )
    return (completed.stdout or "").strip()


def _status() -> dict[str, object]:
    raw = _inside_flask(
        "catalog.flask_app.services.zero_touch_federation_cli",
        "--json",
        "status",
    )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ZeroTouchStartError("FCP returned unreadable Federation status") from exc
    if not isinstance(payload, dict):
        raise ZeroTouchStartError("FCP returned invalid Federation status")
    return payload


def _discovery_path() -> Path:
    configured = str(os.environ.get("FCP_DATA_DIR") or "").strip()
    root = Path(configured) if configured else ROOT / "data"
    return root / "tailscale_discovery.json"


def _discover(*, web_port: int) -> dict[str, object]:
    discovery: ModuleType = load_host_module("tailscale_host_discovery")
    payload = discovery.discover(web_ports=(web_port,))
    discovery.write_snapshot(_discovery_path(), payload)
    if not isinstance(payload, dict):
        raise ZeroTouchStartError("Tailscale discovery returned invalid state")
    return payload


def _federations(snapshot: dict[str, object]) -> list[dict[str, object]]:
    raw = snapshot.get("federations")
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def _auto_join(snapshot: dict[str, object]) -> None:
    federations = _federations(snapshot)
    if len(federations) != 1:
        if not federations:
            raise ZeroTouchStartError("no Federation was discovered")
        raise ZeroTouchStartError(
            "more than one Federation was discovered; refusing an ambiguous automatic join"
        )

    client: ModuleType = load_host_module("tailnet_join_client")
    bridge: ModuleType = load_host_module("tailnet_join_bridge")
    joined, reason = client.join_discovered_federation(
        snapshot_path=_discovery_path(),
        grant_file=bridge.grant_path(),
    )
    if not joined:
        raise ZeroTouchStartError(
            f"the discovered Federation did not authorize this device ({reason})"
        )
    _inside_flask(
        "catalog.flask_app.services.zero_touch_federation_cli",
        "--json",
        "redeem-auto-join",
    )


def _initialize_first_federation(
    credentials: tuple[str, str] | None = None,
) -> None:
    if credentials is None:
        if not sys.stdin.isatty():
            raise ZeroTouchStartError(
                "first Federation initialization requires an interactive terminal"
            )
        email = input("Federation administrator email: ").strip()
        if not email:
            raise ZeroTouchStartError("administrator email is required")
        password = getpass.getpass("Federation administrator password: ")
    else:
        email, password = credentials
        email = email.strip()
        if not email:
            raise ZeroTouchStartError("administrator email is required")
        if not password:
            raise ZeroTouchStartError("administrator password is required")

    try:
        _inside_flask(
            "catalog.flask_app.services.zero_touch_federation_cli",
            "--json",
            "initialize",
            "--email",
            email,
            input_text=password + "\n",
        )
    finally:
        password = ""


def _bootstrap_capabilities() -> dict[str, object]:
    raw = _inside_flask(
        "catalog.flask_app.services.automatic_capability_bootstrap",
        "--json",
    )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ZeroTouchStartError("FCP returned unreadable capability-bootstrap state") from exc
    if not isinstance(payload, dict) or payload.get("startup_completed") is not True:
        raise ZeroTouchStartError("automatic capability bootstrap did not complete")
    return payload


def zero_touch_start(
    *,
    initialize_federation: bool,
    web_port: int,
    first_admin_credentials: tuple[str, str] | None = None,
) -> dict[str, object]:
    """Join or initialize once, then self-configure the trusted full FCP device."""

    status = _status()
    if status.get("state") != "connected":
        snapshot = _discover(web_port=web_port)
        federations = _federations(snapshot)
        if federations:
            _auto_join(snapshot)
        elif initialize_federation:
            if first_admin_credentials is None:
                _initialize_first_federation()
            else:
                _initialize_first_federation(first_admin_credentials)
        else:
            raise ZeroTouchStartError(
                "no Federation was discovered. This device is join-only; initialize the Federation only on the first device."
            )

    final_status = _status()
    if final_status.get("state") != "connected":
        raise ZeroTouchStartError("trusted Federation membership was not established")
    capability = _bootstrap_capabilities()
    return {
        "federation_state": final_status.get("state"),
        "node_id": final_status.get("node_id"),
        "federation_id": final_status.get("federation_id"),
        "capability": capability,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finish FCP Tailscale startup without manual device onboarding.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--initialize-federation",
        action="store_true",
        help="Permit first-Federation creation when discovery finds none.",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=int(os.environ.get("FCP_TAILSCALE_DISCOVERY_PORT") or "5000"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.web_port <= 65_535:
        print("Zero-touch FCP startup failed: invalid discovery web port", file=sys.stderr)
        return 2
    try:
        result = zero_touch_start(
            initialize_federation=args.initialize_federation,
            web_port=args.web_port,
        )
    except (OSError, RuntimeError, ValueError, ZeroTouchStartError) as exc:
        print(f"Zero-touch FCP startup failed: {exc}", file=sys.stderr)
        return 2

    capability = result["capability"]
    assert isinstance(capability, dict)
    print("Zero-touch Federation startup complete.")
    print(f"  Federation: {result.get('federation_id')}")
    print(f"  Device:     {result.get('node_id')}")
    print(f"  Benchmarks: {capability.get('benchmarks_run', 0)} run")
    print(f"  Services:   {capability.get('contributions_enabled', 0)} enabled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ZeroTouchStartError", "zero_touch_start", "main"]

"""First-device FCP startup with credentials collected before long startup work.

This host-side boundary exists only for the explicit first-Federation command.
It collects the destructive RESET confirmation and the sole initial human admin
before Docker reset/build work begins. The password remains a local in-memory
Python value; it is never placed in process arguments, environment variables,
or files and is never sent through the reset/build subprocess.
"""

from __future__ import annotations

import argparse
import getpass
import os
import subprocess
import sys
import webbrowser
from pathlib import Path

try:
    from scripts import zero_touch_federation_start as zero_touch
except ImportError:  # direct execution from scripts/
    import zero_touch_federation_start as zero_touch

ROOT = Path(__file__).resolve().parents[1]


class FirstFederationStartError(RuntimeError):
    pass


def _required_environment(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise FirstFederationStartError(f"required startup environment is missing: {name}")
    return value


def _run_fresh_reset() -> None:
    if os.name == "nt":
        command = [
            os.environ.get("COMSPEC") or "cmd.exe",
            "/d",
            "/c",
            "call",
            str(ROOT / "start.cmd"),
            "--fresh",
        ]
    else:
        command = ["sh", str(ROOT / "start.sh"), "--fresh"]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=os.environ.copy(),
        input="RESET\n",
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise FirstFederationStartError(
            f"FCP service startup failed with exit code {completed.returncode}"
        )


def _check_responder() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "federation_host_runner.py"),
            "tailnet_join_responder",
            "--check",
        ],
        cwd=ROOT,
        env=os.environ.copy(),
        check=False,
    )
    if completed.returncode != 0:
        raise FirstFederationStartError(
            "Automatic Federation joining is unavailable; refusing zero-touch startup."
        )


def _ensure_windows_firewall(port: str) -> None:
    if os.name != "nt":
        return
    show = subprocess.run(
        [
            "netsh",
            "advfirewall",
            "firewall",
            "show",
            "rule",
            "name=FCP tailnet join responder",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if show.returncode == 0:
        return
    add = subprocess.run(
        [
            "netsh",
            "advfirewall",
            "firewall",
            "add",
            "rule",
            "name=FCP tailnet join responder",
            "dir=in",
            "action=allow",
            "protocol=TCP",
            f"localport={port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if add.returncode != 0:
        print(
            f"Could not open port {port} in Windows Firewall automatically.",
            file=sys.stderr,
        )
        print("Run this once in an elevated prompt if another trusted device cannot join:")
        print(
            "  netsh advfirewall firewall add rule "
            'name="FCP tailnet join responder" dir=in action=allow '
            f"protocol=TCP localport={port}"
        )


def _start_responder(*, tailscale_ip: str, port: str, app_url: str) -> None:
    subprocess.Popen(  # noqa: S603 - fixed local interpreter/script boundary
        [
            sys.executable,
            str(ROOT / "scripts" / "federation_host_runner.py"),
            "tailnet_join_responder",
            "--bind",
            tailscale_ip,
            "--port",
            port,
            "--app-url",
            app_url,
        ],
        cwd=ROOT,
        env=os.environ.copy(),
    )


def first_federation_start(*, open_browser: bool) -> dict[str, object]:
    if not sys.stdin.isatty():
        raise FirstFederationStartError(
            "first Federation initialization requires an interactive terminal"
        )

    tailscale_ip = _required_environment("FCP_TAILSCALE_IP")
    web_port = _required_environment("FCP_TAILSCALE_DISCOVERY_PORT")
    responder_port = _required_environment("FCP_AUTO_JOIN_PORT")
    app_url = _required_environment("FCP_AUTO_JOIN_APP_URL")

    print()
    print("FIRST FEDERATION INITIALIZATION")
    print("Enter the reset confirmation and first administrator credentials now.")
    print(
        "The password is held only in this host process until FCP is ready; "
        "it is not placed in command-line arguments, environment variables, or files."
    )
    print()

    reset = input("Type RESET to continue: ").strip()
    if reset != "RESET":
        raise FirstFederationStartError("Fresh install cancelled. No state was removed.")
    email = input("Federation administrator email: ").strip()
    if not email:
        raise FirstFederationStartError("Federation administrator email is required.")
    password = getpass.getpass("Federation administrator password: ")
    if not password:
        raise FirstFederationStartError("Federation administrator password is required.")

    try:
        _run_fresh_reset()
        _check_responder()
        _ensure_windows_firewall(responder_port)
        result = zero_touch.zero_touch_start(
            initialize_federation=True,
            web_port=int(web_port),
            first_admin_credentials=(email, password),
        )
    finally:
        password = ""

    capability = result.get("capability")
    if not isinstance(capability, dict):
        raise FirstFederationStartError("automatic capability bootstrap returned invalid state")

    _start_responder(
        tailscale_ip=tailscale_ip,
        port=responder_port,
        app_url=app_url,
    )

    print("Zero-touch Federation startup complete.")
    print(f"  Federation: {result.get('federation_id')}")
    print(f"  Device:     {result.get('node_id')}")
    print(f"  Benchmarks: {capability.get('benchmarks_run', 0)} run")
    print(f"  Services:   {capability.get('contributions_enabled', 0)} enabled")
    print()
    print(f"FCP Tailscale address: {tailscale_ip}")
    print("Zero-touch startup is complete.")

    if open_browser:
        webbrowser.open(f"http://{tailscale_ip}:{web_port}/federation")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Initialize the first FCP Federation with credentials collected up front.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Open the Federation page after successful initialization.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        first_federation_start(open_browser=args.open_browser)
    except (OSError, RuntimeError, ValueError, FirstFederationStartError) as exc:
        print(f"First Federation startup failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["FirstFederationStartError", "first_federation_start", "main"]

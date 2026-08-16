"""Canonical host CLI for creating and joining an FCP Federation.

The host command deliberately uses only Python's standard library, Docker and
the already logged-in Tailscale client. Application services execute inside the
Flask container so Windows/Conda package drift cannot change onboarding.

Normal operator flow::

    fcp init
    fcp join
    fcp status

``fcp init --fresh`` is the factory-reset path. The only retained state is the
complete immutable MTConnect recording tree and its integrity metadata.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
DISCOVERY_SCRIPT = ROOT / "catalog" / "federation" / "tailscale_host_discovery.py"
TAILSCALE_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")
AUTO_PAIR_SCHEMA = "fcp.federation.tailscale-auto-pair.v1"
FEDERATION_CLI = "catalog.flask_app.services.headless_federation_cli"
CAPABILITY_CLI = "catalog.flask_app.services.headless_capability_cli"


class FCPCLIError(RuntimeError):
    pass


def _run(
    args: list[str],
    *,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = False,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            cwd=ROOT,
            env=env,
            check=check,
            text=True,
            capture_output=capture,
            input=input_text,
        )
    except FileNotFoundError as exc:
        raise FCPCLIError(f"Required command is not installed: {args[0]}") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip() if capture else ""
        suffix = f": {detail}" if detail else ""
        raise FCPCLIError(f"Command failed: {' '.join(args[:3])}{suffix}") from exc


def _git_commit() -> str:
    completed = _run(["git", "rev-parse", "HEAD"], capture=True)
    return completed.stdout.strip()


def _tailscale_ip() -> str:
    completed = _run(["tailscale", "ip", "-4"], capture=True)
    for line in completed.stdout.splitlines():
        try:
            address = ipaddress.IPv4Address(line.strip())
        except ipaddress.AddressValueError:
            continue
        if address in TAILSCALE_NETWORK:
            return str(address)
    raise FCPCLIError("No active Tailscale IPv4 address was found. Sign in to Tailscale first.")


def _runtime_env(*, device_name: str | None = None) -> dict[str, str]:
    address = _tailscale_ip()
    env = os.environ.copy()
    env.update(
        {
            "FCP_WEB_BIND": address,
            "FCP_RELAY_BIND": address,
            "FCP_PAIRING_RELAY_URL": f"ws://{address}:8765",
            "FCP_TAILSCALE_AUTO_JOIN": "1",
            "FCP_BUILD_COMMIT": _git_commit(),
        }
    )
    if device_name:
        env["FCP_DEVICE_NAME"] = device_name
    return env


def _factory_reset(env: dict[str, str]) -> None:
    print("Factory-resetting FCP (machine recordings are preserved)...")
    _run([sys.executable, str(ROOT / "reset_fcp.py"), "--yes"], env=env)


def _start_runtime(env: dict[str, str]) -> None:
    print("Starting FCP services...")
    _run(["docker", "compose", "up", "-d", "--build"], env=env)


def _container_command(
    module: str,
    *arguments: str,
    env: dict[str, str],
    input_text: str | None = None,
    interactive: bool = False,
) -> None:
    command = ["docker", "compose", "exec"]
    if not interactive:
        command.append("-T")
    command.extend(["flask", "python", "-m", module, *arguments])
    _run(command, env=env, input_text=input_text)


def _create_admin(env: dict[str, str]) -> None:
    print("\nCreate the first Federation administrator.")
    username = input("Username: ").strip()
    if not username:
        raise FCPCLIError("Username must not be empty.")
    # The current human-auth schema names this login identifier `email`.
    # Keep the product prompt as Username; the existing Flask command provides
    # hidden Password/Repeat Password prompts and validation.
    command = [
        "docker",
        "compose",
        "exec",
        "flask",
        "python",
        "-m",
        "flask",
        "--app",
        "catalog.flask_app.app:create_app",
        "fcp-user",
        "create-admin",
        "--email",
        username,
    ]
    _run(command, env=env)


def _discover() -> dict[str, Any]:
    if not DISCOVERY_SCRIPT.is_file():
        raise FCPCLIError("Tailscale discovery helper is missing from this checkout.")
    descriptor, name = tempfile.mkstemp(prefix="fcp-discovery-", suffix=".json")
    os.close(descriptor)
    target = Path(name)
    try:
        _run([sys.executable, str(DISCOVERY_SCRIPT), "--output", str(target)])
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FCPCLIError("FCP discovery returned an unreadable snapshot.") from exc
    finally:
        target.unlink(missing_ok=True)
    if not isinstance(payload, dict) or payload.get("tailscale_available") is not True:
        raise FCPCLIError("Tailscale discovery is unavailable.")
    return payload


def _select_federation(payload: dict[str, Any], selector: str | None) -> dict[str, Any]:
    raw = payload.get("federations")
    federations = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    if selector:
        wanted = selector.casefold()
        federations = [
            item
            for item in federations
            if wanted
            in {
                str(item.get("federation_fingerprint", "")).casefold(),
                str(item.get("federation_label", "")).casefold(),
                str(item.get("device_name", "")).casefold(),
            }
        ]
    if not federations:
        raise FCPCLIError("No joinable FCP Federation was discovered on this Tailnet.")
    if len(federations) != 1:
        labels = ", ".join(
            f"{item.get('federation_label')} [{item.get('federation_fingerprint')}]"
            for item in federations
        )
        raise FCPCLIError(
            "More than one Federation was found. Re-run with --federation LABEL_OR_FINGERPRINT. "
            f"Found: {labels}"
        )
    selected = federations[0]
    if selected.get("auto_join") is not True:
        raise FCPCLIError(
            "The discovered Federation is not running in Tailscale auto-join mode. "
            "Start its leader with the canonical `fcp init` runtime."
        )
    return selected


def _auto_pair(federation: dict[str, Any]) -> str:
    address = str(federation.get("tailscale_ip") or "")
    port = federation.get("web_port")
    try:
        parsed = ipaddress.IPv4Address(address)
    except ipaddress.AddressValueError as exc:
        raise FCPCLIError("Discovered Federation has an invalid Tailscale address.") from exc
    if parsed not in TAILSCALE_NETWORK or isinstance(port, bool) or not isinstance(port, int):
        raise FCPCLIError("Discovered Federation has an invalid auto-join endpoint.")
    request = Request(
        f"http://{address}:{port}/onboarding/federation/auto-pair",
        data=b"",
        headers={
            "Accept": "application/json",
            "User-Agent": "FCP-CLI/1",
            "Content-Length": "0",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            raw = response.read(64 * 1024 + 1)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise FCPCLIError("Federation auto-join authority could not be reached.") from exc
    if len(raw) > 64 * 1024:
        raise FCPCLIError("Federation auto-join response was unexpectedly large.")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FCPCLIError("Federation auto-join returned invalid JSON.") from exc
    if not isinstance(payload, dict) or payload.get("schema") != AUTO_PAIR_SCHEMA:
        raise FCPCLIError("Federation auto-join returned an unsupported response.")
    token = payload.get("pairing_code")
    if not isinstance(token, str) or not token.startswith("FCP1-"):
        raise FCPCLIError("Federation auto-join did not return valid signed authority.")
    return token


def _capability_arguments(args: argparse.Namespace) -> list[str]:
    values = ["bootstrap"]
    if getattr(args, "only", ""):
        values.extend(["--only", args.only])
    if getattr(args, "disable", ""):
        values.extend(["--disable", args.disable])
    return values


def command_init(args: argparse.Namespace) -> int:
    env = _runtime_env(device_name=args.device_name or socket.gethostname())
    if args.fresh:
        _factory_reset(env)
    _start_runtime(env)
    _create_admin(env)
    print("Creating device identity and Federation...")
    _container_command(FEDERATION_CLI, "create", env=env)
    print("Inspecting, benchmarking, and enabling this device's capabilities...")
    _container_command(CAPABILITY_CLI, *_capability_arguments(args), env=env)
    print("\nFederation ready.")
    print(f"Web: http://{env['FCP_WEB_BIND']}:5000/")
    return 0


def command_join(args: argparse.Namespace) -> int:
    env = _runtime_env(device_name=args.device_name or socket.gethostname())
    if args.fresh:
        _factory_reset(env)
    _start_runtime(env)
    print("Discovering FCP Federation through Tailscale...")
    federation = _select_federation(_discover(), args.federation)
    print(f"Found: {federation.get('federation_label')}")
    token = _auto_pair(federation)
    try:
        print("Joining Federation...")
        _container_command(
            FEDERATION_CLI,
            "join-stdin",
            env=env,
            input_text=token + "\n",
        )
    finally:
        token = ""  # do not retain authority beyond redemption
    print("Benchmarking and enabling all eligible contributions...")
    _container_command(CAPABILITY_CLI, *_capability_arguments(args), env=env)
    print("\nDevice connected and contributing.")
    return 0


def command_status(args: argparse.Namespace) -> int:
    env = _runtime_env()
    _container_command(FEDERATION_CLI, "status", env=env)
    return 0


def _add_contribution_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--only",
        default="",
        metavar="TYPE,...",
        help="Contribute only these capability types.",
    )
    parser.add_argument(
        "--disable",
        default="",
        metavar="TYPE,...",
        help="Disable these capability types while contributing everything else.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fcp",
        description="Create, join, and operate an FCP Federation.",
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Create the first FCP Federation node.")
    init.add_argument(
        "--fresh",
        action="store_true",
        help="Factory-reset FCP first; preserve only immutable machine recordings.",
    )
    init.add_argument("--device-name", default="", help="Display name for this device.")
    _add_contribution_filters(init)
    init.set_defaults(func=command_init)

    join = subparsers.add_parser("join", help="Auto-discover and join an FCP Federation.")
    join.add_argument(
        "--fresh",
        action="store_true",
        help="Factory-reset FCP first; preserve only immutable machine recordings.",
    )
    join.add_argument("--device-name", default="", help="Display name for this device.")
    join.add_argument(
        "--federation",
        default=None,
        help="Label, leader device name, or fingerprint when multiple Federations exist.",
    )
    _add_contribution_filters(join)
    join.set_defaults(func=command_join)

    status = subparsers.add_parser("status", help="Show this device's Federation state.")
    status.set_defaults(func=command_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (FCPCLIError, OSError, RuntimeError) as exc:
        print(f"FCP command failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

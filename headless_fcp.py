"""Cross-platform no-browser FCP launcher for physical acceptance.

The goal is deliberately small:

* ``start`` starts a normal FCP installation and creates/reuses the first local
  Federation through the existing onboarding authority;
* ``connect`` with no code creates one signed one-use pairing code on that first
  node;
* ``connect FCP1-...`` starts a normal FCP installation and joins the existing
  Federation through the existing pairing service;
* recorders use ``connect_recorder.py`` instead and are join-only.

This is not a second Federation implementation.  Docker Compose starts the same
services as the supported launchers and all identity/pairing mutations are
performed inside the Flask container by
``catalog.flask_app.services.headless_federation_cli``.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
_FULL_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
_LOOPBACKS = {"127.0.0.1", "localhost", "::1"}
_WILDCARDS = {"0.0.0.0", "::"}


class HeadlessStartError(RuntimeError):
    pass


def _run(
    command: Sequence[str],
    *,
    env: dict[str, str],
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        check=False,
    )
    if check and result.returncode != 0:
        detail = ""
        if capture:
            detail = (result.stderr or result.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise HeadlessStartError(
            f"command failed ({result.returncode}): {' '.join(command)}{suffix}"
        )
    return result


def _git_commit(env: dict[str, str]) -> str:
    commit = _run(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"],
        env=env,
        capture=True,
    ).stdout.strip()
    if not _FULL_SHA.fullmatch(commit):
        raise HeadlessStartError("could not resolve one immutable Git commit")
    status = _run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        env=env,
        capture=True,
    ).stdout
    if status.strip():
        raise HeadlessStartError(
            "headless acceptance refuses a checkout with local changes"
        )
    return commit.lower()


def _tailscale_ipv4(env: dict[str, str]) -> str | None:
    try:
        result = _run(
            ["tailscale", "ip", "-4"],
            env=env,
            check=False,
            capture=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if address.version == 4:
            return candidate
    return None


def _prepare_env(*, bind: str | None) -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("COMPOSE_PROJECT_NAME", "fcp")
    env.setdefault("FCP_DATA_DIR", str((ROOT / "data").resolve()))
    env.setdefault("FCP_RESULTS_DIR", str((ROOT / "results").resolve()))
    env.setdefault("FCP_WEB_PORT", "5000")
    env.setdefault("FCP_RELAY_PORT", "8765")
    env.setdefault("FCP_AI_MODEL", "llama3.2:3b")

    if bind:
        env["FCP_WEB_BIND"] = bind
        env["FCP_RELAY_BIND"] = bind
    elif not env.get("FCP_WEB_BIND") and not env.get("FCP_RELAY_BIND"):
        tailnet = _tailscale_ipv4(env)
        if tailnet:
            env["FCP_WEB_BIND"] = tailnet
            env["FCP_RELAY_BIND"] = tailnet
        else:
            env["FCP_WEB_BIND"] = "127.0.0.1"
            env["FCP_RELAY_BIND"] = "127.0.0.1"
    else:
        env.setdefault("FCP_WEB_BIND", "127.0.0.1")
        env.setdefault("FCP_RELAY_BIND", "127.0.0.1")

    env["FCP_BUILD_COMMIT"] = _git_commit(env)
    return env


def _require_host_tools(env: dict[str, str]) -> None:
    for command in (["git", "--version"], ["docker", "--version"]):
        try:
            _run(command, env=env, capture=True)
        except OSError as exc:
            raise HeadlessStartError(f"required command not found: {command[0]}") from exc
    _run(["docker", "info"], env=env, capture=True)
    _run(["docker", "compose", "version"], env=env, capture=True)


def _start_update_agent(env: dict[str, str]) -> None:
    data_dir = Path(env["FCP_DATA_DIR"])
    log_dir = data_dir / "federation" / "update-agent"
    log_dir.mkdir(parents=True, exist_ok=True)

    if os.name == "nt":
        command = [
            "powershell",
            "-NoProfile",
            "-WindowStyle",
            "Hidden",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts" / "windows" / "fcp_update_agent.ps1"),
            "-RepoRoot",
            str(ROOT),
            "-DataDirectory",
            str(data_dir),
        ]
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        subprocess.Popen(  # noqa: S603 - fixed local script and arguments
            command,
            cwd=ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        return

    log_path = log_dir / "agent.log"
    stream = log_path.open("a", encoding="utf-8")
    try:
        subprocess.Popen(  # noqa: S603 - fixed local script and arguments
            [
                sys.executable,
                str(ROOT / "scripts" / "posix" / "fcp_update_agent.py"),
                "--repo-root",
                str(ROOT),
                "--data-directory",
                str(data_dir),
            ],
            cwd=ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        stream.close()


def _compose(env: dict[str, str], *args: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return _run(["docker", "compose", *args], env=env, capture=capture)


def _fresh_reset(env: dict[str, str]) -> None:
    print("Resetting bounded device/Federation state for a fresh acceptance start...")
    _compose(env, "stop", "flask", "recorder", "relay", capture=True)
    _compose(
        env,
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        "flask",
        "-m",
        "catalog.flask_app.services.device_state_reset",
    )


def _ensure_model(env: dict[str, str]) -> None:
    model = env["FCP_AI_MODEL"]
    existing = _compose(
        env,
        "exec",
        "-T",
        "ollama",
        "ollama",
        "show",
        model,
        capture=True,
    )
    if existing.returncode == 0:
        return
    print(f"Installing required Ollama model: {model}")
    _compose(env, "--profile", "model-install", "run", "--rm", "ollama-pull")
    verified = _compose(
        env,
        "exec",
        "-T",
        "ollama",
        "ollama",
        "show",
        model,
        capture=True,
    )
    if verified.returncode != 0:
        raise HeadlessStartError(f"configured Ollama model is unavailable: {model}")


def _wait_for_flask(env: dict[str, str], *, seconds: int = 90) -> None:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "flask",
        "python",
        "-c",
        (
            "import urllib.request; "
            "r=urllib.request.urlopen('http://127.0.0.1:5000/onboarding', timeout=2); "
            "assert 200 <= r.status < 500"
        ),
    ]
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = _run(command, env=env, check=False, capture=True)
        if result.returncode == 0:
            return
        time.sleep(1)
    _compose(env, "logs", "--tail", "80", "flask", capture=False)
    raise HeadlessStartError("FCP Flask service did not become ready")


def _start_runtime(env: dict[str, str], *, fresh: bool) -> None:
    _require_host_tools(env)
    print(f"Building FCP services from {env['FCP_BUILD_COMMIT']} ...")
    _compose(env, "build", "relay", "flask", "recorder")
    if fresh:
        _fresh_reset(env)
    _start_update_agent(env)
    print("Starting Federation relay, Ollama, managed recorder, and Flask ...")
    _compose(env, "up", "-d", "relay", "ollama", "recorder")
    _ensure_model(env)
    _compose(env, "up", "-d", "flask")
    _wait_for_flask(env)
    if fresh:
        _compose(
            env,
            "exec",
            "-T",
            "flask",
            "python",
            "-m",
            "catalog.flask_app.services.device_state_reset",
            "--verify-fresh",
        )


def _inside_flask(env: dict[str, str], *args: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return _compose(
        env,
        "exec",
        "-T",
        "flask",
        "python",
        "-m",
        "catalog.flask_app.services.headless_federation_cli",
        *args,
        capture=capture,
    )


def _format_ws_url(host: str, port: str) -> str:
    host = host.strip()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"ws://{host}:{port}"


def _reachable_relay_url(env: dict[str, str], explicit: str | None) -> str | None:
    if explicit:
        return explicit.strip()
    configured = str(env.get("FCP_PAIRING_RELAY_URL") or "").strip()
    if configured:
        return configured
    bind = str(env.get("FCP_RELAY_BIND") or "").strip()
    if bind in _LOOPBACKS:
        return None
    if bind in _WILDCARDS:
        bind = _tailscale_ipv4(env) or ""
    if not bind or bind in _LOOPBACKS or bind in _WILDCARDS:
        return None
    return _format_ws_url(bind, env.get("FCP_RELAY_PORT", "8765"))


def _print_runtime(env: dict[str, str]) -> None:
    print()
    print("Headless FCP runtime is ready.")
    print(f"  Build:       {env['FCP_BUILD_COMMIT']}")
    print(f"  Web bind:    {env['FCP_WEB_BIND']}:{env['FCP_WEB_PORT']}")
    print(f"  Relay bind:  {env['FCP_RELAY_BIND']}:{env['FCP_RELAY_PORT']}")
    print(f"  Data:        {env['FCP_DATA_DIR']}")


def _start_command(args: argparse.Namespace) -> int:
    env = _prepare_env(bind=args.bind)
    _start_runtime(env, fresh=args.fresh)
    _inside_flask(env, "create")
    _print_runtime(env)
    relay_url = _reachable_relay_url(env, args.relay_url)
    if relay_url:
        print()
        print("Pairing code for the next normal node or recorder:")
        _inside_flask(env, "pairing-code", "--relay-url", relay_url)
    else:
        print()
        print(
            "No remotely reachable relay address was selected. "
            "Run `python headless_fcp.py connect --relay-url ws://HOST:8765` "
            "after choosing a trusted LAN/VPN address."
        )
    return 0


def _connect_command(args: argparse.Namespace) -> int:
    env = _prepare_env(bind=args.bind)
    code = str(args.pairing_code or "").strip()
    if code:
        _start_runtime(env, fresh=args.fresh)
        _inside_flask(env, "join", code)
        _print_runtime(env)
        return 0

    # No code means "mint a code on the existing first/local-authority node".
    # Starting the runtime is idempotent and makes the command convenient after
    # a reboot without creating a Federation or replacing any binding.
    _start_runtime(env, fresh=False)
    relay_url = _reachable_relay_url(env, args.relay_url)
    if relay_url is None:
        raise HeadlessStartError(
            "no reachable relay URL is known; use --relay-url ws://HOST:8765 "
            "or --bind HOST on a trusted LAN/VPN"
        )
    _inside_flask(env, "pairing-code", "--relay-url", relay_url)
    return 0


def _status_command(args: argparse.Namespace) -> int:
    env = _prepare_env(bind=args.bind)
    _inside_flask(env, "--json", "status")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="No-browser FCP startup and Federation pairing.",
        allow_abbrev=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser(
        "start",
        help="Start the first normal FCP node and create/reuse its local Federation.",
    )
    start.add_argument(
        "--fresh",
        action="store_true",
        help="Reset bounded local FCP/Federation setup state before creating the first node.",
    )
    start.add_argument(
        "--bind",
        help="Trusted host/VPN IP to bind both web and relay services. Auto-uses Tailscale when available.",
    )
    start.add_argument(
        "--relay-url",
        help="Explicit reachable ws:// or wss:// relay URL advertised in the first pairing code.",
    )
    start.set_defaults(handler=_start_command)

    connect = subparsers.add_parser(
        "connect",
        help="Without a code, mint one on the first node; with FCP1-..., start and join this normal node.",
    )
    connect.add_argument("pairing_code", nargs="?", metavar="FCP1-...")
    connect.add_argument(
        "--fresh",
        action="store_true",
        help="Reset this joining installation before redeeming the code. Never used when only minting a code.",
    )
    connect.add_argument(
        "--bind",
        help="Trusted host/VPN IP for this FCP runtime. Auto-uses Tailscale when available.",
    )
    connect.add_argument(
        "--relay-url",
        help="Reachable relay URL to embed when minting a code on the first node.",
    )
    connect.set_defaults(handler=_connect_command)

    status = subparsers.add_parser("status", help="Print the saved/revalidated Federation state as JSON.")
    status.add_argument("--bind", help=argparse.SUPPRESS)
    status.set_defaults(handler=_status_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (HeadlessStartError, OSError, ValueError) as exc:
        print(f"Headless FCP failed: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Headless FCP interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

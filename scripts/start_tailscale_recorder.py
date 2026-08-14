"""Fail-closed Tailscale launcher for the headless MTConnect recorder."""

from __future__ import annotations

import getpass
import ipaddress
import os
import shutil
import socket
import subprocess
import sys
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

import start_recorder
from catalog.flask_app.services.federation_pairing_service import (
    PairingCodeCodec,
    RemotePairingStore,
)

DEFAULT_DEVICE_NAME: Final = "Maskin 4 recorder - Mekanisk Service Halden"
PAIRING_STATE_RELATIVE: Final = Path(
    "federation/onboarding/remote_pairing.json"
)
TAILSCALE_NETWORK: Final = ipaddress.ip_network("100.64.0.0/10")


def _option_value(arguments: list[str], name: str) -> str | None:
    for index in range(len(arguments) - 1, -1, -1):
        value = arguments[index]
        if value == name:
            if index + 1 >= len(arguments) or arguments[index + 1].startswith("--"):
                raise RuntimeError(f"{name} requires a value.")
            return arguments[index + 1]
        prefix = f"{name}="
        if value.startswith(prefix):
            return value[len(prefix) :]
    return None


def _has_option(arguments: list[str], name: str) -> bool:
    return any(
        value == name or value.startswith(f"{name}=") for value in arguments
    )


def _data_directory(arguments: list[str]) -> Path:
    option = _option_value(arguments, "--data-dir")
    if option is not None and not option.strip():
        raise RuntimeError("--data-dir requires a non-empty value.")
    configured = option or os.environ.get("FCP_RECORDER_DATA_DIR") or "data"
    return Path(configured).resolve()


def _tailscale_ipv4() -> ipaddress.IPv4Address:
    executable = shutil.which("tailscale")
    if executable is None:
        raise RuntimeError(
            "Tailscale was not found. Install it and sign this host in to the "
            "shared tailnet before starting the recorder."
        )
    try:
        completed = subprocess.run(
            [executable, "ip", "-4"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            "Tailscale status could not be read; the recorder was not started."
        ) from exc
    first = next(
        (line.strip() for line in completed.stdout.splitlines() if line.strip()),
        "",
    )
    try:
        address = ipaddress.ip_address(first)
    except ValueError as exc:
        raise RuntimeError(
            "Tailscale is installed but has no signed-in IPv4 address. Run "
            "'tailscale up' once, then run this command again."
        ) from exc
    if (
        completed.returncode != 0
        or not isinstance(address, ipaddress.IPv4Address)
        or address not in TAILSCALE_NETWORK
    ):
        raise RuntimeError(
            "Tailscale did not return a signed-in 100.64.0.0/10 address; "
            "the recorder was not started."
        )
    return address


def _pairing_key(arguments: list[str]) -> str | None:
    membership = _data_directory(arguments) / PAIRING_STATE_RELATIVE
    if membership.is_file():
        os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)
        return None
    supplied = os.environ.pop("FCP_RECORDER_FEDERATION_KEY", "").strip()
    if not supplied:
        if not sys.stdin.isatty():
            raise RuntimeError(
                "First startup needs an interactive FCP1 pairing-code prompt "
                "or FCP_RECORDER_FEDERATION_KEY."
            )
        supplied = getpass.getpass(
            "Paste a fresh FCP1 pairing code from the current Federation leader: "
        ).strip()
    if not supplied.startswith("FCP1-"):
        raise RuntimeError("The pairing code must start with FCP1-.")
    return supplied


def _recorder_arguments(arguments: list[str]) -> list[str]:
    if any(
        value.strip().startswith("FCP1-")
        or value.strip().startswith("--federation-k")
        for value in arguments
    ):
        raise RuntimeError(
            "Do not put the FCP1 code on this command line; run the command "
            "without it and use the hidden prompt."
        )
    result = ["--require-federation", "--require-data-sharing"]
    if not _has_option(arguments, "--device-name"):
        result.extend(("--device-name", DEFAULT_DEVICE_NAME))
    if not _has_option(arguments, "--data-dir"):
        configured_data = os.environ.get("FCP_RECORDER_DATA_DIR", "").strip()
        if configured_data:
            result.extend(("--data-dir", configured_data))
    result.extend(arguments)
    return result


def _relay_url(arguments: list[str], pairing_key: str | None) -> str:
    try:
        if pairing_key is not None:
            return PairingCodeCodec().decode(pairing_key).relay_url
        state = RemotePairingStore(
            _data_directory(arguments) / PAIRING_STATE_RELATIVE
        ).load()
    except (OSError, RuntimeError, ValueError) as exc:
        raise RuntimeError(
            "The Federation pairing code or saved membership is invalid."
        ) from exc
    if state is None:
        raise RuntimeError("No saved Federation membership was found.")
    return state.relay_url


def _require_tailnet_relay(relay_url: str) -> None:
    parsed = urlsplit(relay_url)
    host = parsed.hostname
    if not host:
        raise RuntimeError("The Federation pairing relay has no host.")
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise RuntimeError(
            "The Federation relay must use the leader's literal Tailscale "
            "100.64.0.0/10 IPv4 address. Generate the pairing code while the "
            "leader is open on that numeric address."
        ) from exc
    if (
        not isinstance(address, ipaddress.IPv4Address)
        or address not in TAILSCALE_NETWORK
    ):
        raise RuntimeError(
            "The Federation relay is not a Tailscale 100.64.0.0/10 address. "
            "Generate the pairing code while the leader is open on its numeric "
            "Tailscale address."
        )
    executable = shutil.which("tailscale")
    if executable is None:
        raise RuntimeError("Tailscale disappeared during the relay preflight.")
    try:
        peer_check = subprocess.run(
            [
                executable,
                "ping",
                "--timeout=5s",
                "--c=1",
                str(address),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=8.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            "The Federation leader could not be checked as a Tailscale peer."
        ) from exc
    if peer_check.returncode != 0:
        raise RuntimeError(
            "The Federation relay address is not a reachable peer in this "
            "tailnet. Check the leader's Tailscale login and tailnet ACL."
        )
    try:
        connection = socket.create_connection((str(address), port), timeout=5.0)
    except OSError as exc:
        raise RuntimeError(
            f"The Federation relay is not reachable over Tailscale on TCP {port}."
        ) from exc
    connection.close()


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    pairing_key: str | None = None
    try:
        try:
            recorder_arguments = _recorder_arguments(arguments)
            parser = start_recorder.build_parser()
            if any(value in {"-h", "--help"} for value in arguments):
                parser.print_help()
                return 0
            _data_directory(recorder_arguments)
            try:
                parser.parse_args(recorder_arguments)
            except SystemExit as exc:
                return int(exc.code or 0)
            address = _tailscale_ipv4()
            pairing_key = _pairing_key(recorder_arguments)
            _require_tailnet_relay(_relay_url(recorder_arguments, pairing_key))
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 2

        if pairing_key is not None:
            os.environ["FCP_RECORDER_FEDERATION_KEY"] = pairing_key
            pairing_key = None
        print(f"Tailscale ready: {address}")
        print(
            "Starting the recorder; Federation membership and the recorder data "
            "publication route are required."
        )
        return start_recorder.main(recorder_arguments)
    finally:
        pairing_key = None
        os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)


if __name__ == "__main__":
    raise SystemExit(main())

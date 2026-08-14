"""Fail-closed Tailscale launcher for the headless MTConnect recorder."""

from __future__ import annotations

import getpass
import ipaddress
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Final

import start_recorder

DEFAULT_DEVICE_NAME: Final = "Machine 4 recorder - Mekanisk Service Halden"
PAIRING_STATE_RELATIVE: Final = Path(
    "federation/onboarding/remote_pairing.json"
)
TAILSCALE_NETWORK: Final = ipaddress.ip_network("100.64.0.0/10")


def _option_value(arguments: list[str], name: str) -> str | None:
    for index, value in enumerate(arguments):
        if value == name and index + 1 < len(arguments):
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
    configured = (
        _option_value(arguments, "--data-dir")
        or os.environ.get("FCP_RECORDER_DATA_DIR")
        or "data"
    )
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
        return None
    supplied = os.environ.get("FCP_RECORDER_FEDERATION_KEY", "").strip()
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
    if any(value.startswith("FCP1-") for value in arguments):
        raise RuntimeError(
            "Do not put the FCP1 code on this command line; run the command "
            "without it and use the hidden prompt."
        )
    result = ["--require-federation", "--require-data-sharing"]
    if not _has_option(arguments, "--device-name"):
        result.extend(("--device-name", DEFAULT_DEVICE_NAME))
    result.extend(arguments)
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        address = _tailscale_ipv4()
        recorder_arguments = _recorder_arguments(arguments)
        pairing_key = _pairing_key(arguments)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if pairing_key is not None:
        os.environ["FCP_RECORDER_FEDERATION_KEY"] = pairing_key
    print(f"Tailscale ready: {address}")
    print(
        "Starting the recorder; Federation membership and the recorder data "
        "publication route are required."
    )
    try:
        return start_recorder.main(recorder_arguments)
    finally:
        os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)
        pairing_key = None


if __name__ == "__main__":
    raise SystemExit(main())

"""Fail-closed zero-touch Tailscale launcher for the headless MTConnect recorder."""

from __future__ import annotations

import ipaddress
import os
import shutil
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final
from urllib.parse import urlsplit

import start_recorder
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.flask_app.services.federation_pairing_service import (
    PairingCodeCodec,
    RemotePairingStore,
)
from catalog.node.client import RelayNodeClient
from scripts.federation_host_runner import load_host_module

DEFAULT_DEVICE_NAME: Final = "FCP MTConnect recorder"
PAIRING_STATE_RELATIVE: Final = Path("federation/onboarding/remote_pairing.json")
TAILSCALE_NETWORK = ipaddress.ip_network("100.64.0.0/10")


async def _restore_remote_session_creators(
    client: RelayNodeClient,
    status: dict[str, Any],
) -> dict[str, Any]:
    """Restore omitted immutable session creators from authoritative revision 1.

    Remote coordinator status may omit ``created_by_node_id``. Recorder storage
    selection intentionally trusts only the session creator's advertised storage
    authority, so recover that immutable identity from the authoritative
    ``session.created`` event rather than guessing from connectivity or
    capabilities.

    Reading that event is a bounded authenticated replay of a session this node
    has already joined, so sessions the local state does not yet know are left
    untouched. ``RelayNodeClient.connect`` reads coordinator status before it
    reconciles local membership, and a status probe must never be what stops a
    node from connecting. An unrecoverable creator simply stays absent, which
    recorder selection already reports as an unavailable owner.
    """

    sessions = status.get("sessions")
    if not isinstance(sessions, list):
        return status

    joined = {item.session_id for item in client.state.joined_sessions()}
    replacements: dict[str, str] = {}
    for value in sessions:
        if not isinstance(value, dict):
            continue
        session_id = value.get("session_id")
        creator = value.get("created_by_node_id")
        if (
            not isinstance(session_id, str)
            or not session_id
            or session_id not in joined
            or (isinstance(creator, str) and creator)
        ):
            continue
        try:
            events, _current_revision = await client.coordinator_replay_page(
                session_id=session_id,
                last_applied_revision=0,
                limit=1,
            )
        except (FederationOperationError, FederationValidationError):
            continue
        if not events:
            continue
        first = events[0]
        event_creator = getattr(first, "actor_node_id", None)
        if (
            getattr(first, "revision", None) == 1
            and getattr(first, "event_type", None) == "session.created"
            and getattr(first, "session_id", None) == session_id
            and isinstance(event_creator, str)
            and event_creator
        ):
            replacements[session_id] = event_creator

    if not replacements:
        return status

    restored = dict(status)
    restored["sessions"] = [
        (
            {**value, "created_by_node_id": replacements[value["session_id"]]}
            if isinstance(value, dict)
            and value.get("session_id") in replacements
            else value
        )
        for value in sessions
    ]
    return restored


def _install_remote_session_creator_fallback() -> Callable[[], None]:
    """Make complete remote status available to recorder storage selection.

    Returns the callable that restores the original method. The launcher owns
    one recorder process, but the entry point is also exercised in-process, and
    a class patch that outlives its caller would silently change every later
    client in that process.
    """

    if getattr(RelayNodeClient.coordinator_status, "_fcp_creator_fallback", False):
        return lambda: None
    original = RelayNodeClient.coordinator_status

    async def coordinator_status(self: RelayNodeClient) -> dict[str, Any]:
        status = await original(self)
        return await _restore_remote_session_creators(self, status)

    coordinator_status._fcp_creator_fallback = True
    RelayNodeClient.coordinator_status = coordinator_status

    def restore() -> None:
        if RelayNodeClient.coordinator_status is coordinator_status:
            RelayNodeClient.coordinator_status = original

    return restore


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
    return any(value == name or value.startswith(f"{name}=") for value in arguments)


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
            "Tailscale was not found. Install it and sign this host in to the shared tailnet before starting the recorder."
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
        (line.strip() for line in completed.stdout.splitlines() if line.strip()), ""
    )
    try:
        address = ipaddress.ip_address(first)
    except ValueError as exc:
        raise RuntimeError(
            "Tailscale is installed but has no signed-in IPv4 address. Run 'tailscale up' once, then run this command again."
        ) from exc
    if (
        completed.returncode != 0
        or not isinstance(address, ipaddress.IPv4Address)
        or address not in TAILSCALE_NETWORK
    ):
        raise RuntimeError(
            "Tailscale did not return a signed-in 100.64.0.0/10 address; the recorder was not started."
        )
    return address


def _discovery_web_port() -> int:
    raw = str(os.environ.get("FCP_TAILSCALE_DISCOVERY_PORT") or "5000").strip()
    try:
        port = int(raw)
    except ValueError as exc:
        raise RuntimeError("FCP_TAILSCALE_DISCOVERY_PORT must be an integer.") from exc
    if not 1 <= port <= 65_535:
        raise RuntimeError("FCP_TAILSCALE_DISCOVERY_PORT is outside the valid port range.")
    return port


def _pairing_key(arguments: list[str]) -> str | None:
    """Return one auto-join grant on first start; saved members need no grant."""

    data_dir = _data_directory(arguments)
    membership = data_dir / PAIRING_STATE_RELATIVE
    if membership.is_file():
        os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)
        return None

    # The normal Tailscale recorder path never accepts a human-supplied key.
    # Clear any inherited legacy value so it cannot silently bypass discovery.
    os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)

    discovery = load_host_module("tailscale_host_discovery")
    snapshot_path = data_dir / "tailscale_discovery.json"
    snapshot = discovery.discover(web_ports=(_discovery_web_port(),))
    discovery.write_snapshot(snapshot_path, snapshot)
    federations = snapshot.get("federations") if isinstance(snapshot, dict) else None
    candidates = [item for item in federations if isinstance(item, dict)] if isinstance(federations, list) else []
    if not candidates:
        raise RuntimeError(
            "No FCP Federation was discovered over Tailscale. Start the Federation creator first; manual FCP1 pairing remains a recovery path through start_recorder.py."
        )
    if len(candidates) != 1:
        raise RuntimeError(
            "More than one FCP Federation was discovered; refusing an ambiguous recorder auto-join."
        )

    client = load_host_module("tailnet_join_client")
    bridge = load_host_module("tailnet_join_bridge")
    grant_file = data_dir / bridge.GRANT_RELATIVE
    joined, reason = client.join_discovered_federation(
        snapshot_path=snapshot_path,
        grant_file=grant_file,
    )
    if not joined:
        raise RuntimeError(
            f"The discovered Federation did not authorize this recorder ({reason})."
        )
    code = bridge.load_grant(grant_file)
    bridge.clear_grant(grant_file)
    if not isinstance(code, str) or not code.startswith("FCP1-"):
        raise RuntimeError("The verified Federation responder returned no usable enrollment grant.")
    return code


def _recorder_arguments(arguments: list[str]) -> list[str]:
    if any(
        value.strip().startswith("FCP1-")
        or value.strip().startswith("--federation-k")
        for value in arguments
    ):
        raise RuntimeError(
            "Do not put an FCP1 code on this command line. The Tailscale recorder joins automatically; use start_recorder.py only for explicit recovery pairing."
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
            "The Federation enrollment grant or saved membership is invalid."
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
            "The Federation relay must use the leader's literal Tailscale 100.64.0.0/10 IPv4 address."
        ) from exc
    if (
        not isinstance(address, ipaddress.IPv4Address)
        or address not in TAILSCALE_NETWORK
    ):
        raise RuntimeError(
            "The Federation relay is not a Tailscale 100.64.0.0/10 address."
        )
    executable = shutil.which("tailscale")
    if executable is None:
        raise RuntimeError("Tailscale disappeared during the relay preflight.")
    try:
        peer_check = subprocess.run(
            [executable, "ping", "--timeout=5s", "--c=1", str(address)],
            check=False,
            capture_output=True,
            text=True,
            timeout=8.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            "The Federation leader could not be checked as a Tailscale peer."
        ) from exc

    # On Windows, `tailscale ping` can successfully reach a peer via DERP and
    # still exit 1 when it cannot upgrade that path to a direct connection.
    # A DERP pong is valid Tailscale reachability; the following TCP connect is
    # the authoritative check that the advertised relay service is usable.
    peer_output = f"{peer_check.stdout}\n{peer_check.stderr}".lower()
    peer_reached = peer_check.returncode == 0 or "pong from " in peer_output
    if not peer_reached:
        raise RuntimeError(
            "The Federation relay address is not a reachable peer in this tailnet. Check the leader's Tailscale login and tailnet ACL."
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
    restore_coordinator_status: Callable[[], None] = lambda: None
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
            # start_recorder consumes this only in-process and persists only the
            # resulting public-safe reconnect binding.  The one-use key is
            # removed from the environment in the finally block below.
            os.environ["FCP_RECORDER_FEDERATION_KEY"] = pairing_key
            pairing_key = None
        restore_coordinator_status = _install_remote_session_creator_fallback()
        print(f"Tailscale ready: {address}")
        print(
            "Starting the recorder; Federation membership and the recorder data publication route are required."
        )
        return start_recorder.main(recorder_arguments)
    finally:
        pairing_key = None
        os.environ.pop("FCP_RECORDER_FEDERATION_KEY", None)
        restore_coordinator_status()


if __name__ == "__main__":
    raise SystemExit(main())

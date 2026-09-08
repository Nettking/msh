"""Product relay entrypoint with optional replicated C03 authority.

When ``FCP_REPLICATED_CONTROL_PLANE_CONFIG`` is absent this module delegates
verbatim to the established provider relay service. When configured, all three
voter hosts run the authenticated consensus/credential endpoints and the
existing WebSocket provider relay against a quorum-fenced coordinator facade.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path

from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_product import ReplicatedControlPlaneDeployment
from catalog.federation.control_plane_replication import ControlPlaneError
from catalog.federation.control_plane_runtime import (
    PhysicalReadyReplicatedFederationRuntime,
)
from catalog.federation.control_plane_status import (
    CONTROL_PLANE_STATUS_FILE,
    write_status,
)
from catalog.federation.errors import FederationOperationError, FederationValidationError
from catalog.federation.service_incarnation import (
    STOP_COMPLETED,
    STOP_FAILURE,
    STOP_OPERATOR,
    incarnation_state_file,
    record_service_start,
    record_service_stop,
)
from catalog.relay.provider_service import (
    ProviderAuthorityRelayServer,
    main as legacy_main,
)
from catalog.relay.service import (
    RelayConfigurationError,
    _build_parser,
    _wait_for_relay_shutdown,
)

CONFIG_ENV = "FCP_REPLICATED_CONTROL_PLANE_CONFIG"
BOOTSTRAP_FEDERATION_ENV = "FCP_C03_BOOTSTRAP_FEDERATION_ID"
BOOTSTRAP_SESSION_ENV = "FCP_C03_BOOTSTRAP_SESSION_ID"
DEFAULT_AUTH_DATABASE = "/app/data/auth/users.sqlite3"
DEFAULT_AUTH_SALT = "/app/data/auth/password-salt"
STATUS_INTERVAL_SECONDS = 1.0


def _optional_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


async def _publish_control_plane_status(
    runtime: PhysicalReadyReplicatedFederationRuntime,
    status_path: Path,
) -> None:
    """Keep one public-safe C03 status surface current for acceptance probes."""

    while True:
        try:
            write_status(
                status_path,
                runtime.node,
                ready=runtime.ready,
                voter_only=False,
            )
        except OSError as exc:
            # Status is evidence/diagnostics only. A transient filesystem error
            # must not manufacture authority or stop an otherwise healthy
            # quorum-fenced relay; physical acceptance will fail closed if the
            # surface stays absent.
            logging.warning("C03 status publication unavailable (%s)", type(exc).__name__)
        await asyncio.sleep(STATUS_INTERVAL_SECONDS)


async def _serve_replicated(args, config_path: Path) -> None:
    deployment = ReplicatedControlPlaneDeployment.from_file(config_path)
    if Path(args.database) != deployment.coordinator_database:
        raise ControlPlaneError(
            "relay database must match replicated control-plane coordinator_database"
        )

    runtime = PhysicalReadyReplicatedFederationRuntime(
        deployment,
        human_auth_database=Path(
            os.getenv("FCP_AUTH_DATABASE", DEFAULT_AUTH_DATABASE)
        ),
        human_auth_password_salt=Path(
            os.getenv("FCP_AUTH_PASSWORD_SALT_FILE", DEFAULT_AUTH_SALT)
        ),
        bootstrap_federation_id=_optional_env(BOOTSTRAP_FEDERATION_ENV),
        bootstrap_session_id=_optional_env(BOOTSTRAP_SESSION_ENV),
    )
    coordinator = PhysicalReadyReplicatedSessionCoordinator(runtime)
    relay = ProviderAuthorityRelayServer(
        coordinator,
        host=args.host,
        port=args.port,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        tls_key_password=None,
        unsafe_development_plaintext=args.unsafe_development_plaintext,
        auth_timeout_seconds=args.auth_timeout_seconds,
        send_timeout_seconds=args.send_timeout_seconds,
        heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
        sweep_interval_seconds=args.sweep_interval_seconds,
        max_connections=args.max_connections,
    )

    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = []
    import signal

    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, stop_requested.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed_signals.append(shutdown_signal)

    status_path = deployment.coordinator_database.parent / CONTROL_PLANE_STATUS_FILE
    runtime.start()
    write_status(status_path, runtime.node, ready=runtime.ready, voter_only=False)
    status_task = asyncio.create_task(
        _publish_control_plane_status(runtime, status_path),
        name="fcp-c03-status",
    )
    await relay.start()
    try:
        await _wait_for_relay_shutdown(relay, stop_requested)
    finally:
        for shutdown_signal in installed_signals:
            with suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(shutdown_signal)
        status_task.cancel()
        with suppress(asyncio.CancelledError):
            await status_task
        await relay.stop()
        runtime.close()


def main(argv: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    configured = os.getenv(CONFIG_ENV, "").strip()
    if not configured:
        return legacy_main(values)
    if not values or values[0] != "serve":
        # Administrative one-shot commands still use the established service;
        # they do not silently acquire replicated leader authority.
        return legacy_main(values)

    parser = _build_parser()
    args = parser.parse_args(values)
    if args.tls_key_password_prompt:
        # Keep the established interactive encrypted-key path rather than
        # duplicating terminal-secret handling here.
        return legacy_main(values)

    config_path = Path(configured)
    incarnation = incarnation_state_file(Path(args.database).parent, "relay")
    record_service_start(incarnation, service="relay")
    try:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        asyncio.run(_serve_replicated(args, config_path))
        record_service_stop(incarnation, service="relay", reason=STOP_COMPLETED)
        return 0
    except KeyboardInterrupt:
        record_service_stop(incarnation, service="relay", reason=STOP_OPERATOR)
        return 0
    except (
        ControlPlaneError,
        FederationValidationError,
        FederationOperationError,
        RelayConfigurationError,
        OSError,
        sqlite3.Error,
    ) as error:
        code = getattr(error, "code", "replicated-relay-command-failed")
        record_service_stop(incarnation, service="relay", reason=str(code))
        print(f"relay command failed ({code})", file=sys.stderr)
        return 2
    except Exception:  # noqa: BLE001 - nonzero exit lets Docker restart/fail closed
        record_service_stop(incarnation, service="relay", reason=STOP_FAILURE)
        print(f"relay command failed ({STOP_FAILURE})", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

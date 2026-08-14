"""Run the relay-aware automatic storage failover coordinator."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import secrets
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageFailoverCoordinator,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.recorder_storage_relay import (
    RecorderAwareStorageControlRelayChannel,
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
    STORAGE_CONTROL_CAPABILITY_VERSION,
)
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.shared_file_storage import FederationLogicalStorageAuthority

from .client import RelayNodeClient
from .state import EnrollmentState
from .storage_agent import (
    DEFAULT_ENROLLMENT_TOKEN_ENV,
    DEFAULT_SESSION_INVITATION_ENV,
)


@dataclass(frozen=True)
class StorageAuthoritySettings:
    """Everything one logical-storage authority process needs to run.

    The authority predates any in-process host, so its composition lived inside
    an argparse handler and could only be started by hand. Field names match the
    original option names exactly, which keeps the CLI a thin adapter and lets a
    supervised host supply the same values without a second composition.
    """

    relay_control_database: str
    storage_control_database: str
    publication_database: str
    failover_database: str
    state_dir: str
    relay: str
    display_name: str
    session_id: str
    acknowledgements_database: str | None = None
    allow_insecure_local: bool = False
    heartbeat_interval: float = 10.0
    request_timeout: float = 15.0
    scan_interval: float = 2.0
    lease_seconds: float = 300.0

    def __post_init__(self) -> None:
        for name in (
            "relay_control_database",
            "storage_control_database",
            "publication_database",
            "failover_database",
            "state_dir",
            "relay",
            "display_name",
            "session_id",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise FederationValidationError(
                    "invalid-storage-authority-settings",
                    name,
                    "must be non-empty text",
                )
        for name in (
            "heartbeat_interval",
            "request_timeout",
            "scan_interval",
            "lease_seconds",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise FederationValidationError(
                    "invalid-storage-authority-settings",
                    name,
                    "must be a finite positive number of seconds",
                )

    @classmethod
    def from_namespace(
        cls, arguments: argparse.Namespace
    ) -> StorageAuthoritySettings:
        return cls(
            relay_control_database=arguments.relay_control_database,
            storage_control_database=arguments.storage_control_database,
            publication_database=arguments.publication_database,
            failover_database=arguments.failover_database,
            state_dir=arguments.state_dir,
            relay=arguments.relay,
            display_name=arguments.display_name,
            session_id=arguments.session_id,
            acknowledgements_database=arguments.acknowledgements_database,
            allow_insecure_local=bool(arguments.allow_insecure_local),
            heartbeat_interval=float(arguments.heartbeat_interval),
            request_timeout=float(arguments.request_timeout),
            scan_interval=float(arguments.scan_interval),
            lease_seconds=float(arguments.lease_seconds),
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--relay-control-database", required=True)
    parser.add_argument("--storage-control-database", required=True)
    parser.add_argument("--publication-database", required=True)
    parser.add_argument("--failover-database", required=True)
    parser.add_argument(
        "--acknowledgements-database",
        help=(
            "Coordinator acknowledgement journal for recorder ingress. "
            "Defaults beside --storage-control-database."
        ),
    )
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--relay", required=True)
    parser.add_argument("--display-name", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--allow-insecure-local", action="store_true")
    parser.add_argument("--heartbeat-interval", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    parser.add_argument("--scan-interval", type=float, default=2.0)
    parser.add_argument("--lease-seconds", type=float, default=300.0)
    parser.add_argument("command", choices=("scan-once", "run"))
    return parser


async def _connect_authority(
    arguments: StorageAuthoritySettings,
) -> RelayNodeClient:
    client = RelayNodeClient(
        state_directory=Path(arguments.state_dir),
        relay_url=arguments.relay,
        display_name=arguments.display_name,
        allow_insecure_local=arguments.allow_insecure_local,
        heartbeat_interval=arguments.heartbeat_interval,
        request_timeout=arguments.request_timeout,
    )
    enrollment = client.state.status()["enrollment_state"]
    enrollment_token = (
        os.environ.get(DEFAULT_ENROLLMENT_TOKEN_ENV)
        if enrollment == EnrollmentState.UNENROLLED.value
        else None
    )
    if enrollment == EnrollmentState.UNENROLLED.value and not enrollment_token:
        raise FederationValidationError(
            "storage-failover-enrollment-required",
            DEFAULT_ENROLLMENT_TOKEN_ENV,
            "first startup requires a protected enrollment token",
        )
    await client.connect(enrollment_token=enrollment_token)
    joined = {item.session_id for item in client.state.joined_sessions()}
    if arguments.session_id not in joined:
        invitation = os.environ.get(DEFAULT_SESSION_INVITATION_ENV)
        if not invitation:
            await client.disconnect(error_code="storage-failover-session-required")
            raise FederationValidationError(
                "storage-failover-session-invitation-required",
                DEFAULT_SESSION_INVITATION_ENV,
                "first session join requires a protected invitation token",
            )
        session = await client.join_session(invitation)
        if session.get("session_id") != arguments.session_id:
            await client.disconnect(error_code="storage-failover-session-mismatch")
            raise FederationValidationError(
                "storage-failover-session-mismatch",
                "session_id",
                "the invitation joined another session",
            )
    return client


def _recorder_storage_capability(
    control: PhaseDControlPlane,
    client: RelayNodeClient,
    session_id: str,
) -> CapabilityAnnouncement:
    snapshot = control.snapshot(session_id)
    ready_groups = tuple(
        sorted(
            group_id
            for group_id, assignment in snapshot.groups.items()
            if assignment.primary_provider_id is not None
            and snapshot.leader_grants.get(group_id) is not None
        )
    )
    return CapabilityAnnouncement(
        capability_id="logical-storage-authority",
        node_id=client.node_id,
        session_id=session_id,
        type=STORAGE_CONTROL_CAPABILITY_TYPE,
        protocol=STORAGE_CONTROL_CAPABILITY_PROTOCOL,
        protocol_version=STORAGE_CONTROL_CAPABILITY_VERSION,
        status=(
            CapabilityStatus.READY
            if ready_groups
            else CapabilityStatus.UNAVAILABLE
        ),
        properties={
            "kind": "recorder-logical-storage-authority",
            "group_ids": list(ready_groups),
        },
        announced_at=datetime.now(timezone.utc),
    )


def _acknowledgements_database(arguments: StorageAuthoritySettings) -> Path:
    if arguments.acknowledgements_database:
        return Path(arguments.acknowledgements_database)
    return Path(arguments.storage_control_database).with_name(
        "recorder_ingest_acknowledgements.sqlite3"
    )


def _catalog_cursor_secret(arguments: StorageAuthoritySettings) -> bytes:
    """Load or create the authority's restart-stable private cursor key."""

    directory = Path(arguments.state_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / "storage_catalog_cursor.key"
    try:
        value = path.read_bytes()
    except FileNotFoundError:
        value = secrets.token_bytes(32)
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            value = path.read_bytes()
        else:
            try:
                remaining = memoryview(value)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("cursor secret write was incomplete")
                    remaining = remaining[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    if len(value) != 32:
        raise FederationValidationError(
            "invalid-catalog-cursor-secret",
            "state_dir",
            "storage catalog cursor key must contain exactly 32 bytes",
        )
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return value


def _recorder_ingest_authorizer(
    coordinator: SessionCoordinator,
):
    """Require current membership and an active recorder contribution."""

    def authorize(actor_node_id: str, session_id: str) -> None:
        coordinator.store.require_membership(
            session_id=session_id,
            node_id=actor_node_id,
        )
        capabilities = coordinator.store.list_capabilities(
            session_id=session_id,
            node_id=actor_node_id,
            capability_type="recorder",
        )
        if not any(
            capability.status is CapabilityStatus.READY
            and capability.protocol == "mtconnect"
            and capability.properties.get("logical_storage") is True
            for capability in capabilities
        ):
            raise FederationValidationError(
                "recorder-capability-required",
                "actor_node_id",
                "recorder ingress requires an active logical-storage recorder contribution",
            )

    return authorize


def _federation_storage_read_authorizer(
    coordinator: SessionCoordinator,
):
    """Limit catalog discovery and reads to current session members."""

    def authorize(
        actor_node_id: str,
        session_id: str,
        _group_id: str,
        _dataset_id: str | None,
    ) -> None:
        coordinator.store.require_membership(
            session_id=session_id,
            node_id=actor_node_id,
        )

    return authorize


async def _run(
    arguments: StorageAuthoritySettings,
    *,
    command: str,
    stop: asyncio.Event | None = None,
    on_announced: Callable[[CapabilityAnnouncement], None] | None = None,
) -> list[dict[str, object]]:
    """Compose and run the authority once, or until ``stop`` is set.

    ``stop`` and ``on_announced`` exist so a supervised host can shut the
    authority down cleanly and observe readiness without scraping stdout. The
    CLI passes neither and behaves exactly as before.
    """

    client: RelayNodeClient | None = None
    endpoint: RelayStorageEndpoint | None = None
    failover: StorageFailoverCoordinator | None = None
    try:
        client = await _connect_authority(arguments)
        endpoint = RelayStorageEndpoint(
            client,
            request_timeout=arguments.request_timeout,
        )
        await endpoint.start()
        control = PhaseDControlPlane(Path(arguments.storage_control_database))
        coordinator = SessionCoordinator(Path(arguments.relay_control_database))
        channel = RecorderAwareStorageControlRelayChannel(
            client,
            endpoint,
            timeout=arguments.request_timeout,
        )
        logical_client = PhaseDLogicalStorageClient(
            session_id=arguments.session_id,
            actor_node_id=client.node_id,
            control_plane=control,
            transport=endpoint,
            acknowledgements=DurableAcknowledgementStore(
                _acknowledgements_database(arguments)
            ),
            catalog_cursor_secret=_catalog_cursor_secret(arguments),
        )
        recorder_authority = FederationLogicalStorageAuthority(
            client=client,
            logical_client=logical_client,
            session_id=arguments.session_id,
            authorize_ingest=_recorder_ingest_authorizer(coordinator),
            authorize_read=_federation_storage_read_authorizer(coordinator),
        )
        channel.set_recorder_ingest_handler(recorder_authority.handle_request)
        failover = StorageFailoverCoordinator(
            session_coordinator=coordinator,
            control_plane=control,
            publication_store=StorageControlPublicationStore(
                Path(arguments.publication_database)
            ),
            credentials=client.credentials,
            channel=channel,
            failover_store=LiveFailoverStore(
                Path(arguments.failover_database)
            ),
            session_id=arguments.session_id,
            lease_seconds=arguments.lease_seconds,
        )
        async def announce() -> None:
            announcement = _recorder_storage_capability(
                control,
                client,
                arguments.session_id,
            )
            await client.announce_capability(announcement)
            if on_announced is not None:
                on_announced(announcement)

        if command == "scan-once":
            await failover.start()
            results = await failover.scan_once()
            await announce()
            return [result.to_dict() for result in results]
        await failover.start()
        while stop is None or not stop.is_set():
            await failover.scan_once()
            # Groups, grants and failover state can change after startup. Keep
            # discovery truthful so recorder publishers and federation readers
            # do not remain stuck on a stale unavailable/ready announcement.
            await announce()
            if stop is None:
                await asyncio.sleep(arguments.scan_interval)
                continue
            # A supervised host must be able to stop between scans rather than
            # after a full interval, so shutdown is not delayed by the cadence.
            try:
                await asyncio.wait_for(stop.wait(), arguments.scan_interval)
            except asyncio.TimeoutError:
                continue
        return []
    finally:
        if failover is not None:
            await failover.close()
        elif endpoint is not None:
            await endpoint.close()
        if client is not None and client.connected_event.is_set():
            await client.disconnect()


async def run_storage_authority(
    settings: StorageAuthoritySettings,
    *,
    stop: asyncio.Event | None = None,
    on_announced: Callable[[CapabilityAnnouncement], None] | None = None,
) -> None:
    """Run the supervision loop until ``stop`` is set.

    This is the seam a supervised in-process host uses instead of spawning the
    CLI, so both paths compose the authority exactly once.
    """

    await _run(settings, command="run", stop=stop, on_announced=on_announced)


async def scan_storage_authority_once(
    settings: StorageAuthoritySettings,
) -> list[dict[str, object]]:
    """Compose the authority, scan once, announce, and shut down."""

    return await _run(settings, command="scan-once")


def main() -> int:
    arguments = _parser().parse_args()
    try:
        results = asyncio.run(
            _run(
                StorageAuthoritySettings.from_namespace(arguments),
                command=arguments.command,
            )
        )
        if arguments.command == "scan-once":
            print(json.dumps(results, ensure_ascii=False, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        return 130
    except (FederationValidationError, TimeoutError, OSError) as exc:
        if isinstance(exc, FederationValidationError):
            error = {
                "code": exc.code,
                "field": exc.field,
                "message": exc.message,
            }
        else:
            error = {
                "code": "storage-failover-runtime-error",
                "field": "runtime",
                "message": str(exc) or type(exc).__name__,
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "StorageAuthoritySettings",
    "main",
    "run_storage_authority",
    "scan_storage_authority_once",
]

"""Run the relay-aware automatic storage failover coordinator."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
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
    RecorderLogicalStorageAuthority,
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
    STORAGE_CONTROL_CAPABILITY_VERSION,
)
from catalog.federation.relay_storage import RelayStorageEndpoint

from .client import RelayNodeClient
from .state import EnrollmentState
from .storage_agent import (
    DEFAULT_ENROLLMENT_TOKEN_ENV,
    DEFAULT_SESSION_INVITATION_ENV,
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
    arguments: argparse.Namespace,
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


def _acknowledgements_database(arguments: argparse.Namespace) -> Path:
    if arguments.acknowledgements_database:
        return Path(arguments.acknowledgements_database)
    return Path(arguments.storage_control_database).with_name(
        "recorder_ingest_acknowledgements.sqlite3"
    )


async def _run(arguments: argparse.Namespace) -> int:
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
        )
        recorder_authority = RecorderLogicalStorageAuthority(
            client=client,
            logical_client=logical_client,
            session_id=arguments.session_id,
        )
        channel.set_recorder_ingest_handler(recorder_authority.handle_request)
        failover = StorageFailoverCoordinator(
            session_coordinator=SessionCoordinator(
                Path(arguments.relay_control_database)
            ),
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
        await client.announce_capability(
            _recorder_storage_capability(
                control,
                client,
                arguments.session_id,
            )
        )
        if arguments.command == "scan-once":
            await failover.start()
            results = await failover.scan_once()
            print(
                json.dumps(
                    [result.to_dict() for result in results],
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        await failover.run_forever(scan_interval=arguments.scan_interval)
        return 0
    finally:
        if failover is not None:
            await failover.close()
        elif endpoint is not None:
            await endpoint.close()
        if client is not None and client.connected_event.is_set():
            await client.disconnect()


def main() -> int:
    arguments = _parser().parse_args()
    try:
        return asyncio.run(_run(arguments))
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

"""Run one safe F4.2 former-primary replica reinstatement over the relay."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.live_catchup import (
    LiveCatchupStore,
    LiveFormerPrimaryCatchupCoordinator,
)
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageControlRelayChannel,
)
from catalog.federation.live_reinstatement import (
    LiveFormerPrimaryReinstatementCoordinator,
)
from catalog.federation.phase_d_control import PhaseDControlPlane
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
    parser.add_argument("--failover-database", required=True)
    parser.add_argument("--catchup-database", required=True)
    parser.add_argument("--publication-database", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--relay", required=True)
    parser.add_argument("--display-name", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--returning-provider-id", required=True)
    parser.add_argument("--catchup-limit", type=int, default=100)
    parser.add_argument("--allow-insecure-local", action="store_true")
    parser.add_argument("--heartbeat-interval", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=15.0)
    return parser


async def _connect(arguments: argparse.Namespace) -> RelayNodeClient:
    client = RelayNodeClient(
        state_directory=Path(arguments.state_dir),
        relay_url=arguments.relay,
        display_name=arguments.display_name,
        allow_insecure_local=arguments.allow_insecure_local,
        heartbeat_interval=arguments.heartbeat_interval,
        request_timeout=arguments.request_timeout,
    )
    enrollment = client.state.status()["enrollment_state"]
    token = (
        os.environ.get(DEFAULT_ENROLLMENT_TOKEN_ENV)
        if enrollment == EnrollmentState.UNENROLLED.value
        else None
    )
    if enrollment == EnrollmentState.UNENROLLED.value and not token:
        raise FederationValidationError(
            "storage-reinstatement-enrollment-required",
            DEFAULT_ENROLLMENT_TOKEN_ENV,
            "first startup requires a protected enrollment token",
        )
    await client.connect(enrollment_token=token)
    joined = {item.session_id for item in client.state.joined_sessions()}
    if arguments.session_id not in joined:
        invitation = os.environ.get(DEFAULT_SESSION_INVITATION_ENV)
        if not invitation:
            await client.disconnect(error_code="storage-reinstatement-session-required")
            raise FederationValidationError(
                "storage-reinstatement-session-invitation-required",
                DEFAULT_SESSION_INVITATION_ENV,
                "first session join requires a protected invitation token",
            )
        session = await client.join_session(invitation)
        if session.get("session_id") != arguments.session_id:
            await client.disconnect(
                error_code="storage-reinstatement-session-mismatch"
            )
            raise FederationValidationError(
                "storage-reinstatement-session-mismatch",
                "session_id",
                "the invitation joined another session",
            )
    return client


async def _run(arguments: argparse.Namespace) -> int:
    client: RelayNodeClient | None = None
    endpoint: RelayStorageEndpoint | None = None
    channel: StorageControlRelayChannel | None = None
    try:
        client = await _connect(arguments)
        endpoint = RelayStorageEndpoint(
            client,
            request_timeout=arguments.request_timeout,
        )
        await endpoint.start()
        channel = StorageControlRelayChannel(
            client,
            endpoint,
            timeout=arguments.request_timeout,
        )
        await channel.start()

        control = PhaseDControlPlane(Path(arguments.storage_control_database))
        failover_store = LiveFailoverStore(Path(arguments.failover_database))
        catchup_store = LiveCatchupStore(Path(arguments.catchup_database))
        catchup = LiveFormerPrimaryCatchupCoordinator(
            session_coordinator=SessionCoordinator(
                Path(arguments.relay_control_database)
            ),
            control_plane=control,
            failover_store=failover_store,
            transport=endpoint,
            credentials=client.credentials,
            catchup_store=catchup_store,
            session_id=arguments.session_id,
        )
        reinstatement = LiveFormerPrimaryReinstatementCoordinator(
            control_plane=control,
            publication_store=StorageControlPublicationStore(
                Path(arguments.publication_database)
            ),
            failover_store=failover_store,
            catchup_store=catchup_store,
            catchup=catchup,
            channel=channel,
            credentials=client.credentials,
            session_id=arguments.session_id,
        )
        result = await reinstatement.run_once(
            group_id=arguments.group_id,
            returning_provider_id=arguments.returning_provider_id,
            catchup_limit=arguments.catchup_limit,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
        return 0 if result.status in {"completed", "retryable"} else 2
    finally:
        if channel is not None:
            await channel.close()
        if endpoint is not None:
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
                "code": "storage-reinstatement-runtime-error",
                "field": "runtime",
                "message": str(exc) or type(exc).__name__,
            }
        print(json.dumps({"error": error}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

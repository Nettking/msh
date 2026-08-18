"""Creator-owned logical-storage authority over the reviewed trusted relay path.

The generic :class:`RelayNodeClient` deliberately rejects non-loopback plaintext
WebSockets.  Physical FCP pairing already has a narrower trusted-network client
for the private LAN/Tailscale deployment, where the outer private transport is
responsible for reachability confidentiality and the relay still authenticates
every node cryptographically.

The full workbench storage authority uses that same reviewed client rather than
weakening the generic node transport policy.  Only the immutable Federation
creator reaches this runtime; the lifecycle monitor fences non-creators before
starting it.

The creator also owns the local coordinator database, so its storage authority
can reconcile its own relay client state without operator-supplied enrollment
or invitation secrets.  A token is minted only when the coordinator does not
already know the creator identity, and an invitation is minted only when the
creator is not already a member of the target session.  Both remain the existing
bounded one-use primitives and are consumed immediately in memory.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthenticationError, AuthorizationError
from catalog.federation.live_failover import LiveFailoverStore, StorageFailoverCoordinator
from catalog.federation.models import CapabilityAnnouncement
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.recorder_storage_relay import RecorderAwareStorageControlRelayChannel
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.shared_file_storage import FederationLogicalStorageAuthority
from catalog.node.storage_failover import (
    StorageAuthoritySettings,
    _acknowledgements_database,
    _catalog_cursor_secret,
    _federation_storage_read_authorizer,
    _recorder_ingest_authorizer,
    _recorder_storage_capability,
)

from .federation_pairing_service import PairingRelayNodeClient

_BOOTSTRAP_TTL_SECONDS = 300


def _local_pairing_material(
    coordinator: SessionCoordinator,
    *,
    node_id: str,
    session_id: str,
) -> tuple[str | None, str | None]:
    """Issue only the local creator material that is genuinely still missing."""

    existing = coordinator.store.get_node(node_id)
    if existing is not None and existing.get("revoked_at") is not None:
        raise AuthenticationError(
            "revoked-node",
            "the creator identity is revoked and cannot run storage authority",
            "node_id",
        )

    enrollment_token: str | None = None
    if existing is None:
        enrollment = coordinator.create_enrollment_token(
            ttl_seconds=_BOOTSTRAP_TTL_SECONDS,
            max_uses=1,
        )
        enrollment_token = str(enrollment["token"])

    invitation_token: str | None = None
    try:
        coordinator.store.require_membership(session_id=session_id, node_id=node_id)
    except AuthorizationError:
        invitation = coordinator.create_invitation(
            session_id=session_id,
            actor_node_id=node_id,
            ttl_seconds=_BOOTSTRAP_TTL_SECONDS,
            max_uses=1,
            request_id=f"storage-authority-self-join-{node_id}",
        )
        invitation_token = str(invitation["token"])

    return enrollment_token, invitation_token


async def _connect_creator(
    settings: StorageAuthoritySettings,
) -> PairingRelayNodeClient:
    client = PairingRelayNodeClient(
        state_directory=Path(settings.state_dir),
        relay_url=settings.relay,
        display_name=settings.display_name,
        heartbeat_interval=settings.heartbeat_interval,
        request_timeout=settings.request_timeout,
    )
    coordinator = SessionCoordinator(Path(settings.relay_control_database))
    enrollment_token, invitation_token = _local_pairing_material(
        coordinator,
        node_id=client.node_id,
        session_id=settings.session_id,
    )

    await client.connect(enrollment_token=enrollment_token)
    joined = {item.session_id for item in client.state.joined_sessions()}
    if settings.session_id not in joined:
        if invitation_token is None:
            # The coordinator says the creator is already a member.  A correct
            # authenticated status reconciliation must therefore have restored
            # the local joined-session state.  Never fabricate it locally.
            await client.disconnect(error_code="storage-authority-session-state-mismatch")
            raise AuthorizationError(
                "storage-authority-session-state-mismatch",
                "the relay did not restore the creator's existing Federation membership",
                "session_id",
            )
        joined_session = await client.join_session(invitation_token)
        if joined_session.get("session_id") != settings.session_id:
            await client.disconnect(error_code="storage-authority-session-mismatch")
            raise AuthorizationError(
                "storage-authority-session-mismatch",
                "the creator storage authority joined a different Federation session",
                "session_id",
            )
    return client


async def run_trusted_storage_authority(
    settings: StorageAuthoritySettings,
    *,
    stop: asyncio.Event | None = None,
    on_announced: Callable[[CapabilityAnnouncement], None] | None = None,
) -> None:
    """Run the existing storage composition with creator self-bootstrap only."""

    client: PairingRelayNodeClient | None = None
    endpoint: RelayStorageEndpoint | None = None
    failover: StorageFailoverCoordinator | None = None
    try:
        client = await _connect_creator(settings)
        endpoint = RelayStorageEndpoint(client, request_timeout=settings.request_timeout)
        await endpoint.start()
        control = PhaseDControlPlane(Path(settings.storage_control_database))
        coordinator = SessionCoordinator(Path(settings.relay_control_database))
        channel = RecorderAwareStorageControlRelayChannel(
            client,
            endpoint,
            timeout=settings.request_timeout,
        )
        logical_client = PhaseDLogicalStorageClient(
            session_id=settings.session_id,
            actor_node_id=client.node_id,
            control_plane=control,
            transport=endpoint,
            acknowledgements=DurableAcknowledgementStore(
                _acknowledgements_database(settings)
            ),
            catalog_cursor_secret=_catalog_cursor_secret(settings),
        )
        recorder_authority = FederationLogicalStorageAuthority(
            client=client,
            logical_client=logical_client,
            session_id=settings.session_id,
            authorize_ingest=_recorder_ingest_authorizer(coordinator),
            authorize_read=_federation_storage_read_authorizer(coordinator),
        )
        channel.set_recorder_ingest_handler(recorder_authority.handle_request)
        failover = StorageFailoverCoordinator(
            session_coordinator=coordinator,
            control_plane=control,
            publication_store=StorageControlPublicationStore(
                Path(settings.publication_database)
            ),
            credentials=client.credentials,
            channel=channel,
            failover_store=LiveFailoverStore(Path(settings.failover_database)),
            session_id=settings.session_id,
            lease_seconds=settings.lease_seconds,
        )

        async def announce() -> None:
            announcement = _recorder_storage_capability(
                control,
                client,
                settings.session_id,
            )
            await client.announce_capability(announcement)
            if on_announced is not None:
                on_announced(announcement)

        await failover.start()
        while stop is None or not stop.is_set():
            await failover.scan_once()
            await announce()
            if stop is None:
                await asyncio.sleep(settings.scan_interval)
                continue
            try:
                await asyncio.wait_for(stop.wait(), settings.scan_interval)
            except asyncio.TimeoutError:
                continue
    finally:
        if failover is not None:
            await failover.close()
        elif endpoint is not None:
            await endpoint.close()
        if client is not None and client.connected_event.is_set():
            await client.disconnect()


__all__ = ["run_trusted_storage_authority"]

"""Creator-owned logical-storage authority over the reviewed trusted relay path.

The generic :class:`RelayNodeClient` deliberately rejects non-loopback plaintext
WebSockets. Physical FCP pairing already has a narrower trusted-network client
for the private LAN/Tailscale deployment, where the outer private transport is
responsible for reachability confidentiality and the relay still authenticates
every node cryptographically.

A full FCP device has exactly one authenticated relay connection for its node
identity. The storage authority therefore borrows that existing connection when
it is supervised by Flask instead of opening a second connection with the same
identity. Product messages are composed behind the existing single-reader relay
chain, so storage ingress does not race remote-AI/analysis traffic for frames.

The older self-bootstrap path remains available for direct runtime tests and
standalone composition. It is never used by the full-workbench supervisor.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.control_sync import StorageControlPublicationStore
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
)
from catalog.federation.live_failover import (
    STORAGE_CONTROL_REFRESH_MESSAGE,
    STORAGE_CONTROL_RELAY_KIND,
    STORAGE_FAILOVER_RELAY_KIND,
    LiveFailoverStore,
    StorageFailoverCoordinator,
)
from catalog.federation.models import CapabilityAnnouncement
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.recorder_storage_relay import (
    RECORDER_LOGICAL_STORAGE_KIND,
    RecorderAwareStorageControlRelayChannel,
)
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
_SHARED_OTHER_MESSAGE_LIMIT = 64


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


def _require_borrowed_creator(
    client: PairingRelayNodeClient,
    settings: StorageAuthoritySettings,
) -> None:
    if not client.connected_event.is_set():
        raise FederationOperationError(
            "storage-authority-shared-relay-disconnected",
            "the creator's shared Federation relay connection is not connected",
            "connection",
        )
    joined = {item.session_id for item in client.state.joined_sessions()}
    if settings.session_id not in joined:
        raise AuthorizationError(
            "storage-authority-session-state-mismatch",
            "the shared creator relay client is not joined to this Federation session",
            "session_id",
        )


class SharedRecorderAwareStorageControlRelayChannel(
    RecorderAwareStorageControlRelayChannel
):
    """Storage-control stage that forwards unrelated shared-client messages.

    The original storage-control channel is intentionally terminal because the
    standalone storage authority owns its entire relay application queue. A full
    workbench has additional consumers behind storage, so this composition keeps
    the same storage handling while forwarding every unrelated frame downstream.
    """

    def __init__(
        self,
        client: Any,
        endpoint: Any,
        *,
        timeout: float = 15.0,
        other_message_limit: int = _SHARED_OTHER_MESSAGE_LIMIT,
    ) -> None:
        super().__init__(client, endpoint, timeout=timeout)
        self._shared_other_messages: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=other_message_limit
        )

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._shared_other_messages.get()
        return await asyncio.wait_for(self._shared_other_messages.get(), timeout=timeout)

    def _forward_other(self, message: Any) -> None:
        try:
            self._shared_other_messages.put_nowait(message)
        except asyncio.QueueFull as exc:
            raise FederationOperationError(
                "storage-authority-downstream-full",
                "the bounded downstream Federation message queue is full",
            ) from exc

    async def _receiver_loop(self) -> None:
        while True:
            message = await self.endpoint.receive_other()
            payload = getattr(message, "payload", None)
            if not isinstance(payload, dict):
                self._forward_other(message)
                continue
            kind = payload.get("kind")
            message_kind = payload.get("message")
            if kind == STORAGE_CONTROL_RELAY_KIND:
                if message_kind == "response":
                    self._accept_response(message, payload)
                elif message_kind == STORAGE_CONTROL_REFRESH_MESSAGE:
                    self._accept_refresh(message, payload)
                continue
            if (
                kind == STORAGE_FAILOVER_RELAY_KIND
                and message_kind == "report-response"
            ):
                self._accept_report(message, payload)
                continue
            if kind == RECORDER_LOGICAL_STORAGE_KIND and message_kind == "request":
                self._accept_recorder_request(message, payload)
                continue
            self._forward_other(message)


async def run_trusted_storage_authority(
    settings: StorageAuthoritySettings,
    *,
    stop: asyncio.Event | None = None,
    on_announced: Callable[[CapabilityAnnouncement], None] | None = None,
    client: PairingRelayNodeClient | None = None,
    message_source: object | None = None,
    on_message_source: Callable[[object], None] | None = None,
) -> None:
    """Run logical storage over either an owned or a borrowed creator client.

    Supplying ``client`` is the full-workbench path: the already-authenticated
    creator connection is reused and is never disconnected here. ``message_source``
    must be the upstream single-reader stage for that shared connection. Without
    a supplied client the legacy direct self-bootstrap path remains unchanged.
    """

    owns_client = client is None
    endpoint: RelayStorageEndpoint | None = None
    failover: StorageFailoverCoordinator | None = None
    try:
        if client is None:
            client = await _connect_creator(settings)
        else:
            _require_borrowed_creator(client, settings)

        endpoint = RelayStorageEndpoint(
            client,
            request_timeout=settings.request_timeout,
            message_source=message_source,  # type: ignore[arg-type]
        )
        await endpoint.start()
        control = PhaseDControlPlane(Path(settings.storage_control_database))
        coordinator = SessionCoordinator(Path(settings.relay_control_database))
        channel_type = (
            SharedRecorderAwareStorageControlRelayChannel
            if message_source is not None
            else RecorderAwareStorageControlRelayChannel
        )
        channel = channel_type(
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
        if on_message_source is not None:
            on_message_source(channel)
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
        if endpoint is not None:
            await endpoint.close()
        if (
            owns_client
            and client is not None
            and client.connected_event.is_set()
        ):
            await client.disconnect()


__all__ = [
    "SharedRecorderAwareStorageControlRelayChannel",
    "run_trusted_storage_authority",
]

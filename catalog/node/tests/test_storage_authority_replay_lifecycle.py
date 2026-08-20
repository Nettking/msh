"""Relay lifecycle of the creator's logical-storage authority.

Physical acceptance testing pinned the recorder at ``authority-unavailable``
while the leader's ``fcp-federation-storage-authority`` thread died repeatedly
with ``asyncio.CancelledError`` raised out of ``RelayNodeClient.request_replay``.

The shared replay pass is deliberately one task per session, awaited through
``asyncio.shield`` so one caller going away cannot abandon a bounded replay that
other callers still need. ``disconnect`` cancels that task, and ``shield``
reports a cancelled inner task as a bare ``CancelledError`` — indistinguishable
from the caller's own cancellation, and a ``BaseException`` that walks straight
through every ``except Exception`` supervision boundary.

These tests run the real relay server so an ordinary connection teardown is
produced the way the physical rig produces it: a second authenticated
connection for the same node identity replaces the first.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection, connect

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.protocol import MAX_MESSAGE_BYTES, RelayEnvelope, utc_now
from catalog.federation.recorder_storage_relay import (
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
    STORAGE_CONTROL_CAPABILITY_VERSION,
)
from catalog.mtconnect_recorder.federation_node import select_storage_authority
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import NodeCredentials
from catalog.node.state import ConnectionState
from catalog.relay.authentication import authentication_message
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
REQUEST_TIMEOUT_SECONDS = 3.0
STORAGE_GROUP = "fcp-local-storage"


async def _start_relay(database: Path) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: NOW),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        send_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def _client(
    state_directory: Path, relay: RelayServer, display_name: str
) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=state_directory,
        relay_url=relay.url,
        display_name=display_name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=REQUEST_TIMEOUT_SECONDS,
        clock=lambda: NOW,
    )


async def _enroll(relay: RelayServer, client: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60, max_uses=1
    )["token"]
    await client.connect(enrollment_token=token)


async def _replace_connection(
    relay: RelayServer, credentials: NodeCredentials
) -> ClientConnection:
    """Authenticate a second connection for one identity, as the relay allows.

    ``RelayServer._register_connection`` closes the previous connection for the
    same node before it acknowledges this one, so awaiting ``auth.accepted``
    means the earlier connection has already been closed.
    """

    websocket = await connect(
        relay.url,
        proxy=None,
        open_timeout=REQUEST_TIMEOUT_SECONDS,
        close_timeout=REQUEST_TIMEOUT_SECONDS,
        max_size=MAX_MESSAGE_BYTES,
        compression=None,
    )
    challenge = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS)
    )
    assert challenge.message_type == "auth.challenge"
    nonce = str(challenge.payload["nonce"])
    signed = authentication_message(
        challenge_id=challenge.request_id,
        nonce=nonce,
        node_id=credentials.identity.node_id,
        protocol_version=challenge.protocol_version,
    )
    await websocket.send(
        RelayEnvelope(
            request_id=challenge.request_id,
            actor_node_id=credentials.identity.node_id,
            message_type="auth.response",
            authorization_context={"kind": "node-identity-proof"},
            payload={"nonce": nonce, "signature": credentials.sign(signed)},
            sent_at=utc_now(),
        ).to_json()
    )
    accepted = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS)
    )
    assert accepted.message_type == "auth.accepted"
    return websocket


class _ReplayGate:
    """Hold one client's replay request open without touching the relay."""

    def __init__(self, client: RelayNodeClient) -> None:
        self.started = asyncio.Event()
        self._release = asyncio.Event()
        self._original = client.request

        async def gated(message_type: str, **kwargs: Any) -> dict[str, Any]:
            if message_type == "event.replay":
                self.started.set()
                await self._release.wait()
            return await self._original(message_type, **kwargs)

        client.request = gated  # type: ignore[method-assign]

    def release(self) -> None:
        self._release.set()


def _storage_authority_capability(
    client: RelayNodeClient, session_id: str
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id="logical-storage-authority",
        node_id=client.node_id,
        session_id=session_id,
        type=STORAGE_CONTROL_CAPABILITY_TYPE,
        protocol=STORAGE_CONTROL_CAPABILITY_PROTOCOL,
        protocol_version=STORAGE_CONTROL_CAPABILITY_VERSION,
        status=CapabilityStatus.READY,
        properties={
            "kind": "recorder-logical-storage-authority",
            "group_ids": [STORAGE_GROUP],
        },
        announced_at=NOW,
    )


async def _shutdown(
    relay: RelayServer | None,
    *clients: RelayNodeClient | None,
    sockets: tuple[ClientConnection | None, ...] = (),
) -> None:
    for socket in sockets:
        if socket is not None:
            await socket.close()
    active = [client for client in clients if client is not None]
    if active:
        await asyncio.gather(
            *(client.disconnect() for client in active),
            return_exceptions=True,
        )
    if relay is not None:
        await relay.stop()


def _recorder_visible_status(
    status: dict[str, Any], *, session_id: str, creator_node_id: str
) -> dict[str, Any]:
    """Mirror the launcher's recovery of the omitted immutable session creator."""

    restored = dict(status)
    restored["sessions"] = [
        {**value, "created_by_node_id": creator_node_id}
        if isinstance(value, dict) and value.get("session_id") == session_id
        else value
        for value in status["sessions"]
    ]
    return restored


def test_initial_connect_replay_reports_a_replaced_connection_as_a_relay_error(
    tmp_path: Path,
) -> None:
    """``connect`` must fail closed with a code, not a bare cancellation."""

    async def scenario() -> None:
        relay = await _start_relay(tmp_path / "relay.sqlite3")
        creator: RelayNodeClient | None = None
        replacement: ClientConnection | None = None
        gate: _ReplayGate | None = None
        try:
            creator = _client(tmp_path / "creator", relay, "Creator")
            await _enroll(relay, creator)
            session = await creator.create_session("Physical acceptance")
            session_id = str(session["session_id"])
            await creator.disconnect()

            gate = _ReplayGate(creator)
            connecting = asyncio.create_task(creator.connect())
            await asyncio.wait_for(gate.started.wait(), timeout=5)

            replacement = await _replace_connection(relay, creator.credentials)

            with pytest.raises(RelayRemoteError) as rejected:
                await asyncio.wait_for(connecting, timeout=5)

            assert isinstance(rejected.value, Exception)
            assert rejected.value.code == "connection-closed"
            status = creator.state.status()
            assert status["connection_state"] == ConnectionState.ERROR.value
            assert status["last_error_code"] == "initial-replay-failed"
            assert creator._replay_tasks == {}
            assert creator._gap_replay_tasks == {}
            assert session_id
        finally:
            if gate is not None:
                gate.release()
            await _shutdown(relay, creator, sockets=(replacement,))

    asyncio.run(scenario())


def test_capability_announce_replay_reports_a_replaced_connection(
    tmp_path: Path,
) -> None:
    """The announce loop must survive teardown as a retryable relay failure."""

    async def scenario() -> None:
        relay = await _start_relay(tmp_path / "relay.sqlite3")
        creator: RelayNodeClient | None = None
        replacement: ClientConnection | None = None
        gate: _ReplayGate | None = None
        try:
            creator = _client(tmp_path / "creator", relay, "Creator")
            await _enroll(relay, creator)
            session = await creator.create_session("Physical acceptance")
            session_id = str(session["session_id"])

            gate = _ReplayGate(creator)
            announcing = asyncio.create_task(
                creator.announce_capability(
                    _storage_authority_capability(creator, session_id)
                )
            )
            await asyncio.wait_for(gate.started.wait(), timeout=5)

            replacement = await _replace_connection(relay, creator.credentials)

            with pytest.raises(RelayRemoteError) as rejected:
                await asyncio.wait_for(announcing, timeout=5)

            assert isinstance(rejected.value, Exception)
            assert rejected.value.code == "connection-closed"
            assert creator._replay_tasks == {}
        finally:
            if gate is not None:
                gate.release()
            await _shutdown(relay, creator, sockets=(replacement,))

    asyncio.run(scenario())


def test_the_authority_reconnects_and_the_recorder_then_selects_it(
    tmp_path: Path,
) -> None:
    """The acceptance path must complete after a teardown, not stay down."""

    async def scenario() -> None:
        relay = await _start_relay(tmp_path / "relay.sqlite3")
        creator: RelayNodeClient | None = None
        recorder: RelayNodeClient | None = None
        replacement: ClientConnection | None = None
        gate: _ReplayGate | None = None
        try:
            creator = _client(tmp_path / "creator", relay, "Creator")
            await _enroll(relay, creator)
            session = await creator.create_session("Physical acceptance")
            session_id = str(session["session_id"])
            invitation = await creator.create_invitation(
                session_id, ttl_seconds=60, max_uses=1
            )

            # A teardown while the shared replay is in flight.
            gate = _ReplayGate(creator)
            announcing = asyncio.create_task(
                creator.announce_capability(
                    _storage_authority_capability(creator, session_id)
                )
            )
            await asyncio.wait_for(gate.started.wait(), timeout=5)
            replacement = await _replace_connection(relay, creator.credentials)
            with pytest.raises(RelayRemoteError):
                await asyncio.wait_for(announcing, timeout=5)
            gate.release()
            await replacement.close()
            replacement = None

            # What the supervisor does next: reconnect and announce again.
            creator.request = RelayNodeClient.request.__get__(  # type: ignore[method-assign]
                creator, RelayNodeClient
            )
            await creator.connect()
            await creator.announce_capability(
                _storage_authority_capability(creator, session_id)
            )

            recorder = _client(tmp_path / "recorder", relay, "Recorder")
            await _enroll(relay, recorder)
            joined = await recorder.join_session(str(invitation["token"]))
            assert joined["session_id"] == session_id

            events, _current = await recorder.coordinator_replay_page(
                session_id=session_id,
                last_applied_revision=0,
                limit=1,
            )
            assert events[0].revision == 1
            assert events[0].event_type == "session.created"
            assert events[0].actor_node_id == creator.node_id

            selection = select_storage_authority(
                _recorder_visible_status(
                    await recorder.coordinator_status(),
                    session_id=session_id,
                    creator_node_id=creator.node_id,
                ),
                session_id=session_id,
                requested_group=None,
            )

            assert selection.state == "ready"
            assert selection.authority_node_id == creator.node_id
            assert selection.group_id == STORAGE_GROUP
        finally:
            if gate is not None:
                gate.release()
            await _shutdown(relay, creator, recorder, sockets=(replacement,))

    asyncio.run(scenario())


def test_a_replaced_connection_leaves_no_pending_replay_task(
    tmp_path: Path,
) -> None:
    """Nothing may still be pending when the authority's loop is closed."""

    async def scenario() -> None:
        relay = await _start_relay(tmp_path / "relay.sqlite3")
        creator: RelayNodeClient | None = None
        replacement: ClientConnection | None = None
        gate: _ReplayGate | None = None
        try:
            creator = _client(tmp_path / "creator", relay, "Creator")
            await _enroll(relay, creator)
            session = await creator.create_session("Physical acceptance")
            session_id = str(session["session_id"])

            gate = _ReplayGate(creator)
            waiting = asyncio.create_task(creator.request_replay(session_id))
            await asyncio.wait_for(gate.started.wait(), timeout=5)
            shared = creator._replay_tasks[session_id]

            replacement = await _replace_connection(relay, creator.credentials)
            with pytest.raises(RelayRemoteError):
                await asyncio.wait_for(waiting, timeout=5)

            assert shared.done()
            assert creator._replay_tasks == {}
            assert creator._gap_replay_tasks == {}
            assert creator._receiver_task is None
            assert creator._heartbeat_task is None
            assert creator._pending == {}
        finally:
            if gate is not None:
                gate.release()
            await _shutdown(relay, creator, sockets=(replacement,))

    asyncio.run(scenario())

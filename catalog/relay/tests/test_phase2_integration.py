from __future__ import annotations

import asyncio
import json
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import (
    CapabilityAnnouncement,
    CapabilityStatus,
)
from catalog.federation.persistence import STATUS_PAGE_MAX_BYTES
from catalog.federation.protocol import (
    MAX_MESSAGE_BYTES,
    RelayEnvelope,
    utc_now,
)
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import NodeCredentials
from catalog.node.state import NodeStateError, ReconnectPolicy
from catalog.relay.authentication import authentication_message
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)
REQUEST_TIMEOUT_SECONDS = 3.0


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
    state_directory: Path,
    relay: RelayServer,
    display_name: str,
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


async def _enroll(
    relay: RelayServer,
    client: RelayNodeClient,
) -> str:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await client.connect(enrollment_token=token)
    return token


async def _shared_session(
    owner: RelayNodeClient,
    member: RelayNodeClient,
    *,
    display_name: str = "Phase 2 integration",
) -> tuple[str, str]:
    session = await owner.create_session(display_name)
    session_id = str(session["session_id"])
    invitation = await owner.create_invitation(
        session_id,
        ttl_seconds=60,
        max_uses=1,
    )
    invitation_token = str(invitation["token"])
    joined = await member.join_session(invitation_token)
    assert joined["session_id"] == session_id
    # Joining creates an authoritative event. Synchronize the existing member
    # before later live fanout so a missing revision can never be masked.
    await owner.request_replay(session_id)
    return session_id, invitation_token


async def _shutdown(
    relay: RelayServer | None,
    *clients: RelayNodeClient | None,
) -> None:
    active = [client for client in clients if client is not None]
    if active:
        await asyncio.gather(
            *(client.disconnect() for client in active),
            return_exceptions=True,
        )
    if relay is not None:
        await relay.stop()


async def _open_authenticated_socket(
    relay: RelayServer,
    credentials: NodeCredentials,
) -> ClientConnection:
    websocket = await connect(
        relay.url,
        proxy=None,
        open_timeout=REQUEST_TIMEOUT_SECONDS,
        close_timeout=REQUEST_TIMEOUT_SECONDS,
        max_size=MAX_MESSAGE_BYTES,
        compression=None,
    )
    challenge = RelayEnvelope.from_json(
        await asyncio.wait_for(
            websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
        )
    )
    assert challenge.message_type == "auth.challenge"
    nonce = str(challenge.payload["nonce"])
    signed = authentication_message(
        challenge_id=challenge.request_id,
        nonce=nonce,
        node_id=credentials.identity.node_id,
        protocol_version=challenge.protocol_version,
    )
    response = RelayEnvelope(
        request_id=challenge.request_id,
        actor_node_id=credentials.identity.node_id,
        message_type="auth.response",
        authorization_context={"kind": "node-identity-proof"},
        payload={
            "nonce": nonce,
            "signature": credentials.sign(signed),
        },
        sent_at=utc_now(),
    )
    await websocket.send(response.to_json())
    accepted = RelayEnvelope.from_json(
        await asyncio.wait_for(
            websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
        )
    )
    assert accepted.message_type == "auth.accepted"
    return websocket


async def _inject_event(
    relay: RelayServer,
    target: RelayNodeClient,
    event: dict[str, Any],
    *,
    request_id: str,
) -> None:
    connection = await relay._connection_for(target.node_id)
    assert connection is not None
    await relay._send_live(
        connection,
        RelayEnvelope(
            request_id=request_id,
            session_id=str(event["session_id"]),
            actor_node_id=relay.coordinator.coordinator_id,
            target_node_id=target.node_id,
            message_type="event.replay",
            authorization_context={
                "kind": "validated-event-delivery",
                "validated_by": relay.coordinator.coordinator_id,
            },
            payload={"event": event},
            sent_at=utc_now(),
        ),
    )


def test_f2_001_and_f2_002_two_outbound_nodes_route_without_listeners(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        first: RelayNodeClient | None = None
        second: RelayNodeClient | None = None
        enrollment_tokens: tuple[str, str] | None = None
        invitation_token: str | None = None
        try:
            relay = await _start_relay(tmp_path / "relay" / "control.sqlite3")
            bind_attempts: list[object] = []

            def reject_node_listener_bind(
                _socket: socket.socket,
                address: object,
            ) -> None:
                bind_attempts.append(address)
                raise AssertionError("node agents must not bind listener sockets")

            # The relay is already listening. Any later Python socket bind
            # therefore belongs to node-side work and must fail this scenario.
            monkeypatch.setattr(socket.socket, "bind", reject_node_listener_bind)
            first = _client(tmp_path / "node-a", relay, "Node A")
            second = _client(tmp_path / "node-b", relay, "Node B")
            assert first.node_id != second.node_id

            enrollment_tokens = (
                await _enroll(relay, first),
                await _enroll(relay, second),
            )
            session_id, invitation_token = await _shared_session(
                first, second
            )

            with pytest.raises(RelayRemoteError) as secret_payload:
                await first.send_message(
                    session_id=session_id,
                    target_node_id=second.node_id,
                    payload={"label": invitation_token},
                )
            assert secret_payload.value.code == "nonpublic-payload"

            first_ack = await first.send_message(
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"text": "from node A"},
            )
            assert first_ack["delivered"] is True
            routed_to_second = await second.receive_message(
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert routed_to_second.payload == {"text": "from node A"}
            assert routed_to_second.actor_node_id == first.node_id
            assert routed_to_second.authorization_context["kind"] == (
                "validated-session-membership"
            )

            second_ack = await second.send_message(
                session_id=session_id,
                target_node_id=first.node_id,
                payload={"text": "from node B"},
            )
            assert second_ack["delivered"] is True
            routed_to_first = await first.receive_message(
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert routed_to_first.payload == {"text": "from node B"}

            assert relay.bound_port > 0
            assert relay._server is not None
            assert relay._server.sockets
            assert first.inbound_listener_ports == ()
            assert second.inbound_listener_ports == ()
            assert bind_attempts == []

            public_status = json.dumps(
                {
                    "first": await first.status(),
                    "second": await second.status(),
                },
                sort_keys=True,
            )
            lowered = public_status.lower()
            assert "private key" not in lowered
            assert "private_key" not in lowered
            assert "signature" not in lowered
            for secret in (*enrollment_tokens, invitation_token):
                assert secret not in public_status
                assert secret not in caplog.text
        finally:
            await _shutdown(relay, first, second)

    asyncio.run(scenario())


def test_f2_003_reconnect_replays_41_through_43_and_survives_restarts(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "relay" / "control.sqlite3"
        first_state = tmp_path / "node-a"
        second_state = tmp_path / "node-b"
        relay: RelayServer | None = None
        first: RelayNodeClient | None = None
        second: RelayNodeClient | None = None
        restarted_first: RelayNodeClient | None = None
        restarted_second: RelayNodeClient | None = None
        try:
            relay = await _start_relay(database)
            first = _client(first_state, relay, "Node A")
            second = _client(second_state, relay, "Node B")
            await _enroll(relay, first)
            await _enroll(relay, second)
            session_id, _ = await _shared_session(first, second)

            for revision in range(4, 41):
                event = await first.append_event(
                    session_id=session_id,
                    event_type="test.revision",
                    payload={"expected_revision": revision},
                    request_id=f"append-{revision}",
                )
                assert event.revision == revision
            await second.request_replay(session_id)
            assert second.state.last_applied_revision(session_id) == 40
            await second.disconnect()

            expected_missing: list[tuple[int, str]] = []
            for revision in range(41, 44):
                event = await first.append_event(
                    session_id=session_id,
                    event_type="test.revision",
                    payload={"expected_revision": revision},
                    request_id=f"append-{revision}",
                )
                expected_missing.append((event.revision, event.event_id))

            second = _client(second_state, relay, "Node B")
            await second.connect()
            replayed = second.state.applied_events(
                session_id, after_revision=40
            )
            assert [(event.revision, event.event_id) for event in replayed] == (
                expected_missing
            )
            assert second.state.last_applied_revision(session_id) == 43
            assert second.state.get_session(session_id).replaying is False

            first_node_id = first.node_id
            second_node_id = second.node_id
            await first.disconnect()
            await second.disconnect()
            await relay.stop()
            relay = await _start_relay(database)

            restarted_first = _client(first_state, relay, "Node A")
            restarted_second = _client(second_state, relay, "Node B")
            assert restarted_first.node_id == first_node_id
            assert restarted_second.node_id == second_node_id
            await restarted_first.connect()
            await restarted_second.connect()
            assert restarted_first.state.last_applied_revision(session_id) == 43
            assert restarted_second.state.last_applied_revision(session_id) == 43

            await restarted_first.send_message(
                session_id=session_id,
                target_node_id=restarted_second.node_id,
                payload={"after": "coordinator restart"},
            )
            routed = await restarted_second.receive_message(
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert routed.payload == {"after": "coordinator restart"}
        finally:
            await _shutdown(
                relay,
                first,
                second,
                restarted_first,
                restarted_second,
            )

    asyncio.run(scenario())


def test_f2_010_run_forever_reconnects_and_replays_without_duplicates(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "relay" / "control.sqlite3"
        owner_state = tmp_path / "node-a"
        member_state = tmp_path / "node-b"
        relay: RelayServer | None = None
        restarted_relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        setup_member: RelayNodeClient | None = None
        reconnecting_member: RelayNodeClient | None = None
        run_task: asyncio.Task[None] | None = None
        retry_requested = asyncio.Event()
        retry_allowed = asyncio.Event()
        initial_replay_complete = asyncio.Event()
        reconnect_replay_complete = asyncio.Event()
        retry_delays: list[float] = []
        heartbeat_wait = asyncio.Event()
        try:
            relay = await _start_relay(database)
            owner = _client(owner_state, relay, "Node A")
            setup_member = _client(member_state, relay, "Node B")
            await _enroll(relay, owner)
            await _enroll(relay, setup_member)
            session_id, _ = await _shared_session(owner, setup_member)

            for revision in range(4, 41):
                event = await owner.append_event(
                    session_id=session_id,
                    event_type="test.reconnect",
                    payload={"expected_revision": revision},
                    request_id=f"reconnect-append-{revision}",
                )
                assert event.revision == revision
            await setup_member.request_replay(session_id)
            assert setup_member.state.last_applied_revision(session_id) == 40
            await setup_member.disconnect()

            async def wait_for_relay_restart(delay: float) -> None:
                if delay == 300:
                    await heartbeat_wait.wait()
                    return
                retry_delays.append(delay)
                retry_requested.set()
                await retry_allowed.wait()

            reconnecting_member = RelayNodeClient(
                state_directory=member_state,
                relay_url=relay.url,
                display_name="Node B",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=REQUEST_TIMEOUT_SECONDS,
                reconnect_policy=ReconnectPolicy(
                    initial_delay_seconds=0.25,
                    multiplier=2,
                    max_delay_seconds=1,
                    max_attempts=4,
                ),
                clock=lambda: NOW,
                sleep=wait_for_relay_restart,
            )
            original_request_replay = reconnecting_member.request_replay

            async def observe_replay(
                requested_session_id: str,
            ) -> dict[str, Any]:
                result = await original_request_replay(requested_session_id)
                revision = reconnecting_member.state.last_applied_revision(
                    requested_session_id
                )
                if revision == 40:
                    initial_replay_complete.set()
                if revision == 43:
                    reconnect_replay_complete.set()
                return result

            reconnecting_member.request_replay = observe_replay  # type: ignore[method-assign]
            run_task = asyncio.create_task(
                reconnecting_member.run_forever(),
                name="f2-010-node-reconnect",
            )
            await asyncio.wait_for(
                initial_replay_complete.wait(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            relay_port = relay.bound_port
            await relay.stop()
            await asyncio.wait_for(
                retry_requested.wait(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            restarted_relay = RelayServer(
                SessionCoordinator(database, clock=lambda: NOW),
                host="127.0.0.1",
                port=relay_port,
                auth_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                send_timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                heartbeat_timeout_seconds=300,
                sweep_interval_seconds=300,
            )
            for revision in range(41, 44):
                event, created = restarted_relay.coordinator.append_event(
                    session_id=session_id,
                    actor_node_id=owner.node_id,
                    request_id=f"reconnect-append-{revision}",
                    event_type="test.reconnect",
                    payload={"expected_revision": revision},
                )
                assert created is True
                assert event.revision == revision
            await restarted_relay.start()
            retry_allowed.set()

            await asyncio.wait_for(
                reconnect_replay_complete.wait(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            replayed = reconnecting_member.state.applied_events(
                session_id,
                after_revision=40,
            )
            assert [event.revision for event in replayed] == [41, 42, 43]
            assert len({event.event_id for event in replayed}) == 3
            assert (
                reconnecting_member.state.last_applied_revision(session_id)
                == 43
            )
            assert reconnecting_member.connected_event.is_set()
            assert retry_delays == [0.25]

            authoritative = restarted_relay.coordinator.replay(
                session_id=session_id,
                actor_node_id=owner.node_id,
                last_applied_revision=40,
            )
            assert [event.revision for event in authoritative] == [41, 42, 43]
            assert restarted_relay.coordinator.store.get_session(
                session_id
            ).revision == 43
        finally:
            if run_task is not None:
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
            await _shutdown(
                restarted_relay,
                owner,
                setup_member,
                reconnecting_member,
            )
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())


def test_f2_004_duplicate_event_and_request_are_applied_once(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        first: RelayNodeClient | None = None
        second: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            first = _client(tmp_path / "node-a", relay, "Node A")
            second = _client(tmp_path / "node-b", relay, "Node B")
            await _enroll(relay, first)
            await _enroll(relay, second)
            session_id, _ = await _shared_session(first, second)

            event = await first.append_event(
                session_id=session_id,
                event_type="test.duplicate",
                payload={"logical": "once"},
                request_id="append-duplicate-event",
            )
            await second.request_replay(session_id)
            event_value = event.to_dict()
            await _inject_event(
                relay,
                second,
                event_value,
                request_id="duplicate-delivery-one",
            )
            await _inject_event(
                relay,
                second,
                event_value,
                request_id="duplicate-delivery-two",
            )
            await second.request_replay(session_id)
            matching = [
                item
                for item in second.state.applied_events(session_id)
                if item.event_id == event.event_id
            ]
            assert len(matching) == 1
            assert second.state.last_applied_revision(session_id) == event.revision

            repeated_id = f"repeated-{uuid.uuid4().hex}"
            first_result = await first.send_message(
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"sequence": 1},
                request_id=repeated_id,
            )
            first_delivery = await second.receive_message(
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert first_result["delivered"] is True
            assert first_delivery.payload == {"sequence": 1}

            duplicate_result = await first.send_message(
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"sequence": 1},
                request_id=repeated_id,
            )
            assert duplicate_result["duplicate"] is True
            assert duplicate_result["delivered"] is False

            await first.send_message(
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"sequence": 2},
            )
            next_delivery = await second.receive_message(
                timeout=REQUEST_TIMEOUT_SECONDS
            )
            assert next_delivery.payload == {"sequence": 2}
        finally:
            await _shutdown(relay, first, second)

    asyncio.run(scenario())


def test_f2_005_revision_gap_stays_replaying_until_network_replay_recovers(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        first: RelayNodeClient | None = None
        second: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            first = _client(tmp_path / "node-a", relay, "Node A")
            second = _client(tmp_path / "node-b", relay, "Node B")
            await _enroll(relay, first)
            await _enroll(relay, second)
            session_id, _ = await _shared_session(first, second)

            for revision in range(4, 42):
                event, created = relay.coordinator.append_event(
                    session_id=session_id,
                    actor_node_id=first.node_id,
                    request_id=f"authority-{revision}",
                    event_type="test.gap",
                    payload={"expected_revision": revision},
                )
                assert created is True
                assert event.revision == revision
            await second.request_replay(session_id)
            assert second.state.last_applied_revision(session_id) == 41

            event_42, _ = relay.coordinator.append_event(
                session_id=session_id,
                actor_node_id=first.node_id,
                request_id="authority-42",
                event_type="test.gap",
                payload={"expected_revision": 42},
            )
            event_43, _ = relay.coordinator.append_event(
                session_id=session_id,
                actor_node_id=first.node_id,
                request_id="authority-43",
                event_type="test.gap",
                payload={"expected_revision": 43},
            )
            assert (event_42.revision, event_43.revision) == (42, 43)

            gap_detected = asyncio.Event()
            replay_allowed = asyncio.Event()
            replay_finished = asyncio.Event()
            original_request_replay = second.request_replay

            async def gated_replay(requested_session_id: str) -> dict[str, Any]:
                gap_detected.set()
                await replay_allowed.wait()
                result = await original_request_replay(requested_session_id)
                replay_finished.set()
                return result

            second.request_replay = gated_replay  # type: ignore[method-assign]
            await _inject_event(
                relay,
                second,
                event_43.to_dict(),
                request_id="inject-revision-43",
            )
            await asyncio.wait_for(
                gap_detected.wait(), timeout=REQUEST_TIMEOUT_SECONDS
            )
            session_state = second.state.get_session(session_id)
            assert session_state.last_applied_revision == 41
            assert session_state.replaying is True
            assert [
                event.revision
                for event in second.state.applied_events(
                    session_id, after_revision=41
                )
            ] == []

            replay_allowed.set()
            await asyncio.wait_for(
                replay_finished.wait(), timeout=REQUEST_TIMEOUT_SECONDS
            )
            recovered = second.state.applied_events(
                session_id, after_revision=41
            )
            assert [event.revision for event in recovered] == [42, 43]
            assert len({event.event_id for event in recovered}) == 2
            assert second.state.last_applied_revision(session_id) == 43
            assert second.state.get_session(session_id).replaying is False
        finally:
            await _shutdown(relay, first, second)

    asyncio.run(scenario())


def test_f2_006_revocation_rejects_traffic_and_reconnect_but_peer_survives(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        first: RelayNodeClient | None = None
        second: RelayNodeClient | None = None
        restarted_second: RelayNodeClient | None = None
        raw_socket: ClientConnection | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            first = _client(tmp_path / "node-a", relay, "Node A")
            second = _client(tmp_path / "node-b", relay, "Node B")
            await _enroll(relay, first)
            await _enroll(relay, second)
            session_id, _ = await _shared_session(first, second)
            second_credentials = second.credentials

            changed = relay.coordinator.revoke_node(
                node_id=second.node_id,
                reason="F2-006",
                request_id="revoke-node-b",
            )
            assert changed is True
            with pytest.raises(RelayRemoteError) as target_rejected:
                await first.send_message(
                    session_id=session_id,
                    target_node_id=second.node_id,
                    payload={"healthy": "sender"},
                )
            assert target_rejected.value.code == "target-unavailable"
            assert first.state.status()["enrollment_state"] == "enrolled"
            assert first.state.status()["connection_state"] == "connected"
            with pytest.raises(RelayRemoteError) as rejected:
                await second.send_message(
                    session_id=session_id,
                    target_node_id=first.node_id,
                    payload={"must": "be rejected"},
                )
            assert rejected.value.code == "revoked-node"
            await asyncio.wait_for(
                second.disconnected_event.wait(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            assert second.state.status()["enrollment_state"] == "revoked"

            heartbeat = await first.request("heartbeat", payload={})
            assert heartbeat["connection_state"] == "connected"
            audit = relay.coordinator.audit_entries()
            assert any(
                entry["action"] == "node.revoke"
                and entry["reason"] == "node-revoked"
                for entry in audit
            )
            assert any(
                entry["actor_node_id"] == second.node_id
                and entry["reason"] == "revoked-node"
                for entry in audit
            )

            restarted_second = _client(
                tmp_path / "node-b", relay, "Node B"
            )
            with pytest.raises(NodeStateError) as local_rejection:
                await restarted_second.connect()
            assert local_rejection.value.code == "node-revoked"

            raw_socket = await connect(
                relay.url,
                proxy=None,
                open_timeout=REQUEST_TIMEOUT_SECONDS,
                close_timeout=REQUEST_TIMEOUT_SECONDS,
                max_size=MAX_MESSAGE_BYTES,
                compression=None,
            )
            challenge = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    raw_socket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            nonce = str(challenge.payload["nonce"])
            proof = authentication_message(
                challenge_id=challenge.request_id,
                nonce=nonce,
                node_id=second_credentials.identity.node_id,
                protocol_version=challenge.protocol_version,
            )
            await raw_socket.send(
                RelayEnvelope(
                    request_id=challenge.request_id,
                    actor_node_id=second_credentials.identity.node_id,
                    message_type="auth.response",
                    authorization_context={"kind": "node-identity-proof"},
                    payload={
                        "nonce": nonce,
                        "signature": second_credentials.sign(proof),
                    },
                    sent_at=utc_now(),
                ).to_json()
            )
            reconnect_rejection = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    raw_socket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            assert reconnect_rejection.message_type == "relay.error"
            assert reconnect_rejection.payload["error"]["code"] == (
                "revoked-node"
            )
        finally:
            if raw_socket is not None:
                await raw_socket.close()
            await _shutdown(relay, first, second, restarted_second)

    asyncio.run(scenario())


def test_f2_007_enrolled_nonmember_cannot_route_into_session(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        nonmember: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            owner = _client(tmp_path / "node-owner", relay, "Owner")
            nonmember = _client(
                tmp_path / "node-nonmember", relay, "Nonmember"
            )
            await _enroll(relay, owner)
            await _enroll(relay, nonmember)
            session = await owner.create_session("Private session")
            session_id = str(session["session_id"])

            with pytest.raises(RelayRemoteError) as rejected:
                await nonmember.send_message(
                    session_id=session_id,
                    target_node_id=owner.node_id,
                    payload={"unauthorized": True},
                )
            assert rejected.value.code == "not-session-member"
            assert owner._inbound.empty()
            assert any(
                entry["actor_node_id"] == nonmember.node_id
                and entry["session_id"] == session_id
                and entry["reason"] == "not-session-member"
                for entry in relay.coordinator.audit_entries()
            )
        finally:
            await _shutdown(relay, owner, nonmember)

    asyncio.run(scenario())


def test_wrong_private_key_is_rejected_without_leaking_proof_or_token(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        enrolled: RelayNodeClient | None = None
        wrong_identity: RelayNodeClient | None = None
        websocket: ClientConnection | None = None
        enrollment_token: str | None = None
        signature: str | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            enrolled = _client(tmp_path / "node-a", relay, "Node A")
            wrong_identity = _client(
                tmp_path / "wrong-identity", relay, "Wrong key"
            )
            enrollment_token = await _enroll(relay, enrolled)
            await enrolled.disconnect()

            websocket = await connect(
                relay.url,
                proxy=None,
                open_timeout=REQUEST_TIMEOUT_SECONDS,
                close_timeout=REQUEST_TIMEOUT_SECONDS,
                max_size=MAX_MESSAGE_BYTES,
                compression=None,
            )
            challenge = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            nonce = str(challenge.payload["nonce"])
            proof = authentication_message(
                challenge_id=challenge.request_id,
                nonce=nonce,
                node_id=enrolled.node_id,
                protocol_version=challenge.protocol_version,
            )
            signature = wrong_identity.credentials.sign(proof)
            await websocket.send(
                RelayEnvelope(
                    request_id=challenge.request_id,
                    actor_node_id=enrolled.node_id,
                    message_type="auth.response",
                    authorization_context={"kind": "node-identity-proof"},
                    payload={"nonce": nonce, "signature": signature},
                    sent_at=utc_now(),
                ).to_json()
            )
            rejected = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            assert rejected.message_type == "relay.error"
            assert rejected.payload["error"]["code"] == (
                "invalid-authentication-signature"
            )
            response_text = rejected.to_json()
            assert signature not in response_text
            assert enrollment_token not in response_text
            assert signature not in caplog.text
            assert enrollment_token not in caplog.text
            assert any(
                entry["reason"] == "invalid-authentication-signature"
                and entry["actor_node_id"] == enrolled.node_id
                for entry in relay.coordinator.audit_entries()
            )
        finally:
            if websocket is not None:
                await websocket.close()
            await _shutdown(relay, enrolled, wrong_identity)

    asyncio.run(scenario())


def test_network_boundary_accepts_additive_fields_and_rejects_bad_frames(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        enrolled: RelayNodeClient | None = None
        websocket: ClientConnection | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            enrolled = _client(tmp_path / "node-a", relay, "Node A")
            await _enroll(relay, enrolled)
            credentials = enrolled.credentials
            await enrolled.disconnect()
            websocket = await _open_authenticated_socket(relay, credentials)

            additive = RelayEnvelope(
                request_id="additive-heartbeat",
                actor_node_id=enrolled.node_id,
                message_type="heartbeat",
                authorization_context={"kind": "authenticated-node"},
                payload={},
                sent_at=utc_now(),
                protocol_version="1.99",
            ).to_dict()
            additive["future_optional_field"] = {
                "safe_to_ignore": True,
            }
            await websocket.send(json.dumps(additive))
            accepted = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            assert accepted.message_type == "heartbeat.accepted"
            assert accepted.request_id == "additive-heartbeat"

            unsupported = RelayEnvelope(
                request_id="unsupported-major",
                actor_node_id=enrolled.node_id,
                message_type="heartbeat",
                authorization_context={"kind": "authenticated-node"},
                payload={},
                sent_at=utc_now(),
            ).to_dict()
            unsupported["protocol_version"] = "2.0"
            await websocket.send(json.dumps(unsupported))
            version_rejection = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            assert version_rejection.message_type == "relay.error"
            assert version_rejection.payload["error"]["code"] == (
                "unsupported-protocol-major"
            )

            await websocket.send("{")
            malformed_rejection = RelayEnvelope.from_json(
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            )
            assert malformed_rejection.message_type == "relay.error"
            assert malformed_rejection.payload["error"]["code"] == (
                "malformed-message"
            )

            await websocket.send("x" * (MAX_MESSAGE_BYTES + 1))
            with pytest.raises(ConnectionClosed) as oversized:
                await asyncio.wait_for(
                    websocket.recv(), timeout=REQUEST_TIMEOUT_SECONDS
                )
            assert oversized.value.rcvd is not None
            assert oversized.value.rcvd.code == 1009
            await websocket.wait_closed()
            await relay.stop()
            assert any(
                entry["reason"] == "message-too-large"
                for entry in relay.coordinator.audit_entries()
            )
        finally:
            if websocket is not None:
                await websocket.close()
            await _shutdown(relay, enrolled)

    asyncio.run(scenario())


def test_reconnect_reconciles_committed_membership_and_offline_removal(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        member: RelayNodeClient | None = None
        restarted_owner: RelayNodeClient | None = None
        restarted_member: RelayNodeClient | None = None
        removed_member: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3")
            owner_state = tmp_path / "node-owner"
            member_state = tmp_path / "node-member"
            owner = _client(owner_state, relay, "Owner")
            member = _client(member_state, relay, "Member")
            await _enroll(relay, owner)
            await _enroll(relay, member)

            session = relay.coordinator.create_session(
                actor_node_id=owner.node_id,
                display_name="Committed before local acknowledgement",
                request_id="lost-create-response",
            )
            invitation = relay.coordinator.create_invitation(
                session_id=session.session_id,
                actor_node_id=owner.node_id,
                ttl_seconds=60,
                max_uses=1,
            )
            relay.coordinator.join_session(
                node_id=member.node_id,
                token=str(invitation["token"]),
                request_id="lost-join-response",
            )
            assert owner.state.joined_sessions() == ()
            assert member.state.joined_sessions() == ()

            await owner.disconnect()
            await member.disconnect()
            restarted_owner = _client(owner_state, relay, "Owner")
            restarted_member = _client(member_state, relay, "Member")
            await restarted_owner.connect()
            await restarted_member.connect()

            assert [
                item.session_id
                for item in restarted_owner.state.joined_sessions()
            ] == [session.session_id]
            assert [
                item.session_id
                for item in restarted_member.state.joined_sessions()
            ] == [session.session_id]
            assert restarted_owner.state.last_applied_revision(
                session.session_id
            ) == 3
            assert restarted_member.state.last_applied_revision(
                session.session_id
            ) == 3

            await restarted_member.disconnect()
            relay.coordinator.remove_member(
                session_id=session.session_id,
                actor_node_id=restarted_owner.node_id,
                target_node_id=restarted_member.node_id,
                request_id="offline-member-removal",
                reason="offline reconciliation test",
            )
            removed_member = _client(member_state, relay, "Member")
            await removed_member.connect()

            assert removed_member.connected_event.is_set()
            assert removed_member.state.joined_sessions() == ()
            assert (
                removed_member.state.get_session(
                    session.session_id
                ).membership_state.value
                == "removed"
            )
        finally:
            await _shutdown(
                relay,
                owner,
                member,
                restarted_owner,
                restarted_member,
                removed_member,
            )

    asyncio.run(scenario())


def test_paginated_status_is_bounded_and_reconciliation_is_atomic(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        member: RelayNodeClient | None = None
        interrupted: RelayNodeClient | None = None
        recovered: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay-status.sqlite3")
            owner = _client(tmp_path / "status-owner", relay, "Status Owner")
            member_state = tmp_path / "status-member"
            member = _client(member_state, relay, "Status Member")
            await _enroll(relay, owner)
            await _enroll(relay, member)

            stale_session_id, _ = await _shared_session(
                owner,
                member,
                display_name="Locally stale membership",
            )
            remote_session_ids: list[str] = []
            for index in range(66):
                session = relay.coordinator.create_session(
                    actor_node_id=owner.node_id,
                    display_name=f"Paged status session {index:03d}",
                    request_id=f"status-create-{index:03d}",
                )
                remote_session_ids.append(session.session_id)
                invitation = relay.coordinator.create_invitation(
                    session_id=session.session_id,
                    actor_node_id=owner.node_id,
                    ttl_seconds=60,
                    max_uses=1,
                )
                relay.coordinator.join_session(
                    node_id=member.node_id,
                    token=str(invitation["token"]),
                    request_id=f"status-join-{index:03d}",
                )

            announcements = [
                CapabilityAnnouncement(
                    capability_id=f"large-status-{index}",
                    node_id=member.node_id,
                    session_id=remote_session_ids[0],
                    type="storage",
                    protocol="msh-storage-status-test",
                    protocol_version="1.0",
                    status=CapabilityStatus.READY,
                    properties={"description": character * 44_000},
                    announced_at=NOW,
                )
                for index, character in enumerate(("a", "b"))
            ]
            announcements.extend(
                CapabilityAnnouncement(
                    capability_id=f"small-status-{index:03d}",
                    node_id=member.node_id,
                    session_id=remote_session_ids[0],
                    type="storage",
                    protocol="msh-storage-status-test",
                    protocol_version="1.0",
                    status=CapabilityStatus.READY,
                    properties={"label": f"provider-{index:03d}"},
                    announced_at=NOW,
                )
                for index in range(65)
            )
            for index, announcement in enumerate(announcements):
                relay.coordinator.announce_capability(
                    announcement,
                    actor_node_id=member.node_id,
                    request_id=f"status-announce-{index:03d}",
                )

            pages: list[dict[str, Any]] = []
            cursor: str | None = None
            while True:
                page = await member.request(
                    "status.get",
                    payload={} if cursor is None else {"cursor": cursor},
                )
                pages.append(page)
                encoded = json.dumps(
                    page,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                assert len(encoded) <= STATUS_PAGE_MAX_BYTES
                pagination = page["pagination"]
                if not pagination["has_more"]:
                    break
                cursor = pagination["next_cursor"]
                assert isinstance(cursor, str)

            assert len(pages) >= 3
            assert sum(
                len(page["capabilities"]) for page in pages
            ) == len(announcements)
            aggregate = await member.coordinator_status()
            assert {
                item["session_id"] for item in aggregate["sessions"]
            } == {stale_session_id, *remote_session_ids}
            assert [
                item["capability_id"] for item in aggregate["capabilities"]
            ] == [
                item.capability_id
                for item in sorted(
                    announcements,
                    key=lambda item: (
                        item.session_id,
                        item.type,
                        item.node_id,
                        item.capability_id,
                    ),
                )
            ]
            assert "pagination" not in aggregate

            await member.disconnect()
            relay.coordinator.remove_member(
                session_id=stale_session_id,
                actor_node_id=owner.node_id,
                target_node_id=member.node_id,
                request_id="remove-stale-before-paged-reconnect",
                reason="verify paged reconciliation is atomic",
            )

            interrupted = _client(
                member_state,
                relay,
                "Status Member",
            )
            original_request = interrupted.request
            status_requests = 0

            async def fail_second_status_page(
                message_type: str,
                **kwargs: Any,
            ) -> dict[str, Any]:
                nonlocal status_requests
                if message_type == "status.get":
                    status_requests += 1
                    if status_requests == 2:
                        raise RelayRemoteError(
                            "injected-status-page-failure",
                            "test interrupted the second status page",
                        )
                return await original_request(message_type, **kwargs)

            interrupted.request = fail_second_status_page  # type: ignore[method-assign]
            with pytest.raises(RelayRemoteError) as failed:
                await interrupted.connect()
            assert failed.value.code == "injected-status-page-failure"
            assert status_requests == 2
            assert [
                item.session_id
                for item in interrupted.state.joined_sessions()
            ] == [stale_session_id]

            recovered = _client(member_state, relay, "Status Member")
            await recovered.connect()
            assert [
                item.session_id for item in recovered.state.joined_sessions()
            ] == sorted(remote_session_ids)
            assert (
                recovered.state.get_session(
                    stale_session_id
                ).membership_state.value
                == "removed"
            )
            assert all(
                recovered.state.last_applied_revision(session_id) > 0
                for session_id in remote_session_ids
            )
        finally:
            await _shutdown(
                relay,
                owner,
                member,
                interrupted,
                recovered,
            )

    asyncio.run(scenario())

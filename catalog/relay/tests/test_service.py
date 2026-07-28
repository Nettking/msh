from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.protocol import RelayEnvelope, utc_now
from catalog.node.identity import IdentityStore, NodeCredentials
from catalog.relay.authentication import authentication_message
from catalog.relay.service import RelayConfigurationError, RelayServer


@dataclass
class AuthenticatedClient:
    websocket: ClientConnection
    credentials: NodeCredentials

    @property
    def node_id(self) -> str:
        return self.credentials.identity.node_id

    async def send(
        self,
        message_type: str,
        *,
        request_id: str | None = None,
        session_id: str | None = None,
        target_node_id: str | None = None,
        payload: dict[str, object] | None = None,
        additive_field: bool = False,
    ) -> str:
        request_id = request_id or f"request-{uuid.uuid4().hex}"
        envelope = RelayEnvelope(
            request_id=request_id,
            session_id=session_id,
            actor_node_id=self.node_id,
            target_node_id=target_node_id,
            message_type=message_type,
            authorization_context={"kind": "node-request"},
            payload=payload or {},
            sent_at=utc_now(),
        )
        if additive_field:
            value = envelope.to_dict()
            value["future_optional"] = {"accepted": True}
            frame = json.dumps(value)
        else:
            frame = envelope.to_json()
        await self.websocket.send(frame)
        return request_id

    async def receive(self, message_type: str) -> RelayEnvelope:
        frame = await asyncio.wait_for(self.websocket.recv(), timeout=2)
        envelope = RelayEnvelope.from_json(frame)
        assert envelope.message_type == message_type
        return envelope

    async def close(self) -> None:
        await self.websocket.close()


def _credentials(path: Path, display_name: str) -> NodeCredentials:
    return IdentityStore(path, display_name=display_name).load_or_create()


async def _challenge(
    relay: RelayServer,
) -> tuple[ClientConnection, RelayEnvelope]:
    websocket = await connect(
        relay.url,
        max_size=65_536,
        compression=None,
        open_timeout=2,
        close_timeout=2,
    )
    challenge = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=2)
    )
    assert challenge.message_type == "auth.challenge"
    return websocket, challenge


async def _send_authentication(
    websocket: ClientConnection,
    challenge: RelayEnvelope,
    credentials: NodeCredentials,
    *,
    actor_node_id: str | None = None,
) -> None:
    actor_node_id = actor_node_id or credentials.identity.node_id
    nonce = str(challenge.payload["nonce"])
    signature = credentials.sign(
        authentication_message(
            challenge_id=challenge.request_id,
            nonce=nonce,
            node_id=actor_node_id,
            protocol_version=challenge.protocol_version,
        )
    )
    response = RelayEnvelope(
        request_id=challenge.request_id,
        actor_node_id=actor_node_id,
        message_type="auth.response",
        authorization_context={"kind": "connection-authentication"},
        payload={"nonce": nonce, "signature": signature},
        sent_at=utc_now(),
    )
    await websocket.send(response.to_json())


async def _enroll(
    relay: RelayServer,
    credentials: NodeCredentials,
    token: str,
) -> AuthenticatedClient:
    websocket, challenge = await _challenge(relay)
    enrollment = RelayEnvelope(
        request_id=f"enroll-{uuid.uuid4().hex}",
        actor_node_id=credentials.identity.node_id,
        message_type="node.enroll",
        authorization_context={"kind": "enrollment-token"},
        payload={"identity": credentials.identity.to_dict(), "token": token},
        sent_at=utc_now(),
    )
    await websocket.send(enrollment.to_json())
    proof_required = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=2)
    )
    assert proof_required.message_type == "node.enroll.proof-required"
    assert proof_required.payload["challenge_id"] == challenge.request_id
    await _send_authentication(websocket, challenge, credentials)
    accepted = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=2)
    )
    assert accepted.message_type == "node.enroll.accepted"
    assert accepted.payload["node_id"] == credentials.identity.node_id
    return AuthenticatedClient(websocket, credentials)


async def _authenticate(
    relay: RelayServer, credentials: NodeCredentials
) -> AuthenticatedClient:
    websocket, challenge = await _challenge(relay)
    await _send_authentication(websocket, challenge, credentials)
    accepted = RelayEnvelope.from_json(
        await asyncio.wait_for(websocket.recv(), timeout=2)
    )
    assert accepted.message_type == "auth.accepted"
    return AuthenticatedClient(websocket, credentials)


def test_relay_rejects_non_loopback_plaintext_by_default(tmp_path: Path) -> None:
    async def scenario() -> None:
        relay = RelayServer(
            SessionCoordinator(tmp_path / "relay.sqlite3"),
            host="0.0.0.0",
            port=0,
        )
        with pytest.raises(
            RelayConfigurationError, match="limited to loopback"
        ):
            await relay.start()

    asyncio.run(scenario())


def test_relay_rejects_nonfinite_or_unbounded_intervals(
    tmp_path: Path,
) -> None:
    coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
    for field, value in (
        ("auth_timeout_seconds", float("nan")),
        ("send_timeout_seconds", float("inf")),
        ("heartbeat_timeout_seconds", 3_601),
        ("sweep_interval_seconds", 0),
    ):
        with pytest.raises(RelayConfigurationError):
            RelayServer(coordinator, **{field: value})


def test_pre_auth_oversized_frame_is_closed_and_audited(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        websocket: ClientConnection | None = None
        try:
            websocket, _ = await _challenge(relay)
            await websocket.send("x" * 65_537)
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(websocket.recv(), timeout=2)
            await websocket.wait_closed()
            await relay.stop()
            assert any(
                entry["reason"] == "message-too-large"
                for entry in coordinator.audit_entries()
            )
        finally:
            if websocket is not None:
                await websocket.close()
            await relay.stop()

    asyncio.run(scenario())


def test_nonpublic_envelope_metadata_is_rejected_without_echo_or_persistence(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database = tmp_path / "relay.sqlite3"
        coordinator = SessionCoordinator(database)
        credentials = _credentials(tmp_path / "node-a", "Node A")
        enrollment_token = coordinator.create_enrollment_token()["token"]
        relay = RelayServer(coordinator, port=0, sweep_interval_seconds=60)
        await relay.start()
        client: AuthenticatedClient | None = None
        try:
            client = await _enroll(relay, credentials, enrollment_token)
            secret = coordinator.create_enrollment_token()["token"]
            base = RelayEnvelope(
                request_id="safe-heartbeat",
                actor_node_id=client.node_id,
                message_type="heartbeat",
                authorization_context={"kind": "node-request"},
                payload={},
                sent_at=utc_now(),
            ).to_dict()

            secret_request = {**base, "request_id": f"pasted-{secret}"}
            await client.websocket.send(json.dumps(secret_request))
            request_rejection = await client.receive("relay.error")
            assert request_rejection.payload["error"]["code"] == (
                "nonpublic-envelope-field"
            )
            assert secret not in request_rejection.to_json()

            secret_context = {
                **base,
                "request_id": "secret-context-heartbeat",
                "authorization_context": {
                    "kind": "node-request",
                    "note": f"pasted {secret}",
                },
            }
            await client.websocket.send(json.dumps(secret_context))
            context_rejection = await client.receive("relay.error")
            assert context_rejection.payload["error"]["code"] == (
                "nonpublic-authorization-context"
            )
            assert secret not in context_rejection.to_json()

            safe_context = RelayEnvelope(
                request_id="additive-context-heartbeat",
                actor_node_id=client.node_id,
                message_type="heartbeat",
                authorization_context={
                    "kind": "node-request",
                    "future_optional": {"label": "safe-additive"},
                },
                payload={},
                sent_at=utc_now(),
            )
            await client.websocket.send(safe_context.to_json())
            accepted = await client.receive("heartbeat.accepted")
            assert accepted.request_id == "additive-context-heartbeat"

            assert secret not in json.dumps(
                coordinator.audit_entries(),
                sort_keys=True,
            )
            database_bytes = b"".join(
                path.read_bytes()
                for path in (
                    database,
                    Path(f"{database}-wal"),
                    Path(f"{database}-shm"),
                )
                if path.exists()
            )
            assert secret.encode() not in database_bytes
        finally:
            if client is not None:
                await client.close()
            await relay.stop()

    asyncio.run(scenario())


def test_two_enrolled_nodes_join_and_route_with_deduplication(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        first_credentials = _credentials(tmp_path / "node-a", "Node A")
        second_credentials = _credentials(tmp_path / "node-b", "Node B")
        first_token = coordinator.create_enrollment_token()["token"]
        second_token = coordinator.create_enrollment_token()["token"]
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        first: AuthenticatedClient | None = None
        second: AuthenticatedClient | None = None
        try:
            first = await _enroll(relay, first_credentials, first_token)
            second = await _enroll(relay, second_credentials, second_token)

            await first.send(
                "session.create",
                payload={"display_name": "Relay test"},
            )
            created = await first.receive("session.create.accepted")
            session = created.payload["session"]
            assert isinstance(session, dict)
            session_id = str(session["session_id"])

            await first.send(
                "session.invite",
                session_id=session_id,
                payload={"ttl_seconds": 60, "max_uses": 1},
            )
            invitation = await first.receive("session.invite.accepted")

            await first.send(
                "session.create",
                payload={"display_name": "Different session"},
            )
            other_created = await first.receive("session.create.accepted")
            other_session = other_created.payload["session"]
            assert isinstance(other_session, dict)
            await second.send(
                "session.join",
                session_id=str(other_session["session_id"]),
                payload={"token": invitation.payload["token"]},
            )
            wrong_invitation_scope = await second.receive("relay.error")
            assert (
                wrong_invitation_scope.payload["error"]["code"]
                == "wrong-session-invitation"
            )

            await second.send(
                "session.join",
                session_id=session_id,
                payload={"token": invitation.payload["token"]},
            )
            joined = await second.receive("session.join.accepted")
            assert joined.session_id == session_id
            joined_event = await first.receive("event.replay")
            assert joined_event.payload["event"]["event_type"] == "node.joined"

            repeated_id = f"route-{uuid.uuid4().hex}"
            await first.send(
                "relay.message",
                request_id=repeated_id,
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"text": "hello"},
                additive_field=True,
            )
            routed = await second.receive("relay.message")
            assert routed.actor_node_id == first.node_id
            assert routed.authorization_context["kind"] == (
                "validated-session-membership"
            )
            delivered = await first.receive("relay.message.accepted")
            assert delivered.payload["delivered"] is True

            await first.send(
                "relay.message",
                request_id=repeated_id,
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"text": "hello"},
            )
            duplicate = await first.receive("relay.message.accepted")
            assert duplicate.payload == {
                "delivered": False,
                "duplicate": True,
                "target_node_id": second.node_id,
            }

            marker_id = await first.send(
                "relay.message",
                session_id=session_id,
                target_node_id=second.node_id,
                payload={"text": "marker"},
            )
            marker = await second.receive("relay.message")
            assert marker.request_id == marker_id
            await first.receive("relay.message.accepted")
        finally:
            if first is not None:
                await first.close()
            if second is not None:
                await second.close()
            await relay.stop()

    asyncio.run(scenario())


def test_wrong_session_and_revocation_are_rejected_without_affecting_peer(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        first_credentials = _credentials(tmp_path / "node-a", "Node A")
        second_credentials = _credentials(tmp_path / "node-b", "Node B")
        first_token = coordinator.create_enrollment_token()["token"]
        second_token = coordinator.create_enrollment_token()["token"]
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        first: AuthenticatedClient | None = None
        second: AuthenticatedClient | None = None
        try:
            first = await _enroll(relay, first_credentials, first_token)
            second = await _enroll(relay, second_credentials, second_token)
            await first.send(
                "session.create",
                payload={"display_name": "Private session"},
            )
            created = await first.receive("session.create.accepted")
            session = created.payload["session"]
            assert isinstance(session, dict)
            session_id = str(session["session_id"])

            await second.send(
                "relay.message",
                session_id=session_id,
                target_node_id=first.node_id,
                payload={"text": "not a member"},
            )
            rejected = await second.receive("relay.error")
            assert rejected.payload["error"]["code"] == "not-session-member"

            changed = await relay.revoke_node(
                node_id=second.node_id,
                reason="relay test",
                request_id="revoke-node-b",
            )
            assert changed is True
            live_revocation = RelayEnvelope.from_json(
                await asyncio.wait_for(second.websocket.recv(), timeout=2)
            )
            assert live_revocation.message_type == "relay.error"
            assert (
                live_revocation.payload["error"]["code"] == "revoked-node"
            )
            with pytest.raises(ConnectionClosed):
                await second.websocket.recv()

            websocket, challenge = await _challenge(relay)
            try:
                await _send_authentication(
                    websocket, challenge, second_credentials
                )
                revoked = RelayEnvelope.from_json(
                    await asyncio.wait_for(websocket.recv(), timeout=2)
                )
                assert revoked.message_type == "relay.error"
                assert revoked.payload["error"]["code"] == "revoked-node"
            finally:
                await websocket.close()

            await first.send("heartbeat")
            heartbeat = await first.receive("heartbeat.accepted")
            assert heartbeat.payload["connection_state"] == "connected"
            assert any(
                entry["reason"] == "revoked-node"
                for entry in coordinator.audit_entries()
            )
        finally:
            if first is not None:
                await first.close()
            if second is not None:
                await second.close()
            await relay.stop()

    asyncio.run(scenario())


def test_wrong_private_key_and_unsupported_protocol_are_safe_errors(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        enrolled_credentials = _credentials(tmp_path / "node-a", "Node A")
        wrong_credentials = _credentials(tmp_path / "node-b", "Node B")
        token = coordinator.create_enrollment_token()["token"]
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        enrolled: AuthenticatedClient | None = None
        try:
            enrolled = await _enroll(relay, enrolled_credentials, token)
            await enrolled.close()
            enrolled = None

            websocket, challenge = await _challenge(relay)
            try:
                nonce = str(challenge.payload["nonce"])
                signature = wrong_credentials.sign(
                    authentication_message(
                        challenge_id=challenge.request_id,
                        nonce=nonce,
                        node_id=enrolled_credentials.identity.node_id,
                        protocol_version=challenge.protocol_version,
                    )
                )
                response = RelayEnvelope(
                    request_id=challenge.request_id,
                    actor_node_id=enrolled_credentials.identity.node_id,
                    message_type="auth.response",
                    authorization_context={"kind": "connection-authentication"},
                    payload={"nonce": nonce, "signature": signature},
                    sent_at=utc_now(),
                )
                await websocket.send(response.to_json())
                rejected = RelayEnvelope.from_json(
                    await asyncio.wait_for(websocket.recv(), timeout=2)
                )
                assert rejected.message_type == "relay.error"
                assert rejected.payload == {
                    "error": {
                        "code": "invalid-authentication-signature",
                        "field": "signature",
                        "message": "authentication rejected",
                    }
                }
                assert signature not in rejected.to_json()
            finally:
                await websocket.close()

            websocket, challenge = await _challenge(relay)
            try:
                incompatible = {
                    **RelayEnvelope(
                        request_id=challenge.request_id,
                        actor_node_id=enrolled_credentials.identity.node_id,
                        message_type="auth.response",
                        authorization_context={
                            "kind": "connection-authentication"
                        },
                        payload={"nonce": challenge.payload["nonce"]},
                        sent_at=utc_now(),
                    ).to_dict(),
                    "protocol_version": "2.0",
                }
                await websocket.send(json.dumps(incompatible))
                rejected = RelayEnvelope.from_json(
                    await asyncio.wait_for(websocket.recv(), timeout=2)
                )
                assert rejected.message_type == "relay.error"
                assert (
                    rejected.payload["error"]["code"]
                    == "unsupported-protocol-major"
                )
            finally:
                await websocket.close()
        finally:
            if enrolled is not None:
                await enrolled.close()
            await relay.stop()

    asyncio.run(scenario())


def test_never_enrolled_identity_is_rejected_and_audited(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        unknown_credentials = _credentials(
            tmp_path / "unknown-node", "Unknown node"
        )
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        websocket: ClientConnection | None = None
        try:
            websocket, challenge = await _challenge(relay)
            await _send_authentication(
                websocket, challenge, unknown_credentials
            )
            rejected = RelayEnvelope.from_json(
                await asyncio.wait_for(websocket.recv(), timeout=2)
            )

            assert rejected.message_type == "relay.error"
            assert rejected.payload["error"]["code"] == "unknown-node"
            assert any(
                entry["action"] == "auth.response"
                and entry["actor_node_id"]
                == unknown_credentials.identity.node_id
                and entry["reason"] == "unknown-node"
                for entry in coordinator.audit_entries()
            )
        finally:
            if websocket is not None:
                await websocket.close()
            await relay.stop()

    asyncio.run(scenario())


def test_failed_enrollment_proof_does_not_enroll_or_consume_token(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        coordinator = SessionCoordinator(tmp_path / "relay.sqlite3")
        intended_credentials = _credentials(tmp_path / "node-a", "Node A")
        wrong_credentials = _credentials(tmp_path / "node-b", "Node B")
        token = coordinator.create_enrollment_token()["token"]
        relay = RelayServer(
            coordinator,
            port=0,
            sweep_interval_seconds=60,
        )
        await relay.start()
        enrolled: AuthenticatedClient | None = None
        try:
            websocket, challenge = await _challenge(relay)
            try:
                enrollment = RelayEnvelope(
                    request_id=f"enroll-{uuid.uuid4().hex}",
                    actor_node_id=intended_credentials.identity.node_id,
                    message_type="node.enroll",
                    authorization_context={"kind": "enrollment-token"},
                    payload={
                        "identity": intended_credentials.identity.to_dict(),
                        "token": token,
                    },
                    sent_at=utc_now(),
                )
                await websocket.send(enrollment.to_json())
                proof_required = RelayEnvelope.from_json(
                    await asyncio.wait_for(websocket.recv(), timeout=2)
                )
                assert (
                    proof_required.message_type
                    == "node.enroll.proof-required"
                )
                await _send_authentication(
                    websocket,
                    challenge,
                    wrong_credentials,
                    actor_node_id=intended_credentials.identity.node_id,
                )
                rejected = RelayEnvelope.from_json(
                    await asyncio.wait_for(websocket.recv(), timeout=2)
                )
                assert (
                    rejected.payload["error"]["code"]
                    == "invalid-authentication-signature"
                )
            finally:
                await websocket.close()

            assert (
                coordinator.store.get_node(
                    intended_credentials.identity.node_id
                )
                is None
            )
            enrolled = await _enroll(
                relay, intended_credentials, token
            )
        finally:
            if enrolled is not None:
                await enrolled.close()
            await relay.stop()

    asyncio.run(scenario())

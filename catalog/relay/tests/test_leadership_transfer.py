"""Real-socket handoff regressions with a concurrently connected managed node."""
from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.protocol import RelayEnvelope, utc_now
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import IdentityStore
from catalog.node.leadership import transfer_leadership
from catalog.relay.authentication import authentication_message
from catalog.relay.provider_service import ProviderAuthorityRelayServer

BOUND = 60


@asynccontextmanager
async def topology(root):
    coordinator = SessionCoordinator(root / "authority.sqlite3")
    async with ProviderAuthorityRelayServer(
        coordinator, host="127.0.0.1", port=0,
        auth_timeout_seconds=BOUND, heartbeat_timeout_seconds=300,
    ) as relay:
        clients = [RelayNodeClient(
            state_directory=root / name, relay_url=relay.url, display_name=name,
            allow_insecure_local=True, request_timeout=BOUND,
        ) for name in ("flask", "recorder", "outsider")]
        flask, recorder, outsider = clients
        try:
            for client in clients:
                token = coordinator.create_enrollment_token(ttl_seconds=300)
                await client.connect(enrollment_token=token["token"])
            session = await flask.create_session("Retained Federation")
            sid = session["session_id"]
            invitation = await flask.create_invitation(sid)
            await recorder.join_session(invitation["token"])
            coordinator.transfer_session_leader(
                session_id=sid, actor_node_id=flask.node_id,
                target_node_id=recorder.node_id, request_id="initial-handoff",
            )
            yield SimpleNamespace(root=root, coordinator=coordinator, relay=relay,
                                  flask=flask, recorder=recorder, outsider=outsider, sid=sid)
        finally:
            for client in reversed(clients):
                await client.disconnect()


def call_args(t, **overrides):
    result = {
        "state_directory": t.recorder.state_directory, "relay_url": t.relay.url,
        "actor_node_id": t.recorder.node_id, "session_id": t.sid,
        "target_node_id": t.flask.node_id, "expected_term": 2,
        "request_id": "return-to-flask", "timeout_seconds": BOUND, "allow_insecure_local": True,
    }
    result.update(overrides)
    return result


def retained_state(t):
    with sqlite3.connect(t.root / "authority.sqlite3") as db:
        result = {
            "nodes": db.execute("SELECT node_id,public_key,enrolled_at,revoked_at FROM nodes ORDER BY node_id").fetchall(),
            "memberships": db.execute("SELECT session_id,node_id,joined_at,removed_at FROM session_memberships ORDER BY node_id").fetchall(),
            "sessions": db.execute("SELECT session_id,created_by_node_id,created_at FROM sessions").fetchall(),
            "connections": db.execute("SELECT node_id,state,connection_id,connected_at FROM node_connectivity ORDER BY node_id").fetchall(),
        }
    result["identity"] = {
        (c.node_id, name): hashlib.sha256((c.state_directory / name).read_bytes()).hexdigest()
        for c in (t.flask, t.recorder) for name in ("identity.pem", "identity.json")
    }
    result["local_membership"] = [(s.session_id, s.membership_state.value) for s in t.recorder.state.joined_sessions()]
    return result


def test_handoff_preserves_managed_socket_identity_membership_and_audit(tmp_path):
    async def scenario():
        async with topology(tmp_path) as t:
            before = retained_state(t)
            sockets = {n: c for n, c in t.relay._connections.items()}
            receipt = await transfer_leadership(**call_args(t))
            assert receipt["leadership"]["leader_node_id"] == t.flask.node_id
            assert receipt["leadership"]["term"] == 3
            assert receipt["event_id"]
            assert retained_state(t) == before
            assert all(t.relay._connections[n] is c for n, c in sockets.items())
            assert not t.recorder.disconnected_event.is_set()
            await t.recorder.coordinator_status()
            await t.flask.coordinator_status()
            with sqlite3.connect(t.root / "authority.sqlite3") as db:
                events = db.execute("SELECT payload_json FROM session_events WHERE event_type='session.leader.changed'").fetchall()
                audit = db.execute("SELECT actor_node_id,reason FROM audit_log WHERE action='session.leader.change'").fetchall()
            assert len(events) == 2
            assert (t.recorder.node_id, "explicit-handover") in audit
            with pytest.raises(RelayRemoteError, match="handoff") as rejected:
                await transfer_leadership(**call_args(t))
            assert rejected.value.code == "leadership-term-mismatch"
            assert t.coordinator.session_leadership(t.sid).term == 3
            assert retained_state(t) == before
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 2))


@pytest.mark.parametrize("case,code", [
    ("nonleader", "federation-leader-required"),
    ("outsider", "not-session-member"),
    ("offline-target", "leader-target-offline"),
    ("outsider-target", "not-session-member"),
    ("stale-term", "leadership-term-mismatch"),
    ("unknown-session", "not-session-member"),
    ("revoked-actor", "revoked-node"),
])
def test_rejected_handoff_does_not_replace_managed_connection(tmp_path, case, code):
    async def scenario():
        async with topology(tmp_path) as t:
            args = call_args(t)
            if case == "nonleader":
                args.update(state_directory=t.flask.state_directory, actor_node_id=t.flask.node_id,
                            target_node_id=t.recorder.node_id)
            elif case == "outsider":
                args.update(state_directory=t.outsider.state_directory, actor_node_id=t.outsider.node_id)
            elif case == "offline-target":
                await t.flask.disconnect()
                # Explicit durable disconnect avoids racing the relay's close handler.
                t.coordinator.disconnected(node_id=t.flask.node_id)
            elif case == "outsider-target":
                args["target_node_id"] = t.outsider.node_id
            elif case == "stale-term":
                args["expected_term"] = 1
            elif case == "unknown-session":
                args["session_id"] = "session-missing"
            elif case == "revoked-actor":
                t.coordinator.revoke_node(node_id=t.recorder.node_id, reason="test", request_id="revoke")
            before = t.coordinator.session_leadership(t.sid)
            socket = t.relay._connections.get(t.recorder.node_id)
            with pytest.raises(RelayRemoteError) as rejected:
                await transfer_leadership(**args)
            assert rejected.value.code == code
            assert t.coordinator.session_leadership(t.sid) == before
            assert t.relay._connections.get(t.recorder.node_id) is socket
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 2))


@pytest.mark.parametrize("tamper", ["target_node_id", "session_id", "expected_term", "request_id", "regular-proof"])
def test_signature_binds_entire_handoff_and_cannot_upgrade_regular_login(tmp_path, tamper):
    async def scenario():
        async with topology(tmp_path) as t:
            command = {key: call_args(t)[key] for key in ("session_id", "target_node_id", "expected_term", "request_id")}
            async with connect(t.relay.url, proxy=None, close_timeout=5) as websocket:
                challenge = RelayEnvelope.from_json(await websocket.recv())
                signature = t.recorder.credentials.sign(authentication_message(
                    challenge_id=challenge.request_id, nonce=challenge.payload["nonce"],
                    node_id=t.recorder.node_id, protocol_version=challenge.protocol_version,
                    leadership_transfer=None if tamper == "regular-proof" else command,
                ))
                if tamper != "regular-proof":
                    command[tamper] = 3 if tamper == "expected_term" else "changed-value"
                await websocket.send(RelayEnvelope(
                    request_id=challenge.request_id, actor_node_id=t.recorder.node_id,
                    message_type="auth.response", sent_at=utc_now(),
                    authorization_context={"kind": "one-shot-leadership-transfer"},
                    payload={"nonce": challenge.payload["nonce"], "signature": signature,
                             "leadership_transfer": command},
                ).to_json())
                response = RelayEnvelope.from_json(await websocket.recv())
                assert response.message_type == "relay.error"
                assert response.payload["error"]["code"] == "invalid-authentication-signature"
                assert t.coordinator.session_leadership(t.sid).term == 2
                assert not t.recorder.disconnected_event.is_set()
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 2))


def test_racing_handoffs_commit_one_transition(tmp_path):
    async def scenario():
        async with topology(tmp_path) as t:
            result = await asyncio.gather(
                transfer_leadership(**call_args(t, request_id="one")),
                transfer_leadership(**call_args(t, request_id="two")),
                return_exceptions=True,
            )
            assert sum(isinstance(r, dict) for r in result) == 1
            assert sum(isinstance(r, RelayRemoteError) and r.code == "leadership-term-mismatch" for r in result) == 1
            assert t.coordinator.session_leadership(t.sid).term == 3
            assert not t.recorder.disconnected_event.is_set()
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 2))


def test_cli_does_not_create_missing_identity(tmp_path):
    from catalog.node.leadership import main
    assert main([
        "--state-directory", str(tmp_path / "missing"), "--relay-url", "ws://127.0.0.1:1",
        "--actor-node-id", "node-existing", "--session-id", "session-existing",
        "--target-node-id", "node-flask", "--expected-term", "2",
        "--request-id", "one", "--allow-insecure-local",
    ]) == 1
    assert not (tmp_path / "missing").exists()


def test_missing_identity_cannot_be_replaced_by_new_enrollment(tmp_path):
    identity = IdentityStore(tmp_path / "identity", display_name="retained").load_or_create()
    (tmp_path / "identity" / "identity.pem").unlink()
    async def scenario():
        with pytest.raises(FederationValidationError):
            await transfer_leadership(
                state_directory=tmp_path / "identity", relay_url="ws://127.0.0.1:1",
                actor_node_id=identity.identity.node_id, target_node_id="node-target",
                session_id="session-existing", expected_term=2, request_id="one",
                allow_insecure_local=True,
            )
    asyncio.run(scenario())
    assert not (tmp_path / "identity" / "identity.pem").exists()


def test_old_request_stays_fenced_after_original_leader_returns(tmp_path):
    async def scenario():
        async with topology(tmp_path) as t:
            await transfer_leadership(**call_args(t))
            await transfer_leadership(**call_args(
                t, state_directory=t.flask.state_directory, actor_node_id=t.flask.node_id,
                target_node_id=t.recorder.node_id, expected_term=3, request_id="return-again",
            ))
            with pytest.raises(RelayRemoteError) as rejected:
                await transfer_leadership(**call_args(t))
            assert rejected.value.code == "leadership-term-mismatch"
            leadership = t.coordinator.session_leadership(t.sid)
            assert leadership.term == 4 and leadership.leader_node_id == t.recorder.node_id
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 2))


def test_timeout_opens_only_one_connection_and_never_retries(tmp_path):
    async def scenario():
        credentials = IdentityStore(tmp_path / "identity", display_name="retained").load_or_create()
        connections = []
        async def silent_relay(websocket):
            connections.append(websocket)
            await websocket.wait_closed()
        async with serve(silent_relay, "127.0.0.1", 0) as server:
            with pytest.raises(TimeoutError):
                await transfer_leadership(
                    state_directory=tmp_path / "identity",
                    relay_url=f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                    actor_node_id=credentials.identity.node_id, target_node_id="node-target",
                    session_id="session-retained", expected_term=2, request_id="one",
                    timeout_seconds=5, allow_insecure_local=True,
                )
            assert len(connections) == 1
    asyncio.run(asyncio.wait_for(scenario(), BOUND))


def test_handoff_uses_real_replicated_journal_without_replacing_clients(tmp_path):
    from catalog.federation.tests.test_control_plane_public_journal import (
        SESSION,
        _cluster,
        _populate,
        _wait,
    )
    async def scenario():
        async with _cluster(tmp_path) as cluster:
            clients = await _populate(cluster, tmp_path)
            actor, target = clients[:2]
            relay = cluster.relays[0]
            connections = dict(relay._connections)
            receipt = await transfer_leadership(
                state_directory=actor.state_directory, relay_url=relay.url,
                actor_node_id=actor.node_id, target_node_id=target.node_id,
                session_id=SESSION, expected_term=1, request_id="replicated-handoff",
                timeout_seconds=BOUND, allow_insecure_local=True,
            )
            assert receipt["leadership"]["term"] == 2
            assert receipt["leadership"]["leader_node_id"] == target.node_id
            assert receipt["event_id"]
            await _wait(
                lambda: all(runtime.replicated_leader(SESSION) == (target.node_id, 2)
                            for runtime in cluster.runtimes),
                "the committed handoff did not reach every replica",
            )
            assert all(relay._connections[n] is c for n, c in connections.items())
            await actor.coordinator_status()
            with pytest.raises(RelayRemoteError):
                await transfer_leadership(
                    state_directory=actor.state_directory, relay_url=relay.url,
                    actor_node_id=actor.node_id, target_node_id=target.node_id,
                    session_id=SESSION, expected_term=1, request_id="replicated-handoff",
                    timeout_seconds=BOUND, allow_insecure_local=True,
                )
            assert all(runtime.replicated_leader(SESSION) == (target.node_id, 2)
                       for runtime in cluster.runtimes)
    asyncio.run(asyncio.wait_for(scenario(), BOUND * 5))

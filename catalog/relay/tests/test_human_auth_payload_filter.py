"""Typed sign-in origins route; neighbouring private payloads and actors do not."""
from __future__ import annotations

import asyncio

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.human_auth import (
    AUTHORITY_EVENT,
    AUTHORITY_SCHEMA,
    MEMBER_ENDPOINT_EVENT,
    MEMBER_ENDPOINT_SCHEMA,
    USER_EVENT,
    USER_SCHEMA,
)
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import derive_node_id_from_public_key
from catalog.relay.service import RelayServer, _ensure_bounded_json

PUBLIC_KEY = "ed25519:" + "A" * 43
NODE_ID = derive_node_id_from_public_key(PUBLIC_KEY)


def _member(origin="http://127.0.0.1:46450", **extra):
    return {"schema": MEMBER_ENDPOINT_SCHEMA, "node_id": NODE_ID, "base_url": origin, **extra}


def _authority(origin="https://leader.example", **extra):
    return {"schema": AUTHORITY_SCHEMA, "node_id": NODE_ID, "public_key": PUBLIC_KEY, "base_url": origin, **extra}


def _reject(payload, event_type=MEMBER_ENDPOINT_EVENT):
    with pytest.raises(FederationValidationError) as caught:
        _ensure_bounded_json(payload, field="payload", event_type=event_type)
    assert caught.value.code == "nonpublic-payload"


@pytest.mark.parametrize("origin", [
    "http://127.0.0.1:46450", "http://100.64.0.1:5000",
    "https://member.example", "https://member.example:8443", "http://[::1]:5000",
])
def test_existing_routing_schemas_admit_canonical_bounded_origins(origin):
    _ensure_bounded_json(_member(origin), field="payload", event_type=MEMBER_ENDPOINT_EVENT)
    _ensure_bounded_json(_authority(origin), field="payload", event_type=AUTHORITY_EVENT)


@pytest.mark.parametrize("origin", [
    "http://user:password@member.example", "http://member.example?token=private",
    "http://member.example#token", "http://member.example/private",
    "http://member.example/", "HTTP://member.example", " http://member.example",
    "http://member.example\n", "http://member.example\x7f", "http://member.example\\private",
    "http://member.example:0", "http://member.example:65536", "http://[invalid]",
    "file:///private", "ftp://member.example", "http://", "http://member%2fprivate.example",
    "http://fcp_enroll_private.example", "http://fcp_join_private.example",
    "http://" + "a" * 2048,
])
def test_invalid_or_credential_bearing_origins_are_not_exempt(origin):
    _reject(_member(origin))


@pytest.mark.parametrize("payload,event_type", [
    (_member(password="private"), MEMBER_ENDPOINT_EVENT),
    (_member(backend_path="/private/data"), MEMBER_ENDPOINT_EVENT),
    (_member(note="extra public fields also do not expand this schema"), MEMBER_ENDPOINT_EVENT),
    (_member(schema="fcp.some-other.v1"), MEMBER_ENDPOINT_EVENT),
    (_member(), "human_auth.unrecognized"),
    (_member(), USER_EVENT),
    (_member(), AUTHORITY_EVENT),
    (_member(), None),
    ({"nested": _member()}, MEMBER_ENDPOINT_EVENT),
    ([_member()], MEMBER_ENDPOINT_EVENT),
    (_authority(public_key="ed25519:invalid"), AUTHORITY_EVENT),
    (_authority(node_id="node-other"), AUTHORITY_EVENT),
])
def test_only_exact_top_level_existing_routing_contracts_earn_the_allowance(payload, event_type):
    _reject(payload, event_type)


def test_real_relay_preserves_member_self_current_leader_and_privacy_rules(tmp_path):
    async def exercise():
        coordinator = SessionCoordinator(tmp_path / "authority.sqlite3")
        async with RelayServer(
            coordinator, host="127.0.0.1", port=0,
            auth_timeout_seconds=30, send_timeout_seconds=30,
            heartbeat_timeout_seconds=300, sweep_interval_seconds=300,
        ) as relay:
            clients = [RelayNodeClient(
                state_directory=tmp_path / name, relay_url=relay.url, display_name=name,
                allow_insecure_local=True, request_timeout=30, heartbeat_interval=300,
            ) for name in ("creator", "member")]
            creator, member = clients
            try:
                for client in clients:
                    token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)
                    await client.connect(enrollment_token=token["token"])
                session = await creator.create_session("Human sign-in")
                sid = session["session_id"]
                invitation = await creator.create_invitation(sid)
                await member.join_session(invitation["token"])
                payload = _member(node_id=member.node_id)
                event = await member.append_event(session_id=sid, event_type=MEMBER_ENDPOINT_EVENT, payload=payload)
                assert event.payload == payload

                async def refused(client, event_type, payload, code):
                    before = coordinator.store.get_session(sid).revision
                    with pytest.raises(RelayRemoteError) as caught:
                        await client.append_event(session_id=sid, event_type=event_type, payload=payload)
                    assert caught.value.code == code
                    assert coordinator.store.get_session(sid).revision == before

                authority = _authority(node_id=creator.node_id, public_key=creator.credentials.identity.public_key)
                accepted = await creator.append_event(session_id=sid, event_type=AUTHORITY_EVENT, payload=authority)
                assert accepted.payload == authority
                await refused(member, MEMBER_ENDPOINT_EVENT, _member(node_id=creator.node_id), "human-auth-node-mismatch")
                member_authority = _authority(node_id=member.node_id, public_key=member.credentials.identity.public_key)
                await refused(member, AUTHORITY_EVENT, member_authority, "human-auth-authority-required")
                await refused(member, MEMBER_ENDPOINT_EVENT, _member(node_id=member.node_id, password="private"), "nonpublic-payload")
                await refused(member, MEMBER_ENDPOINT_EVENT, _member("https://member.example?token=private", node_id=member.node_id), "nonpublic-payload")
                await refused(member, "knowledge.note", payload, "nonpublic-payload")

                coordinator.transfer_session_leader(
                    session_id=sid, actor_node_id=creator.node_id,
                    target_node_id=member.node_id, request_id="verified-handoff",
                )
                await refused(creator, AUTHORITY_EVENT, authority, "human-auth-authority-required")
                current = await member.append_event(session_id=sid, event_type=AUTHORITY_EVENT, payload=member_authority)
                assert current.payload == member_authority
                user = {"schema": USER_SCHEMA, "email": "operator@example.com", "active": True, "roles": ["operator"]}
                await refused(creator, USER_EVENT, user, "human-auth-authority-required")
                approved = await member.append_event(session_id=sid, event_type=USER_EVENT, payload=user)
                assert approved.payload == user
            finally:
                for client in reversed(clients):
                    await client.disconnect()

    asyncio.run(exercise())

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.direct_peer import DirectPeerDescriptor
from catalog.federation.errors import FederationValidationError
from catalog.federation.session_route_rendezvous import (
    SessionRouteDescriptor,
    SessionRouteRegistry,
)
from catalog.node.client import RelayNodeClient, RelayRemoteError
from catalog.node.identity import IdentityStore, verify_signature
from catalog.node.route_rendezvous import SessionRouteRendezvousClient
from catalog.relay.route_rendezvous import RouteRendezvousRelayServer

NOW = datetime(2026, 8, 1, 8, tzinfo=timezone.utc)
REQUEST_TIMEOUT_SECONDS = 3.0


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def _encryption_public_key() -> str:
    raw = X25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"x25519:{encoded}"


def _direct_descriptor(node_id: str, suffix: str) -> DirectPeerDescriptor:
    peer_id = f"peer-{suffix}"
    return DirectPeerDescriptor(
        node_id=node_id,
        peer_id=peer_id,
        multiaddr=f"/ip4/203.0.113.10/tcp/4001/p2p/{peer_id}",
        encryption_public_key=_encryption_public_key(),
    )


async def _start_relay(
    database: Path,
    clock: MutableClock,
) -> RouteRendezvousRelayServer:
    relay = RouteRendezvousRelayServer(
        SessionCoordinator(database, clock=clock),
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
    relay: RouteRendezvousRelayServer,
    display_name: str,
    clock: MutableClock,
) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=state_directory,
        relay_url=relay.url,
        display_name=display_name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=REQUEST_TIMEOUT_SECONDS,
        clock=clock,
    )


async def _enroll(
    relay: RouteRendezvousRelayServer,
    client: RelayNodeClient,
) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await client.connect(enrollment_token=token)


async def _shared_session(
    owner: RelayNodeClient,
    member: RelayNodeClient,
) -> str:
    session = await owner.create_session("F6.6.2 route rendezvous")
    session_id = str(session["session_id"])
    invitation = await owner.create_invitation(
        session_id,
        ttl_seconds=60,
        max_uses=1,
    )
    joined = await member.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)
    return session_id


async def _shutdown(
    relay: RouteRendezvousRelayServer | None,
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


def test_f662_signed_descriptor_is_bound_to_session_node_peer_and_window(
    tmp_path: Path,
) -> None:
    credentials = IdentityStore(
        tmp_path / "identity", display_name="Route node"
    ).load_or_create(now=NOW)
    direct = _direct_descriptor(credentials.identity.node_id, "signed")
    descriptor = SessionRouteDescriptor.create(
        session_id="session-signed",
        descriptor=direct,
        generation=1,
        issued_at=NOW,
        ttl_seconds=300,
        sign=credentials.sign,
    )

    parsed = SessionRouteDescriptor.from_dict(descriptor.to_dict())
    assert parsed == descriptor
    assert parsed.to_direct_peer_descriptor() == direct
    assert verify_signature(
        credentials.identity.public_key,
        parsed.signing_bytes(),
        parsed.signature,
    )

    tampered = descriptor.to_dict()
    tampered["multiaddr"] = (
        "/ip4/203.0.113.11/tcp/4001/p2p/another-peer"
    )
    with pytest.raises(FederationValidationError) as mismatch:
        SessionRouteDescriptor.from_dict(tampered)
    assert mismatch.value.code == "session-route-peer-mismatch"


def test_f662_registry_rejects_forgery_rollback_conflict_and_expiry(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    owner = IdentityStore(
        tmp_path / "owner", display_name="Owner"
    ).load_or_create(now=clock())
    attacker = IdentityStore(
        tmp_path / "attacker", display_name="Attacker"
    ).load_or_create(now=clock())
    direct = _direct_descriptor(owner.identity.node_id, "registry")
    registry = SessionRouteRegistry(max_entries=2)

    forged = SessionRouteDescriptor.create(
        session_id="session-registry",
        descriptor=direct,
        generation=1,
        issued_at=clock(),
        ttl_seconds=60,
        sign=attacker.sign,
    )
    with pytest.raises(FederationValidationError) as invalid_signature:
        registry.publish(
            forged,
            public_key=owner.identity.public_key,
            verify_signature=verify_signature,
            now=clock(),
        )
    assert invalid_signature.value.code == "invalid-session-route-signature"

    first = SessionRouteDescriptor.create(
        session_id="session-registry",
        descriptor=direct,
        generation=1,
        issued_at=clock(),
        ttl_seconds=60,
        sign=owner.sign,
    )
    assert registry.publish(
        first,
        public_key=owner.identity.public_key,
        verify_signature=verify_signature,
        now=clock(),
    )
    assert not registry.publish(
        first,
        public_key=owner.identity.public_key,
        verify_signature=verify_signature,
        now=clock(),
    )

    second = SessionRouteDescriptor.create(
        session_id="session-registry",
        descriptor=_direct_descriptor(owner.identity.node_id, "registry-new"),
        generation=2,
        issued_at=clock(),
        ttl_seconds=60,
        sign=owner.sign,
    )
    assert registry.publish(
        second,
        public_key=owner.identity.public_key,
        verify_signature=verify_signature,
        now=clock(),
    )

    with pytest.raises(FederationValidationError) as rollback:
        registry.publish(
            first,
            public_key=owner.identity.public_key,
            verify_signature=verify_signature,
            now=clock(),
        )
    assert rollback.value.code == "session-route-rollback"

    conflicting = SessionRouteDescriptor.create(
        session_id="session-registry",
        descriptor=_direct_descriptor(owner.identity.node_id, "registry-conflict"),
        generation=2,
        issued_at=clock(),
        ttl_seconds=60,
        sign=owner.sign,
    )
    with pytest.raises(FederationValidationError) as conflict:
        registry.publish(
            conflicting,
            public_key=owner.identity.public_key,
            verify_signature=verify_signature,
            now=clock(),
        )
    assert conflict.value.code == "session-route-generation-conflict"

    clock.advance(61)
    assert registry.resolve(
        session_id="session-registry",
        node_id=owner.identity.node_id,
        now=clock(),
    ) is None
    assert registry.count(now=clock()) == 0


def test_f662_two_enrolled_members_publish_resolve_register_and_withdraw(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = MutableClock()
        relay: RouteRendezvousRelayServer | None = None
        owner: RelayNodeClient | None = None
        member: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3", clock)
            owner = _client(tmp_path / "owner", relay, "Owner", clock)
            member = _client(tmp_path / "member", relay, "Member", clock)
            await _enroll(relay, owner)
            await _enroll(relay, member)
            session_id = await _shared_session(owner, member)

            owner_routes = SessionRouteRendezvousClient(owner, clock=clock)
            member_routes = SessionRouteRendezvousClient(member, clock=clock)
            direct = _direct_descriptor(owner.node_id, "integration")
            published = await owner_routes.publish(
                session_id=session_id,
                descriptor=direct,
                ttl_seconds=60,
            )
            assert published.generation == 1

            resolved = await member_routes.resolve(
                session_id=session_id,
                target_node_id=owner.node_id,
            )
            assert resolved == direct

            class Registrar:
                descriptor: DirectPeerDescriptor | None = None

                def register_peer(self, descriptor: DirectPeerDescriptor) -> None:
                    self.descriptor = descriptor

            registrar = Registrar()
            registered = await member_routes.resolve_and_register(
                registrar,
                session_id=session_id,
                target_node_id=owner.node_id,
            )
            assert registered == direct
            assert registrar.descriptor == direct

            assert await owner_routes.withdraw(session_id=session_id)
            with pytest.raises(RelayRemoteError) as unavailable:
                await member_routes.resolve(
                    session_id=session_id,
                    target_node_id=owner.node_id,
                )
            assert unavailable.value.code == "session-route-unavailable"
        finally:
            await _shutdown(relay, owner, member)

    asyncio.run(scenario())


def test_f662_relay_rejects_forged_rollback_and_cross_session_resolution(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = MutableClock()
        relay: RouteRendezvousRelayServer | None = None
        owner: RelayNodeClient | None = None
        member: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3", clock)
            owner = _client(tmp_path / "owner", relay, "Owner", clock)
            member = _client(tmp_path / "member", relay, "Member", clock)
            await _enroll(relay, owner)
            await _enroll(relay, member)
            session_id = await _shared_session(owner, member)

            direct = _direct_descriptor(owner.node_id, "forged")
            forged = SessionRouteDescriptor.create(
                session_id=session_id,
                descriptor=direct,
                generation=1,
                issued_at=clock(),
                ttl_seconds=60,
                sign=member.credentials.sign,
            )
            with pytest.raises(RelayRemoteError) as forged_error:
                await owner.request(
                    "route.publish",
                    session_id=session_id,
                    payload={"descriptor": forged.to_dict()},
                )
            assert forged_error.value.code == "invalid-session-route-signature"

            owner_routes = SessionRouteRendezvousClient(owner, clock=clock)
            await owner_routes.publish(
                session_id=session_id,
                descriptor=direct,
                ttl_seconds=60,
                generation=2,
            )
            with pytest.raises(RelayRemoteError) as rollback:
                await owner_routes.publish(
                    session_id=session_id,
                    descriptor=direct,
                    ttl_seconds=60,
                    generation=1,
                )
            assert rollback.value.code == "session-route-rollback"

            private_session = await owner.create_session("Owner only")
            private_session_id = str(private_session["session_id"])
            await owner_routes.publish(
                session_id=private_session_id,
                descriptor=direct,
                ttl_seconds=60,
            )
            member_routes = SessionRouteRendezvousClient(member, clock=clock)
            with pytest.raises(RelayRemoteError):
                await member_routes.resolve(
                    session_id=private_session_id,
                    target_node_id=owner.node_id,
                )
        finally:
            await _shutdown(relay, owner, member)

    asyncio.run(scenario())


def test_f662_expired_route_is_not_returned_after_clock_advance(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        clock = MutableClock()
        relay: RouteRendezvousRelayServer | None = None
        owner: RelayNodeClient | None = None
        member: RelayNodeClient | None = None
        try:
            relay = await _start_relay(tmp_path / "relay.sqlite3", clock)
            owner = _client(tmp_path / "owner", relay, "Owner", clock)
            member = _client(tmp_path / "member", relay, "Member", clock)
            await _enroll(relay, owner)
            await _enroll(relay, member)
            session_id = await _shared_session(owner, member)

            owner_routes = SessionRouteRendezvousClient(owner, clock=clock)
            member_routes = SessionRouteRendezvousClient(member, clock=clock)
            await owner_routes.publish(
                session_id=session_id,
                descriptor=_direct_descriptor(owner.node_id, "expiring"),
                ttl_seconds=5,
            )
            clock.advance(6)
            with pytest.raises(RelayRemoteError) as expired:
                await member_routes.resolve(
                    session_id=session_id,
                    target_node_id=owner.node_id,
                )
            assert expired.value.code == "session-route-unavailable"
            assert relay.route_registry.count(now=clock()) == 0
        finally:
            await _shutdown(relay, owner, member)

    asyncio.run(scenario())

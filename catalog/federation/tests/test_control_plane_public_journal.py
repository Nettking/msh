"""Real relay clients must retain one public journal across C03 leader loss."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_replication import ReplicaNode
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.tests.test_control_plane_physical_runtime import _deployments
from catalog.node.client import RelayNodeClient
from catalog.relay.provider_service import ProviderAuthorityRelayServer

SESSION = "session-public-journal"
FEDERATION = "federation-public-journal"
WAIT_SECONDS = 60.0


async def _wait(predicate, description: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_SECONDS
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(description)


def _journal(runtime: FederationV1ReleaseRuntime) -> tuple[dict, ...]:
    return tuple(
        event.to_dict()
        for event in runtime.local.store.replay_events(
            session_id=SESSION, last_applied_revision=0
        )
    )


def _client_journal(client: RelayNodeClient) -> tuple[dict, ...]:
    return tuple(event.to_dict() for event in client.state.applied_events(SESSION))


class _Cluster:
    def __init__(self, root: Path) -> None:
        self.runtimes = []
        self.relays = []
        self.clients = []
        self.running_runtimes: set[int] = set()
        self.running_relays: set[int] = set()
        for index, deployment in enumerate(_deployments(root)):
            # Sidecars use fixed sibling filenames. Each physical-equivalent
            # runtime therefore needs its own directory, not just a distinct DB.
            state = root / f"voter-{index}"
            state.mkdir()
            deployment = replace(
                deployment,
                replica_database=state / "replica.sqlite3",
                replay_database=state / "replay.sqlite3",
                coordinator_database=state / "coordinator.sqlite3",
            )
            runtime = FederationV1ReleaseRuntime(
                deployment,
                legacy_node_state_database=state / "legacy.sqlite3",
                legacy_pairing_state_path=state / "legacy.json",
            )
            self.runtimes.append(runtime)
            self.relays.append(self.relay_for(index))

    def relay_for(self, index: int, **options) -> ProviderAuthorityRelayServer:
        return ProviderAuthorityRelayServer(
            PhysicalReadyReplicatedSessionCoordinator(self.runtimes[index]),
            host="127.0.0.1",
            port=0,
            **options,
        )

    def client(self, path: Path, relay_index: int, name: str) -> RelayNodeClient:
        client = RelayNodeClient(
            state_directory=path,
            relay_url=self.relays[relay_index].url,
            display_name=name,
            allow_insecure_local=True,
            request_timeout=30,
        )
        self.clients.append(client)
        return client

    async def stop_relay(self, index: int) -> None:
        if index in self.running_relays:
            await self.relays[index].stop()
            self.running_relays.remove(index)

    async def stop_runtime(self, index: int) -> None:
        if index in self.running_runtimes:
            await asyncio.to_thread(self.runtimes[index].close)
            self.running_runtimes.remove(index)


@asynccontextmanager
async def _cluster(root: Path):
    cluster = _Cluster(root)
    try:
        for index, runtime in enumerate(cluster.runtimes):
            await asyncio.to_thread(runtime.start)
            cluster.running_runtimes.add(index)
        for index, relay in enumerate(cluster.relays):
            await relay.start()
            cluster.running_relays.add(index)
        creator = cluster.runtimes[0]
        await asyncio.to_thread(
            creator.bootstrap_new_federation,
            federation_id=FEDERATION,
            session_id=SESSION,
            creator_node_id=creator.node.voter_id,
            display_name="Public journal continuity",
        )
        await _wait(
            lambda: all(runtime.ready for runtime in cluster.runtimes),
            "the three authenticated voters did not become ready",
        )
        yield cluster
    finally:
        # Close every real socket/thread even when a regression assertion fails.
        await asyncio.gather(
            *(client.disconnect() for client in cluster.clients),
            return_exceptions=True,
        )
        for index in tuple(cluster.running_relays):
            await cluster.stop_relay(index)
        for index in tuple(cluster.running_runtimes):
            await cluster.stop_runtime(index)


async def _populate(cluster: _Cluster, root: Path) -> list[RelayNodeClient]:
    clients = []
    for index, runtime in enumerate(cluster.runtimes):
        client = cluster.client(runtime.deployment.identity_directory, 0, f"voter-{index}")
        await client.connect()
        clients.append(client)

    member = cluster.client(root / "joining-member", 0, "joining-member")
    enrollment = cluster.relays[0].coordinator.create_enrollment_token(max_uses=1)
    await member.connect(enrollment_token=enrollment["token"])
    invitation = await clients[0].create_invitation(SESSION, max_uses=1)
    joined = await member.join_session(invitation["token"])
    assert joined["session_id"] == SESSION
    clients.append(member)

    for owner, capability_id in ((clients[1], "readings"), (member, "analysis")):
        await owner.announce_capability(
            CapabilityAnnouncement(
                capability_id=capability_id,
                node_id=owner.node_id,
                session_id=SESSION,
                type="journal-regression",
                protocol="bounded-public-payload",
                protocol_version="1",
                status=CapabilityStatus.READY,
                properties={"purpose": capability_id, "supported": ["read"]},
                announced_at=datetime.now(timezone.utc),
            )
        )
    for client in clients:
        await client.request_replay(SESSION)

    committed = cluster.runtimes[0].node.store.commit_index
    await _wait(
        lambda: all(
            runtime.node.store.last_applied >= committed
            and runtime.local.store.get_session(SESSION) is not None
            and {"readings", "analysis"}
            <= set(runtime.node.state["capabilities"].get(SESSION, {}))
            and {"readings", "analysis"}
            <= {
                item.capability_id
                for item in runtime.local.store.list_capabilities(session_id=SESSION)
            }
            for runtime in cluster.runtimes
        ),
        "membership and capability authority did not reach all three voters",
    )
    return clients


def test_client_public_journal_survives_automatic_quorum_failover(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            clients = await _populate(cluster, tmp_path)
            original = cluster.runtimes[0]
            prefix = _journal(original)
            assert {"node.joined", "capability.registered"} <= {
                event["event_type"] for event in prefix
            }
            registered = [
                event for event in prefix if event["event_type"] == "capability.registered"
            ]
            assert len(registered) == 2
            assert all(_client_journal(client) == prefix for client in clients)
            # Capture the full follower prefix before the failure. Assert it
            # below so this regression also reaches the real reconnect failure.
            follower_prefixes = [_journal(runtime) for runtime in cluster.runtimes[1:]]
            previous_term = original.node.state["leaders"][SESSION]["term"]
            previous_consensus_term = original.node.store.current_term

            await cluster.stop_runtime(0)
            await cluster.stop_relay(0)
            for client in clients:
                await client.disconnect()
            retained = [_client_journal(client) for client in clients]
            successor_index = None

            def automatic_successor_ready() -> bool:
                nonlocal successor_index
                elected = [
                    index
                    for index in (1, 2)
                    if cluster.runtimes[index].node.role == ReplicaNode.LEADER
                ]
                if len(elected) != 1:
                    return False
                index = elected[0]
                runtime = cluster.runtimes[index]
                leadership = runtime.node.state["leaders"][SESSION]
                if (
                    leadership["leader_node_id"] != runtime.node.voter_id
                    or leadership["term"] <= previous_term
                    or runtime.node.store.current_term <= previous_consensus_term
                ):
                    return False
                successor_index = index
                return True

            # Observe the production lifecycle election; never elect or promote
            # a chosen survivor manually and never shorten its consensus timers.
            await _wait(automatic_successor_ready, "surviving quorum did not promote a leader")
            assert successor_index is not None
            successor = cluster.runtimes[successor_index]
            reconnected = []
            for index, old_client in enumerate(clients):
                client = cluster.client(
                    old_client.state_directory, successor_index, f"reconnect-{index}"
                )
                assert client.node_id == old_client.node_id
                assert _client_journal(client) == retained[index]
                # This used to raise revision-ahead because the successor had
                # invented a shorter/different local public journal.
                await client.connect()
                assert _client_journal(client)[: len(retained[index])] == retained[index]
                reconnected.append(client)

            for before in follower_prefixes:
                assert before[: len(prefix)] == prefix
            for client in reconnected:
                await client.request_replay(SESSION)
            journal = _journal(successor)
            assert journal[: len(prefix)] == prefix
            assert [event["revision"] for event in journal] == list(range(1, len(journal) + 1))
            assert len({event["event_id"] for event in journal}) == len(journal)
            transitions = [
                event
                for event in journal[len(prefix) :]
                if event["event_type"] == "session.leader.changed"
            ]
            assert transitions
            assert transitions[0]["payload"]["previous_leader_node_id"] == original.node.voter_id
            assert transitions[-1]["payload"]["leader_node_id"] == successor.node.voter_id
            assert [event["payload"]["term"] for event in transitions] == list(
                range(previous_term + 1, previous_term + 1 + len(transitions))
            )
            assert all(_client_journal(client) == journal for client in reconnected)
            await _wait(
                lambda: all(_journal(runtime) == journal for runtime in cluster.runtimes[1:]),
                "surviving materialized journals did not retain the complete public event identity",
            )
            assert all(
                runtime.node.state["federation_id"] == FEDERATION
                for runtime in cluster.runtimes[1:]
            )

    asyncio.run(scenario())


def test_follower_relay_lifecycle_cannot_append_an_independent_public_revision(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            await _populate(cluster, tmp_path)
            leader = cluster.runtimes[0]
            follower = cluster.runtimes[1]
            original_authority = dict(leader.node.state["leaders"][SESSION])
            leader_prefix = _journal(leader)
            follower_before = _journal(follower)

            # A follower sees no local WebSocket connection for the true leader.
            # Restart its actual provider relay and let the normal stale sweep
            # cross a real configured timeout. That local absence must not
            # create durable health/leadership events while C03 retains quorum.
            await cluster.stop_relay(1)
            cluster.relays[1] = cluster.relay_for(
                1, heartbeat_timeout_seconds=0.2, sweep_interval_seconds=0.05
            )
            await cluster.relays[1].start()
            cluster.running_relays.add(1)
            await asyncio.sleep(0.3)
            # Exercise the same production callback explicitly as well, so a
            # delayed event-loop sweep cannot make this a vacuous silence test.
            _stale, emitted = cluster.relays[1].coordinator.sweep_stale(
                heartbeat_timeout_seconds=0.2
            )

            assert leader.node.role == ReplicaNode.LEADER
            assert follower.node.role == ReplicaNode.FOLLOWER
            assert leader.node.state["leaders"][SESSION] == original_authority
            assert follower.node.state["leaders"][SESSION] == original_authority
            assert emitted == ()
            assert _journal(follower) == follower_before
            assert follower_before == leader_prefix
            local_leadership = follower.local.leadership.current(SESSION)
            assert local_leadership.leader_node_id == original_authority["leader_node_id"]
            assert local_leadership.term == original_authority["term"]

    asyncio.run(scenario())

"""A real voter election must not terminate the relay's local health sweep."""

from __future__ import annotations

import copy
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_replication import ReplicaNode
from catalog.federation.errors import AuthorizationError
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.tests.test_control_plane_physical_runtime import _deployments

SESSION = "session-health-leadership-race"


@pytest.fixture
def runtimes(tmp_path: Path):
    with ExitStack() as cleanup:
        replicas = []
        for index, deployment in enumerate(_deployments(tmp_path)):
            state = tmp_path / f"voter-{index}"
            state.mkdir()
            runtime = FederationV1ReleaseRuntime(
                replace(
                    deployment,
                    replica_database=state / "replica.sqlite3",
                    replay_database=state / "replay.sqlite3",
                    coordinator_database=state / "coordinator.sqlite3",
                ),
                legacy_node_state_database=state / "legacy.sqlite3",
                legacy_pairing_state_path=state / "legacy.json",
            )
            # Drive elections explicitly through the real authenticated transport
            # so the exact pre-lock interleaving is independent of timer speed.
            # No lifecycle clock, role, quorum response or durable state is faked.
            runtime.legacy_witness_server.start()
            runtime.server.start()
            cleanup.callback(runtime.close)
            runtime.credential_server.start()
            replicas.append(runtime)
        leader = replicas[0]
        leader.bootstrap_new_federation(
            federation_id="federation-health-leadership-race",
            session_id=SESSION,
            creator_node_id=leader.node.voter_id,
            display_name="Health leadership race",
        )
        for runtime in replicas:
            runtime.materialize()
            assert runtime.ready
        yield tuple(replicas)


def _history(runtime: FederationV1ReleaseRuntime) -> tuple[dict, ...]:
    return tuple(
        event.to_dict()
        for event in runtime.local.store.replay_events(
            session_id=SESSION, last_applied_revision=0,
        )
    )


def test_peer_election_before_health_journal_lock_keeps_sweep_local(
    runtimes, monkeypatch: pytest.MonkeyPatch,
) -> None:
    leader, successor, third = runtimes
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    node_id = third.node.voter_id
    announcement = CapabilityAnnouncement(
        capability_id="health-race-capability", node_id=node_id,
        session_id=SESSION, type="demo.health-race", protocol="demo.health-race",
        protocol_version="1", status=CapabilityStatus.READY,
        properties={"purpose": "local health regression"}, announced_at=leader.clock(),
    )
    facade.announce_capability(
        announcement, actor_node_id=node_id, request_id="announce-health-race",
    )
    for runtime in runtimes:
        runtime.materialize()
    leader.local.store.mark_connected(
        node_id=node_id, connection_id="health-race-connection",
        now=leader.clock() - timedelta(minutes=1),
    )
    before_history = _history(leader)
    before_state = copy.deepcopy(leader.node.state)
    before_commit = leader.node.store.commit_index
    before_term = leader.node.store.current_term
    original_operation = leader.journal.operation
    elections = []

    @contextmanager
    def elect_before_journal_lock():
        # _health has already selected its LEADER branch. RequestVote and
        # AppendEntries now step it down before the real journal takes its lock.
        assert leader.node.role == ReplicaNode.LEADER
        assert successor.node.start_election(successor.transport)
        assert successor.node.synchronize(successor.transport) == 2
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.store.current_term > before_term
        assert leader.node.role == ReplicaNode.FOLLOWER
        assert leader.node.leader_id == successor.node.voter_id
        elections.append(successor.node.store.current_term)
        with original_operation() as database:
            yield database

    with monkeypatch.context() as patch:
        patch.setattr(leader.journal, "operation", elect_before_journal_lock)
        stale, events = facade.sweep_stale(heartbeat_timeout_seconds=1)

    assert len(elections) == 1
    assert stale == (node_id,)
    assert events == ()
    with leader.local.store.read_transaction() as database:
        connectivity = database.execute(
            "SELECT state,connection_id,last_error FROM node_connectivity WHERE node_id=?",
            (node_id,),
        ).fetchone()
    assert tuple(connectivity) == ("disconnected", None, "stale heartbeat")
    # Local cleanup may run again; it must neither kill the caller nor generate
    # an independent public health revision from the former leader.
    assert facade.sweep_stale(heartbeat_timeout_seconds=1) == ((), ())
    assert facade.disconnected(node_id=node_id) == ()
    assert facade.relay_started() is None
    for runtime in runtimes:
        runtime.materialize()
        assert runtime.node.state == before_state
        assert runtime.node.store.commit_index == before_commit
        assert runtime.node.store.last_log_index() == before_commit
        assert _history(runtime) == before_history
        assert next(
            item for item in runtime.local.store.list_capabilities(session_id=SESSION)
            if item.capability_id == announcement.capability_id
        ).status is CapabilityStatus.READY

    # The health-only fallback must not make any durable facade operation legal
    # on the stepped-down voter, even though its readiness seal is still valid.
    with pytest.raises(AuthorizationError) as rejected:
        facade.create_enrollment_token(max_uses=1)
    assert rejected.value.code == "federation-quorum-leader-required"
    assert leader.journal._pending() is None
    assert leader.node.store.commit_index == before_commit
    assert _history(leader) == before_history


def test_health_does_not_swallow_unrelated_authorization_failure(runtimes) -> None:
    leader = runtimes[0]
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    before_commit = leader.node.store.commit_index
    before_history = _history(leader)
    calls = []

    def require_foreign_membership(*, emit_health_events=True):
        calls.append(emit_health_events)
        leader.local.store.require_membership(
            session_id="foreign-session", node_id=leader.node.voter_id,
        )

    # Use the real membership rejection, not a substituted quorum exception.
    with pytest.raises(AuthorizationError) as rejected:
        facade._health(require_foreign_membership)
    assert rejected.value.code == "not-session-member"
    assert calls == [True]
    assert leader.node.role == ReplicaNode.LEADER
    assert leader.node.store.commit_index == before_commit
    assert _history(leader) == before_history

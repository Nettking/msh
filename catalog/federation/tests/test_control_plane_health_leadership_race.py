"""A real voter election must not terminate the relay's local health sweep."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_replication import ReplicaNode, StaleTerm
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


def _pending_row(runtime):
    with sqlite3.connect(runtime.journal.pending_path) as database:
        row = database.execute(
            "SELECT command_json,reserved_index,proposing_term FROM pending WHERE slot=1",
        ).fetchone()
    return row


def _private_digest(runtime):
    # Assertions must never display private grants or request response bodies.
    with runtime.local.store.read_transaction() as database:
        encoded = json.dumps(runtime.journal.private.capture(database), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def test_pending_proposal_does_not_block_local_connectivity_or_authorize_other_writes(
    runtimes, monkeypatch: pytest.MonkeyPatch,
) -> None:
    leader, successor, third = runtimes
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    store = leader.local.store
    node_id = third.node.voter_id
    facade.create_enrollment_token(max_uses=1)
    announcement = CapabilityAnnouncement(
        capability_id="pending-health-capability", node_id=node_id,
        session_id=SESSION, type="demo.pending-health", protocol="demo.pending-health",
        protocol_version="1", status=CapabilityStatus.READY,
        properties={"purpose": "pending local health regression"}, announced_at=leader.clock(),
    )
    facade.announce_capability(
        announcement, actor_node_id=node_id, request_id="announce-pending-health",
    )
    for runtime in runtimes:
        runtime.materialize()
        store.mark_connected(
            node_id=runtime.node.voter_id, connection_id=f"local-{runtime.node.voter_id}",
            now=leader.clock() - timedelta(minutes=1) if runtime is third else leader.clock(),
        )
    before_history = _history(leader)
    before_state = copy.deepcopy(leader.node.state)
    before_commit = leader.node.store.commit_index
    before_private = _private_digest(leader)
    original_propose = leader.node.propose
    proposed = []

    def elect_after_outbox_save(command, transport):
        assert command.command_type == "PRODUCT_TRANSACTION"
        pending = _pending_row(leader)
        assert pending is not None
        assert pending[0].encode() == command.canonical_bytes()
        assert successor.node.start_election(successor.transport)
        assert successor.node.synchronize(successor.transport) == 2
        assert leader.node.role == ReplicaNode.FOLLOWER
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.store.current_term > pending[2]
        proposed.append(command.command_id)
        # The real old leader rejects the append; no consensus result is mocked.
        return original_propose(command, transport)

    with monkeypatch.context() as patch:
        patch.setattr(leader.node, "propose", elect_after_outbox_save)
        with pytest.raises(StaleTerm, match="only the current leader"):
            facade.create_enrollment_token(max_uses=1)
    assert len(proposed) == 1
    pending = _pending_row(leader)
    assert pending is not None
    assert leader.node.store.receipt_for_command(proposed[0]) is None
    assert leader.node.store.entry_for_command(proposed[0]) is None
    assert _private_digest(leader) == before_private

    def assert_authority_unchanged():
        assert _pending_row(leader) == pending
        assert _private_digest(leader) == before_private
        assert _history(leader) == before_history
        for runtime in runtimes:
            assert runtime.node.state == before_state
            assert runtime.node.store.commit_index == before_commit
            assert runtime.node.store.last_log_index() == before_commit
            store.require_membership(session_id=SESSION, node_id=runtime.node.voter_id)
        assert next(
            item for item in store.list_capabilities(session_id=SESSION)
            if item.capability_id == announcement.capability_id
        ).status is CapabilityStatus.READY

    assert facade.sweep_stale(heartbeat_timeout_seconds=30) == ((node_id,), ())
    with store.read_transaction() as database:
        assert tuple(database.execute(
            "SELECT state,connection_id,last_error FROM node_connectivity WHERE node_id=?",
            (node_id,),
        ).fetchone()) == ("disconnected", None, "stale heartbeat")
    assert facade.disconnected(node_id=successor.node.voter_id) == ()
    assert facade.relay_started() is None
    assert facade.sweep_stale(heartbeat_timeout_seconds=30) == ((), ())
    with store.read_transaction() as database:
        assert database.execute(
            "SELECT COUNT(*) FROM node_connectivity WHERE state='connected'",
        ).fetchone()[0] == 0
    assert_authority_unchanged()

    forbidden = (
        "UPDATE session_events SET payload_json=payload_json",
        "UPDATE enrollment_tokens SET use_count=use_count+1",
        "UPDATE capabilities SET status='unavailable'",
        "DELETE FROM session_memberships",
        "UPDATE node_connectivity SET node_id=node_id",
        "INSERT INTO node_connectivity(node_id,state) VALUES('foreign','connected')",
        "DELETE FROM node_connectivity",
        "CREATE TABLE forbidden_health_table(value TEXT)",
        "DROP TRIGGER fcp_c03_journal_guard_update",
        "ATTACH DATABASE ':memory:' AS forbidden_health_database",
        "PRAGMA user_version=77",
    )
    for statement in forbidden:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized|prohibited"), leader.journal.local_connectivity_operation() as database:
            database.execute(
                "UPDATE node_connectivity SET state='connected' WHERE node_id=?", (node_id,),
            )
            database.execute(statement)
        with store.read_transaction() as database:
            assert database.execute(
                "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,),
            ).fetchone()[0] == "disconnected"
        assert_authority_unchanged()

    with pytest.raises(RuntimeError, match="rollback-only"), leader.journal.local_connectivity_operation() as database:
        database.execute(
            "UPDATE node_connectivity SET state='connected' WHERE node_id=?", (node_id,),
        )
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            database.execute("DELETE FROM session_memberships")
    with store.read_transaction() as database:
        assert database.execute(
            "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,),
        ).fetchone()[0] == "disconnected"

    with (
        leader._lifecycle_lock,
        store.raw_transaction(),
        pytest.raises(RuntimeError, match="cannot inherit an active transaction"),
        leader.journal.local_connectivity_operation(),
    ):
        pytest.fail("local-only context inherited an authority stage")
    with pytest.raises(AuthorizationError) as rejected:
        facade.create_enrollment_token(max_uses=1)
    assert rejected.value.code == "federation-quorum-leader-required"
    assert_authority_unchanged()

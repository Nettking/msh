"""Bootstrap retries use real persistent consensus, including prior-term logs.

The existing deterministic transport exercises the consensus protocol without
claiming network authentication; socket-level release tests cover that boundary.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from catalog.federation.control_plane_bootstrap_proposal import (
    propose_bootstrap_command,
)
from catalog.federation.control_plane_journal import (
    PRODUCT_JOURNAL_INITIALIZE,
    journal_prefix_digest,
)
from catalog.federation.control_plane_replication import (
    ControlPlaneError,
    DuplicateCommandError,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
    StaleTerm,
)
from catalog.federation.tests.test_control_plane_replication import (
    CONFIGURATION,
    _command,
    _elected_cluster,
    _genesis,
    _replicas,
)


def test_fresh_bootstrap_commits_the_exact_genesis_on_a_real_quorum(tmp_path: Path) -> None:
    nodes, transport = _replicas(tmp_path)
    leader = nodes["voter-a"]
    assert leader.start_election(transport)
    command = _genesis()
    entry, _events = propose_bootstrap_command(leader, transport, command)
    assert entry.command == command
    for node in nodes.values():
        assert node.store.receipt_for_command(command.command_id).content_hash == command.content_hash
        assert node.state["federation_id"] == command.payload["federation_id"]


@pytest.mark.parametrize("compact_and_restart", [False, True])
def test_successor_reuses_exact_committed_bootstrap_identity(
    tmp_path: Path, compact_and_restart: bool,
) -> None:
    nodes, transport = _elected_cluster(tmp_path)
    original = _genesis()
    successor = nodes["voter-b"]
    if compact_and_restart:
        successor.compact()
        successor = ReplicaNode(
            "voter-b", PersistentReplicaStore(successor.store.database, CONFIGURATION),
        )
        nodes["voter-b"] = successor
        transport.replicas["voter-b"] = successor
        assert successor.store.entry_for_command(original.command_id) is None
    transport.partition("voter-a")
    assert successor.start_election(transport)
    last_index = successor.store.last_log_index()
    entry, _events = propose_bootstrap_command(
        successor, transport, replace(original, issued_by="voter-b"),
    )
    assert entry.command == original
    assert successor.store.last_log_index() == last_index
    assert successor.store.receipt_for_command(original.command_id).content_hash == original.content_hash
    assert successor.state == nodes["voter-c"].state


@pytest.mark.parametrize("field", ["display_name", "federation_id", "session_id"])
def test_successor_cannot_reinterpret_a_committed_bootstrap_payload(tmp_path: Path, field: str) -> None:
    nodes, transport = _elected_cluster(tmp_path)
    successor = nodes["voter-b"]
    successor.compact()
    transport.partition("voter-a")
    assert successor.start_election(transport)
    original = _genesis()
    before = successor.state
    conflict = replace(original, issued_by="voter-b", payload={**original.payload, field: "changed"})
    with pytest.raises(DuplicateCommandError, match="payload conflict"):
        propose_bootstrap_command(successor, transport, conflict)
    assert successor.state == before
    assert successor.store.last_log_index() == 1


def test_an_old_receipt_does_not_replace_current_quorum(tmp_path: Path) -> None:
    nodes, transport = _elected_cluster(tmp_path)
    leader = nodes["voter-a"]
    transport.partition("voter-a")
    with pytest.raises(QuorumUnavailable, match="current voter quorum"):
        propose_bootstrap_command(leader, transport, _genesis())
    assert leader.store.last_log_index() == 1


@pytest.mark.parametrize("successor_id", ["voter-a", "voter-b"])
def test_inherited_uncommitted_genesis_requires_a_real_current_term_barrier(
    tmp_path: Path, successor_id: str,
) -> None:
    nodes, transport = _replicas(tmp_path)
    original_leader = nodes["voter-a"]
    assert original_leader.start_election(transport)
    transport.partition("voter-a")
    command = _genesis()
    with pytest.raises(QuorumUnavailable):
        original_leader.propose(command, transport)
    pending = original_leader.store.entry_for_command(command.command_id)
    assert pending is not None
    assert original_leader.store.commit_index == 0
    if successor_id == "voter-b":
        # The follower durably receives the old leader's append, but the old
        # leader disappears before receiving its acknowledgement/committing.
        response = nodes[successor_id].receive_append_entries(
            leader_id="voter-a", leader_term=pending.log_term,
            prev_log_index=0, prev_log_term=0, entries=(pending,),
            leader_commit=0, cluster_id=CONFIGURATION.cluster_id,
        )
        assert response.success
    else:
        transport.heal()
    successor = ReplicaNode(
        successor_id, PersistentReplicaStore(nodes[successor_id].store.database, CONFIGURATION),
    )
    nodes[successor_id] = successor
    transport.replicas[successor_id] = successor
    assert successor.start_election(transport)
    assert successor.store.current_term > pending.log_term
    entry, _events = propose_bootstrap_command(
        successor, transport, replace(command, issued_by=successor_id),
    )
    assert entry == pending
    assert successor.store.commit_index == pending.log_index + 1
    barrier = successor.store.entries(after=pending.log_index)[0]
    assert barrier.log_term == successor.store.current_term
    assert barrier.command.command_type == "NODE_ENROLL"
    assert barrier.command.payload == successor.state["nodes"][successor_id]
    assert successor.store.receipt_for_command(command.command_id).content_hash == command.content_hash
    assert set(successor.state["nodes"]) == set(CONFIGURATION.voter_ids)


def test_follower_and_non_bootstrap_commands_are_refused(tmp_path: Path) -> None:
    nodes, transport = _elected_cluster(tmp_path)
    with pytest.raises(StaleTerm, match="current leader"):
        propose_bootstrap_command(nodes["voter-b"], transport, _genesis())
    ordinary = _command("ordinary-enroll", "NODE_ENROLL", {
        "node_id": "node-new", "display_name": "New", "public_key": "public-new",
    })
    with pytest.raises(ControlPlaneError, match="supported bootstrap"):
        propose_bootstrap_command(nodes["voter-a"], transport, ordinary)
    assert nodes["voter-a"].store.last_log_index() == 1


def test_committed_genesis_retry_settles_an_inherited_different_target_promotion(
    tmp_path: Path,
) -> None:
    nodes, transport = _elected_cluster(tmp_path)
    session_id = _genesis().payload["session_id"]
    nodes["voter-a"].propose(_command("initialize-public-journal", PRODUCT_JOURNAL_INITIALIZE, {
        "session_id": session_id, "expected_revision": 0,
        "prefix_digest": journal_prefix_digest([]), "public_rows": [], "final": True,
    }), transport)
    interrupted = nodes["voter-b"]
    assert interrupted.start_election(transport)
    transport.partition("voter-b")
    promotion = _command("interrupted-bootstrap-promotion-b", "LEADER_TRANSITION", {
        "session_id": session_id, "previous_leader_node_id": "voter-a",
        "leader_node_id": "voter-b", "term": 2,
        "occurred_at": "2026-09-08T09:00:00+00:00",
        "reason": "replicated-quorum-bootstrap-recovery",
    }, issued_by="voter-b")
    with pytest.raises(QuorumUnavailable):
        interrupted.propose(promotion, transport)
    pending = interrupted.store.entry_for_command(promotion.command_id)
    assert pending is not None
    successor = nodes["voter-c"]
    # C receives the exact durable append, but B disappears before the ACK
    # can establish a commit. No synthetic receipt or commit index is installed.
    response = successor.receive_append_entries(
        leader_id="voter-b", leader_term=pending.log_term,
        prev_log_index=pending.log_index - 1,
        prev_log_term=interrupted.store.term_at(pending.log_index - 1),
        entries=(pending,), leader_commit=interrupted.store.commit_index,
        cluster_id=CONFIGURATION.cluster_id,
    )
    assert response.success
    assert successor.store.receipt_for_command(promotion.command_id) is None
    assert successor.start_election(transport)
    assert successor.store.current_term > pending.log_term
    assert successor.state["leaders"][session_id]["leader_node_id"] == "voter-a"

    recovered, _events = propose_bootstrap_command(
        successor, transport, replace(_genesis(), issued_by="voter-c"),
    )
    assert recovered.command == _genesis()
    assert successor.store.entry_for_command(promotion.command_id) == pending
    assert successor.store.receipt_for_command(promotion.command_id).content_hash == promotion.content_hash
    assert successor.store.commit_index == pending.log_index + 1
    barrier = successor.store.entries(after=pending.log_index)[0]
    assert barrier.command.command_type == "NODE_ENROLL"
    assert barrier.log_term == successor.store.current_term
    assert successor.state["leaders"][session_id]["leader_node_id"] == "voter-b"
    inherited_rows = successor.state["product_journal"]["sessions"][session_id]["rows"]
    assert len(inherited_rows) == 1
    assert inherited_rows[0]["event_type"] == "session.leader.changed"

    next_promotion = _command("following-bootstrap-promotion-c", "LEADER_TRANSITION", {
        "session_id": session_id, "previous_leader_node_id": "voter-b",
        "leader_node_id": "voter-c", "term": 3,
        "occurred_at": "2026-09-08T09:00:01+00:00",
        "reason": "replicated-quorum-bootstrap-recovery",
    }, issued_by="voter-c")
    propose_bootstrap_command(successor, transport, next_promotion)
    rows = successor.state["product_journal"]["sessions"][session_id]["rows"]
    assert rows[:1] == inherited_rows
    assert [row["revision"] for row in rows] == [1, 2]
    assert len({row["event_id"] for row in rows}) == 2
    assert successor.state["leaders"][session_id]["term"] == 3
    assert successor.state == nodes["voter-a"].state

"""Phase 1 tests for the bounded replicated Federation authority foundation."""

from __future__ import annotations

from pathlib import Path

import pytest

from catalog.federation.control_plane_replication import (
    AuthorityCommand,
    AuthorityStateMachine,
    ControlPlaneError,
    DuplicateCommandError,
    InProcessTransport,
    LogConflict,
    LogEntry,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
    Snapshot,
    StaleTerm,
    VoterConfiguration,
)

VOTERS = ("voter-a", "voter-b", "voter-c")
CONFIGURATION = VoterConfiguration("cluster-test", VOTERS)


def _stores(tmp_path: Path) -> dict[str, PersistentReplicaStore]:
    return {
        voter: PersistentReplicaStore(tmp_path / f"{voter}.sqlite3", CONFIGURATION)
        for voter in VOTERS
    }


def _replicas(tmp_path: Path) -> tuple[dict[str, ReplicaNode], InProcessTransport]:
    replicas = {
        voter: ReplicaNode(voter, store) for voter, store in _stores(tmp_path).items()
    }
    return replicas, InProcessTransport(replicas)


def _command(
    command_id: str,
    command_type: str,
    payload: dict[str, object],
    *,
    issued_by: str = "voter-a",
) -> AuthorityCommand:
    return AuthorityCommand(
        command_id=command_id,
        command_type=command_type,
        cluster_id=CONFIGURATION.cluster_id,
        issued_by=issued_by,
        payload=payload,
    )


def _genesis() -> AuthorityCommand:
    nodes = [
        {"node_id": voter, "display_name": voter, "public_key": f"key-{voter}"}
        for voter in VOTERS
    ]
    return _command(
        "genesis-1",
        "FEDERATION_GENESIS",
        {
            "federation_id": "federation-stable",
            "session_id": "session-stable",
            "creator_node_id": "voter-a",
            "display_name": "Acceptance Federation",
            "voter_ids": list(VOTERS),
            "nodes": nodes,
            "members": list(VOTERS),
        },
    )


def _elected_cluster(
    tmp_path: Path,
) -> tuple[dict[str, ReplicaNode], InProcessTransport]:
    replicas, transport = _replicas(tmp_path)
    assert replicas["voter-a"].start_election(transport)
    replicas["voter-a"].propose(_genesis(), transport)
    return replicas, transport


def test_command_envelope_is_deterministic_and_bounded() -> None:
    command = _command(
        "command-1",
        "NODE_ENROLL",
        {"node_id": "node-1", "display_name": "Node 1", "public_key": "key-1"},
    )
    assert (
        command.canonical_bytes()
        == AuthorityCommand.from_dict(command.to_dict()).canonical_bytes()
    )
    with pytest.raises(ControlPlaneError, match="unsupported-voter-reconfiguration"):
        AuthorityCommand(
            command_id="command-2",
            command_type="VOTER_CONFIG_CHANGE",
            cluster_id=CONFIGURATION.cluster_id,
            issued_by="voter-a",
            payload={},
        )


def test_term_and_vote_survive_restart(tmp_path: Path) -> None:
    store = PersistentReplicaStore(tmp_path / "replica.sqlite3", CONFIGURATION)
    store.set_term_and_vote(7, "voter-b")
    restarted = PersistentReplicaStore(tmp_path / "replica.sqlite3", CONFIGURATION)
    assert restarted.current_term == 7
    assert restarted.voted_for == "voter-b"


def test_three_voter_election_requires_quorum(tmp_path: Path) -> None:
    replicas, transport = _replicas(tmp_path)
    transport.blocked.update({("voter-a", "voter-b"), ("voter-a", "voter-c")})
    assert replicas["voter-a"].start_election(transport) is False
    assert replicas["voter-a"].role == ReplicaNode.FOLLOWER

    transport.blocked.remove(("voter-a", "voter-b"))
    assert replicas["voter-a"].start_election(transport) is True
    assert replicas["voter-a"].role == ReplicaNode.LEADER


def test_voter_votes_once_per_term_and_rejects_old_term(tmp_path: Path) -> None:
    replicas, transport = _replicas(tmp_path)
    assert replicas["voter-a"].start_election(transport)
    assert replicas["voter-b"].store.voted_for == "voter-a"
    assert replicas["voter-c"].store.voted_for == "voter-a"
    assert (
        replicas["voter-b"].receive_vote_request(
            candidate_id="voter-c",
            term=1,
            last_log_index=0,
            last_log_term=0,
            cluster_id=CONFIGURATION.cluster_id,
        ).granted
        is False
    )
    assert (
        replicas["voter-b"].receive_vote_request(
            candidate_id="voter-c",
            term=0,
            last_log_index=0,
            last_log_term=0,
            cluster_id=CONFIGURATION.cluster_id,
        ).granted
        is False
    )


def test_outdated_candidate_log_cannot_win(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    candidate = replicas["voter-b"]
    assert (
        candidate.receive_vote_request(
            candidate_id="voter-b",
            term=2,
            last_log_index=0,
            last_log_term=0,
            cluster_id=CONFIGURATION.cluster_id,
        ).granted
        is False
    )
    assert candidate.store.current_term == 2


def test_normal_append_requires_quorum_and_converges(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    command = _command(
        "enroll-1",
        "NODE_ENROLL",
        {"node_id": "new-node", "display_name": "New", "public_key": "key-new"},
    )
    entry, _events = replicas["voter-a"].propose(command, transport)
    assert entry.log_index == 2
    assert all(
        replica.state["nodes"].get("new-node") == command.payload
        for replica in replicas.values()
    )
    assert len({replica.store.commit_index for replica in replicas.values()}) == 1


def test_quorum_loss_blocks_authority_commit(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    transport.blocked.update({("voter-a", "voter-b"), ("voter-a", "voter-c")})
    with pytest.raises(QuorumUnavailable):
        replicas["voter-a"].propose(
            _command(
                "enroll-blocked",
                "NODE_ENROLL",
                {"node_id": "blocked", "display_name": "Blocked", "public_key": "key"},
            ),
            transport,
        )
    assert replicas["voter-a"].store.commit_index == 1


def test_prefix_mismatch_and_committed_overwrite_are_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-b"]
    with pytest.raises(LogConflict, match="prefix"):
        follower.store.append_entries(
            (), prev_log_index=1, prev_log_term=999, leader_commit=1
        )
    committed = follower.store.entries(after=0)[0]
    conflicting = LogEntry(
        committed.log_index,
        committed.log_term + 1,
        _command(
            "different",
            "NODE_ENROLL",
            {"node_id": "x", "display_name": "X", "public_key": "k"},
        ),
    )
    with pytest.raises(LogConflict, match="committed"):
        follower.store.append_entries(
            (conflicting,),
            prev_log_index=0,
            prev_log_term=0,
            leader_commit=1,
        )


def test_divergent_uncommitted_suffix_is_replaced(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    divergent = LogEntry(
        2,
        1,
        _command(
            "old-suffix",
            "NODE_ENROLL",
            {"node_id": "old", "display_name": "Old", "public_key": "old-key"},
        ),
    )
    follower.store.append_entries(
        (divergent,), prev_log_index=1, prev_log_term=1, leader_commit=1
    )
    replacement = LogEntry(
        2,
        replicas["voter-a"].store.current_term,
        _command(
            "new-suffix",
            "NODE_ENROLL",
            {"node_id": "new", "display_name": "New", "public_key": "new-key"},
        ),
    )
    assert follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=replicas["voter-a"].store.current_term,
        prev_log_index=1,
        prev_log_term=1,
        entries=(replacement,),
        leader_commit=1,
        cluster_id=CONFIGURATION.cluster_id,
    ).success
    assert follower.store.entries(after=1)[0].command.command_id == "new-suffix"


def test_duplicate_command_replay_is_safe_but_conflict_fails(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    command = _command(
        "same-id",
        "NODE_ENROLL",
        {"node_id": "same", "display_name": "Same", "public_key": "key"},
    )
    first, _ = replicas["voter-a"].propose(command, transport)
    second, _ = replicas["voter-a"].propose(command, transport)
    assert first == second
    with pytest.raises(DuplicateCommandError):
        replicas["voter-a"].propose(
            _command(
                "same-id",
                "NODE_ENROLL",
                {
                    "node_id": "different",
                    "display_name": "Different",
                    "public_key": "key-2",
                },
            ),
            transport,
        )


def test_state_machine_replay_is_deterministic_and_creator_is_immutable() -> None:
    machine = AuthorityStateMachine(CONFIGURATION)
    state = machine.initial_state()
    state, _ = machine.apply(state, _genesis())
    state, events = machine.apply(
        state,
        _command(
            "leader-2",
            "LEADER_TRANSITION",
            {
                "session_id": "session-stable",
                "previous_leader_node_id": "voter-a",
                "leader_node_id": "voter-b",
                "term": 2,
                "reason": "failure",
                "occurred_at": "2026-09-07T00:00:00Z",
            },
        ),
    )
    assert events[0]["event_type"] == "session.leader.changed"
    assert state["federation_id"] == "federation-stable"
    assert state["sessions"]["session-stable"]["creator_node_id"] == "voter-a"
    assert state["leaders"]["session-stable"]["leader_node_id"] == "voter-b"
    with pytest.raises(ControlPlaneError):
        machine.apply(
            state,
            _command(
                "genesis-2",
                "FEDERATION_GENESIS",
                {**_genesis().payload, "federation_id": "other"},
            ),
        )


def test_snapshot_survives_restart_and_rejects_older_snapshot(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    replicas["voter-a"].propose(
        _command(
            "enroll-snapshot",
            "NODE_ENROLL",
            {"node_id": "snap", "display_name": "Snapshot", "public_key": "snap-key"},
        ),
        transport,
    )
    snapshot = replicas["voter-a"].create_snapshot()
    replicas["voter-b"].install_snapshot(snapshot)
    restarted = PersistentReplicaStore(tmp_path / "voter-b.sqlite3", CONFIGURATION)
    assert restarted.snapshot() == snapshot
    assert restarted.projection()["nodes"]["snap"]["node_id"] == "snap"
    with pytest.raises(ControlPlaneError, match="older"):
        restarted.install_snapshot(
            Snapshot(
                cluster_id=CONFIGURATION.cluster_id,
                voter_configuration=CONFIGURATION,
                last_included_index=1,
                last_included_term=1,
                state=replicas["voter-b"].state,
                digest=snapshot.digest,
            )
        )


def test_stale_leader_is_fenced_after_higher_term(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    old_term = replicas["voter-a"].store.current_term
    assert replicas["voter-b"].start_election(transport) is True
    assert replicas["voter-b"].store.current_term > old_term
    with pytest.raises(StaleTerm):
        replicas["voter-a"].propose(
            _command(
                "stale",
                "NODE_ENROLL",
                {
                    "node_id": "stale",
                    "display_name": "Stale",
                    "public_key": "stale-key",
                },
            ),
            transport,
        )

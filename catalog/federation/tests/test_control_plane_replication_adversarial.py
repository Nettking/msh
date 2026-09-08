"""Isolated regressions for Astra's rejected Phase-1 checkpoint.

All replica databases live in pytest temporary directories. No host transport.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from catalog.federation.control_plane_replication import (
    AppendResponse,
    ControlPlaneError,
    DuplicateCommandError,
    LogConflict,
    LogEntry,
    PersistentReplicaStore,
    QuorumUnavailable,
    ReplicaNode,
    StaleTerm,
    VoteResponse,
)
from catalog.federation.tests.test_control_plane_replication import (
    CONFIGURATION,
    VOTERS,
    _command,
    _elected_cluster,
    _genesis,
    _replicas,
)


def enroll(number):
    return _command(
        f"enroll-{number}",
        "NODE_ENROLL",
        {
            "node_id": f"node-{number}",
            "display_name": f"Node {number}",
            "public_key": f"key-{number}",
        },
    )


def restart(replicas, transport, voter):
    old = replicas[voter]
    new = ReplicaNode(voter, PersistentReplicaStore(old.store.database, CONFIGURATION))
    replicas[voter] = transport.replicas[voter] = new
    return new


def converge(replicas):
    assert len({r.store.commit_index for r in replicas.values()}) == 1
    first = replicas[VOTERS[0]].state
    assert all(r.state == first for r in replicas.values())


@pytest.mark.parametrize("restart_pending", [False, True])
def test_pending_retry_never_reports_commit_without_quorum(tmp_path, restart_pending):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    transport.blocked.update((VOTERS[0], v) for v in VOTERS[1:])
    with pytest.raises(QuorumUnavailable):
        leader.propose(enroll(1), transport)
    if restart_pending:
        leader = restart(replicas, transport, VOTERS[0])
        transport.blocked.clear()
        assert leader.start_election(transport)
        transport.blocked.update((VOTERS[0], v) for v in VOTERS[1:])
    for _ in range(3):
        with pytest.raises(QuorumUnavailable):
            leader.propose(enroll(1), transport)
        assert leader.store.commit_index == 1
    transport.blocked.clear()
    if restart_pending:
        # Current-term command commits the inherited pending prefix safely.
        leader.propose(enroll(2), transport)
    entry, _ = leader.propose(enroll(1), transport)
    assert leader.store.commit_index >= entry.log_index
    converge(replicas)


@pytest.mark.parametrize("install_on_other", [False, True])
def test_receipts_survive_compaction_install_and_restart(tmp_path, install_on_other):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    original, _ = leader.propose(enroll(1), transport)
    snapshot = leader.create_snapshot()
    target = VOTERS[1] if install_on_other else VOTERS[0]
    replicas[target].install_snapshot(snapshot)
    assert not replicas[target].store.entries()
    new = restart(replicas, transport, target)
    assert new.start_election(transport)
    before = new.state
    repeated, events = new.propose(enroll(1), transport)
    assert repeated == original and events == () and new.state == before
    with pytest.raises(DuplicateCommandError):
        new.propose(replace(enroll(2), command_id=enroll(1).command_id), transport)


def test_snapshot_cannot_lose_applied_authority(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    old = leader.create_snapshot()
    leader.propose(enroll(1), transport)
    before = leader.state
    with pytest.raises(ControlPlaneError, match="older"):
        leader.install_snapshot(old)
    assert leader.state == before
    assert restart(replicas, transport, VOTERS[0]).state == before


@pytest.mark.parametrize("missing", [1, 4])
@pytest.mark.parametrize("restart_behind", [False, True])
@pytest.mark.parametrize("compact", [False, True])
def test_disconnected_follower_catches_up(tmp_path, missing, restart_behind, compact):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    transport.blocked.add((VOTERS[0], VOTERS[2]))
    for number in range(missing):
        leader.propose(enroll(number), transport)
    if compact:
        leader.install_snapshot(leader.create_snapshot())
    leader.propose(enroll(100), transport)
    if restart_behind:
        restart(replicas, transport, VOTERS[2])
    transport.blocked.clear()
    leader.replicate(transport)
    converge(replicas)


def test_divergent_follower_repaired_by_leader(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    follower = replicas[VOTERS[2]]
    follower.store.append_local(LogEntry(2, 1, enroll("divergent")))
    assert replicas[VOTERS[0]].start_election(transport)
    replicas[VOTERS[0]].propose(enroll("real"), transport)
    converge(replicas)
    assert "node-divergent" not in follower.state["nodes"]


def test_short_append_cannot_commit_divergent_suffix(tmp_path):
    replicas, _ = _elected_cluster(tmp_path)
    follower = replicas[VOTERS[1]]
    follower.store.append_local(LogEntry(2, 1, enroll("divergent")))
    response = follower.receive_append_entries(
        leader_id=VOTERS[0],
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(),
        leader_commit=2,
        cluster_id=CONFIGURATION.cluster_id,
    )
    assert response.success and response.match_index == 1
    assert follower.store.commit_index == 1


@pytest.mark.parametrize(
    "case", ["gap", "reorder", "cluster", "future-term", "decreasing-term"]
)
def test_malformed_append_is_atomic(tmp_path, case):
    replicas, _ = _elected_cluster(tmp_path)
    follower = replicas[VOTERS[1]]
    entry = LogEntry(2, 1, enroll(1))
    entries = (entry,)
    if case == "gap":
        entries = (replace(entry, log_index=3),)
    elif case == "reorder":
        entries = (replace(entry, log_index=3), entry)
    elif case == "cluster":
        entries = (replace(entry, command=replace(entry.command, cluster_id="wrong")),)
    elif case == "future-term":
        entries = (replace(entry, log_term=4),)
    elif case == "decreasing-term":
        entries = (replace(entry, log_term=2), LogEntry(3, 1, enroll(2)))
    before = follower.store.entries()
    response = follower.receive_append_entries(
        leader_id=VOTERS[0],
        leader_term=2,
        prev_log_index=1,
        prev_log_term=1,
        entries=entries,
        leader_commit=3,
        cluster_id=CONFIGURATION.cluster_id,
    )
    assert not response.success
    assert follower.store.entries() == before and follower.store.commit_index == 1


def test_invalid_domain_command_never_enters_log(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    command = _command(
        "bad",
        "LEADER_TRANSITION",
        {
            "session_id": "missing",
            "previous_leader_node_id": VOTERS[0],
            "leader_node_id": VOTERS[1],
            "term": 2,
            "reason": "failover",
            "occurred_at": "2026-09-08T00:00:00Z",
        },
    )
    with pytest.raises(ControlPlaneError):
        leader.propose(command, transport)
    assert (
        leader.store.last_log_index()
        == leader.store.commit_index
        == leader.store.last_applied
        == 1
    )
    leader.propose(enroll(1), transport)
    converge(replicas)


def test_validation_includes_pending_predecessors(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    transport.blocked.update((VOTERS[0], v) for v in VOTERS[1:])
    with pytest.raises(QuorumUnavailable):
        leader.propose(enroll(1), transport)
    transport.blocked.clear()
    leader.propose(
        _command(
            "join",
            "SESSION_MEMBER_ADD",
            {
                "session_id": "session-stable",
                "node_id": "node-1",
                "occurred_at": "2026-09-08T00:00:00Z",
            },
        ),
        transport,
    )
    converge(replicas)
    assert leader.state["memberships"]["session-stable"]["node-1"]


@pytest.mark.parametrize("kind", ["vote", "append"])
def test_higher_term_response_fences_sender(tmp_path, kind):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    if kind == "vote":
        transport.request_vote = lambda *a, **kw: VoteResponse(50, False)
        assert not leader.start_election(transport)
    else:
        transport.append_entries = lambda *a, **kw: AppendResponse(50, False, 0)
        with pytest.raises(StaleTerm):
            leader.propose(enroll(1), transport)
    assert leader.store.current_term == 50 and leader.role == ReplicaNode.FOLLOWER


@pytest.mark.parametrize("kind", ["vote", "append"])
def test_delayed_success_after_term_advance_is_not_success(tmp_path, kind):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]

    def response(*args, **kwargs):
        leader.receive_vote_request(
            candidate_id=VOTERS[1],
            term=50,
            last_log_index=100,
            last_log_term=49,
            cluster_id=CONFIGURATION.cluster_id,
        )
        return (
            VoteResponse(kwargs["term"], True)
            if kind == "vote"
            else AppendResponse(kwargs["leader_term"], True, 2)
        )

    if kind == "vote":
        transport.request_vote = response
        assert not leader.start_election(transport)
    else:
        transport.append_entries = response
        with pytest.raises(StaleTerm):
            leader.propose(enroll(1), transport)
    assert leader.role == ReplicaNode.FOLLOWER


@pytest.mark.parametrize("iteration", range(5))
def test_concurrent_votes_are_durable_and_exclusive(tmp_path, iteration):
    replicas, transport = _replicas(tmp_path)
    voter = replicas[VOTERS[2]]
    barrier = Barrier(2)

    def vote(candidate):
        barrier.wait(timeout=5)
        return voter.receive_vote_request(
            candidate_id=candidate,
            term=7,
            last_log_index=0,
            last_log_term=0,
            cluster_id=CONFIGURATION.cluster_id,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(vote, VOTERS[:2]))
    assert sum(r.granted for r in results) == 1
    winner = VOTERS[results.index(next(r for r in results if r.granted))]
    assert restart(replicas, transport, VOTERS[2]).store.voted_for == winner


@pytest.mark.parametrize(
    "field", ["password", "relay_url", "cluster_term", "private_key"]
)
def test_unknown_fields_rejected_before_append(field):
    with pytest.raises(ControlPlaneError, match="fields"):
        replace(enroll(1), payload={**enroll(1).payload, field: "private-value"})


@pytest.mark.parametrize(
    "change", ["voters", "creator", "duplicate-node", "duplicate-key", "unknown-member"]
)
def test_invalid_genesis_rejected(tmp_path, change):
    replicas, transport = _replicas(tmp_path)
    leader = replicas[VOTERS[0]]
    assert leader.start_election(transport)
    payload = _genesis().payload
    if change == "voters":
        payload["voter_configuration"] = {
            **CONFIGURATION.to_dict(),
            "voter_ids": ["x", "y", "z"],
        }
    elif change == "creator":
        payload["creator_node_id"] = "unknown"
    elif change == "duplicate-node":
        payload["nodes"].append(payload["nodes"][0])
    elif change == "duplicate-key":
        payload["nodes"][1]["public_key"] = payload["nodes"][0]["public_key"]
    else:
        payload["members"].append("unknown")
    with pytest.raises(ControlPlaneError):
        leader.propose(replace(_genesis(), payload=payload), transport)
    assert leader.store.last_log_index() == 0


def test_conflicting_second_genesis_rejected(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    with pytest.raises(ControlPlaneError, match="genesis"):
        replicas[VOTERS[0]].propose(
            replace(_genesis(), command_id="other-genesis"), transport
        )


def test_public_projection_excludes_keys_and_rejects_location(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    payload = {
        "session_id": "new-session",
        "creator_node_id": VOTERS[0],
        "display_name": "Safe name",
        "occurred_at": "2026-09-08T00:00:00Z",
    }
    leader.propose(_command("new-session", "SESSION_CREATE", payload), transport)
    event = leader.state["session_events"]["new-session"][1]
    assert event["payload"] == {"node_id": VOTERS[0]}
    with pytest.raises(ControlPlaneError, match="nonpublic"):
        leader.propose(
            _command(
                "private",
                "SESSION_CREATE",
                {
                    **payload,
                    "session_id": "private-session",
                    "display_name": "192.168.1.2:5000",
                },
            ),
            transport,
        )


def test_revoked_actor_and_capability_ownership(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    capability = {
        "session_id": "session-stable",
        "capability_id": "cap-one",
        "node_id": VOTERS[1],
        "capability_type": "recorder",
        "display_name": "Recorder",
        "occurred_at": "2026-09-08T00:00:00Z",
    }
    leader.propose(
        _command("cap", "CAPABILITY_DECLARE", capability, issued_by=VOTERS[1]),
        transport,
    )
    with pytest.raises(ControlPlaneError, match="owner"):
        leader.propose(
            _command(
                "steal", "CAPABILITY_DECLARE", {**capability, "node_id": VOTERS[0]}
            ),
            transport,
        )
    with pytest.raises(ControlPlaneError, match="owner"):
        leader.propose(
            _command(
                "withdraw",
                "CAPABILITY_WITHDRAW",
                {
                    k: capability[k]
                    for k in ("session_id", "capability_id", "node_id", "occurred_at")
                },
            ),
            transport,
        )
    leader.propose(
        _command(
            "revoke",
            "NODE_REVOKE",
            {
                "node_id": VOTERS[1],
                "reason": "retired",
                "occurred_at": "2026-09-08T00:00:00Z",
            },
        ),
        transport,
    )
    with pytest.raises(ControlPlaneError, match="eligible"):
        leader.propose(
            _command("revoked", "CAPABILITY_DECLARE", capability, issued_by=VOTERS[1]),
            transport,
        )


def test_snapshot_preserves_compatible_pending_suffix(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    snapshot = leader.create_snapshot()
    leader.store.append_local(LogEntry(2, 1, enroll(1)))
    leader.install_snapshot(snapshot)
    assert leader.store.entries()[0].log_index == 2
    leader.propose(enroll(1), transport)
    converge(replicas)


def test_snapshot_discards_incompatible_pending_suffix(tmp_path):
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    follower = replicas[VOTERS[2]]
    follower.store.append_local(LogEntry(2, 1, enroll("wrong")))
    follower.store.append_local(LogEntry(3, 1, enroll("wrong-later")))
    transport.blocked.add((VOTERS[0], VOTERS[2]))
    assert leader.start_election(transport)
    leader.propose(enroll("right"), transport)
    follower.install_snapshot(leader.create_snapshot())
    assert follower.store.entries() == ()
    assert "node-wrong" not in follower.state["nodes"]


def test_equal_index_different_term_snapshot_rejected(tmp_path):
    replicas, _ = _elected_cluster(tmp_path)
    leader = replicas[VOTERS[0]]
    with pytest.raises(LogConflict, match="term"):
        leader.install_snapshot(replace(leader.create_snapshot(), last_included_term=2))

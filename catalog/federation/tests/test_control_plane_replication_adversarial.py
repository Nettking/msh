"""Adversarial Phase 1 tests for the replicated Federation authority log.

Every test here targets one of the seven blocking findings raised against the
rejected Phase 1 checkpoint.  The suite is deliberately hostile: it partitions
the cluster, restarts replicas, replays commands, forges snapshots, races
votes, and injects malformed and privileged payloads.  Nothing here is allowed
to be tolerated as a soft failure — a safety violation must surface as an
exception or an assertion failure, never as a skipped or xfailed test.

KNOWN PHASE 1 LIMITATIONS asserted (not worked around) below:

*  Command receipts form a bounded dedup window (``MAX_COMMAND_RECEIPTS``);
   beyond it the oldest receipts are evicted.
*  A leader will not directly commit an uncommitted entry that predates its
   own term — Phase 1 has no no-op command to close Raft's leader-completeness
   gap — so such a retry fails closed with ``QuorumUnavailable`` and is
   committed implicitly by the next current-term entry instead.
*  The transport is in-process and unauthenticated; authenticated encrypted
   voter transport is explicitly out of Phase 1 scope.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from catalog.federation.control_plane_replication import (
    AppendResponse,
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
    SnapshotResponse,
    StaleTerm,
    VoterConfiguration,
    VoteResponse,
)

VOTERS = ("voter-a", "voter-b", "voter-c")
CONFIGURATION = VoterConfiguration("cluster-test", VOTERS)
CLUSTER = CONFIGURATION.cluster_id
SESSION = "session-stable"
AT = "2026-09-08T00:00:00Z"

#: Repetitions for the races that expose non-deterministic vote/term bugs.
CONCURRENCY_ROUNDS = 25


# ---------------------------------------------------------------------------
# fixtures and helpers
# ---------------------------------------------------------------------------


def _command(
    command_id: str,
    command_type: str,
    payload: dict[str, object],
    *,
    issued_by: str = "voter-a",
    cluster_id: str = CLUSTER,
) -> AuthorityCommand:
    return AuthorityCommand(
        command_id=command_id,
        command_type=command_type,
        cluster_id=cluster_id,
        issued_by=issued_by,
        payload=payload,
    )


def _enroll(command_id: str, node_id: str, **kwargs: Any) -> AuthorityCommand:
    return _command(
        command_id,
        "NODE_ENROLL",
        {
            "node_id": node_id,
            "display_name": node_id.title(),
            "public_key": f"key-{node_id}",
        },
        **kwargs,
    )


def _genesis(**overrides: object) -> AuthorityCommand:
    payload: dict[str, object] = {
        "federation_id": "federation-stable",
        "session_id": SESSION,
        "creator_node_id": "voter-a",
        "display_name": "Acceptance Federation",
        "voter_ids": list(VOTERS),
        "nodes": [
            {"node_id": voter, "display_name": voter, "public_key": f"key-{voter}"}
            for voter in VOTERS
        ],
        "members": list(VOTERS),
    }
    payload.update(overrides)
    return _command("genesis-1", "FEDERATION_GENESIS", payload)


def _machine() -> AuthorityStateMachine:
    return AuthorityStateMachine(CONFIGURATION)


def _replicas(root: Path) -> tuple[dict[str, ReplicaNode], InProcessTransport]:
    replicas = {
        voter: ReplicaNode(
            voter, PersistentReplicaStore(root / f"{voter}.sqlite3", CONFIGURATION)
        )
        for voter in VOTERS
    }
    return replicas, InProcessTransport(replicas)


def _elected_cluster(
    root: Path,
) -> tuple[dict[str, ReplicaNode], InProcessTransport]:
    replicas, transport = _replicas(root)
    assert replicas["voter-a"].start_election(transport)
    replicas["voter-a"].propose(_genesis(), transport)
    return replicas, transport


def _restart(
    replicas: dict[str, ReplicaNode],
    transport: InProcessTransport,
    voter: str,
    root: Path,
) -> ReplicaNode:
    """Reopen one replica from its durable store, as a crash-restart would."""
    node = ReplicaNode(
        voter, PersistentReplicaStore(root / f"{voter}.sqlite3", CONFIGURATION)
    )
    replicas[voter] = node
    transport.replicas[voter] = node
    return node


def _isolate_leader(transport: InProcessTransport, leader: str) -> None:
    for target in VOTERS:
        if target != leader:
            transport.blocked.add((leader, target))


class _HostileTransport:
    """Base transport that answers every RPC with a fixed term."""

    def __init__(self, term: int) -> None:
        self.term = term

    def request_vote(self, target: str, **_request: Any) -> VoteResponse:
        return VoteResponse(self.term, False, target)

    def append_entries(self, _target: str, **_request: Any) -> AppendResponse:
        return AppendResponse(self.term, False)

    def install_snapshot(self, _target: str, **_request: Any) -> SnapshotResponse:
        return SnapshotResponse(self.term, False)


class _SideEffectTransport:
    """Wraps a real transport and runs a hook after each RPC."""

    def __init__(self, inner: InProcessTransport, hook: Any, *, on: str) -> None:
        self.inner = inner
        self.hook = hook
        self.on = on

    def request_vote(self, target: str, **request: Any) -> VoteResponse:
        response = self.inner.request_vote(target, **request)
        if self.on == "vote":
            self.hook(request)
        return response

    def append_entries(self, target: str, **request: Any) -> AppendResponse:
        response = self.inner.append_entries(target, **request)
        if self.on == "append":
            self.hook(request)
        return response

    def install_snapshot(self, target: str, **request: Any) -> SnapshotResponse:
        return self.inner.install_snapshot(target, **request)


# ---------------------------------------------------------------------------
# Finding 1 — pending command retry and durable deduplication
# ---------------------------------------------------------------------------


def test_pending_retry_never_reports_success_without_quorum(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    command = _enroll("retry-1", "pending-node")

    _isolate_leader(transport, "voter-a")
    with pytest.raises(QuorumUnavailable):
        leader.propose(command, transport)
    pending = leader.store.entry_for_command("retry-1")
    assert pending is not None
    assert pending.log_index > leader.store.commit_index
    assert leader.store.commit_index == 1

    # The log entry exists.  That alone must never look like success.
    with pytest.raises(QuorumUnavailable):
        leader.propose(command, transport)
    assert leader.store.commit_index == 1
    assert "pending-node" not in leader.state["nodes"]

    transport.heal()
    entry, _events = leader.propose(command, transport)
    assert entry.log_index == pending.log_index
    assert leader.store.commit_index == entry.log_index
    for replica in replicas.values():
        assert replica.state["nodes"]["pending-node"]["node_id"] == "pending-node"


def test_restart_with_pending_command_fails_closed_then_converges(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    command = _enroll("retry-restart", "restart-node")

    _isolate_leader(transport, "voter-a")
    with pytest.raises(QuorumUnavailable):
        leader.propose(command, transport)
    pending = leader.store.entry_for_command("retry-restart")
    pending_index, pending_term = pending.log_index, pending.log_term

    transport.heal()
    leader = _restart(replicas, transport, "voter-a", tmp_path)
    assert leader.role == ReplicaNode.FOLLOWER
    assert leader.store.commit_index == 1
    assert leader.store.entry_for_command("retry-restart") is not None
    assert "restart-node" not in leader.state["nodes"]

    assert leader.start_election(transport)
    assert leader.store.current_term > pending_term

    # KNOWN PHASE 1 LIMITATION: an uncommitted entry from a previous term is
    # never committed directly, because that would break Raft's
    # leader-completeness rule.  It fails closed instead of faking success.
    with pytest.raises(QuorumUnavailable, match="predates the current leader term"):
        leader.propose(command, transport)
    assert leader.store.commit_index == 1

    # A fresh current-term entry commits, implicitly committing the prefix.
    follow_up = _enroll("retry-followup", "followup-node")
    entry, _events = leader.propose(follow_up, transport)
    assert entry.log_index == pending_index + 1
    assert leader.store.commit_index == entry.log_index
    assert leader.state["nodes"]["restart-node"]["node_id"] == "restart-node"
    assert leader.store.last_applied == leader.store.commit_index


def test_committed_retry_after_restart_is_idempotent(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    command = _enroll("committed-retry", "committed-node")
    first, _events = replicas["voter-a"].propose(command, transport)

    leader = _restart(replicas, transport, "voter-a", tmp_path)
    assert leader.start_election(transport)
    head_before = leader.store.last_log_index()

    second, _events = leader.propose(command, transport)
    assert second.log_index == first.log_index
    assert second.log_term == first.log_term
    assert leader.store.last_log_index() == head_before


def test_conflicting_reused_command_id_is_refused(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("shared-id", "first-node"), transport)

    with pytest.raises(DuplicateCommandError):
        leader.propose(_enroll("shared-id", "second-node"), transport)

    # The same refusal must hold while the original is still uncommitted.
    _isolate_leader(transport, "voter-a")
    with pytest.raises(QuorumUnavailable):
        leader.propose(_enroll("pending-id", "pending-first"), transport)
    with pytest.raises(DuplicateCommandError):
        leader.propose(_enroll("pending-id", "pending-second"), transport)


def test_dedup_survives_compaction_snapshot_and_restart(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    command = _enroll("dedup-compact", "dedup-node")
    original, _events = leader.propose(command, transport)

    snapshot = leader.compact()
    assert snapshot.last_included_index == original.log_index
    # The log entry itself is gone; only the receipt can carry the dedup.
    assert leader.store.entry_for_command("dedup-compact") is None
    assert leader.store.receipt_for_command("dedup-compact") is not None

    leader = _restart(replicas, transport, "voter-a", tmp_path)
    assert leader.store.receipt_for_command("dedup-compact") is not None
    assert leader.start_election(transport)
    head_before = leader.store.last_log_index()

    replayed, _events = leader.propose(command, transport)
    assert replayed.log_index == original.log_index
    assert leader.store.last_log_index() == head_before

    with pytest.raises(DuplicateCommandError):
        leader.propose(_enroll("dedup-compact", "different-node"), transport)


def test_snapshot_carries_receipts_so_followers_inherit_dedup(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("inherited", "inherited-node"), transport)
    snapshot = leader.compact()
    assert any(
        receipt.command_id == "inherited" for receipt in snapshot.command_receipts
    )

    follower = replicas["voter-c"]
    follower.install_snapshot(snapshot)
    assert follower.store.receipt_for_command("inherited") is not None
    # A conflicting reuse of that ID is refused by the follower's log too.
    conflicting = LogEntry(
        snapshot.last_included_index + 1,
        snapshot.last_included_term,
        _enroll("inherited", "forged-node"),
    )
    with pytest.raises(DuplicateCommandError):
        follower.store.append_entries(
            (conflicting,),
            prev_log_index=snapshot.last_included_index,
            prev_log_term=snapshot.last_included_term,
            leader_commit=snapshot.last_included_index,
        )


# ---------------------------------------------------------------------------
# Finding 3 — partition, rejoin and follower catch-up
# ---------------------------------------------------------------------------


def test_follower_missing_one_entry_catches_up(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    transport.blocked.add(("voter-a", "voter-c"))
    leader.propose(_enroll("gap-1", "gap-node-1"), transport)
    assert replicas["voter-c"].store.last_log_index() == 1

    transport.heal()
    assert leader.synchronize(transport) == 2
    assert replicas["voter-c"].store.commit_index == leader.store.commit_index
    assert replicas["voter-c"].state == leader.state


def test_follower_missing_many_entries_catches_up(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    transport.blocked.add(("voter-a", "voter-c"))
    for index in range(2, 8):
        leader.propose(_enroll(f"gap-{index}", f"gap-node-{index}"), transport)
    assert replicas["voter-c"].store.last_log_index() == 1
    assert leader.store.last_log_index() == 7

    transport.heal()
    assert leader.synchronize(transport) == 2
    assert replicas["voter-c"].store.last_log_index() == 7
    assert replicas["voter-c"].store.commit_index == 7
    assert replicas["voter-c"].store.last_applied == 7
    assert replicas["voter-c"].state == leader.state


def test_restarted_follower_catches_up(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    transport.blocked.add(("voter-a", "voter-c"))
    for index in range(2, 5):
        leader.propose(_enroll(f"restart-{index}", f"restart-node-{index}"), transport)

    follower = _restart(replicas, transport, "voter-c", tmp_path)
    assert follower.store.last_log_index() == 1
    assert "restart-node-2" not in follower.state["nodes"]

    transport.heal()
    assert leader.synchronize(transport) == 2
    assert follower.store.commit_index == leader.store.commit_index
    assert follower.store.last_applied == follower.store.commit_index
    assert follower.state == leader.state


def test_divergent_uncommitted_suffix_is_repaired_by_backtracking(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    transport.partition("voter-c")

    # The isolated follower accumulates an uncommitted suffix from the old term.
    divergent = (
        LogEntry(2, 1, _enroll("div-2", "div-node-2")),
        LogEntry(3, 1, _enroll("div-3", "div-node-3")),
    )
    follower.store.append_entries(
        divergent, prev_log_index=1, prev_log_term=1, leader_commit=1
    )
    assert follower.store.commit_index == 1

    # Meanwhile the surviving majority elects a new term and writes its own log.
    leader = replicas["voter-b"]
    assert leader.start_election(transport)
    assert leader.store.current_term == 2
    for index in range(2, 5):
        leader.propose(_enroll(f"real-{index}", f"real-node-{index}"), transport)

    transport.heal()
    # Force the optimistic next_index a new leader starts from, so the repair
    # has to backtrack over the whole divergent suffix.
    leader._next_index["voter-c"] = leader.store.last_log_index() + 1
    assert leader.synchronize(transport) == 2

    assert follower.store.last_log_index() == leader.store.last_log_index()
    assert [entry.command.command_id for entry in follower.store.entries()] == [
        entry.command.command_id for entry in leader.store.entries()
    ]
    assert "div-node-2" not in follower.state["nodes"]
    assert "div-node-3" not in follower.state["nodes"]
    assert follower.state == leader.state


def test_convergence_after_partition_rejoin_and_restart(tmp_path: Path) -> None:
    """Partition, write, restart the straggler, rejoin: all replicas converge."""
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    transport.partition("voter-c")
    for index in range(2, 6):
        leader.propose(_enroll(f"conv-{index}", f"conv-node-{index}"), transport)

    _restart(replicas, transport, "voter-c", tmp_path)
    _restart(replicas, transport, "voter-b", tmp_path)
    transport.heal()
    assert leader.synchronize(transport) == 2

    states = [replica.state for replica in replicas.values()]
    assert all(state == states[0] for state in states)
    commits = {replica.store.commit_index for replica in replicas.values()}
    assert commits == {leader.store.commit_index}
    for replica in replicas.values():
        assert replica.store.last_applied == replica.store.commit_index


# ---------------------------------------------------------------------------
# Finding 2 — snapshot and compaction safety
# ---------------------------------------------------------------------------


def _history(root: Path) -> tuple[dict[str, ReplicaNode], InProcessTransport]:
    replicas, transport = _elected_cluster(root)
    leader = replicas["voter-a"]
    for index in range(2, 5):
        leader.propose(_enroll(f"hist-{index}", f"hist-node-{index}"), transport)
    return replicas, transport


def test_stale_snapshot_relative_to_applied_authority_is_rejected(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("early", "early-node"), transport)
    early = leader.create_snapshot()
    assert early.last_included_index == 2

    leader.propose(_enroll("later", "later-node"), transport)
    assert leader.store.last_applied == 3

    with pytest.raises(ControlPlaneError, match="applied authority is ahead"):
        leader.store.install_snapshot(early)
    assert leader.store.last_applied == 3
    assert leader.state["nodes"]["later-node"]["node_id"] == "later-node"


def test_older_snapshot_than_compaction_boundary_is_rejected(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("boundary-1", "boundary-node-1"), transport)
    early = leader.create_snapshot()
    leader.propose(_enroll("boundary-2", "boundary-node-2"), transport)
    leader.compact()
    assert leader.store.last_snapshot_index == 3

    with pytest.raises(ControlPlaneError, match="older"):
        leader.store.install_snapshot(early)
    assert leader.store.last_snapshot_index == 3


def test_equal_index_conflicting_term_snapshot_is_rejected(tmp_path: Path) -> None:
    replicas, _transport = _history(tmp_path)
    leader = replicas["voter-a"]
    snapshot = leader.compact()
    assert snapshot.last_included_index > 0

    forged = Snapshot(
        cluster_id=CLUSTER,
        voter_configuration=CONFIGURATION,
        last_included_index=snapshot.last_included_index,
        last_included_term=snapshot.last_included_term + 1,
        state=snapshot.state,
        digest=snapshot.digest,
        command_receipts=snapshot.command_receipts,
    )
    with pytest.raises(ControlPlaneError, match="conflicting"):
        leader.store.install_snapshot(forged)
    assert leader.store.last_snapshot_term == snapshot.last_included_term


def test_compatible_snapshot_is_accepted_and_advances_authority(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    follower = replicas["voter-c"]
    transport.partition("voter-c")
    for index in range(2, 5):
        leader.propose(_enroll(f"compat-{index}", f"compat-node-{index}"), transport)

    # The follower already holds the same prefix, uncommitted past index 1.
    follower.store.append_entries(
        leader.store.entries(after=1),
        prev_log_index=1,
        prev_log_term=1,
        leader_commit=1,
    )
    assert follower.store.commit_index == 1
    assert follower.store.last_applied == 1

    snapshot = leader.create_snapshot()
    follower.install_snapshot(snapshot)
    assert follower.store.last_snapshot_index == snapshot.last_included_index
    assert follower.store.commit_index == snapshot.last_included_index
    assert follower.store.last_applied == snapshot.last_included_index
    assert follower.state == leader.state


def test_compatible_later_suffix_is_retained_and_replayed(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    follower = replicas["voter-c"]
    transport.partition("voter-c")

    leader.propose(_enroll("suffix-2", "suffix-node-2"), transport)
    midpoint = leader.create_snapshot()
    assert midpoint.last_included_index == 2
    leader.propose(_enroll("suffix-3", "suffix-node-3"), transport)

    follower.store.append_entries(
        leader.store.entries(after=1),
        prev_log_index=1,
        prev_log_term=1,
        leader_commit=1,
    )
    assert follower.store.last_log_index() == 3
    assert follower.store.commit_index == 1

    follower.install_snapshot(midpoint)
    # The compatible suffix past the snapshot survives compaction.
    retained = follower.store.entries()
    assert [entry.log_index for entry in retained] == [3]
    assert follower.store.commit_index == 2
    assert follower.store.last_applied == 2
    assert "suffix-node-3" not in follower.state["nodes"]

    transport.heal()
    assert leader.synchronize(transport) == 2
    assert follower.store.commit_index == 3
    assert follower.store.last_applied == 3
    assert follower.state == leader.state


def test_incompatible_uncommitted_suffix_is_discarded(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    transport.partition("voter-c")
    follower.store.append_entries(
        (
            LogEntry(2, 1, _enroll("bad-suffix-2", "bad-node-2")),
            LogEntry(3, 1, _enroll("bad-suffix-3", "bad-node-3")),
        ),
        prev_log_index=1,
        prev_log_term=1,
        leader_commit=1,
    )
    assert follower.store.commit_index == 1

    leader = replicas["voter-b"]
    assert leader.start_election(transport)
    for index in range(2, 4):
        leader.propose(_enroll(f"good-{index}", f"good-node-{index}"), transport)
    snapshot = leader.compact()
    assert snapshot.last_included_term == 2

    follower.install_snapshot(snapshot)
    assert follower.store.entries() == ()
    assert follower.store.commit_index == snapshot.last_included_index
    assert follower.store.last_applied == snapshot.last_included_index
    assert "bad-node-2" not in follower.state["nodes"]
    assert follower.state["nodes"]["good-node-2"]["node_id"] == "good-node-2"


def test_snapshot_never_discards_committed_authority(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    transport.partition("voter-c")
    # The follower has committed and applied a history the snapshot contradicts.
    follower.store.append_entries(
        (LogEntry(2, 1, _enroll("committed-2", "committed-node-2")),),
        prev_log_index=1,
        prev_log_term=1,
        leader_commit=2,
    )
    follower.apply_committed()
    assert follower.store.commit_index == 2
    assert follower.store.last_applied == 2

    leader = replicas["voter-b"]
    assert leader.start_election(transport)
    leader.propose(_enroll("other-2", "other-node-2"), transport)
    conflicting = leader.compact()
    assert conflicting.last_included_index == 2
    assert conflicting.last_included_term == 2

    with pytest.raises(ControlPlaneError, match="committed authority"):
        follower.store.install_snapshot(conflicting)
    assert follower.state["nodes"]["committed-node-2"]["node_id"] == "committed-node-2"
    assert follower.store.commit_index == 2


def test_follower_behind_compaction_boundary_gets_snapshot_then_suffix(
    tmp_path: Path,
) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    follower = replicas["voter-c"]
    transport.partition("voter-c")
    for index in range(2, 5):
        leader.propose(_enroll(f"behind-{index}", f"behind-node-{index}"), transport)
    boundary = leader.compact().last_included_index
    assert leader.store.entries() == ()
    leader.propose(_enroll("after-compaction", "after-node"), transport)

    transport.heal()
    assert follower.store.last_log_index() == 1
    assert leader.synchronize(transport) == 2

    assert follower.store.last_snapshot_index == boundary
    assert follower.store.commit_index == leader.store.commit_index
    assert follower.store.last_applied == follower.store.commit_index
    assert follower.state == leader.state
    assert follower.state["nodes"]["after-node"]["node_id"] == "after-node"


def test_restart_after_snapshot_produces_identical_state(tmp_path: Path) -> None:
    replicas, transport = _history(tmp_path)
    leader = replicas["voter-a"]
    snapshot = leader.compact()
    before = leader.state

    restarted = _restart(replicas, transport, "voter-a", tmp_path)
    assert restarted.state == before
    assert restarted.store.snapshot() == snapshot
    assert restarted.store.last_snapshot_index == snapshot.last_included_index
    assert restarted.store.last_applied == snapshot.last_included_index
    assert restarted.store.commit_index == snapshot.last_included_index


# ---------------------------------------------------------------------------
# Finding 4 — invalid command poisoning
# ---------------------------------------------------------------------------


def test_invalid_command_cannot_reach_the_log(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    head = leader.store.last_log_index()

    invalid = _command(
        "invalid-1",
        "SESSION_MEMBER_ADD",
        {"session_id": "no-such-session", "node_id": "voter-b", "occurred_at": AT},
    )
    with pytest.raises(ControlPlaneError, match="membership target is unknown"):
        leader.propose(invalid, transport)

    assert leader.store.last_log_index() == head
    for replica in replicas.values():
        assert replica.store.entry_for_command("invalid-1") is None
        assert replica.store.receipt_for_command("invalid-1") is None


def test_committed_invalid_command_does_not_wedge_replay(tmp_path: Path) -> None:
    """A committed log must always be replayable, even if a command is invalid."""
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    poisoned = LogEntry(
        2,
        1,
        _command(
            "poison",
            "SESSION_MEMBER_ADD",
            {"session_id": "no-such-session", "node_id": "voter-b", "occurred_at": AT},
        ),
    )
    response = follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(poisoned,),
        leader_commit=2,
        cluster_id=CLUSTER,
    )
    assert response.success
    assert follower.store.commit_index == 2
    # commit_index must not outrun last_applied permanently.
    assert follower.store.last_applied == 2
    assert "membership target is unknown" in follower.rejected_commands[2]

    healthy = LogEntry(3, 1, _enroll("after-poison", "after-poison-node"))
    assert follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=2,
        prev_log_term=1,
        entries=(healthy,),
        leader_commit=3,
        cluster_id=CLUSTER,
    ).success
    assert follower.store.last_applied == 3
    assert follower.state["nodes"]["after-poison-node"]["node_id"] == (
        "after-poison-node"
    )


def test_validation_uses_the_ordered_pending_state(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    _isolate_leader(transport, "voter-a")

    enroll = _enroll("ordered-enroll", "late-node")
    with pytest.raises(QuorumUnavailable):
        leader.propose(enroll, transport)
    assert "late-node" not in leader.state["nodes"]
    assert "late-node" in leader.pending_state()["nodes"]

    member = _command(
        "ordered-add",
        "SESSION_MEMBER_ADD",
        {"session_id": SESSION, "node_id": "late-node", "occurred_at": AT},
    )
    # Valid only against the ordered pending log, so it must be accepted into
    # the log — and still refused a success report until quorum returns.
    with pytest.raises(QuorumUnavailable):
        leader.propose(member, transport)
    assert leader.store.entry_for_command("ordered-add") is not None

    transport.heal()
    leader.propose(enroll, transport)
    leader.propose(member, transport)
    assert leader.state["memberships"][SESSION]["late-node"] is True
    assert leader.store.last_applied == leader.store.commit_index


# ---------------------------------------------------------------------------
# Finding 5 — higher-term handling, fencing and serialization
# ---------------------------------------------------------------------------


def test_higher_term_vote_response_forces_step_down(tmp_path: Path) -> None:
    replicas, _transport = _replicas(tmp_path)
    node = replicas["voter-a"]
    assert node.start_election(_HostileTransport(9)) is False
    assert node.role == ReplicaNode.FOLLOWER
    assert node.store.current_term == 9
    assert node.store.voted_for is None


def test_higher_term_append_response_forces_step_down(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    assert leader.role == ReplicaNode.LEADER
    commit_before = leader.store.commit_index

    with pytest.raises(StaleTerm):
        leader.propose(_enroll("fenced", "fenced-node"), _HostileTransport(9))

    assert leader.role == ReplicaNode.FOLLOWER
    assert leader.store.current_term == 9
    assert leader.store.commit_index == commit_before
    assert "fenced-node" not in leader.state["nodes"]


def test_stale_delayed_election_cannot_become_leader(tmp_path: Path) -> None:
    replicas, transport = _replicas(tmp_path)
    candidate = replicas["voter-a"]

    def fence(request: dict[str, Any]) -> None:
        # A competing leader wins while our own vote replies are still in
        # flight; the delayed completion must not take office.
        candidate.store.set_term_and_vote(request["term"] + 3, None)

    hostile = _SideEffectTransport(transport, fence, on="vote")
    assert candidate.start_election(hostile) is False
    assert candidate.role == ReplicaNode.FOLLOWER
    assert candidate.leader_id is None
    assert candidate.store.current_term > 1


def test_step_down_during_proposal_prevents_completion(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    commit_before = leader.store.commit_index

    def lose_leadership(_request: dict[str, Any]) -> None:
        leader.role = ReplicaNode.FOLLOWER
        leader.leader_id = None

    hostile = _SideEffectTransport(transport, lose_leadership, on="append")
    with pytest.raises(StaleTerm, match="leadership was lost"):
        leader.propose(_enroll("lost", "lost-node"), hostile)

    assert leader.store.commit_index == commit_before
    assert "lost-node" not in leader.state["nodes"]
    pending = leader.store.entry_for_command("lost")
    assert pending is not None
    assert pending.log_index > leader.store.commit_index


@pytest.mark.parametrize("attempt", range(CONCURRENCY_ROUNDS))
def test_concurrent_competing_votes_grant_at_most_one(
    tmp_path: Path, attempt: int
) -> None:
    replicas, _transport = _replicas(tmp_path / f"round-{attempt}")
    voter = replicas["voter-a"]
    results: dict[str, VoteResponse] = {}
    barrier = threading.Barrier(2)

    def cast(candidate: str) -> None:
        barrier.wait()
        results[candidate] = voter.receive_vote_request(
            candidate_id=candidate,
            term=5,
            last_log_index=0,
            last_log_term=0,
            cluster_id=CLUSTER,
        )

    threads = [
        threading.Thread(target=cast, args=(candidate,))
        for candidate in ("voter-b", "voter-c")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()

    granted = [name for name, response in results.items() if response.granted]
    assert len(granted) == 1
    assert voter.store.voted_for == granted[0]
    assert voter.store.current_term == 5


@pytest.mark.parametrize("attempt", range(CONCURRENCY_ROUNDS))
def test_concurrent_append_delivery_keeps_state_coherent(
    tmp_path: Path, attempt: int
) -> None:
    replicas, _transport = _elected_cluster(tmp_path / f"round-{attempt}")
    follower = replicas["voter-c"]
    entries = tuple(
        LogEntry(index, 1, _enroll(f"race-{index}", f"race-node-{index}"))
        for index in range(2, 6)
    )
    barrier = threading.Barrier(len(entries))
    failures: list[BaseException] = []

    def deliver(entry: LogEntry) -> None:
        try:
            barrier.wait()
            follower.receive_append_entries(
                leader_id="voter-a",
                leader_term=1,
                prev_log_index=entry.log_index - 1,
                prev_log_term=1,
                entries=(entry,),
                leader_commit=entry.log_index,
                cluster_id=CLUSTER,
            )
        # Any exception at all from concurrent delivery is a failure here.
        except Exception as exc:  # noqa: BLE001 - reported by the assert below
            failures.append(exc)

    threads = [threading.Thread(target=deliver, args=(entry,)) for entry in entries]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()

    assert not failures
    # Whatever interleaving occurred, the replica stays one consistent history.
    assert follower.store.last_applied == follower.store.commit_index
    assert follower.store.commit_index <= follower.store.last_log_index()
    follower.store.projection()  # raises on a torn or mis-digested projection


def test_old_leader_cannot_continue_authority_after_fencing(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    old_leader = replicas["voter-a"]
    assert replicas["voter-b"].start_election(transport)
    assert replicas["voter-b"].store.current_term == 2
    assert old_leader.role == ReplicaNode.FOLLOWER

    with pytest.raises(StaleTerm):
        old_leader.propose(_enroll("zombie", "zombie-node"), transport)

    stale = replicas["voter-c"].receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(LogEntry(2, 1, _enroll("zombie-2", "zombie-node-2")),),
        leader_commit=2,
        cluster_id=CLUSTER,
    )
    assert stale.success is False
    assert stale.term == 2
    assert replicas["voter-c"].store.last_log_index() == 1


# ---------------------------------------------------------------------------
# Finding 6 — AppendEntries invariants
# ---------------------------------------------------------------------------


def test_gapped_entries_are_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    with pytest.raises(ControlPlaneError, match="contiguous"):
        follower.store.append_entries(
            (LogEntry(3, 1, _enroll("gapped", "gapped-node")),),
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=1,
        )
    assert follower.store.last_log_index() == 1


def test_reordered_entries_are_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    second = LogEntry(2, 1, _enroll("order-2", "order-node-2"))
    third = LogEntry(3, 1, _enroll("order-3", "order-node-3"))
    with pytest.raises(ControlPlaneError, match="contiguous"):
        follower.store.append_entries(
            (third, second), prev_log_index=1, prev_log_term=1, leader_commit=1
        )
    assert follower.store.last_log_index() == 1


def test_non_monotonic_entry_terms_are_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    with pytest.raises(ControlPlaneError, match="monotonic"):
        follower.store.append_entries(
            (
                LogEntry(2, 5, _enroll("mono-2", "mono-node-2")),
                LogEntry(3, 4, _enroll("mono-3", "mono-node-3")),
            ),
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=1,
            leader_term=5,
        )
    assert follower.store.last_log_index() == 1


def test_entry_term_above_leader_term_is_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    with pytest.raises(ControlPlaneError, match="exceeds the leader term"):
        follower.store.append_entries(
            (LogEntry(2, 9, _enroll("forged-term", "forged-term-node")),),
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=1,
            leader_term=1,
        )
    assert follower.store.last_log_index() == 1


def test_cluster_mismatch_is_rejected(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]

    foreign = LogEntry(
        2, 1, _enroll("foreign", "foreign-node", cluster_id="cluster-other")
    )
    with pytest.raises(ControlPlaneError, match="cluster identity mismatch"):
        follower.store.append_entries(
            (foreign,), prev_log_index=1, prev_log_term=1, leader_commit=1
        )

    response = follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(),
        leader_commit=1,
        cluster_id="cluster-other",
    )
    assert response.success is False
    assert follower.store.last_log_index() == 1


def test_prefix_mismatch_reports_a_usable_backtrack_hint(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    response = follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=7,
        prev_log_term=1,
        entries=(),
        leader_commit=1,
        cluster_id=CLUSTER,
    )
    assert response.success is False
    assert response.conflict_index == follower.store.last_log_index() + 1


def test_committed_history_cannot_be_overwritten(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("durable", "durable-node"), transport)
    follower = replicas["voter-c"]
    assert follower.store.commit_index == 2

    with pytest.raises(LogConflict, match="committed"):
        follower.store.append_entries(
            (LogEntry(2, 3, _enroll("overwrite", "overwrite-node")),),
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=2,
        )
    assert follower.state["nodes"]["durable-node"]["node_id"] == "durable-node"


def test_short_request_cannot_commit_a_divergent_follower_suffix(
    tmp_path: Path,
) -> None:
    """A heartbeat proves only its own prefix — never a follower-only suffix."""
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    rogue = LogEntry(2, 1, _enroll("rogue", "rogue-node"))
    follower.store.append_entries(
        (rogue,), prev_log_index=1, prev_log_term=1, leader_commit=1
    )
    assert follower.store.commit_index == 1

    response = follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(),
        leader_commit=99,
        cluster_id=CLUSTER,
    )
    assert response.success
    assert follower.store.commit_index == 1
    assert follower.store.last_applied == 1
    assert "rogue-node" not in follower.state["nodes"]


def test_commit_advances_only_to_the_proven_prefix(tmp_path: Path) -> None:
    replicas, _transport = _elected_cluster(tmp_path)
    follower = replicas["voter-c"]
    follower.store.append_entries(
        (
            LogEntry(2, 1, _enroll("proven-2", "proven-node-2")),
            LogEntry(3, 1, _enroll("proven-3", "proven-node-3")),
        ),
        prev_log_index=1,
        prev_log_term=1,
        leader_commit=1,
    )
    # A request carrying one entry proves index 2, and no further.
    response = follower.receive_append_entries(
        leader_id="voter-a",
        leader_term=1,
        prev_log_index=1,
        prev_log_term=1,
        entries=(LogEntry(2, 1, _enroll("proven-2", "proven-node-2")),),
        leader_commit=99,
        cluster_id=CLUSTER,
    )
    assert response.success
    assert response.match_index == 2
    assert follower.store.commit_index == 2
    assert "proven-node-3" not in follower.state["nodes"]


# ---------------------------------------------------------------------------
# Finding 7 — schemas, genesis binding, membership and public projection
# ---------------------------------------------------------------------------


def test_conflicting_genesis_is_refused(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    head = leader.store.last_log_index()
    with pytest.raises(ControlPlaneError, match="Federation identity cannot change"):
        leader.propose(
            _command(
                "genesis-conflict",
                "FEDERATION_GENESIS",
                {**_genesis().payload, "federation_id": "other-federation"},
            ),
            transport,
        )
    assert leader.store.last_log_index() == head
    assert leader.state["federation_id"] == "federation-stable"


def test_genesis_voter_set_mismatch_is_refused(tmp_path: Path) -> None:
    replicas, transport = _replicas(tmp_path)
    leader = replicas["voter-a"]
    assert leader.start_election(transport)
    with pytest.raises(ControlPlaneError, match="voter configuration mismatch"):
        leader.propose(_genesis(voter_ids=["voter-a", "voter-b", "voter-z"]), transport)
    assert leader.state["federation_id"] is None
    assert leader.store.last_log_index() == 0

    # The correct voter set binds into genesis and is visible in the projection.
    leader.propose(_genesis(), transport)
    assert leader.state["voter_ids"] == list(VOTERS)


def test_store_refuses_a_changed_voter_configuration(tmp_path: Path) -> None:
    path = tmp_path / "voter-a.sqlite3"
    PersistentReplicaStore(path, CONFIGURATION)
    rebound = VoterConfiguration(CLUSTER, ("voter-a", "voter-b", "voter-z"))
    with pytest.raises(ControlPlaneError, match="voter configuration is immutable"):
        PersistentReplicaStore(path, rebound)

    renamed = VoterConfiguration("cluster-other", VOTERS)
    with pytest.raises(ControlPlaneError, match="cluster identity changed"):
        PersistentReplicaStore(path, renamed)


@pytest.mark.parametrize(
    ("command_type", "payload", "message"),
    [
        ("NODE_ENROLL", {"node_id": "n", "display_name": "N"}, "missing"),
        ("NODE_ENROLL", {"node_id": "", "display_name": "N", "public_key": "k"}, "malformed"),
        (
            "NODE_ENROLL",
            {"node_id": "n", "display_name": "N", "public_key": 7},
            "malformed",
        ),
        (
            "LEADER_TRANSITION",
            {
                "session_id": SESSION,
                "previous_leader_node_id": "voter-a",
                "leader_node_id": "voter-b",
                "term": -1,
                "occurred_at": AT,
            },
            "non-negative integer",
        ),
        (
            "FEDERATION_GENESIS",
            {**_genesis().payload, "nodes": [{"node_id": "n"}]},
            "malformed",
        ),
        (
            "FEDERATION_GENESIS",
            {**_genesis().payload, "voter_ids": "voter-a"},
            "must be an array",
        ),
    ],
)
def test_malformed_payloads_are_refused_at_the_envelope(
    command_type: str, payload: dict[str, object], message: str
) -> None:
    with pytest.raises(ControlPlaneError, match=message):
        _command("malformed", command_type, payload)


def test_non_object_payload_is_refused() -> None:
    with pytest.raises(ControlPlaneError, match="must be an object"):
        _command("not-an-object", "NODE_ENROLL", ["node-1"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "extra",
    [
        {"api_key": "secret"},
        {"_internal_actor": "root"},
        {"credentials": {"token": "t"}},
        {"unexpected": 1},
    ],
)
def test_unexpected_fields_are_refused(extra: dict[str, object]) -> None:
    payload = {
        "node_id": "n-1",
        "display_name": "N",
        "public_key": "k",
        **extra,
    }
    with pytest.raises(ControlPlaneError, match="unexpected fields"):
        _command("injected", "NODE_ENROLL", payload)


def test_revoked_actor_cannot_issue_authority(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(
        _command(
            "revoke-b",
            "NODE_REVOKE",
            {"node_id": "voter-b", "reason": "compromised", "occurred_at": AT},
        ),
        transport,
    )
    assert "voter-b" in leader.state["revocations"]

    with pytest.raises(ControlPlaneError, match="issued_by is revoked"):
        leader.propose(
            _enroll("by-revoked", "ghost-node", issued_by="voter-b"), transport
        )
    # A revoked node is also no longer an eligible leadership target.
    with pytest.raises(ControlPlaneError, match="revoked"):
        leader.propose(
            _command(
                "revoked-leader",
                "LEADER_TRANSITION",
                {
                    "session_id": SESSION,
                    "previous_leader_node_id": "voter-a",
                    "leader_node_id": "voter-b",
                    "term": 2,
                    "occurred_at": AT,
                },
            ),
            transport,
        )


def test_non_member_and_removed_actors_are_refused(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]

    with pytest.raises(ControlPlaneError, match="not an enrolled node"):
        leader.propose(
            _enroll("by-stranger", "stranger-node", issued_by="voter-z"), transport
        )

    leader.propose(
        _command(
            "remove-c",
            "SESSION_MEMBER_REMOVE",
            {"session_id": SESSION, "node_id": "voter-c", "occurred_at": AT},
        ),
        transport,
    )
    assert leader.state["memberships"][SESSION]["voter-c"] is False

    with pytest.raises(ControlPlaneError, match="not a member of the session"):
        leader.propose(
            _command(
                "by-removed",
                "SESSION_MEMBER_ADD",
                {"session_id": SESSION, "node_id": "voter-b", "occurred_at": AT},
                issued_by="voter-c",
            ),
            transport,
        )


def test_invalid_leadership_targets_are_refused(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]

    with pytest.raises(ControlPlaneError, match="not contiguous or eligible"):
        leader.propose(
            _command(
                "leader-skip",
                "LEADER_TRANSITION",
                {
                    "session_id": SESSION,
                    "previous_leader_node_id": "voter-a",
                    "leader_node_id": "voter-b",
                    "term": 5,
                    "occurred_at": AT,
                },
            ),
            transport,
        )

    with pytest.raises(ControlPlaneError, match="not contiguous or eligible"):
        leader.propose(
            _command(
                "leader-wrong-previous",
                "LEADER_TRANSITION",
                {
                    "session_id": SESSION,
                    "previous_leader_node_id": "voter-c",
                    "leader_node_id": "voter-b",
                    "term": 2,
                    "occurred_at": AT,
                },
            ),
            transport,
        )

    leader.propose(_enroll("outsider", "outsider-node"), transport)
    with pytest.raises(ControlPlaneError, match="not a member of the session"):
        leader.propose(
            _command(
                "leader-outsider",
                "LEADER_TRANSITION",
                {
                    "session_id": SESSION,
                    "previous_leader_node_id": "voter-a",
                    "leader_node_id": "outsider-node",
                    "term": 2,
                    "occurred_at": AT,
                },
            ),
            transport,
        )

    with pytest.raises(ControlPlaneError, match="leadership session is unknown"):
        leader.propose(
            _command(
                "leader-no-session",
                "LEADER_TRANSITION",
                {
                    "session_id": "no-such-session",
                    "previous_leader_node_id": "voter-a",
                    "leader_node_id": "voter-b",
                    "term": 2,
                    "occurred_at": AT,
                },
            ),
            transport,
        )


def test_capability_ownership_is_enforced(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]

    # Declaring a capability on someone else's behalf is refused.
    with pytest.raises(ControlPlaneError, match="does not own the capability"):
        leader.propose(
            _command(
                "cap-forged",
                "CAPABILITY_DECLARE",
                {
                    "session_id": SESSION,
                    "capability_id": "cap-1",
                    "capability_type": "recorder",
                    "owner_node_id": "voter-b",
                    "occurred_at": AT,
                },
                issued_by="voter-a",
            ),
            transport,
        )

    leader.propose(
        _command(
            "cap-own",
            "CAPABILITY_DECLARE",
            {
                "session_id": SESSION,
                "capability_id": "cap-1",
                "capability_type": "recorder",
                "owner_node_id": "voter-a",
                "occurred_at": AT,
            },
        ),
        transport,
    )
    assert leader.state["capabilities"][SESSION]["cap-1"]["owner_node_id"] == "voter-a"

    # Withdrawing someone else's capability is refused.
    with pytest.raises(ControlPlaneError, match="does not own the capability"):
        leader.propose(
            _command(
                "cap-steal",
                "CAPABILITY_WITHDRAW",
                {"session_id": SESSION, "capability_id": "cap-1", "occurred_at": AT},
                issued_by="voter-b",
            ),
            transport,
        )
    assert "cap-1" in leader.state["capabilities"][SESSION]

    leader.propose(
        _command(
            "cap-withdraw",
            "CAPABILITY_WITHDRAW",
            {"session_id": SESSION, "capability_id": "cap-1", "occurred_at": AT},
        ),
        transport,
    )
    assert "cap-1" not in leader.state["capabilities"][SESSION]


def test_public_events_expose_only_allowlisted_fields(tmp_path: Path) -> None:
    replicas, transport = _elected_cluster(tmp_path)
    leader = replicas["voter-a"]
    leader.propose(_enroll("evented", "evented-node"), transport)
    leader.propose(
        _command(
            "member-add",
            "SESSION_MEMBER_ADD",
            {"session_id": SESSION, "node_id": "evented-node", "occurred_at": AT},
        ),
        transport,
    )

    events = leader.state["session_events"][SESSION]
    joined = [event for event in events if event["event_type"] == "node.joined"][-1]
    assert set(joined["payload"]) == {"session_id", "node_id"}
    assert joined["occurred_at"] == AT
    assert joined["actor_node_id"] == "voter-a"
    # occurred_at is promoted to the envelope, never duplicated into payload.
    assert "occurred_at" not in joined["payload"]


def test_injected_fields_cannot_enter_public_events(tmp_path: Path) -> None:
    """Genesis event import is the only arbitrary-dict surface; it is filtered."""
    replicas, transport = _replicas(tmp_path)
    leader = replicas["voter-a"]
    assert leader.start_election(transport)
    leader.propose(
        _genesis(
            session_events=[
                {
                    "session_id": SESSION,
                    "revision": 1,
                    "event_type": "node.joined",
                    "actor_node_id": "voter-a",
                    "occurred_at": AT,
                    "payload": {
                        "session_id": SESSION,
                        "node_id": "voter-a",
                        "api_key": "super-secret",
                        "_internal_actor": "root",
                        "password": "hunter2",
                    },
                }
            ]
        ),
        transport,
    )

    imported = leader.state["session_events"][SESSION][0]
    assert set(imported["payload"]) == {"session_id", "node_id"}
    serialized = repr(leader.state["session_events"])
    for secret in ("super-secret", "_internal_actor", "hunter2"):
        assert secret not in serialized


def test_genesis_event_import_rejects_unknown_types_and_gaps(tmp_path: Path) -> None:
    with pytest.raises(ControlPlaneError, match="unsupported genesis event type"):
        _machine().apply(
            _machine().initial_state(),
            _genesis(
                session_events=[
                    {
                        "session_id": SESSION,
                        "revision": 1,
                        "event_type": "node.exfiltrated",
                        "actor_node_id": "voter-a",
                        "occurred_at": AT,
                        "payload": {},
                    }
                ]
            ),
        )
    with pytest.raises(ControlPlaneError, match="not contiguous"):
        _machine().apply(
            _machine().initial_state(),
            _genesis(
                session_events=[
                    {
                        "session_id": SESSION,
                        "revision": 4,
                        "event_type": "node.joined",
                        "actor_node_id": "voter-a",
                        "occurred_at": AT,
                        "payload": {"session_id": SESSION, "node_id": "voter-a"},
                    }
                ]
            ),
        )

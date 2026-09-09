"""Real authenticated voters resume a partly committed witnessed public journal."""

from __future__ import annotations

from pathlib import Path

import pytest

from catalog.federation.control_plane_journal import (
    PRODUCT_JOURNAL_INITIALIZE,
    journal_history_digest,
    journal_prefix_digest,
)
from catalog.federation.control_plane_legacy_migration import _read_event_journal
from catalog.federation.control_plane_readiness import BOOTSTRAP_SEAL_CAPABILITY_ID
from catalog.federation.control_plane_replication import ReplicaNode
from catalog.federation.tests.test_c03_offline_creator_migration import (
    CREATOR,
    FEDERATION,
    SESSION,
    _event,
    _legacy_events,
    _runtime,
    _topology,
    _write_member_witness,
)


def _wire_events(runtime):
    return tuple(event.to_dict() for event in runtime.local.store.replay_events(
        session_id=SESSION, last_applied_revision=0,
    ))


def test_different_voter_completes_exact_witnessed_prefix_after_first_real_chunk(
    tmp_path: Path, monkeypatch,
) -> None:
    deployments = _topology(tmp_path)
    voter_ids = tuple(deployment.local_voter_id for deployment in deployments)
    base = _legacy_events(voter_ids)
    # Cross the production 64 KiB chunk limit with a few ordinary public rows;
    # retain the normal payload, command, manifest, and state size limits.
    events = base + tuple(_event(
        revision, "demo.bootstrap.note", voter_ids[0],
        {"sequence": revision, "text": "Synthetic public bootstrap history. " * 350},
    ) for revision in range(len(base) + 1, len(base) + 9))
    witnesses = [_write_member_witness(
        tmp_path, voter_id=voter_id, events=events,
    ) for voter_id in voter_ids]
    runtimes = [_runtime(deployment, *witness) for deployment, witness in zip(
        deployments, witnesses, strict=True,
    )]
    original = runtimes[0]
    original_propose = original._propose_bootstrap_command
    interrupted = {}
    started = []

    def interrupt_after_committed_chunk(command):
        result = original_propose(command)
        if command.command_type == PRODUCT_JOURNAL_INITIALIZE and not command.payload["final"]:
            interrupted["command"] = command
            interrupted["state"] = original.node.state
            original._stop.set()
            raise RuntimeError("interrupted after committed non-final journal chunk")
        return result

    monkeypatch.setattr(original, "_propose_bootstrap_command", interrupt_after_committed_chunk)
    try:
        for runtime in runtimes:
            runtime.start()
            started.append(runtime)
        with pytest.raises(RuntimeError, match="committed non-final journal chunk"):
            original._attempt_existing_federation_bootstrap()
        interrupted_state = interrupted["state"]
        staged = interrupted_state["product_journal"]["initializing"][SESSION]
        assert 0 < len(staged["rows"]) < staged["expected_revision"]
        assert SESSION not in interrupted_state["product_journal"]["sessions"]
        assert BOOTSTRAP_SEAL_CAPABILITY_ID not in interrupted_state["capabilities"][SESSION]
        assert not original.ready
        command = interrupted["command"]
        receipt = original.node.store.receipt_for_command(command.command_id)
        assert receipt.content_hash == command.content_hash
        initial_leadership = interrupted_state["leaders"][SESSION]
        assert initial_leadership["leader_node_id"] == voter_ids[0]
        original.close()
        started.remove(original)

        successor, follower = runtimes[1:]
        assert successor.node.state["product_journal"]["initializing"][SESSION] == staged
        # Use the real witnessed recovery entrypoint. Existing migration fixture
        # timers keep the injected process boundary deterministic; all elections,
        # witness attestations, replication and acknowledgements use real sockets.
        successor._attempt_existing_federation_bootstrap()
        successor.materialize()
        follower.materialize()
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.ready and follower.ready
        state = successor.node.state
        journal = state["product_journal"]["sessions"][SESSION]
        rows = journal["rows"]
        assert SESSION not in state["product_journal"]["initializing"]
        assert rows[:len(staged["rows"])] == staged["rows"]
        assert journal_prefix_digest(rows[:staged["expected_revision"]]) == staged["prefix_digest"]
        assert journal["provenance"] == {
            "kind": "witnessed", "source_revision": len(events),
            "history_digest": journal_history_digest(rows[:len(events)]),
        }
        assert [row["revision"] for row in rows] == list(range(1, len(rows) + 1))
        assert len({row["event_id"] for row in rows}) == len(rows)
        leadership = successor.local.session_leadership(session_id=SESSION)
        assert leadership.creator_node_id == CREATOR
        assert leadership.leader_node_id == successor.node.voter_id
        assert leadership.term > initial_leadership["term"]
        assert _wire_events(successor)[:len(events)] == tuple(event.to_dict() for event in events)
        assert _wire_events(follower) == _wire_events(successor)
        assert state["federation_id"] == FEDERATION

        returning = _runtime(deployments[0], *witnesses[0])
        returning.start()
        started.append(returning)
        assert successor.node.synchronize(successor.transport) + 1 >= successor.node.quorum
        returning.materialize()
        assert returning.ready
        assert returning.node.role == ReplicaNode.FOLLOWER
        assert _wire_events(returning) == _wire_events(successor)
        for runtime in (successor, follower, returning):
            assert runtime.node.state["leaders"][SESSION]["creator_node_id"] == CREATOR
            assert runtime.node.store.receipt_for_command(command.command_id).content_hash == command.content_hash
        for node_db, _pairing in witnesses:
            assert _read_event_journal(node_db, SESSION) == events
    finally:
        for runtime in reversed(started):
            runtime.close()

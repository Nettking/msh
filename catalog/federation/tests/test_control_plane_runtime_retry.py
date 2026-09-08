"""Quorum loss must preserve the exact pending automatic promotion command."""

import threading
from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.control_plane_replication import QuorumUnavailable
from catalog.federation.control_plane_runtime import (
    PhysicalReadyReplicatedFederationRuntime,
)
from catalog.federation.tests.test_control_plane_readiness import _seal
from catalog.federation.tests.test_control_plane_replication import _elected_cluster


@pytest.mark.parametrize("reelect", [False, True])
def test_pending_promotion_retries_identical_command_after_clock_moves(tmp_path, reelect):
    nodes, transport = _elected_cluster(tmp_path)
    nodes["voter-a"].propose(_seal(), transport)
    successor = nodes["voter-b"]
    assert successor.start_election(transport)

    # Drive the product promotion method over the real persistent consensus
    # nodes, with deterministic link loss instead of lifecycle timer races.
    runtime = object.__new__(PhysicalReadyReplicatedFederationRuntime)
    runtime.node = successor
    runtime.transport = transport
    runtime._lifecycle_lock = threading.RLock()
    now = datetime(2026, 9, 8, tzinfo=timezone.utc)
    runtime.clock = lambda: now
    transport.blocked.update({("voter-b", "voter-a"), ("voter-b", "voter-c")})
    with pytest.raises(QuorumUnavailable):
        runtime._promote_operational_sessions()
    pending = successor.store.entries(after=successor.store.commit_index)[-1]
    assert successor.state["leaders"]["session-stable"]["leader_node_id"] == "voter-a"

    now += timedelta(minutes=1)
    transport.blocked.clear()
    if reelect:
        assert successor.start_election(transport)
        assert successor.store.current_term > pending.log_term
    runtime._promote_operational_sessions()
    receipt = successor.store.receipt_for_command(pending.command.command_id)
    assert receipt is not None
    assert receipt.content_hash == pending.command.content_hash
    assert successor.state["leaders"]["session-stable"]["leader_node_id"] == "voter-b"

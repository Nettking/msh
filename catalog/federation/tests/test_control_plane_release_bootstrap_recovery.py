"""Fresh bootstrap survives interruption at real committed release boundaries."""

from __future__ import annotations

import time
from dataclasses import replace
from pathlib import Path

import pytest

from catalog.federation.control_plane_replication import ControlPlaneError, ReplicaNode
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.tests.test_control_plane_physical_runtime import _deployments

SESSION = "session-fresh-bootstrap-recovery"
FEDERATION = "federation-fresh-bootstrap-recovery"


def _wait(predicate, message: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


def _runtime(deployment) -> FederationV1ReleaseRuntime:
    directory = deployment.replica_database.parent
    return FederationV1ReleaseRuntime(
        deployment,
        legacy_node_state_database=directory / "legacy-node.sqlite3",
        legacy_pairing_state_path=directory / "legacy-pairing.json",
    )


def _events(runtime):
    if runtime.local.store.get_session(SESSION) is None:
        return ()
    return tuple(event.to_dict() for event in runtime.local.store.replay_events(
        session_id=SESSION, last_applied_revision=0,
    ))


@pytest.mark.parametrize("boundary", ["after-genesis", "after-journal-before-seal"])
def test_actual_release_voters_resume_interrupted_fresh_bootstrap(
    tmp_path: Path, monkeypatch, boundary: str,
) -> None:
    deployments = []
    for index, deployment in enumerate(_deployments(tmp_path)):
        directory = tmp_path / f"host-{index}"
        directory.mkdir()
        deployments.append(replace(
            deployment,
            replica_database=directory / "replica.sqlite3",
            replay_database=directory / "replay.sqlite3",
            coordinator_database=directory / "coordinator.sqlite3",
        ))
    runtimes = [_runtime(deployment) for deployment in deployments]
    started = []
    interrupted = {}
    original = runtimes[0]
    arguments = {
        "federation_id": FEDERATION, "session_id": SESSION,
        "creator_node_id": original.node.voter_id,
        "display_name": "Interrupted fresh Federation",
    }

    def interrupt_before_next_stage(**_kwargs):
        # Inject failure only after real quorum work reached the chosen stage.
        # Repeated calls also refuse, preventing the old process from completing
        # its own bootstrap while the test closes its actual sockets/thread.
        interrupted.setdefault("state", original.node.state)
        original._stop.set()
        raise RuntimeError("injected bootstrap process interruption")

    method = "_seal_authority" if boundary == "after-genesis" else "_commit_readiness_seal"
    monkeypatch.setattr(original, method, interrupt_before_next_stage)
    try:
        for runtime in runtimes:
            runtime.start()
            started.append(runtime)
        with pytest.raises(RuntimeError, match="injected bootstrap process interruption"):
            original.bootstrap_new_federation(**arguments)
        state = interrupted["state"]
        assert state["federation_id"] == FEDERATION
        initialized = state.get("product_journal", {}).get("sessions", {}).get(SESSION)
        if boundary == "after-genesis":
            assert initialized is None
        else:
            assert initialized["revision"] == 4
        assert "fcp.control-plane.bootstrap-complete" not in state["capabilities"].get(SESSION, {})
        genesis = next(entry.command for entry in original.node.store.entries()
                       if entry.command.command_type == "FEDERATION_GENESIS")
        original.close()
        started.remove(original)
        survivors = runtimes[1:]

        def recovered():
            leaders = [runtime for runtime in survivors if runtime.node.role == ReplicaNode.LEADER]
            return len(leaders) == 1 and all(runtime.ready for runtime in survivors) and (
                leaders[0].node.state["leaders"][SESSION]["leader_node_id"] == leaders[0].node.voter_id
            )

        _wait(recovered, "two real surviving voters did not finish fresh bootstrap automatically")
        successor = next(runtime for runtime in survivors if runtime.node.role == ReplicaNode.LEADER)
        _wait(lambda: all(_events(runtime) == _events(successor) for runtime in survivors),
              "survivor public projections did not converge")
        canonical = successor.node.state["product_journal"]["sessions"][SESSION]["rows"]
        assert [row["event_type"] for row in canonical[:4]] == [
            "session.created", "node.joined", "node.joined", "node.joined",
        ]
        if initialized is not None:
            assert canonical[:4] == initialized["rows"]
        assert [row["revision"] for row in canonical] == list(range(1, len(canonical) + 1))
        assert len({row["event_id"] for row in canonical}) == len(canonical)
        leadership = successor.local.session_leadership(session_id=SESSION)
        assert leadership.creator_node_id == arguments["creator_node_id"]
        assert leadership.leader_node_id == successor.node.voter_id
        assert leadership.term >= 2
        with pytest.raises(ControlPlaneError, match="conflicts"):
            successor.bootstrap_new_federation(**{**arguments, "display_name": "Wrong identity"})

        returning = _runtime(deployments[0])
        returning.start()
        started.append(returning)
        _wait(lambda: returning.ready and _events(returning) == _events(successor),
              "returning original voter did not adopt the exact completed public journal")
        for runtime in (*survivors, returning):
            assert runtime.node.state["federation_id"] == FEDERATION
            assert runtime.node.state["leaders"][SESSION]["creator_node_id"] == arguments["creator_node_id"]
            entries = [entry for entry in runtime.node.store.entries()
                       if entry.command.command_type == "FEDERATION_GENESIS"]
            assert len(entries) == 1
            assert entries[0].command.content_hash == genesis.content_hash
            assert _events(runtime) == _events(successor)
    finally:
        for runtime in reversed(started):
            runtime.close()

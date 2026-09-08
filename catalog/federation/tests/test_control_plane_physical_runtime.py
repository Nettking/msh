"""Product-level C03 recovery regressions for Federation v1 physical readiness."""

from __future__ import annotations

import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.control_plane_product import (
    DeploymentPeer,
    ReplicatedControlPlaneDeployment,
)
from catalog.federation.control_plane_replication import ReplicaNode
from catalog.federation.federation_v1_runtime import FederationV1Runtime
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
SECRET = bytes(range(32))


def _free_port_triple(used: set[int]) -> int:
    """Reserve-test consecutive control/credential/migration ports."""
    for _ in range(100):
        first = socket.socket()
        first.bind(("127.0.0.1", 0))
        port = int(first.getsockname()[1])
        first.close()
        candidates = {port, port + 1, port + 2}
        if port >= 65533 or candidates & used:
            continue
        sockets: list[socket.socket] = []
        try:
            for candidate in (port, port + 1, port + 2):
                sock = socket.socket()
                sock.bind(("127.0.0.1", candidate))
                sockets.append(sock)
        except OSError:
            continue
        finally:
            for sock in sockets:
                sock.close()
        used.update(candidates)
        return port
    raise RuntimeError("could not allocate consecutive test ports")


def _deployments(root: Path):
    credentials = []
    ports = []
    used: set[int] = set()
    for name in ("a", "b", "c"):
        identity_dir = root / f"identity-{name}"
        credentials.append(IdentityStore(identity_dir, display_name=name).create(now=NOW))
        ports.append(_free_port_triple(used))

    secret_file = root / "control-plane.secret"
    secret_file.write_bytes(SECRET)
    peers = tuple(
        DeploymentPeer(
            voter_id=credentials[index].identity.node_id,
            public_key=credentials[index].identity.public_key,
            host="127.0.0.1",
            port=ports[index],
            display_name=("a", "b", "c")[index],
        )
        for index in range(3)
    )
    deployments = []
    for index, peer in enumerate(peers):
        deployments.append(
            ReplicatedControlPlaneDeployment(
                cluster_id="cluster-physical-runtime",
                local_voter_id=peer.voter_id,
                identity_directory=root / f"identity-{('a', 'b', 'c')[index]}",
                local_display_name=peer.display_name,
                replica_database=root / f"replica-{index}.sqlite3",
                replay_database=root / f"replay-{index}.sqlite3",
                coordinator_database=root / f"coordinator-{index}.sqlite3",
                transport_secret_file=secret_file,
                listen_host="127.0.0.1",
                listen_port=ports[index],
                peers=peers,
            )
        )
    return tuple(deployments)


def _runtime(deployment: ReplicatedControlPlaneDeployment):
    return FederationV1Runtime(
        deployment,
        heartbeat_seconds=0.05,
        election_timeout_seconds=0.15,
        election_stagger_seconds=0.05,
        credential_sync_seconds=0.1,
    )


def _wait(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError("condition did not become true before timeout")


def test_permanent_leader_loss_recovers_same_federation_and_fences_returning_host(
    tmp_path: Path,
) -> None:
    deployments = _deployments(tmp_path)
    runtimes = [_runtime(item) for item in deployments]
    old_runtime = runtimes[0]
    successor = runtimes[1]
    third = runtimes[2]
    restarted_old = None
    for runtime in runtimes:
        runtime.start()
    try:
        creator = old_runtime.node.voter_id
        old_runtime.bootstrap_new_federation(
            federation_id="federation-survives-leader-loss",
            session_id="session-survives-leader-loss",
            creator_node_id=creator,
            display_name="Persistent Federation",
        )
        old_federation_id = old_runtime.node.state["federation_id"]
        old_term = old_runtime.node.store.current_term
        _wait(lambda: successor.ready and third.ready)

        # The elected coordinator disappears and stays unavailable while the
        # two surviving authenticated voters retain quorum 2/3.
        old_runtime.close()
        time.sleep(0.25)

        def surviving_quorum_has_promoted_leader() -> bool:
            nonlocal successor
            # Background elections can finish between two manual lifecycle
            # calls. Observe the actual automatic winner instead of assuming
            # that the last voter nudged must have won the election.
            leaders = [
                runtime for runtime in runtimes[1:]
                if runtime.node.role == ReplicaNode.LEADER
            ]
            if len(leaders) != 1:
                return False
            elected = leaders[0]
            leadership = elected.node.state["leaders"]["session-survives-leader-loss"]
            if leadership["leader_node_id"] != elected.node.voter_id:
                return False
            successor = elected
            return True

        _wait(surviving_quorum_has_promoted_leader)

        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.store.current_term > old_term
        assert successor.node.state["federation_id"] == old_federation_id
        leadership = successor.node.state["leaders"]["session-survives-leader-loss"]
        assert leadership["leader_node_id"] == successor.node.voter_id
        assert leadership["creator_node_id"] == creator
        assert successor.ready

        # The old machine may return later, but it must join the newer term as a
        # follower and converge to the same Federation instead of reclaiming
        # authority from its stale local state.
        restarted_old = _runtime(deployments[0])
        restarted_old.start()

        def returning_host_has_converged() -> bool:
            # An acknowledgement from the other survivor says nothing about
            # the returning host. Require that host to receive the current
            # term and committed prefix before checking its fenced state.
            successor.node.synchronize(successor.transport)
            return (
                restarted_old.node.role == ReplicaNode.FOLLOWER
                and restarted_old.node.store.current_term >= successor.node.store.current_term
                and restarted_old.node.store.last_applied >= successor.node.store.commit_index
            )

        _wait(returning_host_has_converged)
        restarted_old.node.apply_committed()
        restarted_old.materialize()
        assert restarted_old.node.role == ReplicaNode.FOLLOWER
        assert restarted_old.node.store.current_term >= successor.node.store.current_term
        assert restarted_old.node.state["federation_id"] == old_federation_id
        returned_leadership = restarted_old.node.state["leaders"][
            "session-survives-leader-loss"
        ]
        assert returned_leadership["leader_node_id"] == successor.node.voter_id
        assert returned_leadership["creator_node_id"] == creator
    finally:
        if restarted_old is not None:
            restarted_old.close()
        # old_runtime was already closed above; close only the still-running
        # survivors here because socket-server close is deliberately one-shot.
        for runtime in runtimes[1:]:
            runtime.close()


def test_unsealed_replica_never_self_elects_without_proven_bootstrap(tmp_path: Path) -> None:
    deployments = _deployments(tmp_path)
    runtimes = [_runtime(item) for item in deployments]
    for runtime in runtimes:
        runtime.start()
    try:
        candidate = runtimes[1]
        candidate._next_election_at = 0.0
        candidate._lifecycle_round()
        assert candidate.ready is False
        assert candidate.node.state["federation_id"] is None
        assert candidate.node.role == ReplicaNode.FOLLOWER
    finally:
        for runtime in runtimes:
            runtime.close()

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
from catalog.federation.control_plane_runtime import (
    PhysicalReadyReplicatedFederationRuntime,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
SECRET = bytes(range(32))


def _free_port_pair() -> int:
    """Reserve-test a consecutive control/credential port pair."""
    for _ in range(100):
        first = socket.socket()
        first.bind(("127.0.0.1", 0))
        port = int(first.getsockname()[1])
        first.close()
        if port >= 65534:
            continue
        second = socket.socket()
        try:
            second.bind(("127.0.0.1", port + 1))
        except OSError:
            second.close()
            continue
        second.close()
        return port
    raise RuntimeError("could not allocate consecutive test ports")


def _deployments(root: Path):
    credentials = []
    ports = []
    for name in ("a", "b", "c"):
        identity_dir = root / f"identity-{name}"
        credentials.append(IdentityStore(identity_dir, display_name=name).create(now=NOW))
        ports.append(_free_port_pair())

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
    return PhysicalReadyReplicatedFederationRuntime(
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
        successor._next_election_at = 0.0
        successor._lifecycle_round()
        if successor.node.role != ReplicaNode.LEADER:
            third._next_election_at = 0.0
            third._lifecycle_round()
            successor = third

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
        _wait(lambda: successor.node.synchronize(successor.transport) >= 1)
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
        # old_runtime was already closed above; close() is idempotent enough for
        # the lifecycle thread but the underlying socket server is not, so only
        # close the still-running survivors here.
        for runtime in runtimes[1:]:
            runtime.close()


def test_unsealed_replica_never_self_elects_without_creator_bootstrap(tmp_path: Path) -> None:
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

"""Physical-topology regressions for the native voter-only Recorder service."""

from __future__ import annotations

import socket
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.control_plane_product import (
    DeploymentPeer,
    ReplicatedControlPlaneDeployment,
)
from catalog.federation.control_plane_replication import ControlPlaneError, ReplicaNode
from catalog.federation.control_plane_runtime import (
    PhysicalReadyReplicatedFederationRuntime,
)
from catalog.federation.recorder_control_plane_voter import (
    RecorderControlPlaneVoter,
    validate_recorder_voter_isolation,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
SECRET = bytes(range(32))


def _free_pair() -> int:
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
    raise RuntimeError("could not allocate test port pair")


def _topology(root: Path):
    names = ("nettking", "nitro", "msh-recorder")
    credentials = []
    ports = []
    for name in names:
        identity = root / f"identity-{name}"
        credentials.append(IdentityStore(identity, display_name=name).create(now=NOW))
        ports.append(_free_pair())
    secret = root / "transport.secret"
    secret.write_bytes(SECRET)
    peers = tuple(
        DeploymentPeer(
            voter_id=credentials[index].identity.node_id,
            public_key=credentials[index].identity.public_key,
            host="127.0.0.1",
            port=ports[index],
            display_name=names[index],
        )
        for index in range(3)
    )
    deployments = tuple(
        ReplicatedControlPlaneDeployment(
            cluster_id="cluster-recorder-voter",
            local_voter_id=peers[index].voter_id,
            identity_directory=root / f"identity-{names[index]}",
            local_display_name=names[index],
            replica_database=root / "control" / names[index] / "replica.sqlite3",
            replay_database=root / "control" / names[index] / "replay.sqlite3",
            coordinator_database=root / "control" / names[index] / "coordinator.sqlite3",
            transport_secret_file=secret,
            listen_host="127.0.0.1",
            listen_port=ports[index],
            peers=peers,
        )
        for index in range(3)
    )
    return deployments


def _product_runtime(deployment: ReplicatedControlPlaneDeployment):
    return PhysicalReadyReplicatedFederationRuntime(
        deployment,
        heartbeat_seconds=0.1,
        election_timeout_seconds=60.0,
        election_stagger_seconds=5.0,
        credential_sync_seconds=5.0,
    )


def test_recorder_voter_keeps_quorum_without_touching_protected_data(
    tmp_path: Path,
) -> None:
    deployments = _topology(tmp_path)
    protected = tmp_path / "record data"
    protected.mkdir()
    sentinel = protected / "measurement.jsonl"
    original = b"protected-measurement-bytes\n"
    sentinel.write_bytes(original)

    nettking = _product_runtime(deployments[0])
    nitro = _product_runtime(deployments[1])
    recorder = RecorderControlPlaneVoter(
        deployments[2], protected_record_data=protected, status_interval_seconds=0.05
    )
    nettking.start()
    nitro.start()
    recorder.start()
    try:
        creator = nettking.node.voter_id
        nettking.bootstrap_new_federation(
            federation_id="same-federation-after-leader-loss",
            session_id="session-recorder-quorum",
            creator_node_id=creator,
            display_name="Recorder quorum Federation",
        )
        nettking.node.synchronize(nettking.transport)
        recorder.node.apply_committed()
        assert recorder.node.state["federation_id"] == "same-federation-after-leader-loss"
        assert recorder.node.role == ReplicaNode.FOLLOWER

        old_term = nettking.node.store.current_term
        nettking.close()

        # Nitro can establish quorum using only itself + the voter-only Recorder.
        assert nitro.node.start_election(nitro.transport) is True
        assert nitro.node.store.current_term > old_term
        nitro._promote_operational_sessions()
        assert nitro.node.synchronize(nitro.transport) >= 1
        nitro.materialize()

        state = nitro.node.state
        assert state["federation_id"] == "same-federation-after-leader-loss"
        leader = state["leaders"]["session-recorder-quorum"]
        assert leader["leader_node_id"] == nitro.node.voter_id
        assert leader["creator_node_id"] == creator
        assert recorder.node.role == ReplicaNode.FOLLOWER
        assert recorder.node.leader_id == nitro.node.voter_id

        assert sentinel.read_bytes() == original
        assert sorted(path.name for path in protected.iterdir()) == [sentinel.name]
    finally:
        nitro.close()
        recorder.close()


def test_recorder_voter_refuses_any_control_path_inside_protected_data(
    tmp_path: Path,
) -> None:
    deployments = list(_topology(tmp_path))
    protected = tmp_path / "record data"
    protected.mkdir()
    recorder = deployments[2]
    unsafe = ReplicatedControlPlaneDeployment(
        cluster_id=recorder.cluster_id,
        local_voter_id=recorder.local_voter_id,
        identity_directory=recorder.identity_directory,
        local_display_name=recorder.local_display_name,
        replica_database=protected / "replica.sqlite3",
        replay_database=recorder.replay_database,
        coordinator_database=recorder.coordinator_database,
        transport_secret_file=recorder.transport_secret_file,
        listen_host=recorder.listen_host,
        listen_port=recorder.listen_port,
        peers=recorder.peers,
    )
    with pytest.raises(ControlPlaneError, match="overlaps protected record data"):
        validate_recorder_voter_isolation(unsafe, protected)

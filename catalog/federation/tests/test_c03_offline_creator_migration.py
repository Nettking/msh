"""C03 regressions for migrating the same Federation after creator loss."""

from __future__ import annotations

import socket
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.control_plane_legacy_migration import LegacyMigrationError
from catalog.federation.control_plane_product import (
    DeploymentPeer,
    ReplicatedControlPlaneDeployment,
)
from catalog.federation.control_plane_readiness import (
    BOOTSTRAP_SEAL_CAPABILITY_ID,
    authority_ready,
)
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.models import SessionEvent
from catalog.federation.onboarding_models import (
    FederationConnectionState,
    FederationSessionBinding,
)
from catalog.federation.recorder_control_plane_voter import RecorderControlPlaneVoter
from catalog.flask_app.services.federation_pairing_service import (
    RemotePairingState,
    RemotePairingStore,
)
from catalog.node.identity import IdentityStore
from catalog.node.state import ConnectionState, EnrollmentState, NodeState

NOW = datetime(2026, 9, 8, 9, tzinfo=timezone.utc)
SECRET = bytes(range(32))
FEDERATION = "federation-created-on-offline-beast"
SESSION = "session-created-on-offline-beast"
CREATOR = "historical-beast-creator"
HISTORICAL_REVOKED = "historical-revoked-member"


def _free_triple(used: set[int]) -> int:
    for _ in range(200):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
        probe.close()
        candidates = {port, port + 1, port + 2}
        if port >= 65533 or candidates & used:
            continue
        sockets: list[socket.socket] = []
        try:
            for candidate in sorted(candidates):
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
    raise RuntimeError("could not allocate isolated C03 test port triple")


def _event(
    revision: int,
    event_type: str,
    actor: str,
    payload: dict[str, object],
) -> SessionEvent:
    return SessionEvent(
        session_id=SESSION,
        revision=revision,
        event_id=f"legacy-event-{revision}",
        event_type=event_type,
        occurred_at=NOW + timedelta(seconds=revision),
        actor_node_id=actor,
        payload=payload,
    )


def _legacy_events(voter_ids: tuple[str, str, str]) -> tuple[SessionEvent, ...]:
    a, b, recorder = voter_ids
    return (
        _event(
            1,
            "session.created",
            CREATOR,
            {"session_id": SESSION, "display_name": "Original Federation"},
        ),
        _event(2, "node.joined", CREATOR, {"node_id": CREATOR}),
        _event(3, "node.joined", a, {"node_id": a}),
        _event(4, "node.joined", b, {"node_id": b}),
        _event(5, "node.joined", recorder, {"node_id": recorder}),
        _event(
            6,
            "node.joined",
            HISTORICAL_REVOKED,
            {"node_id": HISTORICAL_REVOKED},
        ),
        _event(
            7,
            "capability.registered",
            a,
            {
                "session_id": SESSION,
                "capability_id": "legacy-storage-capability",
                "node_id": a,
                "type": "storage",
                "protocol": "legacy-storage",
                "protocol_version": "1",
                "status": "ready",
                "properties": {},
                "announced_at": (NOW + timedelta(seconds=7)).isoformat(),
            },
        ),
        _event(
            8,
            "node.revoked",
            CREATOR,
            {
                "node_id": HISTORICAL_REVOKED,
                "reason": "legacy-security-revocation",
            },
        ),
    )


def _write_member_witness(
    root: Path,
    *,
    voter_id: str,
    events: tuple[SessionEvent, ...],
) -> tuple[Path, Path]:
    base = root / f"legacy-{voter_id}"
    node_db = base / "device" / "node_state.sqlite3"
    pairing = base / "onboarding" / "remote_pairing.json"
    state = NodeState(node_db, node_id=voter_id, now=NOW)
    state.set_enrollment_state(EnrollmentState.ENROLLED, now=NOW)
    state.set_connection_state(ConnectionState.CONNECTED, now=NOW)
    state.join_session(SESSION, now=NOW)
    for event in events:
        state.apply_event(event, now=event.occurred_at)
    RemotePairingStore(pairing).save(
        RemotePairingState(
            relay_url="wss://legacy-relay.example",
            binding=FederationSessionBinding(
                federation_id=FEDERATION,
                internal_session_id=SESSION,
                device_id=voter_id,
                state=FederationConnectionState.CONNECTED,
                revision=len(events),
                trusted=True,
                created_at=NOW,
                last_verified_at=NOW + timedelta(minutes=1),
            ),
        )
    )
    return node_db, pairing


def _topology(root: Path):
    names = ("nettking", "nitro", "msh-recorder")
    credentials = []
    used: set[int] = set()
    ports = []
    for name in names:
        credentials.append(
            IdentityStore(root / f"identity-{name}", display_name=name).create(now=NOW)
        )
        ports.append(_free_triple(used))
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
            cluster_id="cluster-offline-creator-migration",
            local_voter_id=peers[index].voter_id,
            identity_directory=root / f"identity-{names[index]}",
            local_display_name=names[index],
            replica_database=root / "c03" / names[index] / "replica.sqlite3",
            replay_database=root / "c03" / names[index] / "replay.sqlite3",
            coordinator_database=root / "c03" / names[index] / "coordinator.sqlite3",
            transport_secret_file=secret,
            listen_host="127.0.0.1",
            listen_port=ports[index],
            peers=peers,
        )
        for index in range(3)
    )
    return deployments


def _runtime(
    deployment: ReplicatedControlPlaneDeployment,
    node_db: Path,
    pairing: Path,
) -> FederationV1ReleaseRuntime:
    return FederationV1ReleaseRuntime(
        deployment,
        bootstrap_federation_id=FEDERATION,
        bootstrap_session_id=SESSION,
        legacy_node_state_database=node_db,
        legacy_pairing_state_path=pairing,
        heartbeat_seconds=60.0,
        election_timeout_seconds=60.0,
        election_stagger_seconds=5.0,
        credential_sync_seconds=60.0,
    )


def test_offline_creator_migrates_same_federation_with_two_witnesses_and_recorder_quorum(
    tmp_path: Path,
) -> None:
    deployments = _topology(tmp_path)
    voter_ids = tuple(item.local_voter_id for item in deployments)
    events = _legacy_events(voter_ids)
    first_db, first_pairing = _write_member_witness(
        tmp_path, voter_id=voter_ids[0], events=events
    )
    second_db, second_pairing = _write_member_witness(
        tmp_path, voter_id=voter_ids[1], events=events
    )

    protected = tmp_path / "record data"
    protected.mkdir()
    sentinel = protected / "measurement.jsonl"
    original = b"protected-recorder-measurement\n"
    sentinel.write_bytes(original)

    first = _runtime(deployments[0], first_db, first_pairing)
    second = _runtime(deployments[1], second_db, second_pairing)
    recorder = RecorderControlPlaneVoter(
        deployments[2],
        protected_record_data=protected,
        status_interval_seconds=0.05,
    )
    first.start()
    second.start()
    recorder.start()
    try:
        # Historical creator/old coordinator is intentionally absent. The
        # current voter proves consensus quorum with the Recorder and proves
        # legacy-history quorum with the second surviving product member.
        first._attempt_existing_federation_bootstrap()
        state = first.node.state
        assert authority_ready(state)
        assert state["federation_id"] == FEDERATION
        assert state["sessions"][SESSION]["creator_node_id"] == CREATOR
        leadership = state["leaders"][SESSION]
        assert leadership["creator_node_id"] == CREATOR
        assert leadership["leader_node_id"] == first.node.voter_id
        assert int(leadership["term"]) >= 2

        assert state["memberships"][SESSION][CREATOR] is False
        assert CREATOR in state["revocations"]
        assert HISTORICAL_REVOKED in state["revocations"]
        assert (
            state["revocations"][HISTORICAL_REVOKED]["reason"]
            == "legacy-security-revocation"
        )
        capability = state["capabilities"][SESSION]["legacy-storage-capability"]
        assert capability["owner_node_id"] == voter_ids[0]
        assert capability["capability_type"] == "storage"
        assert BOOTSTRAP_SEAL_CAPABILITY_ID in state["capabilities"][SESSION]
        seal = state["capabilities"][SESSION][BOOTSTRAP_SEAL_CAPABILITY_ID]
        assert seal["owner_node_id"] == first.node.voter_id

        second.node.apply_committed()
        recorder.node.apply_committed()
        assert second.node.state["federation_id"] == FEDERATION
        assert recorder.node.state["federation_id"] == FEDERATION
        assert sentinel.read_bytes() == original
        assert sorted(path.name for path in protected.iterdir()) == [sentinel.name]
    finally:
        first.close()
        second.close()
        recorder.close()


def test_offline_creator_migration_rejects_disagreeing_surviving_histories(
    tmp_path: Path,
) -> None:
    deployments = _topology(tmp_path)
    voter_ids = tuple(item.local_voter_id for item in deployments)
    first_events = _legacy_events(voter_ids)
    second_events = list(first_events)
    second_events[6] = _event(
        7,
        "capability.registered",
        voter_ids[1],
        {
            "session_id": SESSION,
            "capability_id": "different-capability",
            "node_id": voter_ids[1],
            "type": "storage",
        },
    )
    first_db, first_pairing = _write_member_witness(
        tmp_path, voter_id=voter_ids[0], events=first_events
    )
    second_db, second_pairing = _write_member_witness(
        tmp_path, voter_id=voter_ids[1], events=tuple(second_events)
    )
    protected = tmp_path / "record data"
    protected.mkdir()

    first = _runtime(deployments[0], first_db, first_pairing)
    second = _runtime(deployments[1], second_db, second_pairing)
    recorder = RecorderControlPlaneVoter(
        deployments[2], protected_record_data=protected, status_interval_seconds=0.05
    )
    first.start()
    second.start()
    recorder.start()
    try:
        with pytest.raises(LegacyMigrationError, match="matching authenticated member witnesses"):
            first._attempt_existing_federation_bootstrap()
        assert first.node.state["federation_id"] is None
        assert first.ready is False
    finally:
        first.close()
        second.close()
        recorder.close()


def test_offline_creator_migration_rejects_gap_in_local_witness_journal(
    tmp_path: Path,
) -> None:
    deployments = _topology(tmp_path)
    voter_ids = tuple(item.local_voter_id for item in deployments)
    events = _legacy_events(voter_ids)
    node_db, pairing = _write_member_witness(
        tmp_path, voter_id=voter_ids[0], events=events
    )
    with sqlite3.connect(node_db) as database:
        database.execute(
            "DELETE FROM applied_events WHERE session_id=? AND revision=?",
            (SESSION, 4),
        )
        database.commit()

    runtime = _runtime(deployments[0], node_db, pairing)
    try:
        with pytest.raises(LegacyMigrationError, match="journal is incomplete"):
            runtime.legacy_witness_server.attest(FEDERATION, SESSION)
    finally:
        # The runtime has not been started, so close only the raw witness socket
        # bound during construction; calling the lifecycle close path would try
        # to shut down servers that never entered serve_forever.
        runtime.legacy_witness_server._server.server_close()
        runtime.credential_server._server.server_close()
        runtime.server._server.server_close()

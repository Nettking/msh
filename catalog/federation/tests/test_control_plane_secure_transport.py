"""Adversarial tests for the authenticated encrypted C03 voter transport."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.control_plane_replication import (
    AuthorityCommand,
    ControlPlaneError,
    PersistentReplicaStore,
    ReplicaNode,
    VoterConfiguration,
)
from catalog.federation.control_plane_transport import (
    PersistentReplayGuard,
    ReplayRejected,
    SecureEnvelopeCodec,
    SecureReplicationServer,
    SecureSocketReplicationTransport,
    TransportSecurityError,
    VoterEndpoint,
    VoterIdentityRegistry,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 8, tzinfo=timezone.utc)
SECRET = bytes(range(32))
OTHER_SECRET = bytes(reversed(range(32)))
AT = "2026-09-08T00:00:00Z"


def _credentials(root: Path, name: str):
    return IdentityStore(root / f"identity-{name}", display_name=name).create(now=NOW)


def _cluster(root: Path):
    credentials = [_credentials(root, name) for name in ("a", "b", "c")]
    by_id = {item.identity.node_id: item for item in credentials}
    configuration = VoterConfiguration("cluster-secure", tuple(by_id))
    registry = VoterIdentityRegistry(
        configuration,
        {node_id: item.identity.public_key for node_id, item in by_id.items()},
    )
    nodes = {
        voter: ReplicaNode(
            voter,
            PersistentReplicaStore(root / f"replica-{voter}.sqlite3", configuration),
        )
        for voter in configuration.voter_ids
    }
    codecs = {
        voter: SecureEnvelopeCodec(
            by_id[voter],
            registry,
            SECRET,
            PersistentReplayGuard(root / f"replay-{voter}.sqlite3"),
        )
        for voter in configuration.voter_ids
    }
    servers = {
        voter: SecureReplicationServer(nodes[voter], codecs[voter])
        for voter in configuration.voter_ids
    }
    for server in servers.values():
        server.start()
    endpoints = {
        voter: VoterEndpoint(*servers[voter].address)
        for voter in configuration.voter_ids
    }
    transports = {
        voter: SecureSocketReplicationTransport(codecs[voter], endpoints)
        for voter in configuration.voter_ids
    }
    return configuration, registry, by_id, nodes, codecs, servers, transports


def _close(servers) -> None:
    for server in servers.values():
        server.close()


def _genesis(configuration: VoterConfiguration, registry: VoterIdentityRegistry, leader: str):
    return AuthorityCommand(
        command_id="genesis-secure",
        command_type="FEDERATION_GENESIS",
        cluster_id=configuration.cluster_id,
        issued_by=leader,
        payload={
            "federation_id": "federation-secure",
            "session_id": "session-secure",
            "creator_node_id": leader,
            "display_name": "Secure Federation",
            "voter_ids": list(configuration.voter_ids),
            "nodes": [
                {
                    "node_id": voter,
                    "display_name": voter,
                    "public_key": registry.public_key(voter),
                }
                for voter in configuration.voter_ids
            ],
            "members": list(configuration.voter_ids),
            "occurred_at": AT,
        },
    )


def test_secure_transport_elects_and_commits_over_real_sockets(tmp_path: Path) -> None:
    configuration, registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    try:
        leader = configuration.voter_ids[0]
        assert nodes[leader].start_election(transports[leader])
        entry, _events = nodes[leader].propose(
            _genesis(configuration, registry, leader), transports[leader]
        )
        assert entry.log_index == 1
        assert nodes[leader].store.commit_index == 1
        for node in nodes.values():
            assert node.state["federation_id"] == "federation-secure"
    finally:
        _close(servers)


def test_wire_encrypts_command_fields(tmp_path: Path) -> None:
    configuration, _registry, _creds, _nodes, codecs, servers, _transports = _cluster(tmp_path)
    try:
        sender, recipient = configuration.voter_ids[:2]
        sealed = codecs[sender].seal(
            recipient,
            "request_vote",
            {
                "candidate_id": sender,
                "term": 7,
                "last_log_index": 3,
                "last_log_term": 6,
                "cluster_id": configuration.cluster_id,
                "secret_marker": "must-not-appear-on-wire",
            },
        )
        assert b"must-not-appear-on-wire" not in sealed.wire
        assert b"candidate_id" not in sealed.wire
        opened = codecs[recipient].open(sealed.wire, expected_sender=sender)
        assert opened.payload["secret_marker"] == "must-not-appear-on-wire"
    finally:
        _close(servers)


def test_sender_name_tamper_breaks_signature(tmp_path: Path) -> None:
    configuration, _registry, _creds, _nodes, codecs, servers, _transports = _cluster(tmp_path)
    try:
        sender, recipient, forged = configuration.voter_ids
        sealed = codecs[sender].seal(recipient, "request_vote", {"cluster_id": configuration.cluster_id})
        value = json.loads(sealed.wire)
        value["sender_id"] = forged
        tampered = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        with pytest.raises(TransportSecurityError):
            codecs[recipient].open(tampered)
    finally:
        _close(servers)


def test_authenticated_sender_cannot_claim_another_candidate(tmp_path: Path) -> None:
    configuration, _registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    try:
        sender, target, forged = configuration.voter_ids
        with pytest.raises(ControlPlaneError):
            transports[sender].request_vote(
                target,
                candidate_id=forged,
                term=1,
                last_log_index=0,
                last_log_term=0,
                cluster_id=configuration.cluster_id,
            )
        assert nodes[target].store.voted_for is None
    finally:
        _close(servers)


def test_authenticated_sender_cannot_claim_another_leader_for_append(tmp_path: Path) -> None:
    configuration, _registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    try:
        sender, target, forged = configuration.voter_ids
        with pytest.raises(ControlPlaneError):
            transports[sender].append_entries(
                target,
                leader_id=forged,
                leader_term=1,
                prev_log_index=0,
                prev_log_term=0,
                entries=(),
                leader_commit=0,
                cluster_id=configuration.cluster_id,
            )
        assert nodes[target].leader_id is None
    finally:
        _close(servers)


def test_wrong_cluster_secret_cannot_decrypt_valid_signature(tmp_path: Path) -> None:
    configuration, registry, credentials, _nodes, codecs, servers, _transports = _cluster(tmp_path)
    try:
        sender, recipient = configuration.voter_ids[:2]
        wrong = SecureEnvelopeCodec(
            credentials[recipient],
            registry,
            OTHER_SECRET,
            PersistentReplayGuard(tmp_path / "wrong-secret-replay.sqlite3"),
        )
        sealed = codecs[sender].seal(recipient, "request_vote", {"cluster_id": configuration.cluster_id})
        with pytest.raises(TransportSecurityError, match="decryption"):
            wrong.open(sealed.wire, expected_sender=sender)
    finally:
        _close(servers)


def test_replay_is_rejected_and_persists_across_codec_restart(tmp_path: Path) -> None:
    configuration, registry, credentials, _nodes, codecs, servers, _transports = _cluster(tmp_path)
    try:
        sender, recipient = configuration.voter_ids[:2]
        replay_path = tmp_path / "durable-replay.sqlite3"
        receiver = SecureEnvelopeCodec(
            credentials[recipient], registry, SECRET, PersistentReplayGuard(replay_path)
        )
        sealed = codecs[sender].seal(recipient, "request_vote", {"cluster_id": configuration.cluster_id})
        receiver.open(sealed.wire, expected_sender=sender)
        with pytest.raises(ReplayRejected):
            receiver.open(sealed.wire, expected_sender=sender)
        restarted = SecureEnvelopeCodec(
            credentials[recipient], registry, SECRET, PersistentReplayGuard(replay_path)
        )
        with pytest.raises(ReplayRejected):
            restarted.open(sealed.wire, expected_sender=sender)
    finally:
        _close(servers)


def test_registry_rejects_key_substitution(tmp_path: Path) -> None:
    first = _credentials(tmp_path, "first")
    second = _credentials(tmp_path, "second")
    third = _credentials(tmp_path, "third")
    configuration = VoterConfiguration(
        "cluster-keys",
        (first.identity.node_id, second.identity.node_id, third.identity.node_id),
    )
    with pytest.raises(TransportSecurityError, match="not derived"):
        VoterIdentityRegistry(
            configuration,
            {
                first.identity.node_id: second.identity.public_key,
                second.identity.node_id: first.identity.public_key,
                third.identity.node_id: third.identity.public_key,
            },
        )


def test_stale_term_is_rejected_after_valid_authentication(tmp_path: Path) -> None:
    configuration, _registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    try:
        sender, target = configuration.voter_ids[:2]
        nodes[target].store.observe_term(5)
        response = transports[sender].request_vote(
            target,
            candidate_id=sender,
            term=4,
            last_log_index=0,
            last_log_term=0,
            cluster_id=configuration.cluster_id,
        )
        assert response.term == 5
        assert response.granted is False
    finally:
        _close(servers)


def test_surviving_authenticated_voters_elect_after_one_voter_loss(tmp_path: Path) -> None:
    configuration, registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    leader = configuration.voter_ids[0]
    survivors = configuration.voter_ids[1:]
    try:
        assert nodes[leader].start_election(transports[leader])
        nodes[leader].propose(_genesis(configuration, registry, leader), transports[leader])
        # Simulate complete loss of the old coordinator endpoint. The two
        # surviving authenticated voters still form quorum 2/3.
        servers[leader].close()
        del servers[leader]
        successor = survivors[0]
        assert nodes[successor].start_election(transports[successor])
        assert nodes[successor].role == ReplicaNode.LEADER
        assert nodes[successor].state["federation_id"] == "federation-secure"
        assert nodes[successor].store.current_term > nodes[leader].store.current_term
    finally:
        _close(servers)


def test_old_leader_is_fenced_by_higher_authenticated_term(tmp_path: Path) -> None:
    configuration, registry, _creds, nodes, _codecs, servers, transports = _cluster(tmp_path)
    leader, successor, third = configuration.voter_ids
    try:
        assert nodes[leader].start_election(transports[leader])
        nodes[leader].propose(_genesis(configuration, registry, leader), transports[leader])
        old_term = nodes[leader].store.current_term
        # Isolate the old leader at the socket endpoint while the other two vote.
        servers[leader].close()
        del servers[leader]
        assert nodes[successor].start_election(transports[successor])
        new_term = nodes[successor].store.current_term
        assert new_term > old_term
        # Deliver a valid higher-term vote request directly after the old host
        # returns. It must step down before it can regain authority.
        response = nodes[leader].receive_vote_request(
            candidate_id=third,
            term=new_term,
            last_log_index=nodes[third].store.last_log_index(),
            last_log_term=nodes[third].store.term_at(nodes[third].store.last_log_index()) or 0,
            cluster_id=configuration.cluster_id,
        )
        assert response.term == new_term
        assert nodes[leader].role == ReplicaNode.FOLLOWER
        assert nodes[leader].leader_id is None
    finally:
        _close(servers)

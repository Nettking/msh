"""Whole product operations and private grants over the real release runtime."""

from __future__ import annotations

import asyncio
import copy
from pathlib import Path

import pytest

from catalog.federation.control_plane_replication import ControlPlaneError, ReplicaNode
from catalog.federation.errors import AuthenticationError, FederationOperationError
from catalog.federation.tests.test_control_plane_public_journal import (
    SESSION,
    _client_journal,
    _cluster,
    _journal,
    _wait,
)
from catalog.node.client import RelayRemoteError


def _grant_uses(runtime, enrollment_id: str, invitation_id: str) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """Export only use counters, never token hashes or raw grant material."""
    with runtime.local.store.read_transaction() as database:
        enrollment = database.execute(
            "SELECT use_count,max_uses FROM enrollment_tokens WHERE token_id=?",
            (enrollment_id,),
        ).fetchone()
        invitation = database.execute(
            "SELECT use_count,max_uses FROM session_invitations WHERE invitation_id=?",
            (invitation_id,),
        ).fetchone()
    if enrollment is None or invitation is None:
        return None
    return tuple(enrollment), tuple(invitation)


def _grant_counts(runtime) -> tuple[int, int]:
    with runtime.local.store.read_transaction() as database:
        return (
            database.execute("SELECT COUNT(*) FROM enrollment_tokens").fetchone()[0],
            database.execute("SELECT COUNT(*) FROM session_invitations").fetchone()[0],
        )


@pytest.mark.parametrize("failure", ["nested-write", "read-inside-nested-write"])
def test_caught_nested_failure_cannot_commit_partial_work_to_quorum(tmp_path: Path, failure: str) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            runtime = cluster.runtimes[0]
            facade = cluster.relays[0].coordinator
            store = runtime.local.store
            before_state = copy.deepcopy(runtime.node.state)
            before_journal = _journal(runtime)
            before_counts = _grant_counts(runtime)
            before_commit = runtime.node.store.commit_index
            before_receipts = runtime.node.store.command_receipts()
            expected = ValueError if failure == "nested-write" else AuthenticationError
            with pytest.raises(RuntimeError, match="requires an owned transaction"):
                store.assert_committable()

            with pytest.raises(RuntimeError, match="rollback-only after a nested failure"), runtime.journal.operation():
                facade.append_event(
                    session_id=SESSION, actor_node_id=runtime.node.voter_id,
                    request_id="outer-before-failure", event_type="demo.transaction.observation",
                    payload={"stage": "before"},
                )
                facade.create_enrollment_token(max_uses=1)
                with pytest.raises(expected), store.transaction():
                    facade.append_event(
                        session_id=SESSION, actor_node_id=runtime.node.voter_id,
                        request_id="nested-failed-event", event_type="demo.transaction.observation",
                        payload={"stage": "nested"},
                    )
                    facade.create_enrollment_token(max_uses=1)
                    if failure == "nested-write":
                        raise ValueError("nested operation refused")
                    # A read failure escaping a nested write scope poisons that
                    # scope; ordinary read-only exception semantics are unchanged.
                    store.require_membership(session_id=SESSION, node_id="missing-member")
                facade.append_event(
                    session_id=SESSION, actor_node_id=runtime.node.voter_id,
                    request_id="outer-after-caught-failure", event_type="demo.transaction.observation",
                    payload={"stage": "after"},
                )
                facade.create_enrollment_token(max_uses=1)

            # This checks the real controller/consensus boundary, not merely the
            # local SQLite rollback. The old ordering committed the two surviving
            # events/grants to quorum before raising at the outer SQL exit.
            assert runtime.node.state == before_state
            assert runtime.node.store.commit_index == before_commit
            assert runtime.node.store.last_log_index() == before_commit
            assert runtime.node.store.command_receipts() == before_receipts
            assert _journal(runtime) == before_journal
            assert _grant_counts(runtime) == before_counts
            assert runtime.journal._pending() is None
            assert all(replica.node.state == before_state for replica in cluster.runtimes)

            # Poison is scoped to the failed operation, so a fresh real write
            # can still commit and project normally after the complete rollback.
            facade.create_enrollment_token(max_uses=1)
            assert _grant_counts(runtime) == (before_counts[0] + 1, before_counts[1])
            assert runtime.node.store.commit_index == before_commit + 1
            assert _journal(runtime) == before_journal
            assert runtime.journal._pending() is None

    asyncio.run(scenario())


def test_pairing_material_rolls_back_first_grant_when_invitation_half_rejects(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            runtime = cluster.runtimes[0]
            facade = cluster.relays[0].coordinator
            request_id = "pairing-second-half-conflict"
            facade.create_invitation(
                session_id=SESSION, actor_node_id=runtime.node.voter_id,
                request_id=request_id, ttl_seconds=601, max_uses=1,
            )
            before_counts = _grant_counts(runtime)
            before_state = copy.deepcopy(runtime.node.state)
            before_journal = _journal(runtime)
            before_commit = runtime.node.store.commit_index

            # The real coordinator first creates an enrollment grant, then
            # attempts this existing invitation request with conflicting bounds.
            # Its idempotency error proves the second half was reached.
            with pytest.raises(FederationOperationError) as rejected:
                facade.create_pairing_material(
                    session_id=SESSION, actor_node_id=runtime.node.voter_id,
                    request_id=request_id, ttl_seconds=600,
                )
            assert rejected.value.code == "idempotency-conflict"
            assert _grant_counts(runtime) == before_counts
            assert runtime.node.state == before_state
            assert runtime.node.store.commit_index == before_commit
            assert _journal(runtime) == before_journal
            assert runtime.journal._pending() is None

            # The same production operation remains usable after its rollback.
            paired = facade.create_pairing_material(
                session_id=SESSION, actor_node_id=runtime.node.voter_id,
                request_id="pairing-after-rollback", ttl_seconds=600,
            )
            assert _grant_counts(runtime) == (before_counts[0] + 1, before_counts[1] + 1)
            assert _grant_uses(runtime, paired["enrollment"]["token_id"], paired["invitation"]["invitation_id"]) == ((0, 1), (0, 1))
            rotated = facade.create_pairing_material(
                session_id=SESSION, actor_node_id=runtime.node.voter_id,
                request_id="pairing-after-rollback", ttl_seconds=600,
            )
            assert rotated["invitation"]["replaced"] is True
            assert rotated["enrollment"]["token"] != paired["enrollment"]["token"]
            with runtime.local.store.read_transaction() as database:
                old_grant = database.execute(
                    "SELECT use_count,revoked_at FROM enrollment_tokens WHERE token_id=?",
                    (paired["enrollment"]["token_id"],),
                ).fetchone()
                assert old_grant[0] == 0 and old_grant[1] is not None
            old_client = cluster.client(tmp_path / "rotated-old-grant", 0, "Old grant")
            with pytest.raises(RelayRemoteError):
                await old_client.connect(enrollment_token=paired["enrollment"]["token"])
            replacement = cluster.client(tmp_path / "rotated-new-grant", 0, "New grant")
            await replacement.connect(enrollment_token=rotated["enrollment"]["token"])

    asyncio.run(scenario())


def test_raw_store_semantic_mutations_cannot_change_local_or_replicated_authority(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            runtime = cluster.runtimes[0]
            facade = cluster.relays[0].coordinator
            store = runtime.local.store
            target = cluster.runtimes[2].node.voter_id
            stranger = cluster.client(tmp_path / "raw-store-identity", 0, "Raw store identity")
            enrollment = facade.create_enrollment_token(max_uses=1)
            before_state = copy.deepcopy(runtime.node.state)
            before_journal = _journal(runtime)
            before_commit = runtime.node.store.commit_index

            operations = (
                lambda: store.remove_member(
                    session_id=SESSION, actor_node_id=runtime.node.voter_id,
                    target_node_id=target, request_id="raw-remove", reason="raw-store-test",
                    now=runtime.clock(),
                ),
                lambda: store.revoke_node(
                    node_id=target, revoked_by=runtime.node.voter_id,
                    request_id="raw-revoke", reason="raw-store-test", now=runtime.clock(),
                ),
                lambda: store.enroll_node(
                    stranger.credentials.identity, raw_token=enrollment["token"], now=runtime.clock(),
                ),
            )
            for operation in operations:
                # These are actual local store entry points. Their SQL must not
                # escape merely because the replicated facade was bypassed.
                with pytest.raises(ControlPlaneError):
                    operation()
                assert runtime.node.state == before_state
                assert runtime.node.store.commit_index == before_commit
                assert _journal(runtime) == before_journal
                assert store.get_node(stranger.node_id) is None
                store.require_membership(session_id=SESSION, node_id=target)
                with store.read_transaction() as database:
                    assert database.execute(
                        "SELECT revoked_at FROM nodes WHERE node_id=?", (target,),
                    ).fetchone()[0] is None
                    assert database.execute(
                        "SELECT use_count FROM enrollment_tokens WHERE token_id=?",
                        (enrollment["token_id"],),
                    ).fetchone()[0] == 0
                assert runtime.journal._pending() is None

    asyncio.run(scenario())


def test_consumed_grants_and_original_request_receipts_survive_automatic_failover(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with _cluster(tmp_path) as cluster:
            original = cluster.runtimes[0]
            creator = cluster.client(original.deployment.identity_directory, 0, "Original creator")
            await creator.connect()
            member = cluster.client(tmp_path / "grant-member", 0, "Grant member")
            enrollment = cluster.relays[0].coordinator.create_enrollment_token(max_uses=1)
            invitation = await creator.create_invitation(SESSION, max_uses=1, request_id="original-invitation")
            await member.connect(enrollment_token=enrollment["token"])
            join_request = "original-join-request"
            joined = await member.join_session(invitation["token"], request_id=join_request)
            assert joined["session_id"] == SESSION
            event_request = "original-public-request"
            event_payload = {"source": "synthetic-machine", "reading": 17}
            accepted_event = await member.append_event(
                session_id=SESSION, event_type="demo.machine.reading",
                payload=event_payload, request_id=event_request,
            )
            await member.request_replay(SESSION)
            retained = _client_journal(member)
            assert retained == _journal(original)
            assert _grant_uses(original, enrollment["token_id"], invitation["invitation_id"]) == ((1, 1), (1, 1))
            await _wait(
                lambda: all(
                    _journal(runtime) == retained
                    and _grant_uses(runtime, enrollment["token_id"], invitation["invitation_id"]) == ((1, 1), (1, 1))
                    for runtime in cluster.runtimes[1:]
                ),
                "surviving voters did not materialize the consumed grants and complete public prefix",
            )
            previous_term = original.node.state["leaders"][SESSION]["term"]
            previous_consensus_term = original.node.store.current_term
            assert original.node.role == ReplicaNode.LEADER
            await cluster.stop_runtime(0)
            await cluster.stop_relay(0)
            await creator.disconnect()
            await member.disconnect()
            assert _client_journal(member) == retained
            successor_index = None

            def successor_ready() -> bool:
                nonlocal successor_index
                leaders = [index for index in (1, 2) if cluster.runtimes[index].node.role == ReplicaNode.LEADER]
                if len(leaders) != 1:
                    return False
                index = leaders[0]
                runtime = cluster.runtimes[index]
                leadership = runtime.node.state["leaders"][SESSION]
                if (
                    not runtime.ready
                    or leadership["leader_node_id"] != runtime.node.voter_id
                    or leadership["term"] <= previous_term
                    or runtime.node.store.current_term <= previous_consensus_term
                ):
                    return False
                successor_index = index
                return True

            # Keep the production election timers and let the surviving quorum
            # choose its own leader. Neither original credentials nor client
            # replay state is reset or regenerated after the failure.
            await _wait(successor_ready, "surviving quorum did not automatically promote a ready leader")
            assert successor_index is not None
            successor = cluster.runtimes[successor_index]
            reconnected = cluster.client(member.state_directory, successor_index, "Same member after failover")
            assert reconnected.node_id == member.node_id
            assert _client_journal(reconnected) == retained
            await reconnected.connect()
            assert _client_journal(reconnected)[: len(retained)] == retained
            before_retry = _journal(successor)

            # An exact accepted join retry is recovered from its original receipt
            # even though this exact one-use invitation has already been spent.
            replayed_join = await reconnected.join_session(invitation["token"], request_id=join_request)
            assert replayed_join["session_id"] == SESSION
            replayed_event = await reconnected.append_event(
                session_id=SESSION, event_type="demo.machine.reading",
                payload=event_payload, request_id=event_request,
            )
            assert replayed_event.to_dict() == accepted_event.to_dict()
            assert _journal(successor) == before_retry
            assert _grant_uses(successor, enrollment["token_id"], invitation["invitation_id"]) == ((1, 1), (1, 1))

            with pytest.raises(RelayRemoteError) as join_reuse:
                await reconnected.join_session(invitation["token"], request_id="different-join-request")
            assert join_reuse.value.code == "reused-session-invitation"
            with pytest.raises(RelayRemoteError) as changed_request:
                await reconnected.append_event(
                    session_id=SESSION, event_type="demo.machine.reading",
                    payload={**event_payload, "reading": 18}, request_id=event_request,
                )
            assert changed_request.value.code == "idempotency-conflict"
            stranger = cluster.client(tmp_path / "enrollment-reuse", successor_index, "Enrollment reuse")
            with pytest.raises(RelayRemoteError) as enrollment_reuse:
                await stranger.connect(enrollment_token=enrollment["token"])
            assert enrollment_reuse.value.code == "reused-enrollment-token"
            assert successor.local.store.get_node(stranger.node_id) is None
            assert _grant_uses(successor, enrollment["token_id"], invitation["invitation_id"]) == ((1, 1), (1, 1))
            assert _journal(successor) == before_retry
            await reconnected.request_replay(SESSION)
            assert _client_journal(reconnected) == before_retry
            assert _client_journal(reconnected)[: len(retained)] == retained

    asyncio.run(scenario())

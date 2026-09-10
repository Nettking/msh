"""A real voter election must not terminate the relay's local health sweep."""

from __future__ import annotations

import copy
import errno
import hashlib
import json
import linecache
import os
import socket
import socketserver
import sqlite3
import subprocess
import sys
import warnings
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from catalog.federation.control_plane_credentials import (
    CREDENTIAL_PORT_OFFSET,
    CredentialReplicaServer,
    _CredentialRPCServer,
)
from catalog.federation.control_plane_facade import (
    PhysicalReadyReplicatedSessionCoordinator,
)
from catalog.federation.control_plane_legacy_migration import (
    MIGRATION_PORT_OFFSET,
    _WitnessTCPServer,
)
from catalog.federation.control_plane_replication import ReplicaNode, StaleTerm
from catalog.federation.control_plane_transport import _ThreadingRPCServer
from catalog.federation.errors import AuthorizationError
from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.tests.test_control_plane_physical_runtime import _deployments

SESSION = "session-health-leadership-race"


def _fixture_listener_conflict(error: OSError, deployment):
    """Recognize only the actual current listener bind, never setup generally."""
    if os.name == "nt":
        if getattr(error, "winerror", None) not in {10013, 10048}:
            return None
    elif os.name != "posix" or error.errno != errno.EADDRINUSE:
        return None
    cursor = error.__traceback__
    if cursor is None:
        return None
    while cursor.tb_next is not None:
        cursor = cursor.tb_next
    frame = cursor.tb_frame
    if (
        frame.f_code is not socketserver.TCPServer.server_bind.__code__
        or linecache.getline(frame.f_code.co_filename, cursor.tb_lineno).strip()
        != "self.socket.bind(self.server_address)"
    ):
        return None
    server = frame.f_locals.get("self")
    offset = {
        _ThreadingRPCServer: 0,
        _CredentialRPCServer: CREDENTIAL_PORT_OFFSET,
        _WitnessTCPServer: MIGRATION_PORT_OFFSET,
    }.get(type(server))
    if (
        offset is None
        or deployment.listen_host != "127.0.0.1"
        or server.server_address != ("127.0.0.1", deployment.listen_port + offset)
    ):
        return None
    return {
        "listener": type(server).__name__,
        "port": deployment.listen_port + offset,
        "errno": error.errno,
        "winerror": getattr(error, "winerror", None),
    }


@pytest.fixture
def runtimes(tmp_path: Path, request: pytest.FixtureRequest):
    # Probe sockets cannot reserve the later production listeners. Acquire the
    # entire unstarted set before serving, with one fresh allocation on a proven
    # native listener conflict. Nothing after construction is retried.
    for attempt in range(2):
        attempt_root = tmp_path / f"listener-attempt-{attempt + 1}"
        attempt_root.mkdir()
        deployments = _deployments(attempt_root)
        with ExitStack() as cleanup:
            replicas = []
            try:
                for index, deployment in enumerate(deployments):
                    state = attempt_root / f"voter-{index}"
                    state.mkdir()
                    runtime = FederationV1ReleaseRuntime(
                        replace(
                            deployment,
                            replica_database=state / "replica.sqlite3",
                            replay_database=state / "replay.sqlite3",
                            coordinator_database=state / "coordinator.sqlite3",
                        ),
                        legacy_node_state_database=state / "legacy.sqlite3",
                        legacy_pairing_state_path=state / "legacy.json",
                    )
                    cleanup.callback(runtime.close)
                    replicas.append(runtime)
            except OSError as error:
                conflict = _fixture_listener_conflict(error, deployment)
                if conflict is None or attempt == 1:
                    raise
                request.node.add_report_section(
                    "setup", "listener acquisition retry", json.dumps(conflict, sort_keys=True),
                )
                warnings.warn(
                    "Health fixture listener acquisition retry 1/1: "
                    + json.dumps(conflict, sort_keys=True),
                    RuntimeWarning,
                    stacklevel=2,
                )
                # ExitStack closes every acquired listener before reallocation;
                # an error during that cleanup propagates, never retries.
                continue
            for runtime in replicas:
                # Drive elections explicitly through the real authenticated transport
                # so the exact pre-lock interleaving is independent of timer speed.
                # No lifecycle clock, role, quorum response or durable state is faked.
                runtime.legacy_witness_server.start()
                runtime.server.start()
                runtime.credential_server.start()
            leader = replicas[0]
            leader.bootstrap_new_federation(
                federation_id="federation-health-leadership-race",
                session_id=SESSION,
                creator_node_id=leader.node.voter_id,
                display_name="Health leadership race",
            )
            for runtime in replicas:
                runtime.materialize()
                assert runtime.ready
            yield tuple(replicas)
            return


def _history(runtime: FederationV1ReleaseRuntime) -> tuple[dict, ...]:
    return tuple(
        event.to_dict()
        for event in runtime.local.store.replay_events(
            session_id=SESSION, last_applied_revision=0,
        )
    )


def test_peer_election_before_health_journal_lock_keeps_sweep_local(
    runtimes, monkeypatch: pytest.MonkeyPatch,
) -> None:
    leader, successor, third = runtimes
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    node_id = third.node.voter_id
    announcement = CapabilityAnnouncement(
        capability_id="health-race-capability", node_id=node_id,
        session_id=SESSION, type="demo.health-race", protocol="demo.health-race",
        protocol_version="1", status=CapabilityStatus.READY,
        properties={"purpose": "local health regression"}, announced_at=leader.clock(),
    )
    facade.announce_capability(
        announcement, actor_node_id=node_id, request_id="announce-health-race",
    )
    for runtime in runtimes:
        runtime.materialize()
    leader.local.store.mark_connected(
        node_id=node_id, connection_id="health-race-connection",
        now=leader.clock() - timedelta(minutes=1),
    )
    before_history = _history(leader)
    before_state = copy.deepcopy(leader.node.state)
    before_commit = leader.node.store.commit_index
    before_term = leader.node.store.current_term
    original_operation = leader.journal.operation
    elections = []

    @contextmanager
    def elect_before_journal_lock():
        # _health has already selected its LEADER branch. RequestVote and
        # AppendEntries now step it down before the real journal takes its lock.
        assert leader.node.role == ReplicaNode.LEADER
        assert successor.node.start_election(successor.transport)
        assert successor.node.synchronize(successor.transport) == 2
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.store.current_term > before_term
        assert leader.node.role == ReplicaNode.FOLLOWER
        assert leader.node.leader_id == successor.node.voter_id
        elections.append(successor.node.store.current_term)
        with original_operation() as database:
            yield database

    with monkeypatch.context() as patch:
        patch.setattr(leader.journal, "operation", elect_before_journal_lock)
        stale, events = facade.sweep_stale(heartbeat_timeout_seconds=1)

    assert len(elections) == 1
    assert stale == (node_id,)
    assert events == ()
    with leader.local.store.read_transaction() as database:
        connectivity = database.execute(
            "SELECT state,connection_id,last_error FROM node_connectivity WHERE node_id=?",
            (node_id,),
        ).fetchone()
    assert tuple(connectivity) == ("disconnected", None, "stale heartbeat")
    # Local cleanup may run again; it must neither kill the caller nor generate
    # an independent public health revision from the former leader.
    assert facade.sweep_stale(heartbeat_timeout_seconds=1) == ((), ())
    assert facade.disconnected(node_id=node_id) == ()
    assert facade.relay_started() is None
    for runtime in runtimes:
        runtime.materialize()
        assert runtime.node.state == before_state
        assert runtime.node.store.commit_index == before_commit
        assert runtime.node.store.last_log_index() == before_commit
        assert _history(runtime) == before_history
        assert next(
            item for item in runtime.local.store.list_capabilities(session_id=SESSION)
            if item.capability_id == announcement.capability_id
        ).status is CapabilityStatus.READY

    # The health-only fallback must not make any durable facade operation legal
    # on the stepped-down voter, even though its readiness seal is still valid.
    with pytest.raises(AuthorizationError) as rejected:
        facade.create_enrollment_token(max_uses=1)
    assert rejected.value.code == "federation-quorum-leader-required"
    assert leader.journal._pending() is None
    assert leader.node.store.commit_index == before_commit
    assert _history(leader) == before_history


def test_health_does_not_swallow_unrelated_authorization_failure(runtimes) -> None:
    leader = runtimes[0]
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    before_commit = leader.node.store.commit_index
    before_history = _history(leader)
    calls = []

    def require_foreign_membership(*, emit_health_events=True):
        calls.append(emit_health_events)
        leader.local.store.require_membership(
            session_id="foreign-session", node_id=leader.node.voter_id,
        )

    # Use the real membership rejection, not a substituted quorum exception.
    with pytest.raises(AuthorizationError) as rejected:
        facade._health(require_foreign_membership)
    assert rejected.value.code == "not-session-member"
    assert calls == [True]
    assert leader.node.role == ReplicaNode.LEADER
    assert leader.node.store.commit_index == before_commit
    assert _history(leader) == before_history


def _pending_row(runtime):
    with sqlite3.connect(runtime.journal.pending_path) as database:
        row = database.execute(
            "SELECT command_json,reserved_index,proposing_term FROM pending WHERE slot=1",
        ).fetchone()
    return row


def _private_digest(runtime):
    # Assertions must never display private grants or request response bodies.
    with runtime.local.store.read_transaction() as database:
        encoded = json.dumps(runtime.journal.private.capture(database), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def test_pending_proposal_does_not_block_local_connectivity_or_authorize_other_writes(
    runtimes, monkeypatch: pytest.MonkeyPatch,
) -> None:
    leader, successor, third = runtimes
    facade = PhysicalReadyReplicatedSessionCoordinator(leader)
    store = leader.local.store
    node_id = third.node.voter_id
    facade.create_enrollment_token(max_uses=1)
    announcement = CapabilityAnnouncement(
        capability_id="pending-health-capability", node_id=node_id,
        session_id=SESSION, type="demo.pending-health", protocol="demo.pending-health",
        protocol_version="1", status=CapabilityStatus.READY,
        properties={"purpose": "pending local health regression"}, announced_at=leader.clock(),
    )
    facade.announce_capability(
        announcement, actor_node_id=node_id, request_id="announce-pending-health",
    )
    for runtime in runtimes:
        runtime.materialize()
        store.mark_connected(
            node_id=runtime.node.voter_id, connection_id=f"local-{runtime.node.voter_id}",
            now=leader.clock() - timedelta(minutes=1) if runtime is third else leader.clock(),
        )
    before_history = _history(leader)
    before_state = copy.deepcopy(leader.node.state)
    before_commit = leader.node.store.commit_index
    before_private = _private_digest(leader)
    original_propose = leader.node.propose
    proposed = []

    def elect_after_outbox_save(command, transport):
        assert command.command_type == "PRODUCT_TRANSACTION"
        pending = _pending_row(leader)
        assert pending is not None
        assert pending[0].encode() == command.canonical_bytes()
        assert successor.node.start_election(successor.transport)
        assert successor.node.synchronize(successor.transport) == 2
        assert leader.node.role == ReplicaNode.FOLLOWER
        assert successor.node.role == ReplicaNode.LEADER
        assert successor.node.store.current_term > pending[2]
        proposed.append(command.command_id)
        # The real old leader rejects the append; no consensus result is mocked.
        return original_propose(command, transport)

    with monkeypatch.context() as patch:
        patch.setattr(leader.node, "propose", elect_after_outbox_save)
        with pytest.raises(StaleTerm, match="only the current leader"):
            facade.create_enrollment_token(max_uses=1)
    assert len(proposed) == 1
    pending = _pending_row(leader)
    assert pending is not None
    assert leader.node.store.receipt_for_command(proposed[0]) is None
    assert leader.node.store.entry_for_command(proposed[0]) is None
    assert _private_digest(leader) == before_private

    def assert_authority_unchanged():
        assert _pending_row(leader) == pending
        assert _private_digest(leader) == before_private
        assert _history(leader) == before_history
        for runtime in runtimes:
            assert runtime.node.state == before_state
            assert runtime.node.store.commit_index == before_commit
            assert runtime.node.store.last_log_index() == before_commit
            store.require_membership(session_id=SESSION, node_id=runtime.node.voter_id)
        assert next(
            item for item in store.list_capabilities(session_id=SESSION)
            if item.capability_id == announcement.capability_id
        ).status is CapabilityStatus.READY

    assert facade.sweep_stale(heartbeat_timeout_seconds=30) == ((node_id,), ())
    with store.read_transaction() as database:
        assert tuple(database.execute(
            "SELECT state,connection_id,last_error FROM node_connectivity WHERE node_id=?",
            (node_id,),
        ).fetchone()) == ("disconnected", None, "stale heartbeat")
    assert facade.disconnected(node_id=successor.node.voter_id) == ()
    assert facade.relay_started() is None
    assert facade.sweep_stale(heartbeat_timeout_seconds=30) == ((), ())
    with store.read_transaction() as database:
        assert database.execute(
            "SELECT COUNT(*) FROM node_connectivity WHERE state='connected'",
        ).fetchone()[0] == 0
    assert_authority_unchanged()

    forbidden = (
        "UPDATE session_events SET payload_json=payload_json",
        "UPDATE enrollment_tokens SET use_count=use_count+1",
        "UPDATE capabilities SET status='unavailable'",
        "DELETE FROM session_memberships",
        "UPDATE node_connectivity SET node_id=node_id",
        "INSERT INTO node_connectivity(node_id,state) VALUES('foreign','connected')",
        "DELETE FROM node_connectivity",
        "CREATE TABLE forbidden_health_table(value TEXT)",
        "DROP TRIGGER fcp_c03_journal_guard_update",
        "ATTACH DATABASE ':memory:' AS forbidden_health_database",
        "PRAGMA user_version=77",
    )
    for statement in forbidden:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized|prohibited"), leader.journal.local_connectivity_operation() as database:
            database.execute(
                "UPDATE node_connectivity SET state='connected' WHERE node_id=?", (node_id,),
            )
            database.execute(statement)
        with store.read_transaction() as database:
            assert database.execute(
                "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,),
            ).fetchone()[0] == "disconnected"
        assert_authority_unchanged()

    with pytest.raises(RuntimeError, match="rollback-only"), leader.journal.local_connectivity_operation() as database:
        database.execute(
            "UPDATE node_connectivity SET state='connected' WHERE node_id=?", (node_id,),
        )
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            database.execute("DELETE FROM session_memberships")
    with store.read_transaction() as database:
        assert database.execute(
            "SELECT state FROM node_connectivity WHERE node_id=?", (node_id,),
        ).fetchone()[0] == "disconnected"

    with (
        leader._lifecycle_lock,
        store.raw_transaction(),
        pytest.raises(RuntimeError, match="cannot inherit an active transaction"),
        leader.journal.local_connectivity_operation(),
    ):
        pytest.fail("local-only context inherited an authority stage")
    with pytest.raises(AuthorizationError) as rejected:
        facade.create_enrollment_token(max_uses=1)
    assert rejected.value.code == "federation-quorum-leader-required"
    assert_authority_unchanged()


_OWNERSHIP_CASES = ("credential-bind", "witness-bind", "close-before-start")


def _ownership_server_closed(server) -> None:
    assert server._thread is None
    assert server._server.socket.fileno() == -1


def _ownership_rebind(port: int) -> None:
    # No SO_REUSEADDR: this must not share a socket left open by the runtime.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
        probe.listen(1)


def _ownership_child(case: str, root: Path) -> None:
    # These are real product classes and the existing real identity/deployment
    # fixture. No lifecycle threads, RPC calls, elections or product mocks run.
    from catalog.federation.control_plane_runtime import (
        PhysicalReadyReplicatedFederationRuntime,
    )
    from catalog.federation.federation_v1_release_runtime import (
        FederationV1ReleaseRuntime,
    )
    from catalog.federation.tests.test_control_plane_physical_runtime import (
        _deployments,
    )

    deployment = _deployments(root)[0]
    base = deployment.listen_port
    kwargs = {
        "legacy_node_state_database": root / "legacy.sqlite3",
        "legacy_pairing_state_path": root / "legacy.json",
    }
    if case == "close-before-start":
        runtime = FederationV1ReleaseRuntime(deployment, **kwargs)
        servers = (
            runtime.server, runtime.credential_server, runtime.legacy_witness_server,
        )
        assert all(server._thread is None for server in servers)
        assert runtime._lifecycle_thread is None
        print(json.dumps({
            "case": case,
            "stage": "before-close",
            "server_threads_none": all(server._thread is None for server in servers),
            "lifecycle_thread_none": runtime._lifecycle_thread is None,
        }), file=sys.stderr, flush=True)
        runtime.close()
        runtime.close()
        for server in servers:
            _ownership_server_closed(server)
        for port in (base, base + 1, base + 2):
            _ownership_rebind(port)
    else:
        offset = {"credential-bind": 1, "witness-bind": 2}[case]
        with socket.socket() as blocker:
            if os.name == "nt":
                blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            blocker.bind(("127.0.0.1", base + offset))
            blocker.listen(1)
            failure = None
            try:
                FederationV1ReleaseRuntime(deployment, **kwargs)
            except OSError as exc:
                failure = exc
            assert failure is not None, "occupied required port must refuse construction"
            if os.name == "nt":
                assert failure.winerror in {10013, 10048}
            else:
                assert failure.errno == errno.EADDRINUSE

            # Retain the exception, traceback and partial runtime throughout the
            # assertions. Garbage collection cannot supply the missing cleanup.
            frames = []
            cursor = failure.__traceback__
            while cursor is not None:
                frames.append(cursor.tb_frame)
                cursor = cursor.tb_next
            assert any(
                frame.f_code.co_name == "server_bind"
                and isinstance(frame.f_locals.get("self"), socketserver.TCPServer)
                and frame.f_locals["self"].server_address == ("127.0.0.1", base + offset)
                for frame in frames
            ), "the original socket bind exception must reach the caller"
            partial = next(
                frame.f_locals["self"]
                for frame in frames
                if isinstance(
                    frame.f_locals.get("self"), PhysicalReadyReplicatedFederationRuntime,
                )
            )
            print(json.dumps({
                "case": case,
                "original_bind_error": type(failure).__name__,
                "errno": failure.errno,
                "winerror": getattr(failure, "winerror", None),
                "partial_control_socket_closed": partial.server._server.socket.fileno() == -1,
            }), file=sys.stderr, flush=True)
            _ownership_server_closed(partial.server)
            _ownership_rebind(base)
            if offset == 2:
                _ownership_server_closed(partial.credential_server)
                _ownership_rebind(base + 1)
            assert failure.__traceback__ is not None
    print(json.dumps({"case": case, "owned_socket_checks": "PASS"}), flush=True)


@pytest.mark.parametrize("case", _OWNERSHIP_CASES)
def test_owned_voter_sockets_close_without_start_or_successful_construction(
    tmp_path: Path, case: str,
) -> None:
    # A regression may deadlock inside socketserver.shutdown before start. A
    # separate child lets subprocess.run kill and wait for that one process at
    # its deadline; no worker thread is stranded inside the pytest process.
    completed = subprocess.run(
        [
            sys.executable, "-B", "-m",
            "catalog.federation.tests.test_control_plane_health_leadership_race",
            "--child", case, str(tmp_path),
        ],
        cwd=Path(__file__).resolve().parents[3],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert json.loads(completed.stdout) == {"case": case, "owned_socket_checks": "PASS"}


def _endpoint_servers(runtime):
    return (runtime.server, runtime.credential_server, runtime.legacy_witness_server)


def _endpoint_bind_failure(error: OSError, port: int):
    # Independent evidence for the regression's real exclusive socket, not a
    # simulated OSError or the fixture's own retry classifier.
    if os.name == "nt":
        assert getattr(error, "winerror", None) in {10013, 10048}
    else:
        assert error.errno == errno.EADDRINUSE
    cursor = error.__traceback__
    assert cursor is not None
    frames = []
    while cursor is not None:
        frames.append(cursor.tb_frame)
        last = cursor
        cursor = cursor.tb_next
    assert last.tb_frame.f_code is socketserver.TCPServer.server_bind.__code__
    assert linecache.getline(last.tb_frame.f_code.co_filename, last.tb_lineno).strip() == (
        "self.socket.bind(self.server_address)"
    )
    server = last.tb_frame.f_locals["self"]
    assert type(server) is _CredentialRPCServer
    assert server.server_address == ("127.0.0.1", port)
    assert server.socket.fileno() == -1
    return frames


def _endpoint_assert_closed(attempt, *, rebind: bool = False) -> None:
    for runtime in attempt["runtimes"]:
        assert runtime._lifecycle_thread is None
        for server in _endpoint_servers(runtime):
            _ownership_server_closed(server)
        if rebind:
            for offset in range(3):
                _ownership_rebind(runtime.deployment.listen_port + offset)
    for error, partial in attempt["failures"]:
        # Retaining both references excludes garbage collection as cleanup.
        assert error.__traceback__ is not None
        _ownership_server_closed(partial.server)
        if rebind:
            _ownership_rebind(partial.deployment.listen_port)


@contextmanager
def _endpoint_observation(monkeypatch: pytest.MonkeyPatch, *, block_every_attempt=False):
    original_deployments = _deployments
    original_runtime = FederationV1ReleaseRuntime
    attempts = []
    with ExitStack() as blockers:
        def allocate(root):
            if attempts:
                _endpoint_assert_closed(attempts[-1], rebind=True)
                attempts[-1]["closed_before_next_allocation"] = True
            deployments = original_deployments(root)
            attempt = {
                "root": root, "deployments": deployments, "runtimes": [],
                "failures": [], "closed_before_next_allocation": False,
            }
            attempts.append(attempt)
            if len(attempts) == 1 or block_every_attempt:
                # All real probes completed; now occupy voter 1's credential
                # endpoint and keep it occupied through reallocation/assertions.
                blocker = blockers.enter_context(socket.socket())
                if os.name == "nt":
                    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                blocker.bind(("127.0.0.1", deployments[1].listen_port + 1))
                blocker.listen(1)
                attempt["blocker"] = blocker
            return deployments

        def construct(deployment, **kwargs):
            attempt = attempts[-1]
            try:
                runtime = original_runtime(deployment, **kwargs)
            except OSError as error:
                assert deployment.local_voter_id == attempt["deployments"][1].local_voter_id
                frames = _endpoint_bind_failure(error, deployment.listen_port + 1)
                partial = next(
                    frame.f_locals["self"] for frame in frames
                    if isinstance(frame.f_locals.get("self"), original_runtime)
                )
                attempt["failures"].append((error, partial))
                attempt["serving_threads_started_before_failure"] = any(
                    server._thread is not None
                    for previous in attempt["runtimes"] for server in _endpoint_servers(previous)
                )
                raise
            attempt["runtimes"].append(runtime)
            return runtime

        monkeypatch.setitem(globals(), "_deployments", allocate)
        monkeypatch.setitem(globals(), "FederationV1ReleaseRuntime", construct)
        yield attempts


def _endpoint_failure_marker(attempt) -> dict:
    assert len(attempt["runtimes"]) == 1
    assert len(attempt["failures"]) == 1
    error, partial = attempt["failures"][0]
    port = attempt["deployments"][1].listen_port + 1
    _endpoint_bind_failure(error, port)
    _endpoint_assert_closed(attempt)
    assert attempt["blocker"].getsockname() == ("127.0.0.1", port)
    diagnostic_evidence = {
        "diagnostic": "health-fixture-bind",
        "stage": "actual-second-voter-credential-bind-denied",
        "exception_type": type(error).__name__,
        "errno": error.errno,
        "winerror": getattr(error, "winerror", None),
        "listener": "_CredentialRPCServer",
        "host": "127.0.0.1",
        "port": port,
        "real_allocation_completed": True,
        "blocker_still_held": True,
        "previous_runtime_sockets_closed": True,
        "partial_control_socket_closed": partial.server._server.socket.fileno() == -1,
        "traceback_retained": True,
        "serving_threads_started_before_failure": attempt["serving_threads_started_before_failure"],
    }
    print(json.dumps(diagnostic_evidence, sort_keys=True), file=sys.stderr, flush=True)
    return diagnostic_evidence


def test_fixture_reallocates_after_real_second_voter_bind_conflict(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, recwarn,
) -> None:
    with _endpoint_observation(monkeypatch) as attempts:
        try:
            ready = request.getfixturevalue("runtimes")
        except OSError:
            # On the old fixture this is the actual call-level RED failure,
            # after its generator's ExitStack has closed the earlier voter.
            diagnostic_evidence = _endpoint_failure_marker(attempts[0])
            raise
        diagnostic_evidence = _endpoint_failure_marker(attempts[0])
        assert diagnostic_evidence["previous_runtime_sockets_closed"]
        assert len(attempts) == 2
        first, second = attempts
        assert first["closed_before_next_allocation"]
        assert not first["serving_threads_started_before_failure"]
        assert len(recwarn) == 1
        assert recwarn[0].category is RuntimeWarning
        assert "Health fixture listener acquisition retry 1/1:" in str(recwarn[0].message)
        assert f'"port": {first["blocker"].getsockname()[1]}' in str(recwarn[0].message)
        assert first["root"] != second["root"]
        assert {
            deployment.local_voter_id for deployment in first["deployments"]
        }.isdisjoint(deployment.local_voter_id for deployment in second["deployments"])
        blocked_port = first["blocker"].getsockname()[1]
        assert all(
            blocked_port not in range(deployment.listen_port, deployment.listen_port + 3)
            for deployment in second["deployments"]
        )
        assert tuple(second["runtimes"]) == ready
        # Execute the original, unchanged real authorization/commit assertions.
        test_health_does_not_swallow_unrelated_authorization_failure(ready)


def test_fixture_stops_after_two_real_listener_bind_conflicts(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, recwarn,
) -> None:
    with _endpoint_observation(monkeypatch, block_every_attempt=True) as attempts:
        with pytest.raises(OSError) as rejected:
            request.getfixturevalue("runtimes")
        assert len(attempts) == 2
        assert rejected.value is attempts[1]["failures"][0][0]
        assert attempts[0]["closed_before_next_allocation"]
        assert len(recwarn) == 1
        assert recwarn[0].category is RuntimeWarning
        for attempt in attempts:
            assert not attempt["serving_threads_started_before_failure"]
            _endpoint_failure_marker(attempt)
            _endpoint_assert_closed(attempt)


def test_fixture_does_not_retry_listener_bind_error_after_servers_start(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_deployments = _deployments
    original_runtime = FederationV1ReleaseRuntime
    allocations = []
    constructed = []
    with ExitStack() as blockers:
        def allocate(root):
            result = original_deployments(root)
            allocations.append(result)
            return result

        def construct(deployment, **kwargs):
            runtime = original_runtime(deployment, **kwargs)
            constructed.append(runtime)
            return runtime

        def conflict_after_start(_leader, **_kwargs):
            assert len(constructed) == 3
            assert all(
                server._thread is not None and server._thread.is_alive()
                for runtime in constructed for server in _endpoint_servers(runtime)
            )
            # Use the final current deployment so even a wrongly broadened
            # constructor catch would otherwise recognize this exact endpoint.
            target = constructed[-1]
            target.credential_server.close()
            blocker = blockers.enter_context(socket.socket())
            if os.name == "nt":
                blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            blocker.bind(("127.0.0.1", target.deployment.listen_port + 1))
            blocker.listen(1)
            CredentialReplicaServer(
                target.node, target.codec, target.credential_store,
                host="127.0.0.1", port=target.deployment.listen_port + 1,
            )

        monkeypatch.setitem(globals(), "_deployments", allocate)
        monkeypatch.setitem(globals(), "FederationV1ReleaseRuntime", construct)
        monkeypatch.setattr(original_runtime, "bootstrap_new_federation", conflict_after_start)
        with pytest.raises(OSError) as rejected:
            request.getfixturevalue("runtimes")
        assert len(allocations) == 1
        assert len(constructed) == 3
        target = constructed[-1]
        _endpoint_bind_failure(rejected.value, target.deployment.listen_port + 1)
        assert target.deployment.local_voter_id == allocations[0][-1].local_voter_id
        assert target.deployment.listen_host == "127.0.0.1"
        assert target.deployment.listen_port == allocations[0][-1].listen_port
        for runtime in constructed:
            assert runtime._lifecycle_thread is None
            for server in _endpoint_servers(runtime):
                _ownership_server_closed(server)


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] != "--child" or sys.argv[2] not in _OWNERSHIP_CASES:
        raise SystemExit("invalid owned-socket child arguments")
    _ownership_child(sys.argv[2], Path(sys.argv[3]))

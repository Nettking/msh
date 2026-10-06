"""Real public-journal fixture acquisition owns every unstarted voter."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import threading
import time
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace

import pytest

from catalog.federation.control_plane_replication import (
    ControlPlaneError,
    QuorumUnavailable,
    ReplicaNode,
    StaleTerm,
)
from catalog.federation.tests import test_control_plane_public_journal as journal
from catalog.federation.tests.test_control_plane_health_leadership_race import (
    _endpoint_bind_failure,
    _endpoint_servers,
    _ownership_server_closed,
)

RETRY_PREFIX = "Public journal fixture listener acquisition retry 1/1: "


def _assert_unstarted(runtime) -> None:
    assert runtime._lifecycle_thread is None
    assert all(server._thread is None for server in _endpoint_servers(runtime))


def _assert_attempt_closed(attempt) -> None:
    for runtime in attempt["runtimes"]:
        _assert_unstarted(runtime)
        for server in _endpoint_servers(runtime):
            _ownership_server_closed(server)
    for error, partial in attempt["failures"]:
        assert error.__traceback__ is not None
        _ownership_server_closed(partial.server)


@contextmanager
def _construction_observation(
    monkeypatch, *, block_attempts=1, blocked_index=1,
    nonlistener=None, cleanup_error=None,
):
    original_deployments = journal._deployments
    original_runtime = journal.FederationV1ReleaseRuntime
    original_relay_for = journal._Cluster.relay_for
    attempts = []
    with monkeypatch.context() as patch, ExitStack() as final_cleanup:
        def allocate(root):
            if attempts:
                _assert_attempt_closed(attempts[-1])
                attempts[-1]["closed_before_next_allocation"] = True
            deployments = original_deployments(root)
            attempt = {
                "root": root, "deployments": deployments, "runtimes": [],
                "failures": [], "close_order": [], "other_error": None,
                "closed_before_next_allocation": False,
            }
            attempts.append(attempt)
            if len(attempts) <= block_attempts:
                # All real probes have returned. Keep a separate actual owner
                # at a credential endpoint throughout the following attempt.
                blocker = final_cleanup.enter_context(socket.socket())
                if os.name == "nt":
                    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                blocker.bind(("127.0.0.1", deployments[blocked_index].listen_port + 1))
                blocker.listen(1)
                attempt["blocker"] = blocker
            return deployments

        def refuse_filesystem(attempt):
            # Real non-listener OSError, with a filesystem traceback rather
            # than a fabricated bind exception that could fool the classifier.
            try:
                attempt["root"].read_bytes()
            except OSError as error:
                attempt["other_error"] = error
                raise
            raise AssertionError("reading an actual directory must refuse")

        def construct(deployment, **kwargs):
            attempt = attempts[-1]
            if nonlistener == "runtime-setup" and len(attempt["runtimes"]) == 1:
                refuse_filesystem(attempt)
            try:
                runtime = original_runtime(deployment, **kwargs)
            except OSError as error:
                assert deployment.local_voter_id == attempt["deployments"][blocked_index].local_voter_id
                frames = _endpoint_bind_failure(error, deployment.listen_port + 1)
                partial = next(
                    frame.f_locals["self"] for frame in frames
                    if isinstance(frame.f_locals.get("self"), original_runtime)
                )
                attempt["failures"].append((error, partial))
                final_cleanup.callback(partial.server.close)
                for previous in attempt["runtimes"]:
                    _assert_unstarted(previous)
                raise
            original_close = runtime.close
            # Diagnostic cleanup uses the original real API even if a guard
            # below deliberately makes the fixture's close callback raise.
            final_cleanup.callback(original_close)
            index = len(attempt["runtimes"])
            attempt["runtimes"].append(runtime)

            def tracked_close():
                attempt["close_order"].append(index)
                original_close()
                if cleanup_error is not None and index == blocked_index - 1:
                    raise cleanup_error

            runtime.close = tracked_close
            return runtime

        def relay_for(cluster, index, **options):
            if nonlistener == "relay-setup" and index == 1:
                refuse_filesystem(attempts[-1])
            return original_relay_for(cluster, index, **options)

        patch.setattr(journal, "_deployments", allocate)
        patch.setattr(journal, "FederationV1ReleaseRuntime", construct)
        patch.setattr(journal._Cluster, "relay_for", relay_for)
        yield attempts


def _failure_marker(attempt, attempt_count):
    assert len(attempt["runtimes"]) == 1
    assert len(attempt["failures"]) == 1
    error, partial = attempt["failures"][0]
    port = attempt["deployments"][1].listen_port + 1
    _endpoint_bind_failure(error, port)
    assert attempt["blocker"].fileno() >= 0
    assert attempt["blocker"].getsockname() == ("127.0.0.1", port)
    _ownership_server_closed(partial.server)
    previous = attempt["runtimes"][0]
    _assert_unstarted(previous)
    evidence = {
        "diagnostic": "public-journal-fixture-bind",
        "stage": "actual-second-voter-credential-bind-denied",
        "exception_type": type(error).__name__,
        "errno": error.errno,
        "winerror": getattr(error, "winerror", None),
        "listener": "_CredentialRPCServer",
        "host": "127.0.0.1", "port": port,
        "real_allocation_completed": True,
        "blocker_still_held": True,
        "previous_runtime_sockets_closed": all(
            server._server.socket.fileno() == -1 for server in _endpoint_servers(previous)
        ),
        "partial_control_socket_closed": True,
        "serving_threads_started_before_failure": False,
        "traceback_retained": True,
        "attempt_count": attempt_count,
        "closed_before_next_allocation": attempt["closed_before_next_allocation"],
    }
    print(json.dumps(evidence, sort_keys=True), file=sys.stderr, flush=True)
    return evidence


def _retry_warnings(records):
    # Assert only this fixture's own retry record; do not classify unrelated
    # warning ownership by a process-wide total count.
    return [record for record in records
            if record.category is RuntimeWarning and str(record.message).startswith(RETRY_PREFIX)]


def test_public_journal_cluster_reallocates_after_real_second_voter_bind_conflict(
    tmp_path, monkeypatch, recwarn,
):
    with _construction_observation(monkeypatch) as attempts:
        try:
            cluster = journal._Cluster(tmp_path)
        except OSError:
            # Exact9d RED preserves the real bind exception. Observe ownership
            # before this test's final cleanup, then re-raise the original.
            _failure_marker(attempts[0], len(attempts))
            raise
        evidence = _failure_marker(attempts[0], len(attempts))
        assert len(attempts) == 2
        first, second = attempts
        assert evidence["previous_runtime_sockets_closed"] is True
        assert first["closed_before_next_allocation"] is True
        assert len(_retry_warnings(recwarn)) == 1
        assert f'"port": {first["blocker"].getsockname()[1]}' in str(_retry_warnings(recwarn)[0].message)
        assert first["root"] != second["root"]
        assert {d.local_voter_id for d in first["deployments"]}.isdisjoint(
            d.local_voter_id for d in second["deployments"]
        )
        blocked_port = first["blocker"].getsockname()[1]
        assert all(blocked_port not in range(d.listen_port, d.listen_port + 3)
                   for d in second["deployments"])
        assert cluster.runtimes == second["runtimes"]
        assert len(cluster.runtimes) == len(cluster.relays) == 3
        assert cluster.running_runtimes == cluster.running_relays == set()
        for runtime in cluster.runtimes:
            _assert_unstarted(runtime)
            assert not runtime.ready


def test_public_journal_cluster_stops_after_two_real_listener_conflicts(
    tmp_path, monkeypatch, recwarn,
):
    with _construction_observation(monkeypatch, block_attempts=2) as attempts:
        with pytest.raises(OSError) as rejected:
            journal._Cluster(tmp_path)
        assert len(attempts) == 2
        assert rejected.value is attempts[1]["failures"][0][0]
        assert attempts[0]["closed_before_next_allocation"] is True
        assert len(_retry_warnings(recwarn)) == 1
        for attempt in attempts:
            _assert_attempt_closed(attempt)
            assert _failure_marker(attempt, len(attempts))["previous_runtime_sockets_closed"] is True


@pytest.mark.parametrize("where", ["runtime-setup", "relay-setup"])
def test_public_journal_cluster_does_not_retry_nonlistener_errors(
    tmp_path, monkeypatch, recwarn, where,
):
    with _construction_observation(monkeypatch, block_attempts=0, nonlistener=where) as attempts:
        with pytest.raises(OSError) as rejected:
            journal._Cluster(tmp_path)
        assert len(attempts) == 1
        assert rejected.value is attempts[0]["other_error"]
        assert len(attempts[0]["runtimes"]) == (1 if where == "runtime-setup" else 2)
        assert attempts[0]["failures"] == []
        assert _retry_warnings(recwarn) == []
        _assert_attempt_closed(attempts[0])


def test_public_journal_cluster_propagates_cleanup_failure_without_reallocation(
    tmp_path, monkeypatch, recwarn,
):
    cleanup_error = RuntimeError("owned public-journal fixture cleanup refused")
    # Voter2 denial leaves two completed voters. The first close callback raises
    # only after its real close; ExitStack must still drain the other voter.
    with _construction_observation(
        monkeypatch, blocked_index=2, cleanup_error=cleanup_error,
    ) as attempts:
        with pytest.raises(RuntimeError) as rejected:
            journal._Cluster(tmp_path)
        assert rejected.value is cleanup_error
        assert len(attempts) == 1
        assert len(attempts[0]["runtimes"]) == 2
        assert len(attempts[0]["failures"]) == 1
        assert attempts[0]["close_order"] == [1, 0]
        assert attempts[0]["closed_before_next_allocation"] is False
        assert len(_retry_warnings(recwarn)) == 1
        _assert_attempt_closed(attempts[0])


@pytest.mark.parametrize("failure", [None, RuntimeError("setup failed"), SystemExit("setup cancelled")])
def test_initial_bootstrap_schedule_restores_each_real_method_on_every_exit(failure):
    calls = []
    runtimes = [SimpleNamespace(_drive_lifecycle_round=lambda index=index: calls.append(index))
                for index in range(3)]
    original = [runtime._drive_lifecycle_round for runtime in runtimes]

    def exercise():
        with journal._initial_bootstrap_schedule(runtimes):
            for runtime in runtimes:
                runtime._drive_lifecycle_round()
            assert calls == []
            if failure is not None:
                raise failure

    if failure is None:
        exercise()
    else:
        with pytest.raises(type(failure)) as caught:
            exercise()
        assert caught.value is failure
    assert [runtime._drive_lifecycle_round for runtime in runtimes] == original
    for runtime in runtimes:
        runtime._drive_lifecycle_round()
    assert calls == [0, 1, 2]


def test_initial_schedule_restores_inherited_lookup_and_releases_captured_wrapper(monkeypatch):
    calls = []

    class Runtime:
        def _drive_lifecycle_round(self):
            calls.append("original")

    runtime = Runtime()
    assert "_drive_lifecycle_round" not in vars(runtime)
    with journal._initial_bootstrap_schedule([runtime]):
        captured = runtime._drive_lifecycle_round
        captured()
        assert calls == []
    assert "_drive_lifecycle_round" not in vars(runtime)
    captured()
    assert calls == ["original"]
    monkeypatch.setattr(Runtime, "_drive_lifecycle_round", lambda _self: calls.append("later-class-method"))
    runtime._drive_lifecycle_round()
    assert calls == ["original", "later-class-method"]


def test_initial_slow_seal_keeps_original_creator_and_restores_automatic_rounds(
    tmp_path, monkeypatch, record_property,
):
    cluster = journal._Cluster(tmp_path)
    monkeypatch.setattr(journal, "_Cluster", lambda _root: cluster)
    original_drives = [runtime._drive_lifecycle_round for runtime in cluster.runtimes]
    creator = cluster.runtimes[0]
    creator_id = creator.node.voter_id
    original_wait = journal._wait
    wait_checked = []

    async def restored_wait(predicate, description):
        assert [runtime._drive_lifecycle_round for runtime in cluster.runtimes] == original_drives
        wait_checked.append(True)
        await original_wait(predicate, description)

    monkeypatch.setattr(journal, "_wait", restored_wait)
    scope = threading.local()
    propose = creator._propose_bootstrap_command
    append = creator.transport.append_entries
    delayed = []
    ages = []

    def observed_propose(command):
        previous = getattr(scope, "seal", False)
        scope.seal = command.command_id.startswith("bootstrap-seal-")
        try:
            return propose(command)
        finally:
            scope.seal = previous

    def delayed_append(target, **request):
        if getattr(scope, "seal", False) and not delayed:
            before = time.monotonic()
            time.sleep(creator.election_timeout_seconds + creator.heartbeat_seconds + 0.2)
            delayed.append(time.monotonic() - before)
            ages.extend(time.monotonic() - runtime.node.last_leader_contact
                        for runtime in cluster.runtimes[1:])
            assert all(age > runtime.election_timeout_seconds
                       for age, runtime in zip(ages, cluster.runtimes[1:], strict=True))
            assert creator.node.role == ReplicaNode.LEADER
            assert creator.node.leader_id == creator_id
            assert creator.node.store.current_term == request["leader_term"]
        return append(target, **request)

    monkeypatch.setattr(creator, "_propose_bootstrap_command", observed_propose)
    monkeypatch.setattr(creator.transport, "append_entries", delayed_append)

    async def scenario():
        async with journal._cluster(tmp_path) as current:
            assert current is cluster
            assert creator is current.runtimes[0]
            assert all(runtime.ready for runtime in current.runtimes)
            genesis = creator._fresh_genesis()
            assert genesis.payload["creator_node_id"] == creator_id
            assert genesis.payload["federation_id"] == journal.FEDERATION
            assert genesis.payload["session_id"] == journal.SESSION
            assert set(genesis.payload["members"]) == set(creator.node.configuration.voter_ids)
            leadership = creator.node.state["leaders"][journal.SESSION]
            assert leadership["leader_node_id"] == creator_id
            assert leadership["creator_node_id"] == creator_id
            assert leadership["term"] == 1
            assert creator.node.role == ReplicaNode.LEADER
            assert creator.node.leader_id == creator_id
            for runtime in current.runtimes:
                await asyncio.to_thread(runtime.materialize)
            prefix = journal._journal(creator)
            assert prefix
            assert all(journal._journal(runtime) == prefix for runtime in current.runtimes)
            seals = [entry for entry in creator.node.store.entries()
                     if entry.command.command_id.startswith("bootstrap-seal-")]
            assert len(seals) == 1
            assert seals[0].log_index <= creator.node.store.commit_index
            # Supported retry validates the exact committed genesis, without
            # inventing another identity or relaxing its creator provenance.
            with pytest.raises(ControlPlaneError, match="conflicts with committed fresh Federation identity"):
                await asyncio.to_thread(creator.bootstrap_new_federation,
                    federation_id=journal.FEDERATION, session_id=journal.SESSION,
                    creator_node_id="unrelated-creator", display_name="Public journal continuity")
            assert journal._journal(creator) == prefix
        assert not cluster.running_runtimes
        assert not cluster.running_relays

    asyncio.run(scenario())
    assert delayed and wait_checked
    record_property("initial_slow_seal", json.dumps({
        "actual_delay_seconds": delayed[0], "follower_contact_ages": ages,
        "original_creator_and_current_leader_preserved": True,
        "product_term": 1, "restored_before_readiness": True,
        "original_ci_cause_verified": False,
    }, sort_keys=True))


@pytest.mark.parametrize("failure", [StaleTerm("bootstrap leader lost"), QuorumUnavailable("no quorum")])
def test_cluster_does_not_swallow_pre_genesis_fencing_or_quorum_error(tmp_path, monkeypatch, failure):
    cluster = journal._Cluster(tmp_path)
    monkeypatch.setattr(journal, "_Cluster", lambda _root: cluster)
    original_drives = [runtime._drive_lifecycle_round for runtime in cluster.runtimes]
    creator = cluster.runtimes[0]
    calls = []

    def refuse(**kwargs):
        calls.append(kwargs)
        assert creator._fresh_genesis() is None
        raise failure

    monkeypatch.setattr(creator, "bootstrap_new_federation", refuse)

    async def scenario():
        with pytest.raises(type(failure)) as caught:
            async with journal._cluster(tmp_path):
                pytest.fail("failed bootstrap entered the test body")
        assert caught.value is failure

    asyncio.run(scenario())
    assert len(calls) == 1
    assert [runtime._drive_lifecycle_round for runtime in cluster.runtimes] == original_drives
    assert not cluster.running_runtimes
    assert not cluster.running_relays


def test_cluster_retains_real_unsealed_failure_and_restores_lifecycle_before_cleanup(tmp_path, monkeypatch):
    cluster = journal._Cluster(tmp_path)
    monkeypatch.setattr(journal, "_Cluster", lambda _root: cluster)
    creator = cluster.runtimes[0]
    original_drives = [runtime._drive_lifecycle_round for runtime in cluster.runtimes]
    failure = RuntimeError("real public prefix interrupted before readiness seal")
    observed = []

    def interrupted_seal(**_kwargs):
        assert creator._fresh_genesis() is not None
        assert creator.node.state["product_journal"]["sessions"][journal.SESSION]
        assert journal._journal(creator)
        assert not creator.ready
        observed.append(True)
        raise failure

    monkeypatch.setattr(creator, "_commit_readiness_seal", interrupted_seal)
    for index, runtime in enumerate(cluster.runtimes):
        close = runtime.close

        def restored_close(*, _close=close, _index=index):
            assert cluster.runtimes[_index]._drive_lifecycle_round == original_drives[_index]
            _close()

        monkeypatch.setattr(runtime, "close", restored_close)

    async def scenario():
        with pytest.raises(RuntimeError) as caught:
            async with journal._cluster(tmp_path):
                pytest.fail("unsealed failed bootstrap entered the test body")
        assert caught.value is failure

    asyncio.run(scenario())
    assert observed
    assert [runtime._drive_lifecycle_round for runtime in cluster.runtimes] == original_drives
    assert not cluster.running_runtimes
    assert not cluster.running_relays

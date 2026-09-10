"""Real public-journal fixture acquisition owns every unstarted voter."""

from __future__ import annotations

import json
import os
import socket
import sys
from contextlib import ExitStack, contextmanager

import pytest

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

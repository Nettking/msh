"""Independent-node startup must avoid ephemeral listeners and retain failures."""
from __future__ import annotations

import argparse
import asyncio
import errno
import io
import json
from types import SimpleNamespace

import pytest

from catalog.federation.tests.test_control_plane_physical_runtime import (
    _tcp_dynamic_port_range,
)
from demo.icse.network import run, worker


def test_voter_ports_are_outside_native_automatic_allocation_range():
    # Earlier voters open port=0 relay listeners before later voters start.
    # Those native allocations must never consume an unstarted voter triple.
    start, end = _tcp_dynamic_port_range()
    ports = run.allocate_ports()
    listeners = {port + offset for port in ports for offset in range(3)}
    assert len(ports) == 3 and len(listeners) == 9
    assert all(1024 <= port <= 65535 for port in listeners)
    assert not any(start <= port <= end for port in listeners)


def test_exited_startup_retains_safe_cause_without_teardown_masking(tmp_path, monkeypatch):
    response = {"startup_error": {
        "error_type": "OSError", "errno": errno.EADDRINUSE, "winerror": 10048,
        "message": "secret-invitation-marker", "config": "private-identity-marker",
    }}
    process = SimpleNamespace(
        stdin=io.StringIO(), stdout=io.StringIO(json.dumps(response) + "\n"),
        poll=lambda: 1, returncode=1,
    )
    monkeypatch.setattr(run.subprocess, "Popen", lambda *args, **kwargs: process)
    with pytest.raises(RuntimeError) as raised:
        run.Child("voter-b", tmp_path / "worker.json")
    context = getattr(raised.value, "public_context", None)
    assert context == {
        "worker": "voter-b", "error_type": "OSError", "errno": errno.EADDRINUSE,
        "winerror": 10048, "exit_code": 1, "process_stopped": True,
    }
    assert process.stdin.closed and process.stdout.closed
    assert "secret-invitation-marker" not in json.dumps(context)
    assert "private-identity-marker" not in json.dumps(context)


def test_worker_emits_safe_startup_error_and_still_closes_resources(monkeypatch, capsys):
    closed = []

    class FailedNode:
        def __init__(self, config):
            pass

        async def start(self):
            raise OSError(errno.EADDRINUSE, "secret-enrollment-marker")

        async def close(self):
            closed.append(True)

    monkeypatch.setattr(worker, "Node", FailedNode)
    assert asyncio.run(worker.serve({})) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"startup_error": {
        "error_type": "OSError", "errno": errno.EADDRINUSE, "winerror": None,
    }}
    assert closed == [True]
    assert "secret-enrollment-marker" not in captured.out
    assert "secret-enrollment-marker" in captured.err


def test_simulated_ephemeral_relay_cannot_steal_later_voter_listener(monkeypatch):
    # Simulate native port=0 allocation deterministically. This is a counterexample
    # to the old allocator, not a claim about the unavailable CI private traceback.
    occupied = set()
    sockets = []
    ephemeral = iter((51000, 52000, 53000, 52000))
    choices = iter((0, 100, 200))
    peak = 0

    class Socket:
        def __init__(self):
            self.port = None
            sockets.append(self)

        def bind(self, address):
            nonlocal peak
            port = address[1] or next(ephemeral)
            if port in occupied:
                raise OSError(errno.EADDRINUSE, "listener occupied")
            self.port = port
            occupied.add(port)
            peak = max(peak, len(occupied))

        def getsockname(self):
            return "127.0.0.1", self.port

        def close(self):
            if self.port is not None:
                occupied.remove(self.port)
                self.port = None

    monkeypatch.setattr(run, "socket", SimpleNamespace(socket=Socket))
    monkeypatch.setattr(run, "_tcp_dynamic_port_range", lambda: (50000, 60000))
    monkeypatch.setattr(run.secrets, "randbelow", lambda bound: next(choices))
    ports = run.allocate_ports()
    assert not occupied
    assert len(sockets) == 9 and all(sock.port is None for sock in sockets)
    assert peak == 9
    # The old probes selected 51000/52000/53000, then released all reservations.
    # Earlier voter-a's relay can select 52000 before voter-b binds its triple.
    relay = Socket()
    relay.bind(("127.0.0.1", 0))
    for port in ports:
        for offset in range(3):
            Socket().bind(("127.0.0.1", port + offset))
    assert len(occupied) == 10
    for sock in sockets:
        sock.close()
    assert not occupied


def test_allocator_holds_all_triples_and_closes_them_on_exhaustion(monkeypatch):
    occupied = set()
    peak = 0

    class Socket:
        def __init__(self):
            self.port = None

        def bind(self, address):
            nonlocal peak
            port = address[1]
            if port in occupied:
                raise OSError(errno.EADDRINUSE, "occupied")
            self.port = port
            occupied.add(port)
            peak = max(peak, len(occupied))

        def close(self):
            if self.port is not None:
                occupied.remove(self.port)
                self.port = None

    monkeypatch.setattr(run, "socket", SimpleNamespace(socket=Socket))
    monkeypatch.setattr(run, "_tcp_dynamic_port_range", lambda: (50000, 60000))
    monkeypatch.setattr(run.secrets, "randbelow", lambda bound: 0)
    with pytest.raises(RuntimeError, match="cannot allocate"):
        run.allocate_ports()
    assert peak == 3  # First triple stays owned while the second exhausts its probes.
    assert not occupied


@pytest.mark.parametrize("output", [
    "Start : 1026\nCount : 100\n", "Start : 50000\nCount : 0\n",
    "Start : 65000\nCount : 1000\n", "unknown range",
])
def test_unverifiable_native_range_is_refused(monkeypatch, output):
    monkeypatch.setattr(run, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(run.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(stdout=output))
    with pytest.raises(RuntimeError, match="range"):
        run._tcp_dynamic_port_range()


def test_startup_eof_preserves_original_exception_and_closes_pipes(tmp_path, monkeypatch):
    process = SimpleNamespace(
        stdin=io.StringIO(), stdout=io.StringIO(), poll=lambda: 1, returncode=1,
    )
    monkeypatch.setattr(run.subprocess, "Popen", lambda *args, **kwargs: process)
    with pytest.raises(RuntimeError) as raised:
        run.Child("voter-c", tmp_path / "worker.json")
    assert "private worker.stderr.log" in str(raised.value.__cause__)
    assert raised.value.public_context["worker"] == "voter-c"
    assert raised.value.public_context["process_stopped"] is True
    assert process.stdin.closed and process.stdout.closed


def test_failed_startup_cleanup_cannot_claim_all_processes_stopped(tmp_path, monkeypatch, capsys):
    process = SimpleNamespace(
        stdin=io.StringIO(), stdout=io.StringIO(json.dumps({"startup_error": {
            "error_type": "secret-enrollment-marker", "errno": "private-identity-marker",
            "winerror": True,
        }}) + "\n"), poll=lambda: None,
    )
    monkeypatch.setattr(run.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(run, "source_identity", lambda args: {"source_sha": "a" * 40})
    monkeypatch.setattr(run, "provision", lambda state, args: {"voter-b": state / "worker.json"})

    failed_children = []

    def failed_stop(self, *, force=False):
        failed_children.append(self)
        assert force
        raise OSError("secret-cleanup-marker")

    monkeypatch.setattr(run.Child, "stop", failed_stop)
    output = tmp_path / "failed-output"
    try:
        assert run.campaign(argparse.Namespace(output=output, step_pause=0)) == 1
        summary = json.loads((output / "public/summary.json").read_text())
        assert summary["result"] == "FAIL"
        assert summary["all_owned_processes_stopped"] is False
        assert summary["failure"]["startup"]["error_type"] == "unclassified"
        assert summary["failure"]["startup"]["errno"] is None
        assert summary["failure"]["startup"]["winerror"] is None
        assert not summary["events"] and not summary["checks"]
        assert not (output / "public/events.jsonl").exists()
        captured = capsys.readouterr()
        visible = json.dumps(summary) + captured.out + captured.err
        for marker in ("secret-enrollment-marker", "private-identity-marker", "secret-cleanup-marker"):
            assert marker not in visible
        private = (output / "private-state/driver-failure.log").read_text()
        assert "secret-cleanup-marker" in private
    finally:
        process.stdin.close()
        process.stdout.close()
        for child in failed_children:
            child.stderr.close()


def test_process_creation_failure_is_reported_without_publishing_message(tmp_path, monkeypatch):
    logs = []

    def refused_spawn(*args, **kwargs):
        logs.append(kwargs["stderr"])
        raise OSError(errno.EACCES, "private-spawn-marker")

    monkeypatch.setattr(run.subprocess, "Popen", refused_spawn)
    with pytest.raises(run.WorkerStartupFailure) as raised:
        run.Child("reviewer", tmp_path / "worker.json")
    context = raised.value.public_context
    assert context["worker"] == "reviewer"
    assert context["errno"] == errno.EACCES
    assert context["process_stopped"] is True
    assert context["exit_code"] is None
    assert "private-spawn-marker" not in json.dumps(context)
    assert all(stream.closed for stream in logs)


def test_reader_thread_failure_stops_the_unregistered_owned_process(tmp_path, monkeypatch):
    exit_code = None

    def terminate():
        nonlocal exit_code
        exit_code = -15

    def failed_start():
        raise RuntimeError("private-thread-marker")

    process = SimpleNamespace(
        stdin=io.StringIO(), stdout=io.StringIO(), poll=lambda: exit_code,
        terminate=terminate, wait=lambda timeout: exit_code,
    )
    monkeypatch.setattr(run.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(run, "threading", SimpleNamespace(
        Thread=lambda **kwargs: SimpleNamespace(start=failed_start),
    ))
    try:
        with pytest.raises(RuntimeError) as raised:
            run.Child("voter-a", tmp_path / "worker.json")
        context = getattr(raised.value, "public_context", {})
        assert context.get("process_stopped") is True
        assert context["exit_code"] == -15
        assert process.stdin.closed and process.stdout.closed
        assert "private-thread-marker" in str(raised.value.__cause__)
        assert "private-thread-marker" not in json.dumps(context)
    finally:
        process.stdin.close()
        process.stdout.close()

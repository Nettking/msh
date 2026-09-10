"""Real allocator bind failures must close their socket without garbage collection."""

from __future__ import annotations

import errno
import json
import os
import socket
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from catalog.federation.tests import test_control_plane_health_leadership_race as health
from catalog.federation.tests import test_control_plane_physical_runtime as physical

# Use the original fixture and compose the original assertion body unchanged.
runtimes = health.runtimes


@contextmanager
def _real_second_probe_conflict(monkeypatch: pytest.MonkeyPatch, *, retain: bool):
    original_socket = socket.socket
    observed = {
        "probe_binds": 0,
        "native_error": None,
        "blocker": None,
        "retained_probe": None,
    }

    class ProbeSocket(original_socket):
        def bind(self, address):
            if address[1] == 0:
                return super().bind(address)
            observed["probe_binds"] += 1
            if observed["probe_binds"] != 2:
                return super().bind(address)
            # The first real probe has bound successfully. Occupy the second
            # candidate only now; the allocator still executes its real bind.
            blocker = original_socket()
            observed["blocker"] = blocker
            if os.name == "nt":
                blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            blocker.bind(address)
            blocker.listen(1)
            try:
                return super().bind(address)
            except OSError as error:
                observed["native_error"] = {
                    "exception_type": type(error).__name__,
                    "errno": error.errno,
                    "winerror": getattr(error, "winerror", None),
                    "endpoint": list(address),
                }
                if retain:
                    observed["retained_probe"] = self
                raise

    # Scope interception to this allocator module, without changing the global
    # socket module, listener constructors, warnings, or garbage collection.
    with monkeypatch.context() as patch:
        patch.setattr(physical, "socket", SimpleNamespace(socket=ProbeSocket))
        try:
            yield observed
        finally:
            retained = observed["retained_probe"]
            if retained is not None:
                retained.close()
            blocker = observed["blocker"]
            if blocker is not None:
                blocker.close()


def _native_conflict_evidence(observed) -> dict:
    error = observed["native_error"]
    assert error is not None, "the selected real allocator probe must reach native bind failure"
    if os.name == "nt":
        assert error["winerror"] in {10013, 10048}
    else:
        assert os.name == "posix" and error["errno"] == errno.EADDRINUSE
    blocker = observed["blocker"]
    assert blocker is not None and blocker.fileno() >= 0
    assert list(blocker.getsockname()) == error["endpoint"]
    return {
        "diagnostic": "allocator-probe-ownership",
        "stage": "actual-second-probe-bind-denied",
        **error,
        "blocker_still_held": True,
        "probe_binds": observed["probe_binds"],
    }


def test_failed_real_probe_is_closed_before_garbage_collection(monkeypatch):
    with _real_second_probe_conflict(monkeypatch, retain=True) as observed:
        used = set()
        port = physical._free_port_triple(used)
        evidence = _native_conflict_evidence(observed)
        assert used == {port, port + 1, port + 2}
        assert observed["native_error"]["endpoint"][1] not in used
        probe = observed["retained_probe"]
        assert probe is not None
        evidence.update(
            allocator_returned=True,
            failed_probe_retained=True,
            failed_probe_explicitly_closed=probe.fileno() == -1,
        )
        print(json.dumps(evidence, sort_keys=True), file=sys.stderr, flush=True)
        assert probe.fileno() == -1, (
            "failed real allocation probe must be explicitly closed before garbage collection"
        )


def test_real_probe_conflict_preserves_original_health_warning_assertion(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, recwarn,
):
    with _real_second_probe_conflict(monkeypatch, retain=False) as observed:
        try:
            health.test_fixture_stops_after_two_real_listener_bind_conflicts(
                request, monkeypatch, recwarn,
            )
        finally:
            # This is post-call/unwind observation, not an assertion-time hook.
            # Keep warning objects untouched and never retain the failed probe.
            error = observed["native_error"]
            blocker = observed["blocker"]
            evidence = {
                "diagnostic": "allocator-probe-ownership",
                "stage": "actual-second-probe-bind-denied" if error else "probe-failure-not-reached",
                "native_error": error,
                "blocker_still_held": blocker is not None and blocker.fileno() >= 0,
                "probe_binds": observed["probe_binds"],
            }
            records = tuple(recwarn)
            evidence.update(
                failed_probe_retained=False,
                warning_count=len(records),
                warning_categories=[record.category.__name__ for record in records],
                warning_records=[
                    {
                        "category": record.category.__name__,
                        "message": str(record.message),
                        "filename": record.filename,
                        "lineno": record.lineno,
                    }
                    for record in records
                ],
                resource_warning_count=sum(record.category is ResourceWarning for record in records),
                retry_warning_count=sum(
                    record.category is RuntimeWarning
                    and str(record.message).startswith("Health fixture listener acquisition retry 1/1:")
                    for record in records
                ),
                observation_stage="POST_CALL_UNWIND_BEFORE_FIXTURE_TEARDOWN",
            )
            print(json.dumps(evidence, sort_keys=True), file=sys.stderr, flush=True)
        _native_conflict_evidence(observed)
        assert len(recwarn) == 1
        assert recwarn[0].category is RuntimeWarning

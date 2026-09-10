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

from catalog.federation.tests import test_control_plane_physical_runtime as physical


@contextmanager
def _real_second_probe_conflict(monkeypatch: pytest.MonkeyPatch, *, retain: bool):
    original_socket = socket.socket
    observed = {
        "probe_binds": 0,
        "attempt_probe_binds": 0,
        "native_error": None,
        "blocker": None,
        "retained_probe": None,
    }

    class ProbeSocket(original_socket):
        def bind(self, address):
            if address[1] == 0:
                observed["attempt_probe_binds"] = 0
                return super().bind(address)
            observed["probe_binds"] += 1
            observed["attempt_probe_binds"] += 1
            if observed["native_error"] is not None or observed["attempt_probe_binds"] != 2:
                return super().bind(address)
            # The first real probe has bound successfully. Occupy the second
            # candidate only now; the allocator still executes its real bind.
            blocker = original_socket()
            try:
                if os.name == "nt":
                    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                blocker.bind(address)
                blocker.listen(1)
            except BaseException:
                # Acquisition may race another owner. Close this attempt and
                # let the allocator's unchanged retry choose a fresh triple.
                blocker.close()
                raise
            observed["blocker"] = blocker
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


def test_failed_real_probe_emits_no_resource_warning(monkeypatch, recwarn):
    with _real_second_probe_conflict(monkeypatch, retain=False) as observed:
        used = set()
        port = physical._free_port_triple(used)
        evidence = _native_conflict_evidence(observed)
        assert used == {port, port + 1, port + 2}
        assert observed["native_error"]["endpoint"][1] not in used
        assert observed["retained_probe"] is None
        records = tuple(recwarn)
        evidence.update(
            allocator_returned=True,
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
            observation_stage="AT_ALLOCATOR_RETURN_BEFORE_BLOCKER_CLOSE",
        )
        print(json.dumps(evidence, sort_keys=True), file=sys.stderr, flush=True)
        assert len(recwarn) == 0, "failed real allocation probe must not emit ResourceWarning"


def test_probe_conflict_rearms_after_real_blocker_acquisition_failure(monkeypatch, recwarn):
    original_socket = socket.socket
    acquisition = {"occupant": None, "failed_blocker": None, "native_error": None}

    class BusyBlockerSocket(original_socket):
        def bind(self, address):
            # ProbeSocket subclasses this class. Only the fixture's direct
            # blocker gets a competing owner; all binds are real native calls.
            if type(self) is not BusyBlockerSocket or acquisition["occupant"] is not None:
                return super().bind(address)
            occupant = original_socket()
            try:
                if os.name == "nt":
                    occupant.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                occupant.bind(address)
                occupant.listen(1)
            except BaseException:
                occupant.close()
                raise
            acquisition["occupant"] = occupant
            acquisition["failed_blocker"] = self
            try:
                return super().bind(address)
            except OSError as error:
                acquisition["native_error"] = {
                    "exception_type": type(error).__name__,
                    "errno": error.errno,
                    "winerror": getattr(error, "winerror", None),
                    "endpoint": list(address),
                }
                raise

    # Replace only this test module's socket factory, not socket.bind or the
    # process-wide socket module. The allocator still executes unchanged.
    with monkeypatch.context() as patch:
        patch.setattr(
            sys.modules[__name__],
            "socket",
            SimpleNamespace(
                socket=BusyBlockerSocket,
                SOL_SOCKET=socket.SOL_SOCKET,
                SO_EXCLUSIVEADDRUSE=getattr(socket, "SO_EXCLUSIVEADDRUSE", None),
            ),
        )
        try:
            with _real_second_probe_conflict(monkeypatch, retain=True) as observed:
                used = set()
                port = physical._free_port_triple(used)
                error = acquisition["native_error"]
                assert error is not None, "the fixture blocker must reach real native bind denial"
                if os.name == "nt":
                    assert error["winerror"] in {10013, 10048}
                else:
                    assert os.name == "posix" and error["errno"] == errno.EADDRINUSE
                occupant = acquisition["occupant"]
                failed = acquisition["failed_blocker"]
                assert occupant is not None and occupant.fileno() >= 0
                assert list(occupant.getsockname()) == error["endpoint"]
                assert failed is not None
                assert used == {port, port + 1, port + 2}
                assert error["endpoint"][1] not in used
                evidence = {
                    "diagnostic": "allocator-blocker-acquisition",
                    "stage": "after-real-busy-blocker-allocation-retry",
                    "blocker_error": error,
                    "occupant_still_held": True,
                    "allocator_returned": True,
                    "failed_blocker_explicitly_closed": failed.fileno() == -1,
                    "intended_probe_error_recorded": observed["native_error"] is not None,
                    "probe_binds": observed["probe_binds"],
                }
                print(json.dumps(evidence, sort_keys=True), file=sys.stderr, flush=True)
                assert observed["native_error"] is not None, (
                    "blocker acquisition failure must not consume the required probe conflict"
                )
                assert failed.fileno() == -1, "failed fixture blocker must close before retry"
                _native_conflict_evidence(observed)
                assert observed["retained_probe"].fileno() == -1
                assert observed["native_error"]["endpoint"][1] not in used
                assert len(recwarn) == 0
        finally:
            # Retain both references so RED cannot rely on garbage collection.
            failed = acquisition["failed_blocker"]
            if failed is not None:
                failed.close()
            occupant = acquisition["occupant"]
            if occupant is not None:
                occupant.close()

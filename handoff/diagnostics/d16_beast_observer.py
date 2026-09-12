"""External diagnostic observer; original source, calls and exceptions retained."""
import functools
import json
import os
import pathlib
import threading
import time

import pytest

EVENTS = []
LOCK = threading.Lock()
CLUSTER = None


def snapshot(runtime):
    node = runtime.node
    result = {"voter_id": node.voter_id, "role": node.role,
              "leader_id": node.leader_id, "quorum": node.quorum}
    # Only test-owned stores; no live probes or state mutation. Sequential reads
    # are not an atomic consensus snapshot and may perturb diagnostic timing.
    for name, read in {
        "ready": lambda: runtime.ready,
        "term": lambda: node.store.current_term,
        "commit_index": lambda: node.store.commit_index,
        "last_applied": lambda: node.store.last_applied,
        "lifecycle_error": lambda: runtime.lifecycle_error,
        "quorum_failures": lambda: runtime._quorum_failures,
        "last_leader_contact_age": lambda: time.monotonic() - node.last_leader_contact,
        "election_due_in": lambda: runtime._next_election_at - time.monotonic(),
        "heartbeat_seconds": lambda: runtime.heartbeat_seconds,
        "election_timeout_seconds": lambda: runtime.election_timeout_seconds,
        "lifecycle_alive": lambda: bool(runtime._lifecycle_thread and runtime._lifecycle_thread.is_alive()),
    }.items():
        try:
            result[name] = read()
        except Exception as error:
            result[name] = {"observation_error": type(error).__name__}
    return result


def record(stage, **details):
    event = {"stage": stage, "monotonic": time.monotonic(), **details}
    with LOCK:
        EVENTS.append(event)


@pytest.fixture(autouse=True)
def observe_d16(request, monkeypatch):
    global CLUSTER
    if request.node.name != "test_reachable_isolated_leader_cannot_report_current_session_authority":
        return
    from catalog.federation.federation_v1_release_runtime import FederationV1ReleaseRuntime

    original_populate = request.module._populate
    original_require = FederationV1ReleaseRuntime.require_quorum_leader

    @functools.wraps(original_populate)
    async def populate(cluster, root):
        global CLUSTER
        CLUSTER = cluster
        record("populate_entry", voters=[snapshot(r) for r in cluster.runtimes])
        try:
            result = await original_populate(cluster, root)
        except Exception as error:
            record("populate_exception", exception=type(error).__name__, text=str(error),
                   voters=[snapshot(r) for r in cluster.runtimes])
            raise
        record("populate_complete", voters=[snapshot(r) for r in cluster.runtimes])
        return result

    @functools.wraps(original_require)
    def require(runtime):
        started = time.monotonic()
        try:
            return original_require(runtime)
        except Exception as error:
            record("quorum_guard_exception", elapsed=time.monotonic() - started,
                   exception=type(error).__name__, code=getattr(error, "code", None),
                   text=str(error), runtime=snapshot(runtime),
                   running_voters=sorted(CLUSTER.running_runtimes) if CLUSTER else None)
            raise

    monkeypatch.setattr(request.module, "_populate", populate)
    monkeypatch.setattr(FederationV1ReleaseRuntime, "require_quorum_leader", require)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    record("pytest_" + report.when, nodeid=report.nodeid, outcome=report.outcome,
           duration=report.duration)


def pytest_sessionfinish(session, exitstatus):
    dest = pathlib.Path(os.environ["D16_OBSERVER_OUTPUT"])
    dest.write_text(json.dumps({"diagnostic_only": True, "exitstatus": int(exitstatus),
                               "events": EVENTS}, indent=2) + "\n", encoding="utf-8")

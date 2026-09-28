"""Managed health is evidence of actual workers, never a new supervisor."""
from __future__ import annotations

import uuid
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.mtconnect_recorder import managed_service as managed
from catalog.mtconnect_recorder import runtime as capture
from catalog.mtconnect_recorder.worker_health import WorkerHealth

CANDIDATE = "a" * 40
PROCESS = {"candidate_sha": CANDIDATE, "runtime_generation": "b" * 32,
           "supervisor_generation": None, "pid": 123}


class ObservedThread:
    def __init__(self, *, alive=True, **kwargs):
        self.alive = alive
        self.joined = []
        self.target = kwargs.get("target")
        self.args = kwargs.get("args", ())

    def is_alive(self):
        return self.alive

    def start(self):
        self.alive = True

    def join(self, *, timeout):
        self.joined.append(timeout)


@pytest.fixture
def companion(tmp_path, monkeypatch):
    monkeypatch.setattr(managed, "process_provenance", lambda: dict(PROCESS))
    value = managed.ManagedRecorderFederationRuntime(data_directory=tmp_path)
    return value


def running_companion(value):
    thread = ObservedThread()
    value._thread = thread
    value._health_thread = thread
    value._health_generation = uuid.uuid4().hex
    return thread


def connected_workers(value):
    control_thread = ObservedThread()
    health = WorkerHealth("federation-control")
    health.started()
    control = SimpleNamespace(_thread=control_thread, health=health)
    future = Future()
    node = SimpleNamespace(
        _publication_future=future,
        runtime=SimpleNamespace(
            _thread=ObservedThread(), _loop=SimpleNamespace(is_running=lambda: True),
        ),
    )
    value._observe_connected(value._health_generation, node, control,
                             SimpleNamespace(node_id="node-recorder", session_id="session-one"))
    return control, future


def test_unpaired_runtime_does_not_invent_context_or_healthy_workers(companion):
    snapshot = companion.health_snapshot()
    assert set(snapshot) == {"managed_companion", "federation_control", "recorder_publication"}
    for row in snapshot.values():
        assert row["node_id"] is None
        assert row["session_id"] is None
        assert row["worker_generation"] is None
        assert row["alive"] is False
        assert row["healthy"] is False
        assert row["state"] == "starting"
        assert row["candidate_sha"] == CANDIDATE


def test_expected_retry_is_alive_but_not_a_healthy_completed_cycle(companion):
    running_companion(companion)
    connected_workers(companion)
    generation = companion._health_generation
    companion._observe_cycle(generation, "retrying", error=OSError("secret URL must not appear"))
    row = companion.health_snapshot()["managed_companion"]
    assert row["state"] == "recovering"
    assert row["alive"] is True
    assert row["healthy"] is False
    assert row["consecutive_failures"] == 1
    assert row["last_error_code"] == "OSError"
    assert row["session_id"] == "session-one"
    assert "secret" not in str(row)
    companion._observe_cycle(generation, "completed")
    recovered = companion.health_snapshot()["managed_companion"]
    assert recovered["healthy"] is True
    assert recovered["consecutive_failures"] == 0
    companion._observe_cycle(generation, "no-context")
    assert companion.health_snapshot()["managed_companion"]["healthy"] is False


def test_control_thread_and_publication_future_death_are_observed(companion):
    running_companion(companion)
    control, future = connected_workers(companion)
    before = companion.health_snapshot()
    assert before["federation_control"]["alive"] is True
    assert before["recorder_publication"]["alive"] is True
    control._thread.alive = False
    future.set_exception(RuntimeError("publication failed"))
    after = companion.health_snapshot()
    for name in ("federation_control", "recorder_publication"):
        assert after[name]["started"] is True
        assert after[name]["alive"] is False
        assert after[name]["state"] == "failed"
        assert after[name]["healthy"] is False
    assert after["federation_control"]["worker_generation"] == before["federation_control"]["worker_generation"]


@pytest.mark.parametrize("failed_owner", ["thread", "loop"])
def test_pending_publication_future_does_not_hide_a_dead_event_loop(companion, failed_owner):
    running_companion(companion)
    _, future = connected_workers(companion)
    if failed_owner == "thread":
        companion._health_publication_thread.alive = False
    else:
        companion._health_publication_loop.is_running = lambda: False
    assert not future.done()
    row = companion.health_snapshot()["recorder_publication"]
    assert row["future_pending"] is True
    assert row["alive"] is False
    assert row["state"] == "failed"
    assert row["healthy"] is False


def test_managed_federation_context_is_actual_in_memory_and_generation_bound(companion):
    assert companion.federation_snapshot() == {"status": "not-started"}
    snapshot = SimpleNamespace(status="connected", node_id="node-recorder",
        session_id="session-one", federation_id="federation-one")
    companion._node = SimpleNamespace(snapshot=lambda: snapshot)
    observed = companion.federation_snapshot()
    assert observed["session_id"] == "session-one"
    assert observed["node_id"] == "node-recorder"
    companion._health_generation_overlap = True
    assert companion.federation_snapshot() == {"status": "unavailable"}


def test_stop_timeout_retains_actual_survivor_reference(companion):
    thread = running_companion(companion)
    companion.stop(timeout=0.001)
    assert companion._thread is None  # Existing lifecycle stays unchanged.
    assert thread.joined == [0.001]
    row = companion.health_snapshot()["managed_companion"]
    assert row["stop_requested"] is True
    assert row["alive"] is True
    assert row["state"] == "stopping"
    assert row["healthy"] is False
    thread.alive = False
    assert companion.health_snapshot()["managed_companion"]["state"] == "stopped"


def test_old_generation_cannot_mark_replacement_healthy(companion, monkeypatch):
    old = running_companion(companion)
    old_generation = companion._health_generation
    companion.stop(timeout=0)
    monkeypatch.setattr(managed.threading, "Thread", ObservedThread)
    companion.start()
    assert companion._health_generation != old_generation
    companion._observe_cycle(old_generation, "completed")
    row = companion.health_snapshot()["managed_companion"]
    assert old.is_alive()
    assert row["generation_overlap"] is True
    assert row["generation_current"] is False
    assert row["healthy"] is False
    assert row["last_cycle_outcome"] == "starting"
    assert row["overlapped_workers"] == [
        {"worker": "managed_companion", "kind": "thread", "alive": True},
    ]
    old.alive = False
    assert companion.health_snapshot()["managed_companion"]["overlapped_workers"][0]["alive"] is False


def test_thread_observation_keeps_the_generation_bound_at_start(companion, monkeypatch):
    monkeypatch.setattr(managed.threading, "Thread", ObservedThread)
    companion.start()
    thread = companion._thread
    original_generation = companion._health_generation
    companion._health_generation = "replacement-generation"
    calls = []
    monkeypatch.setattr(companion, "_run_companion", lambda generation: calls.append(generation))
    thread.target(*thread.args)
    assert calls == [original_generation]
    assert companion._health_outcome == "starting"


def test_cancelled_publication_cannot_hide_its_still_draining_runtime(companion, monkeypatch):
    old = running_companion(companion)
    control, future = connected_workers(companion)
    owner = companion._health_publication_thread
    old.alive = False
    control._thread.alive = False
    future.cancel()
    companion.stop(timeout=0)
    monkeypatch.setattr(managed.threading, "Thread", ObservedThread)
    companion.start()
    row = companion.health_snapshot()["managed_companion"]
    assert row["generation_overlap"] is True
    assert row["generation_current"] is False
    assert row["healthy"] is False
    observed = {item["worker"]: item for item in row["overlapped_workers"]}
    assert observed["recorder_publication"]["pending"] is False
    assert observed["publication_event_loop"]["alive"] is True
    owner.alive = False
    observed = {
        item["worker"]: item
        for item in companion.health_snapshot()["managed_companion"]["overlapped_workers"]
    }
    assert observed["publication_event_loop"]["alive"] is False


def test_wrong_runtime_generation_is_refused_even_when_thread_is_alive(companion):
    running_companion(companion)
    companion._observe_cycle(companion._health_generation, "completed")
    row = companion.health_snapshot(expected_runtime_generation="c" * 32)["managed_companion"]
    assert row["alive"] is True
    assert row["generation_current"] is False
    assert row["state"] == "stale-generation"
    assert row["healthy"] is False


def test_observer_provenance_or_uuid_failure_does_not_prevent_existing_start_stop(tmp_path, monkeypatch):
    def unavailable():
        raise RuntimeError("observer unavailable")

    monkeypatch.setattr(managed, "process_provenance", unavailable)
    monkeypatch.setattr(managed.uuid, "uuid4", unavailable)
    monkeypatch.setattr(managed.threading, "Thread", ObservedThread)
    value = managed.ManagedRecorderFederationRuntime(data_directory=tmp_path)
    value.start()
    assert value._thread.is_alive()
    row = value.health_snapshot()["managed_companion"]
    assert row["generation_current"] is False
    assert row["healthy"] is False
    value.stop(timeout=0.25)
    assert value._health_thread.joined == [0.25]


def test_companion_exception_is_not_swallowed_or_retried_by_observer(companion, monkeypatch):
    thread = running_companion(companion)
    failure = RuntimeError("original operation failed")
    calls = []

    def fail(generation):
        calls.append(generation)
        raise failure

    monkeypatch.setattr(companion, "_run_companion", fail)
    with pytest.raises(RuntimeError) as observed:
        companion._run(companion._health_generation)
    assert observed.value is failure
    assert calls == [companion._health_generation]
    thread.alive = False
    assert companion.health_snapshot()["managed_companion"]["state"] == "failed"


def test_managed_entrypoint_registers_actual_health_before_capture_and_clears_it(tmp_path: Path, monkeypatch):
    events = []

    class Companion:
        def __init__(self, **_kwargs):
            pass

        def health_snapshot(self):
            return {"managed_companion": {"alive": True, "state": "alive"}}

        def federation_snapshot(self):
            return {"status": "connected", "node_id": "node-recorder", "session_id": "session-one"}

        def start(self):
            events.append("start")

        def stop(self):
            events.append("stop")

    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(managed, "ensure_recorder_upgrade_config", lambda **_kwargs: None)
    monkeypatch.setattr(capture, "_WORKER_HEALTH_PROVIDER", None)
    monkeypatch.setattr(capture, "_FEDERATION_STATUS_PROVIDER", None)

    def run_capture():
        events.append("capture")
        assert capture._worker_health()["managed_companion"]["alive"] is True
        assert capture._federation_status()["session_id"] == "session-one"
        return "unchanged-result"

    assert managed.run_managed_recorder(capture_runner=run_capture, companion_factory=Companion) == "unchanged-result"
    assert events == ["start", "capture", "stop"]
    assert capture._worker_health() == {}
    assert capture._federation_status() == {"status": "not-started"}

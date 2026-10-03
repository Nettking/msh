"""The recorder may start before its MTConnect source exists.

Physical acceptance starts the recorder first and powers up the machine agent
afterwards. The recorder must therefore keep retrying a source that is not
answering yet, must not busy-poll it, and must start recording on its own as
soon as the agent appears — with no manual restart.
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from concurrent.futures import Future
from pathlib import Path
from time import perf_counter, sleep

import pytest

from catalog.mtconnect_recorder import runtime as rt
from catalog.mtconnect_recorder.storage import DurableRecorderStore

from .conftest import Observation, stamp, streams_document

SOURCE = "mazak-cell"
BASE_URL = "http://machine-agent.invalid:5000"
INSTANCE_ID = 1_755_000_001

PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-08-07T09:00:33Z" sender="agent-alpha"
          instanceId="1755000001" version="1.3.0" assetBufferSize="1024"
          assetCount="0" bufferSize="131072"/>
  <Devices>
    <Device id="d1" name="MachineAlpha" uuid="MACHINE-ALPHA-0001">
      <Description manufacturer="Example"/>
      <DataItems>
        <DataItem category="EVENT" id="execution" type="EXECUTION"/>
      </DataItems>
    </Device>
  </Devices>
</MTConnectDevices>
"""


def _streams(*, first: int, last: int) -> str:
    return streams_document(
        instance_id=INSTANCE_ID,
        observations=[
            Observation(
                sequence=sequence,
                component="Controller",
                element="Execution",
                data_item_id="execution",
                value="ACTIVE",
                category="Events",
                timestamp=stamp(sequence),
            )
            for sequence in range(first, last + 1)
        ],
        first_sequence=first,
        last_sequence=last,
    )


class _Agent:
    """An MTConnect agent that is unreachable until it is powered on."""

    def __init__(self) -> None:
        self.online = False
        self.current_fetches = 0

    def client(self, base_url: str, *, timeout: float):
        del timeout
        agent = self

        class _Client:
            def __init__(self) -> None:
                self.base_url = base_url

            def fetch_current(self) -> str:
                agent.current_fetches += 1
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return _streams(first=1, last=3)

            def fetch_probe(self) -> str:
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return PROBE_XML

            def fetch_sample(self, *, from_sequence: int, count: int) -> str:
                del count
                if not agent.online:
                    raise ConnectionError("agent is not answering yet")
                return _streams(first=from_sequence, last=3)

        return _Client()


def _complete_scheduled_cycle(service: rt.RecorderRuntime, *, timeout: float = 2.0) -> None:
    """Schedule one non-blocking cycle, then harvest only that scheduled work."""

    service.run_fetch_cycle()
    deadline = perf_counter() + timeout
    while True:
        service._harvest_capture_results()
        with service.lock:
            if not service._capture_futures:
                return
        if perf_counter() >= deadline:
            pytest.fail("recorder capture did not finish within the test deadline")
        sleep(0.005)


@pytest.fixture
def recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    agent = _Agent()
    monkeypatch.setattr(rt, "MtconnectClient", agent.client)

    clock = {"now": 1_000.0}
    monkeypatch.setattr(rt.time, "monotonic", lambda: clock["now"])

    service = rt.RecorderRuntime()
    service.store = DurableRecorderStore(tmp_path / "data")
    service.enabled = True
    service.configuration_ready = True
    service.sources = {SOURCE: BASE_URL}
    try:
        yield service, agent, clock
    finally:
        service.stop_event.set()
        service.executor.shutdown(wait=True, cancel_futures=False)
        service._harvest_capture_results()
        rt.unregister_stop_target(service)


def test_recorder_started_before_its_source_retries_with_bounded_backoff(
    recorder,
) -> None:
    service, agent, clock = recorder

    _complete_scheduled_cycle(service)

    assert agent.current_fetches == 1
    assert service.state == "error"
    status = service.source_status[SOURCE]
    assert "ConnectionError" in status["last_error"]
    assert status["next_retry_seconds"] >= rt.BACKOFF_INITIAL
    assert service.observations_written == 0

    # The source is not due again yet, so the recorder must not busy-poll it.
    _complete_scheduled_cycle(service)
    assert agent.current_fetches == 1

    # Backoff grows while the agent stays offline, and stays bounded.
    for _ in range(12):
        clock["now"] += rt.BACKOFF_MAX
        _complete_scheduled_cycle(service)
    assert service.source_status[SOURCE]["next_retry_seconds"] <= rt.BACKOFF_MAX


def test_recorder_starts_recording_when_the_source_appears(recorder) -> None:
    service, agent, clock = recorder

    _complete_scheduled_cycle(service)
    assert service.state == "error"
    offline_attempts = agent.current_fetches

    # The machine is powered on. No operator restarts the recorder.
    agent.online = True
    clock["now"] += rt.BACKOFF_MAX
    _complete_scheduled_cycle(service)

    assert agent.current_fetches > offline_attempts
    assert service.state == "recording"
    assert service.last_error == ""
    assert service.observations_written > 0
    assert service.raw_batches_written > 0
    assert service.last_commit_at

    status = service.source_status[SOURCE]
    assert status["last_error"] == ""
    assert status["last_success_at"]
    assert status["agent_instance_id"] == INSTANCE_ID
    # A recovered source is polled at the normal cadence again.
    assert service.backoff[SOURCE] == rt.BACKOFF_INITIAL
    assert service.next_attempt_at[SOURCE] == 0.0


def test_first_real_data_is_durably_written_with_its_raw_manifest(recorder) -> None:
    service, agent, clock = recorder

    agent.online = True
    clock["now"] += rt.BACKOFF_MAX
    _complete_scheduled_cycle(service)

    archived = service.store.iter_raw_batches(
        source_name=SOURCE,
        instance_id=INSTANCE_ID,
    )
    assert archived
    assert service.checkpoints[SOURCE].agent_instance_id == INSTANCE_ID
    assert service.checkpoints[SOURCE].next_sequence > 1


def test_slow_source_does_not_block_healthy_polling_or_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    service.enabled = True
    service.configuration_ready = True
    service.sources = {
        "slow": "http://slow.invalid:5000",
        "healthy": "http://healthy.invalid:5000",
    }
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")

    calls: Counter[str] = Counter()
    slow_started = threading.Event()
    release_slow = threading.Event()
    slow_finished = threading.Event()
    calls_lock = threading.Lock()

    def capture(source_name: str, base_url: str) -> tuple[str, bool, str]:
        del base_url
        with calls_lock:
            calls[source_name] += 1
        if source_name == "slow":
            slow_started.set()
            assert release_slow.wait(2.0)
            slow_finished.set()
        return source_name, True, ""

    monkeypatch.setattr(service, "capture_source", capture)
    try:
        service.run_fetch_cycle()
        assert slow_started.wait(1.0)

        deadline = perf_counter() + 1.0
        while True:
            service.run_fetch_cycle()
            with calls_lock:
                healthy_calls = calls["healthy"]
                slow_calls = calls["slow"]
            if healthy_calls >= 2:
                break
            if perf_counter() >= deadline:
                pytest.fail("healthy source did not make progress while slow source was blocked")
            sleep(0.005)

        service.publish_status(force=True)
        status = json.loads(rt.STATUS_FILE.read_text(encoding="utf-8"))

        assert slow_calls == 1
        assert healthy_calls >= 2
        assert not slow_finished.is_set()
        assert status["heartbeat_at"]
        assert status["state"] == "recording"
        assert status["sources"] == ["healthy", "slow"]
    finally:
        release_slow.set()
        service.stop_event.set()
        service.executor.shutdown(wait=True, cancel_futures=False)
        service._harvest_capture_results()
        rt.unregister_stop_target(service)


def test_run_once_drains_inflight_capture_before_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    service.enabled = True
    service.configuration_ready = True
    service.sources = {"slow": "http://slow.invalid:5000"}

    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def capture(source_name: str, base_url: str) -> tuple[str, bool, str]:
        del base_url
        started.set()
        assert release.wait(2.0)
        finished.set()
        return source_name, True, ""

    monkeypatch.setattr(service, "capture_source", capture)
    monkeypatch.setattr(service, "load_state", lambda: None)
    monkeypatch.setattr(service, "refresh_configuration", lambda **_kwargs: None)
    monkeypatch.setattr(service, "publish_status", lambda **_kwargs: None)
    monkeypatch.setattr(rt, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(rt, "RUN_ONCE", True)

    runner = threading.Thread(target=service.run, daemon=True)
    runner.start()
    assert started.wait(1.0)
    sleep(0.02)
    assert runner.is_alive()
    assert not finished.is_set()

    release.set()
    runner.join(timeout=2.0)
    assert not runner.is_alive()
    assert finished.is_set()


def test_control_pause_ack_waits_for_capture_store_and_checkpoint_future(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    operation_id = "pause-operation-001"
    pending: Future[tuple[str, bool, str]] = Future()
    service.enabled = False
    service.configuration_ready = True
    service.control_operation_id = operation_id
    service.sources = {SOURCE: BASE_URL}
    service._capture_futures[SOURCE] = (BASE_URL, pending)
    service.last_commit_at = None
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")

    try:
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

        assert service.state == "draining"
        assert service.pause_acknowledged_operation_id is None
        assert status["capture_control"]["operation_id"] == operation_id
        assert status["capture_control"]["capture_scheduling"] is False
        assert status["capture_control"]["inflight_capture_tasks"] == 1
        assert status["capture_control"]["durable_boundary"] is False
        assert status["records_buffered"] is None
        assert status["last_flush_at"] is None
        assert status["last_commit_at"] is None

        pending.set_result((SOURCE, True, ""))
        service.run_fetch_cycle()
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

        assert service.state == "stopped"
        assert status["capture_control"]["inflight_capture_tasks"] == 0
        assert status["capture_control"]["acknowledged_operation_id"] == operation_id
        assert status["capture_control"]["acknowledged_at"]
        assert status["capture_control"]["durable_boundary"] is True
        assert status["records_buffered"] == 0
        assert status["last_flush_at"] is None
        assert status["last_commit_at"] is None
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_pause_does_not_claim_durable_boundary_after_unhandled_capture_future_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    operation_id = "pause-operation-002"
    pending: Future[tuple[str, bool, str]] = Future()
    service.enabled = False
    service.configuration_ready = True
    service.control_operation_id = operation_id
    service.sources = {SOURCE: BASE_URL}
    service._capture_futures[SOURCE] = (BASE_URL, pending)
    service.last_commit_at = None
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")

    try:
        service.acknowledge_capture_pause()
        pending.set_exception(RuntimeError("unexpected capture worker failure"))
        service.run_fetch_cycle()
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

        assert service.state == "draining"
        assert service.pause_acknowledged_operation_id is None
        assert status["source_status"][SOURCE]["last_error"].startswith("RuntimeError:")
        assert status["capture_control"]["drain_error_operation_id"] == operation_id
        assert status["capture_control"]["inflight_capture_tasks"] == 0
        assert status["capture_control"]["durable_boundary"] is False
        assert status["last_flush_at"] is None
        assert status["last_commit_at"] is None
        persisted = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert persisted["capture_drain_failures"][0]["state"] == "unresolved"
        assert persisted["capture_drain_failures"][0]["pause_operation_id"] == operation_id

        # A later Stop operation cannot erase an unresolved write-boundary
        # failure, and a Recorder process restart must retain that refusal.
        service.control_operation_id = "pause-operation-003"
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
        assert service.pause_acknowledged_operation_id is None
        assert status["capture_control"]["drain_error_operation_id"] == operation_id
        assert status["capture_control"]["durable_boundary"] is False

        restored = rt.RecorderRuntime()
        try:
            restored.enabled = False
            restored.control_operation_id = "pause-operation-004"
            restored.load_state()
            restored.acknowledge_capture_pause()
            assert restored.pause_acknowledged_operation_id is None
            assert restored.capture_drain_error_operation_id == operation_id

            original_save_state = restored.save_state

            def fail_state_write() -> None:
                raise OSError("state write refused")

            monkeypatch.setattr(restored, "save_state", fail_state_write)
            with pytest.raises(OSError, match="state write refused"):
                restored._clear_capture_drain_failures_after_recovery(SOURCE)
            assert restored.capture_drain_error_operation_id == operation_id
            assert restored.capture_drain_failures[0]["state"] == "unresolved"

            class ResourcePauseSignal(BaseException):
                pass

            def refuse_inside_admitted_capture() -> None:
                raise ResourcePauseSignal()

            monkeypatch.setattr(restored, "save_state", refuse_inside_admitted_capture)
            with pytest.raises(ResourcePauseSignal):
                restored._clear_capture_drain_failures_after_recovery(SOURCE)
            assert restored.capture_drain_error_operation_id == operation_id
            assert restored.capture_drain_failures[0]["state"] == "unresolved"

            monkeypatch.setattr(restored, "save_state", original_save_state)
            # Only the successful recovery path calls this completion method.
            restored._clear_capture_drain_failures_after_recovery(SOURCE)
            assert restored.capture_drain_error_operation_id is None
            assert restored.capture_drain_failures[0]["state"] == "recovered"
            persisted = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
            assert persisted["capture_drain_failures"][0]["state"] == "recovered"
        finally:
            restored.executor.shutdown(wait=True, cancel_futures=False)
            rt.unregister_stop_target(restored)
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_checkpoint_alias_reconciliation_moves_pause_incident_with_its_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    service = rt.RecorderRuntime()
    new_name = "MACHINE-ALPHA-0001"
    service.checkpoints[SOURCE] = rt.SourceCheckpoint(
        source_name=SOURCE,
        base_url=BASE_URL,
        machine_id="MachineAlpha",
        agent_instance_id=INSTANCE_ID,
        next_sequence=4,
        probe_sha256="a" * 64,
    )
    service.capture_drain_failures = [
        {
            "incident_id": "incident-1",
            "source_name": SOURCE,
            "pause_operation_id": "pause-operation-alias",
            "state": "unresolved",
        }
    ]
    service._refresh_capture_drain_error_operation_id()

    try:
        assert service.reconcile_checkpoint_aliases({new_name: BASE_URL}) is True
        assert SOURCE not in service.checkpoints
        assert service.checkpoints[new_name].storage_aliases == [SOURCE]
        incident = service.capture_drain_failures[0]
        assert incident["source_name"] == new_name
        assert incident["original_source_name"] == SOURCE
        assert service.capture_drain_error_operation_id == "pause-operation-alias"

        service.save_state()
        restored = rt.RecorderRuntime()
        try:
            restored.load_state()
            assert restored.capture_drain_failures[0]["source_name"] == new_name
            restored._clear_capture_drain_failures_after_recovery(new_name)
            assert restored.capture_drain_error_operation_id is None
        finally:
            restored.executor.shutdown(wait=True, cancel_futures=False)
            rt.unregister_stop_target(restored)
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_late_drain_failure_uses_reconciled_checkpoint_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    service = rt.RecorderRuntime()
    new_name = "MACHINE-ALPHA-0001"
    service.checkpoints[SOURCE] = rt.SourceCheckpoint(
        source_name=SOURCE,
        base_url=BASE_URL,
        machine_id="MachineAlpha",
        agent_instance_id=INSTANCE_ID,
        next_sequence=4,
        probe_sha256="a" * 64,
    )

    try:
        # This is the ordering in refresh_configuration: the alias moves before
        # an older capture future reports its escaped exception.
        assert service.reconcile_checkpoint_aliases({new_name: BASE_URL}) is True
        service.sources = {new_name: BASE_URL}
        service.control_operation_id = "pause-operation-late-alias"
        pending: Future[tuple[str, bool, str]] = Future()
        service._capture_futures[SOURCE] = (BASE_URL, pending)
        pending.set_exception(RuntimeError("late future failure"))
        service._harvest_capture_results()

        incident = service.capture_drain_failures[0]
        assert incident["source_name"] == new_name
        assert incident["original_source_name"] == SOURCE
        assert service.capture_drain_error_operation_id == "pause-operation-late-alias"
        persisted = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert persisted["capture_drain_failures"][0]["source_name"] == new_name
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_capture_result_from_another_source_cannot_satisfy_restart_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    service = rt.RecorderRuntime()
    service.sources = {SOURCE: BASE_URL, "SECOND-MACHINE": BASE_URL}
    service.control_operation_id = "pause-from-prior-runtime"
    service.restart_pause_recovery_required = True
    service.restart_pause_recovery_sources = {
        (SOURCE, rt.normalize_agent_base_url(BASE_URL)),
        ("SECOND-MACHINE", rt.normalize_agent_base_url(BASE_URL)),
    }
    pending: Future[rt.CaptureResult] = Future()
    service._capture_futures[SOURCE] = (BASE_URL, pending)

    try:
        pending.set_result(
            rt.CaptureResult("SECOND-MACHINE", True, "", transaction_complete=True)
        )
        service._harvest_capture_results()

        assert len(service.restart_pause_recovery_sources) == 2
        assert service.restart_pause_recovery_required is True
        assert service._capture_outcomes[SOURCE] is False
        assert service.capture_drain_failures[0]["source_name"] == SOURCE
        assert service.capture_drain_failures[0]["state"] == "unresolved"
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_startup_does_not_acknowledge_pause_request_from_previous_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = {"enabled": False, "operation_id": "pause-from-prior-runtime"}
    configured_sources = {SOURCE: BASE_URL, "SECOND-MACHINE": BASE_URL}
    monkeypatch.setattr(rt, "MANAGED_MODE", True)
    monkeypatch.setattr(
        rt,
        "_read_json",
        lambda path: control if path == rt.CONTROL_FILE else {"sources": []},
    )
    monkeypatch.setattr(
        rt,
        "_managed_configuration",
        lambda _config: (configured_sources, 0.2),
    )
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    # No incident reached disk before the prior process exited. Startup still
    # sees the old disabled operation and must fail closed without inventing
    # its acknowledgement.
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "empty-state.json")
    predecessor = rt.RecorderRuntime()
    predecessor.control_operation_id = control["operation_id"]

    def fail_incident_write() -> None:
        raise OSError("state volume unavailable")

    monkeypatch.setattr(predecessor, "save_state", fail_incident_write)
    predecessor._record_capture_drain_failure(
        SOURCE,
        RuntimeError("capture worker escaped before pause"),
    )
    assert predecessor.capture_drain_failures[0]["state"] == "unresolved"
    assert "could not be persisted" in predecessor.last_error
    predecessor.executor.shutdown(wait=True, cancel_futures=False)
    rt.unregister_stop_target(predecessor)

    service = rt.RecorderRuntime()

    try:
        service.load_state()
        service.refresh_configuration(force=True)
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

        assert service.capture_drain_failures == []
        assert service.pause_acknowledged_operation_id is None
        assert service.state == "draining"
        assert status["capture_control"]["pause_request_predates_runtime"] is True
        assert status["capture_control"]["restart_recovery_required"] is True
        assert status["capture_control"]["durable_boundary"] is False

        # A newer Start/Stop alone still cannot erase the lost incident. First
        # complete a successful source transaction in this runtime.
        control.update(enabled=True, operation_id="new-start-operation")
        service.refresh_configuration(force=True)
        control.update(enabled=False, operation_id="new-stop-operation")
        service.refresh_configuration(force=True)
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
        assert service.pause_acknowledged_operation_id is None
        assert status["capture_control"]["restart_recovery_pending_sources"] == 2
        assert status["capture_control"]["durable_boundary"] is False

        # The worker's successful post-restart source result clears this
        # startup-only recovery latch; a later fresh Stop can then be proven.
        control.update(enabled=True, operation_id="recovery-start-operation")
        service.refresh_configuration(force=True)
        resource_paused: Future[rt.CaptureResult] = Future()
        service._capture_futures[SOURCE] = (BASE_URL, resource_paused)
        resource_paused.set_result(
            rt.CaptureResult(SOURCE, True, "", transaction_complete=False)
        )
        service._harvest_capture_results()
        assert service.restart_pause_recovery_required is True
        assert len(service.restart_pause_recovery_sources) == 2

        first_recovery: Future[rt.CaptureResult] = Future()
        service._capture_futures[SOURCE] = (BASE_URL, first_recovery)
        first_recovery.set_result(
            rt.CaptureResult(SOURCE, True, "", transaction_complete=True)
        )
        service._harvest_capture_results()
        assert len(service.restart_pause_recovery_sources) == 1
        assert service.restart_pause_recovery_required is True
        # A config refresh while another source is recovering must not put an
        # already completed source back into the inherited recovery frontier.
        service.refresh_configuration(force=True)
        assert len(service.restart_pause_recovery_sources) == 1

        second_recovery: Future[rt.CaptureResult] = Future()
        service._capture_futures["SECOND-MACHINE"] = (BASE_URL, second_recovery)
        second_recovery.set_result(
            rt.CaptureResult("SECOND-MACHINE", True, "", transaction_complete=True)
        )
        service._harvest_capture_results()
        assert service.restart_pause_recovery_required is False
        control.update(enabled=False, operation_id="new-stop-after-recovery")
        service.refresh_configuration(force=True)
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
        assert service.pause_acknowledged_operation_id == "new-stop-after-recovery"
        assert status["capture_control"]["pause_request_predates_runtime"] is False
        assert status["capture_control"]["restart_recovery_required"] is False
        assert status["capture_control"]["durable_boundary"] is True
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_new_endpoint_cannot_satisfy_previous_runtime_recovery_frontier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = {"enabled": False, "operation_id": "pause-from-prior-runtime"}
    configured_sources = {SOURCE: BASE_URL}
    monkeypatch.setattr(rt, "MANAGED_MODE", True)
    monkeypatch.setattr(
        rt,
        "_read_json",
        lambda path: control if path == rt.CONTROL_FILE else {"sources": []},
    )
    monkeypatch.setattr(
        rt,
        "_managed_configuration",
        lambda _config: (configured_sources, 0.2),
    )
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    service = rt.RecorderRuntime()
    previous_endpoint = rt.normalize_agent_base_url(BASE_URL)
    replacement_endpoint = "http://replacement.example:5000"

    try:
        service.refresh_configuration(force=True)
        assert service.restart_pause_recovery_sources == {(SOURCE, previous_endpoint)}

        # Repointing the same logical source does not rewrite the uncertain
        # prior-runtime frontier to the new endpoint.
        control.update(enabled=True, operation_id="new-start")
        configured_sources[SOURCE] = replacement_endpoint
        service.refresh_configuration(force=True)
        assert service.restart_pause_recovery_sources == {(SOURCE, previous_endpoint)}

        pending: Future[rt.CaptureResult] = Future()
        service._capture_futures[SOURCE] = (replacement_endpoint, pending)
        pending.set_result(
            rt.CaptureResult(SOURCE, True, "", transaction_complete=True)
        )
        service._harvest_capture_results()

        assert service.restart_pause_recovery_required is True
        assert service.restart_pause_recovery_sources == {(SOURCE, previous_endpoint)}

        # The exact old-endpoint transaction may still finish after the
        # repoint. It satisfies recovery, but its result must not mark the new
        # endpoint healthy.
        old_endpoint_result: Future[rt.CaptureResult] = Future()
        service._capture_futures[SOURCE] = (BASE_URL, old_endpoint_result)
        old_endpoint_result.set_result(
            rt.CaptureResult(SOURCE, True, "", transaction_complete=True)
        )
        service._harvest_capture_results()

        assert service.restart_pause_recovery_required is False
        assert service.restart_pause_recovery_sources == set()
        assert service._capture_outcomes.get(SOURCE) is None
        assert service.source_status[SOURCE]["base_url"] == replacement_endpoint
        assert service.source_status[SOURCE]["last_success_at"] is None
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_recovery_completion_credits_checkpoint_alias_after_source_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control = {"enabled": False, "operation_id": "pause-from-prior-runtime"}
    configured_sources = {SOURCE: BASE_URL}
    new_name = "MACHINE-ALPHA-0001"
    monkeypatch.setattr(rt, "MANAGED_MODE", True)
    monkeypatch.setattr(
        rt,
        "_read_json",
        lambda path: control if path == rt.CONTROL_FILE else {"sources": []},
    )
    monkeypatch.setattr(
        rt,
        "_managed_configuration",
        lambda _config: (configured_sources, 0.2),
    )
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "state.json")
    service = rt.RecorderRuntime()
    service.checkpoints[SOURCE] = rt.SourceCheckpoint(
        source_name=SOURCE,
        base_url=BASE_URL,
        machine_id="MachineAlpha",
        agent_instance_id=INSTANCE_ID,
        next_sequence=4,
        probe_sha256="a" * 64,
    )
    pending: Future[rt.CaptureResult] = Future()

    try:
        service.refresh_configuration(force=True)
        service._capture_futures[SOURCE] = (BASE_URL, pending)

        # The future was scheduled under the old alias; configuration refresh
        # maps both the checkpoint and inherited frontier to the stable alias.
        control.update(enabled=True, operation_id="new-start")
        configured_sources.clear()
        configured_sources[new_name] = BASE_URL
        service.refresh_configuration(force=True)
        normalized_url = rt.normalize_agent_base_url(BASE_URL)
        assert service.restart_pause_recovery_sources == {(new_name, normalized_url)}

        pending.set_result(
            rt.CaptureResult(SOURCE, True, "", transaction_complete=True)
        )
        service._harvest_capture_results()

        assert service.restart_pause_recovery_required is False
        assert service.restart_pause_recovery_sources == set()
        assert new_name in service.checkpoints
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_drain_recovery_does_not_rewrite_state_when_no_incident_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    service.capture_drain_failures = [
        {
            "incident_id": "recovered-incident",
            "source_name": SOURCE,
            "pause_operation_id": "pause-old",
            "state": "recovered",
        },
        {
            "incident_id": "other-source-incident",
            "source_name": "other-machine",
            "pause_operation_id": "pause-other",
            "state": "unresolved",
        },
    ]
    service._refresh_capture_drain_error_operation_id()
    writes: list[bool] = []
    monkeypatch.setattr(service, "save_state", lambda: writes.append(True))

    try:
        service._clear_capture_drain_failures_after_recovery(SOURCE)

        assert writes == []
        assert service.capture_drain_error_operation_id == "pause-other"
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)


def test_pause_stays_unproven_when_drain_failure_has_no_operation_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = rt.RecorderRuntime()
    service.enabled = False
    service.configuration_ready = True
    service.control_operation_id = "pause-operation-current"
    service.capture_drain_failures = [
        {
            "incident_id": "incident-without-owner",
            "source_name": SOURCE,
            "pause_operation_id": None,
            "state": "unresolved",
        }
    ]
    service._refresh_capture_drain_error_operation_id()
    monkeypatch.setattr(rt, "STATUS_FILE", tmp_path / "status.json")

    try:
        service.acknowledge_capture_pause()
        service.publish_status(force=True)
        status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

        assert service.capture_drain_error_operation_id is None
        assert service.pause_acknowledged_operation_id is None
        assert status["capture_control"]["unresolved_drain_failures"] == 1
        assert status["capture_control"]["durable_boundary"] is False
    finally:
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)

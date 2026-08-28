"""B06: a recorder's required loops must be observably stuck, not silently so.

A standalone recorder is headless. Its Federation update, host update,
activation and recorder-control loops each catch every failure and retry from
durable state on the next poll, and that lifecycle is correct: none of them may
end capture. What was wrong is that the failure was then discarded. A loop that
had failed on every pass since startup was indistinguishable from a loop with
nothing to do -- so an operator whose ``/federation/recorders`` scan or source
change never applied, or whose device never joined an **Update all devices**
rollout, had nothing anywhere to read.

The status heartbeat is the one operator surface such a device has, and it
already carries liveness and Federation membership. These cases pin that a stuck
required loop is visible in that same observation, that it clears when the loop
recovers, and that making it visible did not turn a retryable failure into
something that can end capture or grow the heartbeat at poll rate.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import Any

import pytest

from catalog.mtconnect_recorder import federation_control, federation_update
from catalog.mtconnect_recorder import runtime as rt
from catalog.mtconnect_recorder.worker_health import (
    MAX_ERROR_CHARS,
    WorkerHealth,
    error_code,
)


class _CodedFailure(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def test_a_failing_loop_is_counted_rather_than_discarded() -> None:
    health = WorkerHealth("federation-control")

    assert health.snapshot() == {
        "started": False,
        "consecutive_failures": 0,
        "last_error_code": "",
    }

    health.started()
    for _ in range(3):
        health.record_failure(_CodedFailure("recorder-control-unreachable"))

    assert health.snapshot() == {
        "started": True,
        "consecutive_failures": 3,
        "last_error_code": "recorder-control-unreachable",
    }


def test_a_recovered_loop_clears_its_record_completely() -> None:
    """A condition that has been repaired must not survive in the record."""

    health = WorkerHealth("federation-update")
    health.started()
    health.record_failure(OSError("relay unreachable"))
    health.record_success()

    assert health.snapshot() == {
        "started": True,
        "consecutive_failures": 0,
        "last_error_code": "",
    }


def test_a_persistent_failure_is_announced_once_rather_than_every_poll(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Making a stuck loop visible must not grow a log at poll rate.

    These loops poll about once a second on the same device the failure is
    already about. The condition is announced when it appears or changes; the
    counter carries the persistence.
    """

    log = logging.getLogger("recorder-worker-health-test")
    health = WorkerHealth("host-update-agent", log=log)

    with caplog.at_level(logging.WARNING, logger=log.name):
        for _ in range(5):
            health.record_failure(_CodedFailure("host-update-unavailable"))
        # A different failure is a new condition and is announced again.
        health.record_failure(_CodedFailure("host-update-refused"))

    announcements = [record.getMessage() for record in caplog.records]
    assert len(announcements) == 2
    assert "consecutive failures: 1" in announcements[0]
    assert "consecutive failures: 6" in announcements[1]


def test_an_error_code_is_named_and_bounded() -> None:
    assert error_code(_CodedFailure("recorder-control-unreachable")) == (
        "recorder-control-unreachable"
    )
    assert error_code(OSError("no space left on device")) == "OSError"
    assert len(error_code(_CodedFailure("x" * 4000))) == MAX_ERROR_CHARS


def _drive_once(worker: Any) -> None:
    """Run exactly one pass of a worker's own loop, then stop it."""

    original_wait = worker._stop.wait

    def wait_then_stop(timeout: float | None = None) -> bool:
        worker._stop.set()
        return original_wait(0)

    worker._stop.wait = wait_then_stop  # type: ignore[method-assign]
    worker._run()


def test_a_failing_control_loop_records_its_failure_and_keeps_running(
    tmp_path: Path,
) -> None:
    """The consequence: an operator's control command silently never applies."""

    class _Binding:
        internal_session_id = "session-1"

    class _Remote:
        binding = _Binding()

    class _Store:
        def load(self) -> _Remote:
            return _Remote()

    class _Runtime:
        def __init__(self) -> None:
            self.calls = 0

        def session_events(self, *_args: object, **_kwargs: object) -> None:
            # The relay is gone: reading control commands is exactly the
            # bounded transport failure this loop retries rather than dies on.
            self.calls += 1
            raise _CodedFailure("pairing-relay-disconnected")

    class _Node:
        remote_store = _Store()

        def __init__(self) -> None:
            self.runtime = _Runtime()

    node = _Node()
    worker = federation_control.RecorderFederationControlWorker(
        node,
        data_directory=tmp_path,
        poll_seconds=0.001,
    )
    worker.health.started()

    _drive_once(worker)

    assert node.runtime.calls >= 1
    snapshot = worker.health.snapshot()
    assert snapshot["started"] is True
    assert snapshot["consecutive_failures"] >= 1
    assert snapshot["last_error_code"] == "pairing-relay-disconnected"


def test_a_failing_update_loop_records_its_failure_and_keeps_running(
    tmp_path: Path,
) -> None:
    worker = federation_update.RecorderFederationUpdateWorker.__new__(
        federation_update.RecorderFederationUpdateWorker
    )
    worker._stop = threading.Event()
    worker._thread = None
    worker.poll_seconds = 0.001
    worker.health = WorkerHealth("federation-update")

    def _failing_pass() -> bool:
        raise _CodedFailure("recorder-update-unreachable")

    worker.process_once = _failing_pass  # type: ignore[method-assign]
    worker.health.started()
    _drive_once(worker)

    snapshot = worker.health.snapshot()
    assert snapshot["consecutive_failures"] >= 1
    assert snapshot["last_error_code"] == "recorder-update-unreachable"


def test_the_heartbeat_publishes_required_loop_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stuck loop has to be readable from the file that proves liveness."""

    status_file = tmp_path / "source_state" / "mtconnect_recorder_status.json"
    monkeypatch.setattr(rt, "STATUS_FILE", status_file)
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "source_state" / "state.json")
    monkeypatch.setattr(rt, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(rt, "MANAGED_MODE", False)

    health = WorkerHealth("federation-control")
    health.started()
    health.record_failure(_CodedFailure("pairing-relay-disconnected"))
    monkeypatch.setattr(
        rt,
        "_WORKER_HEALTH_PROVIDER",
        lambda: {"federation_control": health.snapshot()},
        raising=False,
    )

    service = rt.RecorderRuntime()
    try:
        service.publish_status(force=True)
    finally:
        service.stop_event.set()
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)

    published = json.loads(status_file.read_text(encoding="utf-8"))
    assert published["workers"] == {
        "federation_control": {
            "started": True,
            "consecutive_failures": 1,
            "last_error_code": "pairing-relay-disconnected",
        }
    }


def test_a_broken_health_provider_cannot_break_the_heartbeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reporting is reporting. It must never decide the heartbeat's fate.

    The published map is also bounded, so a provider cannot grow the file an
    operator reads under exactly the conditions that produced the failure.
    """

    status_file = tmp_path / "source_state" / "mtconnect_recorder_status.json"
    monkeypatch.setattr(rt, "STATUS_FILE", status_file)
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "source_state" / "state.json")
    monkeypatch.setattr(rt, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(rt, "MANAGED_MODE", False)

    def _broken() -> dict[str, Any]:
        raise RuntimeError("health provider is broken")

    monkeypatch.setattr(rt, "_WORKER_HEALTH_PROVIDER", _broken, raising=False)

    service = rt.RecorderRuntime()
    try:
        service.publish_status(force=True)
        published = json.loads(status_file.read_text(encoding="utf-8"))
        assert published["workers"] == {"status": "unavailable"}

        monkeypatch.setattr(
            rt,
            "_WORKER_HEALTH_PROVIDER",
            lambda: {
                f"worker-{index}": {"started": True, "consecutive_failures": 0}
                for index in range(rt.MAX_PUBLISHED_WORKERS + 25)
            },
            raising=False,
        )
        service.publish_status(force=True)
        published = json.loads(status_file.read_text(encoding="utf-8"))
        assert len(published["workers"]) == rt.MAX_PUBLISHED_WORKERS
    finally:
        service.stop_event.set()
        service.executor.shutdown(wait=True, cancel_futures=False)
        rt.unregister_stop_target(service)

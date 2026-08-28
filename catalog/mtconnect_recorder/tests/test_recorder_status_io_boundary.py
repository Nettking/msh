"""B06: a failing host filesystem must not decide the recorder's lifecycle.

The recorder publishes an operational heartbeat on every cycle and once more
while shutting down. That write used to escape the run loop: an ``OSError`` from
a full, read-only or otherwise failing filesystem ended capture, and the same
error raised again inside ``run``'s ``finally`` replaced the real stop reason and
skipped stop-target cleanup.

The supervised native recorder makes the consequence concrete. Its supervisor
decides restart from the child's exit code, and ``Test-IntentionalStop`` treats a
graceful zero as the operator's own Ctrl+C -- the one exit it never restarts. A
heartbeat write that fails during shutdown turned that operator stop into a
nonzero exit, so the supervisor restarted capture behind the operator.

Capture already contains its own I/O failures inside the per-source boundary.
These cases pin the same boundary for the heartbeat, and pin that containment
never invents a heartbeat that was not written.
"""

from __future__ import annotations

import errno
import json
import logging
import signal
from pathlib import Path

import pytest

from catalog.mtconnect_recorder import runtime as rt
from catalog.mtconnect_recorder.storage import DurableRecorderStore

from .conftest import Observation, stamp, streams_document

SOURCE = "mazak-cell"
BASE_URL = "http://machine-agent.invalid:5000"
INSTANCE_ID = 1_755_000_101

PROBE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.3">
  <Header creationTime="2026-08-07T09:00:33Z" sender="agent-alpha"
          instanceId="1755000101" version="1.3.0" assetBufferSize="1024"
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


def _online_client(base_url: str, *, timeout: float):
    del timeout

    class _Client:
        def __init__(self) -> None:
            self.base_url = base_url

        def fetch_current(self) -> str:
            return _streams(first=1, last=3)

        def fetch_probe(self) -> str:
            return PROBE_XML

        def fetch_sample(self, *, from_sequence: int, count: int) -> str:
            del count
            return _streams(first=from_sequence, last=3)

    return _Client()


class _FailingFilesystem:
    """Fail exactly the recorder heartbeat write, on demand."""

    def __init__(self, real, status_file: Path) -> None:
        self._real = real
        self._status_file = status_file
        self.failing = False
        self.refused = 0

    def write(self, path, payload) -> None:
        if self.failing and Path(path) == self._status_file:
            self.refused += 1
            raise OSError(errno.ENOSPC, "No space left on device")
        self._real(path, payload)


@pytest.fixture
def recorder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    status_file = tmp_path / "source_state" / "mtconnect_recorder_status.json"
    filesystem = _FailingFilesystem(rt._write_json_atomic, status_file)

    monkeypatch.setattr(rt, "_write_json_atomic", filesystem.write)
    monkeypatch.setattr(rt, "STATUS_FILE", status_file)
    monkeypatch.setattr(rt, "STATE_FILE", tmp_path / "source_state" / "state.json")
    monkeypatch.setattr(rt, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(rt, "MANAGED_MODE", False)
    monkeypatch.setattr(rt, "MtconnectClient", _online_client)
    # ``run`` refreshes configuration from the environment on entry, so the
    # source has to be declared the way an unmanaged recorder receives it.
    monkeypatch.setenv("FCP_RECORDER_SOURCES", f"{SOURCE}={BASE_URL}")
    monkeypatch.delenv("FCP_RECORDER_SOURCES_JSON", raising=False)
    # ``run`` records a module-level stop reason; keep cases independent.
    monkeypatch.setattr(rt, "_STOP_REASON", None, raising=False)

    service = rt.RecorderRuntime()
    service.store = DurableRecorderStore(tmp_path / "data")
    service.enabled = True
    service.configuration_ready = True
    service.sources = {SOURCE: BASE_URL}
    try:
        yield service, filesystem, status_file
    finally:
        service.stop_event.set()
        service.executor.shutdown(wait=True, cancel_futures=False)
        service._harvest_capture_results()
        rt.unregister_stop_target(service)


def test_an_operator_stop_on_a_failing_filesystem_still_exits_gracefully(
    recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exit the supervisor reads as "the operator stopped this"."""

    service, filesystem, _status_file = recorder
    monkeypatch.setattr(rt, "RUN_ONCE", False)

    def cycle() -> None:
        # The host filesystem fills while the recorder is running, and the
        # operator presses Ctrl+C.
        filesystem.failing = True
        service.request_stop(signal.SIGINT)

    monkeypatch.setattr(service, "run_fetch_cycle", cycle)

    service.run()

    # ``run`` returning is what keeps ``start_recorder`` at exit code 0, which is
    # the only exit the supervisor treats as an operator stop.
    assert filesystem.refused >= 1
    assert service.state == "stopped"
    # The operator's own reason survives; a failed write cannot relabel it.
    assert rt.last_stop_reason() == rt.SIGNAL_STOP_REASON
    # Shutdown bookkeeping still ran instead of being skipped by the raise.
    assert all(target is not service for target in rt._STOP_TARGETS)


def test_capture_continues_while_the_heartbeat_cannot_be_written(
    recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A heartbeat write failure must not end capture of primary evidence."""

    service, filesystem, _status_file = recorder
    monkeypatch.setattr(rt, "RUN_ONCE", True)
    filesystem.failing = True

    service.run()

    assert filesystem.refused >= 1
    assert service.observations_written > 0
    assert service.raw_batches_written > 0
    archived = list(
        service.store.iter_raw_batches(
            source_name=SOURCE,
            instance_id=INSTANCE_ID,
        )
    )
    assert archived


def test_a_refused_heartbeat_leaves_the_published_status_stale(
    recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Containment must never invent a heartbeat that was not written.

    A host updater proves a replacement recorder is alive from this file. If the
    write is refused the file has to stay exactly as it was, so a stale
    heartbeat keeps reading as "not proven healthy".
    """

    service, filesystem, status_file = recorder
    monkeypatch.setattr(rt, "RUN_ONCE", True)

    service.publish_status(force=True)
    published = status_file.read_bytes()
    assert json.loads(published)["status_publication_error"] == ""

    filesystem.failing = True
    service.publish_status(force=True)

    assert filesystem.refused == 1
    assert status_file.read_bytes() == published


def test_a_refused_heartbeat_is_reported_once_the_filesystem_recovers(
    recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contained is not swallowed: the operator still learns it happened."""

    service, filesystem, status_file = recorder
    monkeypatch.setattr(rt, "RUN_ONCE", True)

    filesystem.failing = True
    service.publish_status(force=True)
    assert service.status_publication_error.startswith("OSError:")

    filesystem.failing = False
    service.publish_status(force=True)
    recovered = json.loads(status_file.read_text(encoding="utf-8"))
    assert "No space left on device" in recovered["status_publication_error"]

    service.publish_status(force=True)
    settled = json.loads(status_file.read_text(encoding="utf-8"))
    assert settled["status_publication_error"] == ""


def test_a_persistent_failure_is_announced_once_rather_than_every_cycle(
    recorder, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Containment must not answer one amplification with another.

    The heartbeat is published every cycle. A filesystem that is still failing
    on the next cycle would otherwise write a warning per second into the
    recorder's own log, on the same host that is already out of room.
    """

    service, filesystem, _status_file = recorder
    filesystem.failing = True

    with caplog.at_level(logging.WARNING, logger=rt.log.name):
        for _ in range(5):
            service.publish_status(force=True)

    assert filesystem.refused == 5
    announcements = [
        record
        for record in caplog.records
        if "status heartbeat could not be written" in record.getMessage()
    ]
    assert len(announcements) == 1

    # A different host failure is a new condition and is announced again.
    def refuse_read_only(path, payload):
        del payload
        if Path(path) == Path(rt.STATUS_FILE):
            raise OSError(errno.EROFS, "Read-only file system")
        raise AssertionError("unexpected write")

    monkeypatch.setattr(rt, "_write_json_atomic", refuse_read_only)
    with caplog.at_level(logging.WARNING, logger=rt.log.name):
        service.publish_status(force=True)
    announcements = [
        record
        for record in caplog.records
        if "status heartbeat could not be written" in record.getMessage()
    ]
    assert len(announcements) == 2

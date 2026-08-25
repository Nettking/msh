from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import FilesystemMeasurement, PressureThresholds
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.resource_pressure import (
    RecorderAdmissionController,
    RecorderResourceBudget,
    attach_runtime_resource_pressure,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore

NOW = datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc)
SOURCE = "machine"
BASE_URL = "http://agent:5000"

PROBE_XML = (
    '<MTConnectDevices xmlns="urn:mtconnect.org:MTConnectDevices:1.7">'
    '<Header instanceId="7"/>'
    '<Devices><Device id="machine" name="machine" uuid="machine-1">'
    '<DataItems><DataItem id="x" category="SAMPLE" type="POSITION"/>'
    "</DataItems></Device></Devices></MTConnectDevices>"
)
CURRENT_XML = (
    '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
    '<Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/>'
    '<Streams><DeviceStream name="machine" uuid="machine-1">'
    '<ComponentStream component="Linear" componentId="x">'
    '<Samples><Position dataItemId="x" sequence="1" '
    'timestamp="2026-08-25T00:00:00Z">1</Position></Samples>'
    "</ComponentStream></DeviceStream></Streams></MTConnectStreams>"
)


def _controller() -> RecorderAdmissionController:
    measurement = FilesystemMeasurement(
        resource_id="device:data",
        observed_at=NOW,
        total_bytes=100_000,
        free_bytes=19_000,
        total_inodes=2000,
        free_inodes=1000,
        available=True,
    )
    return RecorderAdmissionController(
        thresholds=PressureThresholds(
            critical_free_bytes=10_000,
            pressure_free_bytes=20_000,
            warning_free_bytes=30_000,
            critical_free_inodes=10,
            pressure_free_inodes=20,
            warning_free_inodes=30,
            max_measurement_age_seconds=60,
            future_measurement_tolerance_seconds=2,
        ),
        measurer=lambda _path: measurement,
        clock=lambda: NOW,
    )


def test_cold_start_probe_is_not_persisted_when_new_capture_is_at_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    runtime.enabled = True
    runtime.configuration_ready = True
    attach_runtime_resource_pressure(
        runtime,
        controller=_controller(),
        budget=RecorderResourceBudget(
            probe_bytes=512,
            sample_bytes=512,
            observation_bytes=1024,
            compatibility_bytes=1024,
            compatibility_state_bytes=1024,
            data_inodes=1,
            state_inodes=1,
            probe_inodes=1,
        ),
        state_file=state_file,
    )

    class Client:
        def __init__(self, base_url: str, *, timeout: float) -> None:
            del base_url, timeout

        def fetch_current(self) -> str:
            return CURRENT_XML

        def fetch_probe(self) -> str:
            return PROBE_XML

        def fetch_sample(self, *, from_sequence: int, count: int) -> str:
            del from_sequence, count
            pytest.fail("sample fetch must not start after probe admission refuses")

    monkeypatch.setattr(recorder_runtime, "MtconnectClient", Client)
    try:
        source, success, error = runtime.capture_source(SOURCE, BASE_URL)

        assert source == SOURCE
        assert success is True
        assert error == ""
        assert tuple(runtime.store.probe_root.rglob("*.xml.gz")) == ()
        assert tuple(runtime.store.probe_root.rglob("*.manifest.json")) == ()
        assert tuple(runtime.store.raw_root.rglob("*.xml.gz")) == ()
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        assert status["resource_admission"]["state"] == "paused"
        assert status["resource_admission"]["level"] == "pressure"
    finally:
        runtime.executor.shutdown(wait=True, cancel_futures=True)
        recorder_runtime.unregister_stop_target(runtime)

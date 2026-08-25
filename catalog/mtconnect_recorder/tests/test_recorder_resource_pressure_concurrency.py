from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path

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
WORKERS = 8


def _streams_xml() -> str:
    return (
        '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
        '<Header instanceId="7" firstSequence="1" lastSequence="1" nextSequence="2"/>'
        '<Streams><DeviceStream name="machine" uuid="machine-1">'
        '<ComponentStream component="Linear" componentId="x">'
        '<Samples><Position dataItemId="x" sequence="1" '
        'timestamp="2026-08-25T00:00:00Z">1</Position></Samples>'
        "</ComponentStream></DeviceStream></Streams></MTConnectStreams>"
    )


def test_eight_recorder_workers_share_one_pressure_envelope(tmp_path: Path) -> None:
    measurement = FilesystemMeasurement(
        resource_id="device:shared",
        observed_at=NOW,
        total_bytes=100_000,
        free_bytes=50_000,
        total_inodes=10_000,
        free_inodes=10_000,
        available=True,
    )
    controller = RecorderAdmissionController(
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
    state_file = tmp_path / "state.json"
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    guard = attach_runtime_resource_pressure(
        runtime,
        controller=controller,
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
    batch = recorder_runtime.parse_streams(
        _streams_xml(),
        source_name=SOURCE,
        probe=None,
        max_observations=10,
        max_sequence_span=10,
    )

    release = threading.Event()
    all_attempted = threading.Event()
    lock = threading.Lock()
    attempted = 0
    admitted = 0
    refused = 0

    def worker(index: int) -> None:
        nonlocal attempted, admitted, refused
        guard.begin_capture(f"{SOURCE}-{index}", BASE_URL)
        try:
            try:
                guard.begin_new(
                    source_name=f"{SOURCE}-{index}",
                    requested_from=1,
                    xml_text=_streams_xml(),
                    batch=batch,
                )
            except BaseException:  # the internal pause signal is boundary-only
                with lock:
                    refused += 1
                    attempted += 1
                    if attempted == WORKERS:
                        all_attempted.set()
                return
            with lock:
                admitted += 1
                attempted += 1
                if attempted == WORKERS:
                    all_attempted.set()
            assert release.wait(timeout=5)
        finally:
            guard.end_transaction()
            guard.end_capture()

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(WORKERS)]
    try:
        for thread in threads:
            thread.start()
        assert all_attempted.wait(timeout=5)

        during = controller.assessment(runtime.store.data_dir)
        assert 0 < admitted < WORKERS
        assert refused == WORKERS - admitted
        assert during.reserved_bytes > 0
        # One admitted transaction may cross from WARNING into PRESSURE. Once
        # that happens, every later new transaction is refused. The invariant is
        # therefore the CRITICAL emergency floor, not the PRESSURE threshold.
        assert during.level.name == "PRESSURE"
        assert during.effective_free_bytes >= 10_000
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=5)
        runtime.executor.shutdown(wait=True, cancel_futures=True)
        recorder_runtime.unregister_stop_target(runtime)

    after = controller.assessment(runtime.store.data_dir)
    assert after.reserved_bytes == 0
    assert after.reserved_inodes == 0

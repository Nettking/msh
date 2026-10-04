from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureLevel,
    PressureThresholds,
)
from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.model import ProbeModel, SourceCheckpoint
from catalog.mtconnect_recorder.publication_frontier_runtime import (
    install_publication_frontier_runtime,
)
from catalog.mtconnect_recorder.recovery_frontier import RecorderRecoveryFrontier
from catalog.mtconnect_recorder.resource_pressure import (
    RecorderAdmissionController,
    RecorderResourceBudget,
    RecorderResourcePaused,
    attach_runtime_resource_pressure,
    install_runtime_resource_pressure,
)
from catalog.mtconnect_recorder.schema_compat import CHECKPOINT_SCHEMA
from catalog.mtconnect_recorder.storage import DurableRecorderStore

# Production startup reaches runtime through catalog.mtconnect_recorder.run(),
# which composes B01 admission before the B03 frontier. The full suite may have
# imported the runtime submodule directly earlier, so explicitly install both
# boundaries in production order instead of depending on package import order.
install_runtime_resource_pressure(recorder_runtime)
install_publication_frontier_runtime(recorder_runtime)

NOW = datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc)
SOURCE = "machine"
BASE_URL = "http://agent:5000"


def _streams_xml(sequences: list[int], *, instance_id: int = 7) -> str:
    observations = "".join(
        f'<Position dataItemId="x" sequence="{sequence}" '
        f'timestamp="2026-08-25T00:00:{index:02d}Z">{sequence}</Position>'
        for index, sequence in enumerate(sequences)
    )
    first = min(sequences)
    last = max(sequences)
    return (
        '<MTConnectStreams xmlns="urn:mtconnect.org:MTConnectStreams:1.7">'
        f'<Header instanceId="{instance_id}" firstSequence="{first}" '
        f'lastSequence="{last}" nextSequence="{last + 1}"/>'
        '<Streams><DeviceStream name="machine" uuid="machine-1">'
        '<ComponentStream component="Linear" componentId="x">'
        f"<Samples>{observations}</Samples>"
        "</ComponentStream></DeviceStream></Streams></MTConnectStreams>"
    )


def _batch(sequences: list[int]):
    return recorder_runtime.parse_streams(
        _streams_xml(sequences),
        source_name=SOURCE,
        probe=None,
        max_observations=100,
        max_sequence_span=100,
    )


def _probe() -> ProbeModel:
    return ProbeModel(sha256="a" * 64, devices={}, data_items={})


def _checkpoint(*, next_sequence: int) -> SourceCheckpoint:
    return SourceCheckpoint(
        source_name=SOURCE,
        base_url=BASE_URL,
        machine_id="machine-1",
        agent_instance_id=7,
        next_sequence=next_sequence,
        probe_sha256="a" * 64,
    )


def _thresholds() -> PressureThresholds:
    return PressureThresholds(
        critical_free_bytes=10_000,
        pressure_free_bytes=20_000,
        warning_free_bytes=30_000,
        critical_free_inodes=10,
        pressure_free_inodes=20,
        warning_free_inodes=30,
        max_measurement_age_seconds=60,
        future_measurement_tolerance_seconds=2,
    )


def _budget() -> RecorderResourceBudget:
    return RecorderResourceBudget(
        sample_bytes=512,
        observation_bytes=1024,
        compatibility_bytes=1024,
        compatibility_state_bytes=1024,
        data_inodes=1,
        state_inodes=1,
    )


def _measurement(
    *,
    free_bytes: int,
    resource_id: str = "device:data",
    free_inodes: int = 1000,
) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=NOW,
        total_bytes=100_000,
        free_bytes=free_bytes,
        total_inodes=2000,
        free_inodes=free_inodes,
        available=True,
    )


def _controller(
    *,
    free_bytes: int,
    split_state: bool = False,
) -> RecorderAdmissionController:
    def measure(path: Path | str) -> FilesystemMeasurement:
        resource_id = (
            "device:state"
            if split_state and str(path).endswith("state.json")
            else "device:data"
        )
        return _measurement(free_bytes=free_bytes, resource_id=resource_id)

    return RecorderAdmissionController(
        thresholds=_thresholds(),
        measurer=measure,
        clock=lambda: NOW,
    )


def _runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    free_bytes: int,
    split_state: bool = False,
):
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    runtime.enabled = True
    runtime.configuration_ready = True
    controller = _controller(free_bytes=free_bytes, split_state=split_state)
    guard = attach_runtime_resource_pressure(
        runtime,
        controller=controller,
        budget=_budget(),
        state_file=state_file,
    )
    return runtime, guard, controller, state_file


def _close(runtime) -> None:
    runtime.executor.shutdown(wait=True, cancel_futures=True)
    recorder_runtime.unregister_stop_target(runtime)


def _prepare_pending_raw(runtime) -> None:
    batch = _batch([1, 2])
    xml_text = _streams_xml([1, 2])
    # Use the base frontier deliberately: this fixture represents evidence that
    # was persisted by the previous process before the current pressure state.
    frontier = RecorderRecoveryFrontier(runtime.store)
    frontier.mark_pending(
        source_name=SOURCE,
        requested_from=1,
        xml_text=xml_text,
        batch=batch,
    )
    runtime.store.store_raw_batch(
        source_name=SOURCE,
        requested_from=1,
        xml_text=xml_text,
        batch=batch,
    )
    runtime.checkpoints[SOURCE] = _checkpoint(next_sequence=1)


def test_completion_admission_may_finish_at_pressure_but_never_spend_critical_floor() -> None:
    pressure = _controller(free_bytes=19_000)
    with pressure.reserve_completion("/data", bytes_required=5_000):
        assessed = pressure.assessment("/data")
        assert assessed.level == PressureLevel.PRESSURE
        assert assessed.effective_free_bytes == 14_000

    with (
        pytest.raises(HostResourceRefused) as floor,
        pressure.reserve_completion("/data", bytes_required=9_000),
    ):
        pass
    assert floor.value.code == "emergency_reserve"
    assert floor.value.assessment.level == PressureLevel.CRITICAL

    critical = _controller(free_bytes=10_000)
    with (
        pytest.raises(HostResourceRefused) as stopped,
        critical.reserve_completion("/data", bytes_required=1),
    ):
        pass
    assert stopped.value.code == "resource_critical"


def test_new_capture_pauses_before_frontier_or_raw_and_does_not_mark_source_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _guard, _controller_value, _state_file = _runtime(
        tmp_path,
        monkeypatch,
        free_bytes=19_000,
    )
    probe = _probe()
    sample_xml = _streams_xml([1])

    class Client:
        def __init__(self, base_url: str, *, timeout: float) -> None:
            del base_url, timeout

        def fetch_current(self) -> str:
            return sample_xml

        def fetch_sample(self, *, from_sequence: int, count: int) -> str:
            del count
            assert from_sequence == 1
            return sample_xml

    try:
        monkeypatch.setattr(recorder_runtime, "MtconnectClient", Client)
        monkeypatch.setattr(runtime, "_load_probe", lambda **_kwargs: probe)

        result = runtime.capture_source(SOURCE, BASE_URL)
        source, success, error = result

        assert source == SOURCE
        assert success is True
        assert error == ""
        assert result.transaction_complete is False
        assert tuple(runtime.store.raw_root.rglob("*.xml.gz")) == ()
        assert tuple(runtime.store.raw_root.rglob("*.manifest.json")) == ()
        assert tuple(runtime.store.raw_root.rglob(".recovery-frontier.json")) == ()
        assert runtime.backoff[SOURCE] == recorder_runtime.BACKOFF_INITIAL
        status = runtime.source_status[SOURCE]
        assert status["last_error"] == ""
        assert status["resource_admission"]["state"] == "paused"
        assert status["resource_admission"]["level"] == "pressure"
        assert status["resource_admission"]["code"] == "resource_pressure"

        runtime._harvest_capture_results()
        assert runtime.state == "degraded"
        assert runtime.last_error == ""
        assert "local host resource pressure" in runtime.message
    finally:
        _close(runtime)


def test_pending_raw_recovery_finishes_at_pressure_when_critical_floor_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    _prepare_pending_raw(runtime)
    controller = _controller(free_bytes=19_000)
    attach_runtime_resource_pressure(
        runtime,
        controller=controller,
        budget=_budget(),
        state_file=state_file,
    )
    try:
        recovered = runtime._recover_archived_batches(
            source_name=SOURCE,
            base_url=BASE_URL,
            instance_id=7,
            expected=1,
            probe=_probe(),
        )

        assert recovered == 3
        assert runtime.checkpoints[SOURCE].next_sequence == 3
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert len(tuple(runtime.store.raw_root.rglob("*.manifest.json"))) == 1
        assert tuple(runtime.store.observation_root.rglob("*.ndjson"))
        assert tuple(runtime.store.normalized_root.rglob("*.jsonl"))
        frontier = RecorderRecoveryFrontier(runtime.store).lookup(
            source_name=SOURCE,
            instance_id=7,
            expected=3,
        )
        assert frontier.initialized is True
        assert frontier.ref is None
        payload = json.loads(state_file.read_text(encoding="utf-8"))
        assert payload["schema"] == CHECKPOINT_SCHEMA
        assert payload["sources"][SOURCE]["next_sequence"] == 3
    finally:
        _close(runtime)


def test_critical_recovery_pause_keeps_raw_pending_and_checkpoint_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file = tmp_path / "state.json"
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", state_file)
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = DurableRecorderStore(tmp_path / "data")
    runtime.sources = {SOURCE: BASE_URL}
    _prepare_pending_raw(runtime)
    controller = _controller(free_bytes=10_000)
    attach_runtime_resource_pressure(
        runtime,
        controller=controller,
        budget=_budget(),
        state_file=state_file,
    )
    try:
        with pytest.raises(RecorderResourcePaused) as raised:
            runtime._recover_archived_batches(
                source_name=SOURCE,
                base_url=BASE_URL,
                instance_id=7,
                expected=1,
                probe=_probe(),
            )

        assert raised.value.pause.assessment.level == PressureLevel.CRITICAL
        assert runtime.checkpoints[SOURCE].next_sequence == 1
        assert len(tuple(runtime.store.raw_root.rglob("*.xml.gz"))) == 1
        assert len(tuple(runtime.store.raw_root.rglob("*.manifest.json"))) == 1
        pending = json.loads(
            next(runtime.store.raw_root.rglob(".recovery-frontier.json")).read_text(
                encoding="utf-8"
            )
        )
        assert pending["state"] == "pending"
        assert pending["first_sequence"] == 1
        assert tuple(runtime.store.observation_root.rglob("*.ndjson")) == ()
        assert tuple(runtime.store.normalized_root.rglob("*.jsonl")) == ()
        assert not state_file.exists()
    finally:
        _close(runtime)


def test_data_and_checkpoint_requirements_share_one_envelope_on_same_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, guard, controller, state_file = _runtime(
        tmp_path,
        monkeypatch,
        free_bytes=50_000,
    )
    try:
        batch = _batch([1])
        guard.begin_capture(SOURCE, BASE_URL)
        guard.begin_new(
            source_name=SOURCE,
            requested_from=1,
            xml_text=_streams_xml([1]),
            batch=batch,
        )
        data = controller.assessment(runtime.store.data_dir)
        state = controller.assessment(state_file)
        assert data.resource_id == state.resource_id == "device:data"
        assert data.reserved_bytes == state.reserved_bytes
        assert data.reserved_bytes > 0
        # B03's producer-side publication pointer is part of this same finite
        # recorder transaction, so it consumes one additional data inode.
        assert data.reserved_inodes == 3
    finally:
        guard.end_transaction()
        guard.end_capture()
        _close(runtime)


def test_distinct_data_and_checkpoint_filesystems_are_reserved_independently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, guard, controller, state_file = _runtime(
        tmp_path,
        monkeypatch,
        free_bytes=50_000,
        split_state=True,
    )
    try:
        guard.begin_capture(SOURCE, BASE_URL)
        guard.begin_new(
            source_name=SOURCE,
            requested_from=1,
            xml_text=_streams_xml([1]),
            batch=_batch([1]),
        )
        data = controller.assessment(runtime.store.data_dir)
        state = controller.assessment(state_file)
        assert data.resource_id == "device:data"
        assert state.resource_id == "device:state"
        assert data.reserved_bytes > 0
        assert state.reserved_bytes > 0
        # The discovery pointer belongs to data, not checkpoint state.
        assert data.reserved_inodes == 2
        assert state.reserved_inodes == 1
    finally:
        guard.end_transaction()
        guard.end_capture()
        _close(runtime)

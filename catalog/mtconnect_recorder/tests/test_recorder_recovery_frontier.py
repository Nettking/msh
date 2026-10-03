from __future__ import annotations

import json
from pathlib import Path

import pytest

from catalog.mtconnect_recorder import runtime as recorder_runtime
from catalog.mtconnect_recorder.model import (
    MtconnectProtocolError,
    ProbeModel,
    SourceCheckpoint,
)
from catalog.mtconnect_recorder.recovery_frontier import RecorderRecoveryFrontier


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
        source_name="machine",
        probe=None,
        max_observations=100,
        max_sequence_span=100,
    )


def _probe() -> ProbeModel:
    return ProbeModel(sha256="a" * 64, devices={}, data_items={})


def _checkpoint(*, next_sequence: int) -> SourceCheckpoint:
    return SourceCheckpoint(
        source_name="machine",
        base_url="http://agent:5000",
        machine_id="machine-1",
        agent_instance_id=7,
        next_sequence=next_sequence,
        probe_sha256="a" * 64,
    )


def _runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(recorder_runtime, "STATE_FILE", tmp_path / "state.json")
    runtime = recorder_runtime.RecorderRuntime()
    runtime.store = recorder_runtime.DurableRecorderStore(tmp_path / "data")
    return runtime


def _close(runtime) -> None:
    runtime.executor.shutdown(wait=True, cancel_futures=True)
    recorder_runtime.unregister_stop_target(runtime)


def test_pending_frontier_recovers_without_archive_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
    try:
        batch = _batch([1, 2])
        xml_text = _streams_xml([1, 2])
        frontier = RecorderRecoveryFrontier(runtime.store)
        frontier.mark_pending(
            source_name="machine",
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )
        runtime.store.store_raw_batch(
            source_name="machine",
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )
        runtime.checkpoints["machine"] = _checkpoint(next_sequence=1)

        def forbidden_scan(**_kwargs):
            raise AssertionError("healthy recovery walked the lifetime raw archive")

        monkeypatch.setattr(runtime.store, "iter_raw_batches", forbidden_scan)
        recovered = runtime._recover_archived_batches(
            source_name="machine",
            base_url="http://agent:5000",
            instance_id=7,
            expected=1,
            probe=_probe(),
        )

        assert recovered == 3
        assert runtime.checkpoints["machine"].next_sequence == 3
        assert frontier.lookup(
            source_name="machine",
            instance_id=7,
            expected=3,
        ).ref is None
        assert list(runtime.store.observation_root.rglob("*.ndjson"))
        assert list(runtime.store.normalized_root.rglob("*.jsonl"))
    finally:
        _close(runtime)


def test_legacy_archive_is_scanned_once_then_migrated_to_fixed_frontier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
    try:
        batch = _batch([1, 2])
        runtime.store.store_raw_batch(
            source_name="machine",
            requested_from=1,
            xml_text=_streams_xml([1, 2]),
            batch=batch,
        )
        runtime.checkpoints["machine"] = _checkpoint(next_sequence=3)
        original_scan = runtime.store.iter_raw_batches
        calls = 0

        def counted_scan(**kwargs):
            nonlocal calls
            calls += 1
            return original_scan(**kwargs)

        monkeypatch.setattr(runtime.store, "iter_raw_batches", counted_scan)
        assert runtime._recover_archived_batches(
            source_name="machine",
            base_url="http://agent:5000",
            instance_id=7,
            expected=3,
            probe=_probe(),
        ) == 3
        assert calls == 1

        def forbidden_scan(**_kwargs):
            raise AssertionError("migrated archive was scanned again")

        monkeypatch.setattr(runtime.store, "iter_raw_batches", forbidden_scan)
        assert runtime._recover_archived_batches(
            source_name="machine",
            base_url="http://agent:5000",
            instance_id=7,
            expected=3,
            probe=_probe(),
        ) == 3
    finally:
        _close(runtime)


def test_stale_pending_frontier_after_checkpoint_self_heals_without_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
    try:
        frontier = RecorderRecoveryFrontier(runtime.store)
        frontier.mark_pending(
            source_name="machine",
            requested_from=1,
            xml_text=_streams_xml([1, 2]),
            batch=_batch([1, 2]),
        )
        runtime.checkpoints["machine"] = _checkpoint(next_sequence=3)

        def forbidden_scan(**_kwargs):
            raise AssertionError("stale pointer forced an archive scan")

        monkeypatch.setattr(runtime.store, "iter_raw_batches", forbidden_scan)
        assert runtime._recover_archived_batches(
            source_name="machine",
            base_url="http://agent:5000",
            instance_id=7,
            expected=3,
            probe=_probe(),
        ) == 3
        payload = json.loads(
            frontier.path(source_name="machine", instance_id=7).read_text(
                encoding="utf-8"
            )
        )
        assert payload["state"] == "clear"
        assert payload["next_sequence"] == 3
    finally:
        _close(runtime)


def test_frontier_manifest_mismatch_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
    try:
        batch = _batch([1, 2])
        xml_text = _streams_xml([1, 2])
        frontier = RecorderRecoveryFrontier(runtime.store)
        frontier.mark_pending(
            source_name="machine",
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )
        raw = runtime.store.store_raw_batch(
            source_name="machine",
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )
        manifest = json.loads(raw.manifest_path.read_text(encoding="utf-8"))
        manifest["requested_from"] = 99
        raw.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        with pytest.raises(MtconnectProtocolError, match="does not match"):
            frontier.lookup(source_name="machine", instance_id=7, expected=1)
    finally:
        _close(runtime)


def test_capture_publishes_pending_frontier_before_first_raw_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
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
        frontier = RecorderRecoveryFrontier(runtime.store)

        def fail_before_raw(**_kwargs):
            payload = json.loads(
                frontier.path(source_name="machine", instance_id=7).read_text(
                    encoding="utf-8"
                )
            )
            assert payload["state"] == "pending"
            assert payload["first_sequence"] == 1
            raise OSError("injected raw write failure")

        monkeypatch.setattr(runtime.store, "store_batch", fail_before_raw)
        _, success, error = runtime.capture_source("machine", "http://agent:5000")

        assert success is False
        assert "injected raw write failure" in error
        assert not list(runtime.store.raw_root.rglob("*.xml.gz"))
    finally:
        _close(runtime)


def test_successful_capture_result_marks_its_transaction_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
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
        result = runtime.capture_source("machine", "http://agent:5000")

        assert tuple(result) == ("machine", True, "")
        assert result.transaction_complete is True
        assert runtime.checkpoints["machine"].next_sequence == 2
    finally:
        _close(runtime)


def test_state_loss_rebuild_keeps_explicit_full_archive_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path, monkeypatch)
    try:
        first = _batch([1, 2])
        second = _batch([3, 4])
        runtime.store.store_raw_batch(
            source_name="machine",
            requested_from=1,
            xml_text=_streams_xml([1, 2]),
            batch=first,
        )
        runtime.store.store_raw_batch(
            source_name="machine",
            requested_from=3,
            xml_text=_streams_xml([3, 4]),
            batch=second,
        )
        # A pointer to only the newest transaction must not truncate the
        # deliberate state-loss rebuild. With no checkpoint, history scanning
        # is recovery work rather than a recurring healthy-poll cost.
        RecorderRecoveryFrontier(runtime.store).mark_pending(
            source_name="machine",
            requested_from=3,
            xml_text=_streams_xml([3, 4]),
            batch=second,
        )

        assert runtime.checkpoints == {}
        recovered = runtime._recover_archived_batches(
            source_name="machine",
            base_url="http://agent:5000",
            instance_id=7,
            expected=1,
            probe=_probe(),
        )
        assert recovered == 5
        assert runtime.checkpoints["machine"].next_sequence == 5
    finally:
        _close(runtime)

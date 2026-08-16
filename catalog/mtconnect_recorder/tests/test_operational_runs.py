"""S3 MachineRun segmentation and duration-accounting regressions."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.model import (
    BoundaryConfidence,
    BoundaryReason,
)
from catalog.mtconnect_recorder.operational.runs import build_machine_runs

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233
SECOND = 1_000_000


def _observation(
    *,
    sequence: int,
    second: int,
    value: str,
    mtconnect_type: str = "EXECUTION",
    device_uuid: str = "device-alpha",
    machine_id: str = "machine-alpha",
    instance: int = INSTANCE,
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": machine_id,
        "device_uuid": device_uuid,
        "device_name": machine_id,
        "category": CATEGORY_EVENT,
        "observation_type": mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "data_item_id": f"{device_uuid}-path-1-{mtconnect_type.lower()}",
        "stream_component": "Path",
        "stream_component_id": "path-1",
        "stream_component_name": "PATH-1",
        "native_value": value,
        "attributes": {"timestamp": f"2026-08-07T09:00:{second:02d}Z"},
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=instance,
        raw_batch_sha256="a" * 64,
    )


def test_program_stops_remain_inside_one_machine_run_and_are_accounted():
    runs = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=5, value="ACTIVE"),
            _observation(sequence=3, second=15, value="PROGRAM_STOPPED"),
            _observation(sequence=4, second=25, value="ACTIVE"),
            _observation(sequence=5, second=35, value="PROGRAM_STOPPED"),
            _observation(sequence=6, second=45, value="ACTIVE"),
            _observation(sequence=7, second=55, value="READY"),
        ]
    )

    assert len(runs) == 1
    run = runs[0]
    assert run.start_boundary.reason is BoundaryReason.EXECUTION_ACTIVE
    assert run.end_boundary.reason is BoundaryReason.EXECUTION_READY
    assert run.wall_duration_us == 50 * SECOND
    assert run.active_duration_us == 30 * SECOND
    assert run.program_stopped_duration_us == 20 * SECOND
    assert run.feed_hold_duration_us == 0
    assert run.other_execution_duration_us == 0
    assert run.unknown_duration_us == 0
    assert run.duration_buckets_reconcile is True


def test_feed_hold_and_other_execution_have_separate_duration_buckets():
    feed_hold = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(sequence=3, second=20, value="FEED_HOLD"),
            _observation(sequence=4, second=30, value="ACTIVE"),
            _observation(sequence=5, second=40, value="READY"),
        ]
    )[0]
    assert feed_hold.wall_duration_us == 30 * SECOND
    assert feed_hold.active_duration_us == 20 * SECOND
    assert feed_hold.feed_hold_duration_us == 10 * SECOND
    assert feed_hold.duration_buckets_reconcile is True

    other = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(sequence=3, second=20, value="OPTIONAL_STOP"),
            _observation(sequence=4, second=25, value="ACTIVE"),
            _observation(sequence=5, second=30, value="READY"),
        ]
    )[0]
    assert other.wall_duration_us == 20 * SECOND
    assert other.active_duration_us == 15 * SECOND
    assert other.other_execution_duration_us == 5 * SECOND
    assert other.duration_buckets_reconcile is True


def test_ready_separates_multiple_same_device_runs():
    runs = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=5, value="ACTIVE"),
            _observation(sequence=3, second=10, value="READY"),
            _observation(sequence=4, second=20, value="ACTIVE"),
            _observation(sequence=5, second=30, value="READY"),
        ]
    )

    assert len(runs) == 2
    assert [(run.start_sequence, run.end_sequence) for run in runs] == [(2, 3), (4, 5)]
    assert all(run.end_boundary.reason is BoundaryReason.EXECUTION_READY for run in runs)


def test_interleaved_devices_create_independent_machine_runs():
    runs = build_machine_runs(
        [
            _observation(
                sequence=1,
                second=0,
                value="ACTIVE",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _observation(
                sequence=2,
                second=1,
                value="ACTIVE",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _observation(
                sequence=3,
                second=10,
                value="READY",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _observation(
                sequence=4,
                second=20,
                value="READY",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
        ]
    )

    assert len(runs) == 2
    by_device = {run.device_key: run for run in runs}
    assert set(by_device) == {"uuid:device-a", "uuid:device-b"}
    assert by_device["uuid:device-a"].wall_duration_us == 10 * SECOND
    assert by_device["uuid:device-b"].wall_duration_us == 19 * SECOND
    assert all(run.duration_buckets_reconcile for run in runs)


def test_capture_beginning_active_creates_a_partial_start():
    run = build_machine_runs(
        [
            _observation(sequence=1, second=10, value="ACTIVE"),
            _observation(sequence=2, second=20, value="READY"),
        ]
    )[0]

    assert run.start_boundary.reason is BoundaryReason.CAPTURE_BEGINS_ACTIVE
    assert run.start_boundary.confidence is BoundaryConfidence.PARTIAL
    assert run.partial_start is True
    assert run.end_boundary.reason is BoundaryReason.EXECUTION_READY
    assert run.partial_end is False


def test_capture_ending_active_closes_the_run_as_partial():
    run = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(
                sequence=3,
                second=20,
                value="O1234",
                mtconnect_type="PROGRAM",
            ),
        ]
    )[0]

    assert run.start_boundary.reason is BoundaryReason.EXECUTION_ACTIVE
    assert run.end_boundary.reason is BoundaryReason.CAPTURE_END
    assert run.end_boundary.confidence is BoundaryConfidence.PARTIAL
    assert run.partial_end is True
    assert run.wall_duration_us == 10 * SECOND
    assert run.duration_buckets_reconcile is True


def test_agent_gap_inside_active_closes_and_restarts_partial_runs():
    runs = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(
                sequence=3,
                second=15,
                value="O1234",
                mtconnect_type="PROGRAM",
            ),
            # Sequence 4 is genuinely absent from the complete Agent stream.
            _observation(sequence=5, second=20, value="ACTIVE"),
            _observation(sequence=6, second=30, value="READY"),
        ]
    )

    assert len(runs) == 2
    first, second = runs
    assert first.end_boundary.reason is BoundaryReason.SEQUENCE_GAP
    assert first.partial_end is True
    assert first.wall_duration_us == 5 * SECOND
    assert second.start_boundary.reason is BoundaryReason.SEQUENCE_GAP
    assert second.partial_start is True
    assert second.end_boundary.reason is BoundaryReason.EXECUTION_READY
    assert second.wall_duration_us == 10 * SECOND
    assert all(run.duration_buckets_reconcile for run in runs)


def test_timestamp_regression_splits_runs_without_negative_duration():
    runs = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(
                sequence=3,
                second=20,
                value="O1",
                mtconnect_type="PROGRAM",
            ),
            _observation(
                sequence=4,
                second=5,
                value="10",
                mtconnect_type="LINE",
            ),
            _observation(sequence=5, second=15, value="READY"),
        ]
    )

    assert len(runs) == 2
    first, second = runs
    assert first.end_boundary.reason is BoundaryReason.TIMESTAMP_REGRESSION
    assert second.start_boundary.reason is BoundaryReason.TIMESTAMP_REGRESSION
    assert first.wall_duration_us == 10 * SECOND
    assert second.wall_duration_us == 10 * SECOND
    assert all(run.wall_duration_us >= 0 for run in runs)
    assert all(run.duration_buckets_reconcile for run in runs)


def test_known_next_agent_instance_marks_prior_open_run_instance_end():
    runs = build_machine_runs(
        [
            _observation(
                sequence=1,
                second=0,
                value="ACTIVE",
                instance=INSTANCE,
            ),
            _observation(
                sequence=1,
                second=10,
                value="ACTIVE",
                instance=INSTANCE + 1,
            ),
        ]
    )

    assert len(runs) == 2
    assert runs[0].agent_instance_id == INSTANCE
    assert runs[0].end_boundary.reason is BoundaryReason.AGENT_INSTANCE_END
    assert runs[1].agent_instance_id == INSTANCE + 1
    assert runs[1].end_boundary.reason is BoundaryReason.CAPTURE_END
    assert all(run.partial_end for run in runs)


def test_tool_changes_do_not_split_machine_runs():
    runs = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="READY"),
            _observation(sequence=2, second=5, value="ACTIVE"),
            _observation(
                sequence=3,
                second=10,
                value="1",
                mtconnect_type="TOOL_NUMBER",
            ),
            _observation(
                sequence=4,
                second=15,
                value="2",
                mtconnect_type="TOOL_NUMBER",
            ),
            _observation(sequence=5, second=20, value="READY"),
        ]
    )

    assert len(runs) == 1
    assert runs[0].wall_duration_us == 15 * SECOND


def test_resume_after_capture_begins_stopped_is_strong_inference_not_observed_start():
    run = build_machine_runs(
        [
            _observation(sequence=1, second=0, value="PROGRAM_STOPPED"),
            _observation(sequence=2, second=10, value="ACTIVE"),
            _observation(sequence=3, second=20, value="READY"),
        ]
    )[0]

    assert run.start_boundary.reason is BoundaryReason.EXECUTION_ACTIVE
    assert run.start_boundary.confidence is BoundaryConfidence.STRONG_INFERENCE
    assert run.partial_start is True


def test_machine_run_identity_and_fingerprint_are_deterministic_and_device_scoped():
    rows = [
        _observation(sequence=1, second=0, value="READY"),
        _observation(sequence=2, second=10, value="ACTIVE"),
        _observation(sequence=3, second=20, value="READY"),
    ]
    first = build_machine_runs(rows)[0]
    rebuilt = build_machine_runs(list(reversed(rows)))[0]

    assert rebuilt == first
    assert first.run_id.startswith("machine-run:")
    assert len(first.run_id.removeprefix("machine-run:")) == 64
    assert len(first.fingerprint) == 64

    other_device = build_machine_runs(
        [
            _observation(
                sequence=1,
                second=0,
                value="READY",
                device_uuid="device-beta",
                machine_id="machine-beta",
            ),
            _observation(
                sequence=2,
                second=10,
                value="ACTIVE",
                device_uuid="device-beta",
                machine_id="machine-beta",
            ),
            _observation(
                sequence=3,
                second=20,
                value="READY",
                device_uuid="device-beta",
                machine_id="machine-beta",
            ),
        ]
    )[0]
    assert other_device.run_id != first.run_id


def test_unusable_execution_timeline_never_invents_a_machine_run():
    assert (
        build_machine_runs(
            [
                _observation(
                    sequence=1,
                    second=0,
                    value="O1234",
                    mtconnect_type="PROGRAM",
                )
            ]
        )
        == ()
    )

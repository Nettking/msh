"""S2 sequence continuity and device partitioning for MTConnect evidence."""

from __future__ import annotations

from copy import deepcopy

import pytest

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.timeline import (
    AgentStreamError,
    AgentStreamReport,
    build_agent_stream_reports,
    partition_agent_stream,
    partition_agent_streams,
)

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _observation(
    *,
    sequence: int,
    source: str = SOURCE,
    instance: int = INSTANCE,
    device_uuid: str | None = "device-alpha",
    machine_id: str = "machine-alpha",
    timestamp: str | None = None,
) -> CanonicalObservation:
    return CanonicalObservation.from_recorder_record(
        {
            "sequence": sequence,
            "machine_id": machine_id,
            "device_uuid": device_uuid,
            "device_name": machine_id,
            "category": CATEGORY_EVENT,
            "observation_type": "Execution",
            "type": "EXECUTION",
            "data_item_id": f"{device_uuid}-execution",
            "stream_component": "Path",
            "stream_component_id": "path-1",
            "stream_component_name": "PATH-1",
            "native_value": "ACTIVE",
            "attributes": {
                "timestamp": timestamp or f"2026-08-07T09:00:{sequence:02d}Z"
            },
        },
        source_name=source,
        agent_instance_id=instance,
        raw_batch_sha256="a" * 64,
    )


def test_agent_stream_is_ordered_by_sequence_not_input_order():
    rows = [
        _observation(sequence=3),
        _observation(sequence=1),
        _observation(sequence=2),
    ]

    report = build_agent_stream_reports(rows)[0]

    assert [item.sequence for item in report.observations] == [1, 2, 3]
    assert report.is_sequence_continuous is True
    assert report.sequence_discontinuities == ()


def test_interleaved_devices_do_not_create_false_sequence_gaps():
    rows = [
        _observation(sequence=1, device_uuid="device-a", machine_id="machine-a"),
        _observation(sequence=2, device_uuid="device-b", machine_id="machine-b"),
        _observation(sequence=3, device_uuid="device-a", machine_id="machine-a"),
        _observation(sequence=4, device_uuid="device-b", machine_id="machine-b"),
    ]

    report = build_agent_stream_reports(rows)[0]

    assert [item.sequence for item in report.observations] == [1, 2, 3, 4]
    assert report.is_sequence_continuous is True
    assert report.sequence_discontinuities == ()


def test_real_agent_sequence_gap_is_reported_once():
    rows = [
        _observation(sequence=1, device_uuid="device-a", machine_id="machine-a"),
        _observation(sequence=2, device_uuid="device-b", machine_id="machine-b"),
        _observation(sequence=4, device_uuid="device-a", machine_id="machine-a"),
        _observation(sequence=5, device_uuid="device-b", machine_id="machine-b"),
    ]

    report = build_agent_stream_reports(rows)[0]

    assert report.is_sequence_continuous is False
    assert len(report.sequence_discontinuities) == 1
    gap = report.sequence_discontinuities[0]
    assert gap.source_key == rows[0].source_key
    assert gap.agent_instance_id == INSTANCE
    assert gap.previous_sequence == 2
    assert gap.next_sequence == 4
    assert gap.missing_start_sequence == 3
    assert gap.missing_end_sequence == 3
    assert gap.missing_count == 1


def test_multi_observation_gap_keeps_exact_missing_range():
    report = build_agent_stream_reports(
        [_observation(sequence=10), _observation(sequence=14)]
    )[0]

    gap = report.sequence_discontinuities[0]
    assert (gap.missing_start_sequence, gap.missing_end_sequence, gap.missing_count) == (
        11,
        13,
        3,
    )


def test_agent_instances_have_independent_sequence_spaces():
    rows = [
        _observation(sequence=2, instance=INSTANCE + 1),
        _observation(sequence=1, instance=INSTANCE),
        _observation(sequence=1, instance=INSTANCE + 1),
        _observation(sequence=2, instance=INSTANCE),
    ]

    reports = build_agent_stream_reports(rows)

    assert [(item.agent_instance_id, item.is_sequence_continuous) for item in reports] == [
        (INSTANCE, True),
        (INSTANCE + 1, True),
    ]
    assert [[row.sequence for row in item.observations] for item in reports] == [
        [1, 2],
        [1, 2],
    ]


def test_sources_have_independent_sequence_spaces():
    reports = build_agent_stream_reports(
        [
            _observation(sequence=1, source="Machine A"),
            _observation(sequence=2, source="Machine A"),
            _observation(sequence=1, source="Machine B"),
            _observation(sequence=2, source="Machine B"),
        ]
    )

    assert len(reports) == 2
    assert all(report.is_sequence_continuous for report in reports)
    assert [[row.sequence for row in report.observations] for report in reports] == [
        [1, 2],
        [1, 2],
    ]


def test_timestamp_regression_never_reorders_or_creates_a_sequence_gap():
    rows = [
        _observation(sequence=1, timestamp="2026-08-07T09:00:10Z"),
        _observation(sequence=2, timestamp="2026-08-07T09:00:05Z"),
    ]

    report = build_agent_stream_reports(rows)[0]

    assert [item.sequence for item in report.observations] == [1, 2]
    assert report.observations[1].observed_at_us < report.observations[0].observed_at_us
    assert report.is_sequence_continuous is True


def test_duplicate_sequence_in_one_agent_scope_is_rejected():
    with pytest.raises(AgentStreamError, match="duplicate canonical sequence"):
        build_agent_stream_reports(
            [
                _observation(sequence=1, device_uuid="device-a"),
                _observation(sequence=1, device_uuid="device-b"),
            ]
        )


def test_sequence_preparation_does_not_mutate_canonical_evidence():
    rows = [_observation(sequence=2), _observation(sequence=1)]
    snapshot = deepcopy(rows)

    build_agent_stream_reports(rows)

    assert rows == snapshot


# ---------------------------------------------------------------------------
# S2.2: device partitioning happens only after Agent-wide gap detection.
# ---------------------------------------------------------------------------


def test_checked_agent_stream_partitions_interleaved_devices_without_local_gaps():
    report = build_agent_stream_reports(
        [
            _observation(sequence=1, device_uuid="device-a", machine_id="machine-a"),
            _observation(sequence=2, device_uuid="device-b", machine_id="machine-b"),
            _observation(sequence=3, device_uuid="device-a", machine_id="machine-a"),
            _observation(sequence=4, device_uuid="device-b", machine_id="machine-b"),
            _observation(sequence=5, device_uuid="device-a", machine_id="machine-a"),
        ]
    )[0]

    partitions = partition_agent_stream(report)
    by_device = {item.device_key: item for item in partitions}

    assert [row.sequence for row in by_device["uuid:device-a"].observations] == [
        1,
        3,
        5,
    ]
    assert [row.sequence for row in by_device["uuid:device-b"].observations] == [2, 4]
    assert all(not item.agent_sequence_discontinuities for item in partitions)


def test_device_partition_uses_the_same_namespaced_identity_as_s1():
    report = build_agent_stream_reports(
        [
            _observation(sequence=1, device_uuid="same", machine_id="machine-a"),
            _observation(sequence=2, device_uuid=None, machine_id="same"),
        ]
    )[0]

    assert [item.device_key for item in partition_agent_stream(report)] == [
        "machine:same",
        "uuid:same",
    ]


def test_real_agent_gap_is_carried_to_every_device_partition_as_agent_evidence():
    report = build_agent_stream_reports(
        [
            _observation(sequence=1, device_uuid="device-a", machine_id="machine-a"),
            _observation(sequence=2, device_uuid="device-b", machine_id="machine-b"),
            _observation(sequence=4, device_uuid="device-a", machine_id="machine-a"),
            _observation(sequence=5, device_uuid="device-b", machine_id="machine-b"),
        ]
    )[0]

    partitions = partition_agent_stream(report)

    assert len(report.sequence_discontinuities) == 1
    assert len(partitions) == 2
    assert all(
        item.agent_sequence_discontinuities == report.sequence_discontinuities
        for item in partitions
    )
    assert report.sequence_discontinuities[0].missing_start_sequence == 3


def test_partitioning_multiple_agent_reports_keeps_agent_scopes_separate():
    reports = build_agent_stream_reports(
        [
            _observation(sequence=1, instance=INSTANCE, device_uuid="device-a"),
            _observation(sequence=1, instance=INSTANCE + 1, device_uuid="device-a"),
        ]
    )

    partitions = partition_agent_streams(reports)

    assert [(item.agent_instance_id, item.device_key) for item in partitions] == [
        (INSTANCE, "uuid:device-a"),
        (INSTANCE + 1, "uuid:device-a"),
    ]


def test_partition_rejects_an_observation_outside_the_report_scope():
    valid = _observation(sequence=1)
    foreign = _observation(sequence=2, instance=INSTANCE + 1)
    invalid_report = AgentStreamReport(
        source_key=valid.source_key,
        agent_instance_id=valid.agent_instance_id,
        observations=(valid, foreign),
        sequence_discontinuities=(),
    )

    with pytest.raises(AgentStreamError, match="outside AgentStreamReport scope"):
        partition_agent_stream(invalid_report)


def test_partition_rejects_a_manually_unsorted_agent_report():
    invalid_report = AgentStreamReport(
        source_key=_observation(sequence=1).source_key,
        agent_instance_id=INSTANCE,
        observations=(_observation(sequence=2), _observation(sequence=1)),
        sequence_discontinuities=(),
    )

    with pytest.raises(AgentStreamError, match="not strictly sequence ordered"):
        partition_agent_stream(invalid_report)


def test_device_partitioning_does_not_mutate_canonical_evidence():
    rows = [
        _observation(sequence=1, device_uuid="device-a"),
        _observation(sequence=2, device_uuid="device-b"),
    ]
    snapshot = deepcopy(rows)

    partition_agent_stream(build_agent_stream_reports(rows)[0])

    assert rows == snapshot

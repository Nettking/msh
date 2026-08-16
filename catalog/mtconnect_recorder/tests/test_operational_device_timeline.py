"""S2 device timeline reconstruction over canonical MTConnect evidence."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.timeline import (
    TimelineStatus,
    build_device_timeline_reports,
)
from catalog.mtconnect_recorder.operational.roles import SemanticRole

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _observation(
    *,
    sequence: int,
    value: str,
    mtconnect_type: str = "EXECUTION",
    observation_type: str | None = None,
    data_item_id: str | None = None,
    device_uuid: str = "device-alpha",
    machine_id: str = "machine-alpha",
    component_type: str = "Path",
    component_id: str = "path-1",
    component_name: str = "PATH-1",
    sub_type: str | None = None,
    timestamp: str | None = None,
    instance: int = INSTANCE,
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": machine_id,
        "device_uuid": device_uuid,
        "device_name": machine_id,
        "category": CATEGORY_EVENT,
        "observation_type": observation_type
        or mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "sub_type": sub_type,
        "data_item_id": data_item_id
        or f"{device_uuid}-{component_id}-{mtconnect_type.lower()}",
        "stream_component": component_type,
        "stream_component_id": component_id,
        "stream_component_name": component_name,
        "native_value": value,
        "attributes": {
            "timestamp": timestamp or f"2026-08-07T09:00:{sequence:02d}Z"
        },
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=instance,
        raw_batch_sha256="a" * 64,
    )


def _single(rows):
    reports = build_device_timeline_reports(rows)
    assert len(reports) == 1
    return reports[0]


def test_execution_and_context_are_reconstructed_in_sequence_order():
    report = _single(
        [
            _observation(sequence=1, value="READY"),
            _observation(sequence=2, value="O1234", mtconnect_type="PROGRAM"),
            _observation(sequence=3, value="12", mtconnect_type="TOOL_NUMBER"),
            _observation(sequence=4, value="ACTIVE"),
            _observation(sequence=5, value="PROGRAM_STOPPED"),
            _observation(sequence=6, value="ACTIVE"),
            _observation(sequence=7, value="READY"),
        ]
    )

    assert report.status is TimelineStatus.READY
    assert [span.state for span in report.execution_spans] == [
        "READY",
        "ACTIVE",
        "PROGRAM_STOPPED",
        "ACTIVE",
        "READY",
    ]
    assert [
        (transition.role, transition.new_value)
        for transition in report.context_transitions
    ] == [
        (SemanticRole.PROGRAM, "O1234"),
        (SemanticRole.TOOL_NUMBER, 12.0),
    ]
    assert report.final_context is not None
    assert report.final_context.value(SemanticRole.PROGRAM).value == "O1234"
    assert report.final_context.value(SemanticRole.TOOL_NUMBER).value == 12.0


def test_interleaved_devices_keep_execution_and_context_fully_independent():
    reports = build_device_timeline_reports(
        [
            _observation(
                sequence=1,
                value="ACTIVE",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _observation(
                sequence=2,
                value="READY",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _observation(
                sequence=3,
                value="O-A",
                mtconnect_type="PROGRAM",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _observation(
                sequence=4,
                value="O-B",
                mtconnect_type="PROGRAM",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _observation(
                sequence=5,
                value="READY",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _observation(
                sequence=6,
                value="ACTIVE",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
        ]
    )
    by_device = {report.device_key: report for report in reports}

    assert [span.state for span in by_device["uuid:device-a"].execution_spans] == [
        "ACTIVE",
        "READY",
    ]
    assert [span.state for span in by_device["uuid:device-b"].execution_spans] == [
        "READY",
        "ACTIVE",
    ]
    assert (
        by_device["uuid:device-a"].final_context.value(SemanticRole.PROGRAM).value
        == "O-A"
    )
    assert (
        by_device["uuid:device-b"].final_context.value(SemanticRole.PROGRAM).value
        == "O-B"
    )


def test_real_agent_gap_closes_execution_and_clears_inherited_context():
    report = _single(
        [
            _observation(sequence=1, value="O1234", mtconnect_type="PROGRAM"),
            _observation(sequence=2, value="ACTIVE"),
            _observation(sequence=4, value="ACTIVE"),
        ]
    )

    assert len(report.agent_sequence_discontinuities) == 1
    assert len(report.execution_spans) == 2
    assert report.execution_spans[0].end_reason == "SEQUENCE_GAP"
    assert report.execution_spans[0].partial_end is True
    assert report.execution_spans[1].start_reason == "POST_GAP_EXECUTION"
    assert report.execution_spans[1].partial_start is True

    assert report.final_context is not None
    assert report.final_context.value(SemanticRole.PROGRAM) is None
    assert SemanticRole.PROGRAM in report.final_context.pending_roles


def test_context_is_reestablished_role_by_role_after_a_gap():
    report = _single(
        [
            _observation(sequence=1, value="O1", mtconnect_type="PROGRAM"),
            _observation(sequence=2, value="1", mtconnect_type="TOOL_NUMBER"),
            _observation(sequence=4, value="O2", mtconnect_type="PROGRAM"),
            _observation(sequence=5, value="2", mtconnect_type="TOOL_NUMBER"),
        ]
    )

    assert report.final_context is not None
    assert report.final_context.is_partial is False
    assert report.final_context.value(SemanticRole.PROGRAM).value == "O2"
    assert report.final_context.value(SemanticRole.TOOL_NUMBER).value == 2.0
    post_gap = [
        item for item in report.context_transitions if item.sequence >= 4
    ]
    assert all(item.establishes_context for item in post_gap)


def test_large_positive_timestamp_jump_does_not_reset_context():
    report = _single(
        [
            _observation(
                sequence=1,
                value="O1",
                mtconnect_type="PROGRAM",
                timestamp="2026-08-07T09:00:00Z",
            ),
            _observation(
                sequence=2,
                value="ACTIVE",
                timestamp="2026-08-07T10:00:00Z",
            ),
        ]
    )

    assert report.timestamp_discontinuities == ()
    assert report.final_context.value(SemanticRole.PROGRAM).value == "O1"


def test_timestamp_regression_is_explicit_and_splits_time_accountable_state():
    report = _single(
        [
            _observation(
                sequence=1,
                value="ACTIVE",
                timestamp="2026-08-07T09:00:10Z",
            ),
            _observation(
                sequence=2,
                value="O1",
                mtconnect_type="PROGRAM",
                timestamp="2026-08-07T09:00:11Z",
            ),
            _observation(
                sequence=3,
                value="10",
                mtconnect_type="LINE",
                timestamp="2026-08-07T09:00:05Z",
            ),
            _observation(
                sequence=4,
                value="READY",
                timestamp="2026-08-07T09:00:06Z",
            ),
        ]
    )

    assert len(report.timestamp_discontinuities) == 1
    discontinuity = report.timestamp_discontinuities[0]
    assert (discontinuity.previous_sequence, discontinuity.next_sequence) == (2, 3)
    assert [span.state for span in report.execution_spans] == [
        "ACTIVE",
        "ACTIVE",
        "READY",
    ]
    assert report.execution_spans[0].end_reason == "TIMESTAMP_REGRESSION"
    assert report.execution_spans[1].start_reason == "TIMESTAMP_REGRESSION"
    assert all(
        span.end_observed_at_us >= span.start_observed_at_us
        for span in report.execution_spans
    )


def test_capture_beginning_active_is_explicitly_partial():
    report = _single(
        [
            _observation(sequence=1, value="ACTIVE"),
            _observation(sequence=2, value="READY"),
        ]
    )

    first = report.execution_spans[0]
    assert first.state == "ACTIVE"
    assert first.start_reason == "CAPTURE_BEGINS_WITH_EXECUTION"
    assert first.partial_start is True


def test_execution_unavailable_surfaces_without_inventing_spans():
    report = _single(
        [_observation(sequence=1, value="O1", mtconnect_type="PROGRAM")]
    )

    assert report.status is TimelineStatus.EXECUTION_UNAVAILABLE
    assert report.execution_is_usable is False
    assert report.execution_spans == ()
    assert report.final_context.value(SemanticRole.PROGRAM).value == "O1"


def test_execution_ambiguity_fails_closed():
    report = _single(
        [
            _observation(
                sequence=1,
                value="ACTIVE",
                sub_type="ACTIVE",
                data_item_id="execution-active",
            ),
            _observation(
                sequence=2,
                value="READY",
                sub_type="MAIN",
                data_item_id="execution-main",
            ),
        ]
    )

    assert report.status is TimelineStatus.EXECUTION_AMBIGUOUS
    assert report.execution_spans == ()


def test_multi_path_execution_fails_closed_in_v1():
    report = _single(
        [
            _observation(
                sequence=1,
                value="ACTIVE",
                component_id="path-1",
                component_name="PATH-1",
                data_item_id="path-1-exec",
            ),
            _observation(
                sequence=2,
                value="READY",
                component_id="path-2",
                component_name="PATH-2",
                data_item_id="path-2-exec",
            ),
        ]
    )

    assert report.status is TimelineStatus.EXECUTION_MULTI_CHANNEL
    assert report.execution_spans == ()


def test_multiple_compatible_execution_items_on_one_component_are_all_consumed():
    report = _single(
        [
            _observation(
                sequence=1,
                value="READY",
                data_item_id="exec-primary",
            ),
            _observation(
                sequence=2,
                value="ACTIVE",
                data_item_id="exec-mirror",
            ),
            _observation(
                sequence=3,
                value="ACTIVE",
                data_item_id="exec-primary",
            ),
            _observation(
                sequence=4,
                value="READY",
                data_item_id="exec-mirror",
            ),
        ]
    )

    assert report.status is TimelineStatus.READY
    assert [span.state for span in report.execution_spans] == [
        "READY",
        "ACTIVE",
        "READY",
    ]


def test_unavailable_tool_value_clears_context_without_becoming_a_concrete_tool():
    report = _single(
        [
            _observation(sequence=1, value="12", mtconnect_type="TOOL_NUMBER"),
            _observation(
                sequence=2,
                value="UNAVAILABLE",
                mtconnect_type="TOOL_NUMBER",
            ),
        ]
    )

    transitions = report.context_transitions
    assert transitions[0].new_value == 12.0
    assert transitions[1].previous_value == 12.0
    assert transitions[1].new_value is None
    tool = report.final_context.value(SemanticRole.TOOL_NUMBER)
    assert tool.available is False
    assert tool.value is None


def test_same_timestamp_preserves_sequence_authority():
    stamp = "2026-08-07T09:00:00Z"
    report = _single(
        [
            _observation(sequence=1, value="READY", timestamp=stamp),
            _observation(sequence=2, value="ACTIVE", timestamp=stamp),
            _observation(sequence=3, value="READY", timestamp=stamp),
        ]
    )

    assert [span.state for span in report.execution_spans] == [
        "READY",
        "ACTIVE",
        "READY",
    ]
    assert report.timestamp_discontinuities == ()


def test_agent_instances_never_share_device_timeline_state():
    reports = build_device_timeline_reports(
        [
            _observation(sequence=1, value="ACTIVE", instance=INSTANCE),
            _observation(sequence=1, value="READY", instance=INSTANCE + 1),
        ]
    )

    observed = [
        (item.agent_instance_id, item.execution_spans[0].state)
        for item in reports
    ]
    assert observed == [
        (INSTANCE, "ACTIVE"),
        (INSTANCE + 1, "READY"),
    ]

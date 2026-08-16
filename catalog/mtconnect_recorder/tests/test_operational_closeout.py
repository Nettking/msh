"""S7 end-to-end acceptance matrix for the v1 operational segmentation track.

These fixtures are intentionally small and anonymised. They exercise the
complete S2→S6 chain rather than one helper in isolation and correspond one for
one with the eleven regression scenarios required by the authoritative plan.
"""

from __future__ import annotations

import zipfile
from datetime import datetime, timedelta, timezone

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.cycles import ProductionClassification
from catalog.mtconnect_recorder.operational.model import BoundaryReason
from catalog.mtconnect_recorder.operational.projection import (
    build_agent_segmentation_bundle,
    rebuild_operational_segmentation,
)
from catalog.mtconnect_recorder.operational.store import OperationalSegmentationStore
from catalog.mtconnect_recorder.operational.validation import (
    build_validation_report,
    render_validation_report,
    validate_reference_capture_zip,
)

from .conftest import Observation, streams_document

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233
BASE_TIME = datetime(2026, 8, 7, 9, 0, 0, tzinfo=timezone.utc)


def _stamp(offset_seconds: float) -> str:
    return (BASE_TIME + timedelta(seconds=offset_seconds)).isoformat().replace(
        "+00:00", "Z"
    )


def _observation(
    sequence: int,
    value: str,
    mtconnect_type: str,
    *,
    offset_seconds: float | None = None,
    category: str = CATEGORY_EVENT,
    device_uuid: str = "device-alpha",
    machine_id: str = "machine-alpha",
    component_type: str = "Path",
    component_id: str = "path-1",
    component_name: str = "PATH-1",
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": machine_id,
        "device_uuid": device_uuid,
        "device_name": machine_id,
        "category": category,
        "observation_type": mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "sub_type": None,
        "data_item_id": f"{device_uuid}-{component_id}-{mtconnect_type.lower()}",
        "stream_component": component_type,
        "stream_component_id": component_id,
        "stream_component_name": component_name,
        "native_value": value,
        "attributes": {
            "timestamp": _stamp(
                float(sequence) if offset_seconds is None else offset_seconds
            )
        },
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=INSTANCE,
        raw_batch_sha256="a" * 64,
    )


def _execution(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "EXECUTION", **kwargs)


def _program(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "PROGRAM", **kwargs)


def _comment(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "PROGRAM_COMMENT", **kwargs)


def _line(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "LINE", **kwargs)


def _tool(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "TOOL_NUMBER", **kwargs)


def _pallet(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence, value, "PALLET_ID", **kwargs)


def _feed(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence,
        value,
        "PATH_FEEDRATE",
        category=CATEGORY_SAMPLE,
        **kwargs,
    )


def _production_prefix() -> list[CanonicalObservation]:
    return [
        _program(1, "O1000"),
        _tool(2, "72"),
        _execution(3, "ACTIVE"),
        _feed(4, "500"),
    ]


def test_s7_01_program_stopped_periods_remain_inside_one_machine_run():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000", offset_seconds=0),
            _tool(2, "72", offset_seconds=1),
            _execution(3, "ACTIVE", offset_seconds=2),
            _feed(4, "500", offset_seconds=3),
            _execution(5, "PROGRAM_STOPPED", offset_seconds=10),
            _execution(6, "ACTIVE", offset_seconds=20),
            _execution(7, "PROGRAM_STOPPED", offset_seconds=25),
            _execution(8, "ACTIVE", offset_seconds=30),
            _execution(9, "READY", offset_seconds=40),
        ]
    )

    assert len(bundle.runs) == 1
    assert bundle.runs[0].program_stopped_duration_us == 15_000_000
    assert bundle.runs[0].duration_buckets_reconcile is True


def test_s7_02_short_active_without_process_motion_is_non_production():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _feed(4, "0"),
            _execution(5, "READY"),
        ]
    )

    assert len(bundle.cycles) == 1
    assert bundle.cycles[0].classification is ProductionClassification.NON_PRODUCTION


def test_s7_03_pallet_transition_with_post_motion_creates_strong_cycle_boundary():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000"),
            _tool(2, "72"),
            _pallet(3, "A"),
            _execution(4, "ACTIVE"),
            _feed(5, "500"),
            _pallet(6, "B"),
            _feed(7, "700"),
            _execution(8, "READY"),
        ]
    )

    assert len(bundle.cycles) == 2
    assert bundle.cycles[0].end_boundary.reason is BoundaryReason.PALLET_CONTEXT_CHANGE
    assert bundle.cycles[1].start_boundary.reason is BoundaryReason.PALLET_CONTEXT_CHANGE
    assert bundle.cycles[1].start_boundary.confidence.value == "STRONG_INFERENCE"


def test_s7_04_repeated_concrete_tools_create_one_episode_per_tenure():
    bundle = build_agent_segmentation_bundle(
        [
            *_production_prefix(),
            _tool(5, "155"),
            _tool(6, "108"),
            _tool(7, "91"),
            _tool(8, "60"),
            _execution(9, "READY"),
        ]
    )

    assert [item.entry_context.tool_number for item in bundle.episodes] == [
        72.0,
        155.0,
        108.0,
        91.0,
        60.0,
    ]


def test_s7_05_long_interruption_remains_inside_one_tool_episode():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000", offset_seconds=0),
            _tool(2, "72", offset_seconds=1),
            _execution(3, "ACTIVE", offset_seconds=2),
            _feed(4, "500", offset_seconds=3),
            _execution(5, "PROGRAM_STOPPED", offset_seconds=10),
            _line(6, "100", offset_seconds=120),
            _execution(7, "ACTIVE", offset_seconds=610),
            _execution(8, "READY", offset_seconds=620),
        ]
    )

    assert len(bundle.episodes) == 1
    episode = bundle.episodes[0]
    assert episode.program_stopped_duration_us == 600_000_000
    assert episode.duration_buckets_reconcile is True


def test_s7_06_nested_program_and_line_churn_is_context_history_only():
    bundle = build_agent_segmentation_bundle(
        [
            *_production_prefix(),
            _program(5, "O9001"),
            _line(6, "10"),
            _comment(7, "nested macro"),
            _program(8, "O9002"),
            _line(9, "20"),
            _program(10, "O1000"),
            _execution(11, "READY"),
        ]
    )

    assert len(bundle.episodes) == 1
    roles = [item.role.value for item in bundle.episodes[0].context_transitions]
    assert roles == ["PROGRAM", "LINE", "PROGRAM_COMMENT", "PROGRAM", "LINE", "PROGRAM"]


def test_s7_07_agent_sequence_gap_closes_run_and_resets_context():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _feed(4, "500"),
            # Sequence 5 is genuinely absent from the complete Agent stream.
            _execution(6, "ACTIVE"),
            _feed(7, "600"),
            _execution(8, "READY"),
        ]
    )

    assert len(bundle.reports) == 1
    assert len(bundle.reports[0].agent_sequence_discontinuities) == 1
    assert any(run.end_boundary.reason is BoundaryReason.SEQUENCE_GAP for run in bundle.runs)
    post_gap = max(bundle.episodes, key=lambda item: item.first_sequence)
    assert post_gap.entry_context.tool_number is None
    assert post_gap.tool_context_partial is True


def test_s7_08_equivalent_canonical_input_rebuilds_to_identical_content(tmp_path):
    rows = [
        *_production_prefix(),
        _tool(5, "155"),
        _execution(6, "READY"),
    ]
    first = OperationalSegmentationStore(tmp_path / "first.sqlite3")
    second = OperationalSegmentationStore(tmp_path / "second.sqlite3")

    a = rebuild_operational_segmentation(rows, first)
    b = rebuild_operational_segmentation(reversed(rows), second)

    assert a.content_fingerprint == b.content_fingerprint
    assert first.content_fingerprint() == second.content_fingerprint()


def test_s7_09_interleaved_devices_share_agent_sequence_but_not_state():
    a = {"device_uuid": "device-a", "machine_id": "machine-a"}
    b = {"device_uuid": "device-b", "machine_id": "machine-b"}
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "OA", **a),
            _tool(2, "10", **a),
            _execution(3, "ACTIVE", **a),
            _program(4, "OB", **b),
            _tool(5, "20", **b),
            _execution(6, "ACTIVE", **b),
            _feed(7, "500", **a),
            _feed(8, "600", **b),
            _tool(9, "21", **b),
            _execution(10, "READY", **b),
            _execution(11, "READY", **a),
        ]
    )

    assert len(bundle.runs) == 2
    assert {item.device_key for item in bundle.runs} == {
        "uuid:device-a",
        "uuid:device-b",
    }
    episodes_a = [item for item in bundle.episodes if item.device_key == "uuid:device-a"]
    episodes_b = [item for item in bundle.episodes if item.device_key == "uuid:device-b"]
    assert len(episodes_a) == 1
    assert len(episodes_b) == 2


def test_s7_10_unknown_tool_context_does_not_fabricate_change_boundary():
    bundle = build_agent_segmentation_bundle(
        [
            _program(1, "O1000"),
            _tool(2, "UNAVAILABLE"),
            _execution(3, "ACTIVE"),
            _feed(4, "500"),
            _tool(5, "72"),
            _tool(6, "UNAVAILABLE"),
            _tool(7, "155"),
            _execution(8, "READY"),
        ]
    )

    assert len(bundle.episodes) == 1
    assert bundle.episodes[0].tool_context_partial is True
    assert bundle.episodes[0].start_boundary.reason.value == "CYCLE_START"
    assert bundle.episodes[0].end_boundary.reason.value == "CYCLE_END"


def test_s7_11_timestamp_regression_is_warning_not_negative_duration(tmp_path):
    rows = [
        _program(1, "O1000", offset_seconds=0),
        _tool(2, "72", offset_seconds=1),
        _execution(3, "ACTIVE", offset_seconds=10),
        _feed(4, "500", offset_seconds=11),
        _execution(5, "PROGRAM_STOPPED", offset_seconds=5),
        _execution(6, "ACTIVE", offset_seconds=12),
        _execution(7, "READY", offset_seconds=20),
    ]
    store = OperationalSegmentationStore(tmp_path / "segmentation.sqlite3")
    rebuild_operational_segmentation(rows, store)
    report = build_validation_report(store)

    assert all(item.wall_duration_us >= 0 for item in store.runs())
    assert all(
        item.active_duration_us
        + item.program_stopped_duration_us
        + item.feed_hold_duration_us
        + item.other_execution_duration_us
        + item.unknown_duration_us
        == item.wall_duration_us
        for item in store.runs()
    )
    warnings = report["projections"][0]["devices"][0]["warnings"]
    assert any(item["code"] == "TIMESTAMP_REGRESSION" for item in warnings)


def test_s7_validation_report_is_deterministic_and_contains_required_inspection_fields(
    tmp_path,
):
    store = OperationalSegmentationStore(tmp_path / "segmentation.sqlite3")
    rebuild_operational_segmentation(
        [
            _program(1, "O1000"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _feed(4, "500"),
            _execution(5, "PROGRAM_STOPPED"),
            _execution(6, "ACTIVE"),
            _tool(7, "155"),
            _execution(8, "READY"),
        ],
        store,
    )

    first = build_validation_report(store)
    second = build_validation_report(store)
    assert first == second
    device = first["projections"][0]["devices"][0]
    assert device["run_count"] == 1
    assert device["cycle_count"] == 1
    assert device["episode_count"] == 2
    run = device["runs"][0]
    assert {"start_reason", "end_reason", "durations", "boundaries"} <= set(run)
    cycle = run["cycles"][0]
    assert {"classification", "evidence_flags", "episode_count", "boundaries"} <= set(
        cycle
    )
    episode = cycle["episodes"][0]
    assert {"entry_context", "context_transitions", "durations"} <= set(episode)
    rendered = render_validation_report(first)
    assert "MachineRuns=1" in rendered
    assert "PROGRAM_STOPPED=" in rendered


def test_s7_private_capture_zip_path_reuses_existing_raw_to_canonical_pipeline(
    recorder_archive,
    tmp_path,
):
    observations = [
        Observation(
            sequence=1,
            component="Path",
            component_id="path",
            component_name="path",
            category="Events",
            element="Program",
            data_item_id="pgm",
            value="O1000",
        ),
        Observation(
            sequence=2,
            component="Path",
            component_id="path",
            component_name="path",
            category="Events",
            element="ToolNumber",
            data_item_id="tid",
            value="72",
        ),
        Observation(
            sequence=3,
            component="Path",
            component_id="path",
            component_name="path",
            category="Events",
            element="Execution",
            data_item_id="exec",
            value="ACTIVE",
        ),
        Observation(
            sequence=4,
            component="Path",
            component_id="path",
            component_name="path",
            element="PathFeedrate",
            data_item_id="pf",
            value="500",
        ),
        Observation(
            sequence=5,
            component="Path",
            component_id="path",
            component_name="path",
            category="Events",
            element="ToolNumber",
            data_item_id="tid",
            value="155",
        ),
        Observation(
            sequence=6,
            component="Path",
            component_id="path",
            component_name="path",
            category="Events",
            element="Execution",
            data_item_id="exec",
            value="READY",
        ),
    ]
    xml_text = streams_document(instance_id=INSTANCE, observations=observations)
    manifest = recorder_archive.write_batch(source_name=SOURCE, xml_text=xml_text)
    raw = recorder_archive.raw_payload_path(manifest)
    recorder_archive.rewrite_manifest(
        manifest,
        raw_file="/offline/private/capture/relocated.xml.gz",
    )

    capture_zip = tmp_path / "reference.zip"
    with zipfile.ZipFile(capture_zip, "w") as archive:
        archive.write(manifest, arcname=f"2026-08-07/{manifest.name}")
        archive.write(raw, arcname=f"2026-08-07/{raw.name}")

    report, canonical_reports = validate_reference_capture_zip(
        capture_zip,
        workspace=tmp_path / "validation-workspace",
    )

    assert canonical_reports
    assert all(item.ok for item in canonical_reports)
    projection = report["projections"][0]
    assert projection["observation_count"] == 6
    assert projection["run_count"] == 1
    assert projection["cycle_count"] == 1
    assert projection["episode_count"] == 2

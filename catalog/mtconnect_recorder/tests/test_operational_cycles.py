"""S4 ProductionCycle inference over S2/S3 operational evidence."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.cycles import (
    ProductionClassification,
    ProductionEvidenceFlag,
    ProductionReasonCode,
    build_production_cycles,
)
from catalog.mtconnect_recorder.operational.model import (
    BoundaryConfidence,
    BoundaryReason,
)

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _observation(
    *,
    sequence: int,
    value: str,
    mtconnect_type: str,
    category: str = CATEGORY_EVENT,
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
        "category": category,
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


def _execution(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="EXECUTION",
        **kwargs,
    )


def _program(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PROGRAM",
        **kwargs,
    )


def _tool(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="TOOL_NUMBER",
        **kwargs,
    )


def _pallet(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PALLET_ID",
        **kwargs,
    )


def _path_feed(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PATH_FEEDRATE",
        category=CATEGORY_SAMPLE,
        **kwargs,
    )


def _spindle(
    sequence: int,
    value: str,
    *,
    component_id: str = "spindle-1",
    component_name: str = "SPINDLE-1",
    **kwargs,
) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="SPINDLE_SPEED",
        category=CATEGORY_SAMPLE,
        component_type="Spindle",
        component_id=component_id,
        component_name=component_name,
        **kwargs,
    )


def _load(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="LOAD",
        category=CATEGORY_SAMPLE,
        component_type="Linear",
        component_id="x-axis",
        component_name="X",
        **kwargs,
    )


def test_motion_plus_program_context_is_production_candidate():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _path_feed(3, "1200"),
            _execution(4, "READY"),
        ]
    )

    assert len(cycles) == 1
    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.PRODUCTION_CANDIDATE
    assert ProductionEvidenceFlag.HAS_PATH_FEED_MOTION in cycle.evidence_flags
    assert ProductionEvidenceFlag.HAS_PROGRAM_CONTEXT in cycle.evidence_flags
    assert cycle.reason_codes == (
        ProductionReasonCode.PROCESS_MOTION_PRESENT,
        ProductionReasonCode.MANUFACTURING_CONTEXT_PRESENT,
    )
    program_witness = next(
        item
        for item in cycle.evidence_witnesses
        if item.flag is ProductionEvidenceFlag.HAS_PROGRAM_CONTEXT
    )
    assert program_witness.sequence == 1
    assert program_witness.inherited is True


def test_available_motion_without_process_motion_is_non_production():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _path_feed(3, "0"),
            _execution(4, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.NON_PRODUCTION
    assert ProductionEvidenceFlag.HAS_PATH_FEED_MOTION not in cycle.evidence_flags
    assert cycle.reason_codes == (
        ProductionReasonCode.AVAILABLE_MOTION_NO_PROCESS_MOTION,
    )


def test_missing_motion_semantics_is_uncertain_not_zero_motion():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _execution(3, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.UNCERTAIN
    assert cycle.available_motion_roles == ()
    assert cycle.reason_codes == (
        ProductionReasonCode.MOTION_SEMANTICS_INSUFFICIENT,
    )


def test_motion_without_manufacturing_context_is_uncertain():
    cycles = build_production_cycles(
        [
            _execution(1, "ACTIVE"),
            _path_feed(2, "800"),
            _execution(3, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.UNCERTAIN
    assert ProductionEvidenceFlag.HAS_PATH_FEED_MOTION in cycle.evidence_flags
    assert cycle.reason_codes == (
        ProductionReasonCode.PROCESS_MOTION_PRESENT,
        ProductionReasonCode.MANUFACTURING_CONTEXT_INSUFFICIENT,
    )


def test_load_telemetry_alone_never_becomes_process_motion():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _load(3, "42"),
            _execution(4, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.UNCERTAIN
    assert ProductionEvidenceFlag.HAS_LOAD_TELEMETRY in cycle.evidence_flags
    assert all(
        flag not in cycle.evidence_flags
        for flag in {
            ProductionEvidenceFlag.HAS_SPINDLE_MOTION,
            ProductionEvidenceFlag.HAS_PATH_FEED_MOTION,
            ProductionEvidenceFlag.HAS_AXIS_FEED_MOTION,
        }
    )


def test_other_device_motion_and_context_never_satisfy_candidate():
    cycles = build_production_cycles(
        [
            _execution(
                1,
                "ACTIVE",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _execution(
                2,
                "ACTIVE",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _program(
                3,
                "O-B",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _path_feed(
                4,
                "1000",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
            _execution(
                5,
                "READY",
                device_uuid="device-a",
                machine_id="machine-a",
            ),
            _execution(
                6,
                "READY",
                device_uuid="device-b",
                machine_id="machine-b",
            ),
        ]
    )
    by_device = {cycle.device_key: cycle for cycle in cycles}

    assert (
        by_device["uuid:device-a"].classification
        is ProductionClassification.UNCERTAIN
    )
    assert (
        by_device["uuid:device-b"].classification
        is ProductionClassification.PRODUCTION_CANDIDATE
    )


def test_pallet_change_with_post_change_production_evidence_splits_cycle():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _pallet(2, "A"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "1000"),
            _pallet(5, "B"),
            _path_feed(6, "900"),
            _execution(7, "READY"),
        ]
    )

    assert len(cycles) == 2
    first, second = cycles
    assert first.machine_run_id == second.machine_run_id
    assert first.end_sequence == 5
    assert second.start_sequence == 5
    assert first.end_boundary.reason is BoundaryReason.PALLET_CONTEXT_CHANGE
    assert second.start_boundary.reason is BoundaryReason.PALLET_CONTEXT_CHANGE
    assert second.start_boundary.confidence is BoundaryConfidence.STRONG_INFERENCE
    assert all(
        item.classification is ProductionClassification.PRODUCTION_CANDIDATE
        for item in cycles
    )


def test_pallet_change_without_post_change_production_evidence_does_not_split():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _pallet(2, "A"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "1000"),
            _pallet(5, "B"),
            _path_feed(6, "0"),
            _execution(7, "READY"),
        ]
    )

    assert len(cycles) == 1
    assert cycles[0].start_sequence == 3
    assert cycles[0].end_sequence == 7
    assert (
        cycles[0].classification
        is ProductionClassification.PRODUCTION_CANDIDATE
    )


def test_nested_program_changes_never_split_production_cycle():
    cycles = build_production_cycles(
        [
            _program(1, "MAIN"),
            _execution(2, "ACTIVE"),
            _path_feed(3, "700"),
            _program(4, "SUB-1"),
            _program(5, "SUB-2"),
            _program(6, "MAIN"),
            _execution(7, "READY"),
        ]
    )

    assert len(cycles) == 1
    assert cycles[0].classification is ProductionClassification.PRODUCTION_CANDIDATE


def test_tool_context_can_satisfy_manufacturing_context():
    cycles = build_production_cycles(
        [
            _tool(1, "12"),
            _execution(2, "ACTIVE"),
            _spindle(3, "5000"),
            _execution(4, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.PRODUCTION_CANDIDATE
    assert ProductionEvidenceFlag.HAS_TOOL_CONTEXT in cycle.evidence_flags
    assert ProductionEvidenceFlag.HAS_SPINDLE_MOTION in cycle.evidence_flags


def test_multi_channel_motion_fans_out_without_selecting_one_channel():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _spindle(
                3,
                "0",
                component_id="spindle-1",
                component_name="SPINDLE-1",
                data_item_id="s1-speed",
            ),
            _spindle(
                4,
                "4500",
                component_id="spindle-2",
                component_name="SPINDLE-2",
                data_item_id="s2-speed",
            ),
            _execution(5, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.PRODUCTION_CANDIDATE
    assert ProductionEvidenceFlag.HAS_SPINDLE_MOTION in cycle.evidence_flags
    witness = next(
        item
        for item in cycle.evidence_witnesses
        if item.flag is ProductionEvidenceFlag.HAS_SPINDLE_MOTION
    )
    assert witness.sequence == 4


def test_ambiguous_motion_semantics_without_positive_coherent_motion_is_uncertain():
    cycles = build_production_cycles(
        [
            _program(1, "O1234"),
            _execution(2, "ACTIVE"),
            _path_feed(
                3,
                "0",
                sub_type="ACTUAL",
                data_item_id="feed-actual",
            ),
            _path_feed(
                4,
                "0",
                sub_type="COMMANDED",
                data_item_id="feed-commanded",
            ),
            _execution(5, "READY"),
        ]
    )

    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.UNCERTAIN
    assert cycle.ambiguous_motion_roles
    assert cycle.reason_codes == (
        ProductionReasonCode.MOTION_SEMANTICS_INSUFFICIENT,
    )


def test_cycle_is_contained_in_machine_run_and_identity_is_deterministic():
    rows = [
        _program(1, "O1234"),
        _execution(2, "ACTIVE"),
        _path_feed(3, "1000"),
        _execution(4, "READY"),
    ]

    first = build_production_cycles(rows)[0]
    second = build_production_cycles(rows)[0]

    assert first.cycle_id == second.cycle_id
    assert first.fingerprint == second.fingerprint
    assert first.start_sequence == 2
    assert first.end_sequence == 4
    assert first.wall_duration_us >= 0
    assert first.start_boundary.device_key == first.device_key
    assert first.end_boundary.device_key == first.device_key

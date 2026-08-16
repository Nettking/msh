"""Additional S4 invariants that guard inference boundaries."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.cycles import (
    ProductionClassification,
    ProductionEvidenceFlag,
    build_production_cycles,
)

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _obs(
    sequence: int,
    value: str,
    mtconnect_type: str,
    *,
    category: str = CATEGORY_EVENT,
    component_type: str = "Path",
    component_id: str = "path-1",
    component_name: str = "PATH-1",
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": "machine-alpha",
        "device_uuid": "device-alpha",
        "device_name": "machine-alpha",
        "category": category,
        "observation_type": mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "data_item_id": f"{component_id}-{mtconnect_type.lower()}",
        "stream_component": component_type,
        "stream_component_id": component_id,
        "stream_component_name": component_name,
        "native_value": value,
        "attributes": {"timestamp": f"2026-08-07T09:01:{sequence:02d}Z"},
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=INSTANCE,
        raw_batch_sha256="b" * 64,
    )


def test_program_churn_is_context_history_not_a_cycle_splitter():
    cycles = build_production_cycles(
        [
            _obs(1, "MAIN", "PROGRAM"),
            _obs(2, "ACTIVE", "EXECUTION"),
            _obs(3, "500", "PATH_FEEDRATE", category=CATEGORY_SAMPLE),
            _obs(4, "SUB-A", "PROGRAM"),
            _obs(5, "SUB-B", "PROGRAM"),
            _obs(6, "MAIN", "PROGRAM"),
            _obs(7, "READY", "EXECUTION"),
        ]
    )

    assert len(cycles) == 1
    assert cycles[0].classification is ProductionClassification.PRODUCTION_CANDIDATE


def test_pallet_context_is_a_valid_manufacturing_context_flag():
    cycles = build_production_cycles(
        [
            _obs(1, "P1", "PALLET_ID"),
            _obs(2, "ACTIVE", "EXECUTION"),
            _obs(3, "600", "PATH_FEEDRATE", category=CATEGORY_SAMPLE),
            _obs(4, "READY", "EXECUTION"),
        ]
    )

    assert len(cycles) == 1
    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.PRODUCTION_CANDIDATE
    assert ProductionEvidenceFlag.HAS_PALLET_CONTEXT in cycle.evidence_flags

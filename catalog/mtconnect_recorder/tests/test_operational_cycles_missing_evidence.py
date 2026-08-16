"""S4 must never reinterpret missing in-run samples as zero motion."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.cycles import (
    ProductionClassification,
    ProductionReasonCode,
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
) -> CanonicalObservation:
    record = {
        "sequence": sequence,
        "machine_id": "machine-alpha",
        "device_uuid": "device-alpha",
        "device_name": "machine-alpha",
        "category": category,
        "observation_type": mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "data_item_id": f"path-1-{mtconnect_type.lower()}",
        "stream_component": "Path",
        "stream_component_id": "path-1",
        "stream_component_name": "PATH-1",
        "native_value": value,
        "attributes": {"timestamp": f"2026-08-07T09:02:{sequence:02d}Z"},
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=INSTANCE,
        raw_batch_sha256="c" * 64,
    )


def test_motion_role_seen_outside_run_does_not_turn_missing_samples_into_zero():
    cycles = build_production_cycles(
        [
            _obs(1, "0", "PATH_FEEDRATE", category=CATEGORY_SAMPLE),
            _obs(2, "O1234", "PROGRAM"),
            _obs(3, "ACTIVE", "EXECUTION"),
            _obs(4, "READY", "EXECUTION"),
        ]
    )

    assert len(cycles) == 1
    cycle = cycles[0]
    assert cycle.classification is ProductionClassification.UNCERTAIN
    assert cycle.reason_codes == (
        ProductionReasonCode.MOTION_EVIDENCE_INSUFFICIENT,
    )

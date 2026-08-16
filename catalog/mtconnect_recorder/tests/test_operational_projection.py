"""S6 durability, convergence, provenance and narrow-query regressions."""

from __future__ import annotations

import sqlite3

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.canonical.store import (
    BATCH_STATUS_PROJECTED,
    CanonicalObservationStore,
    ProjectedBatchRecord,
)
from catalog.mtconnect_recorder.operational.projection import (
    project_agent_instance,
    rebuild_from_canonical_store,
    rebuild_operational_segmentation,
)
from catalog.mtconnect_recorder.operational.store import (
    PROJECTION_STATUS_BLOCKED,
    OperationalSegmentationStore,
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
    timestamp_second: int | None = None,
    instance: int = INSTANCE,
) -> CanonicalObservation:
    second = sequence if timestamp_second is None else timestamp_second
    record = {
        "sequence": sequence,
        "machine_id": machine_id,
        "device_uuid": device_uuid,
        "device_name": machine_id,
        "category": category,
        "observation_type": observation_type
        or mtconnect_type.title().replace("_", ""),
        "type": mtconnect_type,
        "data_item_id": data_item_id
        or f"{device_uuid}-{component_id}-{mtconnect_type.lower()}",
        "stream_component": component_type,
        "stream_component_id": component_id,
        "stream_component_name": component_name,
        "native_value": value,
        "attributes": {"timestamp": f"2026-08-07T09:00:{second:02d}Z"},
    }
    return CanonicalObservation.from_recorder_record(
        record,
        source_name=SOURCE,
        agent_instance_id=instance,
        raw_batch_sha256="a" * 64,
    )


def _execution(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="EXECUTION", **kwargs)


def _program(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="PROGRAM", **kwargs)


def _program_comment(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PROGRAM_COMMENT",
        **kwargs,
    )


def _line(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="LINE", **kwargs)


def _tool(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="TOOL_NUMBER", **kwargs)


def _path_feed(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PATH_FEEDRATE",
        category=CATEGORY_SAMPLE,
        **kwargs,
    )


def _reference_rows(*, instance: int = INSTANCE) -> list[CanonicalObservation]:
    return [
        _program(1, "O1234", instance=instance),
        _tool(2, "72", instance=instance),
        _execution(3, "ACTIVE", instance=instance),
        _path_feed(4, "1200", instance=instance),
        _execution(5, "PROGRAM_STOPPED", instance=instance),
        _line(6, "220", instance=instance),
        _program_comment(7, "nested macro", instance=instance),
        _execution(8, "ACTIVE", instance=instance),
        _tool(9, "155", instance=instance),
        _path_feed(10, "900", instance=instance),
        _execution(11, "READY", instance=instance),
    ]


def _second_device_rows() -> list[CanonicalObservation]:
    common = {
        "device_uuid": "device-beta",
        "machine_id": "machine-beta",
        "component_id": "path-beta",
        "component_name": "PATH-BETA",
    }
    return [
        _program(12, "O9000", **common),
        _tool(13, "8", **common),
        _execution(14, "ACTIVE", **common),
        _path_feed(15, "600", **common),
        _execution(16, "READY", **common),
    ]


def test_rebuild_is_deterministic_idempotent_and_queryable(tmp_path):
    rows = _reference_rows()
    store = OperationalSegmentationStore(tmp_path / "operational_segmentation.sqlite3")

    first = rebuild_operational_segmentation(rows, store)
    second = rebuild_operational_segmentation(rows, store)

    assert first.content_fingerprint == second.content_fingerprint
    assert len(store.projection_runs()) == 1
    assert len(store.runs()) == 1
    assert len(store.cycles()) == 1
    assert len(store.episodes()) == 2

    run = store.runs()[0]
    cycles = store.cycles(machine_run_id=run.segment_id)
    assert len(cycles) == 1
    episodes = store.episodes(production_cycle_id=cycles[0].segment_id)
    assert [item.entry_context["tool_number"] for item in episodes] == [72.0, 155.0]
    assert [item.segment_id for item in store.episodes_for_tool("155")] == [
        episodes[1].segment_id
    ]

    transitions = store.context_transitions_for_episode(episodes[0].segment_id)
    assert {item.role for item in transitions} >= {"LINE", "PROGRAM_COMMENT"}

    boundaries = store.boundaries_for_segment(run.segment_id, segment_type="run")
    assert [item.edge for item in boundaries] == ["start", "end"]
    assert boundaries[0].canonical_identity == (
        rows[0].source_key,
        INSTANCE,
        boundaries[0].evidence_sequence,
    )
    assert boundaries[0].raw_batch_sha256 == "a" * 64
    assert store.latest_run().segment_id == run.segment_id
    assert store.latest_episode().segment_id == episodes[-1].segment_id

    fresh_store = OperationalSegmentationStore(tmp_path / "fresh.sqlite3")
    fresh = rebuild_operational_segmentation(rows, fresh_store)
    assert fresh.content_fingerprint == first.content_fingerprint


def test_incremental_agent_instance_rebuild_converges_to_clean_rebuild(tmp_path):
    first_instance = _reference_rows()
    second_instance = _reference_rows(instance=INSTANCE + 1)
    truncated = first_instance[:8]

    incremental = OperationalSegmentationStore(tmp_path / "incremental.sqlite3")
    rebuild_operational_segmentation([*truncated, *second_instance], incremental)
    project_agent_instance(
        first_instance,
        incremental,
        source_key=first_instance[0].source_key,
        agent_instance_id=INSTANCE,
    )

    clean = OperationalSegmentationStore(tmp_path / "clean.sqlite3")
    rebuild_operational_segmentation([*first_instance, *second_instance], clean)

    assert incremental.content_fingerprint() == clean.content_fingerprint()
    assert len(incremental.projection_runs()) == 2


def test_multi_device_shared_agent_rebuild_is_isolated_and_ordered(tmp_path):
    rows = [*_reference_rows(), *_second_device_rows()]
    store = OperationalSegmentationStore(tmp_path / "multi.sqlite3")
    rebuild_operational_segmentation(rows, store)

    statuses = store.device_statuses()
    assert {item.device_key for item in statuses} == {
        "uuid:device-alpha",
        "uuid:device-beta",
    }
    runs = store.runs()
    assert [item.device_key for item in runs] == [
        "uuid:device-alpha",
        "uuid:device-beta",
    ]
    assert all(
        cycle.device_key
        == next(run.device_key for run in runs if run.segment_id == cycle.machine_run_id)
        for cycle in store.cycles()
    )


def test_blocked_execution_semantics_remain_visible_without_runs(tmp_path):
    rows = [
        _execution(1, "ACTIVE", component_id="path-a", component_name="PATH-A"),
        _execution(2, "READY", component_id="path-b", component_name="PATH-B"),
    ]
    store = OperationalSegmentationStore(tmp_path / "blocked.sqlite3")
    result = rebuild_operational_segmentation(rows, store)

    assert result.projections[0].status == PROJECTION_STATUS_BLOCKED
    assert store.runs() == ()
    statuses = store.device_statuses()
    assert len(statuses) == 1
    assert statuses[0].timeline_status == "EXECUTION_MULTI_CHANNEL"


def test_canonical_store_is_read_only_input_for_s6(tmp_path):
    rows = _reference_rows()
    canonical = CanonicalObservationStore(tmp_path / "canonical.sqlite3")
    canonical.record_batch(
        observations=rows,
        batch=ProjectedBatchRecord(
            source_key=rows[0].source_key,
            raw_sha256="a" * 64,
            source_name=SOURCE,
            agent_instance_id=INSTANCE,
            manifest_path="manifest.json",
            raw_path="batch.xml.gz",
            manifest_schema="msh.mtconnect.raw-batch.v1",
            first_sequence=1,
            last_sequence=11,
            manifest_observation_count=len(rows),
            projected_count=len(rows),
            status=BATCH_STATUS_PROJECTED,
            reason=None,
            detail=None,
            projected_at="2026-08-07T09:01:00.000000Z",
        ),
    )
    before = canonical.query_observations()

    segmentation = OperationalSegmentationStore(tmp_path / "segmentation.sqlite3")
    rebuild_from_canonical_store(canonical, segmentation)

    assert canonical.query_observations() == before
    assert len(segmentation.episodes_for_tool(72)) == 1


def test_schema_validation_rejects_incomplete_legacy_shape(tmp_path):
    path = tmp_path / "invalid.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE segmentation_runs (segment_id TEXT PRIMARY KEY)")

    with pytest.raises(FederationValidationError):
        OperationalSegmentationStore(path)


def test_database_enforces_containment_overlap_and_cross_device_parentage(tmp_path):
    rows = [*_reference_rows(), *_second_device_rows()]
    store = OperationalSegmentationStore(tmp_path / "constraints.sqlite3")
    rebuild_operational_segmentation(rows, store)

    with store._connect() as connection:
        cycle_row = dict(
            connection.execute(
                "SELECT * FROM segmentation_cycles ORDER BY start_sequence LIMIT 1"
            ).fetchone()
        )
        columns = tuple(cycle_row)
        placeholders = ", ".join("?" for _ in columns)

        outside = dict(cycle_row)
        outside["segment_id"] = "invalid-cycle-outside"
        outside["start_sequence"] = cycle_row["start_sequence"] - 1
        with pytest.raises(sqlite3.IntegrityError, match="cycle outside parent run"):
            connection.execute(
                f"INSERT INTO segmentation_cycles ({', '.join(columns)}) VALUES ({placeholders})",
                [outside[column] for column in columns],
            )

        wrong_device = dict(cycle_row)
        wrong_device["segment_id"] = "invalid-cycle-device"
        wrong_device["device_key"] = "uuid:device-beta"
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                f"INSERT INTO segmentation_cycles ({', '.join(columns)}) VALUES ({placeholders})",
                [wrong_device[column] for column in columns],
            )

        episode_row = dict(
            connection.execute(
                "SELECT * FROM segmentation_episodes ORDER BY first_sequence LIMIT 1"
            ).fetchone()
        )
        episode_columns = tuple(episode_row)
        episode_placeholders = ", ".join("?" for _ in episode_columns)
        overlap = dict(episode_row)
        overlap["segment_id"] = "invalid-overlap"
        with pytest.raises(sqlite3.IntegrityError, match="episodes overlap inside cycle"):
            connection.execute(
                f"INSERT INTO segmentation_episodes ({', '.join(episode_columns)}) VALUES ({episode_placeholders})",
                [overlap[column] for column in episode_columns],
            )

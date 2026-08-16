"""S5 OperationalEpisode segmentation over S2-S4 evidence."""

from __future__ import annotations

from catalog.mtconnect_recorder.canonical.observation import (
    CATEGORY_EVENT,
    CATEGORY_SAMPLE,
    CanonicalObservation,
)
from catalog.mtconnect_recorder.operational.episodes import (
    EpisodeBoundaryReason,
    build_operational_episodes,
)
from catalog.mtconnect_recorder.operational.model import BoundaryReason
from catalog.mtconnect_recorder.operational.roles import SemanticRole

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
    timestamp: str | None = None,
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
        "sub_type": None,
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
        agent_instance_id=INSTANCE,
        raw_batch_sha256="a" * 64,
    )


def _execution(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="EXECUTION", **kwargs)


def _program(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="PROGRAM", **kwargs)


def _comment(sequence: int, value: str, **kwargs) -> CanonicalObservation:
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


def _tool_group(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="TOOL_GROUP", **kwargs)


def _pallet(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(sequence=sequence, value=value, mtconnect_type="PALLET_ID", **kwargs)


def _path_feed(sequence: int, value: str, **kwargs) -> CanonicalObservation:
    return _observation(
        sequence=sequence,
        value=value,
        mtconnect_type="PATH_FEEDRATE",
        category=CATEGORY_SAMPLE,
        **kwargs,
    )


def test_repeated_concrete_tool_sequence_creates_one_episode_per_tenure():
    episodes = build_operational_episodes(
        [
            _program(1, "O1234"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "1200"),
            _tool(5, "155"),
            _line(6, "10"),
            _tool(7, "108"),
            _comment(8, "macro"),
            _tool(9, "91"),
            _tool(10, "60"),
            _execution(11, "READY"),
        ]
    )

    assert len(episodes) == 5
    assert [episode.entry_context.tool_number for episode in episodes] == [
        72.0,
        155.0,
        108.0,
        91.0,
        60.0,
    ]
    assert episodes[0].start_boundary.reason is EpisodeBoundaryReason.CYCLE_START
    assert episodes[-1].end_boundary.reason is EpisodeBoundaryReason.CYCLE_END
    assert all(
        episode.end_boundary.reason is EpisodeBoundaryReason.TOOL_CHANGE
        for episode in episodes[:-1]
    )
    assert all(
        episode.start_boundary.reason is EpisodeBoundaryReason.TOOL_CHANGE
        for episode in episodes[1:]
    )
    assert all(episode.duration_buckets_reconcile for episode in episodes)


def test_unknown_tool_context_never_fabricates_a_precise_change_boundary():
    episodes = build_operational_episodes(
        [
            _program(1, "O1234"),
            _tool(2, "UNAVAILABLE"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "900"),
            _tool(5, "72"),
            _tool(6, "UNAVAILABLE"),
            _tool(7, "155"),
            _execution(8, "READY"),
        ]
    )

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.tool_context_partial is True
    assert [
        transition.sequence
        for transition in episode.context_transitions
        if transition.role is SemanticRole.TOOL_NUMBER
    ] == [5, 6, 7]
    assert episode.start_boundary.reason is EpisodeBoundaryReason.CYCLE_START
    assert episode.end_boundary.reason is EpisodeBoundaryReason.CYCLE_END


def test_program_stopped_remains_inside_one_episode_and_duration_is_exposed():
    episodes = build_operational_episodes(
        [
            _program(1, "O1234", timestamp="2026-08-07T09:00:00Z"),
            _tool(2, "72", timestamp="2026-08-07T09:00:01Z"),
            _execution(3, "ACTIVE", timestamp="2026-08-07T09:00:02Z"),
            _path_feed(4, "1000", timestamp="2026-08-07T09:00:03Z"),
            _execution(5, "PROGRAM_STOPPED", timestamp="2026-08-07T09:00:10Z"),
            _line(6, "100", timestamp="2026-08-07T09:00:15Z"),
            _execution(7, "ACTIVE", timestamp="2026-08-07T09:00:20Z"),
            _execution(8, "READY", timestamp="2026-08-07T09:00:30Z"),
        ]
    )

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.wall_duration_us == 28_000_000
    assert episode.program_stopped_duration_us == 10_000_000
    assert episode.active_duration_us == 18_000_000
    assert episode.duration_buckets_reconcile is True


def test_program_line_comment_churn_is_context_history_not_episode_identity():
    episodes = build_operational_episodes(
        [
            _program(1, "OMAIN"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "800"),
            _program(5, "OSUB1"),
            _line(6, "120"),
            _comment(7, "nested macro"),
            _program(8, "OMAIN"),
            _execution(9, "READY"),
        ]
    )

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.entry_context.main_program == "OMAIN"
    assert episode.entry_context.subprogram is None
    assert "SUBPROGRAM" in episode.entry_context.partial_fields
    assert [transition.sequence for transition in episode.context_transitions] == [5, 6, 7, 8]


def test_other_device_tool_change_never_splits_this_device():
    episodes = build_operational_episodes(
        [
            _program(1, "OA", device_uuid="device-a", machine_id="machine-a"),
            _tool(2, "72", device_uuid="device-a", machine_id="machine-a"),
            _execution(3, "ACTIVE", device_uuid="device-a", machine_id="machine-a"),
            _program(4, "OB", device_uuid="device-b", machine_id="machine-b"),
            _path_feed(5, "700", device_uuid="device-a", machine_id="machine-a"),
            _tool(6, "1", device_uuid="device-b", machine_id="machine-b"),
            _execution(7, "ACTIVE", device_uuid="device-b", machine_id="machine-b"),
            _path_feed(8, "600", device_uuid="device-b", machine_id="machine-b"),
            _tool(9, "2", device_uuid="device-b", machine_id="machine-b"),
            _execution(10, "READY", device_uuid="device-b", machine_id="machine-b"),
            _execution(11, "READY", device_uuid="device-a", machine_id="machine-a"),
        ]
    )

    by_device: dict[str, list] = {}
    for episode in episodes:
        by_device.setdefault(episode.device_key, []).append(episode)

    assert len(by_device["uuid:device-a"]) == 1
    assert len(by_device["uuid:device-b"]) == 2
    assert by_device["uuid:device-a"][0].entry_context.tool_number == 72.0


def test_tool_group_is_deterministic_fallback_when_tool_number_is_absent():
    episodes = build_operational_episodes(
        [
            _program(1, "O1234"),
            _tool_group(2, "ROUGH"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "500"),
            _tool_group(5, "FINISH"),
            _execution(6, "READY"),
        ]
    )

    assert len(episodes) == 2
    assert all(
        episode.tool_identity_role is SemanticRole.TOOL_GROUP for episode in episodes
    )
    assert [episode.entry_context.tool_group for episode in episodes] == [
        "ROUGH",
        "FINISH",
    ]


def test_pallet_cycle_split_is_preserved_without_becoming_tool_split():
    episodes = build_operational_episodes(
        [
            _program(1, "O1234"),
            _tool(2, "72"),
            _pallet(3, "A"),
            _execution(4, "ACTIVE"),
            _path_feed(5, "600"),
            _pallet(6, "B"),
            _path_feed(7, "700"),
            _execution(8, "READY"),
        ]
    )

    assert len(episodes) == 2
    first, second = episodes
    assert first.production_cycle_id != second.production_cycle_id
    assert first.end_boundary.reason is EpisodeBoundaryReason.CYCLE_END
    assert first.end_boundary.inherited_reason is BoundaryReason.PALLET_CONTEXT_CHANGE
    assert second.start_boundary.reason is EpisodeBoundaryReason.CYCLE_START
    assert second.start_boundary.inherited_reason is BoundaryReason.PALLET_CONTEXT_CHANGE


def test_capture_end_and_sequence_gap_remain_visible_episode_edges():
    capture_end = build_operational_episodes(
        [
            _program(1, "O1"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _path_feed(4, "500"),
        ]
    )
    assert capture_end[-1].end_boundary.reason is EpisodeBoundaryReason.CAPTURE_END

    with_gap = build_operational_episodes(
        [
            _program(1, "O1"),
            _tool(2, "72"),
            _execution(3, "ACTIVE"),
            _execution(5, "ACTIVE"),
            _path_feed(6, "500"),
            _execution(7, "READY"),
        ]
    )
    assert any(
        episode.end_boundary.reason is EpisodeBoundaryReason.SEQUENCE_GAP
        or episode.start_boundary.reason is EpisodeBoundaryReason.SEQUENCE_GAP
        for episode in with_gap
    )


def test_episode_evidence_ranges_are_non_overlapping_and_identity_is_deterministic():
    rows = [
        _program(1, "O1234"),
        _tool(2, "72"),
        _execution(3, "ACTIVE"),
        _path_feed(4, "1000"),
        _tool(5, "155"),
        _tool(6, "108"),
        _execution(7, "READY"),
    ]

    first = build_operational_episodes(rows)
    second = build_operational_episodes(rows)

    assert [(item.episode_id, item.fingerprint) for item in first] == [
        (item.episode_id, item.fingerprint) for item in second
    ]
    for previous, current in zip(first, first[1:], strict=False):
        assert previous.last_sequence < current.first_sequence
        assert previous.end_boundary.observed_at_us <= current.start_boundary.observed_at_us
    assert all(item.duration_buckets_reconcile for item in first)

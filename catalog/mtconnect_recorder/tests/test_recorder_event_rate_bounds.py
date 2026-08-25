from __future__ import annotations

import json
from pathlib import Path

import pytest

from catalog.mtconnect_recorder import storage as recorder_storage
from catalog.mtconnect_recorder.storage import DurableRecorderStore


def _read(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_identical_pathology_events_coalesce_into_one_hourly_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        recorder_storage,
        "_utc_now",
        lambda: "2026-08-25T13:19:30+00:00",
    )
    store = DurableRecorderStore(tmp_path)

    paths = {
        store.record_event(
            source_name="Machine A",
            event_type="sequence_regression",
            payload={
                "previous_instance_id": 7,
                "previous_next_sequence": 1234,
                "agent_instance_id": 7,
                "agent_first_sequence": 1,
                "agent_last_sequence": 1000,
            },
        )
        for _ in range(500)
    }

    assert len(paths) == 1
    path = paths.pop()
    assert list(store.event_root.rglob("*.json")) == [path]
    payload = _read(path)
    assert payload["schema"] == "fcp.mtconnect.recorder_event.v2"
    assert payload["hour_bucket"] == "2026-08-25T13"
    assert payload["occurrence_count"] == 500
    assert payload["coalesced_occurrence_count"] == 499
    assert payload["payload_change_count"] == 0
    assert payload["first_payload"] == payload["latest_payload"]


def test_changing_pathology_payloads_still_use_one_file_per_hour(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        recorder_storage,
        "_utc_now",
        lambda: "2026-08-25T13:45:00+00:00",
    )
    store = DurableRecorderStore(tmp_path)

    path: Path | None = None
    for index in range(500):
        path = store.record_event(
            source_name="Machine A",
            event_type="agent_instance_changed_during_fetch",
            payload={
                "previous_instance_id": index,
                "agent_instance_id": index + 1,
            },
        )

    assert path is not None
    assert list(store.event_root.rglob("*.json")) == [path]
    payload = _read(path)
    assert payload["occurrence_count"] == 500
    assert payload["coalesced_occurrence_count"] == 499
    assert payload["payload_change_count"] == 499
    assert payload["first_payload"] == {
        "previous_instance_id": 0,
        "agent_instance_id": 1,
    }
    assert payload["latest_payload"] == {
        "previous_instance_id": 499,
        "agent_instance_id": 500,
    }
    # The summary retains only first/latest samples and counters; unique remote
    # values cannot make the file itself grow linearly with occurrence count.
    assert path.stat().st_size < 4096


def test_event_file_creation_is_rate_bounded_across_hour_rollover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timestamps = iter(
        [
            "2026-08-25T13:59:59+00:00",
            "2026-08-25T14:00:00+00:00",
            "2026-08-25T14:59:59+00:00",
        ]
    )
    monkeypatch.setattr(recorder_storage, "_utc_now", lambda: next(timestamps))
    store = DurableRecorderStore(tmp_path)

    first = store.record_event(
        source_name="Machine A",
        event_type="probe_changed",
        payload={"probe_sha256": "a" * 64},
    )
    second = store.record_event(
        source_name="Machine A",
        event_type="probe_changed",
        payload={"probe_sha256": "b" * 64},
    )
    third = store.record_event(
        source_name="Machine A",
        event_type="probe_changed",
        payload={"probe_sha256": "c" * 64},
    )

    assert first != second
    assert second == third
    assert len(list(store.event_root.rglob("*.json"))) == 2
    assert _read(first)["occurrence_count"] == 1
    assert _read(second)["occurrence_count"] == 2


def test_corrupt_event_summary_fails_closed_without_creating_an_escape_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        recorder_storage,
        "_utc_now",
        lambda: "2026-08-25T13:20:00+00:00",
    )
    store = DurableRecorderStore(tmp_path)
    path = store.record_event(
        source_name="Machine A",
        event_type="sequence_regression",
        payload={"agent_instance_id": 1},
    )
    path.write_text("not-json", encoding="utf-8")

    with pytest.raises(recorder_storage.MtconnectProtocolError, match="unreadable"):
        store.record_event(
            source_name="Machine A",
            event_type="sequence_regression",
            payload={"agent_instance_id": 1},
        )

    assert list(store.event_root.rglob("*.json")) == [path]


def test_event_type_is_confined_to_a_bounded_identifier(tmp_path: Path) -> None:
    store = DurableRecorderStore(tmp_path)

    with pytest.raises(ValueError, match="event_type"):
        store.record_event(
            source_name="Machine A",
            event_type="../escape",
            payload={"agent_instance_id": 1},
        )

    assert not store.event_root.exists()

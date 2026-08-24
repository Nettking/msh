"""Recorder storage paths must not be controlled by Agent timestamps."""

from __future__ import annotations

import pytest

from catalog.mtconnect_recorder import storage as storage_module
from catalog.mtconnect_recorder.model import MtconnectProtocolError
from catalog.mtconnect_recorder.parsing import parse_streams

from .conftest import Observation, streams_document

SOURCE = "MACHINE-ALPHA-0001"
INSTANCE = 1786093233


def _batch_with_timestamp(timestamp: str):
    xml_text = streams_document(
        instance_id=INSTANCE,
        observations=[
            Observation(
                sequence=1,
                component="Controller",
                element="Execution",
                data_item_id="exec",
                value="ACTIVE",
                category="Events",
                timestamp=timestamp,
            )
        ],
    )
    return xml_text, parse_streams(
        xml_text,
        source_name=SOURCE,
        probe=None,
        received_at="2026-08-24T12:00:00Z",
    )


@pytest.mark.parametrize(
    "timestamp",
    ("../../../x", "..\\..\\..\\x", "2026-02-30T12:00:00Z"),
)
def test_store_batch_rejects_unsafe_timestamp_before_any_write(
    recorder_archive,
    timestamp: str,
    monkeypatch: pytest.MonkeyPatch,
):
    xml_text, batch = _batch_with_timestamp(timestamp)
    writes = []

    def record_write(path, payload):
        writes.append((path, payload))

    monkeypatch.setattr(storage_module, "_write_bytes_atomic", record_write)
    monkeypatch.setattr(storage_module, "_write_json_atomic", record_write)
    monkeypatch.setattr(storage_module, "_write_text_atomic", record_write)

    with pytest.raises(MtconnectProtocolError, match="timestamp"):
        recorder_archive.store.store_batch(
            source_name=SOURCE,
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )

    assert writes == []
    assert not any(
        candidate.is_file()
        for candidate in recorder_archive.data_dir.rglob("*")
    )


@pytest.mark.parametrize(
    "timestamp",
    ("2026-08-24T12:34:56.789Z", "2026-08-24/../../../outside"),
)
def test_store_batch_confines_all_timestamp_partitioned_representations(
    recorder_archive,
    timestamp: str,
):
    xml_text, batch = _batch_with_timestamp(timestamp)

    stored = recorder_archive.store.store_batch(
        source_name=SOURCE,
        requested_from=1,
        xml_text=xml_text,
        batch=batch,
    )

    expected_roots = (
        (stored.raw_path, recorder_archive.store.raw_root),
        (stored.observation_path, recorder_archive.store.observation_root),
        (stored.normalized_path, recorder_archive.store.normalized_root),
    )
    for stored_path, expected_root in expected_roots:
        assert stored_path.resolve().is_relative_to(expected_root.resolve())
        assert stored_path.parent.name == "2026-08-24"
        assert stored_path.is_file()

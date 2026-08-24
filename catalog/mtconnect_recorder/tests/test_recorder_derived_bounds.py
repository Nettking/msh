from __future__ import annotations

import json
from pathlib import Path

import pytest

from catalog.mtconnect_recorder import bounded_storage
from catalog.mtconnect_recorder.model import (
    MtconnectProtocolError,
    ParsedBatch,
    StreamHeader,
)
from catalog.mtconnect_recorder.storage import DurableRecorderStore


def _batch(*, signal_count: int = 2, value: str = "1") -> ParsedBatch:
    observations = [
        {
            "source_record_id": f"1:{sequence}",
            "machine": "Machine",
            "machine_name": "Machine",
            "machine_id": "machine-1",
            "sequence": sequence,
            "timestamp": f"2026-08-24T20:00:{sequence:02d}Z",
            "received_at": "2026-08-24T20:01:00Z",
            "data_item_id": f"item-{sequence}",
            "name": f"signal-{sequence}",
            "category": "SAMPLE",
            "value": value,
        }
        for sequence in range(1, signal_count + 1)
    ]
    return ParsedBatch(
        header=StreamHeader(
            instance_id=7,
            first_sequence=1,
            last_sequence=signal_count,
            next_sequence=signal_count + 1,
        ),
        observations=observations,
        first_observation_sequence=1,
        last_observation_sequence=signal_count,
    )


def _partials(root: Path) -> tuple[Path, ...]:
    return tuple(root.rglob("*.partial")) if root.exists() else ()


def test_observation_archive_limit_fails_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = DurableRecorderStore(tmp_path / "data")
    monkeypatch.setattr(bounded_storage, "MAX_OBSERVATION_ARCHIVE_BYTES", 32)

    with pytest.raises(MtconnectProtocolError, match="observation archive exceeds"):
        store.store_observation_batch(
            source_name="source",
            batch=_batch(),
            raw_sha256="a" * 64,
        )

    assert tuple(store.observation_root.rglob("*.ndjson")) == ()
    assert _partials(store.observation_root) == ()


def test_compatibility_batch_limit_fails_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = DurableRecorderStore(tmp_path / "data")
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_BATCH_BYTES", 128)
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_STATE_BYTES", 1_000_000)

    with pytest.raises(MtconnectProtocolError, match="compatibility batch exceeds"):
        store.store_normalized_batch(
            source_name="source",
            batch=_batch(signal_count=4),
            raw_sha256="b" * 64,
        )

    assert tuple(store.normalized_root.rglob("*.jsonl")) == ()
    assert _partials(store.normalized_root) == ()


def test_compatibility_state_limit_fails_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = DurableRecorderStore(tmp_path / "data")
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_BATCH_BYTES", 1_000_000)
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_STATE_BYTES", 64)

    with pytest.raises(MtconnectProtocolError, match="compatibility state exceeds"):
        store.store_normalized_batch(
            source_name="source",
            batch=_batch(signal_count=2, value="x" * 80),
            raw_sha256="c" * 64,
        )

    assert tuple(store.normalized_root.rglob("*.jsonl")) == ()
    assert _partials(store.normalized_root) == ()


def test_streaming_compatibility_writer_preserves_wide_snapshot_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = DurableRecorderStore(tmp_path / "data")
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_BATCH_BYTES", 1_000_000)
    monkeypatch.setattr(bounded_storage, "MAX_COMPATIBILITY_STATE_BYTES", 1_000_000)

    path, latest = store.store_normalized_batch(
        source_name="source",
        batch=_batch(signal_count=2),
        initial_values={"machine-1": {"existing": "kept"}},
        raw_sha256="d" * 64,
    )

    rows = [json.loads(line) for line in path.read_text("utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["existing"] == "kept"
    assert rows[0]["signal-1"] == "1"
    assert "signal-2" not in rows[0]
    assert rows[1]["signal-1"] == "1"
    assert rows[1]["signal-2"] == "1"
    assert latest == {
        "machine-1": {
            "existing": "kept",
            "signal-1": "1",
            "signal-2": "1",
        }
    }
    assert _partials(store.normalized_root) == ()


def test_store_batch_keeps_raw_first_when_derived_limit_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = DurableRecorderStore(tmp_path / "data")
    batch = _batch(signal_count=2)
    xml_text = "<MTConnectStreams>bounded-fixture</MTConnectStreams>"
    monkeypatch.setattr(bounded_storage, "MAX_OBSERVATION_ARCHIVE_BYTES", 32)

    with pytest.raises(MtconnectProtocolError, match="observation archive exceeds"):
        store.store_batch(
            source_name="source",
            requested_from=1,
            xml_text=xml_text,
            batch=batch,
        )

    assert len(tuple(store.raw_root.rglob("*.xml.gz"))) == 1
    assert len(tuple(store.raw_root.rglob("*.manifest.json"))) == 1
    assert tuple(store.observation_root.rglob("*.ndjson")) == ()
    assert tuple(store.normalized_root.rglob("*.jsonl")) == ()

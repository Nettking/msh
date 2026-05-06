from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pytest

from catalog.runner import data_filtering


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


@pytest.fixture()
def isolated_data_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    index_path = tmp_path / "results" / "runner" / "data_index.json"
    monkeypatch.setattr(data_filtering, "DATA_INDEX_FILE", index_path)
    return index_path


def test_unchanged_files_reuse_cached_metadata(tmp_path: Path, isolated_data_index: Path) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "machine-a" / "2026-04-11.jsonl"
    _write_jsonl(source, [{"timestamp": "2026-04-11T08:00:00Z", "machine": "A"}])

    index_data, entries, stats = data_filtering._refresh_data_index_for_root(data_dir)
    data_filtering._write_data_index(index_data)
    assert stats["reparsed_files"] == 1
    cached_entry = entries["machine-a/2026-04-11.jsonl"]

    _index_data, entries, stats = data_filtering._refresh_data_index_for_root(data_dir)

    assert stats["reused_files"] == 1
    assert stats["reparsed_files"] == 0
    assert entries["machine-a/2026-04-11.jsonl"] == cached_entry


def test_changed_file_is_reindexed(tmp_path: Path, isolated_data_index: Path) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "2026-04-11.jsonl"
    _write_jsonl(source, [{"timestamp": "2026-04-11T08:00:00Z", "machine": "A"}])
    index_data, _entries, _stats = data_filtering._refresh_data_index_for_root(data_dir)
    data_filtering._write_data_index(index_data)

    _write_jsonl(
        source,
        [
            {"timestamp": "2026-04-11T08:00:00Z", "machine": "A"},
            {"timestamp": "2026-04-11T09:00:00Z", "machine": "B"},
        ],
    )
    os.utime(source, None)

    _index_data, entries, stats = data_filtering._refresh_data_index_for_root(data_dir)

    assert stats["reparsed_files"] == 1
    entry = entries["2026-04-11.jsonl"]
    assert entry["record_count"] == 2
    assert entry["machine_ids"] == ["A", "B"]
    assert entry["max_timestamp"].startswith("2026-04-11T09:00:00")


def test_deleted_file_is_removed_from_index(tmp_path: Path, isolated_data_index: Path) -> None:
    data_dir = tmp_path / "data"
    keep = data_dir / "2026-04-11.jsonl"
    remove = data_dir / "2026-04-12.jsonl"
    _write_jsonl(keep, [{"timestamp": "2026-04-11T08:00:00Z"}])
    _write_jsonl(remove, [{"timestamp": "2026-04-12T08:00:00Z"}])
    index_data, _entries, _stats = data_filtering._refresh_data_index_for_root(data_dir)
    data_filtering._write_data_index(index_data)

    remove.unlink()
    _index_data, entries, stats = data_filtering._refresh_data_index_for_root(data_dir)

    assert stats["deleted_files"] == 1
    assert sorted(entries) == ["2026-04-11.jsonl"]


def test_date_range_filtering_skips_files_outside_min_max_timestamp_range(
    tmp_path: Path,
    isolated_data_index: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    _write_jsonl(data_dir / "2026-04-10.jsonl", [{"timestamp": "2026-04-10T08:00:00Z", "machine": "A"}])
    _write_jsonl(data_dir / "2026-04-11.jsonl", [{"timestamp": "2026-04-11T08:00:00Z", "machine": "A"}])
    _write_jsonl(data_dir / "2026-04-12.jsonl", [{"timestamp": "2026-04-12T08:00:00Z", "machine": "A"}])
    data_filtering.discover_available_dates(data_dir)

    opened: list[str] = []
    original_iter_jsonl_records = data_filtering.iter_jsonl_records

    def counting_iter_jsonl_records(path: Path):
        opened.append(Path(path).name)
        yield from original_iter_jsonl_records(path)

    monkeypatch.setattr(data_filtering, "iter_jsonl_records", counting_iter_jsonl_records)

    matched_records, matched_files = data_filtering.filter_data_by_date_range(
        data_dir,
        tmp_path / "filtered",
        date(2026, 4, 11),
        date(2026, 4, 11),
    )

    assert opened == ["2026-04-11.jsonl"]
    assert matched_records == 1
    assert matched_files == 1


def test_files_with_missing_timestamp_metadata_are_handled_conservatively(
    tmp_path: Path,
    isolated_data_index: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "data"
    unknown = data_dir / "legacy.jsonl"
    outside = data_dir / "2026-04-10.jsonl"
    _write_jsonl(unknown, [{"machine": "legacy", "value": 1}])
    _write_jsonl(outside, [{"machine": "outside", "value": 2}])
    data_filtering.discover_available_dates(data_dir)

    opened: list[str] = []
    original_iter_jsonl_records = data_filtering.iter_jsonl_records

    def counting_iter_jsonl_records(path: Path):
        opened.append(Path(path).name)
        yield from original_iter_jsonl_records(path)

    monkeypatch.setattr(data_filtering, "iter_jsonl_records", counting_iter_jsonl_records)

    matched_records, matched_files = data_filtering.filter_data_by_date_range(
        data_dir,
        tmp_path / "filtered",
        date(2026, 4, 11),
        date(2026, 4, 11),
    )

    assert opened == ["legacy.jsonl"]
    assert matched_records == 0
    assert matched_files == 0


def test_filtered_output_matches_expected_records(tmp_path: Path, isolated_data_index: Path) -> None:
    data_dir = tmp_path / "data"
    _write_jsonl(
        data_dir / "machine-a" / "mixed.jsonl",
        [
            {"timestamp": "2026-04-10T23:59:00Z", "machine": "A", "value": "before"},
            {"timestamp": "2026-04-11T08:00:00Z", "machine": "A", "value": "match"},
            {"timestamp": "2026-04-12T00:00:00Z", "machine": "A", "value": "after"},
        ],
    )
    _write_jsonl(data_dir / "machine-b" / "2026-04-11.jsonl", [{"machine": "B", "value": "fallback"}])

    destination = tmp_path / "filtered"
    matched_records, matched_files = data_filtering.filter_data_by_date_range(
        data_dir,
        destination,
        date(2026, 4, 11),
        date(2026, 4, 11),
    )

    output_records = []
    for output_file in sorted(destination.rglob("*.jsonl")):
        output_records.extend(json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines())

    assert matched_records == 2
    assert matched_files == 2
    assert output_records == [
        {"timestamp": "2026-04-11T08:00:00Z", "machine": "A", "value": "match"},
        {"machine": "B", "value": "fallback"},
    ]


def test_filter_progress_logs_operational_counters(
    tmp_path: Path,
    isolated_data_index: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data_dir = tmp_path / "data"
    _write_jsonl(
        data_dir / "2026-04-11.jsonl",
        [
            {"timestamp": "2026-04-11T08:00:00Z", "machine": "A"},
            {"timestamp": "2026-04-11T09:00:00Z", "machine": "A"},
            {"timestamp": "2026-04-11T10:00:00Z", "machine": "A"},
        ],
    )
    _write_jsonl(data_dir / "2026-04-12.jsonl", [{"timestamp": "2026-04-12T08:00:00Z", "machine": "B"}])
    monkeypatch.setattr(data_filtering, "FILTER_PROGRESS_RECORD_INTERVAL", 2)

    matched_records, matched_files = data_filtering.filter_data_by_date_range(
        data_dir,
        tmp_path / "filtered",
        date(2026, 4, 11),
        date(2026, 4, 11),
        active_slice=date(2026, 4, 11),
        remaining_slices=4,
    )

    assert matched_records == 3
    assert matched_files == 1
    output = capsys.readouterr().out
    assert "[runner] Filtering 2 indexed files for 2026-04-11..2026-04-11" in output
    assert "[runner] Index pruning selected 1 candidate files" in output
    assert "[runner] Filter progress: phase=started" in output
    assert "total_indexed_files=2" in output
    assert "candidate_files=1" in output
    assert "candidate_files_opened=1/1" in output
    assert "candidate_files_processed=1/1" in output
    assert "matched_files=1" in output
    assert "matched_records=3" in output
    assert "active_slice=2026-04-11" in output
    assert "remaining_slices=4" in output
    assert "phase=reading candidate" in output
    assert "file_records_processed=2" in output

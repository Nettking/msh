"""Selection tests use only synthetic tar members, never historical telemetry."""
from __future__ import annotations

import hashlib
import io
import json
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from research.state_semantics_evolution_2026.archive_dataset import digest, select_archive


def _archive(path: Path, members: dict[str, str]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, text in members.items():
            payload = text.encode("utf-8")
            member = tarfile.TarInfo(name)
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))


def _rows(machine: str, count: int = 40) -> list[dict]:
    start = datetime(2026, 10, 5, tzinfo=timezone.utc)
    return [{"timestamp": (start + timedelta(seconds=i)).isoformat(),
             "machine_id": machine,
             "execution": None if i < 5 else "ACTIVE" if i < 20 else "STOPPED",
             "Srpm": 125 if i < 5 else 0, "Sload": 0,
             "schema": "fcp.mtconnect.snapshot.v2"}
            for i in range(count)]


def test_archive_selection_is_balanced_temporally_disjoint_and_reproducible(tmp_path: Path):
    source = tmp_path / "private-archive.tar.gz"
    machines = ["Private-Machine-A", "Private-Machine-B"]
    members = {}
    for machine in machines:
        rows = _rows(machine)
        # Files overlap in time and arrive in reverse chronology. Ties must
        # remain wholly inside one block even across different members.
        members[f"jsonl/{machine}/z.jsonl"] = "".join(json.dumps(r) + "\n" for r in rows[::2])
        members[f"jsonl/{machine}/a.jsonl"] = "".join(json.dumps(r) + "\n" for r in rows[1::2] + [rows[3]])
    members["../../must-not-extract"] = "ignored"
    _archive(source, members)
    before = source.read_bytes()
    windows, manifest = select_archive(source, windows_per_machine=3, target_records=6)
    windows2, manifest2 = select_archive(source, windows_per_machine=3, target_records=6)
    assert source.read_bytes() == before
    assert windows == windows2 and manifest == manifest2
    assert len(windows) == 6
    assert manifest["archive_sha256"] == hashlib.sha256(before).hexdigest()
    assert manifest["selection"]["windows_selected"] == 6
    assert len(manifest["members"]) == 4
    for machine in machines:
        machine_windows = [w for w in windows if w[0]["machine_id"] == machine]
        assert len(machine_windows) == 3
        boundaries = [(min(r["timestamp"] for r in w), max(r["timestamp"] for r in w))
                      for w in machine_windows]
        assert all(a[1] < b[0] for a, b in zip(boundaries, boundaries[1:]))
        all_occurrences = [r["timestamp"] for r in _rows(machine)] + [_rows(machine)[3]["timestamp"]]
        for window in machine_windows:
            for timestamp in {r["timestamp"] for r in window}:
                assert sum(r["timestamp"] == timestamp for r in window) == all_occurrences.count(timestamp)
    for window, metadata in zip(windows, manifest["windows"]):
        assert metadata["window_sha256"] == digest(window)
        assert metadata["features"]["records"] == len(window)
        assert pd.Timestamp(metadata["start_utc"]) <= pd.Timestamp(metadata["end_utc"])
    exported = json.dumps(manifest)
    assert all(machine not in exported for machine in machines)
    assert str(tmp_path) not in exported
    assert not (tmp_path.parent / "must-not-extract").exists()


def test_archive_tolerant_reader_accounts_for_excluded_input_without_forward_fill(tmp_path: Path):
    source = tmp_path / "source.tar.gz"
    rows = _rows("Secret", 12)
    del rows[1]["execution"]
    rows[2]["timestamp"] = "not-a-time"
    text = "\ninvalid-json\n[]\n" + "\n".join(json.dumps(r) for r in rows)
    text += '\n{"timestamp":"2026-10-05T00:00:01Z"}\n'
    _archive(source, {"jsonl/private/source.jsonl": text})
    windows, manifest = select_archive(source, windows_per_machine=1, target_records=20)
    quality = manifest["quality"]
    assert quality["blank_lines"] == 1
    assert quality["malformed_json_lines"] == 1
    assert quality["non_dictionary_lines"] == 1
    assert quality["missing_machine_records"] == 1
    assert quality["invalid_timestamp_records"] == 1
    assert len(windows) == 1 and len(windows[0]) == 11
    assert windows[0][1]["execution"] is None
    assert manifest["windows"][0]["features"]["threshold_sensitive_rows"] == 4
    refs = manifest["windows"][0]["source_refs"]
    assert len(refs) == 1
    # Exact one-based raw line ranges preserve replay identity despite skips.
    assert refs[0]["line_ranges_inclusive"] == [[4, 5], [7, 15]]


def test_archive_rejects_invalid_selection_bounds(tmp_path: Path):
    with pytest.raises(ValueError):
        select_archive(tmp_path / "missing.tar.gz", windows_per_machine=0)
    with pytest.raises(ValueError):
        select_archive(tmp_path / "missing.tar.gz", target_records=1)

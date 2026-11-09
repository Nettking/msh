"""Smoke-test the read-only replay CLI on a bounded synthetic JSONL corpus."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from research.state_semantics_evolution_2026 import replay


def test_replay_cli_is_read_only_bounded_and_anonymized(tmp_path: Path, monkeypatch):
    root = tmp_path / "source"
    root.mkdir()
    source = root / "machine.jsonl"
    start = datetime(2026, 1, 15, tzinfo=timezone.utc)
    rows = [
        {
            "machine_id": "Private-Machine-1",
            "timestamp": (start + timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
            "execution": "ACTIVE" if i % 2 else "STOPPED",
            "Srpm": 120 if i % 2 else 0,
            "Sload": 1.2 if i % 2 else 0,
        }
        for i in range(41)
    ]
    original = "".join(json.dumps(r) + "\n" for r in rows)
    source.write_text(original, encoding="utf-8")
    report_file = tmp_path / "analysis" / "report.json"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "replay",
            "--source-root", str(root),
            "--window-records", "40",
            "--max-windows", "1",
            "--max-scan-records", "41",
            "--output", str(report_file),
        ],
    )
    assert replay.main() == 0

    assert source.read_text(encoding="utf-8") == original
    report_text = report_file.read_text(encoding="utf-8")
    report = json.loads(report_text)
    assert "Private-Machine-1" not in report_text
    assert str(root) not in report_text
    assert report["selection"]["windows_selected"] == 1
    assert report["selection"]["window_records"] == 40
    assert report["equivalence_failures"] == 0
    assert report["windows"][0]["records"] == 40
    assert len(report["windows"][0]["window_sha256"]) == 64


def test_e6_boundary_count_treats_invalid_load_as_missing():
    """Numeric rpm remains decisive when production coerces invalid load to NaN."""
    start = datetime(2026, 1, 15, tzinfo=timezone.utc)
    rows = [{"machine_id":"SYNTHETIC","timestamp":(start+timedelta(seconds=i)).isoformat(),
             "execution":"UNAVAILABLE","Srpm":120,"Sload":load}
            for i, load in enumerate(["UNAVAILABLE", None, "bad-load", 0, 2])]
    result = replay._analyze_window(rows)
    assert result["threshold_100_to_150"]["potentially_affected_fallback_rows"] == 4

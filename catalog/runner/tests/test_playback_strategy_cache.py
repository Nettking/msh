from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from catalog.runner import playback


def _metadata() -> dict:
    return {
        "session_config_signature": "session-a",
        "filter_result": {"generated_at": "2026-01-01T00:00:00Z", "matched_records": 0},
        "paths": {"playback_exports_dir": "exports/timeline", "filtered_data_dir": "data"},
    }


def _write_cached_export(session_dir: Path, strategy_signature: str) -> Path:
    export_dir = session_dir / "exports" / "timeline"
    export_dir.mkdir(parents=True)
    pd.DataFrame(columns=["timestamp", "machine_id", "state"]).to_csv(export_dir / "timeline_rows.csv", index=False)
    pd.DataFrame().to_csv(export_dir / "candidate_events.csv", index=False)
    pd.DataFrame().to_csv(export_dir / "strategy_summary.csv", index=False)
    (export_dir / "strategies_used.yaml").write_text(
        f"strategy_config_signature: {strategy_signature}\nstrategies:\n", encoding="utf-8"
    )
    (export_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "export_file": "timeline_rows.csv",
                "session_config_signature": "session-a",
                "filtered_generated_at": "2026-01-01T00:00:00Z",
                "strategy_config_signature": strategy_signature,
                "row_count": 0,
            }
        ),
        encoding="utf-8",
    )
    return export_dir


def test_playback_cache_reuse_compares_current_strategy_signature(tmp_path: Path, monkeypatch) -> None:
    session_dir = tmp_path / "session"
    _write_cached_export(session_dir, "old-signature")
    metadata = _metadata()

    monkeypatch.setattr(playback, "intervention_strategy_config_signature", lambda: "old-signature")
    assert playback.playback_exports_are_reusable(session_dir, metadata) is True

    monkeypatch.setattr(playback, "intervention_strategy_config_signature", lambda: "new-signature")
    assert playback.playback_exports_are_reusable(session_dir, metadata) is False


def test_prepare_session_playback_exports_stores_strategy_signature(tmp_path: Path, monkeypatch) -> None:
    session_dir = tmp_path / "session"
    metadata = _metadata()

    def fake_write_strategy_outputs(df, output_dir):
        output = Path(output_dir)
        pd.DataFrame().to_csv(output / "candidate_events.csv", index=False)
        pd.DataFrame().to_csv(output / "strategy_summary.csv", index=False)
        (output / "strategies_used.yaml").write_text("strategy_config_signature: fresh-signature\n", encoding="utf-8")
        return {
            "candidate_events": output / "candidate_events.csv",
            "strategy_summary": output / "strategy_summary.csv",
            "strategies_used": output / "strategies_used.yaml",
        }

    monkeypatch.setattr(playback, "intervention_strategy_config_signature", lambda: "fresh-signature")
    monkeypatch.setattr(playback, "write_strategy_outputs", fake_write_strategy_outputs)

    export_path, status = playback.prepare_session_playback_exports(session_dir, metadata)

    assert status == "created"
    assert export_path.name == "timeline_rows.csv"
    manifest = json.loads((export_path.parent / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["strategy_config_signature"] == "fresh-signature"

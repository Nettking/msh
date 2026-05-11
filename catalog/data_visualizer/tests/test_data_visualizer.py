import json
from pathlib import Path

from catalog.data_visualizer import data_visualizer


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_load_prepared_frames_reads_nested_session_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_jsonl(
        tmp_path / "data" / "machine-a" / "2026-04-23.jsonl",
        [
            {
                "timestamp": "2026-04-23T12:00:00Z",
                "machine": "machine-a",
                "Srpm": 1200,
                "Sload": 12,
            }
        ],
    )

    frames = data_visualizer.load_prepared_frames()

    assert len(frames) == 1
    assert frames[0]["machine_id"].tolist() == ["machine-a"]
    assert frames[0]["source_file"].tolist() == ["machine-a/2026-04-23.jsonl"]

"""Session playback export preparation and Streamlit launch helpers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, load_jsonl_dataframe
from catalog.common.timeline_exports import TIMELINE_COLUMNS, export_timeline_rows
from catalog.runner.script_catalog import repo_root

PLAYBACK_EXPORT_FILE = "timeline_rows.csv"
PLAYBACK_MANIFEST_FILE = "manifest.json"


def session_playback_export_dir(session_dir: Path, metadata: dict[str, Any]) -> Path:
    """Resolve the conventional playback export directory for a session."""
    paths = metadata.get("paths", {})
    export_rel = str(paths.get("playback_exports_dir", "exports/timeline"))
    return session_dir / export_rel


def _filtered_data_dir(session_dir: Path, metadata: dict[str, Any]) -> Path:
    return session_dir / str(metadata.get("paths", {}).get("filtered_data_dir", "data"))


def _collect_filtered_dataframe(filtered_data_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for file_path in iter_jsonl_files(filtered_data_dir, recursive=True):
        loaded = load_jsonl_dataframe(file_path)
        if not loaded.empty:
            frames.append(loaded)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _manifest_path(export_dir: Path) -> Path:
    return export_dir / PLAYBACK_MANIFEST_FILE


def _export_path(export_dir: Path) -> Path:
    return export_dir / PLAYBACK_EXPORT_FILE


def _read_manifest(export_dir: Path) -> dict[str, Any] | None:
    manifest_path = _manifest_path(export_dir)
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_manifest(export_dir: Path, payload: dict[str, Any]) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)
    target = _manifest_path(export_dir)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def playback_exports_are_reusable(session_dir: Path, metadata: dict[str, Any]) -> bool:
    """Check whether cached session playback exports look reusable."""
    export_dir = session_playback_export_dir(session_dir, metadata)
    export_path = _export_path(export_dir)
    if not export_path.exists():
        return False

    manifest = _read_manifest(export_dir)
    if manifest is None:
        return False

    if str(manifest.get("session_config_signature", "")) != str(metadata.get("session_config_signature", "")):
        return False

    current_filtered_generated_at = metadata.get("filter_result", {}).get("generated_at")
    if str(manifest.get("filtered_generated_at", "")) != str(current_filtered_generated_at):
        return False

    if int(manifest.get("row_count", -1)) < 0:
        return False
    return True


def playback_readiness(session_dir: Path, metadata: dict[str, Any]) -> tuple[bool, list[str]]:
    """Return playback readiness and missing requirements."""
    missing: list[str] = []
    filter_result = metadata.get("filter_result", {})
    matched_records = int(filter_result.get("matched_records", 0) or 0)
    filtered_data_dir = _filtered_data_dir(session_dir, metadata)
    filtered_data_ready = matched_records > 0 and filtered_data_dir.exists()
    if not filtered_data_ready:
        missing.append("session filtered data (run/create filter for this session)")

    return len(missing) == 0, missing


def prepare_session_playback_exports(session_dir: Path, metadata: dict[str, Any]) -> tuple[Path, str]:
    """
    Build playback-ready timeline exports in the session export directory.

    Returns:
        (export_file_path, status) where status is one of ``cached`` or ``created``.
    """
    export_dir = session_playback_export_dir(session_dir, metadata)
    export_path = _export_path(export_dir)
    if playback_exports_are_reusable(session_dir, metadata):
        return export_path, "cached"

    filtered_data_dir = _filtered_data_dir(session_dir, metadata)
    source_df = _collect_filtered_dataframe(filtered_data_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    if source_df.empty:
        pd.DataFrame(columns=TIMELINE_COLUMNS).to_csv(export_path, index=False)
        row_count = 0
    else:
        export_timeline_rows(source_df, output_path=export_path)
        row_count = len(pd.read_csv(export_path))

    _write_manifest(
        export_dir,
        {
            "version": 1,
            "export_file": PLAYBACK_EXPORT_FILE,
            "session_config_signature": metadata.get("session_config_signature"),
            "filtered_generated_at": metadata.get("filter_result", {}).get("generated_at"),
            "row_count": row_count,
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        },
    )
    return export_path, "created"


def launch_playback_app_for_session(session_dir: Path, metadata: dict[str, Any]) -> int:
    """Launch the Streamlit playback app preloaded with session exports."""
    export_dir = session_playback_export_dir(session_dir, metadata)
    root = repo_root()
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "catalog/webapp/app.py",
        "--",
        "--session-export-dir",
        str(export_dir.resolve()),
    ]
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    root_str = str(root)
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{root_str}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = root_str
    print("\nLaunching Streamlit playback app...", flush=True)
    print(f"Command: {' '.join(command)}", flush=True)
    print("Stop Streamlit with Ctrl+C to return to runner.", flush=True)
    return subprocess.run(command, cwd=root, env=env).returncode

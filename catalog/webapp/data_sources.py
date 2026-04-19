"""Data source loading and bootstrap resolution helpers for the webapp."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from catalog.common.timeline_exports import load_timeline_export_with_schema_info
from catalog.webapp.analysis_registry import load_artifact_frame

DEFAULT_EXPORT_CANDIDATES = ["timeline_rows.csv", "timeline_rows.parquet", "timeline_rows.jsonl", "timeline_rows.json"]


@st.cache_data(show_spinner=False)
def load_data_from_path(path: str) -> tuple[pd.DataFrame, pd.DataFrame, set[str], set[str]]:
    """Load and normalize a playback timeline export from a filesystem path."""
    return load_timeline_export_with_schema_info(path)


@st.cache_data(show_spinner=False)
def load_data_from_upload(content: bytes, suffix: str) -> tuple[pd.DataFrame, pd.DataFrame, set[str], set[str]]:
    """Load and normalize a playback timeline export from uploaded bytes."""
    with tempfile.NamedTemporaryFile(prefix="timeline_upload_", suffix=suffix, delete=False) as handle:
        handle.write(content)
        tmp_path = Path(handle.name)
    try:
        return load_timeline_export_with_schema_info(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def read_table(path: str | Path) -> pd.DataFrame:
    """Read generic tabular sources used for exploration and non-playback views."""
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(source)
    if suffix == ".jsonl":
        rows: list[dict] = []
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    if suffix == ".json":
        return pd.read_json(source)
    raise ValueError(f"Unsupported file extension: {suffix}")


@st.cache_data(show_spinner=False)
def load_table_from_path(path: str) -> pd.DataFrame:
    """Load a generic table from a filesystem path with caching."""
    return read_table(path)


@st.cache_data(show_spinner=False)
def load_table_from_upload(content: bytes, suffix: str) -> pd.DataFrame:
    """Load a generic table from uploaded bytes with caching."""
    with tempfile.NamedTemporaryFile(prefix="table_upload_", suffix=suffix, delete=False) as handle:
        handle.write(content)
        tmp_path = Path(handle.name)
    try:
        return read_table(tmp_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


@st.cache_data(show_spinner=False)
def load_registry_frame(path: str) -> pd.DataFrame:
    """Load a fully indexed artifact by path from the analysis registry."""
    return load_artifact_frame(path)


def parse_bootstrap_args() -> argparse.Namespace:
    """Parse bootstrap options passed through Streamlit `--` arguments."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--session-export-dir", default="")
    parser.add_argument("--source-path", default="")
    return parser.parse_known_args(sys.argv[1:])[0]


def resolve_session_export_file(path: str) -> str:
    """Resolve timeline export file from a session export directory."""
    if not path:
        return ""
    root = Path(path).expanduser()
    if not root.exists() or not root.is_dir():
        return ""
    for file_name in DEFAULT_EXPORT_CANDIDATES:
        candidate = root / file_name
        if candidate.exists():
            return str(candidate)
    return ""


def resolve_bootstrap_source() -> tuple[str, str]:
    """Resolve preferred bootstrap source path from CLI args and environment."""
    args = parse_bootstrap_args()
    if args.source_path:
        return args.source_path, "command-line source"

    from_session_dir = resolve_session_export_file(args.session_export_dir)
    if from_session_dir:
        return from_session_dir, f"session export ({args.session_export_dir})"

    env_source = os.getenv("MSH_PLAYBACK_SOURCE_PATH", "").strip()
    if env_source:
        return env_source, "MSH_PLAYBACK_SOURCE_PATH"

    env_export_dir = os.getenv("MSH_PLAYBACK_EXPORT_DIR", "").strip()
    from_env_export = resolve_session_export_file(env_export_dir)
    if from_env_export:
        return from_env_export, f"MSH_PLAYBACK_EXPORT_DIR ({env_export_dir})"

    return "", ""

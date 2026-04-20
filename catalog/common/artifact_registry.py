"""Framework-neutral artifact scanning/indexing and tabular loading helpers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

SUPPORTED_SUFFIXES = {".csv", ".parquet", ".pq", ".jsonl", ".json"}
REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}
INTERNAL_METADATA_FILES = {"runtime_state.json", "session_state.json"}

KEYWORD_RULES: list[tuple[tuple[str, ...], tuple[str, str, str]]] = [
    (("timeline", "playback"), ("Timeline Playback", "Playback-compatible timeline rows for machine state replay.", "playback")),
    (("intervention",), ("Interventions", "Intervention-related rows and summaries.", "analysis")),
    (("override",), ("Override Changes", "Override and feed/speed override change outputs.", "analysis")),
    (("missing",), ("Missing Data", "Missing data and sequence-number quality analyses.", "analysis")),
    (("active_per_day", "machine_day", "data_pr_day"), ("Machine/Day Summary", "Per-machine and per-day summary aggregates.", "analysis")),
    (("state", "activity", "stops"), ("State and Activity", "State/activity summaries and stop detection outputs.", "analysis")),
]


@dataclass(frozen=True)
class DataArtifact:
    path: str
    source_dir: str
    file_name: str
    size_bytes: int
    modified_at: pd.Timestamp
    signature: str
    kind: str
    analysis_name: str
    description: str
    status: str
    supported_views: tuple[str, ...]
    columns: tuple[str, ...]
    row_count: int | None
    machine_count: int | None
    day_count: int | None
    timestamp_min: str | None
    timestamp_max: str | None
    load_error: str | None
    category: str
    visibility: str
    dedupe_key: str | None
    is_internal: bool

    def to_record(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "source_dir": self.source_dir,
            "file_name": self.file_name,
            "size_mb": round(self.size_bytes / (1024 * 1024), 3),
            "modified_at": self.modified_at.isoformat(),
            "signature": self.signature,
            "kind": self.kind,
            "analysis_name": self.analysis_name,
            "description": self.description,
            "status": self.status,
            "supported_views": list(self.supported_views),
            "columns": list(self.columns),
            "row_count": self.row_count,
            "machine_count": self.machine_count,
            "day_count": self.day_count,
            "timestamp_min": self.timestamp_min,
            "timestamp_max": self.timestamp_max,
            "load_error": self.load_error,
            "playback_compatible": self.kind == "playback",
            "category": self.category,
            "visibility": self.visibility,
            "dedupe_key": self.dedupe_key,
            "is_internal": self.is_internal,
        }


def configured_scan_dirs() -> list[str]:
    configured = os.getenv("MSH_SCAN_DIRS", "results,data").strip()
    dirs = [d.strip() for d in configured.split(",") if d.strip()]
    return dirs or ["results", "data"]


def read_raw_table(path: str | Path) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(source)
    if suffix == ".jsonl":
        rows = []
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    if suffix == ".json":
        return pd.read_json(source)
    raise ValueError(f"Unsupported file extension: {suffix}")


def read_preview_table(path: str | Path, max_rows: int = 3000) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(source, nrows=max_rows)

    if suffix == ".jsonl":
        rows = []
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
                if len(rows) >= max_rows:
                    break
        return pd.DataFrame(rows)

    if suffix in {".parquet", ".pq"}:
        try:
            import pyarrow.parquet as pq  # type: ignore

            table = pq.read_table(source)
            return table.slice(0, max_rows).to_pandas()
        except Exception:  # noqa: BLE001
            return pd.read_parquet(source).head(max_rows)

    if suffix == ".json":
        return pd.read_json(source).head(max_rows)

    raise ValueError(f"Unsupported file extension: {suffix}")


def read_table_columns(path: str | Path, max_rows: int = 3000) -> tuple[set[str], str | None]:
    source = Path(path)
    suffix = source.suffix.lower()
    try:
        if suffix == ".csv":
            cols = pd.read_csv(source, nrows=0).columns.tolist()
            return set(cols), None

        if suffix == ".jsonl":
            columns: set[str] = set()
            with source.open("r", encoding="utf-8") as handle:
                for idx, line in enumerate(handle):
                    if idx >= max_rows:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if isinstance(record, dict):
                        columns.update(record.keys())
            return columns, None

        if suffix in {".parquet", ".pq"}:
            try:
                import pyarrow.parquet as pq  # type: ignore

                pf = pq.ParquetFile(source)
                return set(pf.schema.names), None
            except Exception:  # noqa: BLE001
                frame = pd.read_parquet(source).head(1)
                return set(frame.columns), None

        if suffix == ".json":
            frame = read_preview_table(source, max_rows=max_rows)
            return set(frame.columns), None
    except Exception as exc:  # noqa: BLE001
        return set(), str(exc)

    return set(), f"Unsupported file extension: {suffix}"


def _classify_analysis(path: Path, columns: set[str]) -> tuple[str, str, str]:
    p = str(path).lower()
    if REQUIRED_PLAYBACK_COLUMNS.issubset(columns):
        return "Timeline Playback", "Playback-compatible timeline rows for machine state replay.", "playback"

    for keywords, result in KEYWORD_RULES:
        if any(keyword in p for keyword in keywords):
            return result

    return "Tabular Output", "General-purpose tabular output that can be explored in the app.", "exploration"


def _views_for_kind(kind: str, columns: set[str]) -> tuple[str, ...]:
    views = ["summary", "table", "chart"]
    if "timestamp" in columns:
        views.append("time-trend")
    if "machine_id" in columns:
        views.append("machine-filter")
    if kind == "playback":
        views.append("playback")
    return tuple(dict.fromkeys(views))


def _source_root_for(path: Path, roots: list[str]) -> str:
    path_resolved = path.resolve()
    for root in roots:
        root_path = Path(root).expanduser()
        try:
            if path_resolved.is_relative_to(root_path.resolve()):
                return root_path.name.lower()
        except Exception:  # noqa: BLE001
            continue
    return path.parts[0].lower() if path.parts else ""


def _relative_to_root(path: Path, source_dir: str) -> Path:
    source_root = Path(source_dir).expanduser()
    try:
        return path.resolve().relative_to(source_root.resolve())
    except Exception:  # noqa: BLE001
        return path


def _artifact_category(path: Path, source_dir: str, roots: list[str]) -> str:
    root_name = _source_root_for(path, roots)
    rel = _relative_to_root(path, source_dir)
    lower_parts = [part.lower() for part in rel.parts]

    if path.name in INTERNAL_METADATA_FILES:
        return "internal_metadata"
    if root_name == "data":
        return "source_data"
    if "workflows" in lower_parts and "analyses" in lower_parts:
        return "derived_output"
    if "workflows" in lower_parts and "data" in lower_parts:
        return "workflow_data_copy"
    return "derived_output"


def _artifact_visibility(category: str) -> str:
    if category == "internal_metadata":
        return "internal"
    if category == "workflow_data_copy":
        return "hidden_default"
    return "default"


def _dedupe_key(path: Path, category: str) -> str | None:
    if category not in {"source_data", "workflow_data_copy"}:
        return None
    try:
        return path.stem
    except Exception:  # noqa: BLE001
        return None


def _build_artifact(path: Path, source_dir: str, roots: list[str]) -> DataArtifact:
    stat = path.stat()
    signature = hashlib.sha1(f"{path}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")).hexdigest()[:12]
    modified = pd.to_datetime(stat.st_mtime, unit="s", utc=True).tz_convert(None)

    load_error = None
    frame = pd.DataFrame()
    try:
        frame = read_preview_table(path, max_rows=3000)
    except Exception as exc:  # noqa: BLE001
        load_error = str(exc)

    columns = set(frame.columns)
    name, description, kind = _classify_analysis(path, columns)
    views = _views_for_kind(kind, columns)

    timestamp_min = None
    timestamp_max = None
    day_count = None
    if "timestamp" in frame.columns and not frame.empty:
        ts = pd.to_datetime(frame["timestamp"], errors="coerce")
        if ts.notna().any():
            timestamp_min = str(ts.min())
            timestamp_max = str(ts.max())
            day_count = int(ts.dt.date.nunique())

    machine_count = None
    if "machine_id" in frame.columns and not frame.empty:
        machine_count = int(frame["machine_id"].astype("string").str.strip().replace("", pd.NA).dropna().nunique())

    status = "ready" if load_error is None else "read_error"
    category = _artifact_category(path, source_dir, roots)
    visibility = _artifact_visibility(category)
    dedupe_key = _dedupe_key(path, category)
    is_internal = category == "internal_metadata"

    return DataArtifact(
        path=str(path),
        source_dir=source_dir,
        file_name=path.name,
        size_bytes=stat.st_size,
        modified_at=modified,
        signature=signature,
        kind=kind,
        analysis_name=name,
        description=description,
        status=status,
        supported_views=views,
        columns=tuple(frame.columns.tolist()),
        row_count=(None if frame.empty and load_error else int(len(frame))),
        machine_count=machine_count,
        day_count=day_count,
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
        load_error=load_error,
        category=category,
        visibility=visibility,
        dedupe_key=dedupe_key,
        is_internal=is_internal,
    )


def scan_artifacts(scan_dirs: list[str] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    roots = scan_dirs or configured_scan_dirs()
    artifacts: list[dict[str, Any]] = []
    warnings: list[str] = []

    for raw_root in roots:
        root = Path(raw_root).expanduser()
        if not root.exists() or not root.is_dir():
            warnings.append(f"Scan root '{raw_root}' does not exist or is not a directory.")
            continue

        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
                continue
            try:
                artifact = _build_artifact(path, source_dir=str(root), roots=roots)
                artifacts.append(artifact.to_record())
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Failed indexing {path}: {exc}")

    artifacts.sort(key=lambda item: (item["analysis_name"], item["file_name"], item["path"]))
    return artifacts, warnings

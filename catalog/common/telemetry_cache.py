"""Parquet + DuckDB analytical cache for raw JSONL telemetry.

Raw JSONL files remain the source of truth. This module builds a disposable,
repeatable Parquet cache for fast analytical reads and exposes small DuckDB
query helpers over that cache.
"""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records

TELEMETRY_FIELDS: tuple[str, ...] = (
    "timestamp",
    "machine_id",
    "source_file",
    "execution",
    "mode",
    "program",
    "Tool_number",
    "Tool_group",
    "Srpm",
    "Sload",
    "Sovr",
    "Fovr",
    "Frapidovr",
    "Xabs",
    "Yabs",
    "Zabs",
)

NUMERIC_FIELDS: tuple[str, ...] = (
    "Tool_number",
    "Tool_group",
    "Srpm",
    "Sload",
    "Sovr",
    "Fovr",
    "Frapidovr",
    "Xabs",
    "Yabs",
    "Zabs",
)

STRING_FIELDS: tuple[str, ...] = tuple(field for field in TELEMETRY_FIELDS if field not in NUMERIC_FIELDS and field != "timestamp")
CACHE_DIRNAME = "cache"
DEFAULT_CACHE_RELATIVE_PATH = Path("cache") / "parquet"
UNKNOWN_PARTITION = "__unknown__"


@dataclass(frozen=True)
class CacheBuildResult:
    """Summary returned after rebuilding the telemetry cache."""

    row_count: int
    source_file_count: int
    cache_path: Path


@dataclass(frozen=True)
class CacheStatus:
    """Freshness information for the Parquet telemetry cache."""

    exists: bool
    fresh: bool
    cache_path: Path
    source_file_count: int
    parquet_file_count: int
    latest_source_mtime: float | None
    latest_cache_mtime: float | None


def default_cache_dir(data_dir: Path | str = "data") -> Path:
    """Return the conventional cache location under a data directory."""

    return Path(data_dir) / DEFAULT_CACHE_RELATIVE_PATH


def discover_jsonl_files(data_dir: Path | str = "data") -> list[Path]:
    """Discover raw telemetry JSONL files recursively under ``data_dir``.

    Files under ``data/cache`` are ignored so the generated cache never becomes
    an input to itself if auxiliary files are added there later.
    """

    root = Path(data_dir)
    cache_root = (root / CACHE_DIRNAME).resolve()
    files: list[Path] = []
    for file_path in iter_jsonl_files(root, recursive=True):
        try:
            file_path.resolve().relative_to(cache_root)
        except ValueError:
            files.append(file_path)
    return files


def load_jsonl_records(files: Iterable[Path], *, data_dir: Path | str = "data") -> pd.DataFrame:
    """Load JSONL records safely into a normalized telemetry dataframe.

    Missing supported columns are created with NULL values. If records use the
    older ``machine`` key, it is copied into ``machine_id`` when ``machine_id``
    is absent. ``source_file`` is populated from the input file path unless the
    record already provides it.
    """

    root = Path(data_dir)
    rows: list[dict[str, Any]] = []
    for file_path in files:
        source = Path(file_path)
        try:
            source_label = str(source.relative_to(root))
        except ValueError:
            source_label = str(source)
        for record in iter_jsonl_records(source):
            normalized = {field: record.get(field) for field in TELEMETRY_FIELDS}
            if normalized["machine_id"] is None:
                normalized["machine_id"] = record.get("machine")
            if normalized["source_file"] is None:
                normalized["source_file"] = source_label
            rows.append(normalized)

    frame = pd.DataFrame(rows, columns=list(TELEMETRY_FIELDS))
    return _coerce_telemetry_frame(frame)


def rebuild_cache(data_dir: Path | str = "data", cache_dir: Path | str | None = None) -> CacheBuildResult:
    """Rebuild the full Parquet cache from raw JSONL files.

    The rebuild is atomic at the directory level: Parquet is written to a
    temporary sibling directory and then swapped into place. Running this command
    repeatedly rewrites the cache from JSONL source files instead of appending,
    so duplicate cache rows are not introduced by rebuilds.
    """

    root = Path(data_dir)
    output = Path(cache_dir) if cache_dir is not None else default_cache_dir(root)
    files = discover_jsonl_files(root)
    frame = load_jsonl_records(files, data_dir=root)

    output.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent))
    try:
        _write_partitioned_parquet(frame, temp_dir)
        if output.exists():
            shutil.rmtree(output)
        temp_dir.replace(output)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    return CacheBuildResult(row_count=len(frame), source_file_count=len(files), cache_path=output)


def cache_status(data_dir: Path | str = "data", cache_dir: Path | str | None = None) -> CacheStatus:
    """Return whether the cache exists and is at least as new as raw JSONL."""

    root = Path(data_dir)
    output = Path(cache_dir) if cache_dir is not None else default_cache_dir(root)
    sources = discover_jsonl_files(root)
    parquet_files = sorted(output.rglob("*.parquet")) if output.exists() else []
    latest_source_mtime = max((path.stat().st_mtime for path in sources), default=None)
    latest_cache_mtime = max((path.stat().st_mtime for path in parquet_files), default=None)
    exists = bool(parquet_files)
    fresh = exists and (latest_source_mtime is None or (latest_cache_mtime is not None and latest_cache_mtime >= latest_source_mtime))
    return CacheStatus(
        exists=exists,
        fresh=fresh,
        cache_path=output,
        source_file_count=len(sources),
        parquet_file_count=len(parquet_files),
        latest_source_mtime=latest_source_mtime,
        latest_cache_mtime=latest_cache_mtime,
    )


class TelemetryCache:
    """DuckDB query helper for the Parquet telemetry cache."""

    def __init__(self, cache_dir: Path | str = default_cache_dir()) -> None:
        self.cache_dir = Path(cache_dir)

    def exists(self) -> bool:
        return any(self.cache_dir.rglob("*.parquet")) if self.cache_dir.exists() else False

    def latest_sample_per_machine(self, *, as_dataframe: bool = False) -> list[dict[str, Any]] | pd.DataFrame:
        sql = f"""
            SELECT {', '.join(_quoted_columns())}
            FROM {_read_parquet_sql(self.cache_dir)}
            WHERE machine_id IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY machine_id ORDER BY timestamp DESC NULLS LAST, source_file DESC NULLS LAST) = 1
            ORDER BY machine_id
        """
        return self._query(sql, as_dataframe=as_dataframe)

    def samples_by_machine_and_time_range(
        self,
        machine_id: str,
        start: datetime | str,
        end: datetime | str,
        *,
        as_dataframe: bool = False,
    ) -> list[dict[str, Any]] | pd.DataFrame:
        sql = f"""
            SELECT {', '.join(_quoted_columns())}
            FROM {_read_parquet_sql(self.cache_dir)}
            WHERE machine_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
        """
        return self._query(sql, [machine_id, start, end], as_dataframe=as_dataframe)

    def samples_by_date_range(
        self,
        start_date: str,
        end_date: str,
        *,
        as_dataframe: bool = False,
    ) -> list[dict[str, Any]] | pd.DataFrame:
        sql = f"""
            SELECT {', '.join(_quoted_columns())}
            FROM {_read_parquet_sql(self.cache_dir)}
            WHERE CAST(timestamp AS DATE) >= CAST(? AS DATE)
              AND CAST(timestamp AS DATE) <= CAST(? AS DATE)
            ORDER BY timestamp, machine_id
        """
        return self._query(sql, [start_date, end_date], as_dataframe=as_dataframe)

    def machine_activity_summary(self, *, as_dataframe: bool = False) -> list[dict[str, Any]] | pd.DataFrame:
        sql = f"""
            SELECT
                machine_id,
                count(*) AS sample_count,
                min(timestamp) AS first_seen,
                max(timestamp) AS last_seen,
                count(DISTINCT CAST(timestamp AS DATE)) AS active_day_count,
                count(DISTINCT source_file) AS source_file_count
            FROM {_read_parquet_sql(self.cache_dir)}
            GROUP BY machine_id
            ORDER BY machine_id NULLS LAST
        """
        return self._query(sql, as_dataframe=as_dataframe)

    def to_dataframe(self, sql: str, parameters: Sequence[Any] | None = None) -> pd.DataFrame:
        """Run a custom DuckDB query and return a pandas DataFrame."""

        return self._query(sql, parameters, as_dataframe=True)

    def _query(
        self,
        sql: str,
        parameters: Sequence[Any] | None = None,
        *,
        as_dataframe: bool = False,
    ) -> list[dict[str, Any]] | pd.DataFrame:
        if not self.exists():
            empty = pd.DataFrame(columns=list(TELEMETRY_FIELDS))
            return empty if as_dataframe else []
        import duckdb

        with duckdb.connect(database=":memory:") as connection:
            result = connection.execute(sql, parameters or [])
            frame = result.fetchdf()
        return frame if as_dataframe else frame.to_dict(orient="records")


def _coerce_telemetry_frame(frame: pd.DataFrame) -> pd.DataFrame:
    for field in TELEMETRY_FIELDS:
        if field not in frame.columns:
            frame[field] = pd.NA
    frame = frame.loc[:, list(TELEMETRY_FIELDS)].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True)
    for field in STRING_FIELDS:
        frame[field] = frame[field].astype("string")
    for field in NUMERIC_FIELDS:
        frame[field] = pd.to_numeric(frame[field], errors="coerce")
    return frame


def _write_partitioned_parquet(frame: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if frame.empty:
        return

    import pyarrow as pa
    import pyarrow.parquet as pq

    writable = frame.copy()
    writable["_partition_machine"] = writable["machine_id"].map(_partition_value)
    writable["_partition_date"] = writable["timestamp"].dt.strftime("%Y-%m-%d").fillna(UNKNOWN_PARTITION)

    for (machine, date), group in writable.groupby(["_partition_machine", "_partition_date"], dropna=False, sort=True):
        partition_dir = output_dir / f"machine_id={machine}" / f"date={date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        payload = group.loc[:, list(TELEMETRY_FIELDS)]
        table = pa.Table.from_pandas(payload, preserve_index=False)
        pq.write_table(table, partition_dir / "part.parquet")


def _partition_value(value: Any) -> str:
    if pd.isna(value):
        return UNKNOWN_PARTITION
    text = str(value).strip()
    if not text:
        return UNKNOWN_PARTITION
    return text.replace("/", "_").replace("\\", "_").replace("=", "_")


def _quoted_columns() -> list[str]:
    return [f'"{field}"' for field in TELEMETRY_FIELDS]


def _read_parquet_sql(cache_dir: Path) -> str:
    pattern = (cache_dir / "**" / "*.parquet").as_posix().replace("'", "''")
    return f"read_parquet('{pattern}', hive_partitioning=false)"


def latest_cache_timestamp(status: CacheStatus) -> datetime | None:
    """Convert cache mtime from a ``CacheStatus`` to UTC datetime when present."""

    if status.latest_cache_mtime is None:
        return None
    return datetime.fromtimestamp(status.latest_cache_mtime, tz=timezone.utc)

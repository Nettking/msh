"""Date discovery, cache index, and session-filtered data creation.

The runner treats source JSONL as immutable enough for a size/mtime cache during
date discovery, then materializes per-session filtered copies. Filtering prefers
record timestamps but retains a filename-date fallback for older telemetry dumps
that had no timestamp field.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

ROOT_DIR = Path(__file__).resolve().parents[2]

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.common.time_utils import date_from_filename, parse_iso_timestamp, parse_timestamp_to_date
from catalog.runner.session_store import filter_signature, write_session_metadata

# Versioned JSON cache used only for date discovery.
DATA_INDEX_VERSION = 1
DATA_INDEX_FILE = ROOT_DIR / "results" / "runner" / "data_index.json"


def discover_available_dates(data_dir: Path) -> list[date]:
    """Discover all source dates, reusing a per-file cache when size/mtime match.

    The returned dates drive bootstrap and catch-up scheduling, so this function
    is conservative: changed files are reparsed, unchanged files reuse cached
    timestamp-derived or filename-fallback dates.
    """
    dates: set[date] = set()
    data_root = data_dir.resolve()
    index_data = _load_data_index()

    print(f"[runner] Loaded date cache from {DATA_INDEX_FILE}", flush=True)

    cached_root_entries = _load_cached_root_entries(index_data, data_root)
    updated_root_entries: dict[str, dict] = {}

    jsonl_files = list(iter_jsonl_files(data_dir, recursive=True))
    print(f"[runner] Found {len(jsonl_files)} JSONL files for date discovery", flush=True)

    reused_from_cache = 0
    reparsed_files = 0

    for file_path in jsonl_files:
        relative_path = file_path.resolve().relative_to(data_root).as_posix()
        stat_result = file_path.stat()
        file_size = stat_result.st_size
        file_mtime_ns = stat_result.st_mtime_ns
        cache_entry = cached_root_entries.get(relative_path)

        if (
            cache_entry is not None
            and cache_entry.get("size") == file_size
            and cache_entry.get("mtime_ns") == file_mtime_ns
        ):
            file_dates = _deserialize_dates(cache_entry.get("dates", []))
            date_source = str(cache_entry.get("date_source", "none"))
            has_timestamp_date = bool(cache_entry.get("has_timestamp_date", False))
            reused_from_cache += 1
        else:
            file_dates, date_source, has_timestamp_date = _discover_dates_for_file(file_path)
            reparsed_files += 1

        dates.update(file_dates)
        updated_root_entries[relative_path] = {
            "size": file_size,
            "mtime_ns": file_mtime_ns,
            "dates": _serialize_dates(file_dates),
            "date_source": date_source,
            "has_timestamp_date": has_timestamp_date,
        }

    print(
        f"[runner] Date discovery reused {reused_from_cache} cached files and reparsed {reparsed_files} files",
        flush=True,
    )

    _store_cached_root_entries(index_data, data_root, updated_root_entries)

    print(f"[runner] Writing date cache to {DATA_INDEX_FILE}", flush=True)
    _write_data_index(index_data)

    return sorted(dates)


def filter_data_by_date_range(
    source_data_dir: Path,
    destination_data_dir: Path,
    start_date: date,
    end_date: date,
    *,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> tuple[int, int]:
    """Filter JSONL records into a destination directory based on date or hour range.

    Timestamp-bearing records are validated against their own timestamp. For
    legacy files with no parseable timestamps at all, filename-date fallback is
    allowed for date ranges. Hour filtering never uses filename fallback because
    an hour cannot be inferred safely from the path.
    """
    destination_data_dir.mkdir(parents=True, exist_ok=True)
    use_hour_filter = start_date == end_date and start_hour is not None and end_hour is not None

    matched_records = 0
    written_files = 0
    processed_files = 0

    source_files = list(iter_jsonl_files(source_data_dir, recursive=True))
    print(
        f"[runner] Filtering {len(source_files)} files for range {start_date.isoformat()}..{end_date.isoformat()}",
        flush=True,
    )

    for source_file in source_files:
        relative_path = source_file.relative_to(source_data_dir)
        destination_file = destination_data_dir / relative_path
        destination_file.parent.mkdir(parents=True, exist_ok=True)

        file_matched = 0
        fallback_file_date = date_from_filename(source_file)
        file_in_fallback_window = fallback_file_date is not None and start_date <= fallback_file_date <= end_date

        parsed_records: list[dict] = []
        file_has_timestamp = False

        # First pass determines whether this file has any trustworthy record
        # timestamps. Filename fallback is only valid when the whole file lacks
        # timestamp dates; mixing fallback and timestamp filtering would duplicate
        # or mis-scope partially malformed files.
        for record in iter_jsonl_records(source_file):
            parsed_records.append(record)
            if parse_timestamp_to_date(str(record.get("timestamp", ""))) is not None:
                file_has_timestamp = True

        with destination_file.open("w", encoding="utf-8") as dst:
            for record in parsed_records:
                if use_hour_filter:
                    record_dt = _parse_timestamp_to_datetime(str(record.get("timestamp", "")))
                    if record_dt is None:
                        continue
                    if record_dt.date() != start_date:
                        continue
                    if start_hour <= record_dt.hour <= end_hour:
                        dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                        matched_records += 1
                        file_matched += 1
                    continue

                record_date = parse_timestamp_to_date(str(record.get("timestamp", "")))
                if record_date is None:
                    if not file_has_timestamp and file_in_fallback_window:
                        dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                        matched_records += 1
                        file_matched += 1
                    continue

                if start_date <= record_date <= end_date:
                    dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                    matched_records += 1
                    file_matched += 1

        if file_matched > 0:
            written_files += 1
        else:
            destination_file.unlink(missing_ok=True)

        processed_files += 1
        if processed_files % 25 == 0:
            print(
                f"[runner] Filter progress: processed {processed_files}/{len(source_files)} files "
                f"(matched files: {written_files}, matched records: {matched_records})",
                flush=True,
            )

    print(
        f"[runner] Filter complete: processed {processed_files} files, wrote {written_files} files, "
        f"matched {matched_records} records",
        flush=True,
    )

    return matched_records, written_files


def ensure_session_filtered_data(
    *,
    source_data_dir: Path,
    session_dir: Path,
    metadata: dict,
) -> tuple[int, int, str]:
    """Ensure the session-scoped filtered dataset exists for current metadata.

    This is the cache boundary between raw telemetry and session artifacts. If
    the filter signature and filtered output directory look valid, data is reused;
    otherwise the old filtered copy is removed and regenerated from source JSONL.
    """
    filtered_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    filter_result = metadata.setdefault("filter_result", {})
    if _session_filter_cache_is_valid(session_dir, metadata):
        return int(filter_result["matched_records"]), int(filter_result["matched_files"]), "cached"

    if filtered_data_dir.exists():
        shutil.rmtree(filtered_data_dir)

    filter_config = metadata["filter"]
    matched_records, matched_files = filter_data_by_date_range(
        source_data_dir,
        filtered_data_dir,
        date.fromisoformat(str(filter_config["start_date"])),
        date.fromisoformat(str(filter_config["end_date"])),
        start_hour=int(filter_config["start_hour"]) if filter_config.get("start_hour") is not None else None,
        end_hour=int(filter_config["end_hour"]) if filter_config.get("end_hour") is not None else None,
    )
    filter_result["matched_records"] = matched_records
    filter_result["matched_files"] = matched_files
    filter_result["filtered_data_path"] = metadata["paths"]["filtered_data_dir"]
    filter_result["generated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    write_session_metadata(session_dir, metadata)
    return matched_records, matched_files, "created"


def _parse_timestamp_to_datetime(timestamp: str | None) -> datetime | None:
    """Parse a timestamp string into a datetime for hour-level filtering."""
    parsed = parse_iso_timestamp(timestamp, allow_z_suffix=True)
    if parsed is not None:
        return parsed

    value = str(timestamp).strip() if timestamp is not None else ""
    match = re.search(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})", value)
    if not match:
        return None

    try:
        return datetime(
            year=int(match.group(1)[0:4]),
            month=int(match.group(1)[5:7]),
            day=int(match.group(1)[8:10]),
            hour=int(match.group(2)),
            minute=int(match.group(3)),
            second=int(match.group(4)),
        )
    except ValueError:
        return None


def _filtered_data_looks_usable(filtered_data_dir: Path, *, expect_files: bool) -> bool:
    """Perform a lightweight sanity check that cached filtered data is usable."""
    if not filtered_data_dir.exists() or not filtered_data_dir.is_dir():
        return False
    if not expect_files:
        return True
    return any(filtered_data_dir.rglob("*.jsonl"))


def _session_filter_cache_is_valid(session_dir: Path, metadata: dict) -> bool:
    """Return whether filtered data can be reused for the current filter signature."""
    filtered_data_dir = session_dir / str(metadata.get("paths", {}).get("filtered_data_dir", "data"))
    filter_result = metadata.get("filter_result", {})
    if not filtered_data_dir.exists():
        return False
    if filter_result.get("matched_records") is None or filter_result.get("matched_files") is None:
        return False
    matched_files = int(filter_result.get("matched_files", 0))
    if not _filtered_data_looks_usable(filtered_data_dir, expect_files=matched_files > 0):
        return False
    filter_payload = metadata.get("filter", {})
    expected_signature = filter_signature(
        {
            "start_date": filter_payload.get("start_date"),
            "end_date": filter_payload.get("end_date"),
            "start_hour": filter_payload.get("start_hour"),
            "end_hour": filter_payload.get("end_hour"),
        }
    )
    return str(metadata.get("session_config_signature", "")) == expected_signature


def _discover_dates_for_file(file_path: Path) -> tuple[set[date], str, bool]:
    """Discover dates represented by one JSONL file.

    Record timestamps win over filename dates. Filename fallback is retained for
    historical raw dumps where the file path encoded the date but records did not.
    """
    file_dates: set[date] = set()
    file_has_timestamp_date = False

    for record in iter_jsonl_records(file_path):
        record_date = parse_timestamp_to_date(str(record.get("timestamp", "")))
        if record_date is not None:
            file_has_timestamp_date = True
            file_dates.add(record_date)

    if file_has_timestamp_date:
        return file_dates, "timestamp", True

    fallback = date_from_filename(file_path)
    if fallback is not None:
        return {fallback}, "filename_fallback", False

    return set(), "none", False


def _serialize_dates(dates: set[date]) -> list[str]:
    """Convert a set of dates into sorted ISO strings for JSON cache storage."""
    return sorted(day.isoformat() for day in dates)


def _deserialize_dates(serialized_dates: object) -> set[date]:
    """Convert cached ISO date strings back into ``date`` objects."""
    parsed_dates: set[date] = set()
    if not isinstance(serialized_dates, list):
        return parsed_dates

    for value in serialized_dates:
        if not isinstance(value, str):
            continue
        try:
            parsed_dates.add(date.fromisoformat(value))
        except ValueError:
            continue

    return parsed_dates


def _load_data_index() -> dict:
    """Load the runner date-discovery cache from disk."""
    if not DATA_INDEX_FILE.exists():
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    try:
        parsed = json.loads(DATA_INDEX_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    if not isinstance(parsed, dict):
        return {"version": DATA_INDEX_VERSION, "roots": {}}
    if parsed.get("version") != DATA_INDEX_VERSION:
        return {"version": DATA_INDEX_VERSION, "roots": {}}
    if not isinstance(parsed.get("roots"), dict):
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    return parsed


def _load_cached_root_entries(index_data: dict, data_root: Path) -> dict[str, dict]:
    """Extract cached file entries for one data root."""
    roots = index_data.get("roots")
    if not isinstance(roots, dict):
        return {}

    root_entry = roots.get(str(data_root))
    if not isinstance(root_entry, dict):
        return {}

    files = root_entry.get("files")
    if not isinstance(files, dict):
        return {}

    return {key: value for key, value in files.items() if isinstance(key, str) and isinstance(value, dict)}


def _store_cached_root_entries(index_data: dict, data_root: Path, file_entries: dict[str, dict]) -> None:
    """Store updated cached file entries for one data root."""
    roots = index_data.setdefault("roots", {})
    if not isinstance(roots, dict):
        index_data["roots"] = {}
        roots = index_data["roots"]

    roots[str(data_root)] = {"files": file_entries}


def _write_data_index(index_data: dict) -> None:
    """Write the runner date-discovery cache to disk atomically."""
    DATA_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=DATA_INDEX_FILE.parent, delete=False) as tmp:
        json.dump(index_data, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)

    temp_path.replace(DATA_INDEX_FILE)

"""Date discovery, cache index, and session-filtered data creation.

The runner treats source JSONL as immutable enough for a size/mtime cache during
index refresh, then materializes per-session filtered copies. Filtering prefers
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
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.common.time_utils import date_from_filename, parse_iso_timestamp, parse_timestamp_to_date
from catalog.runner.session_store import filter_signature, write_session_metadata

# Versioned JSON cache used for date discovery and conservative file pruning.
DATA_INDEX_VERSION = 2
DATA_INDEX_FILE = ROOT_DIR / "results" / "runner" / "data_index.json"


def discover_available_dates(data_dir: Path) -> list[date]:
    """Discover all source dates, reusing indexed metadata when size/mtime match.

    The returned dates drive bootstrap and catch-up scheduling, so this function
    is conservative: changed files are reparsed, unchanged files reuse cached
    timestamp-derived or filename-fallback dates.
    """
    print(f"[runner] Loaded data index from {DATA_INDEX_FILE}", flush=True)
    index_data, root_entries, stats = _refresh_data_index_for_root(data_dir)

    dates: set[date] = set()
    for entry in root_entries.values():
        dates.update(_deserialize_dates(entry.get("dates", [])))

    print(f"[runner] Found {stats['total_files']} JSONL files for date discovery", flush=True)
    print(
        f"[runner] Date discovery reused {stats['reused_files']} cached files and reparsed {stats['reparsed_files']} files",
        flush=True,
    )
    print(f"[runner] Total indexed files: {len(root_entries)}", flush=True)
    print(f"[runner] Writing data index to {DATA_INDEX_FILE}", flush=True)
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
    source_root = source_data_dir.resolve()
    use_hour_filter = start_date == end_date and start_hour is not None and end_hour is not None

    index_data, root_entries, _stats = _refresh_data_index_for_root(source_data_dir)
    _write_data_index(index_data)

    candidate_entries = _select_candidate_entries(root_entries, start_date, end_date)
    print(
        f"[runner] Filtering {len(root_entries)} indexed files for {start_date.isoformat()}..{end_date.isoformat()}",
        flush=True,
    )
    print(f"[runner] Index pruning selected {len(candidate_entries)} candidate files", flush=True)

    matched_records = 0
    written_files = 0
    opened_files = 0

    for relative_path, _entry in candidate_entries:
        source_file = source_root / relative_path
        if not source_file.is_file():
            continue
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
        opened_files += 1
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

        if opened_files % 25 == 0:
            print(
                f"[runner] Filter progress: opened {opened_files}/{len(candidate_entries)} candidate files "
                f"(matched files: {written_files}, matched records: {matched_records})",
                flush=True,
            )

    print(
        f"[runner] Opened {opened_files} files, matched {written_files} files, matched {matched_records} records",
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


def _refresh_data_index_for_root(data_dir: Path) -> tuple[dict, dict[str, dict], dict[str, int]]:
    """Load, incrementally refresh, and return index entries for one data root."""
    data_root = data_dir.resolve()
    index_data = _load_data_index()
    cached_root_entries = _load_cached_root_entries(index_data, data_root)
    updated_root_entries: dict[str, dict] = {}
    reused_files = 0
    reparsed_files = 0

    jsonl_files = list(iter_jsonl_files(data_dir, recursive=True))
    for file_path in jsonl_files:
        relative_path = file_path.resolve().relative_to(data_root).as_posix()
        stat_result = file_path.stat()
        cache_entry = cached_root_entries.get(relative_path)

        if _cache_entry_matches_file(cache_entry, file_path, stat_result):
            updated_root_entries[relative_path] = dict(cache_entry)
            reused_files += 1
            continue

        updated_root_entries[relative_path] = _index_jsonl_file(file_path, data_root, stat_result)
        reparsed_files += 1

    _store_cached_root_entries(index_data, data_root, updated_root_entries)
    return index_data, updated_root_entries, {
        "total_files": len(jsonl_files),
        "reused_files": reused_files,
        "reparsed_files": reparsed_files,
        "deleted_files": max(0, len(cached_root_entries) - len(updated_root_entries)),
    }


def _cache_entry_matches_file(cache_entry: object, file_path: Path, stat_result: Any) -> bool:
    """Return whether an index entry is valid for the current file stat."""
    if not isinstance(cache_entry, dict):
        return False
    path_matches = cache_entry.get("file_path") in {str(file_path), file_path.as_posix(), file_path.resolve().as_posix()}
    return (
        path_matches
        and cache_entry.get("file_size") == stat_result.st_size
        and cache_entry.get("mtime_ns") == stat_result.st_mtime_ns
    )


def _index_jsonl_file(file_path: Path, data_root: Path, stat_result: Any | None = None) -> dict:
    """Parse one JSONL file into conservative filtering metadata."""
    stat_result = file_path.stat() if stat_result is None else stat_result
    relative_path = file_path.resolve().relative_to(data_root).as_posix()
    filename_date = date_from_filename(file_path)
    timestamp_dates: set[date] = set()
    min_dt: datetime | None = None
    max_dt: datetime | None = None
    record_count = 0
    machine_ids: set[str] = set()

    for record in iter_jsonl_records(file_path):
        record_count += 1
        for key in ("machine_id", "machine", "machineId"):
            machine_id = record.get(key)
            if machine_id not in (None, ""):
                machine_ids.add(str(machine_id))
                break

        record_dt = _parse_timestamp_to_datetime(str(record.get("timestamp", "")))
        if record_dt is not None:
            index_dt = _normalize_datetime_for_index(record_dt)
            min_dt = index_dt if min_dt is None or index_dt < min_dt else min_dt
            max_dt = index_dt if max_dt is None or index_dt > max_dt else max_dt
            timestamp_dates.add(record_dt.date())
            continue

        record_date = parse_timestamp_to_date(str(record.get("timestamp", "")))
        if record_date is not None:
            timestamp_dates.add(record_date)

    if timestamp_dates:
        dates = timestamp_dates
        date_source = "timestamp"
        has_timestamp_date = True
    elif filename_date is not None:
        dates = {filename_date}
        date_source = "filename_fallback"
        has_timestamp_date = False
    else:
        dates = set()
        date_source = "none"
        has_timestamp_date = False

    return {
        "file_path": file_path.resolve().as_posix(),
        "relative_path": relative_path,
        "file_size": stat_result.st_size,
        "size": stat_result.st_size,
        "modified_time": datetime.utcfromtimestamp(stat_result.st_mtime).replace(microsecond=0).isoformat() + "Z",
        "mtime": stat_result.st_mtime,
        "mtime_ns": stat_result.st_mtime_ns,
        "filename_date": filename_date.isoformat() if filename_date is not None else None,
        "min_timestamp": min_dt.isoformat() if min_dt is not None else None,
        "max_timestamp": max_dt.isoformat() if max_dt is not None else None,
        "record_count": record_count,
        "machine_ids": sorted(machine_ids),
        "dates": _serialize_dates(dates),
        "date_source": date_source,
        "has_timestamp_date": has_timestamp_date,
    }


def _normalize_datetime_for_index(value: datetime) -> datetime:
    """Return a timezone-naive datetime so mixed timestamp formats remain sortable."""
    return value.replace(tzinfo=None)


def _select_candidate_entries(root_entries: dict[str, dict], start_date: date, end_date: date) -> list[tuple[str, dict]]:
    """Return files that may contain records in the requested date range."""
    candidates: list[tuple[str, dict]] = []
    for relative_path, entry in sorted(root_entries.items()):
        if _entry_can_be_pruned(entry, start_date, end_date):
            continue
        candidates.append((relative_path, entry))
    return candidates


def _entry_can_be_pruned(entry: dict, start_date: date, end_date: date) -> bool:
    """Conservatively decide whether indexed metadata excludes a file."""
    min_timestamp_date = _date_from_index_timestamp(entry.get("min_timestamp"))
    max_timestamp_date = _date_from_index_timestamp(entry.get("max_timestamp"))
    if min_timestamp_date is not None and max_timestamp_date is not None:
        return max_timestamp_date < start_date or min_timestamp_date > end_date

    filename_date = _date_from_index_value(entry.get("filename_date"))
    if filename_date is not None:
        return filename_date < start_date or filename_date > end_date

    # Unknown metadata must be opened to preserve compatibility/correctness.
    return False


def _date_from_index_timestamp(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = _parse_timestamp_to_datetime(value)
    if parsed is not None:
        return parsed.date()
    return parse_timestamp_to_date(value)


def _date_from_index_value(value: object) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _discover_dates_for_file(file_path: Path) -> tuple[set[date], str, bool]:
    """Discover dates represented by one JSONL file.

    Record timestamps win over filename dates. Filename fallback is retained for
    historical raw dumps where the file path encoded the date but records did not.
    """
    entry = _index_jsonl_file(file_path, file_path.parent)
    return (
        _deserialize_dates(entry.get("dates", [])),
        str(entry.get("date_source", "none")),
        bool(entry.get("has_timestamp_date", False)),
    )


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
    """Load the runner data index from disk."""
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
    """Write the runner data index to disk atomically."""
    DATA_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=DATA_INDEX_FILE.parent, delete=False) as tmp:
        json.dump(index_data, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)

    temp_path.replace(DATA_INDEX_FILE)

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import ast
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from tempfile import mkdtemp
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.common.time_utils import date_from_filename, parse_timestamp_to_date

DEFAULT_SCRIPT_EXCLUSIONS = {
    "runner",
    "auto_connect",
    "data_simulator",
    "interventions",
    "standalone_recorder",
    "standalone-recorder_v2",
}

DATA_INDEX_VERSION = 1
DATA_INDEX_FILE = ROOT_DIR / "results" / "runner" / "data_index.json"


@dataclass(frozen=True)
class ScriptOption:
    number: int
    key: str
    script_path: Path
    description: str


def repo_root() -> Path:
    # catalog/runner/menu_utils.py -> repo root is two parents up
    return Path(__file__).resolve().parents[2]


def _script_description(script_path: Path, fallback: str) -> str:
    try:
        source = script_path.read_text(encoding="utf-8")
        module = ast.parse(source)
        docstring = ast.get_docstring(module)
    except (OSError, SyntaxError, UnicodeDecodeError):
        docstring = None

    if not docstring:
        return fallback

    first_line = docstring.strip().splitlines()[0].strip()
    return first_line or fallback


def discover_runnable_scripts(catalog_dir: Path) -> list[ScriptOption]:
    script_items: list[tuple[str, Path, str]] = []

    for folder in sorted(catalog_dir.iterdir()):
        if not folder.is_dir():
            continue
        if folder.name in DEFAULT_SCRIPT_EXCLUSIONS:
            continue

        convention_script = folder / f"{folder.name}.py"
        main_script = folder / "main.py"

        selected_script: Path | None = None
        if convention_script.exists():
            selected_script = convention_script
            key = folder.name
        elif main_script.exists():
            selected_script = main_script
            key = folder.name
        else:
            continue

        fallback_description = key.replace("_", " ").replace("-", " ")
        description = _script_description(selected_script, fallback_description)
        script_items.append((key, selected_script.relative_to(repo_root()), description))

    script_items.sort(key=lambda item: item[0].lower())
    return [
        ScriptOption(number=index, key=key, script_path=script_path, description=description)
        for index, (key, script_path, description) in enumerate(script_items, start=1)
    ]


def discover_available_dates(data_dir: Path) -> list[date]:
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


def filter_data_by_date_range(source_data_dir: Path, destination_data_dir: Path, start_date: date, end_date: date) -> tuple[int, int]:
    destination_data_dir.mkdir(parents=True, exist_ok=True)
    matched_records = 0
    written_files = 0
    processed_files = 0
    source_files = list(iter_jsonl_files(source_data_dir, recursive=True))
    print(f"[runner] Filtering {len(source_files)} files for range {start_date.isoformat()}..{end_date.isoformat()}", flush=True)

    for source_file in source_files:
        relative_path = source_file.relative_to(source_data_dir)
        destination_file = destination_data_dir / relative_path
        destination_file.parent.mkdir(parents=True, exist_ok=True)

        file_matched = 0
        fallback_file_date = date_from_filename(source_file)
        file_in_fallback_window = fallback_file_date is not None and start_date <= fallback_file_date <= end_date

        parsed_records: list[dict] = []
        file_has_timestamp = False
        for record in iter_jsonl_records(source_file):
            parsed_records.append(record)
            if parse_timestamp_to_date(str(record.get("timestamp", ""))) is not None:
                file_has_timestamp = True

        with destination_file.open("w", encoding="utf-8") as dst:
            for record in parsed_records:
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


def print_numbered_menu(title: str, options: Iterable[str]) -> None:
    print(f"\n{title}", flush=True)
    for index, option in enumerate(options, start=1):
        print(f"{index}) {option}", flush=True)


def prompt_menu_choice(max_choice: int, prompt: str) -> int:
    while True:
        raw = input(prompt).strip()
        if not raw.isdigit():
            print("Please enter a number.", flush=True)
            continue
        value = int(raw)
        if 1 <= value <= max_choice:
            return value
        print(f"Please choose a value between 1 and {max_choice}.", flush=True)


def create_run_workspace(output_base_dir: Path) -> Path:
    output_base_dir.mkdir(parents=True, exist_ok=True)
    path = Path(mkdtemp(prefix="menu_run_", dir=output_base_dir))
    return path


def run_script(script_path: Path, workspace_dir: Path) -> int:
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("MPLBACKEND", "Agg")

    command = [sys.executable, str(script_path)]
    print(f"\nRunning: {' '.join(command)}", flush=True)
    print(f"Working directory: {workspace_dir}", flush=True)

    completed = subprocess.run(command, cwd=workspace_dir, env=env)
    return completed.returncode


def copy_repo_catalog_into_workspace(workspace_dir: Path) -> None:
    source_catalog = repo_root() / "catalog"
    target_catalog = workspace_dir / "catalog"
    if target_catalog.exists():
        shutil.rmtree(target_catalog)
    shutil.copytree(source_catalog, target_catalog)


def _discover_dates_for_file(file_path: Path) -> tuple[set[date], str, bool]:
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
    return sorted(day.isoformat() for day in dates)


def _deserialize_dates(serialized_dates: object) -> set[date]:
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
    roots = index_data.setdefault("roots", {})
    if not isinstance(roots, dict):
        index_data["roots"] = {}
        roots = index_data["roots"]
    roots[str(data_root)] = {"files": file_entries}


def _write_data_index(index_data: dict) -> None:
    DATA_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=DATA_INDEX_FILE.parent, delete=False) as tmp:
        json.dump(index_data, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)
    temp_path.replace(DATA_INDEX_FILE)

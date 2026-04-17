from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any


def iter_jsonl_files(data_dir: Path | str, *, recursive: bool = True) -> Iterator[Path]:
    root = Path(data_dir)
    pattern = "*.jsonl"
    iterator = root.rglob(pattern) if recursive else root.glob(pattern)

    for file_path in sorted(iterator):
        if file_path.is_file():
            yield file_path


def iter_jsonl_records(file_path: Path | str, *, on_malformed_json: Callable[[str], None] | None = None) -> Iterator[dict[str, Any]]:
    source = Path(file_path)

    with source.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                if on_malformed_json is not None:
                    on_malformed_json(f"{source} line {line_number}: {exc}")
                continue

            if isinstance(parsed, dict):
                yield parsed


def iter_records_in_dir(
    data_dir: Path | str,
    *,
    recursive: bool = True,
    on_malformed_json: Callable[[str], None] | None = None,
) -> Iterator[tuple[Path, dict[str, Any]]]:
    for file_path in iter_jsonl_files(data_dir, recursive=recursive):
        for record in iter_jsonl_records(file_path, on_malformed_json=on_malformed_json):
            yield file_path, record

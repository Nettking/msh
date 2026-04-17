"""
Shared helpers for reading JSONL files used by catalog scripts.

This module provides small iterator-based utilities for:

- finding JSONL files in a directory
- reading line-delimited JSON records from a file
- iterating over all records across a directory of JSONL files

Behavior:
- files are read as UTF-8 text
- blank lines are skipped
- malformed JSON lines are skipped
- non-dictionary JSON values are ignored
- file iteration order is sorted for deterministic processing
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any


def iter_jsonl_files(data_dir: Path | str, *, recursive: bool = True) -> Iterator[Path]:
    """
    Yield JSONL files from a directory in sorted order.

    Parameters
    ----------
    data_dir : Path | str
        Directory to scan for ``*.jsonl`` files.
    recursive : bool, default=True
        If True, search recursively using ``rglob``.
        If False, search only the top level using ``glob``.

    Yields
    ------
    pathlib.Path
        Paths to existing JSONL files.

    Notes
    -----
    Only files matching ``*.jsonl`` are yielded. Non-file matches are ignored.
    """
    root = Path(data_dir)
    pattern = "*.jsonl"
    iterator = root.rglob(pattern) if recursive else root.glob(pattern)

    for file_path in sorted(iterator):
        if file_path.is_file():
            yield file_path


def iter_jsonl_records(
    file_path: Path | str,
    *,
    on_malformed_json: Callable[[str], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Yield dictionary records from a JSONL file.

    Parameters
    ----------
    file_path : Path | str
        Path to the JSONL file to read.
    on_malformed_json : Callable[[str], None] | None, optional
        Optional callback invoked with an error message when a line cannot be
        parsed as JSON.

    Yields
    ------
    dict[str, Any]
        Parsed JSON objects for lines that contain valid JSON dictionaries.

    Behavior
    --------
    - Blank lines are skipped.
    - Malformed JSON lines are skipped.
    - JSON values that are not dictionaries are ignored.

    Notes
    -----
    This function is intentionally tolerant: it continues reading after
    malformed lines rather than aborting the file.
    """
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
    """
    Yield `(file_path, record)` pairs for all JSONL records in a directory.

    Parameters
    ----------
    data_dir : Path | str
        Directory containing JSONL files.
    recursive : bool, default=True
        If True, search recursively for JSONL files.
        If False, search only the top level.
    on_malformed_json : Callable[[str], None] | None, optional
        Optional callback invoked when a JSONL line cannot be parsed.

    Yields
    ------
    tuple[pathlib.Path, dict[str, Any]]
        A pair containing the source file path and one parsed dictionary record
        from that file.

    Notes
    -----
    This is a convenience wrapper combining ``iter_jsonl_files`` and
    ``iter_jsonl_records``.
    """
    for file_path in iter_jsonl_files(data_dir, recursive=recursive):
        for record in iter_jsonl_records(file_path, on_malformed_json=on_malformed_json):
            yield file_path, record
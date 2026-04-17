"""
Utility functions for parsing timestamps and extracting dates.

This module provides tolerant parsing helpers used across telemetry scripts.
It focuses on:

- converting ISO-like timestamp strings to ``datetime`` or ``date``
- handling common variations such as trailing ``Z`` (UTC)
- extracting dates from filenames when timestamps are unavailable

Behavior:
- parsing is intentionally permissive (returns None instead of raising)
- MTConnect-style timestamps are partially supported via fallback logic
- filename-based date extraction is used as a last resort in some pipelines
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

# Matches either:
# - ISO date: YYYY-MM-DD
# - Compact date: YYYYMMDD
DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{8})")


def parse_iso_timestamp(timestamp: str | None, *, allow_z_suffix: bool = False) -> datetime | None:
    """
    Parse an ISO-like timestamp string into a ``datetime``.

    Parameters
    ----------
    timestamp : str | None
        Input timestamp string.
    allow_z_suffix : bool, default=False
        If True, convert trailing 'Z' (UTC) into '+00:00' so it can be parsed
        by ``datetime.fromisoformat``.

    Returns
    -------
    datetime | None
        Parsed datetime if successful, otherwise None.

    Behavior
    --------
    - Returns None for missing, empty, or invalid input.
    - Does not raise exceptions.
    - Only supports formats compatible with ``datetime.fromisoformat`` after
      optional normalization.

    Notes
    -----
    Python's ``fromisoformat`` does not accept a trailing 'Z', which is common
    in MTConnect and other telemetry sources. ``allow_z_suffix=True`` enables
    a simple normalization for this case.
    """
    if timestamp is None:
        return None

    value = str(timestamp).strip()
    if not value:
        return None

    if allow_z_suffix and value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_timestamp_to_date(timestamp: str | None) -> date | None:
    """
    Extract a ``date`` from a timestamp string.

    Parameters
    ----------
    timestamp : str | None
        Input timestamp string.

    Returns
    -------
    date | None
        Extracted date if successful, otherwise None.

    Behavior
    --------
    1. Attempts full ISO parsing via ``parse_iso_timestamp``.
    2. If that fails, falls back to extracting a YYYY-MM-DD substring.

    Notes
    -----
    The fallback exists primarily for MTConnect-style timestamps such as:
    ``2026-03-01T12:00:00.0000000Z``

    This function intentionally tolerates imperfect data, but may produce
    less precise results if relying on fallback extraction.
    """
    dt_value = parse_iso_timestamp(timestamp, allow_z_suffix=True)
    if dt_value is not None:
        return dt_value.date()

    # Fallback: extract date substring (e.g., from MTConnect-style timestamps)
    value = str(timestamp).strip() if timestamp is not None else ""
    mtconnect_date = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    if mtconnect_date:
        try:
            return date.fromisoformat(mtconnect_date.group(1))
        except ValueError:
            return None

    return None


def date_from_filename(path: Path | str) -> date | None:
    """
    Extract a date from a filename.

    Parameters
    ----------
    path : Path | str
        File path whose name may contain a date.

    Returns
    -------
    date | None
        Extracted date if a recognizable pattern is found, otherwise None.

    Behavior
    --------
    - Searches the filename for:
        - YYYY-MM-DD
        - YYYYMMDD
    - Converts compact format (YYYYMMDD) to ISO format before parsing.

    Notes
    -----
    This function is typically used as a fallback when timestamps are missing
    or unparseable. It assumes filenames encode a meaningful date, which may
    not always be true.
    """
    file_path = Path(path)
    match = DATE_IN_NAME.search(file_path.name)
    if not match:
        return None

    token = match.group(1)

    # Normalize YYYYMMDD → YYYY-MM-DD
    if len(token) == 8:
        token = f"{token[0:4]}-{token[4:6]}-{token[6:8]}"

    try:
        return date.fromisoformat(token)
    except ValueError:
        return None
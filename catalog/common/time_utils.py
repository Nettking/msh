from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2}|\d{8})")


def parse_iso_timestamp(timestamp: str | None, *, allow_z_suffix: bool = False) -> datetime | None:
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
    dt_value = parse_iso_timestamp(timestamp, allow_z_suffix=True)
    if dt_value is not None:
        return dt_value.date()

    # fallback for common MTConnect format: 2026-03-01T12:00:00.0000000Z
    value = str(timestamp).strip() if timestamp is not None else ""
    mtconnect_date = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    if mtconnect_date:
        try:
            return date.fromisoformat(mtconnect_date.group(1))
        except ValueError:
            return None

    return None


def date_from_filename(path: Path | str) -> date | None:
    file_path = Path(path)
    match = DATE_IN_NAME.search(file_path.name)
    if not match:
        return None

    token = match.group(1)
    if len(token) == 8:
        token = f"{token[0:4]}-{token[4:6]}-{token[6:8]}"

    try:
        return date.fromisoformat(token)
    except ValueError:
        return None

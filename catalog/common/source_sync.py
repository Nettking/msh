"""Small helpers for source-specific telemetry synchronization.

The helpers in this module deliberately store synchronization state as JSON files
under ``data/source_state``. They do not ingest data themselves. Source-specific
connectors, such as the Observer Phoenix exporter, use this module to keep simple
high-water marks without coupling the rest of FCP to one vendor API.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any


UTC_SUFFIX = "Z"


class SourceSyncError(RuntimeError):
    """Raised when source synchronization state cannot be interpreted safely."""


@dataclass
class SourceSyncState:
    """Persisted synchronization metadata for one external source."""

    source_name: str
    version: int = 1
    last_successful_sync_utc: str | None = None
    watermarks: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_watermark(self, name: str) -> str | None:
        value = self.watermarks.get(name)
        return str(value) if value else None

    def set_watermark(self, name: str, value: datetime | str) -> None:
        self.watermarks[name] = format_utc(value)

    def mark_success(self, *, synced_until_utc: datetime | str | None = None, **metadata: Any) -> None:
        self.last_successful_sync_utc = format_utc(utc_now())
        if synced_until_utc is not None:
            self.set_watermark("default", synced_until_utc)
        self.metadata.update(metadata)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def parse_utc(value: datetime | str) -> datetime:
    """Parse a UTC timestamp and return a timezone-aware datetime.

    The source APIs used by FCP commonly return ISO 8601 strings either with a
    trailing ``Z`` or with no timezone marker while documenting UTC semantics.
    Naive values are therefore interpreted as UTC.
    """

    if isinstance(value, datetime):
        result = value
    else:
        text = value.strip()
        if not text:
            raise SourceSyncError("UTC timestamp value is empty.")
        if text.endswith(UTC_SUFFIX):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise SourceSyncError(f"Invalid UTC timestamp: {value!r}") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc).replace(microsecond=0)


def format_utc(value: datetime | str) -> str:
    return parse_utc(value).isoformat().replace("+00:00", UTC_SUFFIX)


def subtract_overlap(value: datetime | str, overlap_minutes: int) -> datetime:
    overlap = max(0, int(overlap_minutes))
    return parse_utc(value) - timedelta(minutes=overlap)


def state_path(data_dir: Path, source_name: str) -> Path:
    safe_source = source_name.strip().replace("/", "_").replace("\\", "_")
    if not safe_source:
        raise SourceSyncError("source_name must not be empty.")
    return data_dir / "source_state" / f"{safe_source}.json"


def load_state(data_dir: Path, source_name: str) -> SourceSyncState:
    path = state_path(data_dir, source_name)
    if not path.exists():
        return SourceSyncState(source_name=source_name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceSyncError(f"Could not read source sync state: {path}") from exc
    if not isinstance(payload, dict):
        raise SourceSyncError(f"Source sync state is not a JSON object: {path}")
    return SourceSyncState(
        source_name=str(payload.get("source_name") or source_name),
        version=int(payload.get("version") or 1),
        last_successful_sync_utc=payload.get("last_successful_sync_utc"),
        watermarks={str(k): str(v) for k, v in dict(payload.get("watermarks") or {}).items()},
        metadata=dict(payload.get("metadata") or {}),
    )


def save_state(data_dir: Path, state: SourceSyncState) -> Path:
    path = state_path(data_dir, state.source_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path

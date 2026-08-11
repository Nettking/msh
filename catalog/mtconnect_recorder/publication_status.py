"""Bounded, public-safe recorder publication diagnostics.

The managed recorder and Flask run in separate processes.  This module defines
the tiny file contract they use to report recorder-to-Federation publication
health without copying reconnect state, endpoint URLs, filesystem paths, or
other credentials into the web-readable status surface.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any


RECORDER_PUBLICATION_STATUS_FILENAME = (
    "mtconnect_recorder_publication_status.json"
)
RECORDER_PUBLICATION_STATUS_SCHEMA = (
    "fcp.mtconnect_recorder.publication_status.v1"
)
RECORDER_PUBLICATION_STATUS_MAX_BYTES = 4096
RECORDER_PUBLICATION_COUNTER_MAX = 1_000_000_000

_OVERALL_STATES = frozenset({"connected", "waiting", "error"})
_TOKEN = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]*\Z")
_ALLOWED_FIELDS = frozenset(
    {
        "schema",
        "updated_at",
        "status",
        "storage_state",
        "storage_group",
        "pending_batches",
        "last_committed_count",
        "last_error_code",
    }
)


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _safe_token(
    value: object,
    *,
    maximum: int,
    fallback: str | None,
) -> str | None:
    if not isinstance(value, str):
        return fallback
    candidate = value.strip()
    if not candidate or len(candidate) > maximum or _TOKEN.fullmatch(candidate) is None:
        return fallback
    return candidate


def _safe_counter(value: object) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    return min(RECORDER_PUBLICATION_COUNTER_MAX, max(0, parsed))


def _safe_timestamp(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return (
        parsed.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def publication_status_from_snapshot(status: object) -> str:
    """Map the internal lifecycle state onto the three-value public contract."""

    value = str(status or "").strip().lower()
    if value == "connected":
        return "connected"
    if value in {"error", "failed", "retrying"}:
        return "error"
    return "waiting"


def recorder_publication_status_payload(
    *,
    status: object,
    storage_state: object,
    storage_group: object = None,
    pending_batches: object = 0,
    last_committed_count: object = 0,
    last_error_code: object = None,
    updated_at: object = None,
) -> dict[str, Any]:
    """Build the complete allowlisted status document.

    Values that cannot be represented safely are discarded or replaced with a
    fixed safe value.  In particular, arbitrary exception messages never enter
    the file.
    """

    overall = str(status or "").strip().lower()
    if overall not in _OVERALL_STATES:
        overall = "error"
    safe_error = _safe_token(
        last_error_code,
        maximum=96,
        fallback="publication-error" if overall == "error" else None,
    )
    return {
        "schema": RECORDER_PUBLICATION_STATUS_SCHEMA,
        "updated_at": _safe_timestamp(updated_at) or _utc_now(),
        "status": overall,
        "storage_state": _safe_token(
            storage_state,
            maximum=64,
            fallback="unknown",
        ),
        "storage_group": _safe_token(
            storage_group,
            maximum=128,
            fallback=None,
        ),
        "pending_batches": _safe_counter(pending_batches),
        "last_committed_count": _safe_counter(last_committed_count),
        "last_error_code": safe_error,
    }


def write_recorder_publication_status(
    path: Path | str,
    **values: object,
) -> dict[str, Any]:
    """Atomically replace ``path`` with one bounded status document."""

    destination = Path(path)
    payload = recorder_publication_status_payload(**values)
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    if len(encoded) > RECORDER_PUBLICATION_STATUS_MAX_BYTES:
        raise ValueError("recorder publication status exceeds its size limit")

    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        try:
            temporary_path.chmod(0o600)
        except OSError:
            pass
        os.replace(temporary_path, destination)
        temporary_path = None
        if os.name != "nt":
            try:
                directory_fd = os.open(destination.parent, os.O_RDONLY)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
    return payload


def read_recorder_publication_status(
    path: Path | str,
) -> dict[str, Any] | None:
    """Read and strictly normalize a status file, or return ``None``."""

    source = Path(path)
    try:
        with source.open("rb") as stream:
            encoded = stream.read(RECORDER_PUBLICATION_STATUS_MAX_BYTES + 1)
    except OSError:
        return None
    if not encoded or len(encoded) > RECORDER_PUBLICATION_STATUS_MAX_BYTES:
        return None
    try:
        raw = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(raw, dict)
        or set(raw) != _ALLOWED_FIELDS
        or raw.get("schema") != RECORDER_PUBLICATION_STATUS_SCHEMA
        or raw.get("status") not in _OVERALL_STATES
    ):
        return None
    timestamp = _safe_timestamp(raw.get("updated_at"))
    storage_state = _safe_token(
        raw.get("storage_state"),
        maximum=64,
        fallback=None,
    )
    if timestamp is None or storage_state is None:
        return None
    storage_group = _safe_token(
        raw.get("storage_group"),
        maximum=128,
        fallback=None,
    )
    if raw.get("storage_group") is not None and storage_group is None:
        return None
    last_error = _safe_token(
        raw.get("last_error_code"),
        maximum=96,
        fallback=None,
    )
    if raw.get("last_error_code") is not None and last_error is None:
        return None
    if raw.get("status") == "error" and last_error is None:
        return None
    for field in ("pending_batches", "last_committed_count"):
        value = raw.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= RECORDER_PUBLICATION_COUNTER_MAX
        ):
            return None
    return {
        "schema": RECORDER_PUBLICATION_STATUS_SCHEMA,
        "updated_at": timestamp,
        "status": raw["status"],
        "storage_state": storage_state,
        "storage_group": storage_group,
        "pending_batches": raw["pending_batches"],
        "last_committed_count": raw["last_committed_count"],
        "last_error_code": last_error,
    }


__all__ = [
    "RECORDER_PUBLICATION_STATUS_FILENAME",
    "RECORDER_PUBLICATION_STATUS_MAX_BYTES",
    "RECORDER_PUBLICATION_STATUS_SCHEMA",
    "publication_status_from_snapshot",
    "read_recorder_publication_status",
    "recorder_publication_status_payload",
    "write_recorder_publication_status",
]

"""Bounded cleanup for crash-left federated analysis workspaces.

The analysis worker materializes one isolated directory per job attempt under a
dedicated FCP-owned workspace root. Normal completion removes that directory,
but a hard process kill can bypass ``finally`` and leave the full input slice on
disk indefinitely. This module gives startup a conservative, bounded way to
retire only stale directories that match FCP's owned workspace layout.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

WORKSPACE_MARKER = ".fcp-analysis-workspace.json"
WORKSPACE_SCHEMA = "fcp.analysis-workspace.v1"
DEFAULT_STALE_AFTER = timedelta(hours=24)
DEFAULT_MAX_SCANNED_ENTRIES = 1024
_ALLOWED_LEGACY_ENTRIES = frozenset({"staging", "inputs", "data", WORKSPACE_MARKER})
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,191}$")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)


@dataclass(frozen=True)
class WorkspaceReconciliationReport:
    scanned_entries: int = 0
    stale_candidates: int = 0
    removed_attempts: int = 0
    preserved_ambiguous: int = 0
    truncated: bool = False


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _plain_directory(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and not path.is_symlink()
        and not bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT)
    )


def _plain_regular_file(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(info.st_mode)
        and not path.is_symlink()
        and not bool(getattr(info, "st_file_attributes", 0) & _REPARSE_POINT)
    )


def _confined(path: Path, root: Path) -> bool:
    try:
        resolved_root = root.resolve(strict=False)
        resolved_path = path.resolve(strict=False)
        resolved_path.relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError):
        return False
    return resolved_path != resolved_root


def _component(value: str, field: str) -> str:
    text = str(value)
    if _COMPONENT_RE.fullmatch(text) is None:
        raise ValueError(
            f"analysis workspace {field} must be one portable path component"
        )
    return text


def owned_workspace_path(
    workspace_root: Path,
    *,
    job_id: str,
    attempt_id: str,
) -> Path:
    """Return the exact confined attempt path for validated durable identities."""

    root = Path(workspace_root)
    candidate = root / _component(job_id, "job_id") / _component(
        attempt_id, "attempt_id"
    )
    if not _confined(candidate, root):
        raise ValueError("analysis workspace path escaped its configured root")
    return candidate


def _marker_matches(path: Path, *, job_id: str, attempt_id: str) -> bool:
    marker = path / WORKSPACE_MARKER
    if not marker.exists() or not _plain_regular_file(marker):
        return False
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return bool(
        isinstance(payload, dict)
        and payload.get("schema") == WORKSPACE_SCHEMA
        and payload.get("job_id") == job_id
        and payload.get("attempt_id") == attempt_id
    )


def _legacy_layout_owned(path: Path) -> bool:
    """Recognize the exact pre-marker worker layout without recursive scanning."""

    try:
        entries = list(path.iterdir())
    except OSError:
        return False
    if any(entry.name not in _ALLOWED_LEGACY_ENTRIES for entry in entries):
        return False
    for entry in entries:
        if entry.name == WORKSPACE_MARKER:
            # A torn marker from a hard kill is still inside the exact FCP layout;
            # unexpected content elsewhere makes the directory ambiguous instead.
            if not _plain_regular_file(entry):
                return False
            continue
        if not _plain_directory(entry):
            return False
    return True


def write_workspace_marker(
    workspace: Path,
    *,
    job_id: str,
    attempt_id: str,
    created_at: datetime,
) -> None:
    """Write the small ownership record before large attempt materialization."""

    marker = Path(workspace) / WORKSPACE_MARKER
    payload: dict[str, Any] = {
        "schema": WORKSPACE_SCHEMA,
        "job_id": str(job_id),
        "attempt_id": str(attempt_id),
        "created_at": _utc(created_at).isoformat().replace("+00:00", "Z"),
        "pid": os.getpid(),
    }
    raw = (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")
    with marker.open("wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def prepare_owned_workspace(
    workspace_root: Path,
    *,
    job_id: str,
    attempt_id: str,
    created_at: datetime,
) -> Path:
    """Reset only an owned same-attempt workspace, then mark the fresh directory.

    Re-dispatch of one durable attempt may legitimately find the directory left
    by a prior process. A path with no matching marker and no exact legacy layout
    is never removed merely because it occupies the expected name.
    """

    root = Path(workspace_root)
    root.mkdir(parents=True, exist_ok=True)
    if not _plain_directory(root):
        raise ValueError("analysis workspace root is not a plain directory")

    workspace = owned_workspace_path(root, job_id=job_id, attempt_id=attempt_id)
    job_dir = workspace.parent
    if os.path.lexists(job_dir):
        if not _plain_directory(job_dir) or not _confined(job_dir, root):
            raise ValueError("analysis job workspace parent is not safely owned")
    else:
        job_dir.mkdir()

    if os.path.lexists(workspace):
        if (
            not _plain_directory(workspace)
            or not _confined(workspace, root)
            or not (
                _marker_matches(workspace, job_id=job_id, attempt_id=attempt_id)
                or _legacy_layout_owned(workspace)
            )
        ):
            raise ValueError("existing analysis attempt workspace is not safely owned")
        shutil.rmtree(workspace)

    workspace.mkdir()
    write_workspace_marker(
        workspace,
        job_id=job_id,
        attempt_id=attempt_id,
        created_at=created_at,
    )
    return workspace


def reconcile_stale_workspaces(
    workspace_root: Path,
    *,
    now: datetime,
    stale_after: timedelta = DEFAULT_STALE_AFTER,
    max_scanned_entries: int = DEFAULT_MAX_SCANNED_ENTRIES,
) -> WorkspaceReconciliationReport:
    """Remove stale owned attempt directories with finite startup work.

    The scan never follows symlinks/reparse points and never recursively searches
    arbitrary content. A directory is removable only when it is an exact
    ``<root>/<job>/<attempt>`` plain directory, is older than ``stale_after``, and
    either carries a matching ownership marker or matches the exact legacy
    pre-marker layout. Unexpected content is preserved for operator review.
    """

    root = Path(workspace_root)
    if stale_after.total_seconds() < 0:
        raise ValueError("stale_after must be non-negative")
    if max_scanned_entries <= 0:
        raise ValueError("max_scanned_entries must be positive")
    if not root.exists():
        return WorkspaceReconciliationReport()
    if not _plain_directory(root):
        return WorkspaceReconciliationReport(preserved_ambiguous=1)

    cutoff = _utc(now) - stale_after
    scanned = 0
    stale = 0
    removed = 0
    ambiguous = 0
    truncated = False

    try:
        job_entries = root.iterdir()
        for job_dir in job_entries:
            scanned += 1
            if scanned > max_scanned_entries:
                truncated = True
                break
            if not _plain_directory(job_dir) or not _confined(job_dir, root):
                ambiguous += 1
                continue
            try:
                _component(job_dir.name, "job_id")
            except ValueError:
                ambiguous += 1
                continue
            try:
                attempts = job_dir.iterdir()
                for attempt_dir in attempts:
                    scanned += 1
                    if scanned > max_scanned_entries:
                        truncated = True
                        break
                    if not _plain_directory(attempt_dir) or not _confined(
                        attempt_dir, root
                    ):
                        ambiguous += 1
                        continue
                    try:
                        _component(attempt_dir.name, "attempt_id")
                    except ValueError:
                        ambiguous += 1
                        continue
                    try:
                        modified = datetime.fromtimestamp(
                            attempt_dir.stat(follow_symlinks=False).st_mtime,
                            tz=timezone.utc,
                        )
                    except OSError:
                        ambiguous += 1
                        continue
                    if modified > cutoff:
                        continue
                    stale += 1
                    owned = _marker_matches(
                        attempt_dir,
                        job_id=job_dir.name,
                        attempt_id=attempt_dir.name,
                    ) or _legacy_layout_owned(attempt_dir)
                    if not owned:
                        ambiguous += 1
                        continue
                    # Recheck the two critical invariants immediately before delete.
                    if not _plain_directory(attempt_dir) or not _confined(
                        attempt_dir, root
                    ):
                        ambiguous += 1
                        continue
                    try:
                        shutil.rmtree(attempt_dir)
                    except OSError:
                        ambiguous += 1
                        continue
                    removed += 1
                if truncated:
                    break
            except OSError:
                ambiguous += 1
                continue
            try:
                job_dir.rmdir()
            except OSError:
                pass
    except OSError:
        ambiguous += 1

    return WorkspaceReconciliationReport(
        scanned_entries=min(scanned, max_scanned_entries),
        stale_candidates=stale,
        removed_attempts=removed,
        preserved_ambiguous=ambiguous,
        truncated=truncated,
    )


__all__ = [
    "DEFAULT_MAX_SCANNED_ENTRIES",
    "DEFAULT_STALE_AFTER",
    "WORKSPACE_MARKER",
    "WORKSPACE_SCHEMA",
    "WorkspaceReconciliationReport",
    "owned_workspace_path",
    "prepare_owned_workspace",
    "reconcile_stale_workspaces",
    "write_workspace_marker",
]

"""TTL cache for listing workflow sessions from disk.

Session directories can grow quickly during catch-up. The Flask views only need
a recent, freshness-sorted list, so this service centralizes the short cache and
explicit invalidation used after control actions.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from catalog.runner.session_store import SessionInfo, list_sessions


@dataclass
class SessionIndexResult:
    sessions: list[SessionInfo]
    cache_state: str
    list_ms: float


class WorkflowSessionIndex:
    """Cache freshness-sorted workflow session listings for Flask views."""

    def __init__(self, *, ttl_seconds: float = 4.0) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, list[SessionInfo]]] = {}

    def invalidate(self, workflows_root: Path | None = None) -> None:
        with self._lock:
            if workflows_root is None:
                self._entries.clear()
                return
            self._entries.pop(str(workflows_root.resolve()), None)

    def get_sessions(self, workflows_root: Path) -> SessionIndexResult:
        """Return sessions sorted by best available freshness timestamp."""
        key = str(workflows_root.resolve())
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                built_at, sessions = entry
                if (now - built_at) <= self.ttl_seconds:
                    return SessionIndexResult(sessions=sessions, cache_state="hit", list_ms=0.0)

        started = time.perf_counter()
        sessions = list_sessions(workflows_root)
        sessions = sorted(sessions, key=_session_freshness_key, reverse=True)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with self._lock:
            self._entries[key] = (time.monotonic(), sessions)
        return SessionIndexResult(sessions=sessions, cache_state="rebuilt", list_ms=elapsed_ms)


_SESSION_INDEX: WorkflowSessionIndex | None = None


def get_workflow_session_index() -> WorkflowSessionIndex:
    global _SESSION_INDEX
    if _SESSION_INDEX is None:
        _SESSION_INDEX = WorkflowSessionIndex()
    return _SESSION_INDEX


def _session_freshness_key(session: SessionInfo) -> tuple[pd.Timestamp, str]:
    metadata = getattr(session, "metadata", {}) or {}
    updated = pd.to_datetime(metadata.get("updated_at"), errors="coerce", utc=True)
    if pd.isna(updated):
        updated = pd.to_datetime(metadata.get("created_at"), errors="coerce", utc=True)
    if pd.isna(updated):
        filter_payload = metadata.get("filter") if isinstance(metadata.get("filter"), dict) else {}
        updated = pd.to_datetime(filter_payload.get("end_date"), errors="coerce", utc=True)
    if pd.isna(updated):
        updated = pd.Timestamp(0, tz="UTC")
    return updated, str(getattr(session, "session_id", ""))

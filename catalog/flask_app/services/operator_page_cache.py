from __future__ import annotations

import threading
import time
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any

from catalog.orchestrator.pipeline import get_runtime_manager

from .catalog_service import ArtifactCatalog
from .control_service import get_control_panel_service
from .overview_service import OverviewSnapshot, build_overview_snapshot
from .workflow_session_index import get_workflow_session_index

logger = logging.getLogger(__name__)


@dataclass
class _CacheEntry:
    value: Any
    built_at_mono: float
    signature: tuple[Any, ...]


class OperatorPageCache:
    def __init__(self, *, overview_ttl_seconds: float = 4.0, control_ttl_seconds: float = 4.0) -> None:
        self.overview_ttl_seconds = float(overview_ttl_seconds)
        self.control_ttl_seconds = float(control_ttl_seconds)
        self._lock = threading.Lock()
        self._overview_entry: _CacheEntry | None = None
        self._control_entries: dict[str, _CacheEntry] = {}

    def invalidate_overview(self) -> None:
        with self._lock:
            self._overview_entry = None

    def invalidate_control(self, selected_session_id: str | None = None) -> None:
        key = (selected_session_id or "").strip()
        with self._lock:
            if key:
                self._control_entries.pop(key, None)
            else:
                self._control_entries.clear()

    def invalidate_all(self) -> None:
        with self._lock:
            self._overview_entry = None
            self._control_entries.clear()

    def get_overview_snapshot(self, catalog: ArtifactCatalog) -> tuple[OverviewSnapshot, str]:
        scan = catalog.ensure_scanned()
        runtime_state = get_runtime_manager().state_snapshot()
        signature = (
            float(scan.scanned_at_epoch),
            runtime_state.get("session_id"),
            runtime_state.get("current_processing_phase"),
            runtime_state.get("last_completed_step"),
            runtime_state.get("last_completed_date"),
            runtime_state.get("next_queued_date"),
            bool(runtime_state.get("update_running")),
            bool(runtime_state.get("discovery_complete")),
            bool(runtime_state.get("historical_catch_up_complete")),
            runtime_state.get("last_failure"),
        )
        now = time.monotonic()
        with self._lock:
            entry = self._overview_entry
            if entry and entry.signature == signature and (now - entry.built_at_mono) <= self.overview_ttl_seconds:
                return entry.value, "hit"

        session_index = get_workflow_session_index().get_sessions(_workflows_root())
        rebuild_started = time.perf_counter()
        rebuilt = build_overview_snapshot(
            catalog,
            scan=scan,
            runtime_state=runtime_state,
            sessions=session_index.sessions,
        )
        rebuild_ms = (time.perf_counter() - rebuild_started) * 1000.0
        with self._lock:
            self._overview_entry = _CacheEntry(value=rebuilt, built_at_mono=time.monotonic(), signature=signature)
        logger.info(
            "overview snapshot rebuild session_list_cache=%s session_list_ms=%.2f rebuild_ms=%.2f",
            session_index.cache_state,
            session_index.list_ms,
            rebuild_ms,
        )
        return rebuilt, "rebuilt"

    def get_control_snapshot(self, *, selected_session_id: str | None = None) -> tuple[dict[str, Any], str]:
        runtime_state = get_runtime_manager().state_snapshot()
        control_service = get_control_panel_service()
        key = (selected_session_id or "").strip()
        signature = (
            key,
            runtime_state.get("session_id"),
            runtime_state.get("current_processing_phase"),
            runtime_state.get("last_completed_step"),
            runtime_state.get("last_completed_date"),
            bool(runtime_state.get("update_running")),
            runtime_state.get("last_failure"),
            control_service.cache_signature(),
        )
        now = time.monotonic()
        with self._lock:
            entry = self._control_entries.get(key)
            if entry and entry.signature == signature and (now - entry.built_at_mono) <= self.control_ttl_seconds:
                return entry.value, "hit"

        session_index = get_workflow_session_index().get_sessions(_workflows_root())
        rebuild_started = time.perf_counter()
        rebuilt = control_service.snapshot(
            selected_session_id=selected_session_id,
            runtime_state=runtime_state,
            sessions=session_index.sessions,
        )
        rebuild_ms = (time.perf_counter() - rebuild_started) * 1000.0
        with self._lock:
            self._control_entries[key] = _CacheEntry(value=rebuilt, built_at_mono=time.monotonic(), signature=signature)
        logger.info(
            "control snapshot rebuild session_list_cache=%s session_list_ms=%.2f rebuild_ms=%.2f selected_session=%s",
            session_index.cache_state,
            session_index.list_ms,
            rebuild_ms,
            key,
        )
        return rebuilt, "rebuilt"


_OPERATOR_PAGE_CACHE: OperatorPageCache | None = None


def get_operator_page_cache() -> OperatorPageCache:
    global _OPERATOR_PAGE_CACHE
    if _OPERATOR_PAGE_CACHE is None:
        _OPERATOR_PAGE_CACHE = OperatorPageCache()
    return _OPERATOR_PAGE_CACHE


def _workflows_root() -> Path:
    return get_control_panel_service().workflows_root

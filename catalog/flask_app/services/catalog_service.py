from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
import threading
import time
from typing import Any

import pandas as pd

from catalog.common.artifact_registry import SUPPORTED_SUFFIXES, configured_scan_dirs, read_raw_table, scan_artifacts

LOGGER = logging.getLogger(__name__)


@dataclass
class ScanSnapshot:
    artifacts: list[dict[str, Any]]
    warnings: list[str]
    scanned_at_epoch: float


@dataclass(frozen=True)
class ScanRootSignature:
    scan_dirs: tuple[str, ...]
    supported_file_count: int
    max_mtime_ns: int


@dataclass(frozen=True)
class CatalogFreshness:
    scanned_at_epoch: float
    age_seconds: float | None
    stale: bool
    scan_in_progress: bool


class ArtifactCatalog:
    def __init__(self, *, signature_ttl_seconds: float = 1.0, cached_snapshot_ttl_seconds: float = 30.0) -> None:
        self._snapshot = ScanSnapshot(artifacts=[], warnings=[], scanned_at_epoch=0.0)
        self._last_scan_signature: ScanRootSignature | None = None
        self._signature_checked_at_epoch = 0.0
        self._signature_ttl_seconds = max(float(signature_ttl_seconds), 0.0)
        self._cached_snapshot_ttl_seconds = max(float(cached_snapshot_ttl_seconds), 0.0)
        self._lock = threading.Lock()
        self._scan_lock = threading.Lock()
        self._scan_in_progress = False
        self._rescan_pending = False
        self._stale_snapshot_logged = False

    @property
    def scan_dirs(self) -> list[str]:
        return configured_scan_dirs()

    def snapshot(self) -> ScanSnapshot:
        with self._lock:
            return self._snapshot

    def freshness(self) -> CatalogFreshness:
        now = time.time()
        with self._lock:
            scanned_at = self._snapshot.scanned_at_epoch
            scan_in_progress = self._scan_in_progress
        age = None if scanned_at <= 0 else max(now - scanned_at, 0.0)
        stale = scanned_at <= 0 or (age is not None and age >= self._cached_snapshot_ttl_seconds)
        return CatalogFreshness(
            scanned_at_epoch=scanned_at,
            age_seconds=age,
            stale=stale,
            scan_in_progress=scan_in_progress,
        )

    def cached_snapshot(self, *, log_if_stale: bool = True) -> ScanSnapshot:
        """Return the latest catalog snapshot without filesystem scanning.

        Ordinary page requests should use this method so they never rglob the
        results tree or wait for artifact discovery while runtime work is busy.
        Explicit rescan controls can still call :meth:`rescan`.
        """
        snapshot = self.snapshot()
        freshness = self.freshness()
        if log_if_stale and freshness.stale:
            with self._lock:
                should_log = not self._stale_snapshot_logged
                if should_log:
                    self._stale_snapshot_logged = True
            if should_log:
                LOGGER.info(
                    "Using stale cached artifact catalog snapshot; explicit rescan is needed. scanned_at_epoch=%.3f age_seconds=%s",
                    freshness.scanned_at_epoch,
                    "unknown" if freshness.age_seconds is None else f"{freshness.age_seconds:.1f}",
                )
        return snapshot

    def start_background_rescan_if_idle(self, *, reason: str = "background") -> bool:
        """Start or queue a best-effort rescan thread without making callers wait."""
        if not self._scan_lock.acquire(blocking=False):
            with self._lock:
                self._rescan_pending = True
            LOGGER.info("Artifact catalog background rescan queued reason=%s", reason)
            return True

        def _worker() -> None:
            scan_reason = reason
            try:
                while True:
                    with self._lock:
                        self._scan_in_progress = True
                    LOGGER.info("Artifact catalog background rescan started reason=%s", scan_reason)
                    artifacts, warnings = scan_artifacts(self.scan_dirs)
                    signature = self._scan_root_signature()
                    snapshot = ScanSnapshot(artifacts=artifacts, warnings=warnings, scanned_at_epoch=time.time())
                    with self._lock:
                        self._snapshot = snapshot
                        self._last_scan_signature = signature
                        self._signature_checked_at_epoch = time.time()
                        self._stale_snapshot_logged = False
                    LOGGER.info(
                        "Artifact catalog background rescan finished reason=%s artifacts=%d warnings=%d",
                        scan_reason,
                        len(artifacts),
                        len(warnings),
                    )
                    with self._lock:
                        if not self._rescan_pending:
                            self._scan_in_progress = False
                            break
                        self._rescan_pending = False
                        scan_reason = "queued_after_active_scan"
                    LOGGER.info("Artifact catalog pending background rescan starting after active scan")
            finally:
                with self._lock:
                    self._scan_in_progress = False
                self._scan_lock.release()

        threading.Thread(target=_worker, name="msh-artifact-catalog-rescan", daemon=True).start()
        return True

    def _scan_root_signature(self) -> ScanRootSignature:
        scan_dirs = self.scan_dirs
        supported_file_count = 0
        max_mtime_ns = 0

        for raw_root in scan_dirs:
            root = Path(raw_root).expanduser()
            if not root.exists() or not root.is_dir():
                continue

            for path in root.rglob("*"):
                if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
                    continue
                try:
                    stat = path.stat()
                except OSError:
                    continue
                supported_file_count += 1
                max_mtime_ns = max(max_mtime_ns, stat.st_mtime_ns)

        return ScanRootSignature(
            scan_dirs=tuple(scan_dirs),
            supported_file_count=supported_file_count,
            max_mtime_ns=max_mtime_ns,
        )

    def rescan(self, *, scan_root_signature: ScanRootSignature | None = None) -> ScanSnapshot:
        with self._scan_lock:
            with self._lock:
                self._scan_in_progress = True
            LOGGER.info("Explicit artifact catalog rescan started")
            try:
                artifacts, warnings = scan_artifacts(self.scan_dirs)
                snapshot = ScanSnapshot(artifacts=artifacts, warnings=warnings, scanned_at_epoch=time.time())
                signature = scan_root_signature or self._scan_root_signature()
                checked_at = time.time()
                with self._lock:
                    self._snapshot = snapshot
                    self._last_scan_signature = signature
                    self._signature_checked_at_epoch = checked_at
                    self._stale_snapshot_logged = False
                LOGGER.info("Explicit artifact catalog rescan finished artifacts=%d warnings=%d", len(artifacts), len(warnings))
                return snapshot
            finally:
                with self._lock:
                    self._scan_in_progress = False

    def ensure_scanned(self, *, force_signature_check: bool = False) -> ScanSnapshot:
        """Synchronously refresh the catalog when callers explicitly need it.

        This may walk all scan roots and should not be used by ordinary page
        loads. Use cached_snapshot() for responsive request-time reads.
        """
        now = time.time()
        with self._lock:
            last_signature = self._last_scan_signature
            signature_checked_at = self._signature_checked_at_epoch
            snapshot = self._snapshot
        should_check_signature = (
            force_signature_check
            or last_signature is None
            or now - signature_checked_at >= self._signature_ttl_seconds
        )
        if not should_check_signature:
            return snapshot

        current_signature = self._scan_root_signature()
        with self._lock:
            self._signature_checked_at_epoch = now
        if last_signature is None:
            return self.rescan(scan_root_signature=current_signature)
        if current_signature != last_signature:
            LOGGER.info(
                "Artifact catalog scan roots changed; auto-rescanning. previous=%s current=%s",
                last_signature,
                current_signature,
            )
            return self.rescan(scan_root_signature=current_signature)
        return self.snapshot()

    def artifact_by_path(self, path: str, *, cached: bool = True) -> dict[str, Any] | None:
        snap = self.cached_snapshot() if cached else self.ensure_scanned()
        for artifact in snap.artifacts:
            if artifact.get("path") == path:
                return artifact
        return None


def safe_load_artifact_frame(path: str) -> tuple[pd.DataFrame | None, str | None]:
    try:
        return read_raw_table(path), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

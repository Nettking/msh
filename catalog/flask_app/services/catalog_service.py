from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
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


class ArtifactCatalog:
    def __init__(self, *, signature_ttl_seconds: float = 1.0) -> None:
        self._snapshot = ScanSnapshot(artifacts=[], warnings=[], scanned_at_epoch=0.0)
        self._last_scan_signature: ScanRootSignature | None = None
        self._signature_checked_at_epoch = 0.0
        self._signature_ttl_seconds = max(float(signature_ttl_seconds), 0.0)

    @property
    def scan_dirs(self) -> list[str]:
        return configured_scan_dirs()

    def snapshot(self) -> ScanSnapshot:
        return self._snapshot

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
        artifacts, warnings = scan_artifacts(self.scan_dirs)
        self._snapshot = ScanSnapshot(artifacts=artifacts, warnings=warnings, scanned_at_epoch=time.time())
        self._last_scan_signature = scan_root_signature or self._scan_root_signature()
        self._signature_checked_at_epoch = time.time()
        return self._snapshot

    def ensure_scanned(self, *, force_signature_check: bool = False) -> ScanSnapshot:
        now = time.time()
        should_check_signature = (
            force_signature_check
            or self._last_scan_signature is None
            or now - self._signature_checked_at_epoch >= self._signature_ttl_seconds
        )
        if not should_check_signature:
            return self._snapshot

        current_signature = self._scan_root_signature()
        self._signature_checked_at_epoch = now
        if self._last_scan_signature is None:
            return self.rescan(scan_root_signature=current_signature)
        if current_signature != self._last_scan_signature:
            LOGGER.info(
                "Artifact catalog scan roots changed; auto-rescanning. previous=%s current=%s",
                self._last_scan_signature,
                current_signature,
            )
            return self.rescan(scan_root_signature=current_signature)
        return self._snapshot

    def artifact_by_path(self, path: str) -> dict[str, Any] | None:
        snap = self.ensure_scanned()
        for artifact in snap.artifacts:
            if artifact.get("path") == path:
                return artifact
        return None


def safe_load_artifact_frame(path: str) -> tuple[pd.DataFrame | None, str | None]:
    try:
        return read_raw_table(path), None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)

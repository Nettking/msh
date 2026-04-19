from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import pandas as pd

from catalog.common.artifact_registry import configured_scan_dirs, read_raw_table, scan_artifacts


@dataclass
class ScanSnapshot:
    artifacts: list[dict[str, Any]]
    warnings: list[str]
    scanned_at_epoch: float


class ArtifactCatalog:
    def __init__(self) -> None:
        self._snapshot = ScanSnapshot(artifacts=[], warnings=[], scanned_at_epoch=0.0)

    @property
    def scan_dirs(self) -> list[str]:
        return configured_scan_dirs()

    def snapshot(self) -> ScanSnapshot:
        return self._snapshot

    def rescan(self) -> ScanSnapshot:
        artifacts, warnings = scan_artifacts(self.scan_dirs)
        self._snapshot = ScanSnapshot(artifacts=artifacts, warnings=warnings, scanned_at_epoch=time.time())
        return self._snapshot

    def ensure_scanned(self) -> ScanSnapshot:
        if not self._snapshot.artifacts and not self._snapshot.warnings:
            return self.rescan()
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

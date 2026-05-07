from __future__ import annotations

import logging
from pathlib import Path
import threading
import time

import pandas as pd

from catalog.flask_app.services.catalog_service import ArtifactCatalog, ScanRootSignature


def test_ensure_scanned_refreshes_when_timeline_export_appears(tmp_path: Path, monkeypatch, caplog) -> None:
    results_root = tmp_path / "results"
    results_root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(results_root))

    catalog = ArtifactCatalog(signature_ttl_seconds=0.0)
    initial = catalog.ensure_scanned()

    assert initial.artifacts == []

    timeline_path = results_root / "workflows" / "session-123" / "exports" / "timeline" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    with caplog.at_level(logging.INFO, logger="catalog.flask_app.services.catalog_service"):
        refreshed = catalog.ensure_scanned()

    artifact = next((item for item in refreshed.artifacts if item["path"] == str(timeline_path)), None)
    assert artifact is not None
    assert artifact["playback_compatible"] is True
    assert "playback" in artifact["supported_views"]
    assert "Artifact catalog scan roots changed; auto-rescanning" in caplog.text


def test_force_signature_check_bypasses_ttl_for_playback_fallback(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    results_root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(results_root))

    catalog = ArtifactCatalog(signature_ttl_seconds=60.0)
    assert catalog.ensure_scanned().artifacts == []

    timeline_path = results_root / "workflows" / "session-123" / "exports" / "timeline" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    assert catalog.ensure_scanned().artifacts == []

    refreshed = catalog.ensure_scanned(force_signature_check=True)

    assert [item["path"] for item in refreshed.artifacts if item["playback_compatible"]] == [str(timeline_path)]


def test_ensure_scanned_reuses_signature_within_ttl(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    results_root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(results_root))

    class CountingCatalog(ArtifactCatalog):
        def __init__(self) -> None:
            super().__init__(signature_ttl_seconds=60.0)
            self.signature_checks = 0

        def _scan_root_signature(self) -> ScanRootSignature:
            self.signature_checks += 1
            return super()._scan_root_signature()

    catalog = CountingCatalog()

    catalog.ensure_scanned()
    catalog.ensure_scanned()

    assert catalog.signature_checks == 1


def test_background_rescan_request_during_active_scan_runs_pending_scan(tmp_path: Path, monkeypatch) -> None:
    results_root = tmp_path / "results"
    results_root.mkdir()
    monkeypatch.setenv("MSH_SCAN_DIRS", str(results_root))
    catalog = ArtifactCatalog()
    first_scan_started = threading.Event()
    allow_first_scan_to_finish = threading.Event()
    scan_calls = 0

    def fake_scan_artifacts(scan_dirs):
        nonlocal scan_calls
        scan_calls += 1
        if scan_calls == 1:
            first_scan_started.set()
            assert allow_first_scan_to_finish.wait(timeout=2.0)
            return [], []
        return [
            {
                "path": str(timeline_path),
                "file_name": "timeline_rows.csv",
                "playback_compatible": True,
                "visibility": "default",
            }
        ], []

    monkeypatch.setattr("catalog.flask_app.services.catalog_service.scan_artifacts", fake_scan_artifacts)

    assert catalog.start_background_rescan_if_idle(reason="startup") is True
    assert first_scan_started.wait(timeout=2.0)

    timeline_path = results_root / "workflows" / "session-123" / "exports" / "timeline" / "timeline_rows.csv"
    timeline_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z"],
            "machine_id": ["M1"],
            "state": ["active"],
        }
    ).to_csv(timeline_path, index=False)

    assert catalog.start_background_rescan_if_idle(reason="playback_export_generated") is True
    allow_first_scan_to_finish.set()

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and not catalog.cached_snapshot(log_if_stale=False).artifacts:
        time.sleep(0.01)

    snap = catalog.cached_snapshot(log_if_stale=False)
    assert scan_calls == 2
    assert [item["path"] for item in snap.artifacts] == [str(timeline_path)]

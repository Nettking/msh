from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.flask_app.services.catalog_service import ScanSnapshot
from catalog.flask_app.services.live_service import LiveTelemetryService


@dataclass
class _FakeCatalog:
    artifacts: list[dict[str, object]]

    def ensure_scanned(self) -> ScanSnapshot:
        return ScanSnapshot(artifacts=self.artifacts, warnings=[], scanned_at_epoch=1.0)


def _artifact(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "signature": "sig",
        "category": "source_data",
        "status": "ready",
        "modified_at": datetime.now(timezone.utc).isoformat(),
    }


def test_live_service_distinguishes_running_and_stopped_states(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = tmp_path / "data" / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    rows = [
        {
            "timestamp": (now - timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 800,
            "Sload": 18,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 900,
            "Sload": 20,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 910,
            "Sload": 19,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            "machine": "IG500",
            "execution": "STOPPED",
            "mode": "MANUAL",
            "program": "P2",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 0,
            "Fovr": 0,
            "Frapidovr": 0,
        },
        {
            "timestamp": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "machine": "IG500",
            "execution": "STOPPED",
            "mode": "MANUAL",
            "program": "P2",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 0,
            "Fovr": 0,
            "Frapidovr": 0,
        },
        {
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "machine": "IG500",
            "execution": "STOPPED",
            "mode": "MANUAL",
            "program": "P2",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 0,
            "Fovr": 0,
            "Frapidovr": 0,
        },
    ]
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    snapshot = LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=30).snapshot(_FakeCatalog([_artifact(source)]))
    by_machine = {item["machine"]: item for item in snapshot.machines}

    assert by_machine["QuickTurn"]["inferred_state"] == "running/active"
    assert by_machine["IG500"]["inferred_state"] == "stopped"


def test_live_service_distinguishes_idle_from_running(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = tmp_path / "data" / "2026-03-24.jsonl"
    source.parent.mkdir(parents=True)
    rows = [
        {
            "timestamp": (now - timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 700,
            "Sload": 12,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 720,
            "Sload": 13,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 710,
            "Sload": 11,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            "machine": "VTC",
            "execution": "READY",
            "mode": "AUTO",
            "program": "P3",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "machine": "VTC",
            "execution": "READY",
            "mode": "AUTO",
            "program": "P3",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "machine": "VTC",
            "execution": "READY",
            "mode": "AUTO",
            "program": "P3",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
    ]
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    snapshot = LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=30).snapshot(_FakeCatalog([_artifact(source)]))
    by_machine = {item["machine"]: item for item in snapshot.machines}

    assert by_machine["QuickTurn"]["inferred_state"] == "running/active"
    assert by_machine["VTC"]["inferred_state"] == "idle"


def test_live_service_emits_recent_candidate_events(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = tmp_path / "data" / "QuickTurn" / "2026-03-23.jsonl"
    source.parent.mkdir(parents=True)
    rows = [
        {
            "timestamp": (now - timedelta(seconds=3)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "READY",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 0,
            "Sload": 0,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 700,
            "Sload": 14,
            "Sovr": 100,
            "Fovr": 100,
            "Frapidovr": 100,
        },
        {
            "timestamp": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "machine": "QuickTurn",
            "execution": "ACTIVE",
            "mode": "AUTO",
            "program": "P1",
            "Srpm": 80,
            "Sload": 2,
            "Sovr": 55,
            "Fovr": 55,
            "Frapidovr": 100,
        },
    ]
    source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    snapshot = LiveTelemetryService(refresh_ttl_seconds=0, stale_after_seconds=30).snapshot(_FakeCatalog([_artifact(source)]))

    assert snapshot.candidate_events
    first_event = snapshot.candidate_events[0]
    assert first_event["machine"] == "QuickTurn"
    assert first_event["event_score"] > 0
    assert first_event["fired_rules"] != "-"
    fired_rules = str(first_event["fired_rules"])
    assert ("ovr_drop" in fired_rules) or ("fovr_drop" in fired_rules) or ("rpm_collapse" in fired_rules) or ("load_collapse" in fired_rules)

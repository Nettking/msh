from __future__ import annotations

from catalog.flask_app.services.catalog_service import ScanSnapshot
from catalog.flask_app.services.overview_service import build_overview_snapshot


def _scan(*artifacts):
    return ScanSnapshot(artifacts=list(artifacts), warnings=[], scanned_at_epoch=123.0)


def _source_artifact(**extra):
    artifact = {
        "visibility": "default",
        "category": "source_data",
        "status": "ready",
        "playback_compatible": False,
        "timestamp_max": "2025-11-11T08:41:55.108206",
        "path": "results/raw.csv",
    }
    artifact.update(extra)
    return artifact


def test_overview_decision_explains_artifacts_without_runtime():
    overview = build_overview_snapshot(
        object(),
        scan=_scan(_source_artifact()),
        runtime_state={},
        sessions=[],
    )

    assert overview.decision["state"] == "waiting"
    assert overview.decision["title"] == "Artifacts exist, but runtime is not started"
    assert overview.decision["primary_action"] == {"label": "Open control", "href": "/control"}
    assert any("1 visible artifact" in fact for fact in overview.decision["facts"])


def test_overview_decision_explains_empty_scan():
    overview = build_overview_snapshot(
        object(),
        scan=_scan(),
        runtime_state={},
        sessions=[],
    )

    assert overview.decision["title"] == "No artifacts have been discovered yet"
    assert "rescan" in overview.decision["next_step"].lower()
    assert overview.decision["primary_action"]["href"] == "/control"


def test_overview_decision_prioritizes_runtime_failure():
    overview = build_overview_snapshot(
        object(),
        scan=_scan(_source_artifact()),
        runtime_state={"last_failure": "MTConnect timeout", "runtime_started_at": "2026-01-01T00:00:00Z"},
        sessions=[],
    )

    assert overview.decision["state"] == "error"
    assert overview.decision["title"] == "Runtime needs attention"
    assert "MTConnect timeout" in overview.decision["summary"]
    assert overview.decision["primary_action"] == {"label": "Open status", "href": "/status"}

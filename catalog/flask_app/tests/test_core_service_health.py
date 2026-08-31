from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.flask_app.services.core_service_health import (
    core_service_health_snapshot,
    flask_health,
    recorder_health,
    relay_health,
)
from catalog.mtconnect_recorder.native_update import RecorderRuntimeStatus

NOW = datetime(2026, 8, 31, 12, 30, tzinfo=timezone.utc)


def _status(
    *,
    heartbeat_at: datetime | None = NOW,
    state: str = "recording",
    federation_status: str = "connected",
) -> RecorderRuntimeStatus:
    return RecorderRuntimeStatus(
        pid=123,
        process_nonce=None,
        supervisor_session=None,
        build_commit=None,
        heartbeat_at=heartbeat_at,
        state=state,
        federation_status=federation_status,
        federation_node_id="node-a",
        federation_session_id="session-a",
    )


def test_relay_listener_can_be_alive_while_authority_readiness_is_degraded(tmp_path: Path) -> None:
    health = relay_health(
        host="relay",
        port=8765,
        coordinator_database=tmp_path / "missing.sqlite3",
        listener_probe=lambda _host, _port: True,
        database_probe=lambda _path: False,
    )

    assert health.liveness == "alive"
    assert health.readiness == "not_ready"
    assert health.dependency == "degraded"
    assert health.code == "relay-authority-store-unavailable"


def test_relay_is_ready_only_when_listener_and_authority_store_are_available(tmp_path: Path) -> None:
    health = relay_health(
        host="relay",
        port=8765,
        coordinator_database=tmp_path / "control.sqlite3",
        listener_probe=lambda _host, _port: True,
        database_probe=lambda _path: True,
    )

    assert health.liveness == "alive"
    assert health.readiness == "ready"
    assert health.dependency == "healthy"


def test_stale_recorder_heartbeat_is_not_liveness(tmp_path: Path) -> None:
    status_file = tmp_path / "status.json"
    health = recorder_health(
        status_file,
        now=NOW,
        reader=lambda _path: _status(heartbeat_at=NOW - timedelta(seconds=11)),
    )

    assert health.liveness == "unavailable"
    assert health.readiness == "not_ready"
    assert health.code == "recorder-heartbeat-stale"


def test_recorder_local_readiness_survives_federation_dependency_failure(tmp_path: Path) -> None:
    health = recorder_health(
        tmp_path / "status.json",
        now=NOW,
        reader=lambda _path: _status(federation_status="reconnecting"),
    )

    assert health.liveness == "alive"
    assert health.readiness == "ready"
    assert health.dependency == "degraded"
    assert health.code == "recorder-federation-degraded"


def test_recorder_error_state_is_alive_but_not_ready(tmp_path: Path) -> None:
    health = recorder_health(
        tmp_path / "status.json",
        now=NOW,
        reader=lambda _path: _status(state="error"),
    )

    assert health.liveness == "alive"
    assert health.readiness == "not_ready"
    assert health.code == "recorder-error"


def test_flask_remains_ready_when_optional_core_dependency_is_degraded(tmp_path: Path) -> None:
    relay = relay_health(
        host="relay",
        port=8765,
        coordinator_database=tmp_path / "control.sqlite3",
        listener_probe=lambda _host, _port: False,
        database_probe=lambda _path: False,
    )
    recorder = recorder_health(
        tmp_path / "status.json",
        now=NOW,
        reader=lambda _path: _status(),
    )

    health = flask_health(relay=relay, recorder=recorder)

    assert health.liveness == "alive"
    assert health.readiness == "ready"
    assert health.dependency == "degraded"
    assert health.code == "flask-dependency-degraded"


def test_snapshot_keeps_three_service_semantics_separate(tmp_path: Path) -> None:
    snapshot = core_service_health_snapshot(
        coordinator_database=tmp_path / "control.sqlite3",
        recorder_status_file=tmp_path / "status.json",
        relay_host="relay",
        relay_port=8765,
        now=NOW,
        listener_probe=lambda _host, _port: True,
        database_probe=lambda _path: True,
        recorder_reader=lambda _path: _status(),
    )

    assert snapshot["status"] == "ready"
    services = {item["service"]: item for item in snapshot["services"]}
    assert set(services) == {"flask", "relay", "recorder"}
    assert services["flask"]["readiness"] == "ready"
    assert services["relay"]["readiness"] == "ready"
    assert services["recorder"]["readiness"] == "ready"

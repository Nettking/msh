"""A crash-looping core service must be visible on the operator health surface.

The three probes in ``core_service_health`` all answer questions about *now*:
is the endpoint reachable, is the store readable, is the heartbeat fresh. A
container on its fiftieth Docker restart answers all three exactly like a
healthy one. These tests pin that a sustained loop changes the verdict, that a
single restart does not, and that an optional dependency can never manufacture
a core-FCP failure.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.federation.service_incarnation import (
    CRASH_LOOP_THRESHOLD,
    STOP_OPERATOR,
    incarnation_state_file,
    record_service_start,
    record_service_stop,
)
from catalog.flask_app.services.core_service_health import (
    core_service_health_snapshot,
    default_incarnation_roots,
)
from catalog.mtconnect_recorder.native_update import RecorderRuntimeStatus

NOW = datetime(2026, 9, 2, 9, 0, tzinfo=timezone.utc)


def _status() -> RecorderRuntimeStatus:
    return RecorderRuntimeStatus(
        pid=123,
        process_nonce=None,
        supervisor_session=None,
        build_commit=None,
        heartbeat_at=NOW,
        state="recording",
        federation_status="connected",
        federation_node_id="node-a",
        federation_session_id="session-a",
    )


def _crash_loop(root: Path, service: str, *, unclean_starts: int) -> None:
    """Drive a service through repeated kills: start, never stop, repeat.

    ``unclean_starts`` counts *restarts after a kill*, so one of them needs two
    incarnations: the one that died and the one that found no recorded stop.
    """

    path = incarnation_state_file(root, service)
    incarnations = unclean_starts + 1
    moment = NOW - timedelta(seconds=5 * incarnations)
    for _ in range(incarnations):
        record_service_start(path, service=service, now=moment)
        moment += timedelta(seconds=5)


def _snapshot(tmp_path: Path, roots: dict[str, Path] | None) -> dict[str, object]:
    return core_service_health_snapshot(
        coordinator_database=tmp_path / "control.sqlite3",
        recorder_status_file=tmp_path / "status.json",
        relay_host="relay",
        relay_port=8765,
        now=NOW,
        listener_probe=lambda _host, _port: True,
        database_probe=lambda _path: True,
        recorder_reader=lambda _path: _status(),
        incarnation_roots=roots,
    )


def _services(snapshot: dict[str, object]) -> dict[str, dict]:
    return {item["service"]: item for item in snapshot["services"]}


def test_a_healthy_deployment_is_unchanged_by_the_new_signal(
    tmp_path: Path,
) -> None:
    """Every probe passes and no service has crashed: nothing may degrade."""

    root = tmp_path / "data"
    for service in ("flask", "relay", "recorder"):
        path = incarnation_state_file(root, service)
        record_service_start(path, service=service, now=NOW)

    snapshot = _snapshot(tmp_path, {s: root for s in ("flask", "relay", "recorder")})

    assert snapshot["status"] == "ready"
    for item in _services(snapshot).values():
        assert item["readiness"] == "ready"
        assert item["restart"]["state"] == "stable"


def test_a_crash_looping_relay_is_not_ready_even_while_it_answers(
    tmp_path: Path,
) -> None:
    """The listener and store probes both pass; the loop is the finding."""

    root = tmp_path / "data"
    _crash_loop(root, "relay", unclean_starts=CRASH_LOOP_THRESHOLD)

    snapshot = _snapshot(tmp_path, {"relay": root})
    relay = _services(snapshot)["relay"]

    assert relay["liveness"] == "alive"
    assert relay["readiness"] == "not_ready"
    assert relay["code"] == "relay-crash-loop"
    assert relay["restart"]["state"] == "crash-loop"
    # The original probe verdict is preserved rather than discarded.
    assert "relay-ready" in relay["message"]
    assert snapshot["status"] == "degraded"


def test_a_crash_looping_dependency_degrades_flask_without_blaming_it(
    tmp_path: Path,
) -> None:
    """Flask itself is serving. Its dependency verdict is what must change."""

    root = tmp_path / "data"
    _crash_loop(root, "recorder", unclean_starts=CRASH_LOOP_THRESHOLD)

    flask = _services(_snapshot(tmp_path, {"recorder": root}))["flask"]

    assert flask["liveness"] == "alive"
    assert flask["readiness"] == "ready"
    assert flask["dependency"] == "degraded"
    assert flask["code"] == "flask-dependency-degraded"
    assert "recorder" in flask["message"]


def test_one_unclean_restart_is_reported_without_degrading_the_service(
    tmp_path: Path,
) -> None:
    """False-positive prevention: a power cut is data, not a failed product."""

    root = tmp_path / "data"
    _crash_loop(root, "relay", unclean_starts=1)

    snapshot = _snapshot(tmp_path, {"relay": root})
    relay = _services(snapshot)["relay"]

    assert relay["readiness"] == "ready"
    assert relay["code"] == "relay-ready"
    assert relay["restart"]["state"] == "restarting"
    assert relay["restart"]["consecutive_unclean"] == 1
    assert snapshot["status"] == "ready"


def test_operator_stops_never_degrade_the_surface(tmp_path: Path) -> None:
    root = tmp_path / "data"
    path = incarnation_state_file(root, "recorder")
    moment = NOW - timedelta(minutes=5)
    for _ in range(CRASH_LOOP_THRESHOLD + 2):
        record_service_start(path, service="recorder", now=moment)
        record_service_stop(
            path, service="recorder", reason=STOP_OPERATOR, now=moment
        )
        moment += timedelta(seconds=10)

    snapshot = _snapshot(tmp_path, {"recorder": root})

    assert snapshot["status"] == "ready"
    assert _services(snapshot)["recorder"]["restart"]["state"] == "stable"


def test_an_absent_record_leaves_the_verdict_to_the_existing_probes(
    tmp_path: Path,
) -> None:
    """A device that has not yet journalled anything is not thereby unhealthy."""

    snapshot = _snapshot(tmp_path, {"relay": tmp_path / "empty"})
    relay = _services(snapshot)["relay"]

    assert relay["readiness"] == "ready"
    assert "restart" not in relay
    assert snapshot["status"] == "ready"


def test_the_snapshot_is_unchanged_when_no_roots_are_supplied(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(tmp_path, None)

    assert snapshot["status"] == "ready"
    for item in _services(snapshot).values():
        assert "restart" not in item


def test_optional_ai_services_are_absent_from_core_health(tmp_path: Path) -> None:
    """An unreachable language model is not a core-FCP failure.

    ``ollama`` and ``model-provider`` also run under ``restart: unless-stopped``,
    so they can crash-loop too. They are deliberately not core services and must
    never appear here, or an optional dependency would read as a broken product.
    """

    root = tmp_path / "data"
    for optional in ("ollama", "model-provider"):
        _crash_loop(root, optional, unclean_starts=CRASH_LOOP_THRESHOLD + 2)

    snapshot = _snapshot(
        tmp_path,
        {"flask": root, "relay": root, "recorder": root},
    )

    assert set(_services(snapshot)) == {"flask", "relay", "recorder"}
    assert snapshot["status"] == "ready"


def test_default_roots_follow_the_deployed_mounts(monkeypatch) -> None:
    """The three roots must already be reachable from the web container."""

    monkeypatch.setenv("FCP_DATA_ROOT", "/app/data")
    monkeypatch.setenv("FCP_RECORDER_DATA_DIR", "/app/data")
    monkeypatch.setenv(
        "FCP_FEDERATION_COORDINATOR_DATABASE", "/var/lib/fcp-relay/control.sqlite3"
    )

    roots = default_incarnation_roots()

    assert roots["flask"] == Path("/app/data")
    assert roots["recorder"] == Path("/app/data")
    assert roots["relay"] == Path("/var/lib/fcp-relay")

"""Read-only semantic health for FCP's required core services.

B06 distinguishes three questions that Docker process state cannot answer by
itself:

* liveness -- is the service endpoint / heartbeat presently observable?
* readiness -- can the service perform its required product role now?
* dependency health -- is the service usable while one of its dependencies is
  degraded?

The probes in this module never mutate authority, launch processes, inspect the
Docker socket, or persist health history. They deliberately reuse existing
runtime evidence and keep every active probe bounded.
"""

from __future__ import annotations

import os
import socket
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from catalog.mtconnect_recorder.native_update import (
    RecorderRuntimeStatus,
    read_recorder_status,
)

RELAY_CONNECT_TIMEOUT_SECONDS = 0.5
RELAY_DATABASE_TIMEOUT_SECONDS = 0.5
RECORDER_HEARTBEAT_MAX_AGE_SECONDS = 10.0


@dataclass(frozen=True)
class CoreServiceHealth:
    """Public-safe semantic health for one required service."""

    service: str
    liveness: str
    readiness: str
    dependency: str
    code: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {
            "service": self.service,
            "liveness": self.liveness,
            "readiness": self.readiness,
            "dependency": self.dependency,
            "code": self.code,
            "message": self.message,
        }


def _bounded_relay_listener_probe(host: str, port: int) -> bool:
    try:
        with socket.create_connection(
            (host, port),
            timeout=RELAY_CONNECT_TIMEOUT_SECONDS,
        ):
            return True
    except (OSError, TimeoutError):
        return False


def _bounded_sqlite_read_probe(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(
            uri,
            uri=True,
            timeout=RELAY_DATABASE_TIMEOUT_SECONDS,
        ) as connection:
            connection.execute("SELECT 1").fetchone()
        return True
    except (OSError, sqlite3.Error):
        return False


def _recorder_heartbeat_fresh(
    status: RecorderRuntimeStatus,
    *,
    now: datetime,
) -> bool:
    heartbeat = status.heartbeat_at
    if heartbeat is None:
        return False
    age = (now - heartbeat).total_seconds()
    return (
        -RECORDER_HEARTBEAT_MAX_AGE_SECONDS
        <= age
        <= RECORDER_HEARTBEAT_MAX_AGE_SECONDS
    )


def relay_health(
    *,
    host: str,
    port: int,
    coordinator_database: Path,
    listener_probe: Callable[[str, int], bool] = _bounded_relay_listener_probe,
    database_probe: Callable[[Path], bool] = _bounded_sqlite_read_probe,
) -> CoreServiceHealth:
    """Report relay listener liveness separately from authority-store readiness."""

    if not listener_probe(host, port):
        return CoreServiceHealth(
            service="relay",
            liveness="unavailable",
            readiness="not_ready",
            dependency="unavailable",
            code="relay-listener-unavailable",
            message="The Federation relay listener is not reachable.",
        )
    if not database_probe(coordinator_database):
        return CoreServiceHealth(
            service="relay",
            liveness="alive",
            readiness="not_ready",
            dependency="degraded",
            code="relay-authority-store-unavailable",
            message=(
                "The relay listener is reachable, but its authoritative Federation "
                "store is not readable."
            ),
        )
    return CoreServiceHealth(
        service="relay",
        liveness="alive",
        readiness="ready",
        dependency="healthy",
        code="relay-ready",
        message="The Federation relay listener and authority store are available.",
    )


def recorder_health(
    status_file: Path,
    *,
    now: datetime | None = None,
    reader: Callable[[Path], RecorderRuntimeStatus] = read_recorder_status,
) -> CoreServiceHealth:
    """Use the existing heartbeat, not another-container PID inference."""

    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    status = reader(status_file)
    if not _recorder_heartbeat_fresh(status, now=moment):
        return CoreServiceHealth(
            service="recorder",
            liveness="unavailable",
            readiness="not_ready",
            dependency="unavailable",
            code="recorder-heartbeat-stale",
            message="The managed recorder has no fresh heartbeat.",
        )

    state = status.state or "unknown"
    ready = state in {"recording", "stopped"}
    federation_healthy = status.federation_status == "connected"

    if not ready:
        return CoreServiceHealth(
            service="recorder",
            liveness="alive",
            readiness="not_ready",
            dependency=("healthy" if federation_healthy else "degraded"),
            code=f"recorder-{state}",
            message=f"The recorder heartbeat is fresh, but its state is {state}.",
        )
    if not federation_healthy:
        return CoreServiceHealth(
            service="recorder",
            liveness="alive",
            readiness="ready",
            dependency="degraded",
            code="recorder-federation-degraded",
            message=(
                "The recorder is ready for its local capture role, but its Federation "
                "dependency is not connected."
            ),
        )
    return CoreServiceHealth(
        service="recorder",
        liveness="alive",
        readiness="ready",
        dependency="healthy",
        code="recorder-ready",
        message=(
            "The managed recorder heartbeat is fresh and its Federation is connected."
        ),
    )


def flask_health(
    *,
    relay: CoreServiceHealth,
    recorder: CoreServiceHealth,
) -> CoreServiceHealth:
    """A serving Flask request proves liveness/readiness; dependencies may degrade."""

    degraded = [
        item.service
        for item in (relay, recorder)
        if item.readiness != "ready" or item.dependency != "healthy"
    ]
    if degraded:
        names = ", ".join(degraded)
        return CoreServiceHealth(
            service="flask",
            liveness="alive",
            readiness="ready",
            dependency="degraded",
            code="flask-dependency-degraded",
            message=(
                "The FCP web/control surface is serving, with degraded dependencies: "
                f"{names}."
            ),
        )
    return CoreServiceHealth(
        service="flask",
        liveness="alive",
        readiness="ready",
        dependency="healthy",
        code="flask-ready",
        message="The FCP web/control surface and required dependencies are ready.",
    )


def core_service_health_snapshot(
    *,
    coordinator_database: Path,
    recorder_status_file: Path,
    relay_host: str | None = None,
    relay_port: int | None = None,
    now: datetime | None = None,
    listener_probe: Callable[[str, int], bool] = _bounded_relay_listener_probe,
    database_probe: Callable[[Path], bool] = _bounded_sqlite_read_probe,
    recorder_reader: Callable[[Path], RecorderRuntimeStatus] = read_recorder_status,
) -> dict[str, object]:
    """Return bounded, public-safe semantic health for the three core services."""

    host = (relay_host or os.getenv("FCP_RELAY_HEALTH_HOST", "relay")).strip()
    host = host or "relay"
    port = relay_port
    if port is None:
        try:
            port = int(os.getenv("FCP_RELAY_PORT", "8765"))
        except ValueError:
            port = 8765
    if not 1 <= int(port) <= 65535:
        port = 8765

    relay = relay_health(
        host=host,
        port=int(port),
        coordinator_database=coordinator_database,
        listener_probe=listener_probe,
        database_probe=database_probe,
    )
    recorder = recorder_health(
        recorder_status_file,
        now=now,
        reader=recorder_reader,
    )
    flask = flask_health(relay=relay, recorder=recorder)
    services = (flask, relay, recorder)
    all_ready = all(
        item.readiness == "ready" and item.dependency == "healthy"
        for item in services
    )
    return {
        "status": "ready" if all_ready else "degraded",
        "services": [item.to_dict() for item in services],
    }


__all__ = [
    "CoreServiceHealth",
    "core_service_health_snapshot",
    "flask_health",
    "recorder_health",
    "relay_health",
]

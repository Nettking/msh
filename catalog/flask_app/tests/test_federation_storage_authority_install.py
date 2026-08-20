"""Supervision of the creator's logical-storage authority.

The authority is the reason a Federation can accept published data at all. These
tests pin the operator-visible lifecycle: install defaults alone do not start it,
non-creators are fenced, and an enabled creator remains restartable.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from flask import Flask

from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.recorder_storage_relay import (
    STORAGE_CONTROL_CAPABILITY_PROTOCOL,
    STORAGE_CONTROL_CAPABILITY_TYPE,
    STORAGE_CONTROL_CAPABILITY_VERSION,
)
from catalog.flask_app.services.federation_storage_authority_install import (
    FederationStorageAuthorityMonitor,
    install_federation_storage_authority,
)

SESSION = "session-alpha"
DEVICE = "node-leader"
RELAY = "ws://100.64.0.1:8765"


@dataclass(frozen=True)
class _Binding:
    internal_session_id: str = SESSION
    device_id: str = DEVICE


@dataclass(frozen=True)
class _Session:
    created_by_node_id: str = DEVICE


class _Store:
    def __init__(self, session: _Session | None) -> None:
        self._session = session

    def get_session(self, session_id: str) -> _Session | None:
        assert session_id == SESSION
        return self._session


class _Coordinator:
    def __init__(self, session: _Session | None) -> None:
        self.store = _Store(session)


@dataclass(frozen=True)
class _Context:
    binding: _Binding
    coordinator: _Coordinator


class _Onboarding:
    def __init__(self, context: _Context | None) -> None:
        self._context = context

    def authorized_context(self) -> _Context | None:
        return self._context


def _app(**config: object) -> Flask:
    app = Flask(__name__)
    app.config.update(config)
    return app


def _monitor(
    *,
    context: _Context | None,
    **config: object,
) -> FederationStorageAuthorityMonitor:
    settings: dict[str, object] = {
        "FEDERATION_STORAGE_AUTHORITY_ENABLED": True,
        "FEDERATION_STORAGE_AUTHORITY_RELAY_URL": RELAY,
    }
    settings.update(config)
    app = _app(**settings)
    return install_federation_storage_authority(
        app, onboarding_service=_Onboarding(context)
    )


def _creator_context() -> _Context:
    return _Context(_Binding(), _Coordinator(_Session()))


def test_the_authority_is_opt_in_at_the_install_layer_and_reports_being_off():
    monitor = _monitor(
        context=_creator_context(),
        FEDERATION_STORAGE_AUTHORITY_ENABLED=False,
    )

    monitor.start()
    snapshot = monitor.snapshot()

    assert snapshot.status == "disabled"
    assert snapshot.enabled is False


def test_defaults_are_installed_without_starting_anything():
    app = _app()

    install_federation_storage_authority(app, onboarding_service=_Onboarding(None))

    assert app.config["FEDERATION_STORAGE_AUTHORITY_ENABLED"] is False
    assert app.config["FEDERATION_STORAGE_AUTHORITY_STATE_DIR"]
    assert app.config["FEDERATION_STORAGE_AUTHORITY_SCAN_INTERVAL_SECONDS"] > 0
    assert app.config["FEDERATION_STORAGE_AUTHORITY_LEASE_SECONDS"] > 0


def test_a_non_creator_device_refuses_instead_of_announcing_into_the_void():
    context = _Context(_Binding(), _Coordinator(_Session(created_by_node_id="other")))
    monitor = _monitor(context=context)

    monitor.start()
    snapshot = monitor.snapshot()

    assert snapshot.status == "not-session-creator"
    assert snapshot.last_error_code == "storage-authority-not-session-creator"


def test_the_creator_is_recognised():
    monitor = _monitor(context=_creator_context())

    with monitor.app.app_context():
        assert monitor.session_creator_state() == "creator"


def test_an_unknown_creator_does_not_block_startup():
    monitor = _monitor(context=_Context(_Binding(), _Coordinator(None)))

    with monitor.app.app_context():
        assert monitor.session_creator_state() == "unknown"


def test_settings_require_a_relay_address():
    monitor = _monitor(
        context=_creator_context(),
        FEDERATION_STORAGE_AUTHORITY_RELAY_URL="",
    )

    with monitor.app.app_context(), pytest.raises(Exception) as caught:
        monitor.build_settings()

    assert getattr(caught.value, "code", "") == "storage-authority-relay-required"


def test_settings_require_a_trusted_federation():
    monitor = _monitor(context=None)

    with monitor.app.app_context(), pytest.raises(Exception) as caught:
        monitor.build_settings()

    assert getattr(caught.value, "code", "") == (
        "storage-authority-federation-required"
    )


def test_a_disabled_authority_never_composes_settings():
    monitor = _monitor(
        context=_creator_context(),
        FEDERATION_STORAGE_AUTHORITY_ENABLED=False,
    )

    with monitor.app.app_context(), pytest.raises(Exception) as caught:
        monitor.build_settings()

    assert getattr(caught.value, "code", "") == "storage-authority-disabled"


def test_settings_carry_the_authorized_session_and_configured_locations():
    monitor = _monitor(
        context=_creator_context(),
        FEDERATION_STORAGE_AUTHORITY_STATE_DIR="state/dir",
        FEDERATION_STORAGE_AUTHORITY_DISPLAY_NAME="Leader authority",
    )

    with monitor.app.app_context():
        settings = monitor.build_settings()

    assert settings.session_id == SESSION
    assert settings.relay == RELAY
    assert settings.state_dir == "state/dir"
    assert settings.display_name == "Leader authority"


def test_readiness_is_reported_from_the_announcement():
    monitor = _monitor(context=_creator_context())

    monitor._on_announced(
        CapabilityAnnouncement(
            capability_id="logical-storage-authority",
            node_id=DEVICE,
            session_id=SESSION,
            type=STORAGE_CONTROL_CAPABILITY_TYPE,
            protocol=STORAGE_CONTROL_CAPABILITY_PROTOCOL,
            protocol_version=STORAGE_CONTROL_CAPABILITY_VERSION,
            status=CapabilityStatus.READY,
            properties={"kind": "x", "group_ids": ["fcp-local-storage"]},
            announced_at=datetime.now(timezone.utc),
        )
    )
    snapshot = monitor.snapshot()

    assert snapshot.status == "ready"
    assert snapshot.ready_group_ids == ("fcp-local-storage",)


def test_an_authority_without_groups_is_not_reported_as_ready():
    monitor = _monitor(context=_creator_context())

    monitor._on_announced(
        CapabilityAnnouncement(
            capability_id="logical-storage-authority",
            node_id=DEVICE,
            session_id=SESSION,
            type=STORAGE_CONTROL_CAPABILITY_TYPE,
            protocol=STORAGE_CONTROL_CAPABILITY_PROTOCOL,
            protocol_version=STORAGE_CONTROL_CAPABILITY_VERSION,
            status=CapabilityStatus.UNAVAILABLE,
            properties={"kind": "x", "group_ids": []},
            announced_at=datetime.now(timezone.utc),
        )
    )
    snapshot = monitor.snapshot()

    assert snapshot.status == "no-groups"
    assert snapshot.ready_group_ids == ()


def test_a_failing_composition_waits_and_stays_restartable(monkeypatch):
    monitor = _monitor(context=_creator_context())
    attempts: list[int] = []

    def failing_settings():
        attempts.append(1)
        if len(attempts) >= 2:
            monitor._stop.set()
        raise RuntimeError("not ready")

    monkeypatch.setattr(monitor, "build_settings", failing_settings)
    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "_RETRY_SECONDS",
        0.01,
    )

    monitor._run()

    assert len(attempts) >= 2
    assert monitor.snapshot().status in {"waiting", "stopped"}
    assert monitor.snapshot().last_error_code == "RuntimeError"


def test_stopping_before_a_loop_exists_is_safe():
    monitor = _monitor(context=_creator_context())

    monitor.stop()

    assert monitor._stop.is_set()


def test_the_running_authority_is_stopped_between_scans(monkeypatch):
    started = threading.Event()
    stopped = threading.Event()

    async def fake_run(settings, *, stop=None, on_announced=None):
        started.set()
        await stop.wait()
        stopped.set()

    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "run_trusted_storage_authority",
        fake_run,
    )
    monitor = _monitor(context=_creator_context())

    monitor.start()
    assert started.wait(5.0)
    monitor.stop()

    assert stopped.wait(5.0)


def test_run_storage_authority_stops_promptly_on_a_long_interval():
    # The standalone node-layer runtime still keeps its interruptible stop seam.
    from catalog.node import storage_failover

    async def scenario() -> float:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        started = loop.time()

        async def composed(settings, *, command, stop=None, on_announced=None):
            assert command == "run"
            await asyncio.wait_for(stop.wait(), 5.0)
            return []

        original = storage_failover._run
        storage_failover._run = composed
        try:
            task = asyncio.create_task(
                storage_failover.run_storage_authority(None, stop=stop)
            )
            await asyncio.sleep(0.05)
            stop.set()
            await asyncio.wait_for(task, 2.0)
        finally:
            storage_failover._run = original
        return loop.time() - started

    assert asyncio.run(scenario()) < 2.0


def test_a_cancelled_runtime_does_not_kill_the_supervising_thread(monkeypatch):
    """``CancelledError`` is a ``BaseException`` and escaped the retry boundary.

    Physical testing showed the thread dying outright with the authority status
    frozen at ``starting`` while every request restarted and re-killed it.
    """

    attempts: list[int] = []

    async def fake_run(settings, *, stop=None, on_announced=None):
        attempts.append(1)
        if len(attempts) == 1:
            raise asyncio.CancelledError
        monitor._stop.set()

    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "run_trusted_storage_authority",
        fake_run,
    )
    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "_RETRY_SECONDS",
        0.01,
    )
    monitor = _monitor(context=_creator_context())
    first_error: list[str | None] = []

    original_set = monitor._set_snapshot

    def record(status, **kwargs):
        original_set(status, **kwargs)
        if status == "retrying":
            first_error.append(kwargs.get("error_code"))

    monkeypatch.setattr(monitor, "_set_snapshot", record)

    monitor._run()

    assert len(attempts) == 2
    assert first_error == ["storage-authority-cancelled"]
    assert monitor.snapshot().status == "stopped"


def test_the_supervised_thread_stays_alive_across_a_cancelled_runtime(monkeypatch):
    started = threading.Event()
    cancelled_once = threading.Event()
    attempts: list[int] = []

    async def fake_run(settings, *, stop=None, on_announced=None):
        attempts.append(1)
        if len(attempts) == 1:
            cancelled_once.set()
            raise asyncio.CancelledError
        started.set()
        await stop.wait()

    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "run_trusted_storage_authority",
        fake_run,
    )
    monkeypatch.setattr(
        "catalog.flask_app.services.federation_storage_authority_install."
        "_RETRY_SECONDS",
        0.01,
    )
    monitor = _monitor(context=_creator_context())

    monitor.start()
    try:
        assert cancelled_once.wait(5.0)
        # The authority must come back by itself rather than needing a new
        # request to notice a dead thread.
        assert started.wait(5.0)
        assert monitor._thread is not None and monitor._thread.is_alive()
    finally:
        monitor.stop()

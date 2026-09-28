"""Aged authority history stays verified without blocking relay scheduling."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.live_failover import (
    LiveFailoverStore,
    StorageFailoverCoordinator,
)
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.federation.tests.test_manifest_read_snapshot import seeded


@pytest.fixture(scope="module")
def aged_database(tmp_path_factory):
    control = seeded(tmp_path_factory.mktemp("scan-history"), count=130)
    control.register_provider("session-1", "authority", StorageProviderRegistration(
        session_id="session-1", provider_id="primary", node_id="owner",
        protocol=STORAGE_PROTOCOL, protocol_version=STORAGE_PROTOCOL_VERSION,
        authorized=True, status="ready",
    ))
    control.change_assignment("session-1", "authority", "storage-main", "primary")
    now = datetime.now(timezone.utc)
    control.grant_leader(
        "session-1", "authority", "storage-main", "primary", "grant", 1, 1,
        lease_expires_at=now + timedelta(minutes=10), occurred_at=now,
    )
    return control.database


def scanner(tmp_path, aged_database, *, online=False):
    path = tmp_path / "control.sqlite3"
    with sqlite3.connect(aged_database) as source, sqlite3.connect(path) as target:
        source.backup(target)
    control = PhaseDControlPlane(path)
    status = {
        "sessions": [],
        "nodes": [{"node_id": "owner", "connection_state": "connected" if online else "disconnected"}],
        "capabilities": [{"node_id": "owner", "type": "storage-provider", "status": "ready", "properties": {"provider_id": "primary"}}],
    }
    coordinator = StorageFailoverCoordinator(
        session_coordinator=SimpleNamespace(status=lambda **_kwargs: status),
        control_plane=control,
        publication_store=None,
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id="authority")),
        channel=SimpleNamespace(set_refresh_handler=lambda _handler: None),
        failover_store=LiveFailoverStore(tmp_path / "failover.sqlite3"),
        session_id="session-1",
    )
    return coordinator, control


def test_live_primary_does_not_revalidate_unneeded_historical_manifests(
    tmp_path, aged_database, monkeypatch,
):
    coordinator, control = scanner(tmp_path, aged_database, online=True)
    def unexpected(*_args, **_kwargs):
        raise AssertionError("A proven live primary does not need failover history")
    monkeypatch.setattr(control.manifests, "head", unexpected)
    assert asyncio.run(coordinator.scan_once()) == ()


@pytest.mark.parametrize("tamper", [None, "history", "head"])
def test_offline_primary_fully_validates_aged_history_on_each_scan(
    tmp_path, aged_database, monkeypatch, tamper,
):
    coordinator, control = scanner(tmp_path, aged_database)
    decoded = []
    original = control.manifests._decode_revision
    def observe(row, **kwargs):
        decoded.append(row["revision"])
        return original(row, **kwargs)
    monkeypatch.setattr(control.manifests, "_decode_revision", observe)
    result = asyncio.run(coordinator.scan_once())
    assert result[0].status == "blocked"
    assert decoded == list(range(131))
    if tamper is None:
        return
    with sqlite3.connect(control.database) as connection:
        if tamper == "head":
            connection.execute("UPDATE storage_manifest_heads SET revision=1")
        else:
            value = json.loads(connection.execute(
                "SELECT manifest_json FROM storage_manifest_revisions WHERE revision=1"
            ).fetchone()[0])
            value["items"][0]["content_hash"] = "sha256:" + "f" * 64
            connection.execute(
                "UPDATE storage_manifest_revisions SET manifest_json=? WHERE revision=1",
                (json.dumps(value),),
            )
    with pytest.raises(FederationValidationError):
        asyncio.run(coordinator.scan_once())


@pytest.mark.parametrize("missing", [False, True])
def test_cancelled_validation_keeps_relay_responsive_and_cannot_mutate_later(
    tmp_path, aged_database, monkeypatch, missing,
):
    coordinator, control = scanner(tmp_path, aged_database)
    if missing:
        with sqlite3.connect(control.database) as connection:
            connection.execute("DELETE FROM storage_manifest_heads")
            connection.execute("DELETE FROM storage_manifest_revisions")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = control.manifests.head
    writes = []
    def held_validation(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(3), "event loop did not run while history was read"
            return original(*args, **kwargs)
        finally:
            finished.set()
    monkeypatch.setattr(control.manifests, "head", held_validation)
    monkeypatch.setattr(control, "set_storage_degraded_state", lambda *_a, **_k: writes.append("degraded"))
    async def scenario():
        task = asyncio.create_task(coordinator.scan_once())
        try:
            assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 2.5)
            # This callback stands for relay heartbeat/response work. It runs
            # before validation is released, not merely after a slow scan ends.
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 3)
        assert writes == []
        if missing:
            with sqlite3.connect(control.database) as connection:
                assert connection.execute("SELECT COUNT(*) FROM storage_manifest_revisions").fetchone()[0] == 0
    asyncio.run(scenario())


def test_read_only_manifest_lookup_never_creates_genesis(tmp_path):
    control = PhaseDControlPlane(tmp_path / "missing.sqlite3")
    control.store.create_group("session-1", "authority", "storage-main")
    with pytest.raises(FederationValidationError) as failure:
        control.manifest("session-1", "storage-main", read_only=True)
    assert failure.value.code == "manifest-not-found"
    with sqlite3.connect(control.database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM storage_manifest_revisions").fetchone()[0] == 0
    assert control.manifest("session-1", "storage-main").revision == 0

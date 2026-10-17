"""Cold manifest validation must leave the shared coordinator writer available."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace

import pytest

from catalog.federation import manifest_store
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest_store import AuthoritativeManifestStore
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.tests.test_manifest_read_snapshot import seeded
from catalog.federation.tests.test_phase_e1_manifest_store import NOW, intent, proposal
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer


@pytest.mark.parametrize("operation", ["commit", "finalize"])
def test_cold_prewarm_allows_authenticated_shared_database_heartbeats(
    tmp_path, monkeypatch, operation,
):
    async def scenario():
        database = tmp_path / "control.sqlite3"
        relay = RelayServer(
            SessionCoordinator(database), host="127.0.0.1", port=0,
            auth_timeout_seconds=10, send_timeout_seconds=10,
            heartbeat_timeout_seconds=300, sweep_interval_seconds=300,
        )
        client = RelayNodeClient(
            state_directory=tmp_path / "owner", relay_url="ws://127.0.0.1:1",
            display_name="Owner", allow_insecure_local=True,
            heartbeat_interval=60, request_timeout=10,
        )
        entered, release = threading.Event(), threading.Event()
        # A broken implementation must fail promptly rather than blocking the
        # event loop in SQLite's 30-second busy handler until test teardown.
        watchdog = threading.Timer(3, release.set)
        worker = None
        await relay.start()
        client.relay_url = relay.url
        try:
            token = relay.coordinator.create_enrollment_token(
                ttl_seconds=60, max_uses=1,
            )["token"]
            await client.connect(enrollment_token=token)
            session_id = (await client.create_session("Manifest heartbeat"))["session_id"]
            control = PhaseDControlPlane(database)
            revision = control.create_group(session_id, client.node_id, "storage-main").revision
            store = control.manifests
            value = replace(proposal(), session_id=session_id, expected_control_revision=revision)
            prepared = None
            if operation == "finalize":
                prepared = store.prepare_intent(replace(
                    intent(), session_id=session_id, prepared_control_revision=revision,
                ))
            store._verified_revisions = manifest_store._VerifiedRevisionCache()
            original = store._history
            calls = 0

            def pause_cold_read(connection, session, group):
                nonlocal calls
                calls += 1
                if calls == 1:
                    # Acquire the actual read snapshot before pausing decoding.
                    connection.execute("SELECT revision FROM storage_manifest_heads").fetchall()
                    entered.set()
                    assert release.wait(5)
                return original(connection, session, group)

            monkeypatch.setattr(store, "_history", pause_cold_read)
            if operation == "commit":
                worker = asyncio.create_task(asyncio.to_thread(store.commit, value))
            else:
                worker = asyncio.create_task(asyncio.to_thread(
                    store.finalize_intent, prepared,
                    acknowledged_provider_ids=("provider-a", "provider-b"),
                    expected_control_revision=revision, manifest_term=1, now=NOW,
                ))
            assert await asyncio.to_thread(entered.wait, 3)
            watchdog.start()
            for _ in range(2):
                response = await client.request("heartbeat", payload={}, timeout=0.5)
                assert response["connection_state"] == "connected"
                assert not release.is_set(), "heartbeat waited for the manifest writer lock"
            assert client.connected_event.is_set()
            release.set()
            result = await worker
            assert result.created and result.item_revision == 1
            assert calls == 2  # Prewarm plus unchanged fresh writer validation.
        finally:
            release.set()
            watchdog.cancel()
            if worker is not None:
                await worker
            await client.disconnect()
            await relay.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["commit", "finalize"])
@pytest.mark.parametrize("tamper", ["json", "index", "index-type", "head", "projection"])
def test_fresh_writer_validation_rejects_tampering_after_prewarm(
    tmp_path, monkeypatch, operation, tamper,
):
    control = seeded(tmp_path, 2)
    store = control.manifests
    prepared = store.prepare_intent(intent(item_id="next", idempotency_key="recorder:next"))
    original = store.history

    def tamper_after_valid_read(session, group):
        result = original(session, group)
        with sqlite3.connect(control.database) as connection:
            if tamper == "json":
                value = json.loads(connection.execute(
                    "SELECT manifest_json FROM storage_manifest_revisions WHERE revision=1",
                ).fetchone()[0])
                value["items"][0]["content_hash"] = "sha256:" + "f" * 64
                connection.execute(
                    "UPDATE storage_manifest_revisions SET manifest_json=? WHERE revision=1",
                    (json.dumps(value),),
                )
            elif tamper in ("index", "index-type"):
                connection.execute(
                    "UPDATE storage_manifest_revisions SET term=? WHERE revision=1",
                    (2 if tamper == "index" else 1.5,),
                )
            elif tamper == "head":
                connection.execute("UPDATE storage_manifest_heads SET manifest_hash='corrupted'")
            else:
                connection.execute("DELETE FROM storage_manifest_items WHERE item_id='batch-0'")
        return result

    monkeypatch.setattr(store, "history", tamper_after_valid_read)
    with pytest.raises(FederationValidationError):
        if operation == "commit":
            store.commit(proposal(item_id="next", idempotency_key="recorder:next"))
        else:
            store.finalize_intent(
                prepared, acknowledged_provider_ids=("provider-a", "provider-b"),
                expected_control_revision=1, manifest_term=1, now=NOW,
            )
    with sqlite3.connect(control.database) as connection:
        assert connection.execute("SELECT MAX(revision) FROM storage_manifest_revisions").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM storage_manifest_items WHERE item_id='next'").fetchone()[0] == 0
        assert connection.execute("SELECT state FROM storage_manifest_intents WHERE item_id='next'").fetchone()[0] == "prepared"


def test_fresh_writer_control_revision_refuses_change_and_normal_retry_succeeds(tmp_path, monkeypatch):
    control = seeded(tmp_path, 2)
    original = control.manifests.history

    def change_control_after_prewarm(session, group):
        result = original(session, group)
        control.store.create_group("session-1", "coordinator-1", "other-group")
        return result

    monkeypatch.setattr(control.manifests, "history", change_control_after_prewarm)
    with pytest.raises(FederationValidationError) as rejected:
        control.manifests.commit(proposal(item_id="next", idempotency_key="recorder:next"))
    assert rejected.value.code == "concurrent-control-change"
    monkeypatch.setattr(control.manifests, "history", original)
    result = control.manifests.commit(proposal(
        item_id="next", idempotency_key="recorder:next", expected_control_revision=2,
    ))
    assert result.created and result.item_revision == 3


def test_fresh_writer_includes_legitimate_concurrent_history_and_keeps_idempotency(tmp_path, monkeypatch):
    control = seeded(tmp_path, 2)
    writer = AuthoritativeManifestStore(control.database)
    original = control.manifests.history
    advanced = False

    def append_after_prewarm(session, group):
        nonlocal advanced
        result = original(session, group)
        if not advanced:
            advanced = True
            writer.commit(proposal(item_id="concurrent", idempotency_key="recorder:concurrent"))
        return result

    monkeypatch.setattr(control.manifests, "history", append_after_prewarm)
    value = proposal(item_id="next", idempotency_key="recorder:next")
    result = control.manifests.commit(value)
    assert result.created and result.item_revision == 4
    assert result.manifest.previous_manifest_hash == writer.at_revision("session-1", "storage-main", 3).manifest_hash
    assert not control.manifests.commit(value).created
    assert writer.head("session-1", "storage-main").revision == 4


def test_mmap_is_bounded_per_connection_and_durability_pragmas_are_unchanged(tmp_path):
    control = seeded(tmp_path, 1)
    for _ in range(2):
        with closing(control.manifests._connect()) as connection:
            assert 0 <= connection.execute("PRAGMA mmap_size").fetchone()[0] <= 1024 * 1024 * 1024
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
            assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 30000


def test_unsupported_mmap_noop_keeps_validation_and_commit_correct(tmp_path, monkeypatch):
    control = seeded(tmp_path, 1)
    original_connect = sqlite3.connect

    class NoMmapConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql.startswith("PRAGMA mmap_size="):
                sql = "PRAGMA mmap_size=0"
            return super().execute(sql, parameters)

    def connect(*args, **kwargs):
        return original_connect(*args, **kwargs, factory=NoMmapConnection)

    monkeypatch.setattr(manifest_store.sqlite3, "connect", connect)
    result = control.manifests.commit(proposal(item_id="next", idempotency_key="recorder:next"))
    assert result.created and result.item_revision == 2
    with closing(control.manifests._connect()) as connection:
        assert connection.execute("PRAGMA mmap_size").fetchone()[0] == 0
        connection.execute("UPDATE storage_manifest_revisions SET term=2 WHERE revision=1")
        connection.commit()
    with pytest.raises(FederationValidationError):
        control.manifests.commit(proposal(item_id="third", idempotency_key="recorder:third"))

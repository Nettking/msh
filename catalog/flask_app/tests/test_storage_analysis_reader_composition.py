from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from flask import Flask

from catalog.ai.relay_remote import RelayRemoteAIEndpoint
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.errors import FederationOperationError
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.flask_app.services import federation_pairing_install as pairing
from catalog.flask_app.services import federation_storage_authority_install as install
from catalog.flask_app.services import trusted_storage_authority_runtime as storage
from catalog.flask_app.tests.test_storage_authority_local_provider_runtime import (
    _LoopbackRelayClient,
    _register_primary,
    _settings,
)


def _monitor(monkeypatch, client, source, *, creator=True, storage_enabled=True):
    app = Flask(__name__)
    app.config["FEDERATION_STORAGE_AUTHORITY_ENABLED"] = storage_enabled
    identity = SimpleNamespace(node_id=client.node_id, session_id="session-a")
    runtime = SimpleNamespace(
        ensure_connected=lambda _state: None,
        _connected_client=lambda: client,
        coordinator_status=lambda: {"sessions": [{
            "session_id": "session-a",
            "created_by_node_id": client.node_id if creator else "other-creator",
        }]},
    )
    monitor = pairing.SavedFederationReconnectMonitor(
        app, SimpleNamespace(relay_runtime=runtime))
    context = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id=identity.session_id),
        credentials=SimpleNamespace(identity=identity),
    )
    monkeypatch.setattr(monitor, "_connected_state_and_context", lambda: (object(), context))
    monitor.ai_bridge._endpoint = source
    monkeypatch.setattr(monitor.ai_bridge, "_transport_context", lambda *_: (
        monitor.ai_bridge._endpoint, None))
    calls = []

    def bind(**kwargs):
        endpoint = RelayLifecycleEndpoint(client, message_source=kwargs["upstream_message_source"])
        calls.append(endpoint)
        return endpoint

    monkeypatch.setattr(pairing.DeviceFederationAuthority, "from_authenticated_relay", bind)
    return monitor, identity, calls


def test_creator_analysis_waits_for_current_storage_stage(tmp_path, monkeypatch):
    client = _LoopbackRelayClient("node-creator")
    source = RelayRemoteAIEndpoint(client)
    monitor, identity, calls = _monitor(monkeypatch, client, source)
    with pytest.raises(FederationOperationError) as error:
        monitor.analysis_authority(identity, tmp_path, lambda: datetime.now(timezone.utc))
    assert error.value.code == "analysis-storage-transport-not-ready"
    assert not calls


@pytest.mark.parametrize("stale", [False, True])
def test_creator_refuses_a_replaced_or_different_clients_storage_view(
    tmp_path, monkeypatch, stale,
):
    client = _LoopbackRelayClient("node-creator")
    ai = RelayRemoteAIEndpoint(client)
    monitor, identity, calls = _monitor(monkeypatch, client, ai)
    source = ai if stale else RelayRemoteAIEndpoint(_LoopbackRelayClient("other-node"))
    view = install._StorageAwareRelayView(source, object())
    monitor.ai_bridge._endpoint = ai if stale else view
    monkeypatch.setattr(monitor.ai_bridge, "_transport_context", lambda *_: (view, None))
    with pytest.raises(FederationOperationError) as error:
        monitor.analysis_authority(identity, tmp_path, lambda: datetime.now(timezone.utc))
    assert error.value.code == "analysis-storage-transport-not-ready"
    assert not calls


@pytest.mark.parametrize("creator,storage_enabled", [(False, True), (True, False)])
def test_analysis_without_local_storage_uses_existing_single_reader(
    tmp_path, monkeypatch, creator, storage_enabled,
):
    client = _LoopbackRelayClient("node-device")
    source = RelayRemoteAIEndpoint(client)
    monitor, identity, calls = _monitor(
        monkeypatch, client, source, creator=creator, storage_enabled=storage_enabled)
    endpoint = monitor.analysis_authority(identity, tmp_path, lambda: datetime.now(timezone.utc))
    assert endpoint is calls[0]
    assert endpoint.message_source is source


def test_authority_restart_restoration_rebinds_analysis_and_keeps_provider_replies(
    tmp_path, monkeypatch,
):
    async def scenario():
        client = _LoopbackRelayClient("node-creator")
        ai = RelayRemoteAIEndpoint(client)
        await ai.start()
        monitor, identity, calls = _monitor(monkeypatch, client, ai)
        settings = _settings(tmp_path)
        control = PhaseDControlPlane(settings.storage_control_database)
        now = datetime.now(timezone.utc)
        provider_id = storage._builtin_local_provider_id(client.node_id)
        _register_primary(control, session_id=settings.session_id,
                          provider_id=provider_id, node_id=client.node_id, now=now)
        shared = SimpleNamespace(bridge=monitor.ai_bridge, message_source=ai)
        previous_generation = monitor.analysis_authority_generation()
        previous_lifecycle = None
        try:
            for attempt in range(3):
                # The monitor may be polled before first install or while restored.
                with pytest.raises(FederationOperationError):
                    monitor.analysis_authority(identity, tmp_path, lambda: now)
                endpoint = RelayStorageEndpoint(client, message_source=ai)
                channel = storage.SharedRecorderAwareStorageControlRelayChannel(client, endpoint)
                lifecycle = None
                view = None
                try:
                    service = storage._ensure_builtin_local_storage_service(
                        endpoint=endpoint, control=control, client=client, settings=settings)
                    assert service is not None
                    await endpoint.start()
                    await channel.start()
                    view = install.FederationStorageAuthorityMonitor._install_storage_view(shared, channel)
                    generation = monitor.analysis_authority_generation()
                    assert generation > previous_generation
                    assert monitor.analysis_authority_generation() == generation
                    # A previous reader remains on the closed old storage stage;
                    # it cannot race the new endpoint for the AI upstream queue.
                    if previous_lifecycle is not None:
                        assert previous_lifecycle.message_source is not ai
                    lifecycle = monitor.analysis_authority(identity, tmp_path, lambda: now)
                    await lifecycle.start()
                    assert lifecycle.message_source is view
                    logical = PhaseDLogicalStorageClient(
                        session_id=settings.session_id, actor_node_id=client.node_id,
                        control_plane=control, transport=endpoint,
                        acknowledgements=DurableAcknowledgementStore(settings.acknowledgements_database))
                    content = {"schema": "test.batch.v1", "value": attempt}
                    outcome = await logical.ingest_batch(
                        group_id="fcp-local-storage", dataset_id="test-dataset",
                        batch_id=f"batch-{attempt}", idempotency_key=f"key-{attempt}",
                        content=content, created_at=now)
                    assert outcome.committed
                    assert service.provider.read(session_id=settings.session_id,
                        group_id="fcp-local-storage", batch_id=f"batch-{attempt}") == content
                    # Unrelated downstream traffic still reaches lifecycle's successor.
                    other = SimpleNamespace(payload={"kind": "another-protocol"})
                    await client._messages.put(other)
                    assert await lifecycle.receive_other(timeout=1) is other
                    if previous_lifecycle is not None:
                        await previous_lifecycle.close()
                        assert previous_lifecycle._reader_task is None
                        previous_lifecycle = None
                finally:
                    await channel.close()
                    await endpoint.close()
                    install.FederationStorageAuthorityMonitor._restore_storage_view(shared, view)
                    previous_lifecycle = lifecycle
                restored = monitor.analysis_authority_generation()
                assert restored > generation
                assert monitor.ai_bridge._endpoint is ai
                previous_generation = restored
            assert len(calls) == 3
        finally:
            for lifecycle in calls:
                await lifecycle.close()
            await ai.close()

    asyncio.run(scenario())

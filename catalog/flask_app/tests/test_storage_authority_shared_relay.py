from __future__ import annotations

import asyncio
from types import SimpleNamespace

from flask import Flask

from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.flask_app.services import federation_storage_authority_install as install
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime


class _MessageSource:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[object] = asyncio.Queue()

    async def receive_other(self, *, timeout: float | None = None):
        if timeout is None:
            return await self.messages.get()
        return await asyncio.wait_for(self.messages.get(), timeout=timeout)


class _RelayClient:
    node_id = "node-creator"

    def __init__(self) -> None:
        self.raw_reads = 0

    async def receive_message(self, *, timeout: float | None = None):
        self.raw_reads += 1
        raise AssertionError("shared storage must not read the root relay queue")

    async def send_message(self, **_kwargs: object) -> dict[str, object]:
        return {"delivered": True}


def test_storage_endpoint_can_live_behind_an_existing_single_reader() -> None:
    async def scenario() -> None:
        client = _RelayClient()
        source = _MessageSource()
        endpoint = RelayStorageEndpoint(client, message_source=source)
        await endpoint.start()
        message = SimpleNamespace(payload={"kind": "not-storage"})
        await source.messages.put(message)
        assert await endpoint.receive_other(timeout=1.0) is message
        assert client.raw_reads == 0
        await endpoint.close()

    asyncio.run(scenario())


class _CreatorClient(_RelayClient):
    def __init__(self) -> None:
        super().__init__()
        self.credentials = SimpleNamespace(identity=SimpleNamespace(node_id=self.node_id))
        self.state = SimpleNamespace(
            joined_sessions=lambda: (SimpleNamespace(session_id="session-a"),)
        )
        self.connected_event = asyncio.Event()
        self.connected_event.set()
        self.announced: list[object] = []
        self.disconnected = False

    async def announce_capability(self, announcement: object) -> object:
        self.announced.append(announcement)
        return announcement

    async def disconnect(self, *, error_code: str | None = None) -> None:
        self.disconnected = True
        self.connected_event.clear()


class _Channel:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.handler = None
        self.closed = False

    def set_recorder_ingest_handler(self, handler: object) -> None:
        self.handler = handler

    async def close(self) -> None:
        self.closed = True


class _Failover:
    def __init__(self, **kwargs: object) -> None:
        self.channel = kwargs["channel"]

    async def start(self) -> None:
        return None

    async def scan_once(self) -> tuple[object, ...]:
        return ()

    async def close(self) -> None:
        await self.channel.close()


def _settings(tmp_path) -> runtime.StorageAuthoritySettings:
    return runtime.StorageAuthoritySettings(
        relay_control_database=str(tmp_path / "control.sqlite3"),
        storage_control_database=str(tmp_path / "control.sqlite3"),
        publication_database=str(tmp_path / "publication.sqlite3"),
        failover_database=str(tmp_path / "failover.sqlite3"),
        acknowledgements_database=str(tmp_path / "ack.sqlite3"),
        state_dir=str(tmp_path / "state"),
        relay="ws://127.0.0.1:8765",
        display_name="Leader authority",
        session_id="session-a",
        scan_interval=0.01,
    )


def _stub_authority(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "SharedRecorderAwareStorageControlRelayChannel", _Channel)
    monkeypatch.setattr(runtime, "StorageFailoverCoordinator", _Failover)
    for name in (
        "PhaseDControlPlane",
        "SessionCoordinator",
        "PhaseDLogicalStorageClient",
        "DurableAcknowledgementStore",
        "FederationLogicalStorageAuthority",
        "LiveFailoverStore",
        "StorageControlPublicationStore",
    ):
        monkeypatch.setattr(
            runtime,
            name,
            lambda *_args, **_kwargs: SimpleNamespace(
                handle_request=lambda *_a, **_k: None
            ),
        )
    monkeypatch.setattr(runtime, "_acknowledgements_database", lambda _s: "ack.sqlite3")
    monkeypatch.setattr(runtime, "_catalog_cursor_secret", lambda _s: b"cursor-secret")
    monkeypatch.setattr(
        runtime, "_recorder_ingest_authorizer", lambda _c: (lambda *_a, **_k: None)
    )
    monkeypatch.setattr(
        runtime,
        "_federation_storage_read_authorizer",
        lambda _c: (lambda *_a, **_k: None),
    )
    monkeypatch.setattr(
        runtime,
        "_recorder_storage_capability",
        lambda _control, _client, session_id: SimpleNamespace(
            session_id=session_id,
            properties={"group_ids": ["fcp-local-storage"]},
        ),
    )


def test_borrowed_creator_connection_is_never_reconnected_or_disconnected(
    tmp_path, monkeypatch
) -> None:
    client = _CreatorClient()
    source = _MessageSource()
    exposed: list[object] = []
    _stub_authority(monkeypatch)

    async def forbidden_connect(_settings: object):
        raise AssertionError("full workbench must not create a second creator client")

    monkeypatch.setattr(runtime, "_connect_creator", forbidden_connect)

    async def scenario() -> None:
        stop = asyncio.Event()

        def announced(_announcement: object) -> None:
            stop.set()

        await runtime.run_trusted_storage_authority(
            _settings(tmp_path),
            stop=stop,
            on_announced=announced,
            client=client,  # type: ignore[arg-type]
            message_source=source,
            on_message_source=exposed.append,
        )

    asyncio.run(scenario())

    assert client.announced
    assert client.disconnected is False
    assert len(exposed) == 1


def test_product_monitor_borrows_current_relay_route_without_retargeting(
    tmp_path,
) -> None:
    app = Flask(__name__)
    app.config.update(
        FEDERATION_STORAGE_AUTHORITY_ENABLED=True,
        # This intentionally differs from the already-connected product route.
        # Physical Tailscale startup used exactly this split: the storage setting
        # named Compose DNS while the saved-membership runtime used the host's
        # tailnet address. Retargeting the shared runtime here caused the two
        # supervisors to replace the same node connection every few seconds.
        FEDERATION_STORAGE_AUTHORITY_RELAY_URL="ws://relay:8765",
        FEDERATION_STORAGE_AUTHORITY_CONTROL_DATABASE=str(tmp_path / "control.sqlite3"),
        FEDERATION_STORAGE_AUTHORITY_PUBLICATION_DATABASE=str(tmp_path / "publication.sqlite3"),
        FEDERATION_STORAGE_AUTHORITY_FAILOVER_DATABASE=str(tmp_path / "failover.sqlite3"),
        FEDERATION_STORAGE_AUTHORITY_ACKNOWLEDGEMENTS_DATABASE=str(tmp_path / "ack.sqlite3"),
        FEDERATION_STORAGE_AUTHORITY_STATE_DIR=str(tmp_path / "state"),
        FEDERATION_STORAGE_AUTHORITY_DISPLAY_NAME="Leader",
        FEDERATION_STORAGE_AUTHORITY_SCAN_INTERVAL_SECONDS=0.1,
        FEDERATION_STORAGE_AUTHORITY_LEASE_SECONDS=10.0,
    )
    binding = SimpleNamespace(
        device_id="node-creator",
        internal_session_id="session-a",
    )
    context = SimpleNamespace(binding=binding)
    client = SimpleNamespace(node_id="node-creator")
    loop = asyncio.new_event_loop()
    seen: list[object] = []

    class Runtime:
        _relay_url = "ws://100.85.20.75:8765"

        def ensure_connected(self, state):
            # Mirror the real bridge's connectivity revalidation. It is safe only
            # when storage passes the route already owned by this runtime.
            assert state.relay_url == self._relay_url
            seen.append(state)

        @staticmethod
        def _connected_client():
            return client

        @staticmethod
        def _start_loop():
            return loop

    runtime_instance = Runtime()
    onboarding = SimpleNamespace(
        relay_runtime=runtime_instance,
        authorized_context=lambda: context,
    )
    source = object()

    def transport_context(shared_runtime, state):
        shared_runtime.ensure_connected(state)
        return source, loop

    app.extensions["federated_ai_product_bridge"] = SimpleNamespace(
        _transport_context=transport_context
    )
    monitor = install.FederationStorageAuthorityMonitor(app, onboarding)

    try:
        with app.app_context():
            shared = monitor._shared_relay_context(monitor.build_settings())
        assert shared is not None
        assert shared.client is client
        assert shared.message_source is source
        assert len(seen) == 1
        assert seen[0].relay_url == runtime_instance._relay_url
    finally:
        loop.close()

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from catalog.federation.errors import AuthenticationError
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime
from catalog.node.client import RelayRemoteError


def test_existing_creator_membership_mints_no_new_material() -> None:
    created: list[str] = []

    class Store:
        @staticmethod
        def get_node(node_id):
            assert node_id == "creator"
            return {"revoked_at": None}

        @staticmethod
        def require_membership(*, session_id, node_id):
            assert session_id == "session-a"
            assert node_id == "creator"

    coordinator = SimpleNamespace(
        store=Store(),
        create_enrollment_token=lambda **_kwargs: created.append("enroll"),
        create_invitation=lambda **_kwargs: created.append("invite"),
    )

    assert runtime._local_pairing_material(
        coordinator,
        node_id="creator",
        session_id="session-a",
    ) == (None, None)
    assert created == []


def test_missing_local_relay_state_uses_bounded_one_use_material() -> None:
    seen: list[tuple[str, dict[str, object]]] = []

    class Store:
        @staticmethod
        def get_node(_node_id):
            return None

        @staticmethod
        def require_membership(*, session_id, node_id):
            raise runtime.AuthorizationError(
                "not-session-member",
                "not joined",
                "session_id",
            )

    def enrollment(**kwargs):
        seen.append(("enroll", kwargs))
        return {"token": "fcp_enroll_private"}

    def invitation(**kwargs):
        seen.append(("invite", kwargs))
        return {"token": "fcp_invite_private"}

    coordinator = SimpleNamespace(
        store=Store(),
        create_enrollment_token=enrollment,
        create_invitation=invitation,
    )

    material = runtime._local_pairing_material(
        coordinator,
        node_id="creator",
        session_id="session-a",
    )

    assert material == ("fcp_enroll_private", "fcp_invite_private")
    assert seen[0] == (
        "enroll",
        {"ttl_seconds": 300, "max_uses": 1},
    )
    assert seen[1][0] == "invite"
    assert seen[1][1]["ttl_seconds"] == 300
    assert seen[1][1]["max_uses"] == 1
    assert seen[1][1]["session_id"] == "session-a"
    assert seen[1][1]["actor_node_id"] == "creator"


def test_revoked_creator_cannot_self_bootstrap() -> None:
    coordinator = SimpleNamespace(
        store=SimpleNamespace(
            get_node=lambda _node_id: {"revoked_at": "2026-08-18T00:00:00Z"}
        )
    )

    with pytest.raises(AuthenticationError) as caught:
        runtime._local_pairing_material(
            coordinator,
            node_id="creator",
            session_id="session-a",
        )

    assert caught.value.code == "revoked-node"


class _CreatorClient:
    """A connected creator client that never yields a relay message."""

    def __init__(self, node_id: str = "node-creator") -> None:
        self.node_id = node_id
        self.credentials = SimpleNamespace(identity=SimpleNamespace(node_id=node_id))
        self.state = SimpleNamespace(joined_sessions=lambda: ())
        self.connected_event = asyncio.Event()
        self.connected_event.set()
        self.announced: list[object] = []
        self.disconnected = False
        self.announce_error: Exception | None = None

    async def announce_capability(self, announcement: object) -> dict[str, object]:
        if self.announce_error is not None:
            raise self.announce_error
        self.announced.append(announcement)
        return {"announcement": announcement}

    async def receive_message(self, *, timeout: float | None = None):
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send_message(self, **_kwargs: object) -> dict[str, object]:
        return {"delivered": True}

    async def disconnect(self, *, error_code: str | None = None) -> None:
        self.disconnected = True
        self.connected_event.clear()


class _Channel:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self.handler: object | None = None
        self.closed = False

    def set_recorder_ingest_handler(self, handler: object) -> None:
        self.handler = handler

    async def close(self) -> None:
        self.closed = True


class _Failover:
    def __init__(self, **kwargs: object) -> None:
        self.channel = kwargs["channel"]
        self.scans = 0

    async def start(self) -> None:
        return None

    async def scan_once(self) -> tuple[object, ...]:
        self.scans += 1
        return ()

    async def close(self) -> None:
        await self.channel.close()


def _settings(tmp_path) -> runtime.StorageAuthoritySettings:
    return runtime.StorageAuthoritySettings(
        relay_control_database=str(tmp_path / "control.sqlite3"),
        storage_control_database=str(tmp_path / "storage.sqlite3"),
        publication_database=str(tmp_path / "publication.sqlite3"),
        failover_database=str(tmp_path / "failover.sqlite3"),
        state_dir=str(tmp_path / "state"),
        relay="ws://127.0.0.1:8765",
        display_name="Leader authority",
        session_id="session-a",
        scan_interval=0.01,
    )


def _compose(monkeypatch, client: _CreatorClient) -> list[object]:
    """Stub the authority composition and keep the real relay endpoint."""

    endpoints: list[object] = []
    real_endpoint = runtime.RelayStorageEndpoint

    class _Endpoint(real_endpoint):  # type: ignore[misc, valid-type]
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)
            endpoints.append(self)

    async def connect_creator(_settings: object) -> _CreatorClient:
        return client

    monkeypatch.setattr(runtime, "_connect_creator", connect_creator)
    monkeypatch.setattr(runtime, "RelayStorageEndpoint", _Endpoint)
    monkeypatch.setattr(runtime, "RecorderAwareStorageControlRelayChannel", _Channel)
    monkeypatch.setattr(runtime, "StorageFailoverCoordinator", _Failover)
    # These lifecycle tests deliberately replace the storage/control composition
    # with tiny fakes. The real built-in provider composition has its own
    # end-to-end regression coverage and must not inspect those fake snapshots.
    monkeypatch.setattr(
        runtime,
        "_ensure_builtin_local_storage_service",
        lambda **_kwargs: None,
    )
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
    return endpoints


def test_a_stopped_authority_closes_its_relay_endpoint(tmp_path, monkeypatch) -> None:
    """A leaked reader task is destroyed pending when its loop is closed.

    The failover coordinator owns only its own control channel, so closing it
    never stopped the shared ``fcp-storage-relay-*`` reader the authority
    started. Every restart left one behind on a loop about to be closed.
    """

    client = _CreatorClient()

    async def scenario() -> list[object]:
        endpoints = _compose(monkeypatch, client)
        stop = asyncio.Event()

        def announced(_announcement: object) -> None:
            stop.set()

        await runtime.run_trusted_storage_authority(
            _settings(tmp_path), stop=stop, on_announced=announced
        )
        assert not [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
        ]
        return endpoints

    endpoints = asyncio.run(scenario())

    assert len(endpoints) == 1
    assert endpoints[0]._closed is True
    assert endpoints[0]._reader_task is None
    assert client.announced
    assert client.disconnected is True


def test_a_relay_teardown_during_announce_is_raised_for_the_supervisor(
    tmp_path, monkeypatch
) -> None:
    """The announce loop must surface a retryable error, not a cancellation."""

    client = _CreatorClient()
    client.announce_error = RelayRemoteError(
        "connection-closed", "relay connection closed"
    )

    async def scenario() -> list[object]:
        endpoints = _compose(monkeypatch, client)
        with pytest.raises(RelayRemoteError) as rejected:
            await runtime.run_trusted_storage_authority(
                _settings(tmp_path), stop=asyncio.Event()
            )
        assert isinstance(rejected.value, Exception)
        assert rejected.value.code == "connection-closed"
        assert not [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task()
        ]
        return endpoints

    endpoints = asyncio.run(scenario())

    assert endpoints[0]._closed is True
    assert client.disconnected is True

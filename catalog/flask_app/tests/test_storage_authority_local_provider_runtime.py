from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from catalog.federation.commit_tracking import DurableAcknowledgementStore
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import STORAGE_PROTOCOL, STORAGE_PROTOCOL_VERSION
from catalog.flask_app.services import trusted_storage_authority_runtime as runtime


class _LoopbackRelayClient:
    """One authenticated node talking to its own provider over relay.message."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._messages: asyncio.Queue[object] = asyncio.Queue()

    async def send_message(
        self,
        *,
        session_id: str,
        target_node_id: str,
        payload: dict[str, object],
        request_id: str | None = None,
    ) -> dict[str, object]:
        del request_id
        assert target_node_id == self.node_id
        await self._messages.put(
            SimpleNamespace(
                actor_node_id=self.node_id,
                session_id=session_id,
                payload=payload,
            )
        )
        return {"delivered": True}

    async def receive_message(self, *, timeout: float | None = None):
        if timeout is None:
            return await self._messages.get()
        return await asyncio.wait_for(self._messages.get(), timeout=timeout)


def _settings(tmp_path: Path) -> runtime.StorageAuthoritySettings:
    storage_root = tmp_path / "authority-storage"
    return runtime.StorageAuthoritySettings(
        relay_control_database=str(tmp_path / "control.sqlite3"),
        storage_control_database=str(tmp_path / "control.sqlite3"),
        publication_database=str(storage_root / "publication.sqlite3"),
        failover_database=str(storage_root / "failover.sqlite3"),
        acknowledgements_database=str(
            storage_root / "recorder_ingest_acknowledgements.sqlite3"
        ),
        state_dir=str(tmp_path / "state"),
        relay="ws://127.0.0.1:8765",
        display_name="Creator",
        session_id="session-a",
    )


def _register_primary(
    control: PhaseDControlPlane,
    *,
    session_id: str,
    provider_id: str,
    node_id: str,
    now: datetime,
) -> None:
    control.create_group(session_id, node_id, "fcp-local-storage")
    control.register_provider(
        session_id,
        node_id,
        StorageProviderRegistration(
            session_id=session_id,
            provider_id=provider_id,
            node_id=node_id,
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    control.change_assignment(
        session_id,
        node_id,
        "fcp-local-storage",
        provider_id,
    )
    control.grant_leader(
        session_id,
        node_id,
        "fcp-local-storage",
        provider_id,
        "grant-1",
        1,
        1,
        lease_expires_at=now + timedelta(minutes=10),
        occurred_at=now,
    )


def test_creator_builtin_storage_provider_is_hosted_and_accepts_logical_ingest(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        now = datetime.now(timezone.utc)
        settings = _settings(tmp_path)
        client = _LoopbackRelayClient("node-creator")
        control = PhaseDControlPlane(settings.storage_control_database)
        provider_id = runtime._builtin_local_provider_id(client.node_id)
        _register_primary(
            control,
            session_id=settings.session_id,
            provider_id=provider_id,
            node_id=client.node_id,
            now=now,
        )

        endpoint = RelayStorageEndpoint(client, request_timeout=2.0)
        try:
            service = runtime._ensure_builtin_local_storage_service(
                endpoint=endpoint,
                control=control,
                client=client,  # type: ignore[arg-type]
                settings=settings,
            )
            assert service is not None
            assert endpoint.services[provider_id] is service

            await endpoint.start()
            logical = PhaseDLogicalStorageClient(
                session_id=settings.session_id,
                actor_node_id=client.node_id,
                control_plane=control,
                transport=endpoint,
                acknowledgements=DurableAcknowledgementStore(
                    Path(settings.acknowledgements_database)
                ),
            )
            content = {"schema": "test.batch.v1", "value": 42}
            outcome = await logical.ingest_batch(
                group_id="fcp-local-storage",
                dataset_id="test-dataset",
                batch_id="batch-1",
                idempotency_key="test-dataset:batch-1",
                content=content,
                created_at=now,
            )

            assert outcome.committed is True
            assert service.provider.read(
                session_id=settings.session_id,
                group_id="fcp-local-storage",
                batch_id="batch-1",
            ) == content
        finally:
            await endpoint.close()

    asyncio.run(scenario())


def test_authority_does_not_auto_host_an_arbitrary_registered_provider(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    client = _LoopbackRelayClient("node-creator")
    control = PhaseDControlPlane(settings.storage_control_database)
    now = datetime.now(timezone.utc)
    _register_primary(
        control,
        session_id=settings.session_id,
        provider_id="explicit-external-provider",
        node_id=client.node_id,
        now=now,
    )
    endpoint = RelayStorageEndpoint(client, request_timeout=2.0)

    service = runtime._ensure_builtin_local_storage_service(
        endpoint=endpoint,
        control=control,
        client=client,  # type: ignore[arg-type]
        settings=settings,
    )

    assert service is None
    assert endpoint.services == {}

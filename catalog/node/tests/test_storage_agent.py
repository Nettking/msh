from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.acknowledgement import AcknowledgementMode
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.phase_d_client import PhaseDLogicalStorageClient
from catalog.federation.relay_storage import RelayStorageEndpoint
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)
from catalog.node.client import RelayNodeClient
from catalog.node.storage_agent import (
    STORAGE_NODE_CONFIG_SCHEMA,
    StorageNodeAgent,
    StorageNodeAgentError,
    StorageNodeConfig,
)
from catalog.relay.service import RelayServer

NOW = datetime(2026, 7, 31, 0, 0, tzinfo=timezone.utc)
TIMEOUT = 5.0


def _config_value(*, relay_url: str, session_id: str) -> dict[str, object]:
    return {
        "schema": STORAGE_NODE_CONFIG_SCHEMA,
        "state_directory": "state",
        "relay_url": relay_url,
        "display_name": "Storage A",
        "provider_id": "provider-a",
        "session_id": session_id,
        "control_database": "state/control.sqlite3",
        "storage_directory": "state/storage",
        "outbox_database": "state/outbox.sqlite3",
        "acknowledgements_database": "state/acks.sqlite3",
        "allow_insecure_local": True,
        "heartbeat_interval": 300,
        "request_timeout": TIMEOUT,
    }


def _write_config(
    root: Path,
    *,
    relay_url: str = "ws://127.0.0.1:1",
    session_id: str = "session-1",
) -> Path:
    path = root / "storage-node.json"
    path.write_text(
        json.dumps(_config_value(relay_url=relay_url, session_id=session_id)),
        encoding="utf-8",
    )
    return path


def test_storage_node_config_is_local_strict_and_credential_free(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)
    config = StorageNodeConfig.load(path)

    assert config.state_directory == (tmp_path / "state").resolve()
    assert config.storage_directory == (tmp_path / "state" / "storage").resolve()
    assert config.capability_id == "storage-provider-a"
    summary = config.public_summary()
    assert summary["provider_id"] == "provider-a"
    assert str(tmp_path) not in json.dumps(summary)

    value = _config_value(
        relay_url="ws://127.0.0.1:1",
        session_id="session-1",
    )
    value["enrollment_token"] = "must-not-be-stored"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(StorageNodeAgentError) as forbidden:
        StorageNodeConfig.load(path)
    assert forbidden.value.code == "credential-in-storage-node-config"


def test_storage_node_status_is_public_and_identity_is_persistent(
    tmp_path: Path,
) -> None:
    config = StorageNodeConfig.load(_write_config(tmp_path))
    first = StorageNodeAgent(config, clock=lambda: NOW)
    first_status = first.status()

    assert first.inbound_listener_ports == ()
    assert first_status["provider"]["registered"] is False
    assert first_status["provider"]["health"]["status"] == "ready"
    assert str(tmp_path) not in json.dumps(first_status)

    second = StorageNodeAgent(config, clock=lambda: NOW)
    assert second.node_id == first.node_id


async def _enroll(relay: RelayServer, client: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await client.connect(enrollment_token=str(token))


def test_storage_node_serves_primary_ingest_over_relay_and_restarts(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        owner_endpoint: RelayStorageEndpoint | None = None
        agent: StorageNodeAgent | None = None
        config: StorageNodeConfig | None = None
        try:
            relay = RelayServer(
                SessionCoordinator(
                    tmp_path / "relay" / "control.sqlite3",
                    clock=lambda: NOW,
                ),
                host="127.0.0.1",
                port=0,
                auth_timeout_seconds=TIMEOUT,
                send_timeout_seconds=TIMEOUT,
                heartbeat_timeout_seconds=300,
                sweep_interval_seconds=300,
            )
            await relay.start()
            owner = RelayNodeClient(
                state_directory=tmp_path / "owner",
                relay_url=relay.url,
                display_name="Owner",
                allow_insecure_local=True,
                heartbeat_interval=300,
                request_timeout=TIMEOUT,
                clock=lambda: NOW,
            )
            await _enroll(relay, owner)
            session = await owner.create_session("Physical federation F1")
            session_id = str(session["session_id"])

            config = StorageNodeConfig.load(
                _write_config(
                    tmp_path,
                    relay_url=relay.url,
                    session_id=session_id,
                )
            )
            agent = StorageNodeAgent(config, clock=lambda: NOW)
            agent.control.create_group(session_id, "coordinator", "storage-main")
            agent.control.register_provider(
                session_id,
                "coordinator",
                StorageProviderRegistration(
                    session_id=session_id,
                    provider_id=config.provider_id,
                    node_id=agent.node_id,
                    protocol=STORAGE_PROTOCOL,
                    protocol_version=STORAGE_PROTOCOL_VERSION,
                    authorized=True,
                    status="ready",
                ),
            )
            agent.control.change_assignment(
                session_id,
                "coordinator",
                "storage-main",
                config.provider_id,
            )
            agent.control.set_acknowledgement_policy(
                session_id,
                "storage-main",
                AcknowledgementMode.PRIMARY,
            )
            agent.control.grant_leader(
                session_id,
                "coordinator",
                "storage-main",
                config.provider_id,
                "grant-1",
                1,
                1,
                lease_expires_at=NOW + timedelta(minutes=15),
                occurred_at=NOW,
            )

            enrollment = relay.coordinator.create_enrollment_token(
                ttl_seconds=60,
                max_uses=1,
            )["token"]
            invitation = await owner.create_invitation(
                session_id,
                ttl_seconds=60,
                max_uses=1,
            )
            await agent.bootstrap(
                enrollment_token=str(enrollment),
                session_invitation=str(invitation["token"]),
            )

            owner_endpoint = RelayStorageEndpoint(
                owner,
                request_timeout=TIMEOUT,
            )
            await owner_endpoint.start()
            logical = PhaseDLogicalStorageClient(
                session_id=session_id,
                actor_node_id=owner.node_id,
                control_plane=agent.control,
                transport=owner_endpoint,
            )
            content = {"source": "physical-test", "sequence": 1}
            outcome = await logical.ingest_batch(
                group_id="storage-main",
                dataset_id="telemetry",
                batch_id="batch-1",
                idempotency_key="physical-test:1",
                content=content,
                created_at=NOW,
            )

            assert outcome.committed
            assert agent.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == content
            status = await owner.coordinator_status()
            assert any(
                capability.get("capability_id") == config.capability_id
                and capability.get("node_id") == agent.node_id
                for capability in status["capabilities"]
            )
            assert agent.status()["provider"]["groups"] == [
                {"group_id": "storage-main", "role": "primary"}
            ]

            stable_node_id = agent.node_id
            await agent.close()
            agent = None
            restarted = StorageNodeAgent(config, clock=lambda: NOW)
            assert restarted.node_id == stable_node_id
            assert restarted.provider.read(
                session_id=session_id,
                group_id="storage-main",
                batch_id="batch-1",
            ) == content
        finally:
            if agent is not None:
                await agent.close()
            if owner_endpoint is not None:
                await owner_endpoint.close()
            if owner is not None:
                await owner.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_storage_relay import (
    RECORDER_LOGICAL_STORAGE_KIND,
    RECORDER_RELAY_MAX_PAYLOAD_BYTES,
    RecorderAwareStorageControlRelayChannel,
    RecorderLogicalStorageAuthority,
    RelayRecorderStorageClient,
)


class _AuthorityRelayClient:
    node_id = "node-authority"

    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return {"delivered": True}


class _LogicalClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ingest_batch(self, **kwargs):
        self.calls.append(kwargs)
        return PhaseDIngestOutcome(
            committed=False,
            retryable=True,
            error_code="storage-pending",
            message="replica acknowledgement pending",
        )


def _request_payload() -> dict[str, object]:
    return {
        "kind": RECORDER_LOGICAL_STORAGE_KIND,
        "message": "request",
        "correlation_id": "corr-1",
        "group_id": "telemetry",
        "dataset_id": "mtconnect:node-recorder:Mazak",
        "batch_id": "Mazak:1:1:2:abc",
        "idempotency_key": "session:dataset:batch",
        "dataset_schema_name": "fcp.mtconnect.observations",
        "dataset_schema_version": 1,
        "created_at": "2026-08-10T18:00:00+00:00",
        "content": {"schema": "fcp.mtconnect.observations.v1", "observations": []},
    }


def test_authority_routes_authenticated_recorder_batch_through_logical_client() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalClient()
        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
        )

        await authority.handle_request(
            "node-recorder",
            "session-1",
            _request_payload(),
        )

        assert len(logical.calls) == 1
        call = logical.calls[0]
        assert call["group_id"] == "telemetry"
        assert call["dataset_id"] == "mtconnect:node-recorder:Mazak"
        assert call["content"] == _request_payload()["content"]
        assert len(relay.sent) == 1
        assert relay.sent[0]["target_node_id"] == "node-recorder"
        response = relay.sent[0]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "accepted"
        assert response["outcome"]["retryable"] is True

    asyncio.run(run())


def test_authority_rejects_cross_session_request_without_calling_storage() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalClient()
        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
        )

        await authority.handle_request(
            "node-recorder",
            "session-other",
            _request_payload(),
        )

        assert logical.calls == []
        response = relay.sent[0]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "rejected"
        assert response["error"]["code"] == "recorder-storage-session-mismatch"

    asyncio.run(run())


class _RecorderRelayClient:
    node_id = "node-recorder"

    def __init__(self) -> None:
        self.inbound: asyncio.Queue[object] = asyncio.Queue()
        self.sent: list[dict[str, object]] = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        payload = kwargs["payload"]
        assert isinstance(payload, dict)
        await self.inbound.put(
            SimpleNamespace(
                actor_node_id="node-authority",
                session_id="session-1",
                payload={
                    "kind": RECORDER_LOGICAL_STORAGE_KIND,
                    "message": "response",
                    "correlation_id": payload["correlation_id"],
                    "status": "accepted",
                    "outcome": {
                        "committed": False,
                        "result": None,
                        "retryable": True,
                        "error_code": "storage-pending",
                        "message": "pending",
                    },
                },
            )
        )
        return {"delivered": True}

    async def receive_message(self):
        return await self.inbound.get()


def test_recorder_client_correlates_authority_response_and_never_sends_credentials() -> None:
    async def run() -> None:
        relay = _RecorderRelayClient()
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )
        try:
            outcome = await client.ingest_batch(
                group_id="telemetry",
                dataset_id="mtconnect:node-recorder:Mazak",
                batch_id="Mazak:1:1:1:abc",
                idempotency_key="session:dataset:batch",
                content={"sequence": 1},
                created_at=datetime.now(timezone.utc),
                dataset_schema_name="fcp.mtconnect.observations",
                dataset_schema_version=1,
            )
        finally:
            await client.close()

        assert outcome.retryable is True
        assert outcome.error_code == "storage-pending"
        payload = relay.sent[0]["payload"]
        assert isinstance(payload, dict)
        lowered = str(payload).lower()
        assert "token" not in lowered
        assert "password" not in lowered
        assert "private_key" not in lowered
        assert relay.sent[0]["target_node_id"] == "node-authority"

    asyncio.run(run())


def test_recorder_client_refuses_payload_larger_than_relay_boundary() -> None:
    async def run() -> None:
        relay = _RecorderRelayClient()
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )
        try:
            with pytest.raises(FederationValidationError) as error:
                await client.ingest_batch(
                    group_id="telemetry",
                    dataset_id="dataset",
                    batch_id="batch",
                    idempotency_key="idempotency",
                    content={"value": "x" * RECORDER_RELAY_MAX_PAYLOAD_BYTES},
                    created_at=datetime.now(timezone.utc),
                )
            assert error.value.code == "recorder-storage-payload-too-large"
        finally:
            await client.close()

    asyncio.run(run())


class _OtherEndpoint:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[object] = asyncio.Queue()

    async def receive_other(self):
        return await self.queue.get()


def test_storage_control_channel_dispatches_recorder_ingress_without_stealing_control() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        endpoint = _OtherEndpoint()
        channel = RecorderAwareStorageControlRelayChannel(
            relay,
            endpoint,
            timeout=1,
        )
        called = asyncio.Event()
        values: list[tuple[str, str, dict[str, object]]] = []

        async def handler(actor: str, session: str, payload: dict[str, object]):
            values.append((actor, session, payload))
            called.set()

        channel.set_recorder_ingest_handler(handler)
        await channel.start()
        try:
            await endpoint.queue.put(
                SimpleNamespace(
                    actor_node_id="node-recorder",
                    session_id="session-1",
                    payload=_request_payload(),
                )
            )
            await asyncio.wait_for(called.wait(), timeout=1)
        finally:
            await channel.close()

        assert values[0][0] == "node-recorder"
        assert values[0][1] == "session-1"
        assert values[0][2]["kind"] == RECORDER_LOGICAL_STORAGE_KIND

    asyncio.run(run())

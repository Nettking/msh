from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CommitState
from catalog.federation.phase_d_client import PhaseDIngestOutcome
from catalog.federation.recorder_storage_relay import (
    RECORDER_CATALOG_MAX_PAGE_SIZE,
    RECORDER_LOGICAL_STORAGE_KIND,
    RECORDER_RELAY_MAX_PAYLOAD_BYTES,
    RecorderAwareStorageControlRelayChannel,
    RecorderLogicalStorageAuthority,
    RelayRecorderStorageClient,
)
from catalog.federation.storage_catalog import (
    CommittedBatchDeltaPage,
    CommittedBatchPage,
    CommittedBatchReference,
)
from catalog.federation.storage_protocol import BatchIngestRequest, StorageOperation

RAW_SHA256 = "sha256:" + "a" * 64
RECORDER_DATASET_ID = "mtconnect:node-recorder:Mazak"
RECORDER_BATCH_ID = f"Mazak:1:1:2:{RAW_SHA256.removeprefix('sha256:')}"


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
        "dataset_id": RECORDER_DATASET_ID,
        "batch_id": RECORDER_BATCH_ID,
        "idempotency_key": (f"session-1:{RECORDER_DATASET_ID}:{RECORDER_BATCH_ID}"),
        "dataset_schema_name": "fcp.mtconnect.observations",
        "dataset_schema_version": 1,
        "created_at": "2026-08-10T18:00:00+00:00",
        "content": {
            "schema": "fcp.mtconnect.observations.v1",
            "source_name": "Mazak",
            "machine_id": "machine-1",
            "agent_instance_id": 1,
            "first_sequence": 1,
            "last_sequence": 2,
            "raw_batch_first_sequence": 1,
            "raw_batch_last_sequence": 2,
            "raw_sha256": RAW_SHA256,
            "probe_sha256": None,
            "observation_count": 2,
            "observations": [
                {"sequence": 1, "data_item_id": "execution", "value": "ACTIVE"},
                {"sequence": 2, "data_item_id": "execution", "value": "READY"},
            ],
        },
    }


def _committed_content() -> dict[str, object]:
    content = _request_payload()["content"]
    assert isinstance(content, dict)
    return content


def _committed_reference() -> CommittedBatchReference:
    content = _committed_content()
    size_bytes = len(
        json.dumps(
            content,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    )
    return CommittedBatchReference(
        session_id="session-1",
        group_id="telemetry",
        dataset_id=RECORDER_DATASET_ID,
        batch_id=RECORDER_BATCH_ID,
        idempotency_key=(f"session-1:{RECORDER_DATASET_ID}:{RECORDER_BATCH_ID}"),
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        size_bytes=size_bytes,
        schema_name="fcp.mtconnect.observations",
        schema_version=1,
        source_id="Mazak",
        first_sequence=1,
        last_sequence=2,
        commit_state=CommitState.COMMITTED,
        acknowledged_provider_ids=("provider-primary",),
        committed_at=datetime(2026, 8, 10, 18, 0, tzinfo=timezone.utc),
        manifest_revision=1,
        manifest_hash="sha256:" + "b" * 64,
    )


def _committed_page() -> CommittedBatchPage:
    reference = _committed_reference()
    return CommittedBatchPage(
        session_id=reference.session_id,
        group_id=reference.group_id,
        dataset_id=reference.dataset_id,
        manifest_revision=reference.manifest_revision,
        manifest_hash=reference.manifest_hash,
        batches=(reference,),
        next_cursor=None,
    )


def _committed_delta_page() -> CommittedBatchDeltaPage:
    page = _committed_page()
    return CommittedBatchDeltaPage(
        session_id=page.session_id,
        group_id=page.group_id,
        dataset_id=page.dataset_id,
        baseline_manifest_revision=0,
        baseline_manifest_hash="sha256:" + "a" * 64,
        manifest_revision=page.manifest_revision,
        manifest_hash=page.manifest_hash,
        batches=page.batches,
        next_cursor=None,
    )


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
        assert call["dataset_id"] == RECORDER_DATASET_ID
        assert call["content"] == _request_payload()["content"]
        assert len(relay.sent) == 1
        assert relay.sent[0]["target_node_id"] == "node-recorder"
        response = relay.sent[0]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "accepted"
        assert response["outcome"]["retryable"] is True

    asyncio.run(run())


def test_authority_rejects_dataset_owned_by_another_recorder() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalClient()
        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
        )

        await authority.handle_request(
            "node-impostor",
            "session-1",
            _request_payload(),
        )

        assert logical.calls == []
        response = relay.sent[0]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "rejected"
        assert response["error"]["code"] == "recorder-dataset-actor-mismatch"

    asyncio.run(run())


def test_authority_honors_ingest_authorizer_rejection() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalClient()
        authorization_calls: list[tuple[str, str]] = []

        def reject_unregistered_recorder(actor: str, session: str) -> None:
            authorization_calls.append((actor, session))
            raise FederationValidationError(
                "recorder-capability-required",
                "actor_node_id",
                "recorder has no active logical-storage capability",
            )

        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
            authorize_ingest=reject_unregistered_recorder,
        )

        await authority.handle_request(
            "node-recorder",
            "session-1",
            _request_payload(),
        )

        assert authorization_calls == [("node-recorder", "session-1")]
        assert logical.calls == []
        response = relay.sent[0]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "rejected"
        assert response["error"]["code"] == "recorder-capability-required"

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


class _LogicalCatalogClient:
    def __init__(self) -> None:
        self.page = _committed_page()
        self.content = _committed_content()
        self.list_calls: list[dict[str, object]] = []
        self.delta_calls: list[dict[str, object]] = []
        self.read_calls: list[CommittedBatchReference] = []

    def list_committed_batches(self, **kwargs: object) -> CommittedBatchPage:
        self.list_calls.append(kwargs)
        return self.page

    def list_committed_batch_delta(self, **kwargs: object) -> CommittedBatchDeltaPage:
        self.delta_calls.append(kwargs)
        return _committed_delta_page()

    async def read_committed_batch(
        self,
        *,
        reference: CommittedBatchReference,
    ) -> object:
        self.read_calls.append(reference)
        return self.content


def test_authority_authorizes_and_roundtrips_catalog_list_and_read() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalCatalogClient()
        authorization_calls: list[tuple[str, str, str, str | None]] = []

        def authorize_read(
            actor: str,
            session: str,
            group: str,
            dataset: str | None,
        ) -> None:
            authorization_calls.append((actor, session, group, dataset))

        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
            authorize_read=authorize_read,
        )
        list_payload = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "operation": StorageOperation.BATCH_LIST.value,
            "correlation_id": "corr-list",
            "group_id": "telemetry",
            "dataset_id": RECORDER_DATASET_ID,
            "limit": 20,
            "cursor": None,
        }

        await authority.handle_request(
            "node-reader",
            "session-1",
            list_payload,
        )

        list_response = relay.sent[-1]["payload"]
        assert isinstance(list_response, dict)
        assert list_response["status"] == "accepted"
        assert list_response["operation"] == StorageOperation.BATCH_LIST.value
        assert CommittedBatchPage.from_dict(list_response["page"]) == logical.page
        assert logical.list_calls == [
            {
                "group_id": "telemetry",
                "dataset_id": RECORDER_DATASET_ID,
                "limit": 20,
                "cursor": None,
            }
        ]

        reference = logical.page.batches[0]
        await authority.handle_request(
            "node-reader",
            "session-1",
            {
                "kind": RECORDER_LOGICAL_STORAGE_KIND,
                "message": "request",
                "operation": StorageOperation.BATCH_READ.value,
                "correlation_id": "corr-read",
                "reference": reference.to_dict(),
            },
        )

        read_response = relay.sent[-1]["payload"]
        assert isinstance(read_response, dict)
        assert read_response["status"] == "accepted"
        assert read_response["operation"] == StorageOperation.BATCH_READ.value
        assert read_response["content"] == logical.content
        assert logical.read_calls == [reference]
        assert authorization_calls == [
            ("node-reader", "session-1", "telemetry", RECORDER_DATASET_ID),
            ("node-reader", "session-1", "telemetry", RECORDER_DATASET_ID),
        ]

        await authority.handle_request(
            "node-reader",
            "session-1",
            {
                "kind": RECORDER_LOGICAL_STORAGE_KIND,
                "message": "request",
                "operation": StorageOperation.BATCH_READ.value,
                "correlation_id": "corr-read-without-reference",
                "group_id": "telemetry",
                "batch_id": RECORDER_BATCH_ID,
            },
        )

        rejected = relay.sent[-1]["payload"]
        assert isinstance(rejected, dict)
        assert rejected["status"] == "rejected"
        assert logical.read_calls == [reference]
        assert len(authorization_calls) == 2

    asyncio.run(run())


def test_authority_roundtrips_delta_and_rejects_incomplete_baseline() -> None:
    async def run() -> None:
        relay = _AuthorityRelayClient()
        logical = _LogicalCatalogClient()
        authority = RecorderLogicalStorageAuthority(
            client=relay,
            logical_client=logical,
            session_id="session-1",
        )
        payload = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "request",
            "operation": StorageOperation.BATCH_LIST.value,
            "listing_mode": "delta",
            "correlation_id": "corr-delta",
            "group_id": "telemetry",
            "dataset_id": RECORDER_DATASET_ID,
            "baseline_manifest_revision": 0,
            "baseline_manifest_hash": "sha256:" + "a" * 64,
            "limit": 20,
            "cursor": None,
        }

        await authority.handle_request("node-reader", "session-1", payload)

        response = relay.sent[-1]["payload"]
        assert isinstance(response, dict)
        assert response["status"] == "accepted"
        assert (
            CommittedBatchDeltaPage.from_dict(response["page"])
            == _committed_delta_page()
        )
        assert logical.delta_calls == [
            {
                "group_id": "telemetry",
                "baseline_manifest_revision": 0,
                "baseline_manifest_hash": "sha256:" + "a" * 64,
                "dataset_id": RECORDER_DATASET_ID,
                "limit": 20,
                "cursor": None,
            }
        ]

        incomplete = dict(payload)
        incomplete["correlation_id"] = "corr-incomplete"
        del incomplete["baseline_manifest_hash"]
        await authority.handle_request("node-reader", "session-1", incomplete)
        rejected = relay.sent[-1]["payload"]
        assert isinstance(rejected, dict)
        assert rejected["status"] == "rejected"
        assert rejected["error"]["code"] == "missing-field"
        assert len(logical.delta_calls) == 1

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


class _RootCorrelatedRelayClient:
    node_id = "node-reader"

    def __init__(
        self,
        *,
        page: CommittedBatchPage | CommittedBatchDeltaPage | None = None,
        content: object | None = None,
    ) -> None:
        self.page = page or _committed_page()
        self.content = _committed_content() if content is None else content
        self.calls: list[dict[str, Any]] = []

    async def request_message_response(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        payload = kwargs["payload"]
        assert isinstance(payload, dict)
        operation = payload["operation"]
        response: dict[str, object] = {
            "kind": RECORDER_LOGICAL_STORAGE_KIND,
            "message": "response",
            "operation": operation,
            "correlation_id": kwargs["correlation_id"],
            "status": "accepted",
        }
        if operation == StorageOperation.BATCH_LIST.value:
            response["page"] = self.page.to_dict()
        elif operation == StorageOperation.BATCH_READ.value:
            response["content"] = self.content
        else:
            raise AssertionError(f"unexpected operation: {operation}")
        return SimpleNamespace(
            actor_node_id=kwargs["target_node_id"],
            session_id=kwargs["session_id"],
            payload=response,
        )


def test_recorder_catalog_uses_root_correlated_request_for_list_and_read() -> None:
    async def run() -> None:
        relay = _RootCorrelatedRelayClient()
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )
        page = await client.list_committed_batches(
            group_id="telemetry",
            dataset_id=RECORDER_DATASET_ID,
            limit=20,
        )
        content = await client.read_committed_batch(page.batches[0])

        assert page == _committed_page()
        assert content == _committed_content()
        assert [call["payload"]["operation"] for call in relay.calls] == [
            StorageOperation.BATCH_LIST.value,
            StorageOperation.BATCH_READ.value,
        ]
        for call in relay.calls:
            assert call["session_id"] == "session-1"
            assert call["target_node_id"] == "node-authority"
            assert call["timeout"] == 1
            assert call["correlation_id"] == call["payload"]["correlation_id"]

    asyncio.run(run())


def test_recorder_catalog_uses_exact_baseline_for_delta_request() -> None:
    async def run() -> None:
        delta = _committed_delta_page()
        relay = _RootCorrelatedRelayClient(page=delta)
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )

        page = await client.list_committed_batch_delta(
            group_id="telemetry",
            dataset_id=RECORDER_DATASET_ID,
            baseline_manifest_revision=delta.baseline_manifest_revision,
            baseline_manifest_hash=delta.baseline_manifest_hash,
        )

        assert page == delta
        payload = relay.calls[0]["payload"]
        assert payload["listing_mode"] == "delta"
        assert payload["baseline_manifest_revision"] == 0
        assert payload["baseline_manifest_hash"] == delta.baseline_manifest_hash

    asyncio.run(run())


def test_recorder_read_requires_reference_and_rejects_tampered_content() -> None:
    async def run() -> None:
        relay = _RootCorrelatedRelayClient(content={"tampered": True})
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )

        with pytest.raises(FederationValidationError) as missing_reference:
            await client.read_committed_batch(RECORDER_BATCH_ID)  # type: ignore[arg-type]
        assert missing_reference.value.code == "invalid-batch-reference"
        assert relay.calls == []

        with pytest.raises(FederationValidationError) as tampered:
            await client.read_committed_batch(_committed_reference())
        assert tampered.value.code == "content-hash-mismatch"
        assert len(relay.calls) == 1
        assert (
            relay.calls[0]["payload"]["operation"] == StorageOperation.BATCH_READ.value
        )

    asyncio.run(run())


def test_recorder_catalog_rejects_oversized_page_before_relay_send() -> None:
    async def run() -> None:
        relay = _RootCorrelatedRelayClient()
        client = RelayRecorderStorageClient(
            relay,
            session_id="session-1",
            authority_node_id="node-authority",
            request_timeout=1,
        )

        with pytest.raises(FederationValidationError) as oversized:
            await client.list_committed_batches(
                group_id="telemetry",
                limit=RECORDER_CATALOG_MAX_PAGE_SIZE + 1,
            )
        assert oversized.value.code == "recorder-catalog-page-too-large"
        assert relay.calls == []

    asyncio.run(run())


def test_recorder_client_correlates_authority_response_and_never_sends_credentials() -> (
    None
):
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


def test_storage_control_channel_dispatches_recorder_ingress_without_stealing_control() -> (
    None
):
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

"""Failure-only diagnostics distinguish a late provider result from a relay rejection."""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from types import SimpleNamespace

import pytest

from catalog.federation import phase_d_service, relay_storage
from catalog.federation.tests.test_phase_e1_service_manifest import (
    PRIMARY_ID,
    _envelope,
    _request,
)
from catalog.node.client import RelayRemoteError


async def scenario(outcome):
    release = asyncio.Event()
    entered = asyncio.Event()
    service = phase_d_service.PhaseDStorageService.__new__(phase_d_service.PhaseDStorageService)
    provider_result = []

    async def ingest(envelope, request):
        del envelope, request
        entered.set()
        await release.wait()
        if outcome == "sqlite-error":
            raise sqlite3.OperationalError("PRIVATE-PROVIDER-MESSAGE")
        if outcome == "timeout-error":
            raise TimeoutError("PRIVATE-PROVIDER-MESSAGE")
        return (SimpleNamespace(to_dict=lambda: {"fixture": "PRIVATE-PROVIDER-BODY"}),
            SimpleNamespace(committed=True, required_replica_acks=0, acknowledged_replica_ids=()),
            SimpleNamespace(revision=1, manifest_hash="sha256:" + "a" * 64))

    service._ingest_primary = ingest
    original_dispatch = service.dispatch

    async def dispatch(envelope):
        result = await original_dispatch(envelope)
        provider_result.append(result)
        return result

    client = SimpleNamespace(node_id="isolated-provider")
    endpoint = relay_storage.RelayStorageEndpoint(client, {PRIMARY_ID: SimpleNamespace(dispatch=dispatch)},
        request_timeout=0.025)
    handler = None
    request = _envelope(_request(), request_id="late-provider-request-original")

    async def send_message(**values):
        nonlocal handler
        if values["payload"]["message"] == "request":
            handler = asyncio.create_task(endpoint._handle_request(
                SimpleNamespace(actor_node_id=request.actor_node_id, session_id=request.session_id),
                values["payload"]))
            return {"delivered": True}
        raise RelayRemoteError("target-disconnected", "PRIVATE-RELAY-MESSAGE", "target_node_id")

    client.send_message = send_message
    incoming = asyncio.Queue()

    async def receive_message(*, timeout=None):
        del timeout
        return await incoming.get()

    client.receive_message = receive_message
    try:
        with pytest.raises(TimeoutError):
            await endpoint.request(target_node_id=client.node_id, envelope=request)
        assert entered.is_set() and not provider_result
        release.set()
        assert handler is not None
        with pytest.raises(RelayRemoteError):
            await handler
        assert endpoint._pending == {}
        return provider_result[0]
    finally:
        release.set()
        if handler is not None and not handler.done():
            handler.cancel()
            await asyncio.gather(handler, return_exceptions=True)
        await endpoint.close()


@pytest.mark.parametrize("outcome", ["success", "sqlite-error", "timeout-error"])
def test_failed_send_retains_actual_late_success_or_caught_provider_error(outcome, caplog):
    caplog.set_level(logging.ERROR, logger=relay_storage.__name__)
    response = asyncio.run(asyncio.wait_for(scenario(outcome), timeout=2))
    assert response.ok is (outcome == "success")
    if outcome != "success":
        assert response.error.code.value == "internal-error" and response.error.retryable is True
    values = [json.loads("{" + record.getMessage().split(" {", 1)[1])
        for record in caplog.records if " {" in record.getMessage()]
    assert [value["storage_stage"] for value in values] == ["response_wait", "response_delivery_failed"]
    before, after = values
    assert before["storage_request_id"] == after["storage_request_id"]
    assert after["storage_exception_type"] == "RelayRemoteError"
    assert after["storage_response_ok"] is (outcome == "success")
    assert after["storage_response_error_code"] == (None if outcome == "success" else "internal-error")
    assert after["storage_relay_error_code"] == "target-disconnected"
    assert after["storage_relay_error_code_sha256"] == relay_storage._diagnostic_text("target-disconnected")
    assert after["storage_relay_request_id"] == relay_storage._diagnostic_text("relay-response-late-provider-request-original")
    assert "PRIVATE-" not in "\n".join(record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("code", ["PRIVATE-TOKEN", "x" * 2049, ["PRIVATE-TOKEN"], True, None])
def test_unknown_or_malformed_error_codes_remain_bounded_redacted_and_nonthrowing(code):
    response = SimpleNamespace(ok="PRIVATE-BOOLEAN", error=SimpleNamespace(code="PRIVATE-ERROR"),
        request_id="PRIVATE-REQUEST")
    error = RuntimeError("PRIVATE-MESSAGE")
    error.code = code
    fields = relay_storage._response_failure_fields(response, error)
    assert fields["storage_response_ok"] is None and fields["storage_response_error_code"] is None
    assert fields["storage_relay_error_code"] is None
    assert fields["storage_relay_request_id"] == relay_storage._diagnostic_text("relay-response-PRIVATE-REQUEST")
    assert "PRIVATE-" not in json.dumps(fields) and len(json.dumps(fields)) < 512


@pytest.mark.parametrize("code", ["target-disconnected", "connection-closed", "relay-rejected",
    "connection-replaced", "request-id-conflict"])
def test_known_local_teardown_and_server_rejection_codes_are_not_conflated(code):
    response = SimpleNamespace(ok=True, error=None, request_id="isolated-response")
    fields = relay_storage._response_failure_fields(
        response, RelayRemoteError(code, "PRIVATE-MESSAGE", "PRIVATE-FIELD"))
    assert fields["storage_relay_error_code"] == code
    assert fields["storage_relay_error_code_sha256"] == relay_storage._diagnostic_text(code)
    assert "PRIVATE-" not in json.dumps(fields)


def test_successful_response_send_keeps_info_severity_without_new_failure_records(caplog):
    async def send():
        request = _envelope(_request(), request_id="successful-response-original")

        async def dispatch(envelope):
            return phase_d_service.StorageResponseEnvelope(
                request_id=envelope.request_id, protocol=phase_d_service.STORAGE_PROTOCOL,
                protocol_version=phase_d_service.STORAGE_PROTOCOL_VERSION, ok=True,
                result={"PRIVATE-BODY": True})

        async def send_message(**values):
            assert values["request_id"] == "relay-response-successful-response-original"
            return {"delivered": True}

        endpoint = relay_storage.RelayStorageEndpoint(SimpleNamespace(send_message=send_message),
            {PRIMARY_ID: SimpleNamespace(dispatch=dispatch)})
        await endpoint._handle_request(
            SimpleNamespace(actor_node_id=request.actor_node_id, session_id=request.session_id),
            {"provider_id": PRIMARY_ID, "frame": json.dumps(request.to_dict())})
        await endpoint.close()

    caplog.set_level(logging.INFO, logger=relay_storage.__name__)
    asyncio.run(asyncio.wait_for(send(), timeout=2))
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)
    result = next(record for record in caplog.records if record.storage_stage == "response_delivery")
    assert result.levelno == logging.INFO and result.storage_delivery_confirmed is True
    assert "PRIVATE-" not in "\n".join(record.getMessage() for record in caplog.records)

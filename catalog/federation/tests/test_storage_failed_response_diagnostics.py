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
    assert "storage_delivery_elapsed_seconds" in before
    assert before["storage_delivery_elapsed_seconds"] >= 0
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


def test_rejected_late_response_logs_redacted_correlation_and_reason(caplog):
    endpoint = relay_storage.RelayStorageEndpoint(
        SimpleNamespace(node_id="PRIVATE-LOCAL-NODE")
    )
    caplog.set_level(logging.WARNING, logger=relay_storage.__name__)

    endpoint._accept_response(
        SimpleNamespace(
            request_id="relay-response-PRIVATE-LOGICAL-REQUEST",
            actor_node_id="PRIVATE-AUTHORITY-NODE",
            session_id="PRIVATE-SESSION",
        ),
        {
            "provider_id": "PRIVATE-PROVIDER",
            "frame": json.dumps({"request_id": "PRIVATE-LOGICAL-REQUEST"}),
        },
    )

    record = next(
        record for record in caplog.records if record.getMessage().startswith(
            "storage response rejected "
        )
    )
    fields = json.loads(record.getMessage().split(" ", 3)[3])
    assert fields["storage_stage"] == "response_rejected"
    assert fields["storage_rejection_reason"] == "request_not_pending"
    assert fields["storage_late_response_valid"] is False
    assert fields["storage_late_response_request_id_matches"] is True
    assert fields["storage_late_response_ok"] is None
    assert fields["storage_request_id"] == relay_storage._diagnostic_text(
        "PRIVATE-LOGICAL-REQUEST"
    )
    assert fields["storage_relay_request_id"] == relay_storage._diagnostic_text(
        "relay-response-PRIVATE-LOGICAL-REQUEST"
    )
    assert fields["storage_session_id"] == relay_storage._diagnostic_text(
        "PRIVATE-SESSION"
    )
    assert fields["storage_actor_node_id"] == relay_storage._diagnostic_text(
        "PRIVATE-AUTHORITY-NODE"
    )
    assert fields["storage_provider_id"] == relay_storage._diagnostic_text(
        "PRIVATE-PROVIDER"
    )
    assert "PRIVATE-" not in record.getMessage()


@pytest.mark.parametrize(
    ("ok", "error_code"),
    [(True, None), (False, "internal-error")],
)
def test_late_response_logs_valid_outcome_without_accepting_it(ok, error_code, caplog):
    endpoint = relay_storage.RelayStorageEndpoint(
        SimpleNamespace(node_id="PRIVATE-LOCAL-NODE")
    )
    caplog.set_level(logging.WARNING, logger=relay_storage.__name__)
    error = (
        relay_storage.StorageError(
            code=relay_storage.StorageErrorCode(error_code),
            message="PRIVATE-RESPONSE-MESSAGE",
            retryable=True,
        )
        if error_code is not None
        else None
    )
    response = relay_storage.StorageResponseEnvelope(
        request_id="PRIVATE-LOGICAL-REQUEST",
        protocol=relay_storage.STORAGE_PROTOCOL,
        protocol_version=relay_storage.STORAGE_PROTOCOL_VERSION,
        ok=ok,
        result={"manifest_revision": 712, "manifest_hash": "sha256:" + "b" * 64}
        if ok
        else None,
        error=error,
    )

    endpoint._accept_response(
        SimpleNamespace(
            request_id="relay-response-PRIVATE-LOGICAL-REQUEST",
            actor_node_id="PRIVATE-AUTHORITY-NODE",
            session_id="PRIVATE-SESSION",
        ),
        {
            "provider_id": "PRIVATE-PROVIDER",
            "frame": json.dumps(response.to_dict()),
        },
    )

    assert endpoint._pending == {}
    record = next(
        record for record in caplog.records if record.getMessage().startswith(
            "storage response rejected "
        )
    )
    fields = json.loads(record.getMessage().split(" ", 3)[3])
    assert fields["storage_rejection_reason"] == "request_not_pending"
    assert fields["storage_late_response_valid"] is True
    assert fields["storage_late_response_request_id_matches"] is True
    assert fields["storage_late_response_ok"] is ok
    assert fields["storage_late_response_error_code"] == error_code
    assert "PRIVATE-" not in record.getMessage()


def test_late_response_with_mismatched_relay_identity_is_not_classified_valid(caplog):
    endpoint = relay_storage.RelayStorageEndpoint(
        SimpleNamespace(node_id="PRIVATE-LOCAL-NODE")
    )
    caplog.set_level(logging.WARNING, logger=relay_storage.__name__)
    response = relay_storage.StorageResponseEnvelope(
        request_id="PRIVATE-ACTUAL-REQUEST",
        protocol=relay_storage.STORAGE_PROTOCOL,
        protocol_version=relay_storage.STORAGE_PROTOCOL_VERSION,
        ok=True,
        result={"manifest_revision": 712, "manifest_hash": "sha256:" + "b" * 64},
    )
    endpoint._accept_response(
        SimpleNamespace(
            request_id="relay-response-PRIVATE-EXPECTED-REQUEST",
            actor_node_id="PRIVATE-AUTHORITY-NODE",
            session_id="PRIVATE-SESSION",
        ),
        {"provider_id": "PRIVATE-PROVIDER", "frame": json.dumps(response.to_dict())},
    )
    record = next(
        record for record in caplog.records if record.getMessage().startswith(
            "storage response rejected "
        )
    )
    fields = json.loads(record.getMessage().split(" ", 3)[3])
    assert fields["storage_rejection_reason"] == "request_not_pending"
    assert fields["storage_late_response_valid"] is False
    assert fields["storage_late_response_request_id_matches"] is False
    assert fields["storage_late_response_ok"] is None
    assert "PRIVATE-" not in record.getMessage()


@pytest.mark.parametrize(
    ("mutate", "expected_valid", "expected_request_match"),
    [
        (lambda response: response["result"].update(value=float("nan")), False, True),
        (lambda response: response.update(request_id="  "), False, False),
        (lambda response: response.update(protocol_version="1.1"), True, True),
        (lambda response: response.update(protocol_version="².0"), False, True),
    ],
    ids=[
        "non-json-result", "blank-request-id", "supported-minor-version",
        "malformed-unicode-major",
    ],
)
def test_late_response_diagnostics_follow_canonical_envelope_validation(
    mutate, expected_valid, expected_request_match,
):
    response = relay_storage.StorageResponseEnvelope(
        request_id="logical-request",
        protocol=relay_storage.STORAGE_PROTOCOL,
        protocol_version=relay_storage.STORAGE_PROTOCOL_VERSION,
        ok=True,
        result={"manifest_revision": 712},
    ).to_dict()
    mutate(response)

    fields = relay_storage._late_response_diagnostic_fields(
        response, expected_request_id="logical-request"
    )

    assert fields["storage_late_response_valid"] is expected_valid
    assert fields["storage_late_response_request_id_matches"] is expected_request_match


def test_late_response_diagnostics_reject_whitespace_only_error_message():
    response = {
        "schema": relay_storage.StorageResponseEnvelope.SCHEMA,
        "request_id": "logical-request",
        "protocol": relay_storage.STORAGE_PROTOCOL,
        "protocol_version": relay_storage.STORAGE_PROTOCOL_VERSION,
        "ok": False,
        "result": None,
        "error": {
            "code": relay_storage.StorageErrorCode.INTERNAL_ERROR.value,
            "message": "   ",
            "retryable": True,
        },
    }

    fields = relay_storage._late_response_diagnostic_fields(
        response, expected_request_id="logical-request"
    )

    assert fields["storage_late_response_valid"] is False
    assert fields["storage_late_response_ok"] is None


def test_late_response_diagnostics_reject_wrong_expected_request_id():
    response = relay_storage.StorageResponseEnvelope(
        request_id="actual-request",
        protocol=relay_storage.STORAGE_PROTOCOL,
        protocol_version=relay_storage.STORAGE_PROTOCOL_VERSION,
        ok=True,
        result={"manifest_revision": 712},
    ).to_dict()

    fields = relay_storage._late_response_diagnostic_fields(
        response, expected_request_id="different-request"
    )

    assert fields["storage_late_response_valid"] is False
    assert fields["storage_late_response_request_id_matches"] is False
    assert fields["storage_late_response_ok"] is None


def test_malformed_late_response_version_does_not_escape_response_reader(caplog):
    endpoint = relay_storage.RelayStorageEndpoint(
        SimpleNamespace(node_id="PRIVATE-LOCAL-NODE")
    )
    caplog.set_level(logging.WARNING, logger=relay_storage.__name__)
    response = {
        "schema": relay_storage.StorageResponseEnvelope.SCHEMA,
        "request_id": "logical-request",
        "protocol": relay_storage.STORAGE_PROTOCOL,
        "protocol_version": "².0",
        "ok": True,
        "result": {"manifest_revision": 712},
    }

    endpoint._accept_response(
        SimpleNamespace(
            request_id="relay-response-logical-request",
            actor_node_id="PRIVATE-AUTHORITY-NODE",
            session_id="PRIVATE-SESSION",
        ),
        {"provider_id": "PRIVATE-PROVIDER", "frame": json.dumps(response)},
    )

    record = next(
        record for record in caplog.records if record.getMessage().startswith(
            "storage response rejected "
        )
    )
    fields = json.loads(record.getMessage().split(" ", 3)[3])
    assert fields["storage_rejection_reason"] == "request_not_pending"
    assert fields["storage_late_response_valid"] is False
    assert fields["storage_late_response_request_id_matches"] is True
    assert fields["storage_late_response_ok"] is None


def test_request_relay_delivery_timeout_is_logged_with_elapsed_time(caplog):
    async def scenario():
        incoming = asyncio.Queue()

        async def send_message(**_values):
            await asyncio.sleep(0.01)
            raise TimeoutError("PRIVATE-RELAY-TIMEOUT")

        async def receive_message(*, timeout=None):
            del timeout
            return await incoming.get()

        client = SimpleNamespace(
            node_id="PRIVATE-LOCAL-NODE",
            send_message=send_message,
            receive_message=receive_message,
        )
        endpoint = relay_storage.RelayStorageEndpoint(client, request_timeout=0.05)
        request = _envelope(_request(), request_id="delivery-timeout-original")
        try:
            with pytest.raises(TimeoutError):
                await endpoint.request(target_node_id=client.node_id, envelope=request)
        finally:
            await endpoint.close()

    caplog.set_level(logging.ERROR, logger=relay_storage.__name__)
    asyncio.run(asyncio.wait_for(scenario(), timeout=2))
    record = next(
        record for record in caplog.records
        if record.getMessage().startswith("storage request relay delivery failed ")
    )
    fields = json.loads(record.getMessage().split(" ", 5)[5])
    assert fields["storage_stage"] == "request_delivery_failed"
    assert fields["storage_exception_type"] == "TimeoutError"
    assert fields["storage_delivery_elapsed_seconds"] >= 0
    assert fields["storage_request_id"] == relay_storage._diagnostic_text(
        "delivery-timeout-original"
    )
    assert "PRIVATE-" not in record.getMessage()


def test_slow_provider_dispatch_and_response_delivery_are_separately_attributed(
    monkeypatch, caplog,
):
    clock = [10.0]
    monkeypatch.setattr(relay_storage.time, "monotonic", lambda: clock[0])
    request = _envelope(_request(), request_id="slow-provider-request")

    async def dispatch(envelope):
        clock[0] += 11.0
        return relay_storage.StorageResponseEnvelope(
            request_id=envelope.request_id,
            protocol=relay_storage.STORAGE_PROTOCOL,
            protocol_version=relay_storage.STORAGE_PROTOCOL_VERSION,
            ok=True,
            result={"manifest_revision": 712, "manifest_hash": "sha256:" + "b" * 64},
        )

    async def send_message(**values):
        assert values["payload"]["message"] == "response"
        clock[0] += 12.0
        return {"delivered": True}

    endpoint = relay_storage.RelayStorageEndpoint(
        SimpleNamespace(node_id="PRIVATE-PROVIDER", send_message=send_message),
        {PRIMARY_ID: SimpleNamespace(dispatch=dispatch)},
        request_timeout=15.0,
    )
    caplog.set_level(logging.WARNING, logger=relay_storage.__name__)
    asyncio.run(
        endpoint._handle_request(
            SimpleNamespace(
                actor_node_id=request.actor_node_id,
                session_id=request.session_id,
            ),
            {"provider_id": PRIMARY_ID, "frame": json.dumps(request.to_dict())},
        )
    )

    records = [record for record in caplog.records if record.levelno == logging.WARNING]
    dispatch_record = next(
        record for record in records if record.getMessage().startswith(
            "storage provider dispatch was slow "
        )
    )
    delivery_record = next(
        record for record in records if record.getMessage().startswith(
            "storage response relay delivery was slow "
        )
    )
    dispatch = json.loads(dispatch_record.getMessage().split(" ", 5)[5])
    delivery = json.loads(delivery_record.getMessage().split(" ", 6)[6])
    assert dispatch["storage_stage"] == "provider_dispatch_complete"
    assert dispatch["storage_elapsed_seconds"] == 11.0
    assert delivery["storage_stage"] == "response_delivery"
    assert delivery["storage_elapsed_seconds"] == 12.0
    assert dispatch["storage_request_id"] == delivery["storage_request_id"]
    assert "PRIVATE-" not in dispatch_record.getMessage() + delivery_record.getMessage()


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

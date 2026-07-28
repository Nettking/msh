from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.errors import FederationValidationError, ProtocolCompatibilityError
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    BatchIngestState,
    StorageError,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    WriteAuthority,
    classify_idempotent_ingest,
    validate_protocol_version,
)

NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


def authority() -> WriteAuthority:
    return WriteAuthority(
        session_id="session-1",
        group_id="storage-main",
        actor_node_id="node-a",
        grant_id="grant-7",
        term=7,
        fencing_token=103,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


def ingest_request() -> BatchIngestRequest:
    content = {"records": [{"sequence": 1, "value": 42.5}]}
    return BatchIngestRequest(
        authority=authority(),
        dataset_id="mazak-telemetry",
        batch_id="batch-1",
        idempotency_key="recorder-a:1-1",
        content_hash=BatchIngestRequest.calculate_content_hash(content),
        content=content,
        created_at=NOW,
    )


def test_protocol_accepts_additive_minor_versions() -> None:
    assert validate_protocol_version("1.0") == "1.0"
    assert validate_protocol_version("1.9") == "1.9"


def test_protocol_rejects_unknown_major_version() -> None:
    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        validate_protocol_version("2.0")

    assert exc_info.value.code == "unsupported-protocol-major"


def test_request_envelope_round_trip_ignores_additive_fields() -> None:
    request = StorageRequestEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.BATCH_INGEST,
        session_id="session-1",
        actor_node_id="node-a",
        authorization_context={"scopes": ["storage:telemetry:write"]},
        payload=ingest_request().to_dict(),
    )
    encoded = request.to_dict()
    encoded["future_optional_field"] = {"accepted": True}

    assert StorageRequestEnvelope.from_dict(encoded) == request


def test_request_envelope_requires_authorization_context() -> None:
    encoded = StorageRequestEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        operation=StorageOperation.HEALTH,
        session_id="session-1",
        actor_node_id="node-a",
        authorization_context={},
        payload={},
    ).to_dict()
    del encoded["authorization_context"]

    with pytest.raises(FederationValidationError) as exc_info:
        StorageRequestEnvelope.from_dict(encoded)

    assert exc_info.value.code == "missing-field"
    assert exc_info.value.field == "authorization_context"


def test_write_authority_round_trip_contains_all_fencing_fields() -> None:
    original = authority()

    assert WriteAuthority.from_dict(original.to_dict()) == original
    assert set(original.to_dict()) == {
        "session_id",
        "group_id",
        "actor_node_id",
        "grant_id",
        "term",
        "fencing_token",
        "lease_expires_at",
    }


def test_batch_ingest_round_trip_and_content_hash_validation() -> None:
    request = ingest_request()

    request.validate_content_hash()
    assert BatchIngestRequest.from_dict(request.to_dict()) == request


def test_batch_ingest_rejects_modified_content() -> None:
    encoded = ingest_request().to_dict()
    encoded["content"] = {"records": [{"sequence": 1, "value": 99.0}]}
    modified = BatchIngestRequest.from_dict(encoded)

    with pytest.raises(FederationValidationError) as exc_info:
        modified.validate_content_hash()

    assert exc_info.value.code == StorageErrorCode.CONTENT_HASH_MISMATCH.value


def test_identical_idempotent_retry_is_already_stored() -> None:
    request = ingest_request()

    result = classify_idempotent_ingest(
        existing_batch_id=request.batch_id,
        existing_content_hash=request.content_hash,
        requested_batch_id=request.batch_id,
        requested_content_hash=request.content_hash,
    )

    assert result is BatchIngestState.ALREADY_STORED


@pytest.mark.parametrize(
    ("requested_batch_id", "requested_content_hash"),
    [("batch-2", None), (None, "sha256:" + "f" * 64)],
)
def test_idempotency_key_reuse_with_different_identity_is_rejected(
    requested_batch_id: str | None, requested_content_hash: str | None
) -> None:
    request = ingest_request()

    with pytest.raises(FederationValidationError) as exc_info:
        classify_idempotent_ingest(
            existing_batch_id=request.batch_id,
            existing_content_hash=request.content_hash,
            requested_batch_id=requested_batch_id or request.batch_id,
            requested_content_hash=requested_content_hash or request.content_hash,
        )

    assert exc_info.value.code == StorageErrorCode.IDEMPOTENCY_CONFLICT.value


def test_success_response_round_trip() -> None:
    ingest_result = BatchIngestResult(
        batch_id="batch-1",
        idempotency_key="recorder-a:1-1",
        content_hash="sha256:" + "a" * 64,
        state=BatchIngestState.STORED,
    )
    response = StorageResponseEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        ok=True,
        result=ingest_result.to_dict(),
    )

    assert StorageResponseEnvelope.from_dict(response.to_dict()) == response


def test_error_response_round_trip() -> None:
    response = StorageResponseEnvelope(
        request_id="request-1",
        protocol=STORAGE_PROTOCOL,
        protocol_version=STORAGE_PROTOCOL_VERSION,
        ok=False,
        error=StorageError(
            code=StorageErrorCode.STALE_FENCING_TOKEN,
            message="write authority is obsolete",
            field="authority.fencing_token",
            retryable=False,
        ),
    )

    assert StorageResponseEnvelope.from_dict(response.to_dict()) == response


def test_response_cannot_contain_result_and_error() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        StorageResponseEnvelope(
            request_id="request-1",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            ok=False,
            result={"unexpected": True},
            error=StorageError(
                code=StorageErrorCode.INTERNAL_ERROR,
                message="failed",
            ),
        )

    assert exc_info.value.code == "invalid-response"

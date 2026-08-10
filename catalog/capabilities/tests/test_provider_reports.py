from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.provider_reports import (
    PROVIDER_REPORT_PROTOCOL_VERSION,
    ProviderResourceReport,
    ProviderSelectionPolicy,
    ProviderStatus,
    provider_protocol_satisfies,
)
from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)


def _report(
    capability_id: str = "provider-a",
    **overrides: object,
) -> ProviderResourceReport:
    values: dict[str, object] = {
        "capability_id": capability_id,
        "node_id": f"node-{capability_id}",
        "session_id": "session-001",
        "capability_type": "language-model",
        "protocol": "ollama-chat",
        "protocol_version": "1.0",
        "status": ProviderStatus.READY,
        "report_revision": 1,
        "max_concurrent_jobs": 4,
        "active_jobs": 1,
        "queue_depth": 0,
        "utilization_millis": 250,
        "attributes": {
            "model": "qwen3-vl:4b",
            "modalities": ["vision", "text", "audio"],
            "features": {"streaming": True, "json": True},
        },
        "reported_at": NOW - timedelta(seconds=10),
        "expires_at": NOW + timedelta(seconds=50),
    }
    values.update(overrides)
    return ProviderResourceReport(**values)  # type: ignore[arg-type]


def test_f72_001_report_round_trip_is_canonical_json() -> None:
    report = _report()
    encoded = report.to_json()

    assert ProviderResourceReport.from_json(encoded) == report
    assert json.loads(encoded) == report.to_dict()
    assert encoded == ProviderResourceReport.from_dict(report.to_dict()).to_json()


def test_f72_002_same_major_additive_fields_are_ignored() -> None:
    value = _report().to_dict()
    value["future_optional_field"] = {"safe": True}

    assert ProviderResourceReport.from_dict(value) == _report()


def test_f72_003_unknown_report_protocol_major_is_rejected() -> None:
    value = _report().to_dict()
    value["report_protocol_version"] = "2.0"

    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        ProviderResourceReport.from_dict(value)

    assert exc_info.value.code == "unsupported-protocol-major"
    assert exc_info.value.field == "report_protocol_version"


def test_f72_004_unknown_report_schema_major_is_rejected() -> None:
    value = _report().to_dict()
    value["schema"] = "fcp.capability-provider-report.v2"

    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        ProviderResourceReport.from_dict(value)

    assert exc_info.value.code == "unsupported-schema-major"


def test_f72_005_active_jobs_cannot_exceed_capacity() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _report(max_concurrent_jobs=2, active_jobs=3)

    assert exc_info.value.code == "active-jobs-exceed-capacity"


def test_f72_006_report_ttl_is_finite_and_bounded() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _report(expires_at=NOW + timedelta(hours=2))

    assert exc_info.value.code == "report-ttl-too-large"


def test_f72_007_report_attributes_reject_credentials() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _report(attributes={"api_token": "secret-value"})

    assert exc_info.value.code == "secret-provider-attribute"


def test_f72_008_report_attributes_reject_locations() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _report(attributes={"endpoint": "http://10.0.0.3:11434"})

    assert exc_info.value.code == "nonpublic-provider-attribute"


def test_f72_009_protocol_minor_must_meet_requested_version() -> None:
    assert provider_protocol_satisfies("1.1", "1.2")
    assert not provider_protocol_satisfies("1.2", "1.1")
    assert not provider_protocol_satisfies("1.0", "2.0")


def test_f72_010_stale_and_future_freshness_is_explicit() -> None:
    expired = _report(
        reported_at=NOW - timedelta(minutes=2),
        expires_at=NOW,
    )
    future = _report(
        reported_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=30),
    )

    assert not expired.is_fresh_at(NOW)
    assert not future.is_fresh_at(NOW)
    assert _report().is_fresh_at(NOW)


def test_f72_011_malformed_and_oversized_report_json_is_rejected() -> None:
    with pytest.raises(FederationValidationError) as malformed:
        ProviderResourceReport.from_json("{")
    assert malformed.value.code == "malformed-message"

    with pytest.raises(FederationValidationError) as oversized:
        ProviderResourceReport.from_json(_report().to_json(), max_bytes=10)
    assert oversized.value.code == "message-too-large"


def test_f72_012_default_report_protocol_version_is_supported() -> None:
    assert _report().report_protocol_version == PROVIDER_REPORT_PROTOCOL_VERSION


def test_f72_031_selection_policy_round_trip_ignores_additive_fields() -> None:
    policy = ProviderSelectionPolicy(
        minimum_available_slots=2,
        max_queue_depth=4,
        max_utilization_millis=700,
        preferred_node_id="node-local",
        excluded_capability_ids=("provider-b", "provider-a"),
    )
    value = policy.to_dict()
    value["future_optional_field"] = True

    assert ProviderSelectionPolicy.from_dict(value) == policy
    assert policy.excluded_capability_ids == ("provider-a", "provider-b")


def test_f72_032_selection_policy_schema_major_is_fail_closed() -> None:
    value = ProviderSelectionPolicy().to_dict()
    value["schema"] = "fcp.capability-selection-policy.v2"

    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        ProviderSelectionPolicy.from_dict(value)

    assert exc_info.value.code == "unsupported-schema-major"

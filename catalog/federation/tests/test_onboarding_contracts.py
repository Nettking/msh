from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.onboarding_compat import (
    CONTRIBUTION_KEYS,
    LegacySetupMigrationPreview,
    federation_id_from_session_id,
    federation_id_matches_session,
    preview_legacy_setup,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
    DeviceInspectionSnapshot,
    FederationConnectionState,
    FederationDiscoveryResult,
    FederationSessionBinding,
)

NOW = datetime(2026, 8, 2, 18, tzinfo=timezone.utc)
LATER = NOW + timedelta(minutes=5)
EXPIRY = NOW + timedelta(hours=1)


def discovery(**changes: object) -> FederationDiscoveryResult:
    values = {
        "discovery_id": "discovery-1",
        "federation_label": "Workshop federation",
        "federation_fingerprint": "sha256:abc123",
        "transport_kind": "relay",
        "verification_code": "ABCD-1234",
        "prior_trust": False,
        "state": "verification-required",
        "discovered_at": NOW,
        "expires_at": EXPIRY,
        "unavailable_reason": None,
    }
    values.update(changes)
    return FederationDiscoveryResult(**values)


def benchmark_result(**changes: object) -> BenchmarkResult:
    values = {
        "run_id": "run-1",
        "device_id": "node-1",
        "benchmark_id": "ai-response-v1",
        "benchmark_version": "1.0.0",
        "target_service_id": "ollama-local",
        "state": "passed",
        "metrics": {
            "latency_ms": 1200,
            "tokens_per_second": 18.5,
        },
        "recommendation": "recommended",
        "dependency_fingerprint": "sha256:def456",
        "diagnostics": ("Warm response completed.",),
        "started_at": NOW,
        "finished_at": LATER,
        "expires_at": EXPIRY,
    }
    values.update(changes)
    return BenchmarkResult(**values)


def candidate(**changes: object) -> ContributionCandidate:
    values = {
        "candidate_id": "candidate-1",
        "device_id": "node-1",
        "capability_type": "language-model",
        "capability_protocol": "msh-ai-v1",
        "display_label": "Local language model",
        "inspection_revision": 2,
        "benchmark_run_ids": ("run-1",),
        "capacity_envelope": {
            "concurrency": 1,
            "latency_band": "interactive",
        },
        "missing_prerequisites": (),
        "policy_state": "allowed",
        "created_at": NOW,
        "expires_at": EXPIRY,
    }
    values.update(changes)
    return ContributionCandidate(**values)


def legacy_payload(mode: str, **changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema": "msh.server_setup.v3",
        "configured": True,
        "user_setup_complete": True,
        "deployment_mode": mode,
        "ai_enabled": True,
        "ai_provider_mode": "local",
        "ai_provider_name": "This computer",
        "ai_profile": "laptop-standard",
        "ai_model": "llama3.2:3b",
        "ollama_base_url": "http://ollama:11434",
        "recorder_sources": "Mazak=http://192.168.1.20:5000",
        "recorder_poll_interval": "0.2",
        "recorder_include_condition": False,
    }
    values.update(changes)
    return values


def test_contract_round_trips_are_canonical_and_accept_additive_fields() -> None:
    models = [
        FederationSessionBinding(
            federation_id="federation-1",
            internal_session_id="session-1",
            device_id="node-1",
            state="connected",
            revision=3,
            trusted=True,
            created_at=NOW,
            last_verified_at=LATER,
        ),
        discovery(),
        DeviceInspectionSnapshot(
            device_id="node-1",
            revision=4,
            os_family="windows",
            architecture="amd64",
            resource_observations={
                "cpu_threads": 16,
                "memory_band": "16-32-gib",
            },
            detected_services=("ollama",),
            registered_handlers=("python-analysis-v1",),
            detected_data_sources=("mtconnect-machine-1",),
            recommended_benchmark_ids=("ai-response-v1",),
            warnings=(),
            created_at=NOW,
            expires_at=EXPIRY,
        ),
        BenchmarkDefinition(
            benchmark_id="ai-response-v1",
            capability_type="language-model",
            capability_protocol="msh-ai-v1",
            implementation_version="1.0.0",
            max_duration_seconds=30,
            max_parallelism=1,
            prerequisites=("ollama-running",),
            metric_names=("latency_ms",),
            invalidation_inputs=("model-fingerprint",),
            privacy_classification="safe-summary",
        ),
        benchmark_result(),
        candidate(),
        ContributionIntent(
            candidate_id="candidate-1",
            device_id="node-1",
            desired_state="enabled",
            decision_revision=1,
            policy_state="allowed",
            activation_state="active",
            reason=None,
            decided_at=NOW,
        ),
    ]

    for model in models:
        payload = model.to_dict()
        payload["future_optional_field"] = "ignored"
        restored = type(model).from_dict(payload)
        assert restored == model
        assert json.loads(json.dumps(payload))["schema"] == model.SCHEMA


def test_unsupported_schema_major_fails_closed() -> None:
    payload = discovery().to_dict()
    payload["schema"] = "msh.onboarding.discovery-result.v2"

    with pytest.raises(ProtocolCompatibilityError) as error:
        FederationDiscoveryResult.from_dict(payload)

    assert error.value.code == "unsupported-schema"


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("federation_label", "msh_join_secret", "secret-metadata"),
        (
            "transport_kind",
            "http://192.168.1.9:9000",
            "nonpublic-metadata",
        ),
    ],
)
def test_discovery_rejects_secret_or_private_transport_metadata(
    field: str,
    value: str,
    code: str,
) -> None:
    with pytest.raises(FederationValidationError) as error:
        discovery(**{field: value})
    assert error.value.code == code


def test_discovery_requires_bounded_expiry_and_unavailable_reason() -> None:
    with pytest.raises(
        FederationValidationError,
        match="later than discovered_at",
    ):
        discovery(expires_at=NOW)
    with pytest.raises(
        FederationValidationError,
        match="required when discovery",
    ):
        discovery(state="unavailable")
    unavailable = discovery(
        state="unavailable",
        unavailable_reason="Relay is unavailable.",
    )
    assert unavailable.state == FederationConnectionState.UNAVAILABLE


def test_connected_binding_requires_prior_trust() -> None:
    with pytest.raises(FederationValidationError) as error:
        FederationSessionBinding(
            federation_id="federation-1",
            internal_session_id="session-1",
            device_id="node-1",
            state="connected",
            revision=1,
            trusted=False,
            created_at=NOW,
        )
    assert error.value.code == "untrusted-connected-state"


def test_inspection_and_metrics_reject_private_or_secret_content() -> None:
    with pytest.raises(FederationValidationError) as error:
        DeviceInspectionSnapshot(
            device_id="node-1",
            revision=1,
            os_family="windows",
            architecture="amd64",
            resource_observations={
                "ollama_url": "http://10.0.0.2:11434",
            },
            detected_services=(),
            registered_handlers=(),
            detected_data_sources=(),
            recommended_benchmark_ids=(),
            warnings=(),
            created_at=NOW,
            expires_at=EXPIRY,
        )
    assert error.value.code == "nonpublic-metadata"

    with pytest.raises(FederationValidationError) as error:
        benchmark_result(metrics={"api_token": "msh_enroll_secret"})
    assert error.value.code == "secret-metadata"


def test_non_passing_benchmark_cannot_recommend_activation() -> None:
    with pytest.raises(FederationValidationError) as error:
        benchmark_result(
            state="failed",
            recommendation="recommended",
        )
    assert error.value.code == "invalid-recommendation"

    failed = benchmark_result(
        state="failed",
        recommendation="not-recommended",
        diagnostics=("Timed out safely.",),
    )
    assert failed.state == BenchmarkState.FAILED
    assert failed.recommendation == BenchmarkRecommendation.NOT_RECOMMENDED


def test_candidate_and_intent_do_not_bypass_policy_or_prerequisites() -> None:
    with pytest.raises(FederationValidationError) as error:
        candidate(
            missing_prerequisites=("model-missing",),
            policy_state="allowed",
        )
    assert error.value.code == "prerequisites-missing"

    with pytest.raises(FederationValidationError) as error:
        ContributionIntent(
            candidate_id="candidate-1",
            device_id="node-1",
            desired_state="enabled",
            decision_revision=1,
            policy_state="approval-required",
            activation_state="active",
            reason=None,
            decided_at=NOW,
        )
    assert error.value.code == "invalid-active-intent"

    with pytest.raises(FederationValidationError) as error:
        ContributionIntent(
            candidate_id="candidate-1",
            device_id="node-1",
            desired_state="enabled",
            decision_revision=1,
            policy_state="blocked",
            activation_state="blocked",
            reason=None,
            decided_at=NOW,
        )
    assert error.value.code == "missing-reason"


def test_federation_id_mapping_is_stable_and_session_bound() -> None:
    first = federation_id_from_session_id("session-1")
    assert first == federation_id_from_session_id("session-1")
    assert first != federation_id_from_session_id("session-2")
    assert first.startswith("federation-")
    assert federation_id_matches_session(first, "session-1") is True
    assert federation_id_matches_session(first, "session-2") is False


@pytest.mark.parametrize(
    ("mode", "expected_enabled"),
    [
        (
            "full-server",
            {"workbench", "runtime", "recorder", "language-model"},
        ),
        (
            "web-workbench",
            {"workbench", "runtime", "language-model"},
        ),
        ("web-ui-only", {"workbench", "language-model"}),
        ("recorder-only", {"recorder"}),
        ("language-model-provider", {"language-model"}),
    ],
)
def test_every_legacy_mode_maps_without_new_compute_or_storage_authority(
    mode: str,
    expected_enabled: set[str],
) -> None:
    preview = preview_legacy_setup(legacy_payload(mode), created_at=NOW)
    enabled = {
        key
        for key, value in preview.contribution_intents.items()
        if value == ContributionDesiredState.ENABLED.value
    }

    assert enabled == expected_enabled
    assert preview.contribution_intents["compute"] == "disabled"
    assert preview.contribution_intents["storage"] == "disabled"
    assert set(preview.contribution_intents) == set(CONTRIBUTION_KEYS)


def test_ai_disabled_stays_disabled_during_migration() -> None:
    preview = preview_legacy_setup(
        legacy_payload("web-workbench", ai_enabled=False),
        created_at=NOW,
    )
    assert preview.contribution_intents["language-model"] == "disabled"


def test_recorder_intent_is_preserved_but_missing_source_is_reported() -> None:
    preview = preview_legacy_setup(
        legacy_payload("recorder-only", recorder_sources=""),
        created_at=NOW,
    )
    assert preview.contribution_intents["recorder"] == "enabled"
    assert preview.preserved_safe_fields["recorder_configured"] is False
    assert (
        "Recorder contribution needs a selected data source."
        in preview.warnings
    )


def test_preview_preserves_private_configuration_by_name_not_value() -> None:
    payload = legacy_payload("full-server")
    preview = preview_legacy_setup(payload, created_at=NOW)
    serialized = json.dumps(preview.to_dict(), sort_keys=True)

    assert set(preview.preserved_private_fields) == {
        "ollama_base_url",
        "recorder_sources",
    }
    assert "192.168.1.20" not in serialized
    assert "http://ollama:11434" not in serialized
    assert preview.preserved_safe_fields["ai_model"] == "llama3.2:3b"


def test_preview_is_read_only_and_deterministic() -> None:
    payload = legacy_payload("web-workbench")
    original = copy.deepcopy(payload)

    first = preview_legacy_setup(payload, created_at=NOW)
    second = preview_legacy_setup(payload, created_at=NOW)

    assert payload == original
    assert first == second
    assert LegacySetupMigrationPreview.from_dict(first.to_dict()) == first


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        (
            {"schema": "msh.server_setup.v99"},
            "unsupported-legacy-schema",
        ),
        (
            {"deployment_mode": "unknown-role"},
            "unknown-deployment-mode",
        ),
        ({"ai_enabled": "yes"}, "invalid-boolean"),
        ({"recorder_sources": ["not-text"]}, "invalid-text"),
        ({"configured": "yes"}, "invalid-boolean"),
    ],
)
def test_malformed_legacy_state_fails_closed(
    changes: dict[str, object],
    code: str,
) -> None:
    with pytest.raises(FederationValidationError) as error:
        preview_legacy_setup(
            legacy_payload("web-workbench", **changes),
            created_at=NOW,
        )
    assert error.value.code == code


def test_unversioned_legacy_setup_uses_compatible_defaults() -> None:
    preview = preview_legacy_setup({}, created_at=NOW)
    assert preview.source_schema == "legacy-unversioned"
    assert preview.source_mode == "web-workbench"
    assert preview.contribution_intents["workbench"] == "enabled"
    assert preview.contribution_intents["runtime"] == "enabled"
    assert preview.contribution_intents["language-model"] == "enabled"
    assert preview.contribution_intents["compute"] == "disabled"
    assert preview.contribution_intents["storage"] == "disabled"


def test_enum_values_are_explicit_contract_values() -> None:
    assert ContributionDesiredState.ASK_LATER.value == "ask-later"
    assert (
        ContributionPolicyState.APPROVAL_REQUIRED.value
        == "approval-required"
    )
    assert ContributionActivationState.SUSPENDED.value == "suspended"

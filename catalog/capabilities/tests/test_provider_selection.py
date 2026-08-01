from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from catalog.capabilities.jobs import (
    ArtifactReference,
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderSelectionPolicy,
    ProviderStatus,
)
from catalog.capabilities.provider_selection import (
    evaluate_provider_candidate,
    select_provider,
)
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)


def _job(**overrides: object) -> JobContract:
    values: dict[str, object] = {
        "job_id": "job-001",
        "session_id": "session-001",
        "request_id": "request-001",
        "idempotency_key": "session-001:request-001",
        "capability": CapabilityRequirement(
            capability_type="language-model",
            protocol="ollama-chat",
            protocol_version="1.0",
            requirements={
                "model": "qwen3-vl:4b",
                "modalities": ["text", "vision"],
                "features": {"streaming": True},
            },
        ),
        "inputs": (
            ArtifactReference(
                reference_id="artifact-input-1",
                session_id="session-001",
                schema_name="msh.prompt.v1",
                media_type="application/json",
                content_hash="sha256:" + "a" * 64,
                size_bytes=128,
            ),
        ),
        "outputs": (
            ArtifactReference(
                reference_id="artifact-output-1",
                session_id="session-001",
                schema_name="msh.ai-response.v1",
                media_type="application/json",
            ),
        ),
        "retry_policy": RetryPolicy(
            max_attempts=3,
            backoff_seconds=(1, 5),
            retryable_error_codes=("worker-lost",),
        ),
        "timeout_policy": TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=60,
            start_timeout_seconds=30,
            run_timeout_seconds=300,
            cancellation_grace_seconds=15,
        ),
    }
    values.update(overrides)
    return JobContract(**values)  # type: ignore[arg-type]


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


def test_f72_013_nested_requirement_subset_is_supported() -> None:
    candidate = evaluate_provider_candidate(_job(), _report(), evaluated_at=NOW)

    assert candidate.eligible
    assert candidate.reasons == ("eligible",)


def test_f72_014_missing_requirement_is_rejected_with_path() -> None:
    candidate = evaluate_provider_candidate(
        _job(),
        _report(
            attributes={
                "model": "qwen3-vl:4b",
                "modalities": ["text", "vision"],
            }
        ),
        evaluated_at=NOW,
    )

    assert not candidate.eligible
    assert "requirement-missing:requirements.features" in candidate.reasons


def test_f72_015_cross_session_report_is_rejected() -> None:
    candidate = evaluate_provider_candidate(
        _job(), _report(session_id="session-other"), evaluated_at=NOW
    )

    assert "session-mismatch" in candidate.reasons


def test_f72_016_only_ready_providers_are_eligible() -> None:
    for status in (
        ProviderStatus.DRAINING,
        ProviderStatus.UNAVAILABLE,
        ProviderStatus.DISABLED,
        ProviderStatus.REVOKED,
    ):
        candidate = evaluate_provider_candidate(
            _job(), _report(status=status), evaluated_at=NOW
        )
        assert not candidate.eligible
        assert f"provider-status-{status.value}" in candidate.reasons


def test_f72_017_stale_and_future_reports_fail_closed() -> None:
    expired = evaluate_provider_candidate(
        _job(),
        _report(
            reported_at=NOW - timedelta(minutes=2),
            expires_at=NOW,
        ),
        evaluated_at=NOW,
    )
    future = evaluate_provider_candidate(
        _job(),
        _report(
            reported_at=NOW + timedelta(seconds=1),
            expires_at=NOW + timedelta(seconds=30),
        ),
        evaluated_at=NOW,
    )

    assert "provider-report-expired" in expired.reasons
    assert "provider-report-from-future" in future.reasons


def test_f72_018_saturated_provider_is_rejected() -> None:
    candidate = evaluate_provider_candidate(
        _job(),
        _report(max_concurrent_jobs=2, active_jobs=2),
        evaluated_at=NOW,
    )

    assert "insufficient-available-slots" in candidate.reasons


def test_f72_019_policy_limits_queue_and_utilization() -> None:
    policy = ProviderSelectionPolicy(
        max_queue_depth=1,
        max_utilization_millis=500,
    )
    candidate = evaluate_provider_candidate(
        _job(),
        _report(queue_depth=2, utilization_millis=501),
        evaluated_at=NOW,
        policy=policy,
    )

    assert "provider-queue-limit-exceeded" in candidate.reasons
    assert "provider-utilization-limit-exceeded" in candidate.reasons


def test_f72_020_selection_is_invariant_to_input_order() -> None:
    provider_a = _report("provider-a", active_jobs=2)
    provider_b = _report("provider-b", active_jobs=1)

    first = select_provider(_job(), (provider_a, provider_b), evaluated_at=NOW)
    second = select_provider(_job(), (provider_b, provider_a), evaluated_at=NOW)

    assert first == second
    assert first.selected_capability_id == "provider-b"


def test_f72_021_available_slots_are_ranked_before_queue() -> None:
    provider_a = _report(
        "provider-a",
        max_concurrent_jobs=10,
        active_jobs=5,
        queue_depth=1,
    )
    provider_b = _report(
        "provider-b",
        max_concurrent_jobs=4,
        active_jobs=1,
        queue_depth=0,
    )

    selected = select_provider(
        _job(), (provider_a, provider_b), evaluated_at=NOW
    )

    assert selected.selected_capability_id == "provider-a"


def test_f72_022_queue_then_utilization_break_equal_capacity() -> None:
    provider_a = _report(
        "provider-a", queue_depth=1, utilization_millis=100
    )
    provider_b = _report(
        "provider-b", queue_depth=0, utilization_millis=900
    )

    selected = select_provider(
        _job(), (provider_a, provider_b), evaluated_at=NOW
    )

    assert selected.selected_capability_id == "provider-b"


def test_f72_023_explicit_node_preference_is_not_primary_semantics() -> None:
    preferred = _report(
        "provider-a",
        node_id="node-local",
        max_concurrent_jobs=2,
        active_jobs=1,
    )
    larger = _report(
        "provider-b",
        node_id="node-remote",
        max_concurrent_jobs=10,
        active_jobs=1,
    )
    policy = ProviderSelectionPolicy(preferred_node_id="node-local")

    selected = select_provider(
        _job(),
        (larger, preferred),
        evaluated_at=NOW,
        policy=policy,
    )

    assert selected.selected_capability_id == "provider-a"
    assert selected.reason == "preferred-node-provider-selected"


def test_f72_024_stable_capability_id_is_final_tie_breaker() -> None:
    provider_a = _report("provider-a", node_id="node-z")
    provider_b = _report("provider-b", node_id="node-a")

    selected = select_provider(
        _job(), (provider_b, provider_a), evaluated_at=NOW
    )

    assert selected.selected_capability_id == "provider-a"


def test_f72_025_duplicate_provider_snapshot_is_rejected() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        select_provider(_job(), (_report(), _report()), evaluated_at=NOW)

    assert exc_info.value.code == "duplicate-provider-report"


def test_f72_026_no_eligible_provider_is_explicit() -> None:
    selected = select_provider(
        _job(),
        (_report(status=ProviderStatus.DRAINING),),
        evaluated_at=NOW,
    )

    assert selected.decision == "no-eligible-provider"
    assert selected.selected_capability_id is None
    assert "provider-a" in selected.rejected


def test_f72_027_multiple_ai_providers_are_not_primary_replicas() -> None:
    provider_a = _report("ai-a", active_jobs=2)
    provider_b = _report("ai-b", active_jobs=1)

    selected = select_provider(
        _job(), (provider_a, provider_b), evaluated_at=NOW
    )

    assert selected.eligible_capability_ids == ("ai-b", "ai-a")
    assert selected.selected_capability_id == "ai-b"


def test_f72_028_timestamp_and_revision_do_not_improve_rank() -> None:
    provider_a = _report(
        "provider-a",
        report_revision=1,
        reported_at=NOW - timedelta(seconds=50),
        expires_at=NOW + timedelta(seconds=10),
    )
    provider_b = _report(
        "provider-b",
        report_revision=999,
        reported_at=NOW - timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=59),
    )

    selected = select_provider(
        _job(), (provider_b, provider_a), evaluated_at=NOW
    )

    assert selected.selected_capability_id == "provider-a"


def test_f72_029_exclusion_removes_otherwise_best_provider() -> None:
    policy = ProviderSelectionPolicy(
        excluded_capability_ids=("provider-a",)
    )
    selected = select_provider(
        _job(),
        (
            _report("provider-a", active_jobs=0),
            _report("provider-b", active_jobs=2),
        ),
        evaluated_at=NOW,
        policy=policy,
    )

    assert selected.selected_capability_id == "provider-b"
    assert selected.rejected["provider-a"] == ("provider-excluded",)


def test_f72_030_selection_does_not_mutate_job_or_reports() -> None:
    job = _job()
    report = _report()
    original_job = replace(job)
    original_report = replace(report)

    select_provider(job, (report,), evaluated_at=NOW)

    assert job == original_job
    assert report == original_report


def test_f72_033_capability_and_protocol_mismatches_are_rejected() -> None:
    wrong_type = evaluate_provider_candidate(
        _job(),
        _report(capability_type="compute-worker"),
        evaluated_at=NOW,
    )
    wrong_protocol = evaluate_provider_candidate(
        _job(),
        _report(protocol="openai-chat"),
        evaluated_at=NOW,
    )
    old_minor = evaluate_provider_candidate(
        _job(
            capability=CapabilityRequirement(
                capability_type="language-model",
                protocol="ollama-chat",
                protocol_version="1.2",
                requirements={
                    "model": "qwen3-vl:4b",
                    "modalities": ["text", "vision"],
                    "features": {"streaming": True},
                },
            )
        ),
        _report(protocol_version="1.1"),
        evaluated_at=NOW,
    )

    assert "capability-type-mismatch" in wrong_type.reasons
    assert "capability-protocol-mismatch" in wrong_protocol.reasons
    assert "capability-protocol-version-incompatible" in old_minor.reasons


def test_f72_034_minimum_available_slots_is_explicit_policy() -> None:
    candidate = evaluate_provider_candidate(
        _job(),
        _report(max_concurrent_jobs=4, active_jobs=2),
        evaluated_at=NOW,
        policy=ProviderSelectionPolicy(minimum_available_slots=3),
    )

    assert "insufficient-available-slots" in candidate.reasons

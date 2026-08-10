from __future__ import annotations

import json

import pytest

from catalog.capabilities.jobs import (
    JOBS_PROTOCOL_VERSION,
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobAttempt,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
    validate_attempt_transition,
    validate_job_transition,
)
from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)


def _sha256(character: str = "a") -> str:
    return "sha256:" + character * 64


def _retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_attempts=3,
        backoff_seconds=(1, 5),
        retryable_error_codes=("worker-lost", "provider-unavailable"),
    )


def _timeout_policy() -> TimeoutPolicy:
    return TimeoutPolicy(
        overall_timeout_seconds=600,
        queue_timeout_seconds=60,
        start_timeout_seconds=30,
        run_timeout_seconds=300,
        cancellation_grace_seconds=15,
    )


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
            requirements={"model": "qwen3-vl:4b", "modalities": ["text", "vision"]},
        ),
        "inputs": (
            ArtifactReference(
                reference_id="artifact-input-1",
                session_id="session-001",
                schema_name="fcp.prompt.v1",
                media_type="application/json",
                content_hash=_sha256("a"),
                size_bytes=128,
            ),
        ),
        "outputs": (
            ArtifactReference(
                reference_id="artifact-output-1",
                session_id="session-001",
                schema_name="fcp.ai-response.v1",
                media_type="application/json",
            ),
        ),
        "retry_policy": _retry_policy(),
        "timeout_policy": _timeout_policy(),
    }
    values.update(overrides)
    return JobContract(**values)  # type: ignore[arg-type]


def test_f71_001_job_contract_round_trip_is_canonical_json() -> None:
    job = _job()

    encoded = job.to_json()

    assert JobContract.from_json(encoded) == job
    assert json.loads(encoded) == job.to_dict()
    assert encoded == JobContract.from_dict(job.to_dict()).to_json()


def test_f71_002_additive_same_major_fields_are_ignored() -> None:
    value = _job().to_dict()
    value["future_optional_field"] = {"safe": True}
    value["capability"]["future_optional_field"] = "ignored"

    restored = JobContract.from_dict(value)

    assert restored == _job()


def test_f71_003_unknown_job_protocol_major_is_rejected() -> None:
    value = _job().to_dict()
    value["protocol_version"] = "2.0"

    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        JobContract.from_dict(value)

    assert exc_info.value.code == "unsupported-protocol-major"
    assert exc_info.value.field == "protocol_version"


def test_f71_004_unknown_nested_schema_major_is_rejected() -> None:
    value = _job().to_dict()
    value["retry_policy"]["schema"] = "fcp.job-retry-policy.v2"

    with pytest.raises(ProtocolCompatibilityError) as exc_info:
        JobContract.from_dict(value)

    assert exc_info.value.code == "unsupported-schema-major"


def test_f71_005_invalid_known_fields_fail_closed() -> None:
    value = _job().to_dict()
    value["retry_policy"]["max_attempts"] = -1

    with pytest.raises(FederationValidationError) as exc_info:
        JobContract.from_dict(value)

    assert exc_info.value.code == "invalid-positive-integer"
    assert exc_info.value.field == "max_attempts"


def test_f71_006_non_json_requirements_are_rejected() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        CapabilityRequirement(
            capability_type="compute-worker",
            protocol="fcp-compute",
            protocol_version="1.0",
            requirements={"not_json": object()},
        )

    assert exc_info.value.code == "invalid-json"


def test_f71_007_cross_session_artifact_reference_is_rejected() -> None:
    reference = ArtifactReference(
        reference_id="artifact-other",
        session_id="session-other",
        schema_name="fcp.input.v1",
        media_type="application/json",
    )

    with pytest.raises(FederationValidationError) as exc_info:
        _job(inputs=(reference,))

    assert exc_info.value.code == "cross-session-reference"


def test_f71_008_artifact_identity_requires_hash_and_size_together() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        ArtifactReference(
            reference_id="artifact-input",
            session_id="session-001",
            schema_name="fcp.input.v1",
            media_type="application/json",
            content_hash=_sha256(),
        )

    assert exc_info.value.code == "incomplete-artifact-identity"


def test_f71_009_retry_policy_is_bounded_and_deterministic() -> None:
    policy = _retry_policy()

    assert policy.allows_retry(attempt_number=1, error_code="worker-lost")
    assert not policy.allows_retry(attempt_number=3, error_code="worker-lost")
    assert not policy.allows_retry(attempt_number=1, error_code="authorization-denied")
    assert policy.delay_after(1) == 1
    assert policy.delay_after(2) == 5
    with pytest.raises(FederationValidationError, match="retry-exhausted"):
        policy.delay_after(3)


def test_f71_010_retry_backoff_must_be_complete_and_monotonic() -> None:
    with pytest.raises(FederationValidationError) as count_error:
        RetryPolicy(
            max_attempts=3,
            backoff_seconds=(1,),
            retryable_error_codes=("worker-lost",),
        )
    assert count_error.value.code == "invalid-backoff-count"

    with pytest.raises(FederationValidationError) as order_error:
        RetryPolicy(
            max_attempts=3,
            backoff_seconds=(5, 1),
            retryable_error_codes=("worker-lost",),
        )
    assert order_error.value.code == "nonmonotonic-backoff"


def test_f71_011_timeout_policy_is_finite_and_bounded() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        TimeoutPolicy(
            overall_timeout_seconds=10,
            queue_timeout_seconds=60,
            start_timeout_seconds=5,
            run_timeout_seconds=5,
            cancellation_grace_seconds=5,
        )

    assert exc_info.value.code == "invalid-overall-timeout"


def test_f71_012_job_transition_requires_queue_and_cancellation_steps() -> None:
    assert validate_job_transition(JobStatus.SUBMITTED, JobStatus.QUEUED) == JobStatus.QUEUED
    assert (
        validate_job_transition(JobStatus.QUEUED, JobStatus.CANCELLATION_REQUESTED)
        == JobStatus.CANCELLATION_REQUESTED
    )
    assert (
        validate_job_transition(JobStatus.CANCELLATION_REQUESTED, JobStatus.CANCELLED)
        == JobStatus.CANCELLED
    )

    with pytest.raises(FederationValidationError) as skipped:
        validate_job_transition(JobStatus.SUBMITTED, JobStatus.ACTIVE)
    assert skipped.value.code == "invalid-job-transition"

    with pytest.raises(FederationValidationError):
        validate_job_transition(JobStatus.QUEUED, JobStatus.CANCELLED)


def test_f71_013_job_transition_is_idempotent_but_never_moves_from_terminal() -> None:
    assert validate_job_transition(JobStatus.ACTIVE, JobStatus.ACTIVE) == JobStatus.ACTIVE

    for terminal in (
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.TIMED_OUT,
    ):
        with pytest.raises(FederationValidationError):
            validate_job_transition(terminal, JobStatus.ACTIVE)


def test_f71_014_attempt_transition_requires_assignment_acceptance_and_start() -> None:
    assert (
        validate_attempt_transition(AttemptStatus.PENDING, AttemptStatus.ASSIGNED)
        == AttemptStatus.ASSIGNED
    )
    assert (
        validate_attempt_transition(AttemptStatus.ASSIGNED, AttemptStatus.ACCEPTED)
        == AttemptStatus.ACCEPTED
    )
    assert (
        validate_attempt_transition(AttemptStatus.ACCEPTED, AttemptStatus.RUNNING)
        == AttemptStatus.RUNNING
    )

    with pytest.raises(FederationValidationError) as skipped:
        validate_attempt_transition(AttemptStatus.PENDING, AttemptStatus.RUNNING)
    assert skipped.value.code == "invalid-attempt-transition"


def test_f71_015_attempt_cancellation_is_explicit_and_terminal() -> None:
    attempt = JobAttempt(
        attempt_id="attempt-1",
        attempt_number=1,
        status=AttemptStatus.RUNNING,
    )

    requested = attempt.transition_to(AttemptStatus.CANCELLATION_REQUESTED)
    cancelled = requested.transition_to(AttemptStatus.CANCELLED)

    assert requested.status == AttemptStatus.CANCELLATION_REQUESTED
    assert cancelled.status == AttemptStatus.CANCELLED
    assert cancelled.terminal
    with pytest.raises(FederationValidationError):
        cancelled.transition_to(AttemptStatus.RUNNING)


def test_f71_016_attempt_generations_are_contiguous_and_single_active() -> None:
    with pytest.raises(FederationValidationError) as generation_error:
        _job(
            status=JobStatus.ACTIVE,
            attempts=(
                JobAttempt("attempt-2", 2, AttemptStatus.RUNNING),
            ),
        )
    assert generation_error.value.code == "nonmonotonic-attempt-generation"

    with pytest.raises(FederationValidationError) as ownership_error:
        _job(
            status=JobStatus.ACTIVE,
            attempts=(
                JobAttempt("attempt-1", 1, AttemptStatus.RUNNING),
                JobAttempt("attempt-2", 2, AttemptStatus.ASSIGNED),
            ),
        )
    assert ownership_error.value.code == "multiple-active-attempts"


def test_f71_017_active_job_requires_exactly_one_active_attempt() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _job(status=JobStatus.ACTIVE)

    assert exc_info.value.code == "active-job-attempt-mismatch"


def test_f71_018_success_requires_one_successful_attempt() -> None:
    succeeded = _job(
        status=JobStatus.SUCCEEDED,
        attempts=(JobAttempt("attempt-1", 1, AttemptStatus.SUCCEEDED),),
    )
    assert succeeded.terminal

    with pytest.raises(FederationValidationError) as exc_info:
        _job(status=JobStatus.SUCCEEDED)
    assert exc_info.value.code == "successful-job-attempt-mismatch"


def test_f71_019_retry_wait_requires_capacity_for_another_attempt() -> None:
    with pytest.raises(FederationValidationError) as exc_info:
        _job(
            status=JobStatus.RETRY_WAIT,
            attempts=(
                JobAttempt("attempt-1", 1, AttemptStatus.FAILED, "worker-lost"),
                JobAttempt("attempt-2", 2, AttemptStatus.FAILED, "worker-lost"),
                JobAttempt("attempt-3", 3, AttemptStatus.FAILED, "worker-lost"),
            ),
        )

    assert exc_info.value.code == "retry-exhausted"


def test_f71_020_cancellation_reason_only_exists_on_cancellation_path() -> None:
    job = _job().transition_to(
        JobStatus.CANCELLATION_REQUESTED, cancellation_reason="user-request"
    )

    assert job.cancellation_reason == "user-request"
    assert job.transition_to(JobStatus.CANCELLATION_REQUESTED) == job
    cancelled = job.transition_to(JobStatus.CANCELLED)
    assert cancelled.cancellation_reason == "user-request"

    with pytest.raises(FederationValidationError):
        _job(cancellation_reason="not-requested")


def test_f71_021_malformed_or_oversized_json_is_rejected() -> None:
    with pytest.raises(FederationValidationError) as malformed:
        JobContract.from_json("{")
    assert malformed.value.code == "malformed-message"

    with pytest.raises(FederationValidationError) as oversized:
        JobContract.from_json(_job().to_json(), max_bytes=10)
    assert oversized.value.code == "message-too-large"


def test_f71_022_protocol_version_defaults_to_supported_version() -> None:
    assert _job().protocol_version == JOBS_PROTOCOL_VERSION


def test_f71_023_missing_schema_and_non_array_fields_fail_structurally() -> None:
    value = _job().to_dict()
    del value["schema"]
    with pytest.raises(FederationValidationError) as missing:
        JobContract.from_dict(value)
    assert missing.value.code == "missing-field"
    assert missing.value.field == "schema"

    value = _job().to_dict()
    value["attempts"] = {"not": "an-array"}
    with pytest.raises(FederationValidationError) as invalid_array:
        JobContract.from_dict(value)
    assert invalid_array.value.code == "invalid-array"
    assert invalid_array.value.field == "attempts"

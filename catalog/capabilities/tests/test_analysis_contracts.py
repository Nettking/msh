"""Job identity, deduplication, and plan validation for analysis work."""

from __future__ import annotations

import json

import pytest

from catalog.capabilities.analysis.contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_DATA_SLICE_SCHEMA,
    ANALYSIS_PLAN_SCHEMA,
    ANALYSIS_PROTOCOL,
    MAX_PLAN_BYTES,
    AnalysisPlan,
    analysis_dispatch_id,
    analysis_grant_id,
    build_analysis_job,
)
from catalog.capabilities.tests.analysis_harness import work_slice
from catalog.federation.errors import (
    FederationValidationError,
    ProtocolCompatibilityError,
)

HASH = "sha256:" + "b" * 64


def _job(work=None):
    return build_analysis_job(
        work or work_slice(),
        plan_hash=HASH,
        plan_size=128,
        slice_hash=HASH,
        slice_size=4096,
    )


def test_same_logical_work_produces_one_stable_identity() -> None:
    first = work_slice()
    second = work_slice()

    assert first.identity_digest == second.identity_digest
    assert first.job_id == second.job_id
    assert first.idempotency_key == second.idempotency_key


def test_identity_changes_when_the_source_data_changes_materially() -> None:
    baseline = work_slice(source_signature="a" * 64)
    changed = work_slice(source_signature="c" * 64)

    assert baseline.identity_digest != changed.identity_digest
    assert baseline.job_id != changed.job_id


@pytest.mark.parametrize(
    "override",
    (
        {"target_date": "2026-08-14"},
        {"script_keys": ("data_pr_day", "data_visualizer")},
        {"runtime_namespace": "clean_20260813T000000Z"},
        {"session_id": "session-other-federation"},
    ),
)
def test_every_meaningful_input_participates_in_job_identity(override) -> None:
    assert work_slice().identity_digest != work_slice(**override).identity_digest


def test_trigger_origin_is_not_part_of_the_work_identity() -> None:
    discovered = work_slice(origin="automatic-discovery")
    uploaded = work_slice(origin="manual-upload")

    assert discovered.identity_digest == uploaded.identity_digest
    assert discovered.plan_bytes() == uploaded.plan_bytes()
    assert _job(discovered).to_json() == _job(uploaded).to_json()


def test_job_carries_input_references_and_never_the_data() -> None:
    job = _job()
    payload = job.to_json()

    assert job.capability.capability_type == ANALYSIS_CAPABILITY_TYPE
    assert job.capability.protocol == ANALYSIS_PROTOCOL
    schemas = {item.schema_name for item in job.inputs}
    assert schemas == {ANALYSIS_PLAN_SCHEMA, ANALYSIS_DATA_SLICE_SCHEMA}
    assert all(item.content_hash == HASH for item in job.inputs)
    assert "timestamp" not in payload
    assert len(payload.encode("utf-8")) < 8_192


def test_capability_requirements_describe_a_capability_not_the_payload() -> None:
    requirements = _job().capability.requirements

    assert set(requirements) == {
        "analysis_contract",
        "analysis_contract_version",
        "input_schemas",
    }


def test_plan_round_trips_and_rejects_foreign_schemas() -> None:
    work = work_slice()
    plan = AnalysisPlan.from_bytes(work.plan_bytes())

    assert plan.job_id == work.job_id
    assert plan.target_dates == work.target_dates
    assert plan.script_keys == work.script_keys

    document = json.loads(work.plan_bytes())
    document["schema"] = "fcp.other-plan.v1"
    with pytest.raises(ProtocolCompatibilityError):
        AnalysisPlan.from_bytes(json.dumps(document).encode())


@pytest.mark.parametrize(
    "mutation",
    (
        {"script_keys": ["../../etc/passwd"]},
        {"script_keys": ["rm -rf /"]},
        {"target_dates": ["not-a-date"]},
        {"runtime_namespace": "../escape"},
        {"source_signature": "not-hex"},
        {"target_dates": []},
    ),
)
def test_plan_rejects_unsafe_or_malformed_values(mutation) -> None:
    document = json.loads(work_slice().plan_bytes())
    document.update(mutation)

    with pytest.raises(FederationValidationError):
        AnalysisPlan.from_bytes(json.dumps(document).encode())


def test_plan_payload_is_bounded() -> None:
    with pytest.raises(FederationValidationError):
        AnalysisPlan.from_bytes(b"{" + b" " * (MAX_PLAN_BYTES + 1))


def test_grant_and_dispatch_identity_are_bound_to_one_attempt() -> None:
    first = analysis_grant_id("analysis-1", "attempt-1")
    second = analysis_grant_id("analysis-1", "attempt-2")

    assert first != second
    assert analysis_dispatch_id("analysis-1", "attempt-1") != analysis_dispatch_id(
        "analysis-1", "attempt-2"
    )

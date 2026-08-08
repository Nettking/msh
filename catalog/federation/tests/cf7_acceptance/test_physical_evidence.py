from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from .physical_evidence import (
    PROVENANCE_CARRIED_FORWARD,
    PROVENANCE_OBSERVED,
    REQUIRED_ENVIRONMENTS,
    REQUIRED_SCENARIOS,
    SCHEMA,
    PhysicalEvidenceError,
    main,
    validate_physical_evidence,
)

COMMIT = "a" * 40
BASELINE = "b" * 40


def _complete_document() -> dict[str, object]:
    return {
        "schema": SCHEMA,
        "commit_sha": COMMIT,
        "executed_at": "2026-08-03T14:00:00Z",
        "operator": "CF7 acceptance operator",
        "environments": {
            name: {
                "os_family": name,
                "fresh_checkout": True,
                "repository_clean": True,
                "result": "passed",
                "command_log": f"evidence/{name}/commands.txt",
                "provenance": PROVENANCE_OBSERVED,
                "observed_commit": COMMIT,
            }
            for name in REQUIRED_ENVIRONMENTS
        },
        "scenarios": {
            scenario_id: {
                "status": "passed",
                "provenance": PROVENANCE_OBSERVED,
                "observed_commit": COMMIT,
                "evidence_refs": [
                    "evidence/scenarios/"
                    + scenario_id.replace(".", "-")
                    + ".txt"
                ],
                "notes": "Observed the required bounded behavior on real equipment.",
            }
            for scenario_id in REQUIRED_SCENARIOS
        },
        "privacy_review": {
            "no_credentials": True,
            "no_private_endpoints": True,
            "no_private_database_paths": True,
            "no_reusable_enrollment_material": True,
            "operator_reviewed": True,
        },
        "notes": ["Evidence references contain redacted command output only."],
    }


def test_complete_physical_evidence_is_candidate_bound_and_accepted() -> None:
    summary = validate_physical_evidence(
        _complete_document(),
        expected_commit=COMMIT,
    )

    assert summary == {
        "schema": SCHEMA,
        "commit_sha": COMMIT,
        "executed_at": "2026-08-03T14:00:00Z",
        "environment_count": len(REQUIRED_ENVIRONMENTS),
        "scenario_count": len(REQUIRED_SCENARIOS),
        "observed_record_count": len(REQUIRED_ENVIRONMENTS)
        + len(REQUIRED_SCENARIOS),
        "carried_forward_record_count": 0,
        "accepted": True,
    }


def test_cli_validates_complete_document_and_rejects_wrong_candidate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "physical-evidence.json"
    path.write_text(json.dumps(_complete_document()), encoding="utf-8")

    assert main([str(path), "--commit", COMMIT, "--repo-root", str(tmp_path)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["accepted"] is True
    assert output["scenario_count"] == len(REQUIRED_SCENARIOS)

    assert main([str(path), "--commit", "c" * 40]) == 2
    assert "physical-evidence-commit-mismatch" in capsys.readouterr().err


def test_template_is_complete_in_shape_but_intentionally_not_accepted() -> None:
    template_path = Path(__file__).with_name("physical_evidence.template.json")
    template = json.loads(template_path.read_text(encoding="utf-8"))

    assert template["schema"] == SCHEMA
    assert tuple(template["environments"]) == REQUIRED_ENVIRONMENTS
    assert tuple(template["scenarios"]) == REQUIRED_SCENARIOS
    assert all(
        scenario["status"] == "pending"
        for scenario in template["scenarios"].values()
    )
    with pytest.raises(PhysicalEvidenceError) as incomplete:
        validate_physical_evidence(template)
    assert incomplete.value.code == "incomplete-physical-evidence"


def test_carried_forward_scenario_requires_explicit_impact_authorization() -> None:
    document = _complete_document()
    scenario_id = "physical.ollama-model-and-accelerator"
    scenario = document["scenarios"][scenario_id]
    scenario["provenance"] = PROVENANCE_CARRIED_FORWARD
    scenario["observed_commit"] = BASELINE

    with pytest.raises(PhysicalEvidenceError) as missing:
        validate_physical_evidence(document)
    assert missing.value.code == "physical-evidence-revalidation-required"

    summary = validate_physical_evidence(
        document,
        carry_forward_check=lambda observed, candidate, scenario: (
            observed == BASELINE
            and candidate == COMMIT
            and scenario == scenario_id
        ),
    )
    assert summary["carried_forward_record_count"] == 1
    assert summary["observed_record_count"] == (
        len(REQUIRED_ENVIRONMENTS) + len(REQUIRED_SCENARIOS) - 1
    )


def test_carried_forward_scenario_fails_when_impact_requires_rerun() -> None:
    document = _complete_document()
    scenario = document["scenarios"]["physical.restart-and-reconciliation"]
    scenario["provenance"] = PROVENANCE_CARRIED_FORWARD
    scenario["observed_commit"] = BASELINE

    with pytest.raises(PhysicalEvidenceError) as rerun:
        validate_physical_evidence(
            document,
            carry_forward_check=lambda _observed, _candidate, _scenario: False,
        )
    assert rerun.value.code == "physical-evidence-rerun-required"


def test_carried_forward_environment_uses_corresponding_fresh_scenario() -> None:
    document = _complete_document()
    environment = document["environments"]["windows"]
    environment["provenance"] = PROVENANCE_CARRIED_FORWARD
    environment["observed_commit"] = BASELINE
    calls: list[tuple[str, str, str]] = []

    def allow(observed: str, candidate: str, scenario: str) -> bool:
        calls.append((observed, candidate, scenario))
        return scenario == "physical.fresh-windows-checkout"

    summary = validate_physical_evidence(document, carry_forward_check=allow)
    assert summary["carried_forward_record_count"] == 1
    assert calls == [(BASELINE, COMMIT, "physical.fresh-windows-checkout")]


def test_observed_provenance_cannot_point_at_an_older_commit() -> None:
    document = _complete_document()
    scenario = document["scenarios"][REQUIRED_SCENARIOS[0]]
    scenario["observed_commit"] = BASELINE

    with pytest.raises(PhysicalEvidenceError) as mismatch:
        validate_physical_evidence(document)
    assert mismatch.value.code == "physical-evidence-observation-commit-mismatch"


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (
            lambda document: document["scenarios"].pop(REQUIRED_SCENARIOS[-1]),
            "invalid-physical-evidence-fields",
        ),
        (
            lambda document: document["scenarios"][REQUIRED_SCENARIOS[0]].update(
                status="pending"
            ),
            "incomplete-physical-evidence",
        ),
        (
            lambda document: document["environments"]["windows"].update(
                repository_clean=False
            ),
            "incomplete-physical-evidence",
        ),
        (
            lambda document: document["privacy_review"].update(
                no_credentials=False
            ),
            "incomplete-physical-evidence",
        ),
        (
            lambda document: document["scenarios"][REQUIRED_SCENARIOS[0]].update(
                evidence_refs=["../outside.txt"]
            ),
            "unsafe-physical-evidence-path",
        ),
        (
            lambda document: document["scenarios"][REQUIRED_SCENARIOS[0]].update(
                provenance="invented"
            ),
            "invalid-physical-evidence-provenance",
        ),
    ],
)
def test_incomplete_or_unbounded_evidence_fails_closed(mutation, expected_code) -> None:
    document = _complete_document()
    mutation(document)

    with pytest.raises(PhysicalEvidenceError) as failure:
        validate_physical_evidence(document)
    assert failure.value.code == expected_code


@pytest.mark.parametrize(
    "private_value",
    [
        "Bearer reusable-secret",
        "http://private-host.invalid:11434",
        "192.168.10.44",
        "C:\\Users\\operator\\msh\\data",
        "/home/operator/msh/data",
        "server_settings.json",
        "invitation_token=secret",
    ],
)
def test_private_values_are_rejected_from_every_text_field(private_value: str) -> None:
    document = deepcopy(_complete_document())
    document["notes"] = [private_value]

    with pytest.raises(PhysicalEvidenceError) as private:
        validate_physical_evidence(document)
    assert private.value.code == "private-physical-evidence"


def test_malformed_and_oversized_documents_fail_without_parsing_secrets(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    malformed = tmp_path / "malformed.json"
    malformed.write_bytes(b"{not-json")
    assert main([str(malformed)]) == 2
    assert "malformed-physical-evidence" in capsys.readouterr().err

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * 262_145)
    assert main([str(oversized)]) == 2
    assert "physical-evidence-too-large" in capsys.readouterr().err

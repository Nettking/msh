from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from .physical_evidence import REQUIRED_SCENARIOS
from .physical_revalidation import (
    PhysicalRevalidationError,
    analyze_commits,
    classify_changed_paths,
    main,
    scenario_can_carry_forward,
)

PAIRING_IMPACT = (
    "physical.multi-host-relay-and-network-path",
    "physical.mobile-and-desktop-browser-review",
    "physical.separate-ai-compute-storage-contributors",
    "physical.restart-and-reconciliation",
    "physical.revocation-and-controlled-rejoin",
)


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _write_impact_map(repo: Path) -> Path:
    source = Path(__file__).with_name("physical_impact_map.json")
    destination = (
        repo
        / "catalog"
        / "federation"
        / "tests"
        / "cf7_acceptance"
        / "physical_impact_map.json"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return destination


def test_pr212_pairing_changes_only_invalidate_pairing_dependent_scenarios() -> None:
    report = classify_changed_paths(
        [
            "catalog/flask_app/services/federation_pairing_service.py",
            "catalog/flask_app/tests/test_federation_pairing_relay.py",
            "catalog/node/client.py",
            "catalog/node/state.py",
            "catalog/node/tests/test_client.py",
        ]
    )

    assert report.safe is True
    assert report.unknown_paths == ()
    assert report.impacted_scenarios == PAIRING_IMPACT
    assert "physical.ollama-model-and-accelerator" in report.carry_forward_scenarios
    assert "physical.mtconnect-recorder-source" in report.carry_forward_scenarios
    assert "physical.benchmark-expiry-and-rerun" in report.carry_forward_scenarios
    assert "physical.contribution-disable-and-reenable" in report.carry_forward_scenarios


def test_docs_tests_and_acceptance_tooling_do_not_invalidate_physical_observations() -> None:
    report = classify_changed_paths(
        [
            "docs/implementation/federation/acceptance/example.md",
            ".github/workflows/cf7.yml",
            "catalog/node/tests/test_client.py",
            "catalog/federation/tests/cf7_acceptance/physical_evidence.py",
            "ops/cf7_physical_readiness.py",
        ]
    )

    assert report.safe is True
    assert report.impacted_scenarios == ()
    assert report.carry_forward_scenarios == REQUIRED_SCENARIOS


def test_runtime_composition_change_requires_every_physical_scenario() -> None:
    report = classify_changed_paths(["docker-compose.yml"])

    assert report.safe is True
    assert report.impacted_scenarios == REQUIRED_SCENARIOS
    assert report.carry_forward_scenarios == ()


def test_unknown_product_path_fails_closed_and_requires_full_rerun() -> None:
    report = classify_changed_paths(["catalog/new_authority/runtime.py"])

    assert report.safe is False
    assert report.unknown_paths == ("catalog/new_authority/runtime.py",)
    assert report.impacted_scenarios == REQUIRED_SCENARIOS
    assert report.carry_forward_scenarios == ()


def test_ai_change_requires_restart_and_reconciliation_revalidation() -> None:
    report = classify_changed_paths(["catalog/ai/runtime.py"])

    assert report.safe is True
    assert "physical.restart-and-reconciliation" in report.impacted_scenarios


def test_git_analyzer_requires_ancestor_and_uses_actual_diff(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "cf7@example.invalid")
    _git(tmp_path, "config", "user.name", "CF7 Test")
    runtime = tmp_path / "catalog" / "ai" / "runtime.py"
    runtime.parent.mkdir(parents=True)
    runtime.write_text("MODEL = 'a'\n", encoding="utf-8")
    _write_impact_map(tmp_path)
    baseline = _commit(tmp_path, "baseline")
    runtime.write_text("MODEL = 'b'\n", encoding="utf-8")
    candidate = _commit(tmp_path, "candidate")

    report = analyze_commits(tmp_path, baseline, candidate)

    assert report.safe is True
    assert report.changed_paths == ("catalog/ai/runtime.py",)
    assert "physical.ollama-model-and-accelerator" in report.impacted_scenarios
    assert scenario_can_carry_forward(
        tmp_path,
        baseline,
        candidate,
        "physical.contribution-disable-and-reenable",
    ) is True
    assert scenario_can_carry_forward(
        tmp_path,
        baseline,
        candidate,
        "physical.ollama-model-and-accelerator",
    ) is False

    with pytest.raises(PhysicalRevalidationError) as non_ancestor:
        analyze_commits(tmp_path, candidate, baseline)
    assert non_ancestor.value.code == "baseline-not-ancestor"


def test_git_analyzer_preserves_old_path_when_product_file_is_renamed(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "cf7@example.invalid")
    _git(tmp_path, "config", "user.name", "CF7 Test")
    product = tmp_path / "catalog" / "node" / "client.py"
    product.parent.mkdir(parents=True)
    product.write_text("VALUE = 1\n", encoding="utf-8")
    _write_impact_map(tmp_path)
    baseline = _commit(tmp_path, "baseline")

    documentation = tmp_path / "docs" / "client.md"
    documentation.parent.mkdir(parents=True)
    product.rename(documentation)
    candidate = _commit(tmp_path, "rename product path")

    report = analyze_commits(tmp_path, baseline, candidate)

    assert report.safe is True
    assert "catalog/node/client.py" in report.changed_paths
    assert "docs/client.md" in report.changed_paths
    assert report.impacted_scenarios == PAIRING_IMPACT


def test_git_analyzer_loads_impact_policy_from_candidate_tree(tmp_path: Path) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "cf7@example.invalid")
    _git(tmp_path, "config", "user.name", "CF7 Test")
    impact_map_path = _write_impact_map(tmp_path)
    baseline = _commit(tmp_path, "baseline")

    policy = json.loads(impact_map_path.read_text(encoding="utf-8"))
    policy["rules"].append(
        {
            "patterns": ["catalog/candidate-policy/**"],
            "scenarios": ["physical.restart-and-reconciliation"],
        }
    )
    impact_map_path.write_text(
        json.dumps(policy, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    product = tmp_path / "catalog" / "candidate-policy" / "runtime.py"
    product.parent.mkdir(parents=True)
    product.write_text("VALUE = 1\n", encoding="utf-8")
    candidate = _commit(tmp_path, "candidate policy and product change")

    report = analyze_commits(tmp_path, baseline, candidate)

    assert report.safe is True
    assert report.unknown_paths == ()
    assert report.impacted_scenarios == ("physical.restart-and-reconciliation",)


def test_git_analyzer_fails_closed_when_candidate_impact_map_is_missing(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "cf7@example.invalid")
    _git(tmp_path, "config", "user.name", "CF7 Test")
    note = tmp_path / "docs" / "note.md"
    note.parent.mkdir(parents=True)
    note.write_text("one\n", encoding="utf-8")
    baseline = _commit(tmp_path, "baseline")
    note.write_text("two\n", encoding="utf-8")
    candidate = _commit(tmp_path, "candidate")

    with pytest.raises(PhysicalRevalidationError) as invalid_map:
        analyze_commits(tmp_path, baseline, candidate)

    assert invalid_map.value.code == "invalid-impact-map"


def test_revalidation_cli_prints_machine_readable_plan(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "cf7@example.invalid")
    _git(tmp_path, "config", "user.name", "CF7 Test")
    docs = tmp_path / "docs" / "note.md"
    docs.parent.mkdir(parents=True)
    docs.write_text("one\n", encoding="utf-8")
    _write_impact_map(tmp_path)
    baseline = _commit(tmp_path, "baseline")
    docs.write_text("two\n", encoding="utf-8")
    candidate = _commit(tmp_path, "candidate")

    assert main(
        [
            "--baseline",
            baseline,
            "--candidate",
            candidate,
            "--repo-root",
            str(tmp_path),
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["safe"] is True
    assert output["impacted_scenarios"] == []
    assert output["carry_forward_scenarios"] == list(REQUIRED_SCENARIOS)

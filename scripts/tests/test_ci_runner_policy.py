from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from scripts.ci_runner_policy import expand_matrix, validate_workflows

LINUX = ["self-hosted", "Linux", "X64", "fcp-linux-fast"]
WINDOWS = ["self-hosted", "Windows", "X64", "fcp-windows"]


class RunnerPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repository = Path(self.temporary.name)
        (self.repository / ".github" / "workflows").mkdir(parents=True)

    def workflow(self, jobs: dict, name: str = "test.yml", events: object = "push") -> None:
        self.raw_workflow(yaml.safe_dump({"on": events, "jobs": jobs}), name)

    def raw_workflow(self, source: str, name: str = "test.yml") -> None:
        (self.repository / ".github" / "workflows" / name).write_text(source, encoding="utf-8")

    def assert_valid(self, jobs: dict, count: int = 1) -> None:
        self.workflow(jobs)
        errors, actual_count = validate_workflows(self.repository)
        self.assertEqual([], errors)
        self.assertEqual(count, actual_count)

    def assert_invalid(self, jobs: dict, reason: str) -> None:
        self.workflow(jobs)
        errors, _ = validate_workflows(self.repository)
        self.assertTrue(errors)
        self.assertIn(reason, "\n".join(errors))

    def test_both_shared_pools_are_valid(self) -> None:
        self.assert_valid({"linux": {"runs-on": LINUX}, "windows": {"runs-on": WINDOWS}}, count=2)

    def test_hosted_runner_images_are_rejected(self) -> None:
        for hosted in ("ubuntu-latest", "ubuntu-24.04", "windows-latest", "windows-2025", "macos-15"):
            with self.subTest(hosted=hosted):
                self.assert_invalid({"test": {"runs-on": hosted}}, "runs-on must be literal")

    def test_incomplete_pinned_group_and_expression_routes_are_rejected(self) -> None:
        for route in (
            ["self-hosted", "fcp-linux-fast"],
            [*LINUX, "a-physical-machine"],
            ["self-hosted", "Linux", "X64", "${{ inputs.pool }}"],
            {"group": "hosted", "labels": LINUX},
            "${{ inputs.runner }}",
            "${{ github.ref == 'refs/heads/main' && 'self-hosted' || 'ubuntu-latest' }}",
        ):
            with self.subTest(route=route):
                self.assert_invalid({"test": {"runs-on": route}}, "runner")

    def test_missing_runner_is_rejected(self) -> None:
        self.assert_invalid({"test": {"steps": []}}, "missing runs-on")

    def test_native_matrix_product_and_nested_reference_are_valid(self) -> None:
        self.assert_valid({"test": {
            "runs-on": "${{ matrix.platform.runner }}",
            "strategy": {"matrix": {"platform": [{"runner": LINUX}, {"runner": WINDOWS}], "python": ["3.12", "3.13"]}},
        }})

    def test_from_json_include_matrix_is_valid(self) -> None:
        self.assert_valid({"test": {
            "runs-on": "${{ fromJSON(matrix.runner) }}",
            "strategy": {"matrix": {"include": [
                {"runner": json.dumps(LINUX)}, {"runner": json.dumps(WINDOWS)},
            ]}},
        }})

    def test_hosted_matrix_product_is_rejected(self) -> None:
        self.assert_invalid({"test": {
            "runs-on": "${{ matrix.runner }}",
            "strategy": {"matrix": {"runner": [LINUX, ["ubuntu-latest"]], "shard": [1, 2]}},
        }}, "ubuntu-latest")

    def test_hosted_matrix_include_is_rejected(self) -> None:
        self.assert_invalid({"test": {
            "runs-on": "${{ fromJSON(matrix.runner) }}",
            "strategy": {"matrix": {"include": [
                {"runner": json.dumps(LINUX)}, {"runner": '["windows-latest"]'},
            ]}},
        }}, "windows-latest")

    def test_static_exclude_removes_hosted_possibility(self) -> None:
        self.assert_valid({"test": {
            "runs-on": "${{ matrix.runner }}",
            "strategy": {"matrix": {"runner": [LINUX, ["ubuntu-latest"]], "exclude": [{"runner": ["ubuntu-latest"]}]}},
        }})

    def test_include_can_add_excluded_hosted_combination_back(self) -> None:
        self.assert_invalid({"test": {
            "runs-on": "${{ matrix.runner }}",
            "strategy": {"matrix": {
                "runner": [LINUX, ["ubuntu-latest"]],
                "exclude": [{"runner": ["ubuntu-latest"]}],
                "include": [{"runner": ["ubuntu-latest"]}],
            }},
        }}, "ubuntu-latest")

    def test_include_cannot_overwrite_original_axis_value(self) -> None:
        self.assert_invalid({"test": {
            "runs-on": "${{ matrix.runner }}",
            "strategy": {"matrix": {"runner": [LINUX], "include": [{"runner": ["macos-latest"]}]}},
        }}, "macos-latest")

    def test_include_merge_uses_only_original_axis_combinations(self) -> None:
        matrix = {
            "fruit": ["apple", "pear"],
            "animal": ["cat", "dog"],
            "include": [
                {"color": "green"},
                {"color": "pink", "animal": "cat"},
                {"fruit": "apple", "shape": "circle"},
                {"fruit": "banana"},
                {"fruit": "banana", "animal": "cat"},
            ],
        }
        self.assertEqual(expand_matrix(matrix), [
            {"fruit": "apple", "animal": "cat", "color": "pink", "shape": "circle"},
            {"fruit": "apple", "animal": "dog", "color": "green", "shape": "circle"},
            {"fruit": "pear", "animal": "cat", "color": "pink"},
            {"fruit": "pear", "animal": "dog", "color": "green"},
            {"fruit": "banana"}, {"fruit": "banana", "animal": "cat"},
        ])

    def test_unresolved_or_malformed_routing_matrices_are_rejected(self) -> None:
        for matrix in (
            "${{ fromJSON(needs.prepare.outputs.matrix) }}",
            {"runner": "${{ inputs.runner }}"},
            {"include": "${{ fromJSON(inputs.matrix) }}"},
            {"include": [{"os": "Linux"}]},
            {"include": [{"runner": "${{ inputs.runner }}"}]},
            {"include": [{"runner": "not JSON"}]},
            {"runner": []},
        ):
            with self.subTest(matrix=matrix):
                self.assert_invalid({"test": {"runs-on": "${{ fromJSON(matrix.runner) }}", "strategy": {"matrix": matrix}}}, "matrix")

    def test_dynamic_non_routing_matrix_does_not_change_literal_pool(self) -> None:
        self.assert_valid({"test": {
            "runs-on": LINUX,
            "strategy": {"matrix": "${{ fromJSON(needs.prepare.outputs.matrix) }}"},
        }})

    def test_local_reusable_workflow_is_audited(self) -> None:
        self.workflow({"test": {"runs-on": WINDOWS}}, "called.yml", events="workflow_call")
        self.assert_valid({"call": {"uses": "./.github/workflows/called.yml"}})

    def test_local_reusable_workflow_cannot_hide_hosted_or_dynamic_routing(self) -> None:
        for route in ("ubuntu-latest", "${{ inputs.runner }}"):
            with self.subTest(route=route):
                self.workflow({"test": {"runs-on": route}}, "called.yml", events="workflow_call")
                self.assert_invalid({"call": {"uses": "./.github/workflows/called.yml"}}, "called.yml:test")

    def test_remote_dynamic_missing_and_escaping_reusable_workflows_are_rejected(self) -> None:
        for target in (
            "owner/repo/.github/workflows/ci.yml@main",
            "${{ inputs.workflow }}",
            "./.github/workflows/missing.yml",
            "./.github/workflows/../../outside.yml",
        ):
            with self.subTest(target=target):
                self.assert_invalid({"call": {"uses": target}}, "workflow")

    def test_reusable_workflow_cycle_is_rejected(self) -> None:
        self.workflow({"again": {"uses": "./.github/workflows/test.yml"}}, "called.yml", events="workflow_call")
        self.assert_invalid({"call": {"uses": "./.github/workflows/called.yml"}}, "recursive")

    def test_duplicate_yaml_keys_cannot_hide_hosted_route(self) -> None:
        self.raw_workflow("on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    runs-on: [self-hosted, Linux, X64, fcp-linux-fast]\n")
        errors, _ = validate_workflows(self.repository)
        self.assertIn("duplicate YAML key", "\n".join(errors))

    def test_pull_request_triggers_are_rejected_in_every_yaml_form(self) -> None:
        for events in (
            "pull_request", "pull_request_target", ["push", "pull_request"],
            {"pull_request": {"types": ["opened"]}}, {"pull_request_target": None},
        ):
            with self.subTest(events=events):
                self.workflow({"test": {"runs-on": LINUX}}, events=events)
                errors, _ = validate_workflows(self.repository)
                self.assertIn("untrusted pull-request triggers", "\n".join(errors))

    def test_unquoted_on_key_is_treated_as_an_event_mapping(self) -> None:
        self.raw_workflow("on:\n  pull_request:\njobs:\n  test:\n    runs-on: [self-hosted, Linux, X64, fcp-linux-fast]\n")
        errors, _ = validate_workflows(self.repository)
        self.assertIn("untrusted pull-request triggers", "\n".join(errors))

    def test_repository_workflows_meet_runner_and_trusted_entry_policy(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        errors, job_count = validate_workflows(repository)
        self.assertEqual([], errors, "\n".join(errors))
        self.assertGreater(job_count, 0)


if __name__ == "__main__":
    unittest.main()

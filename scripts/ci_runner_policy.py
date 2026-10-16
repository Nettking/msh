#!/usr/bin/env python3
"""Fail closed unless every possible Actions job uses an approved shared pool.

Only literal runner labels and static matrix values are provable here. Arbitrary
GitHub expressions, input-selected runners, and remote reusable workflows are
rejected rather than assuming they cannot select a hosted runner.
"""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
import re
import sys
from typing import Any

import yaml


ALLOWED_POOLS = (
    frozenset(("self-hosted", "Linux", "X64", "fcp-linux-fast")),
    frozenset(("self-hosted", "Windows", "X64", "fcp-windows")),
)
MATRIX_REFERENCE = re.compile(r"\$\{\{\s*matrix\.([A-Za-z_][A-Za-z0-9_.]*)\s*\}\}")
JSON_MATRIX_REFERENCE = re.compile(
    r"\$\{\{\s*fromJSON\(\s*matrix\.([A-Za-z_][A-Za-z0-9_.]*)\s*\)\s*\}\}",
    re.IGNORECASE,
)


class PolicyError(ValueError):
    """The workflow cannot be proved to use only approved runner pools."""


class WorkflowLoader(yaml.SafeLoader):
    """Reject duplicate keys instead of silently hiding a runner declaration."""


# GitHub uses YAML 1.2 booleans: the workflow key `on` is a string, not True.
WorkflowLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in entries if tag != "tag:yaml.org,2002:bool"]
    for key, entries in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
WorkflowLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|false)$", re.IGNORECASE), list("tTfF")
)


def _mapping(loader: WorkflowLoader, node: yaml.MappingNode) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise PolicyError("YAML mapping keys must be scalar") from exc
        if duplicate:
            raise PolicyError(f"duplicate YAML key {key!r} at line {key_node.start_mark.line + 1}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


WorkflowLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def expand_matrix(matrix: Any) -> list[dict[str, Any]]:
    """Expand static product, exclude, and include using Actions merge semantics.

    Include entries can augment original axis combinations without overwriting
    their axis values. Entries that cannot augment an original combination add
    a new row. Newly added rows never become candidates for later include merges.
    """
    if not isinstance(matrix, dict) or any(not isinstance(key, str) for key in matrix):
        raise PolicyError("runner matrix must be a static mapping")
    axes = {key: value for key, value in matrix.items() if key not in ("include", "exclude")}
    for key, values in axes.items():
        if not isinstance(values, list) or not values:
            raise PolicyError(f"matrix axis {key!r} must be a nonempty static list")
    includes = matrix.get("include", [])
    excludes = matrix.get("exclude", [])
    for name, entries in (("include", includes), ("exclude", excludes)):
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise PolicyError(f"matrix {name} must be a static list of mappings")
    if not axes:
        if not includes:
            raise PolicyError("runner matrix has no combinations")
        if excludes:
            raise PolicyError("exclude without matrix axes is unsupported")
        return [dict(entry) for entry in includes]

    size = 1
    for values in axes.values():
        size *= len(values)
    if size > 256:
        raise PolicyError("matrix exceeds the Actions limit of 256 combinations")
    originals = [
        dict(zip(axes, values))
        for values in itertools.product(*axes.values())
    ]
    originals = [
        row for row in originals
        if not any(all(row.get(key) == value for key, value in excluded.items()) for excluded in excludes)
    ]
    rows = [dict(row) for row in originals]
    additions = []
    for included in includes:
        matched = False
        for original, row in zip(originals, rows):
            if all(key not in original or original[key] == value for key, value in included.items()):
                row.update(included)
                matched = True
        if not matched:
            additions.append(dict(included))
    rows.extend(additions)
    if not rows:
        raise PolicyError("runner matrix has no combinations")
    if len(rows) > 256:
        raise PolicyError("matrix exceeds the Actions limit of 256 combinations")
    return rows


def resolve_runner(runner: Any, row: dict[str, Any]) -> Any:
    if not isinstance(runner, str):
        return runner
    plain = MATRIX_REFERENCE.fullmatch(runner)
    encoded = JSON_MATRIX_REFERENCE.fullmatch(runner)
    reference = plain or encoded
    if not reference:
        raise PolicyError(f"runs-on must be literal approved runner labels or a supported matrix reference: {runner!r}")
    value: Any = row
    for part in reference.group(1).split("."):
        if not isinstance(value, dict) or part not in value:
            raise PolicyError(f"runner matrix reference has no value: {reference.group(1)}")
        value = value[part]
    if encoded:
        if not isinstance(value, str):
            raise PolicyError("fromJSON runner matrix value must be a literal JSON string")
        try:
            value = json.loads(value)
        except (ValueError, TypeError) as exc:
            raise PolicyError("runner matrix value is not valid JSON") from exc
    return value


def check_labels(labels: Any) -> None:
    if not isinstance(labels, list) or any(not isinstance(label, str) for label in labels):
        raise PolicyError(f"runner must be a literal list of approved shared-pool labels: {labels!r}")
    if len(labels) != 4 or frozenset(labels) not in ALLOWED_POOLS:
        raise PolicyError(f"runner is outside the approved Linux/Windows shared pools: {labels!r}")


def check_triggers(workflow: dict[str, Any]) -> None:
    """PR workflows can replace their own guards before executing on a runner."""
    triggers = workflow.get("on", {})
    if isinstance(triggers, str):
        events = [triggers]
    elif isinstance(triggers, (list, dict)):
        events = list(triggers)
    else:
        raise PolicyError("workflow events must be a string, list, or mapping")
    if any(event in ("pull_request", "pull_request_target") for event in events):
        raise PolicyError("untrusted pull-request triggers cannot execute on persistent runners; use branch push checks")


def validate_workflows(repository: Path) -> tuple[list[str], int]:
    """Return errors and the number of executing jobs audited, without executing YAML."""
    root = repository.resolve()
    workflow_dir = root / ".github" / "workflows"
    files = sorted({*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml")})
    errors: list[str] = []
    audited: set[Path] = set()
    job_count = 0
    if not files:
        return ["no workflow files found in .github/workflows"], 0

    def visit(path: Path, stack: tuple[Path, ...] = ()) -> None:
        nonlocal job_count
        path = path.resolve()
        display = path.relative_to(root).as_posix()
        if path in stack:
            errors.append(f"{display}: recursive reusable workflow call")
            return
        if path in audited:
            return
        try:
            workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=WorkflowLoader)
        except (OSError, yaml.YAMLError, PolicyError) as exc:
            errors.append(f"{display}: {exc}")
            audited.add(path)
            return
        if not isinstance(workflow, dict) or not isinstance(workflow.get("jobs"), dict) or not workflow["jobs"]:
            errors.append(f"{display}: workflow must define jobs")
            audited.add(path)
            return
        try:
            check_triggers(workflow)
        except PolicyError as exc:
            errors.append(f"{display}: {exc}")
        for job_id, job in workflow["jobs"].items():
            location = f"{display}:{job_id}"
            try:
                if not isinstance(job, dict):
                    raise PolicyError("job must be a mapping")
                if "uses" in job:
                    if "runs-on" in job:
                        raise PolicyError("reusable workflow job also declares runs-on")
                    target = job["uses"]
                    if not isinstance(target, str) or not target.startswith("./.github/workflows/"):
                        raise PolicyError("remote or dynamic reusable workflows cannot be audited")
                    called = (root / target).resolve()
                    if called.parent != workflow_dir.resolve() or called.suffix not in (".yml", ".yaml"):
                        raise PolicyError("reusable workflow must be a local .github/workflows YAML file")
                    if not called.is_file():
                        raise PolicyError(f"local reusable workflow does not exist: {target}")
                    visit(called, (*stack, path))
                    continue
                if "runs-on" not in job:
                    raise PolicyError("executing job is missing runs-on")
                runner = job["runs-on"]
                rows = [{}]
                if isinstance(runner, str) and (
                    MATRIX_REFERENCE.fullmatch(runner) or JSON_MATRIX_REFERENCE.fullmatch(runner)
                ):
                    strategy = job.get("strategy", {})
                    if not isinstance(strategy, dict):
                        raise PolicyError("strategy must be a static mapping")
                    rows = expand_matrix(strategy.get("matrix"))
                for row in rows:
                    try:
                        check_labels(resolve_runner(runner, row))
                    except PolicyError as exc:
                        context = f" (matrix {json.dumps(row, sort_keys=True)})" if row else ""
                        raise PolicyError(f"{exc}{context}") from exc
                job_count += 1
            except PolicyError as exc:
                errors.append(f"{location}: {exc}")
        audited.add(path)

    for path in files:
        visit(path)
    return errors, job_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    errors, job_count = validate_workflows(args.repository)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(f"Runner policy failed: {len(errors)} violation(s).", file=sys.stderr)
        return 1
    print(f"Runner policy passed: {job_count} executing jobs use only approved shared self-hosted pools.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

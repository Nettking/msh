"""Impact-based revalidation for CF7 physical acceptance evidence.

This module is acceptance tooling only. It never grants runtime authority.
It determines which already-observed physical scenarios must be rerun after a
candidate changes, using an auditable path-to-scenario impact map. Unknown
product paths fail closed by invalidating the complete physical scenario set.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from .physical_evidence import REQUIRED_SCENARIOS

IMPACT_MAP_PATH = Path(__file__).with_name("physical_impact_map.json")
IMPACT_MAP_REPOSITORY_PATH = (
    "catalog/federation/tests/cf7_acceptance/physical_impact_map.json"
)
IMPACT_MAP_SCHEMA = "msh.cf7.physical-impact-map.v1"
PLAN_SCHEMA = "msh.cf7.physical-revalidation-plan.v1"


class PhysicalRevalidationError(ValueError):
    """Impact analysis could not establish a safe carry-forward decision."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ImpactMap:
    no_physical_impact: tuple[str, ...]
    global_invalidation: tuple[str, ...]
    rules: tuple[tuple[tuple[str, ...], frozenset[str]], ...]


@dataclass(frozen=True)
class ImpactReport:
    changed_paths: tuple[str, ...]
    impacted_scenarios: tuple[str, ...]
    carry_forward_scenarios: tuple[str, ...]
    unknown_paths: tuple[str, ...]

    @property
    def safe(self) -> bool:
        return not self.unknown_paths

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": PLAN_SCHEMA,
            "changed_paths": list(self.changed_paths),
            "impacted_scenarios": list(self.impacted_scenarios),
            "carry_forward_scenarios": list(self.carry_forward_scenarios),
            "unknown_paths": list(self.unknown_paths),
            "safe": self.safe,
        }


def _normalize_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _matches(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _parse_impact_map_text(text: str) -> ImpactMap:
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            "impact map could not be read as UTF-8 JSON",
        ) from exc
    if not isinstance(raw, dict) or raw.get("schema") != IMPACT_MAP_SCHEMA:
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            f"impact map must use schema {IMPACT_MAP_SCHEMA}",
        )
    expected = set(REQUIRED_SCENARIOS)
    declared = raw.get("all_scenarios")
    if not isinstance(declared, list) or set(declared) != expected:
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            "all_scenarios must match the required CF7 physical scenario set",
        )
    no_impact = raw.get("no_physical_impact")
    global_invalidation = raw.get("global_invalidation")
    rules_value = raw.get("rules")
    if (
        not isinstance(no_impact, list)
        or not all(isinstance(item, str) and item for item in no_impact)
        or not isinstance(global_invalidation, list)
        or not all(isinstance(item, str) and item for item in global_invalidation)
        or not isinstance(rules_value, list)
    ):
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            "impact-map pattern collections are malformed",
        )
    rules: list[tuple[tuple[str, ...], frozenset[str]]] = []
    for index, value in enumerate(rules_value):
        if not isinstance(value, dict) or set(value) != {"patterns", "scenarios"}:
            raise PhysicalRevalidationError(
                "invalid-impact-map",
                f"rules[{index}] must contain only patterns and scenarios",
            )
        patterns = value["patterns"]
        scenarios = value["scenarios"]
        if (
            not isinstance(patterns, list)
            or not patterns
            or not all(isinstance(item, str) and item for item in patterns)
            or not isinstance(scenarios, list)
            or not scenarios
            or not all(isinstance(item, str) and item in expected for item in scenarios)
        ):
            raise PhysicalRevalidationError(
                "invalid-impact-map",
                f"rules[{index}] contains invalid patterns or scenarios",
            )
        rules.append((tuple(patterns), frozenset(scenarios)))
    return ImpactMap(
        no_physical_impact=tuple(no_impact),
        global_invalidation=tuple(global_invalidation),
        rules=tuple(rules),
    )


def load_impact_map(path: Path = IMPACT_MAP_PATH) -> ImpactMap:
    try:
        value = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            "impact map could not be read as UTF-8 JSON",
        ) from exc
    return _parse_impact_map_text(value)


def classify_changed_paths(
    changed_paths: Iterable[str],
    *,
    impact_map: ImpactMap | None = None,
) -> ImpactReport:
    """Classify changed paths and fail closed for any unknown product path."""

    rules = impact_map or load_impact_map()
    all_scenarios = set(REQUIRED_SCENARIOS)
    impacted: set[str] = set()
    unknown: list[str] = []
    normalized = tuple(
        sorted({_normalize_path(path) for path in changed_paths if str(path).strip()})
    )

    for path in normalized:
        if _matches(path, rules.no_physical_impact):
            continue
        if _matches(path, rules.global_invalidation):
            impacted.update(all_scenarios)
            continue
        matched = False
        for patterns, scenarios in rules.rules:
            if _matches(path, patterns):
                impacted.update(scenarios)
                matched = True
        if not matched:
            unknown.append(path)

    if unknown:
        impacted.update(all_scenarios)

    ordered_impacted = tuple(
        scenario for scenario in REQUIRED_SCENARIOS if scenario in impacted
    )
    carry_forward = tuple(
        scenario for scenario in REQUIRED_SCENARIOS if scenario not in impacted
    )
    return ImpactReport(
        changed_paths=normalized,
        impacted_scenarios=ordered_impacted,
        carry_forward_scenarios=carry_forward,
        unknown_paths=tuple(unknown),
    )


def _git(
    repo_root: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            check=check,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, UnicodeError, subprocess.SubprocessError) as exc:
        raise PhysicalRevalidationError(
            "git-revalidation-failed",
            "git could not evaluate physical-evidence provenance",
        ) from exc


def _require_commit(value: str, field: str) -> str:
    normalized = value.strip().casefold()
    if len(normalized) != 40 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise PhysicalRevalidationError(
            "invalid-revalidation-commit",
            f"{field} must be one exact lowercase 40-character Git commit SHA",
        )
    return normalized


def load_impact_map_from_commit(
    repo_root: Path,
    candidate_commit: str,
) -> ImpactMap:
    """Read policy from the exact candidate tree being classified."""

    candidate = _require_commit(candidate_commit, "candidate_commit")
    result = _git(
        repo_root,
        "show",
        f"{candidate}:{IMPACT_MAP_REPOSITORY_PATH}",
        check=False,
    )
    if result.returncode != 0:
        raise PhysicalRevalidationError(
            "invalid-impact-map",
            "candidate commit does not contain the physical impact map",
        )
    return _parse_impact_map_text(result.stdout)


def changed_paths_between(
    repo_root: Path,
    baseline_commit: str,
    candidate_commit: str,
) -> tuple[str, ...]:
    baseline = _require_commit(baseline_commit, "baseline_commit")
    candidate = _require_commit(candidate_commit, "candidate_commit")
    ancestor = _git(
        repo_root,
        "merge-base",
        "--is-ancestor",
        baseline,
        candidate,
        check=False,
    )
    if ancestor.returncode != 0:
        raise PhysicalRevalidationError(
            "baseline-not-ancestor",
            "carried evidence must originate from an ancestor of the candidate",
        )
    diff = _git(
        repo_root,
        "diff",
        "--no-renames",
        "--name-only",
        "--diff-filter=ACMRDTUXB",
        f"{baseline}..{candidate}",
    )
    return tuple(line.strip() for line in diff.stdout.splitlines() if line.strip())


def analyze_commits(
    repo_root: Path,
    baseline_commit: str,
    candidate_commit: str,
    *,
    impact_map: ImpactMap | None = None,
) -> ImpactReport:
    changed_paths = changed_paths_between(
        repo_root,
        baseline_commit,
        candidate_commit,
    )
    resolved_impact_map = impact_map
    if resolved_impact_map is None:
        resolved_impact_map = load_impact_map_from_commit(
            repo_root,
            candidate_commit,
        )
    return classify_changed_paths(
        changed_paths,
        impact_map=resolved_impact_map,
    )


def scenario_can_carry_forward(
    repo_root: Path,
    observed_commit: str,
    candidate_commit: str,
    scenario_id: str,
    *,
    impact_map: ImpactMap | None = None,
) -> bool:
    if scenario_id not in REQUIRED_SCENARIOS:
        return False
    report = analyze_commits(
        repo_root,
        observed_commit,
        candidate_commit,
        impact_map=impact_map,
    )
    return report.safe and scenario_id in report.carry_forward_scenarios


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan fail-closed CF7 physical scenario revalidation.",
    )
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    try:
        report = analyze_commits(args.repo_root, args.baseline, args.candidate)
    except PhysicalRevalidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if report.safe else 2


if __name__ == "__main__":
    raise SystemExit(main())

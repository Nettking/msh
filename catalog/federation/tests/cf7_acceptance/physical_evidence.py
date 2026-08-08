"""Validate operator-produced CF7-B physical acceptance evidence.

This module is an acceptance tool, not a runtime authority. It validates a
bounded, redacted document produced after tests on real machines. CI may test
this validator and its template, but CI cannot create accepted physical
evidence on behalf of an operator.

Version 2 keeps per-environment and per-scenario provenance. Physical evidence
may be carried forward from an ancestor commit only when the fail-closed impact
analyzer proves that the corresponding scenario is unaffected by every changed
path. Unknown paths, non-ancestor provenance, or unavailable impact analysis
require a physical rerun.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

SCHEMA = "msh.cf7.physical-evidence.v2"
MAX_DOCUMENT_BYTES = 262_144
MAX_TEXT_LENGTH = 512

REQUIRED_ENVIRONMENTS = ("windows", "linux")
REQUIRED_SCENARIOS = (
    "physical.fresh-windows-checkout",
    "physical.fresh-linux-checkout",
    "physical.multi-host-relay-and-network-path",
    "physical.mtconnect-recorder-source",
    "physical.ollama-model-and-accelerator",
    "physical.mobile-and-desktop-browser-review",
    "physical.recorder-plus-ai-simultaneous-contribution",
    "physical.separate-ai-compute-storage-contributors",
    "physical.benchmark-expiry-and-rerun",
    "physical.contribution-disable-and-reenable",
    "physical.restart-and-reconciliation",
    "physical.revocation-and-controlled-rejoin",
)

PROVENANCE_OBSERVED = "observed"
PROVENANCE_CARRIED_FORWARD = "carried-forward"
CarryForwardCheck = Callable[[str, str, str], bool]

_REQUIRED_PRIVACY_CONFIRMATIONS = (
    "no_credentials",
    "no_private_endpoints",
    "no_private_database_paths",
    "no_reusable_enrollment_material",
    "operator_reviewed",
)

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_IPV4_RE = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
_WINDOWS_ABSOLUTE_RE = re.compile(r"(?i)(?:^|\s)[a-z]:[\\/]")
_FORBIDDEN_FRAGMENTS = (
    "authorization:",
    "bearer ",
    "private_key",
    "private-key",
    "invitation_token",
    "enrollment_token",
    "identity.pem",
    "server_settings.json",
    "sqlite://",
    "postgresql://",
    "mongodb://",
    "redis://",
    "/home/",
    "/users/",
    "\\users\\",
)


class PhysicalEvidenceError(ValueError):
    """The physical evidence document is incomplete, unsafe, or malformed."""

    def __init__(self, code: str, field: str, message: str) -> None:
        super().__init__(f"{code}: {field}: {message}")
        self.code = code
        self.field = field
        self.message = message


def _require_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-object",
            field,
            "must be an object",
        )
    return value


def _require_exact_keys(
    value: Mapping[str, Any],
    *,
    expected: Sequence[str],
    field: str,
) -> None:
    actual = set(value)
    required = set(expected)
    if actual != required:
        missing = sorted(required - actual)
        unexpected = sorted(actual - required)
        details: list[str] = []
        if missing:
            details.append(f"missing {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected {', '.join(unexpected)}")
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-fields",
            field,
            "; ".join(details),
        )


def _require_text(value: object, field: str, *, max_length: int = MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str):
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-text",
            field,
            "must be text",
        )
    normalized = value.strip()
    if not normalized or len(normalized) > max_length:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-text",
            field,
            f"must contain 1-{max_length} characters",
        )
    if any(ord(character) < 32 for character in normalized):
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-text",
            field,
            "must contain printable text only",
        )
    return normalized


def _require_commit(value: object, field: str) -> str:
    commit = _require_text(value, field, max_length=40).casefold()
    if _COMMIT_RE.fullmatch(commit) is None:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-commit",
            field,
            "must be one exact lowercase 40-character Git commit SHA",
        )
    return commit


def _require_utc(value: object, field: str) -> datetime:
    text = _require_text(value, field, max_length=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-time",
            field,
            "must be an ISO-8601 timestamp",
        ) from exc
    if parsed.tzinfo is None:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-time",
            field,
            "must include a timezone",
        )
    utc = parsed.astimezone(timezone.utc)
    if utc.utcoffset() != timezone.utc.utcoffset(utc):
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-time",
            field,
            "must normalize to UTC",
        )
    return utc


def _require_true(value: object, field: str) -> None:
    if value is not True:
        raise PhysicalEvidenceError(
            "incomplete-physical-evidence",
            field,
            "must be true after the operator has verified it",
        )


def _require_relative_evidence_path(value: object, field: str) -> str:
    text = _require_text(value, field, max_length=256).replace("\\", "/")
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts:
        raise PhysicalEvidenceError(
            "unsafe-physical-evidence-path",
            field,
            "must be a relative path without parent traversal",
        )
    if not path.parts or path.parts[0] != "evidence":
        raise PhysicalEvidenceError(
            "unsafe-physical-evidence-path",
            field,
            "must be stored below the local evidence/ directory",
        )
    return path.as_posix()


def _walk_strings(value: object, field: str = "document"):
    if isinstance(value, str):
        yield field, value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _walk_strings(item, f"{field}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            yield from _walk_strings(item, f"{field}[{index}]")


def _reject_private_data(payload: Mapping[str, Any]) -> None:
    for field, text in _walk_strings(payload):
        lowered = text.casefold()
        if any(fragment in lowered for fragment in _FORBIDDEN_FRAGMENTS):
            raise PhysicalEvidenceError(
                "private-physical-evidence",
                field,
                "contains a forbidden credential, endpoint, or local-path fragment",
            )
        if _IPV4_RE.search(text):
            raise PhysicalEvidenceError(
                "private-physical-evidence",
                field,
                "must not contain raw IPv4 addresses",
            )
        if _WINDOWS_ABSOLUTE_RE.search(text):
            raise PhysicalEvidenceError(
                "private-physical-evidence",
                field,
                "must not contain absolute Windows paths",
            )
        if "://" in text:
            raise PhysicalEvidenceError(
                "private-physical-evidence",
                field,
                "must not contain raw URLs or service endpoints",
            )


def _validate_provenance(
    *,
    value: object,
    observed_commit_value: object,
    candidate_commit: str,
    scenario_id: str,
    field: str,
    carry_forward_check: CarryForwardCheck | None,
) -> tuple[str, str]:
    provenance = _require_text(value, f"{field}.provenance").casefold()
    if provenance not in {PROVENANCE_OBSERVED, PROVENANCE_CARRIED_FORWARD}:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-provenance",
            f"{field}.provenance",
            "must be observed or carried-forward",
        )
    observed_commit = _require_commit(
        observed_commit_value,
        f"{field}.observed_commit",
    )
    if provenance == PROVENANCE_OBSERVED:
        if observed_commit != candidate_commit:
            raise PhysicalEvidenceError(
                "physical-evidence-observation-commit-mismatch",
                f"{field}.observed_commit",
                "observed evidence must be produced on the accepted candidate",
            )
        return provenance, observed_commit

    if observed_commit == candidate_commit:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-provenance",
            f"{field}.provenance",
            "same-commit evidence must be marked observed",
        )
    if carry_forward_check is None:
        raise PhysicalEvidenceError(
            "physical-evidence-revalidation-required",
            field,
            "carried-forward evidence requires fail-closed impact analysis",
        )
    try:
        allowed = carry_forward_check(
            observed_commit,
            candidate_commit,
            scenario_id,
        )
    except Exception as exc:
        raise PhysicalEvidenceError(
            "physical-evidence-revalidation-failed",
            field,
            "impact analysis could not authorize carry-forward",
        ) from exc
    if allowed is not True:
        raise PhysicalEvidenceError(
            "physical-evidence-rerun-required",
            field,
            "the candidate changed code that can affect this physical observation",
        )
    return provenance, observed_commit


def _validate_environment(
    name: str,
    value: object,
    *,
    candidate_commit: str,
    carry_forward_check: CarryForwardCheck | None,
) -> dict[str, str | bool]:
    field = f"environments.{name}"
    environment = _require_mapping(value, field)
    _require_exact_keys(
        environment,
        expected=(
            "os_family",
            "fresh_checkout",
            "repository_clean",
            "result",
            "command_log",
            "provenance",
            "observed_commit",
        ),
        field=field,
    )
    os_family = _require_text(environment["os_family"], f"{field}.os_family").casefold()
    if os_family != name:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-environment",
            f"{field}.os_family",
            f"must be {name}",
        )
    _require_true(environment["fresh_checkout"], f"{field}.fresh_checkout")
    _require_true(environment["repository_clean"], f"{field}.repository_clean")
    result = _require_text(environment["result"], f"{field}.result").casefold()
    if result != "passed":
        raise PhysicalEvidenceError(
            "incomplete-physical-evidence",
            f"{field}.result",
            "must be passed",
        )
    scenario_id = f"physical.fresh-{name}-checkout"
    provenance, observed_commit = _validate_provenance(
        value=environment["provenance"],
        observed_commit_value=environment["observed_commit"],
        candidate_commit=candidate_commit,
        scenario_id=scenario_id,
        field=field,
        carry_forward_check=carry_forward_check,
    )
    return {
        "os_family": os_family,
        "fresh_checkout": True,
        "repository_clean": True,
        "result": result,
        "command_log": _require_relative_evidence_path(
            environment["command_log"],
            f"{field}.command_log",
        ),
        "provenance": provenance,
        "observed_commit": observed_commit,
    }


def _validate_scenario(
    scenario_id: str,
    value: object,
    *,
    candidate_commit: str,
    carry_forward_check: CarryForwardCheck | None,
) -> dict[str, object]:
    field = f"scenarios.{scenario_id}"
    scenario = _require_mapping(value, field)
    _require_exact_keys(
        scenario,
        expected=(
            "status",
            "provenance",
            "observed_commit",
            "evidence_refs",
            "notes",
        ),
        field=field,
    )
    status = _require_text(scenario["status"], f"{field}.status").casefold()
    if status != "passed":
        raise PhysicalEvidenceError(
            "incomplete-physical-evidence",
            f"{field}.status",
            "must be passed",
        )
    provenance, observed_commit = _validate_provenance(
        value=scenario["provenance"],
        observed_commit_value=scenario["observed_commit"],
        candidate_commit=candidate_commit,
        scenario_id=scenario_id,
        field=field,
        carry_forward_check=carry_forward_check,
    )
    refs = scenario["evidence_refs"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 8:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-refs",
            f"{field}.evidence_refs",
            "must contain 1-8 relative evidence paths",
        )
    normalized_refs = [
        _require_relative_evidence_path(item, f"{field}.evidence_refs[{index}]")
        for index, item in enumerate(refs)
    ]
    notes = _require_text(scenario["notes"], f"{field}.notes", max_length=1_024)
    return {
        "status": status,
        "provenance": provenance,
        "observed_commit": observed_commit,
        "evidence_refs": normalized_refs,
        "notes": notes,
    }


def validate_physical_evidence(
    payload: object,
    *,
    expected_commit: str | None = None,
    carry_forward_check: CarryForwardCheck | None = None,
) -> dict[str, object]:
    """Return a safe acceptance summary or raise ``PhysicalEvidenceError``."""

    document = _require_mapping(payload, "document")
    _require_exact_keys(
        document,
        expected=(
            "schema",
            "commit_sha",
            "executed_at",
            "operator",
            "environments",
            "scenarios",
            "privacy_review",
            "notes",
        ),
        field="document",
    )
    if document["schema"] != SCHEMA:
        raise PhysicalEvidenceError(
            "unsupported-physical-evidence-schema",
            "schema",
            f"must be {SCHEMA}",
        )
    commit_sha = _require_commit(document["commit_sha"], "commit_sha")
    if expected_commit is not None and commit_sha != _require_commit(
        expected_commit,
        "expected_commit",
    ):
        raise PhysicalEvidenceError(
            "physical-evidence-commit-mismatch",
            "commit_sha",
            "does not match the commit being accepted",
        )
    executed_at = _require_utc(document["executed_at"], "executed_at")
    operator = _require_text(document["operator"], "operator", max_length=128)

    environments = _require_mapping(document["environments"], "environments")
    _require_exact_keys(
        environments,
        expected=REQUIRED_ENVIRONMENTS,
        field="environments",
    )
    normalized_environments = {
        name: _validate_environment(
            name,
            environments[name],
            candidate_commit=commit_sha,
            carry_forward_check=carry_forward_check,
        )
        for name in REQUIRED_ENVIRONMENTS
    }

    scenarios = _require_mapping(document["scenarios"], "scenarios")
    _require_exact_keys(
        scenarios,
        expected=REQUIRED_SCENARIOS,
        field="scenarios",
    )
    normalized_scenarios = {
        scenario_id: _validate_scenario(
            scenario_id,
            scenarios[scenario_id],
            candidate_commit=commit_sha,
            carry_forward_check=carry_forward_check,
        )
        for scenario_id in REQUIRED_SCENARIOS
    }

    privacy_review = _require_mapping(document["privacy_review"], "privacy_review")
    _require_exact_keys(
        privacy_review,
        expected=_REQUIRED_PRIVACY_CONFIRMATIONS,
        field="privacy_review",
    )
    for key in _REQUIRED_PRIVACY_CONFIRMATIONS:
        _require_true(privacy_review[key], f"privacy_review.{key}")

    notes_value = document["notes"]
    if not isinstance(notes_value, list) or len(notes_value) > 20:
        raise PhysicalEvidenceError(
            "invalid-physical-evidence-notes",
            "notes",
            "must be a list with at most 20 safe notes",
        )
    notes = [
        _require_text(item, f"notes[{index}]", max_length=1_024)
        for index, item in enumerate(notes_value)
    ]

    normalized = {
        "schema": SCHEMA,
        "commit_sha": commit_sha,
        "executed_at": executed_at.isoformat().replace("+00:00", "Z"),
        "operator": operator,
        "environments": normalized_environments,
        "scenarios": normalized_scenarios,
        "privacy_review": {key: True for key in _REQUIRED_PRIVACY_CONFIRMATIONS},
        "notes": notes,
    }
    _reject_private_data(normalized)
    carried_count = sum(
        item["provenance"] == PROVENANCE_CARRIED_FORWARD
        for item in normalized_environments.values()
    ) + sum(
        item["provenance"] == PROVENANCE_CARRIED_FORWARD
        for item in normalized_scenarios.values()
    )
    observed_count = len(normalized_environments) + len(normalized_scenarios) - carried_count
    return {
        "schema": SCHEMA,
        "commit_sha": commit_sha,
        "executed_at": normalized["executed_at"],
        "environment_count": len(normalized_environments),
        "scenario_count": len(normalized_scenarios),
        "observed_record_count": observed_count,
        "carried_forward_record_count": carried_count,
        "accepted": True,
    }


def load_physical_evidence(path: Path) -> object:
    raw = path.read_bytes()
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise PhysicalEvidenceError(
            "physical-evidence-too-large",
            "document",
            f"must not exceed {MAX_DOCUMENT_BYTES} bytes",
        )
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PhysicalEvidenceError(
            "malformed-physical-evidence",
            "document",
            "must be valid UTF-8 JSON",
        ) from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate redacted CF7-B physical acceptance evidence.",
    )
    parser.add_argument("evidence", type=Path)
    parser.add_argument(
        "--commit",
        dest="expected_commit",
        help="Require evidence to target this exact 40-character Git commit SHA.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path("."),
        help="Repository used for fail-closed carry-forward impact analysis.",
    )
    args = parser.parse_args(argv)

    def carry_forward_check(
        observed_commit: str,
        candidate_commit: str,
        scenario_id: str,
    ) -> bool:
        from .physical_revalidation import scenario_can_carry_forward

        return scenario_can_carry_forward(
            args.repo_root,
            observed_commit,
            candidate_commit,
            scenario_id,
        )

    try:
        summary = validate_physical_evidence(
            load_physical_evidence(args.evidence),
            expected_commit=args.expected_commit,
            carry_forward_check=carry_forward_check,
        )
    except (OSError, PhysicalEvidenceError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

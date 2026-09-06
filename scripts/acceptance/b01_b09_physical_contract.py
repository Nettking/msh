"""Fail-closed supplemental contract for the Federation v1 B01-B09 campaign.

P01-P12 describe robustness properties of one installation.  B01-B09 are the
cross-host physical acceptance cases that prove the installed federation was
actually exercised.  This module validates operator-supplied, redacted
observation packets; it performs no physical action and never turns a missing
observation into a pass.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from scripts.acceptance.v1_physical_campaign import redact_text, require_commit

SCHEMA: Final = "fcp.v1.b01-b09-physical-contract.v1"
CASE_IDS: Final[tuple[str, ...]] = tuple(f"B{i:02d}" for i in range(1, 10))
STATUSES: Final[frozenset[str]] = frozenset({"pass", "fail", "blocked", "unresolved"})
SHA_RE: Final = re.compile(r"[0-9a-f]{40}")
HOST_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}")
PRIVATE_RE: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"\b(?:https?|wss?)://", re.IGNORECASE),
    re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])"),
    re.compile(r"(?i)\b[a-z]:[\\/][^\r\n\t\"']+"),
)

CASE_CONTRACT: Final[dict[str, dict[str, object]]] = {
    "B01": {
        "title": "dual-recorder admission",
        "p_ids": ["P01", "P02", "P05"],
        "cf7_ids": ["CF7-B", "CF7-C"],
        "required": ["recorder_ids", "admission_observed"],
    },
    "B02": {
        "title": "collision-free recorder identity",
        "p_ids": ["P01", "P02", "P06"],
        "cf7_ids": ["CF7-B"],
        "required": ["recorder_ids", "identities_unique"],
    },
    "B03": {
        "title": "independent recorder-control targeting",
        "p_ids": ["P01", "P03", "P05", "P06"],
        "cf7_ids": ["CF7-B", "CF7-C"],
        "required": [
            "target_node_id",
            "request_target",
            "execution_node_id",
            "report_actor",
            "expected_report_actor",
            "report_target_node_id",
            "corresponding_report_count",
            "nonpublic_payload",
            "nonpublic_property",
            "private_address_leakage",
        ],
    },
    "B04": {
        "title": "MSH Recorder restart",
        "p_ids": ["P04", "P05", "P06"],
        "cf7_ids": ["CF7-B"],
        "required": ["restart_observed", "rejoined"],
    },
    "B05": {
        "title": "Nitro recorder restart",
        "p_ids": ["P04", "P05", "P06"],
        "cf7_ids": ["CF7-B"],
        "required": ["restart_observed", "rejoined"],
    },
    "B06": {
        "title": "coordinator restart/replay",
        "p_ids": ["P05", "P06", "P08"],
        "cf7_ids": ["CF7-C"],
        "required": ["restart_observed", "replay_converged"],
    },
    "B07": {
        "title": "legacy recorder-local convergence",
        "p_ids": ["P06", "P08", "P09"],
        "cf7_ids": ["CF7-B"],
        "required": ["legacy_state_present", "converged"],
    },
    "B08": {
        "title": "stale-state convergence",
        "p_ids": ["P06", "P08", "P10"],
        "cf7_ids": ["CF7-B", "CF7-C"],
        "required": ["stale_state_present", "converged"],
    },
    "B09": {
        "title": "isolated old-SHA negative control",
        "p_ids": ["P01", "P05", "P06"],
        "cf7_ids": ["CF7-B", "CF7-C"],
        "required": ["old_candidate_sha", "isolation_confirmed", "contact_observed"],
    },
}


class ContractError(ValueError):
    """A B01-B09 packet is incomplete, stale, mixed, or unsafe to trust."""


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be non-empty text")
    return value.strip()


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ContractError(f"{field} must be boolean")
    return value


def _sha(value: object, field: str) -> str:
    result = _text(value, field).lower()
    if SHA_RE.fullmatch(result) is None:
        raise ContractError(f"{field} must be a 40-character SHA")
    return result


def _host(value: object, field: str = "host_id") -> str:
    result = _text(value, field)
    if HOST_RE.fullmatch(result) is None:
        raise ContractError(f"{field} is not a safe host alias")
    return result


def _timestamp(value: object) -> datetime:
    text = _text(value, "observed_at")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("observed_at must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ContractError("observed_at must include a timezone")
    return parsed


def _private_material(value: object, *, key: str = "") -> str | None:
    if isinstance(value, Mapping):
        for name, item in value.items():
            found = _private_material(item, key=str(name))
            if found:
                return found
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            found = _private_material(item, key=key)
            if found:
                return found
        return None
    if isinstance(value, str) and any(pattern.search(value) for pattern in PRIVATE_RE):
        return key or "string"
    return None


def _require_verification(packet: Mapping[str, object], expected_candidate: str) -> None:
    verification = packet.get("verification")
    if not isinstance(verification, Mapping):
        raise ContractError("a packet cannot pass without verification provenance")
    if _sha(verification.get("candidate_sha"), "verification.candidate_sha") != expected_candidate:
        raise ContractError("verification candidate SHA differs from packet candidate")
    _host(verification.get("host_id"), "verification.host_id")
    _timestamp(verification.get("verified_at"))
    digest = _text(verification.get("evidence_sha256"), "verification.evidence_sha256").lower()
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ContractError("verification.evidence_sha256 must be a SHA-256 digest")
    if verification.get("redacted") is not True:
        raise ContractError("verification must explicitly attest redaction")
    if verification.get("source") != "physical-observation":
        raise ContractError("verification source must be physical-observation")


def _require_b03(packet: Mapping[str, object]) -> None:
    for field in CASE_CONTRACT["B03"]["required"]:
        if field not in packet:
            raise ContractError(f"B03 is missing {field}")
    target = _text(packet.get("target_node_id"), "target_node_id")
    if _text(packet.get("request_target"), "request_target") != target:
        raise ContractError("B03 request targeted a different node")
    if _text(packet.get("report_target_node_id"), "report_target_node_id") != target:
        raise ContractError("B03 report targeted a different node")
    if _text(packet.get("execution_node_id"), "execution_node_id") != target:
        raise ContractError("B03 executed on a different node")
    if _text(packet.get("report_actor"), "report_actor") != _text(
        packet.get("expected_report_actor"), "expected_report_actor"
    ):
        raise ContractError("B03 report actor does not match expected actor")
    if packet.get("corresponding_report_count") != 1:
        raise ContractError("B03 requires exactly one corresponding report")
    for field in ("nonpublic_payload", "nonpublic_property", "private_address_leakage"):
        if _bool(packet.get(field), field):
            raise ContractError(f"B03 recorded a prohibited {field}")


def _require_b09(packet: Mapping[str, object], expected_candidate: str) -> None:
    old = _sha(packet.get("old_candidate_sha"), "old_candidate_sha")
    if old == expected_candidate:
        raise ContractError("B09 old candidate must differ from the target candidate")
    if not _bool(packet.get("isolation_confirmed"), "isolation_confirmed"):
        raise ContractError("B09 requires explicit isolation confirmation")
    if _bool(packet.get("contact_observed"), "contact_observed"):
        raise ContractError("B09 observed contact with the isolated old candidate")


def validate_packet(
    packet: Mapping[str, object],
    *,
    expected_candidate: str,
    expected_hosts: set[str] | None = None,
    now: datetime | None = None,
    max_age_seconds: int = 24 * 60 * 60,
) -> dict[str, object]:
    """Validate one operator packet, returning only safe contract metadata."""

    candidate = require_commit(expected_candidate)
    if packet.get("schema") != SCHEMA:
        raise ContractError("packet schema mismatch")
    case_id = _text(packet.get("case_id"), "case_id").upper()
    if case_id not in CASE_CONTRACT:
        raise ContractError(f"unsupported physical case: {case_id}")
    packet_candidate = _sha(packet.get("candidate_sha"), "candidate_sha")
    if packet_candidate != candidate:
        raise ContractError("packet targets a different candidate")
    host = _host(packet.get("host_id"))
    if expected_hosts is not None and host not in expected_hosts:
        raise ContractError("packet host is outside the registered acceptance set")
    observed_at = _timestamp(packet.get("observed_at"))
    current = now or datetime.now(timezone.utc)
    age = (current - observed_at).total_seconds()
    if age < -300 or age > max_age_seconds:
        raise ContractError("packet timestamp is outside the allowed observation window")
    status = _text(packet.get("status"), "status").casefold()
    if status not in STATUSES:
        raise ContractError("unsupported B01-B09 status")
    required = CASE_CONTRACT[case_id]["required"]
    for field in required:
        if field not in packet:
            raise ContractError(f"{case_id} is missing {field}")
    if _private_material(packet) is not None:
        raise ContractError("packet contains private endpoint, address, or path material")
    if status == "pass":
        _require_verification(packet, candidate)
        if case_id == "B03":
            _require_b03(packet)
        if case_id == "B09":
            _require_b09(packet, candidate)
    return {
        "schema": SCHEMA,
        "case_id": case_id,
        "candidate_sha": candidate,
        "host_id": host,
        "status": status,
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
        "p_ids": list(CASE_CONTRACT[case_id]["p_ids"]),
        "cf7_ids": list(CASE_CONTRACT[case_id]["cf7_ids"]),
    }


def validate_campaign(
    packets: Sequence[Mapping[str, object]],
    *,
    expected_candidate: str,
    expected_hosts: set[str],
    now: datetime | None = None,
) -> dict[str, object]:
    """Validate a packet set and reject duplicates, mixed candidates, or holes."""

    seen: set[tuple[str, str]] = set()
    safe: list[dict[str, object]] = []
    for packet in packets:
        item = validate_packet(
            packet,
            expected_candidate=expected_candidate,
            expected_hosts=expected_hosts,
            now=now,
        )
        key = (str(item["case_id"]), str(item["host_id"]))
        if key in seen:
            raise ContractError(f"duplicate packet for {key[0]} on {key[1]}")
        seen.add(key)
        safe.append(item)
    missing = [
        case_id
        for case_id in CASE_IDS
        if not any(item["case_id"] == case_id for item in safe)
    ]
    return {
        "schema": SCHEMA,
        "candidate_sha": require_commit(expected_candidate),
        "packet_count": len(safe),
        "cases_seen": sorted({str(item["case_id"]) for item in safe}),
        "missing_cases": missing,
        "complete": not missing,
    }


def write_packet(root: Path, packet: Mapping[str, object], *, expected_candidate: str) -> Path:
    """Validate then write a redacted packet under a local evidence root."""

    validate_packet(packet, expected_candidate=expected_candidate)
    case_id = str(packet["case_id"]).upper()
    host = _host(packet["host_id"])
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    safe_name = f"{stamp}-{case_id}-{host}.json"
    path = root / "b01-b09" / case_id / safe_name
    path.parent.mkdir(parents=True, exist_ok=True)
    # Re-serialize only the supplied redacted object after validation.  The
    # caller owns the human observation, while this function owns the gate.
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)
    return path


def redacted_note(value: object, *, cwd: Path) -> str:
    """Provide the same local-only redaction primitive as the P01-P12 harness."""

    return redact_text(value, cwd=cwd)

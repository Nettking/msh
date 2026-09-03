"""Commit-bound Federation v1 P01-P12 physical robustness evidence harness.

Physical fault actions remain deliberate operator actions. This module verifies
one exact clean candidate, records redacted command/observation packets, enforces
scenario/OS/duration requirements, and seals the final local evidence tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from scripts.acceptance.cf7_physical_readiness import (
    ReadinessError,
    sanitize_text,
    verify_checkout,
)
from scripts.acceptance.v1_physical_campaign_contract import SCENARIOS, ScenarioSpec

SCHEMA: Final = "fcp.v1.physical-campaign.v1"
PACKET_SCHEMA: Final = "fcp.v1.physical-observation.v1"
PRIVACY_SCHEMA: Final = "fcp.v1.physical-privacy.v1"
HOST_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}")
SCENARIO_RE = re.compile(r"P(?:0[1-9]|1[0-2])")
URL_RE = re.compile(r"\b(?:https?|wss?)://[^\s\]\[(){}<>\"']+", re.IGNORECASE)
IPV4_RE = re.compile(r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])")
WINDOWS_PATH_RE = re.compile(r"(?i)\b[a-z]:[\\/][^\r\n\t\"']+")
CREDENTIAL_RE = re.compile(
    r"(?i)\b(?:authorization|bearer|token|password|secret|private[_-]?key)\b"
    r"\s*[:=]\s*(?:bearer\s+)?[^\s,;\"']+"
)
PAIRING_RE = re.compile(r"\bFCP1-[A-Za-z0-9._~+/=-]{6,}", re.IGNORECASE)
PROFILE_RE = re.compile(r"[a-z][a-z0-9-]{0,31}")
PREPARE_ID_RE = re.compile(r"[0-9a-f]{32}")

#: Host profiles the campaign is allowed to reason about. ``unspecified`` is the
#: only value a host may carry without declaring one, and every probe that needs
#: a profile refuses it, so an undeclared host can never satisfy a
#: profile-restricted assertion by accident.
HOST_PROFILES: Final[frozenset[str]] = frozenset(
    {
        "unspecified",
        "local-ai",
        "cnc-recorder",
        "school-control",
    }
)

#: Packet kinds that carry preparation/operator context only. They may never
#: carry an assertion verdict, so preparation output cannot become a pass.
NON_ASSERTION_KINDS: Final[frozenset[str]] = frozenset(
    {"prepare", "operator-action"}
)
ASSERTION_STATUSES: Final[frozenset[str]] = frozenset(
    {"pass", "fail", "not-applicable"}
)
MAX_DETAIL_DEPTH: Final = 6
MAX_DETAIL_ITEMS: Final = 256


class CampaignError(RuntimeError):
    """The physical campaign cannot safely continue."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CampaignError("evidence contains an invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise CampaignError("evidence timestamps must include a timezone")
    return parsed


def require_commit(value: str) -> str:
    commit = value.strip().lower()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise CampaignError("candidate commit must be one lowercase 40-character SHA")
    return commit


def require_host(value: str) -> str:
    if HOST_RE.fullmatch(value) is None:
        raise CampaignError("host id must be a safe 1-48 character alias")
    return value


def require_scenario(value: str) -> str:
    scenario = value.upper()
    if SCENARIO_RE.fullmatch(scenario) is None or scenario not in SCENARIOS:
        raise CampaignError(f"unsupported scenario: {value}")
    return scenario


def os_category() -> str:
    return "windows" if platform.system().casefold() == "windows" else "posix"


def require_candidate_checkout(checkout: Path, commit: str) -> dict[str, object]:
    """Prove the checkout is the exact clean candidate, failing as a campaign error.

    ``verify_checkout`` raises the readiness error type. Every campaign entry
    point already fails closed on ``CampaignError``, so converting here keeps a
    wrong or dirty checkout a clean refusal rather than an unhandled traceback.
    """

    try:
        return verify_checkout(checkout, commit)
    except ReadinessError as exc:
        raise CampaignError(str(exc)) from exc


def redact_text(value: object, *, cwd: Path | None = None) -> str:
    """Redact one string with the campaign's own, stricter pattern set.

    ``sanitize_text`` already removes endpoints, addresses, local paths and
    credential-like values. The campaign privacy scan additionally refuses
    reusable ``FCP1-`` pairing material, so evidence this harness writes itself
    must be redacted to the same standard it is later sealed against. Anything
    the harness writes therefore passes its own privacy scan by construction;
    the scan stays the backstop for material copied in by hand.
    """

    text = sanitize_text(value, cwd=cwd)
    text = URL_RE.sub("<redacted-endpoint>", text)
    text = IPV4_RE.sub("<redacted-address>", text)
    text = WINDOWS_PATH_RE.sub("<redacted-path>", text)
    text = CREDENTIAL_RE.sub("<redacted-credential>", text)
    return PAIRING_RE.sub("<redacted-pairing-material>", text)


def require_profile(value: str) -> str:
    profile = str(value or "").strip().casefold()
    if PROFILE_RE.fullmatch(profile) is None or profile not in HOST_PROFILES:
        raise CampaignError(f"unsupported host profile: {value}")
    return profile


def require_prepare_id(value: str) -> str:
    prepare_id = str(value or "").strip().casefold()
    if PREPARE_ID_RE.fullmatch(prepare_id) is None:
        raise CampaignError("prepare id must be one 32-character hex token")
    return prepare_id


def sanitize_value(value: object, *, cwd: Path, _depth: int = 0) -> object:
    """Redact one JSON-shaped probe detail without trusting its producer.

    Probe results are structured rather than free text, so redaction has to
    reach every nested string and key. Unsupported types, unbounded fan-out and
    non-finite numbers are refused instead of being coerced, because a value the
    privacy scan cannot reason about must not reach the evidence tree.
    """

    if _depth > MAX_DETAIL_DEPTH:
        raise CampaignError("probe detail is nested too deeply")
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CampaignError("probe detail contains a non-finite number")
        return float(value)
    if isinstance(value, str):
        return redact_text(value, cwd=cwd)
    if isinstance(value, Mapping):
        if len(value) > MAX_DETAIL_ITEMS:
            raise CampaignError("probe detail has too many fields")
        redacted: dict[str, object] = {}
        for key, item in value.items():
            name = redact_text(str(key), cwd=cwd)
            if not name:
                raise CampaignError("probe detail contains an empty field name")
            redacted[name] = sanitize_value(item, cwd=cwd, _depth=_depth + 1)
        return redacted
    if isinstance(value, Sequence):
        if len(value) > MAX_DETAIL_ITEMS:
            raise CampaignError("probe detail has too many entries")
        return [
            sanitize_value(item, cwd=cwd, _depth=_depth + 1) for item in value
        ]
    raise CampaignError("probe detail contains an unsupported value type")


def sanitize_detail(
    value: object | None,
    *,
    cwd: Path,
) -> dict[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise CampaignError("probe detail must be an object")
    redacted = sanitize_value(value, cwd=cwd)
    assert isinstance(redacted, dict)
    return redacted


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CampaignError(f"cannot read campaign evidence: {path.name}") from exc
    if not isinstance(value, dict):
        raise CampaignError(f"campaign evidence is not an object: {path.name}")
    return value


def _display_path(path: Path, checkout: Path, root: Path) -> str:
    for base in (checkout, root.parent):
        try:
            return path.relative_to(base).as_posix()
        except ValueError:
            continue
    return path.name


def campaign_path(root: Path) -> Path:
    return root / "campaign.json"


def initialize(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    operator: str,
) -> dict[str, object]:
    expected = require_commit(commit)
    require_candidate_checkout(checkout, expected)
    operator_text = redact_text(operator.strip(), cwd=checkout)
    if not operator_text:
        raise CampaignError("operator must be non-empty")
    path = campaign_path(root)
    if path.exists():
        existing = _load_json(path)
        if existing.get("schema") != SCHEMA or existing.get("candidate_sha") != expected:
            raise CampaignError("existing campaign targets another schema/candidate")
        return existing
    document: dict[str, object] = {
        "schema": SCHEMA,
        "candidate_sha": expected,
        "operator": operator_text,
        "created_at": utc_now(),
        "scenario_ids": list(SCENARIOS),
        "portable_evidence": True,
        "accepted": False,
    }
    _write_json(path, document)
    return document


def load_campaign(
    checkout: Path,
    root: Path,
    commit: str,
) -> dict[str, object]:
    expected = require_commit(commit)
    require_candidate_checkout(checkout, expected)
    path = campaign_path(root)
    if not path.exists():
        raise CampaignError("campaign is not initialized; run init first")
    document = _load_json(path)
    if document.get("schema") != SCHEMA or document.get("candidate_sha") != expected:
        raise CampaignError("campaign schema/candidate does not match this checkout")
    return document


def host_fingerprint() -> str:
    material = f"{platform.node()}|{platform.system()}|{platform.machine()}".encode(
        "utf-8",
        errors="replace",
    )
    return hashlib.sha256(material).hexdigest()[:16]


def register_host(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    role: str,
    profile: str = "unspecified",
) -> dict[str, object]:
    load_campaign(checkout, root, commit)
    expected = require_commit(commit)
    host_id = require_host(host)
    host_profile = require_profile(profile)
    path = root / "hosts" / f"{host_id}.json"
    fingerprint = host_fingerprint()
    if path.exists():
        existing = _load_json(path)
        if existing.get("candidate_sha") != expected:
            raise CampaignError(f"host {host_id} belongs to a different candidate")
        if existing.get("host_fingerprint") != fingerprint:
            raise CampaignError(f"host alias {host_id} already identifies another machine")
        recorded = host_profile_of(existing)
        if recorded != host_profile:
            raise CampaignError(
                f"host {host_id} is already registered as profile {recorded}"
            )
        return existing
    record = {
        "schema": PACKET_SCHEMA,
        "kind": "host",
        "candidate_sha": expected,
        "host_id": host_id,
        "host_fingerprint": fingerprint,
        "os_family": platform.system().casefold() or "unknown",
        "os_category": os_category(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "role": redact_text(role, cwd=checkout),
        "profile": host_profile,
        "recorded_at": utc_now(),
    }
    _write_json(path, record)
    return record


def host_profile_of(record: Mapping[str, object]) -> str:
    """Return one registered host's declared profile, defaulting to unspecified.

    Host evidence recorded before profiles existed carries no field. Reading it
    as ``unspecified`` keeps that evidence valid while still refusing every
    profile-restricted probe on it.
    """

    value = record.get("profile")
    if value is None:
        return "unspecified"
    if not isinstance(value, str):
        raise CampaignError("host evidence has an invalid profile")
    return require_profile(value)


def load_host(
    root: Path,
    host: str,
    *,
    commit: str | None = None,
) -> dict[str, object]:
    host_id = require_host(host)
    path = root / "hosts" / f"{host_id}.json"
    if not path.exists():
        raise CampaignError(f"host {host_id} is not registered")
    record = _load_json(path)
    if record.get("schema") != PACKET_SCHEMA or record.get("kind") != "host":
        raise CampaignError(f"host {host_id} has invalid evidence schema")
    if commit is not None and record.get("candidate_sha") != require_commit(commit):
        raise CampaignError(f"host {host_id} evidence belongs to a different candidate")
    return record


def _packet_path(root: Path, scenario: str, kind: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return (
        root
        / "observations"
        / scenario
        / f"{stamp}-{kind}-{uuid.uuid4().hex[:8]}.json"
    )


def _reject_manufactured_verdict(packet: Mapping[str, object]) -> None:
    """Refuse a preparation/operator packet that carries an assertion verdict.

    PREPARE and OPERATOR ACTION evidence exists so a fault-injection case can be
    staged and attested. If either could carry ``assertion``/``status``, running
    the preparation step would silently satisfy the assertion it was only meant
    to set up. Both the write path and the read path refuse it, so hand-written
    or imported evidence cannot smuggle one in either.
    """

    kind = str(packet.get("kind", ""))
    if kind not in NON_ASSERTION_KINDS:
        return
    if "assertion" in packet or "status" in packet:
        raise CampaignError(
            f"{kind} evidence must not carry an assertion verdict"
        )


def write_packet(root: Path, packet: dict[str, object]) -> Path:
    scenario = require_scenario(str(packet["scenario"]))
    kind = str(packet.get("kind", "observation"))
    _reject_manufactured_verdict(packet)
    status = packet.get("status")
    if status is not None and status not in ASSERTION_STATUSES:
        raise CampaignError(f"unsupported observation status: {status}")
    path = _packet_path(root, scenario, kind)
    _write_json(path, packet)
    return path


def base_packet(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    kind: str,
) -> dict[str, object]:
    expected = require_commit(commit)
    host_record = load_host(root, host, commit=expected)
    return {
        "schema": PACKET_SCHEMA,
        "kind": kind,
        "candidate_sha": expected,
        "scenario": require_scenario(scenario),
        "host_id": host_record["host_id"],
        "host_fingerprint": host_record["host_fingerprint"],
        "os_category": host_record["os_category"],
        "recorded_at": utc_now(),
    }


def _assertion_contract(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
) -> tuple[str, ScenarioSpec]:
    scenario_id = require_scenario(scenario)
    spec = SCENARIOS[scenario_id]
    if assertion not in spec.assertions:
        raise CampaignError(f"unknown assertion for {scenario_id}: {assertion}")
    host_record = load_host(root, host, commit=commit)
    required_os = spec.assertion_os.get(assertion)
    if required_os is not None and host_record.get("os_category") != required_os:
        raise CampaignError(
            f"{scenario_id}/{assertion} must be observed on {required_os}"
        )
    return scenario_id, spec


def observe(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    status: str,
    note: str,
    detail: Mapping[str, object] | None = None,
    source: str = "operator",
) -> Path:
    load_campaign(checkout, root, commit)
    scenario_id, spec = _assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    if status not in ASSERTION_STATUSES:
        raise CampaignError(
            "observation status must be pass, fail, or not-applicable"
        )
    if status == "not-applicable" and assertion not in spec.allow_na:
        raise CampaignError(
            f"{scenario_id}/{assertion} cannot be marked not-applicable"
        )
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="assertion",
    )
    packet.update(
        {
            "assertion": assertion,
            "assertion_text": spec.assertions[assertion],
            "status": status,
            "note": redact_text(note, cwd=checkout),
            "source": redact_text(source, cwd=checkout) or "operator",
        }
    )
    redacted = sanitize_detail(detail, cwd=checkout)
    if redacted is not None:
        packet["detail"] = redacted
    return write_packet(root, packet)


def record_preparation(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    operator_action: str,
    detail: Mapping[str, object] | None = None,
) -> tuple[str, Path]:
    """Record staged state for one fault-injection assertion.

    The returned prepare id is the only handle a later VERIFY accepts, and this
    packet deliberately carries no verdict: preparation proves the case was
    staged on this host, never that the consequence was observed.
    """

    load_campaign(checkout, root, commit)
    scenario_id, spec = _assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    prepare_id = uuid.uuid4().hex
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="prepare",
    )
    packet.update(
        {
            "prepare_for": assertion,
            "prepare_id": prepare_id,
            "assertion_text": spec.assertions[assertion],
            "operator_action": redact_text(operator_action, cwd=checkout),
        }
    )
    redacted = sanitize_detail(detail, cwd=checkout)
    if redacted is not None:
        packet["detail"] = redacted
    return prepare_id, write_packet(root, packet)


def record_operator_action(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
    note: str,
) -> Path:
    """Attest that the operator performed the explicit physical fault action."""

    load_campaign(checkout, root, commit)
    scenario_id, _spec = _assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    identifier = require_prepare_id(prepare_id)
    preparation = find_preparation(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        assertion=assertion,
        prepare_id=identifier,
    )
    action_note = redact_text(note, cwd=checkout)
    if not action_note:
        raise CampaignError("operator action requires a description")
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="operator-action",
    )
    packet.update(
        {
            "prepare_for": assertion,
            "prepare_id": identifier,
            "prepared_at": preparation["recorded_at"],
            "operator_note": action_note,
        }
    )
    return write_packet(root, packet)


def _matching_context_packets(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
    kind: str,
) -> list[dict[str, object]]:
    host_record = load_host(root, host, commit=commit)
    packets = read_packets(root, scenario, expected_commit=commit)
    return [
        packet
        for packet in packets
        if packet.get("kind") == kind
        and packet.get("prepare_for") == assertion
        and packet.get("prepare_id") == prepare_id
        and packet.get("host_id") == host_record["host_id"]
        and packet.get("host_fingerprint") == host_record["host_fingerprint"]
    ]


def find_preparation(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
) -> dict[str, object]:
    matches = _matching_context_packets(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
        prepare_id=require_prepare_id(prepare_id),
        kind="prepare",
    )
    if len(matches) != 1:
        raise CampaignError(
            "no single matching preparation for this candidate, host and assertion"
        )
    return matches[0]


def find_operator_action(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str,
    prepare_id: str,
) -> dict[str, object]:
    matches = _matching_context_packets(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
        prepare_id=require_prepare_id(prepare_id),
        kind="operator-action",
    )
    if not matches:
        raise CampaignError(
            "the explicit operator fault action has not been recorded"
        )
    return max(matches, key=lambda item: str(item.get("recorded_at", "")))


def _run(
    command: list[str],
    *,
    checkout: Path,
    timeout: float,
) -> tuple[int, float, str]:
    if not command:
        raise CampaignError("run requires a command after --")
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=checkout,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        output = "\n".join(
            part for part in (completed.stdout, completed.stderr) if part
        )
        return (
            completed.returncode,
            round(time.monotonic() - started, 3),
            output,
        )
    except subprocess.TimeoutExpired as exc:
        parts: list[str] = []
        for item in (exc.stdout, exc.stderr):
            if isinstance(item, bytes):
                parts.append(item.decode("utf-8", errors="replace"))
            elif item:
                parts.append(item)
        return 124, round(time.monotonic() - started, 3), "\n".join(parts)


def run_command(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    assertion: str | None,
    label: str,
    command: list[str],
    expected_exit: int,
    timeout: float,
) -> Path:
    load_campaign(checkout, root, commit)
    scenario_id = require_scenario(scenario)
    if command and command[0] == "--":
        command = command[1:]
    spec = SCENARIOS[scenario_id]
    if assertion is not None:
        _assertion_contract(
            root,
            commit=commit,
            host=host,
            scenario=scenario_id,
            assertion=assertion,
        )
    else:
        load_host(root, host, commit=commit)
    returncode, duration, output = _run(
        command,
        checkout=checkout,
        timeout=timeout,
    )
    passed = returncode == expected_exit
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="command",
    )
    packet.update(
        {
            "label": redact_text(label, cwd=checkout),
            "command": redact_text(" ".join(command), cwd=checkout),
            "expected_exit": expected_exit,
            "returncode": returncode,
            "duration_seconds": duration,
            "output_tail": redact_text(output, cwd=checkout),
            "passed": passed,
        }
    )
    if assertion is not None:
        packet.update(
            {
                "assertion": assertion,
                "assertion_text": spec.assertions[assertion],
                "status": "pass" if passed else "fail",
            }
        )
    return write_packet(root, packet)


def _disk_snapshot(path: Path) -> dict[str, object]:
    usage = shutil.disk_usage(path)
    result: dict[str, object] = {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }
    if hasattr(os, "statvfs"):
        stat = os.statvfs(path)
        result["inode_total"] = stat.f_files
        result["inode_free"] = stat.f_ffree
    return result


def _validate_packet_host(
    root: Path,
    packet: dict[str, object],
    commit: str,
) -> None:
    host_id = packet.get("host_id")
    if not isinstance(host_id, str):
        raise CampaignError("observation is missing host provenance")
    host = load_host(root, host_id, commit=commit)
    if packet.get("host_fingerprint") != host.get("host_fingerprint"):
        raise CampaignError(
            "observation host fingerprint does not match registered host"
        )
    if packet.get("os_category") != host.get("os_category"):
        raise CampaignError(
            "observation OS provenance does not match registered host"
        )


def read_packets(
    root: Path,
    scenario: str | None = None,
    *,
    expected_commit: str | None = None,
) -> list[dict[str, object]]:
    base = root / "observations"
    paths = (
        sorted((base / scenario).glob("*.json"))
        if scenario
        else sorted(base.glob("*/*.json"))
    )
    packets: list[dict[str, object]] = []
    expected = require_commit(expected_commit) if expected_commit else None
    for path in paths:
        packet = _load_json(path)
        if packet.get("schema") != PACKET_SCHEMA:
            raise CampaignError(f"unexpected observation schema: {path.name}")
        packet_commit = packet.get("candidate_sha")
        if not isinstance(packet_commit, str):
            raise CampaignError(
                f"observation {path.name} is missing candidate identity"
            )
        packet_commit = require_commit(packet_commit)
        if expected is not None and packet_commit != expected:
            raise CampaignError(
                f"observation {path.name} belongs to a different candidate"
            )
        packet_scenario = packet.get("scenario")
        if (
            not isinstance(packet_scenario, str)
            or require_scenario(packet_scenario) != packet_scenario
        ):
            raise CampaignError(f"observation {path.name} has invalid scenario")
        _validate_packet_host(root, packet, packet_commit)
        _reject_manufactured_verdict(packet)
        status = packet.get("status")
        if status is not None and status not in ASSERTION_STATUSES:
            raise CampaignError(
                f"observation {path.name} has an unsupported status"
            )
        parse_time(str(packet.get("recorded_at", "")))
        packets.append(packet)
    return packets


def _active_session(
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str,
) -> dict[str, object]:
    host_record = load_host(root, host, commit=commit)
    packets = read_packets(root, scenario, expected_commit=commit)
    begins = [
        packet
        for packet in packets
        if packet.get("kind") == "begin" and packet.get("run_id") == run_id
    ]
    finishes = [
        packet
        for packet in packets
        if packet.get("kind") == "finish" and packet.get("run_id") == run_id
    ]
    if len(begins) != 1:
        raise CampaignError("sample/finish requires exactly one matching begin")
    if begins[0].get("host_fingerprint") != host_record.get("host_fingerprint"):
        raise CampaignError("timed session belongs to another host")
    if finishes:
        raise CampaignError("timed session has already been finished")
    return begins[0]


def sample_resources(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    label: str,
    run_id: str | None = None,
    extras: Mapping[str, object] | None = None,
) -> Path:
    load_campaign(checkout, root, commit)
    scenario_id = require_scenario(scenario)
    spec = SCENARIOS[scenario_id]
    if spec.minimum_elapsed_seconds and not run_id:
        raise CampaignError(
            f"{scenario_id} samples must include the active --run-id"
        )
    if run_id:
        _active_session(
            root,
            commit=commit,
            host=host,
            scenario=scenario_id,
            run_id=run_id,
        )
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="sample",
    )
    if run_id:
        packet["run_id"] = run_id
    resources: dict[str, object] = {"checkout": _disk_snapshot(checkout)}
    for name in ("data", "results"):
        path = checkout / name
        if path.exists():
            resources[name] = _disk_snapshot(path)
    docker: dict[str, object] = {
        "available": shutil.which("docker") is not None
    }
    if docker["available"]:
        code, duration, output = _run(
            ["docker", "system", "df"],
            checkout=checkout,
            timeout=30,
        )
        docker.update(
            {
                "returncode": code,
                "duration_seconds": duration,
                "summary": redact_text(output, cwd=checkout),
            }
        )
        code, duration, output = _run(
            ["docker", "compose", "ps"],
            checkout=checkout,
            timeout=30,
        )
        docker.update(
            {
                "compose_returncode": code,
                "compose_duration_seconds": duration,
                "compose_summary": redact_text(output, cwd=checkout),
            }
        )
    packet.update(
        {
            "label": redact_text(label, cwd=checkout),
            "resources": resources,
            "docker": docker,
        }
    )
    redacted = sanitize_detail(extras, cwd=checkout)
    if redacted is not None:
        packet["extras"] = redacted
    return write_packet(root, packet)


def begin_session(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
) -> tuple[str, Path]:
    load_campaign(checkout, root, commit)
    scenario_id = require_scenario(scenario)
    load_host(root, host, commit=commit)
    run_id = uuid.uuid4().hex
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="begin",
    )
    packet["run_id"] = run_id
    return run_id, write_packet(root, packet)


def finish_session(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    run_id: str,
) -> Path:
    load_campaign(checkout, root, commit)
    scenario_id = require_scenario(scenario)
    begin = _active_session(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        run_id=run_id,
    )
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="finish",
    )
    packet["run_id"] = run_id
    packet["started_at"] = begin["recorded_at"]
    packet["elapsed_seconds"] = round(
        (
            parse_time(str(packet["recorded_at"]))
            - parse_time(str(begin["recorded_at"]))
        ).total_seconds(),
        3,
    )
    return write_packet(root, packet)


def _timed_session_status(
    packets: list[dict[str, object]],
    spec: ScenarioSpec,
) -> tuple[float, int, list[dict[str, object]]]:
    if not spec.minimum_elapsed_seconds:
        return 0.0, 0, []
    begins = {
        str(packet.get("run_id")): packet
        for packet in packets
        if packet.get("kind") == "begin" and packet.get("run_id")
    }
    sessions: list[dict[str, object]] = []
    for finish in packets:
        if finish.get("kind") != "finish" or not finish.get("run_id"):
            continue
        run_id = str(finish["run_id"])
        begin = begins.get(run_id)
        if begin is None:
            raise CampaignError("timed finish has no matching begin")
        if finish.get("host_fingerprint") != begin.get("host_fingerprint"):
            raise CampaignError("timed begin/finish host provenance differs")
        started = parse_time(str(begin["recorded_at"]))
        ended = parse_time(str(finish["recorded_at"]))
        elapsed = (ended - started).total_seconds()
        if elapsed < 0:
            raise CampaignError("timed session finishes before it starts")
        samples = [
            packet
            for packet in packets
            if packet.get("kind") == "sample"
            and packet.get("run_id") == run_id
            and packet.get("host_fingerprint") == begin.get("host_fingerprint")
            and started <= parse_time(str(packet["recorded_at"])) <= ended
        ]
        sessions.append(
            {
                "run_id": run_id,
                "host_id": begin.get("host_id"),
                "elapsed_seconds": round(elapsed, 3),
                "sample_count": len(samples),
                "qualifies": (
                    elapsed >= spec.minimum_elapsed_seconds
                    and len(samples) >= spec.minimum_samples
                ),
            }
        )
    if not sessions:
        return 0.0, 0, []
    best = max(
        sessions,
        key=lambda item: (
            bool(item["qualifies"]),
            float(item["elapsed_seconds"]),
            int(item["sample_count"]),
        ),
    )
    return (
        float(best["elapsed_seconds"]),
        int(best["sample_count"]),
        sessions,
    )


def scenario_status(
    root: Path,
    scenario: str,
    *,
    expected_commit: str | None = None,
) -> dict[str, object]:
    scenario_id = require_scenario(scenario)
    spec = SCENARIOS[scenario_id]
    packets = read_packets(
        root,
        scenario_id,
        expected_commit=expected_commit,
    )
    latest: dict[str, dict[str, object]] = {}
    for packet in packets:
        assertion = packet.get("assertion")
        if isinstance(assertion, str) and assertion in spec.assertions:
            required_os = spec.assertion_os.get(assertion)
            if required_os is not None and packet.get("os_category") != required_os:
                raise CampaignError(
                    f"stored {scenario_id}/{assertion} evidence has wrong OS provenance"
                )
            current = latest.get(assertion)
            if current is None or str(packet.get("recorded_at", "")) > str(
                current.get("recorded_at", "")
            ):
                latest[assertion] = packet
    missing: list[str] = []
    failing: list[str] = []
    for assertion in spec.assertions:
        packet = latest.get(assertion)
        if packet is None:
            missing.append(assertion)
            continue
        status = packet.get("status")
        if status == "pass":
            continue
        if status == "not-applicable" and assertion in spec.allow_na:
            continue
        failing.append(assertion)
    elapsed, samples, sessions = _timed_session_status(packets, spec)
    duration_ok = elapsed >= spec.minimum_elapsed_seconds
    samples_ok = samples >= spec.minimum_samples
    passed = not missing and not failing and duration_ok and samples_ok
    return {
        "scenario": scenario_id,
        "title": spec.title,
        "passed": passed,
        "missing_assertions": missing,
        "failing_assertions": failing,
        "elapsed_seconds": elapsed,
        "required_elapsed_seconds": spec.minimum_elapsed_seconds,
        "sample_count": samples,
        "required_samples": spec.minimum_samples,
        "timed_sessions": sessions,
    }


def _privacy_digest(root: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "privacy.json":
            continue
        relative = path.relative_to(root).as_posix()
        data = path.read_bytes()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(data)
        digest.update(b"\0")
        count += 1
    return digest.hexdigest(), count


def _contains_private_material(text: str, *, checkout: Path) -> bool:
    for literal in {str(checkout), str(checkout.resolve()), str(Path.home())}:
        if literal and literal in text:
            return True
    return any(
        pattern.search(text) is not None
        for pattern in (
            URL_RE,
            IPV4_RE,
            WINDOWS_PATH_RE,
            CREDENTIAL_RE,
            PAIRING_RE,
        )
    )


def privacy_check(
    checkout: Path,
    root: Path,
    *,
    commit: str,
) -> dict[str, object]:
    load_campaign(checkout, root, commit)
    expected = require_commit(commit)
    read_packets(root, expected_commit=expected)
    for path in sorted((root / "hosts").glob("*.json")):
        host = _load_json(path)
        if host.get("candidate_sha") != expected:
            raise CampaignError(
                f"host {path.name} belongs to a different candidate"
            )
    unsafe: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "privacy.json":
            continue
        if path.suffix.casefold() not in {".json", ".txt", ".log"}:
            unsafe.append(path.relative_to(root).as_posix())
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if _contains_private_material(text, checkout=checkout):
            unsafe.append(path.relative_to(root).as_posix())
    if unsafe:
        raise CampaignError(
            "privacy scan found unredacted or unsupported evidence: "
            + ", ".join(unsafe[:10])
        )
    digest, count = _privacy_digest(root)
    result = {
        "schema": PRIVACY_SCHEMA,
        "candidate_sha": expected,
        "checked_at": utc_now(),
        "file_count": count,
        "evidence_sha256": digest,
        "passed": True,
    }
    _write_json(root / "privacy.json", result)
    return result


def validate_campaign(
    checkout: Path,
    root: Path,
    *,
    commit: str,
) -> dict[str, object]:
    campaign = load_campaign(checkout, root, commit)
    expected = require_commit(commit)
    hosts = [
        _load_json(path)
        for path in sorted((root / "hosts").glob("*.json"))
    ]
    for host in hosts:
        if (
            host.get("schema") != PACKET_SCHEMA
            or host.get("kind") != "host"
            or host.get("candidate_sha") != expected
        ):
            raise CampaignError("host evidence schema/candidate mismatch")
    categories = {str(item.get("os_category")) for item in hosts}
    profiles = sorted({host_profile_of(item) for item in hosts})
    host_coverage_ok = {"windows", "posix"}.issubset(categories)
    statuses = [
        scenario_status(root, scenario, expected_commit=expected)
        for scenario in SCENARIOS
    ]
    privacy_path = root / "privacy.json"
    privacy_ok = False
    if privacy_path.exists():
        privacy = _load_json(privacy_path)
        digest, count = _privacy_digest(root)
        privacy_ok = (
            privacy.get("schema") == PRIVACY_SCHEMA
            and privacy.get("candidate_sha") == expected
            and privacy.get("passed") is True
            and privacy.get("evidence_sha256") == digest
            and privacy.get("file_count") == count
        )
    accepted = (
        host_coverage_ok
        and privacy_ok
        and all(bool(item["passed"]) for item in statuses)
    )
    return {
        "schema": SCHEMA,
        "candidate_sha": campaign["candidate_sha"],
        "accepted": accepted,
        "host_os_categories": sorted(categories),
        "host_profiles": profiles,
        "host_coverage_ok": host_coverage_ok,
        "privacy_ok": privacy_ok,
        "scenarios": statuses,
    }


def plan(scenario: str | None = None) -> dict[str, object]:
    selected = [require_scenario(scenario)] if scenario else list(SCENARIOS)
    return {
        key: {
            "title": SCENARIOS[key].title,
            "assertions": SCENARIOS[key].assertions,
            "allow_not_applicable": sorted(SCENARIOS[key].allow_na),
            "assertion_os": SCENARIOS[key].assertion_os,
            "minimum_elapsed_seconds": SCENARIOS[key].minimum_elapsed_seconds,
            "minimum_samples": SCENARIOS[key].minimum_samples,
        }
        for key in selected
    }


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run commit-bound Federation v1 P01-P12 physical evidence campaign."
        )
    )
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=Path("evidence/v1-physical"),
    )
    sub = parser.add_subparsers(dest="command_name", required=True)

    init = sub.add_parser("init")
    init.add_argument("--commit", required=True)
    init.add_argument("--operator", required=True)

    host = sub.add_parser("host")
    host.add_argument("--commit", required=True)
    host.add_argument("--host", required=True)
    host.add_argument("--role", default="physical-test-host")
    host.add_argument(
        "--profile",
        choices=sorted(HOST_PROFILES),
        default="unspecified",
    )

    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--scenario")

    begin = sub.add_parser("begin")
    begin.add_argument("--commit", required=True)
    begin.add_argument("--host", required=True)
    begin.add_argument("--scenario", required=True)

    sample = sub.add_parser("sample")
    sample.add_argument("--commit", required=True)
    sample.add_argument("--host", required=True)
    sample.add_argument("--scenario", required=True)
    sample.add_argument("--label", default="resource-sample")
    sample.add_argument("--run-id")

    run = sub.add_parser("run")
    run.add_argument("--commit", required=True)
    run.add_argument("--host", required=True)
    run.add_argument("--scenario", required=True)
    run.add_argument("--assertion")
    run.add_argument("--label", required=True)
    run.add_argument("--expect-exit", type=int, default=0)
    run.add_argument("--timeout", type=float, default=300.0)
    run.add_argument("cmd", nargs=argparse.REMAINDER)

    observation = sub.add_parser("observe")
    observation.add_argument("--commit", required=True)
    observation.add_argument("--host", required=True)
    observation.add_argument("--scenario", required=True)
    observation.add_argument("--assertion", required=True)
    observation.add_argument(
        "--status",
        choices=("pass", "fail", "not-applicable"),
        required=True,
    )
    observation.add_argument("--note", default="")

    finish = sub.add_parser("finish")
    finish.add_argument("--commit", required=True)
    finish.add_argument("--host", required=True)
    finish.add_argument("--scenario", required=True)
    finish.add_argument("--run-id", required=True)

    status = sub.add_parser("status")
    status.add_argument("--commit", required=True)
    status.add_argument("--scenario")

    privacy = sub.add_parser("privacy")
    privacy.add_argument("--commit", required=True)

    validate = sub.add_parser("validate")
    validate.add_argument("--commit", required=True)

    args = parser.parse_args(argv)
    checkout = args.checkout.resolve()
    root = args.evidence_root
    if not root.is_absolute():
        root = checkout / root

    try:
        if args.command_name == "init":
            result: object = initialize(
                checkout,
                root,
                commit=args.commit,
                operator=args.operator,
            )
        elif args.command_name == "host":
            result = register_host(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                role=args.role,
                profile=args.profile,
            )
        elif args.command_name == "plan":
            result = plan(args.scenario)
        elif args.command_name == "begin":
            run_id, path = begin_session(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
            )
            result = {
                "run_id": run_id,
                "evidence": _display_path(path, checkout, root),
            }
        elif args.command_name == "sample":
            path = sample_resources(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                label=args.label,
                run_id=args.run_id,
            )
            result = {"evidence": _display_path(path, checkout, root)}
        elif args.command_name == "run":
            path = run_command(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                label=args.label,
                command=args.cmd,
                expected_exit=args.expect_exit,
                timeout=args.timeout,
            )
            packet = _load_json(path)
            result = {
                "evidence": _display_path(path, checkout, root),
                "passed": packet["passed"],
            }
        elif args.command_name == "observe":
            path = observe(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                assertion=args.assertion,
                status=args.status,
                note=args.note,
            )
            result = {
                "evidence": _display_path(path, checkout, root),
                "status": args.status,
            }
        elif args.command_name == "finish":
            path = finish_session(
                checkout,
                root,
                commit=args.commit,
                host=args.host,
                scenario=args.scenario,
                run_id=args.run_id,
            )
            result = {
                "evidence": _display_path(path, checkout, root),
                "scenario_status": scenario_status(
                    root,
                    args.scenario,
                    expected_commit=args.commit,
                ),
            }
        elif args.command_name == "status":
            load_campaign(checkout, root, args.commit)
            if args.scenario:
                result = scenario_status(
                    root,
                    args.scenario,
                    expected_commit=args.commit,
                )
            else:
                result = [
                    scenario_status(
                        root,
                        key,
                        expected_commit=args.commit,
                    )
                    for key in SCENARIOS
                ]
        elif args.command_name == "privacy":
            result = privacy_check(
                checkout,
                root,
                commit=args.commit,
            )
        else:
            result = validate_campaign(
                checkout,
                root,
                commit=args.commit,
            )
        _print(result)
        if args.command_name == "validate" and not bool(result["accepted"]):
            return 2
        return 0
    except (CampaignError, OSError, subprocess.SubprocessError) as exc:
        _print(
            {
                "error": redact_text(str(exc), cwd=checkout),
                "accepted": False,
            }
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())

"""Commit-bound Federation v1 physical robustness campaign harness.

This helper orchestrates P01-P12 without pretending that physical actions can be
proven by CI. It creates portable, redacted observation packets under
``evidence/v1-physical``. Packets from independent hosts can be copied into one
coordinator evidence tree before final validation.

The harness deliberately does not inject destructive faults on its own. The
operator performs the documented physical action and records the resulting
probe/command or observation through this CLI. PASS is derived from the
checked-in scenario contract, exact candidate identity, elapsed-time rules,
host/OS requirements, and a final privacy digest rather than by hand-editing a
summary JSON file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from scripts.acceptance.cf7_physical_readiness import sanitize_text, verify_checkout

SCHEMA: Final = "fcp.v1.physical-campaign.v1"
PACKET_SCHEMA: Final = "fcp.v1.physical-observation.v1"
PRIVACY_SCHEMA: Final = "fcp.v1.physical-privacy.v1"
HOST_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}")
SCENARIO_RE = re.compile(r"P(?:0[1-9]|1[0-2])")


class CampaignError(RuntimeError):
    """The physical campaign cannot safely continue."""


@dataclass(frozen=True)
class ScenarioSpec:
    title: str
    assertions: dict[str, str]
    allow_na: frozenset[str] = field(default_factory=frozenset)
    assertion_os: dict[str, str] = field(default_factory=dict)
    minimum_elapsed_seconds: int = 0
    minimum_samples: int = 0


SCENARIOS: Final[dict[str, ScenarioSpec]] = {
    "P01": ScenarioSpec(
        "Repeated Federation update growth and failure cleanup",
        {
            "windows-resource-baseline": "Windows/Beast resource baseline covers host, Docker, data/results, logs and model storage.",
            "posix-resource-baseline": "POSIX resource baseline covers host, Docker, data/results, logs, model storage and inode state.",
            "windows-three-activations": "At least three supported Windows activations cover unchanged/distinct commits where meaningful.",
            "posix-three-activations": "At least three supported POSIX activations cover unchanged/distinct commits where meaningful.",
            "windows-failed-build-cleanup": "A failed/interrupted Windows build leaves bounded cache/resource state or safe pressure behavior.",
            "posix-failed-build-cleanup": "A failed/interrupted POSIX build leaves bounded cache/resource state or safe pressure behavior.",
            "windows-runtime-state": "Windows runtime proves exact commit and retained FCP state after activation.",
            "posix-runtime-state": "POSIX runtime proves exact commit and retained FCP state after activation.",
            "windows-growth-bounded": "Windows does not return to the historical multi-gigabyte-per-activation growth slope.",
            "posix-growth-bounded": "POSIX does not show unbounded per-activation growth.",
        },
        assertion_os={
            "windows-resource-baseline": "windows",
            "windows-three-activations": "windows",
            "windows-failed-build-cleanup": "windows",
            "windows-runtime-state": "windows",
            "windows-growth-bounded": "windows",
            "posix-resource-baseline": "posix",
            "posix-three-activations": "posix",
            "posix-failed-build-cleanup": "posix",
            "posix-runtime-state": "posix",
            "posix-growth-bounded": "posix",
        },
    ),
    "P02": ScenarioSpec(
        "Independent backing-resource exhaustion",
        {
            "checkout-docker-pressure": "Checkout/build/Docker backing resource refuses optional work safely under pressure.",
            "data-pressure": "Data backing resource pressure is surfaced and does not corrupt committed state.",
            "results-pressure": "Results backing resource pressure refuses optional work before the emergency floor.",
            "model-pressure": "Model/provider backing resource pressure refuses large optional writes safely.",
            "inode-pressure": "Inode/file-capacity pressure is handled where the backing filesystem exposes it.",
            "core-remains-usable": "The old healthy core remains usable while optional pressure-bound work is refused.",
        },
        allow_na=frozenset({"inode-pressure"}),
    ),
    "P03": ScenarioSpec(
        "All supported start/update entry points and concurrency",
        {
            "start-cmd": "start.cmd exercises the supported Windows start contract.",
            "start-sh": "start.sh exercises the supported POSIX start contract.",
            "start-tailscale-cmd": "start-tailscale.cmd exercises the supported Windows/tailnet path.",
            "update-cmd-disposition": "update.cmd follows its final approved/retired disposition without bypassing update policy.",
            "windows-concurrent-launchers": "Concurrent Windows launcher attempts serialize host mutation safely.",
            "posix-concurrent-launchers": "Concurrent POSIX launcher attempts serialize host mutation safely.",
            "launcher-vs-update": "Launcher versus pending/active host update serializes source/build mutation.",
            "identity-and-isolation": "Source/image/runtime identity, bounded build state, and model/network failure isolation are proven.",
        },
        assertion_os={
            "start-cmd": "windows",
            "start-sh": "posix",
            "start-tailscale-cmd": "windows",
            "update-cmd-disposition": "windows",
            "windows-concurrent-launchers": "windows",
            "posix-concurrent-launchers": "posix",
        },
    ),
    "P04": ScenarioSpec(
        "Recorder finite transaction and disk pressure",
        {
            "concurrent-sources": "Recorder exercises up to eight simultaneous sources or the supported configured maximum.",
            "maximum-ingress": "Maximum accepted response, observation and sequence-span bounds are exercised.",
            "aggregate-admission": "Aggregate admission leaves completion room for raw/manifest/observation/JSONL/checkpoint/status/journal writes.",
            "pressure-state-ladder": "WARNING, PRESSURE and CRITICAL behavior is observed without crossing the emergency reserve.",
            "safe-pause": "Critical pressure pauses capture without deleting primary evidence.",
            "recovery-continuity": "Restored capacity resumes sequence/checkpoint continuity without --fresh.",
        },
    ),
    "P05": ScenarioSpec(
        "Service and failure injection",
        {
            "flask-crash": "Flask crash is bounded and visible.",
            "relay-crash": "Relay process crash is bounded and visible.",
            "relay-stale-db-failure": "Relay stale-sweep database failure does not leave a running-but-dead service.",
            "managed-recorder-crash": "Managed recorder crash is supervised without unrelated loss.",
            "native-recorder-crash": "Native recorder child crash follows bounded supervision semantics.",
            "publication-db-failure": "Publication database failure is surfaced and retried without losing durable work.",
            "analysis-db-failure": "Analysis scheduler database failure is surfaced at the required-thread boundary.",
            "poison-recorder-archive": "A poison recorder archive is isolated from later eligible work.",
            "slow-trickle-response": "A slow-trickle MTConnect response is terminated by the finite deadline.",
            "oversized-ingress": "Oversized MTConnect response/observation input is refused within finite bounds.",
            "huge-sequence-gap": "Huge sequence discontinuity is handled without proportional allocation.",
            "malformed-timestamp-path": "Malicious/malformed timestamp path components cannot escape recorder roots.",
            "event-storm": "Repeated discontinuity evidence is deduplicated/rate-bounded.",
            "ollama-absence": "Ollama/model absence degrades the capability without removing core availability.",
            "stale-responder-pid": "Stale responder PID reuse cannot target an unrelated process.",
            "global-invariant": "Logs remain bounded, health is visible, and no unrelated authority/data is lost.",
        },
    ),
    "P06": ScenarioSpec(
        "Native recorder supervision contract",
        {
            "unexpected-child-restart": "One unexpected child crash restarts with bounded backoff.",
            "crash-loop-fence": "Repeated deterministic crashes reach the declared crash-loop fence.",
            "operator-stop": "Operator Ctrl+C/stop does not restart the child.",
            "update-trial-semantics": "Approved update/trial replacement semantics remain unchanged.",
            "checkpoint-continuity": "Recorder checkpoint continuity survives supervised restart.",
            "service-manager-boundary": "Supervisor crash/reboot behavior matches the actually declared external startup/service-manager contract.",
        },
        allow_na=frozenset({"service-manager-boundary"}),
        assertion_os={
            "unexpected-child-restart": "windows",
            "crash-loop-fence": "windows",
            "operator-stop": "windows",
            "update-trial-semantics": "windows",
            "checkpoint-continuity": "windows",
            "service-manager-boundary": "windows",
        },
    ),
    "P07": ScenarioSpec(
        "Long Federation outage with aged corpus",
        {
            "aged-corpus": "A non-trivial historical corpus/outbox exists before disconnecting Federation.",
            "capture-continues": "Local capture/checkpoints continue during control-plane outage.",
            "backlog-durable": "Publication backlog remains durable during outage.",
            "latency-bounded": "Source polling and publication reconciliation latency remain bounded.",
            "workers-alive": "No required worker silently dies during outage.",
            "catchup-progress": "Reconnect catch-up makes continuous measurable forward progress.",
            "poison-isolation": "A poison item cannot block later eligible work during catch-up.",
            "duplicate-suppression": "Duplicate suppression remains correct after outage/reconnect.",
            "restart-progress": "Restart during backlog does not lose durable progress.",
        },
        minimum_elapsed_seconds=3600,
        minimum_samples=2,
    ),
    "P08": ScenarioSpec(
        "Logical storage exhaustion under host pressure",
        {
            "allocation-floor": "A real storage authority reaches allocation/floor under the stricter host-pressure contract.",
            "committed-reads": "Existing committed reads and control surfaces remain available at refusal.",
            "no-partial-commit": "No partial storage commit becomes visible.",
            "recovery-no-repair": "Restored capacity recovers without manual database/filesystem repair.",
        },
    ),
    "P09": ScenarioSpec(
        "Durable-write crash windows, control-plane loss and clock skew",
        {
            "raw-publication": "Interruption around raw temp/final publication recovers without false commit.",
            "raw-manifest": "Interruption around raw manifest publication recovers safely.",
            "observation-archive": "Interruption around observation archive publication recovers safely.",
            "compat-jsonl": "Interruption around compatibility JSONL publication recovers safely.",
            "checkpoint-status": "Interruption around checkpoint/status replacement preserves continuity.",
            "outbox-transaction": "Interruption around outbox transaction preserves durable idempotency/progress.",
            "analysis-slice": "Interruption around deterministic analysis slice archive cannot promote a truncated artifact.",
            "upload-transitions": "Upload staging/database/publication crash windows reconcile without partial exposure.",
            "sqlite-wal": "Coordinator/relay SQLite/WAL interruption preserves authoritative recovery.",
            "coordinator-disappearance": "Coordinator disappearance cannot fabricate continuity or self-promote authority.",
            "positive-clock-skew": "Bounded positive clock offset around storage lease expiry fails safely.",
            "negative-clock-skew": "Bounded negative clock offset around storage lease expiry fails safely.",
            "authority-projection": "No crash/loss/skew case yields silent partial authority projection.",
        },
    ),
    "P10": ScenarioSpec(
        "Model installation through every product path",
        {
            "normal-startup": "Required-model absence/pressure is exercised through normal startup.",
            "federation-update": "Required-model absence/pressure is exercised through Federation update.",
            "browser-install": "Required-model absence/pressure is exercised through browser-triggered installation.",
            "provider-profile-install": "Required-model absence/pressure is exercised through provider/profile installation.",
            "failure-isolation": "Model failure does not remove workbench/Federation/recorder/control availability.",
            "emergency-floor": "No model pull crosses the host emergency resource floor.",
        },
    ),
    "P11": ScenarioSpec(
        "Exact-candidate backup and restore",
        {
            "external-destination": "Backup destination is independent and preflighted before quiescence.",
            "helper-prestaged": "Any helper needed after quiescence is available before services stop.",
            "writers-fenced": "Compose and relevant host writers are fenced and quiescence is proven.",
            "update-refused": "Concurrent update/host mutation is refused while backup owns the host mutation boundary.",
            "failed-copy-safe": "A deliberate copy/verify failure leaves FCP stopped and primary state intact.",
            "successful-backup": "A successful exact-candidate backup completes with manifest/source identity.",
            "sqlite-integrity": "Every applicable restored SQLite database passes integrity/quick-check.",
            "isolated-restore": "Restore is performed in isolation into clean destination resources.",
            "identity-continuity": "Same-installation device/Federation/auth/recorder continuity is verified where applicable.",
            "core-no-model-download": "Core recovery succeeds without requiring model download.",
            "replacement-identity": "Replacement-member recovery uses a new identity rather than cloning member authority.",
            "windows-dpapi": "Windows DPAPI identity boundary is exercised where applicable.",
        },
        allow_na=frozenset({"windows-dpapi"}),
    ),
    "P12": ScenarioSpec(
        "Aged-history 24-hour soak plus accelerated ceiling tests",
        {
            "aged-history": "The soak begins with meaningful history rather than an empty installation.",
            "storage-series": "Free bytes and inode/file counts are sampled for relevant backing resources.",
            "docker-series": "Docker root/VHDX, cache, image and log growth is sampled.",
            "recorder-series": "Recorder corpus growth and per-poll recovery time are sampled.",
            "publication-series": "Publication reconciliation duration/progress and outbox backlog are sampled.",
            "history-series": "Session/member/job/attempt/command/grant/artifact/provider/update history and database sizes are sampled.",
            "orphan-series": "Orphan staging/workspaces and required-thread/process/restart health are sampled.",
            "cpu-ram-series": "CPU/RAM observations are sufficient to identify concrete leaks or OOM isolation defects.",
            "accelerated-ceilings": "Accelerated tests cross the known authority/history page ceilings.",
            "no-unexplained-growth": "The completed soak has no unexplained growth, restart amplification, or backlog slope.",
        },
        minimum_elapsed_seconds=24 * 60 * 60,
        minimum_samples=2,
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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
    verify_checkout(checkout, expected)
    operator_text = sanitize_text(operator.strip(), cwd=checkout)
    if not operator_text:
        raise CampaignError("operator must be non-empty")
    path = campaign_path(root)
    if path.exists():
        existing = _load_json(path)
        if existing.get("candidate_sha") != expected:
            raise CampaignError("existing campaign targets a different candidate")
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
    verify_checkout(checkout, expected)
    path = campaign_path(root)
    if not path.exists():
        raise CampaignError("campaign is not initialized; run init first")
    document = _load_json(path)
    if document.get("schema") != SCHEMA or document.get("candidate_sha") != expected:
        raise CampaignError("campaign schema/candidate does not match this checkout")
    return document


def host_fingerprint() -> str:
    material = (
        f"{platform.node()}|{platform.system()}|{platform.machine()}"
    ).encode("utf-8", errors="replace")
    return hashlib.sha256(material).hexdigest()[:16]


def register_host(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    role: str,
) -> dict[str, object]:
    load_campaign(checkout, root, commit)
    expected = require_commit(commit)
    host_id = require_host(host)
    path = root / "hosts" / f"{host_id}.json"
    fingerprint = host_fingerprint()
    if path.exists():
        existing = _load_json(path)
        if existing.get("candidate_sha") != expected:
            raise CampaignError(f"host {host_id} belongs to a different candidate")
        if existing.get("host_fingerprint") != fingerprint:
            raise CampaignError(f"host alias {host_id} already identifies another machine")
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
        "role": sanitize_text(role, cwd=checkout),
        "recorded_at": utc_now(),
    }
    _write_json(path, record)
    return record


def load_host(root: Path, host: str, *, commit: str | None = None) -> dict[str, object]:
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


def write_packet(root: Path, packet: dict[str, object]) -> Path:
    scenario = require_scenario(str(packet["scenario"]))
    kind = str(packet.get("kind", "observation"))
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
) -> tuple[str, ScenarioSpec, dict[str, object]]:
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
    return scenario_id, spec, host_record


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
) -> Path:
    load_campaign(checkout, root, commit)
    scenario_id, spec, _host_record = _assertion_contract(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        assertion=assertion,
    )
    if status not in {"pass", "fail", "not-applicable"}:
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
            "note": sanitize_text(note, cwd=checkout),
        }
    )
    return write_packet(root, packet)


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
            "label": sanitize_text(label, cwd=checkout),
            "command": sanitize_text(" ".join(command), cwd=checkout),
            "expected_exit": expected_exit,
            "returncode": returncode,
            "duration_seconds": duration,
            "output_tail": sanitize_text(output, cwd=checkout),
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


def sample_resources(
    checkout: Path,
    root: Path,
    *,
    commit: str,
    host: str,
    scenario: str,
    label: str,
) -> Path:
    load_campaign(checkout, root, commit)
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario,
        kind="sample",
    )
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
                "summary": sanitize_text(output, cwd=checkout),
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
                "compose_summary": sanitize_text(output, cwd=checkout),
            }
        )
    packet.update(
        {
            "label": sanitize_text(label, cwd=checkout),
            "resources": resources,
            "docker": docker,
        }
    )
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
    current_host = load_host(root, host, commit=commit)
    begins = [
        item
        for item in read_packets(
            root,
            scenario_id,
            expected_commit=commit,
        )
        if item.get("kind") == "begin" and item.get("run_id") == run_id
    ]
    if len(begins) != 1:
        raise CampaignError("finish requires exactly one matching begin packet")
    if begins[0].get("host_fingerprint") != current_host.get("host_fingerprint"):
        raise CampaignError("timed session must finish on the host that began it")
    packet = base_packet(
        root,
        commit=commit,
        host=host,
        scenario=scenario_id,
        kind="finish",
    )
    packet["run_id"] = run_id
    packet["started_at"] = begins[0]["recorded_at"]
    packet["elapsed_seconds"] = round(
        (
            parse_time(str(packet["recorded_at"]))
            - parse_time(str(begins[0]["recorded_at"]))
        ).total_seconds(),
        3,
    )
    return write_packet(root, packet)


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
        if expected is not None and packet.get("candidate_sha") != expected:
            raise CampaignError(
                f"observation {path.name} belongs to a different candidate"
            )
        packet_scenario = packet.get("scenario")
        if not isinstance(packet_scenario, str) or require_scenario(packet_scenario) != packet_scenario:
            raise CampaignError(f"observation {path.name} has invalid scenario")
        packets.append(packet)
    return packets


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
    finishes = [item for item in packets if item.get("kind") == "finish"]
    elapsed = max(
        (float(item.get("elapsed_seconds", 0.0)) for item in finishes),
        default=0.0,
    )
    samples = sum(1 for item in packets if item.get("kind") == "sample")
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


def privacy_check(
    checkout: Path,
    root: Path,
    *,
    commit: str,
) -> dict[str, object]:
    load_campaign(checkout, root, commit)
    read_packets(root, expected_commit=commit)
    for path in sorted((root / "hosts").glob("*.json")):
        host = _load_json(path)
        if host.get("candidate_sha") != require_commit(commit):
            raise CampaignError(f"host {path.name} belongs to a different candidate")
    unsafe: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "privacy.json":
            continue
        if path.suffix.casefold() not in {".json", ".txt", ".log"}:
            unsafe.append(path.relative_to(root).as_posix())
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if sanitize_text(text, cwd=checkout) != text:
            unsafe.append(path.relative_to(root).as_posix())
    if unsafe:
        raise CampaignError(
            "privacy scan found unredacted or unsupported evidence: "
            + ", ".join(unsafe[:10])
        )
    digest, count = _privacy_digest(root)
    result = {
        "schema": PRIVACY_SCHEMA,
        "candidate_sha": require_commit(commit),
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
                "error": sanitize_text(str(exc), cwd=checkout),
                "accepted": False,
            }
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())

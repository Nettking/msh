"""Prepare and run redacted CF7 physical-acceptance checks.

This helper cannot declare physical acceptance. It verifies one real checkout,
runs the local acceptance gates, probes only explicitly configured physical
seams, and writes redacted summaries below ``evidence/``. Private endpoints
are accepted only through environment variables and are never persisted.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from catalog.federation.tests.cf7_acceptance.physical_evidence import (
    PhysicalEvidenceError,
    load_physical_evidence,
    validate_physical_evidence,
)

LOCAL_SCHEMA: Final = "msh.cf7.physical-readiness.v1"
TOPOLOGY_SCHEMA: Final = "msh.cf7.physical-topology.v1"
MAX_HTTP_BYTES: Final = 1_048_576
DEFAULT_TIMEOUT_SECONDS: Final = 5.0

MACHINE_PROFILES: Final[dict[str, dict[str, object]]] = {
    "local-ai": {
        "expected_os": "windows",
        "roles": ("language-model", "desktop-browser"),
        "required_environment": (
            "MSH_CF7_OLLAMA_URL",
            "MSH_CF7_OLLAMA_MODEL",
        ),
    },
    "cnc-recorder": {
        "expected_os": None,
        "roles": ("recorder", "mtconnect"),
        "required_environment": ("MSH_CF7_MTCONNECT_ENDPOINTS",),
    },
    "school-control": {
        "expected_os": None,
        "roles": ("control", "registered-compute", "storage-candidate"),
        "required_environment": ("MSH_CF7_PEERS",),
    },
}

_URL_RE = re.compile(r"\b(?:https?|wss?)://[^\s\]\[(){}<>\"']+")
_IPV4_RE = re.compile(
    r"(?<![0-9])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9])"
)
_WINDOWS_PATH_RE = re.compile(r"(?i)\b[a-z]:[\\/][^\r\n\t]+")
_TOKEN_RE = re.compile(
    r"(?i)\b(?:authorization|bearer|token|password|secret|"
    r"private[_-]?key)\b\s*[:=]\s*[^\s,;]+"
)


class ReadinessError(RuntimeError):
    """The physical checkout or explicit physical seam is not ready."""


@dataclass(frozen=True)
class CommandResult:
    name: str
    command: tuple[str, ...]
    returncode: int
    duration_seconds: float
    output_tail: str

    @property
    def passed(self) -> bool:
        return self.returncode == 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def os_family() -> str:
    name = platform.system().casefold()
    if name == "windows":
        return "windows"
    if name == "linux":
        return "linux"
    return name or "unknown"


def sanitize_text(value: object, *, cwd: Path | None = None) -> str:
    """Remove endpoints, addresses, local paths, and credential-like values."""

    text = str(value).replace("\x00", "")
    if cwd is not None:
        candidates = {str(cwd), str(cwd.resolve())}
        for candidate in sorted(candidates, key=len, reverse=True):
            if candidate:
                text = text.replace(candidate, "<checkout>")
    home = str(Path.home())
    if home:
        text = text.replace(home, "<home>")
    text = _URL_RE.sub("<redacted-endpoint>", text)
    text = _IPV4_RE.sub("<redacted-address>", text)
    text = _WINDOWS_PATH_RE.sub("<redacted-path>", text)
    text = _TOKEN_RE.sub("<redacted-credential>", text)
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines[-40:]).strip()


def require_commit(value: str) -> str:
    commit = value.strip().casefold()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ReadinessError(
            "candidate commit must be one lowercase 40-character SHA"
        )
    return commit


def _run(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: float,
    name: str,
) -> CommandResult:
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
        )
        output = "\n".join(
            part for part in (completed.stdout, completed.stderr) if part
        )
        returncode = completed.returncode
    except subprocess.TimeoutExpired as exc:
        parts: list[str] = []
        for part in (exc.stdout, exc.stderr):
            if isinstance(part, bytes):
                parts.append(part.decode("utf-8", errors="replace"))
            elif part:
                parts.append(part)
        output = (
            f"command exceeded {timeout_seconds:.0f} seconds\n"
            + "\n".join(parts)
        )
        returncode = 124
    return CommandResult(
        name=name,
        command=tuple(command),
        returncode=returncode,
        duration_seconds=round(time.monotonic() - started, 3),
        output_tail=sanitize_text(output, cwd=cwd),
    )


def _git_output(checkout: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=checkout,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    if completed.returncode != 0:
        raise ReadinessError(
            sanitize_text(completed.stderr or completed.stdout, cwd=checkout)
        )
    return completed.stdout.strip()


def verify_checkout(checkout: Path, expected_commit: str) -> dict[str, object]:
    expected = require_commit(expected_commit)
    actual = require_commit(_git_output(checkout, "rev-parse", "HEAD"))
    status = _git_output(checkout, "status", "--porcelain")
    if actual != expected:
        raise ReadinessError(
            f"checkout is on {actual}, expected frozen commit {expected}"
        )
    if status:
        raise ReadinessError(
            "checkout is not clean; remove local changes before testing"
        )
    return {
        "commit_sha": actual,
        "repository_clean": True,
        "fresh_checkout_confirmed_by_operator": False,
    }


def parse_private_mapping(environment_name: str) -> dict[str, str]:
    raw = os.environ.get(environment_name, "").strip()
    if not raw:
        raise ReadinessError(
            f"set {environment_name} to a JSON object before this probe"
        )
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ReadinessError(
            f"{environment_name} must contain valid JSON"
        ) from exc
    if not isinstance(value, dict) or not value:
        raise ReadinessError(
            f"{environment_name} must contain a non-empty JSON object"
        )
    normalized: dict[str, str] = {}
    for alias, private_value in value.items():
        alias_valid = isinstance(alias, str) and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_-]{0,31}", alias
        )
        if not alias_valid:
            raise ReadinessError(
                f"{environment_name} contains an invalid safe alias"
            )
        if not isinstance(private_value, str) or not private_value.strip():
            raise ReadinessError(
                f"{environment_name}.{alias} must be non-empty text"
            )
        normalized[alias] = private_value.strip()
    return normalized


def _http_json(url: str) -> object:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(  # noqa: S310 - explicit operator endpoint
        request,
        timeout=DEFAULT_TIMEOUT_SECONDS,
    ) as response:
        raw = response.read(MAX_HTTP_BYTES + 1)
        if len(raw) > MAX_HTTP_BYTES:
            raise ReadinessError(
                "physical service response exceeded the bounded read limit"
            )
        return json.loads(raw.decode("utf-8"))


def _safe_join_endpoint(base_url: str, suffix: str) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ReadinessError(
            "private endpoint must be an explicit HTTP or HTTPS URL"
        )
    return urllib.parse.urljoin(
        base_url.rstrip("/") + "/",
        suffix.lstrip("/"),
    )


def probe_ollama() -> dict[str, object]:
    base_url = os.environ.get("MSH_CF7_OLLAMA_URL", "").strip()
    model = os.environ.get("MSH_CF7_OLLAMA_MODEL", "").strip()
    if not base_url or not model:
        raise ReadinessError(
            "set MSH_CF7_OLLAMA_URL and MSH_CF7_OLLAMA_MODEL"
        )
    started = time.monotonic()
    inventory = _http_json(_safe_join_endpoint(base_url, "/api/tags"))
    if not isinstance(inventory, dict):
        raise ReadinessError("Ollama inventory returned an unexpected document")
    models = inventory.get("models")
    if not isinstance(models, list):
        raise ReadinessError("Ollama inventory returned an unexpected document")
    names = {
        item.get("name")
        for item in models
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    return {
        "service": "ollama",
        "reachable": True,
        "target_model": model,
        "target_model_present": model in names,
        "inventory_count": len(names),
        "latency_seconds": round(time.monotonic() - started, 3),
        "private_endpoint_persisted": False,
    }


def probe_mtconnect() -> dict[str, object]:
    endpoints = parse_private_mapping("MSH_CF7_MTCONNECT_ENDPOINTS")
    if len(endpoints) < 2:
        raise ReadinessError(
            "configure aliases for both physical CNC MTConnect agents"
        )
    results: list[dict[str, object]] = []
    for alias, base_url in sorted(endpoints.items()):
        started = time.monotonic()
        reachable = False
        response_kind = "unreachable"
        try:
            request = urllib.request.Request(
                _safe_join_endpoint(base_url, "/probe"),
                method="GET",
                headers={"Accept": "application/xml,text/xml,*/*"},
            )
            with urllib.request.urlopen(  # noqa: S310 - operator endpoint
                request,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            ) as response:
                raw = response.read(MAX_HTTP_BYTES + 1)
                if len(raw) > MAX_HTTP_BYTES:
                    raise ReadinessError(
                        "MTConnect probe exceeded the bounded read limit"
                    )
                body = raw.decode("utf-8", errors="replace")
                reachable = response.status == 200 and (
                    "MTConnect" in body or "MTConnectDevices" in body
                )
                if reachable:
                    response_kind = "mtconnect-probe"
        except OSError:
            reachable = False
        results.append(
            {
                "alias": alias,
                "reachable": reachable,
                "latency_seconds": round(time.monotonic() - started, 3),
                "response_kind": response_kind,
            }
        )
    return {
        "service": "mtconnect",
        "source_count": len(results),
        "all_sources_reachable": all(
            bool(item["reachable"]) for item in results
        ),
        "sources": results,
        "private_endpoints_persisted": False,
    }


def _parse_host_port(value: str) -> tuple[str, int]:
    if value.startswith("["):
        host, separator, port_text = value.rpartition("]:")
        host = host[1:]
    else:
        host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise ReadinessError("peer targets must use host:port values")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ReadinessError(
            "peer target port must be an integer"
        ) from exc
    if not 1 <= port <= 65535:
        raise ReadinessError("peer target port is outside the valid range")
    return host, port


def probe_peers() -> dict[str, object]:
    targets = parse_private_mapping("MSH_CF7_PEERS")
    if len(targets) < 2:
        raise ReadinessError(
            "configure at least two independent peer aliases"
        )
    results: list[dict[str, object]] = []
    for alias, private_target in sorted(targets.items()):
        host, port = _parse_host_port(private_target)
        started = time.monotonic()
        try:
            with socket.create_connection(
                (host, port),
                timeout=DEFAULT_TIMEOUT_SECONDS,
            ):
                reachable = True
        except OSError:
            reachable = False
        results.append(
            {
                "alias": alias,
                "reachable": reachable,
                "latency_seconds": round(time.monotonic() - started, 3),
            }
        )
    return {
        "service": "peer-connectivity",
        "peer_count": len(results),
        "all_peers_reachable": all(
            bool(item["reachable"]) for item in results
        ),
        "peers": results,
        "private_targets_persisted": False,
    }


def _profile(machine: str) -> dict[str, object]:
    try:
        return MACHINE_PROFILES[machine]
    except KeyError as exc:
        raise ReadinessError(
            f"unsupported machine profile: {machine}"
        ) from exc


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def initialize_evidence(
    checkout: Path,
    *,
    evidence_root: Path,
    machine: str,
    operator: str,
    commit: str,
) -> Path:
    profile = _profile(machine)
    expected = require_commit(commit)
    verify_checkout(checkout, expected)
    template = (
        checkout
        / "catalog"
        / "federation"
        / "tests"
        / "cf7_acceptance"
        / "physical_evidence.template.json"
    )
    document = json.loads(template.read_text(encoding="utf-8"))
    document["commit_sha"] = expected
    document["executed_at"] = utc_now()
    document["operator"] = operator.strip() or "operator-pending"
    evidence_root.mkdir(parents=True, exist_ok=True)
    for directory in ("windows", "linux", "scenarios", machine):
        (evidence_root / directory).mkdir(parents=True, exist_ok=True)
    evidence_path = evidence_root / "cf7-physical-evidence.json"
    if not evidence_path.exists():
        _write_json(evidence_path, document)
    _write_json(
        evidence_root / machine / "profile.json",
        {
            "schema": LOCAL_SCHEMA,
            "machine": machine,
            "roles": list(profile["roles"]),
            "expected_os": profile["expected_os"],
            "candidate_commit": expected,
            "created_at": utc_now(),
            "contains_private_endpoints": False,
        },
    )
    return evidence_path


def _probe_for_machine(machine: str) -> dict[str, object]:
    if machine == "local-ai":
        result = probe_ollama()
        ready = bool(result["reachable"]) and bool(
            result["target_model_present"]
        )
    elif machine == "cnc-recorder":
        result = probe_mtconnect()
        ready = bool(result["all_sources_reachable"])
    else:
        result = probe_peers()
        ready = bool(result["all_peers_reachable"])
    if not ready:
        raise ReadinessError(
            f"{machine} physical probe did not satisfy its readiness boundary"
        )
    return result


def preflight(
    checkout: Path,
    *,
    evidence_root: Path,
    machine: str,
    commit: str,
) -> dict[str, object]:
    profile = _profile(machine)
    checkout_state = verify_checkout(checkout, commit)
    family = os_family()
    expected_os = profile["expected_os"]
    if expected_os is not None and family != expected_os:
        raise ReadinessError(
            f"{machine} must run on {expected_os}, detected {family}"
        )
    if sys.version_info < (3, 11):
        raise ReadinessError("Python 3.11 or newer is required")
    missing_commands = [
        name for name in ("git", "docker") if shutil.which(name) is None
    ]
    if missing_commands:
        raise ReadinessError(
            f"missing required command(s): {', '.join(missing_commands)}"
        )
    required_environment = profile["required_environment"]
    missing_environment = [
        name
        for name in required_environment
        if not os.environ.get(str(name), "").strip()
    ]
    if missing_environment:
        raise ReadinessError(
            "missing required environment variable(s): "
            + ", ".join(str(item) for item in missing_environment)
        )
    result = {
        "schema": LOCAL_SCHEMA,
        "machine": machine,
        "roles": list(profile["roles"]),
        "os_family": family,
        "python_version": (
            f"{sys.version_info.major}."
            f"{sys.version_info.minor}."
            f"{sys.version_info.micro}"
        ),
        "checkout": checkout_state,
        "physical_probe": _probe_for_machine(machine),
        "checked_at": utc_now(),
        "accepted": False,
        "note": "Preflight readiness is not physical acceptance.",
    }
    _write_json(evidence_root / machine / "preflight.json", result)
    return result


def gate_commands() -> tuple[tuple[str, tuple[str, ...], float], ...]:
    python = sys.executable
    route_tests = (
        "catalog/flask_app/tests/test_capability_onboarding_route.py",
        "catalog/flask_app/tests/test_capability_inspection_route.py",
        "catalog/flask_app/tests/test_capability_benchmark_route.py",
        "catalog/flask_app/tests/test_capability_contribution_route.py",
        "catalog/flask_app/tests/test_capability_startup_transition_route.py",
        "catalog/flask_app/tests/test_federation_overview_route.py",
    )
    return (
        (
            "cf7-acceptance",
            (
                python,
                "-m",
                "pytest",
                "-o",
                "addopts=",
                "-q",
                "catalog/federation/tests/cf7_acceptance",
            ),
            1_800.0,
        ),
        (
            "capability-routes",
            (
                python,
                "-m",
                "pytest",
                "-o",
                "addopts=",
                "-q",
                *route_tests,
            ),
            1_800.0,
        ),
        ("compose-config", ("docker", "compose", "config"), 180.0),
        ("diff-hygiene", ("git", "diff", "--check"), 60.0),
    )


def _display_command(command: tuple[str, ...], checkout: Path) -> str:
    values = list(command)
    if values and values[0] == sys.executable:
        values[0] = "python"
    return sanitize_text("command=" + " ".join(values), cwd=checkout)


def run_gates(
    checkout: Path,
    *,
    evidence_root: Path,
    machine: str,
    commit: str,
) -> dict[str, object]:
    verify_checkout(checkout, commit)
    results = [
        _run(
            list(command),
            cwd=checkout,
            timeout_seconds=timeout,
            name=name,
        )
        for name, command, timeout in gate_commands()
    ]
    family = os_family()
    log_path = evidence_root / family / "commands.txt"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    chunks: list[str] = []
    for result in results:
        chunks.extend(
            [
                (
                    f"[{result.name}] result="
                    f"{'passed' if result.passed else 'failed'}"
                ),
                f"duration_seconds={result.duration_seconds}",
                _display_command(result.command, checkout),
                result.output_tail or "<no output>",
                "",
            ]
        )
    log_path.write_text("\n".join(chunks), encoding="utf-8")
    checks = [
        {
            "name": result.name,
            "passed": result.passed,
            "returncode": result.returncode,
            "duration_seconds": result.duration_seconds,
        }
        for result in results
    ]
    summary = {
        "schema": LOCAL_SCHEMA,
        "machine": machine,
        "os_family": family,
        "commit_sha": require_commit(commit),
        "passed": all(result.passed for result in results),
        "checks": checks,
        "command_log": f"evidence/{family}/commands.txt",
        "executed_at": utc_now(),
        "accepted": False,
        "note": "Local gates do not replace physical scenarios.",
    }
    _write_json(evidence_root / machine / "gate-summary.json", summary)
    if not summary["passed"]:
        failed = ", ".join(
            str(item["name"]) for item in checks if not item["passed"]
        )
        raise ReadinessError(f"local gate failure: {failed}")
    return summary


def validate_topology(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != TOPOLOGY_SCHEMA:
        raise ReadinessError(f"topology must use schema {TOPOLOGY_SCHEMA}")
    machines = value.get("machines")
    if not isinstance(machines, dict):
        raise ReadinessError("topology machines must be an object")
    if set(machines) != set(MACHINE_PROFILES):
        raise ReadinessError(
            "topology must define local-ai, cnc-recorder and school-control"
        )
    families: set[str] = set()
    for machine, profile in machines.items():
        if not isinstance(profile, dict):
            raise ReadinessError(
                f"topology machine {machine} must be an object"
            )
        family = profile.get("os_family")
        if family not in {"windows", "linux"}:
            raise ReadinessError(
                f"topology machine {machine} must select windows or linux"
            )
        families.add(family)
    if families != {"windows", "linux"}:
        raise ReadinessError(
            "physical topology must include Windows and Linux hosts"
        )
    return {
        "schema": TOPOLOGY_SCHEMA,
        "machine_count": len(machines),
        "os_families": sorted(families),
        "ready": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare redacted CF7 physical tests."
    )
    parser.add_argument("--checkout", type=Path, default=Path.cwd())
    parser.add_argument(
        "--evidence-root",
        type=Path,
        default=Path("evidence"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument(
        "--machine",
        choices=sorted(MACHINE_PROFILES),
        required=True,
    )
    init_parser.add_argument("--operator", required=True)
    init_parser.add_argument("--commit", required=True)

    preflight_parser = subparsers.add_parser("preflight")
    preflight_parser.add_argument(
        "--machine",
        choices=sorted(MACHINE_PROFILES),
        required=True,
    )
    preflight_parser.add_argument("--commit", required=True)

    gate_parser = subparsers.add_parser("gate")
    gate_parser.add_argument(
        "--machine",
        choices=sorted(MACHINE_PROFILES),
        required=True,
    )
    gate_parser.add_argument("--commit", required=True)

    topology_parser = subparsers.add_parser("topology")
    topology_parser.add_argument("path", type=Path)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("path", type=Path)
    validate_parser.add_argument("--commit", required=True)

    args = parser.parse_args(argv)
    checkout = args.checkout.resolve()
    evidence_root = args.evidence_root
    if not evidence_root.is_absolute():
        evidence_root = checkout / evidence_root

    try:
        if args.command == "init":
            path = initialize_evidence(
                checkout,
                evidence_root=evidence_root,
                machine=args.machine,
                operator=args.operator,
                commit=args.commit,
            )
            result: object = {
                "initialized": True,
                "evidence": path.name,
            }
        elif args.command == "preflight":
            result = preflight(
                checkout,
                evidence_root=evidence_root,
                machine=args.machine,
                commit=args.commit,
            )
        elif args.command == "gate":
            result = run_gates(
                checkout,
                evidence_root=evidence_root,
                machine=args.machine,
                commit=args.commit,
            )
        elif args.command == "topology":
            result = validate_topology(args.path)
        else:
            result = validate_physical_evidence(
                load_physical_evidence(args.path),
                expected_commit=args.commit,
            )
    except (
        OSError,
        json.JSONDecodeError,
        ReadinessError,
        PhysicalEvidenceError,
    ) as exc:
        print(sanitize_text(exc, cwd=checkout), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

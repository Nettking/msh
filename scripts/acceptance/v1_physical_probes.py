"""Checked-in, non-destructive probes for the Federation v1 P01-P12 campaign.

Every probe here is read-only with respect to product state. A probe measures
what the physical host already is; it never fills a disk, kills a process,
corrupts a file, moves a clock, or downloads a model. Deliberate fault injection
stays an explicit operator action, and this module only supplies the PREPARE and
VERIFY halves around it.

A probe returns one of three verdicts:

``pass``
    The probe proved its property on this host.
``fail``
    The probe ran and the property does not hold.
``unavailable``
    The probe could not observe the surface at all (Docker absent, no recorder
    corpus yet, an unsupported checkout shape).

``unavailable`` is never promoted to a pass. The runner records it as missing
evidence, so a probe that cannot see anything can never close an assertion.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

from scripts.acceptance.cf7_physical_readiness import (
    ReadinessError,
    sanitize_text,
    verify_checkout,
)
from scripts.acceptance.v1_physical_runtime_binding import RuntimeBinding

PASS: Final = "pass"
FAIL: Final = "fail"
UNAVAILABLE: Final = "unavailable"
PROBE_STATUSES: Final = frozenset({PASS, FAIL, UNAVAILABLE})

OPTION_KEY_RE = re.compile(r"[a-z][a-z0-9_]{0,31}")
OPTION_VALUE_RE = re.compile(r"[A-Za-z0-9 ._:/\\@,+~-]{1,240}")

#: Hard ceilings so a probe stays bounded on a real production-sized host.
MAX_SQLITE_FILES: Final = 64
MAX_LOG_FILES: Final = 256
MAX_TREE_ENTRIES: Final = 200_000
DEFAULT_COMMAND_TIMEOUT: Final = 60.0
MEBIBYTE: Final = 1024 * 1024
GIBIBYTE: Final = 1024 * MEBIBYTE

CORE_COMPOSE_SERVICES: Final = ("web", "relay")
BUILD_COMMIT_LABEL: Final = "no.fcp.build_commit"


class ProbeError(RuntimeError):
    """A probe was asked to do something it cannot do safely."""


@dataclass(frozen=True)
class ProbeContext:
    """Everything a probe may read. Probes never receive shell input."""

    checkout: Path
    evidence_root: Path
    commit: str
    host_id: str
    os_category: str
    profile: str
    scenario: str
    assertion: str
    run_id: str | None = None
    options: Mapping[str, str] = field(default_factory=dict)
    runtime_binding: RuntimeBinding | None = None
    harness_sha: str | None = None

    def option(self, name: str, default: str = "") -> str:
        return str(self.options.get(name, default))

    def int_option(self, name: str, default: int) -> int:
        raw = self.option(name)
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError as exc:
            raise ProbeError(f"option {name} must be an integer") from exc
        if value < 0:
            raise ProbeError(f"option {name} must not be negative")
        return value

    def path_option(self, name: str) -> Path | None:
        raw = self.option(name)
        if not raw:
            return None
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = self.checkout / candidate
        return candidate

    @property
    def data_dir(self) -> Path:
        if self.runtime_binding and self.runtime_binding.data_root is not None:
            return self.runtime_binding.data_root
        return self.checkout / "data"

    @property
    def results_dir(self) -> Path:
        if self.runtime_binding and self.runtime_binding.results_root is not None:
            return self.runtime_binding.results_root
        return self.checkout / "results"

    @property
    def roots_are_bound(self) -> bool:
        """True when the operator named the runtime data/results surfaces.

        When they did, the harness checkout is a different filesystem with
        different free space, so falling back to it does not approximate the
        runtime -- it measures something else entirely and must fail closed
        instead.
        """

        binding = self.runtime_binding
        return binding is not None and (
            binding.data_root is not None or binding.results_root is not None
        )

    def storage_anchor(self) -> Path | None:
        """The path whose filesystem carries runtime storage, if provable."""

        if self.data_dir.exists():
            return self.data_dir
        if self.roots_are_bound:
            return None
        return self.checkout


@dataclass(frozen=True)
class ProbeOutcome:
    probe_id: str
    status: str
    summary: str
    detail: dict[str, object]

    def __post_init__(self) -> None:
        if self.status not in PROBE_STATUSES:
            raise ProbeError(f"invalid probe status: {self.status}")

    @property
    def passed(self) -> bool:
        return self.status == PASS


@dataclass(frozen=True)
class ProbeSpec:
    probe_id: str
    title: str
    run: Callable[[ProbeContext], ProbeOutcome]
    os_category: str | None = None
    profiles: frozenset[str] = frozenset()
    options: frozenset[str] = frozenset()
    reads_evidence: bool = False


def _outcome(
    probe_id: str,
    status: str,
    summary: str,
    detail: Mapping[str, object] | None = None,
) -> ProbeOutcome:
    return ProbeOutcome(probe_id, status, summary, dict(detail or {}))


def _alias(value: object) -> str:
    """Return a stable non-reversible alias for a private name or path."""

    material = str(value).encode("utf-8", errors="replace")
    return hashlib.sha256(material).hexdigest()[:12]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _run(
    command: Sequence[str],
    *,
    cwd: Path,
    timeout: float = DEFAULT_COMMAND_TIMEOUT,
) -> tuple[int, str]:
    """Run one checked-in read-only command without a shell."""

    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except FileNotFoundError:
        return 127, ""
    except (OSError, subprocess.SubprocessError):
        return 126, ""
    return completed.returncode, (completed.stdout or "") + (completed.stderr or "")


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _load_json(path: Path, *, max_bytes: int = 4 * MEBIBYTE) -> object | None:
    try:
        with path.open("rb") as stream:
            raw = stream.read(max_bytes + 1)
    except OSError:
        return None
    if not raw or len(raw) > max_bytes:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _iter_files(root: Path, patterns: Sequence[str], *, limit: int) -> list[Path]:
    found: list[Path] = []
    if not root.exists():
        return found
    for pattern in patterns:
        for path in sorted(root.rglob(pattern)):
            if not path.is_file():
                continue
            found.append(path)
            if len(found) >= limit:
                return found
    return found


def _tree_totals(root: Path) -> dict[str, int]:
    files = 0
    total_bytes = 0
    if root.exists():
        for path in root.rglob("*"):
            if files >= MAX_TREE_ENTRIES:
                break
            try:
                if not path.is_file():
                    continue
                total_bytes += path.stat().st_size
            except OSError:
                continue
            files += 1
    return {"files": files, "bytes": total_bytes, "truncated": files >= MAX_TREE_ENTRIES}


# --------------------------------------------------------------------------
# Candidate and runtime identity
# --------------------------------------------------------------------------


def _probe_checkout_identity(context: ProbeContext) -> ProbeOutcome:
    try:
        result = verify_checkout(context.checkout, context.commit)
    except ReadinessError as exc:
        return _outcome(
            "checkout-identity",
            FAIL,
            "checkout is not the exact clean candidate",
            {"error": sanitize_text(str(exc), cwd=context.checkout)},
        )
    return _outcome(
        "checkout-identity",
        PASS,
        "checkout is clean and exactly at the candidate commit",
        {
            "commit_sha": str(result.get("commit_sha")),
            "repository_clean": True,
        },
    )


def _compose_containers(context: ProbeContext) -> list[dict[str, object]]:
    if context.runtime_binding is not None and context.runtime_binding.kind != "compose":
        return []
    command = ["docker", "compose"]
    cwd = context.checkout
    if context.runtime_binding is not None:
        command = context.runtime_binding.compose_prefix()
        cwd = context.runtime_binding.compose_working_directory or cwd
    command.extend(["ps", "--format", "json"])
    code, output = _run(
        command,
        cwd=cwd,
    )
    if code != 0 or not output.strip():
        return []
    containers: list[dict[str, object]] = []
    stripped = output.strip()
    payloads: list[object] = []
    try:
        parsed = json.loads(stripped)
        payloads = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        for line in stripped.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payloads.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    for payload in payloads:
        if isinstance(payload, dict):
            containers.append(payload)
    return containers


def _container_label(context: ProbeContext, container: str, label: str) -> str:
    cwd = context.checkout
    if context.runtime_binding is not None:
        cwd = context.runtime_binding.compose_working_directory or cwd
    code, output = _run(
        [
            "docker",
            "inspect",
            "--format",
            f'{{{{index .Config.Labels "{label}"}}}}',
            container,
        ],
        cwd=cwd,
    )
    if code != 0:
        return ""
    value = output.strip().splitlines()
    return value[-1].strip() if value else ""


def _container_matches_runtime_binding(
    context: ProbeContext,
    container: str,
) -> bool:
    binding = context.runtime_binding
    if binding is None or binding.kind != "compose":
        return True
    expected = binding.compose_working_directory
    if expected is None:
        return True
    observed = _container_label(
        context,
        container,
        "com.docker.compose.project.working_dir",
    )
    if not observed:
        return True
    return observed.rstrip("\\/").casefold() == str(expected).rstrip("\\/").casefold()


def _recorder_status_path(context: ProbeContext) -> Path:
    """The recorder status surface, preferring the explicitly bound file.

    Every recorder health, continuity and sample path routes through here.  A
    bound deployment keeps its status file outside the harness checkout, so
    reading the checkout-relative path would silently answer about a recorder
    this campaign is not testing -- stale, absent, or belonging to another
    deployment entirely.
    """

    binding = context.runtime_binding
    if binding is not None and binding.recorder_status_file is not None:
        return binding.recorder_status_file
    return context.data_dir / "source_state" / "mtconnect_recorder_status.json"


def _native_recorder_commit_identity(
    context: ProbeContext, detail: dict[str, object]
) -> ProbeOutcome:
    """Prove runtime identity for a recorder that runs without Docker.

    A native recorder has no container to carry a build label, so the bound
    status surface is the only identity it publishes.  Every failure mode here
    is closed: an unreadable surface, an absent native_runtime block, and a
    build commit that is missing, empty or different all FAIL.  Treating a
    missing identity as UNAVAILABLE would let an unidentifiable recorder pass
    the gate that exists to identify it.
    """

    status_file = _recorder_status_path(context)
    detail["runtime_kind"] = "native-recorder"
    detail["status_surface_bound"] = bool(
        context.runtime_binding is not None
        and context.runtime_binding.recorder_status_file is not None
    )
    payload = _load_json(status_file)
    if not isinstance(payload, dict):
        return _outcome(
            "running-commit-identity",
            FAIL,
            "the bound recorder status surface is missing or unreadable",
            detail,
        )
    runtime = payload.get("native_runtime")
    if not isinstance(runtime, dict):
        return _outcome(
            "running-commit-identity",
            FAIL,
            "the recorder status surface publishes no native runtime identity",
            detail,
        )
    recorder_commit = str(runtime.get("build_commit") or "").strip().casefold()
    detail["build_commit_present"] = bool(recorder_commit)
    state = runtime.get("state") or payload.get("state")
    if state is not None:
        detail["recorder_state"] = str(state)
    if not recorder_commit:
        return _outcome(
            "running-commit-identity",
            FAIL,
            "the native recorder does not publish a usable build identity",
            detail,
        )
    detail["build_commit_matches_candidate"] = recorder_commit == context.commit
    if recorder_commit != context.commit:
        return _outcome(
            "running-commit-identity",
            FAIL,
            "the native recorder does not carry the candidate build commit",
            detail,
        )
    return _outcome(
        "running-commit-identity",
        PASS,
        "the native recorder carries the candidate build commit",
        detail,
    )


def _probe_running_commit_identity(context: ProbeContext) -> ProbeOutcome:
    detail: dict[str, object] = {"candidate_sha": context.commit}
    binding = context.runtime_binding
    if binding is not None and binding.kind == "native-recorder":
        # Deliberately before the Docker probe: a native recorder must be
        # provable on a host that has no Docker or Compose at all.
        return _native_recorder_commit_identity(context, detail)
    if not _docker_available():
        return _outcome(
            "running-commit-identity",
            UNAVAILABLE,
            "Docker is not available on this host",
            detail,
        )
    containers = _compose_containers(context)
    if not containers:
        return _outcome(
            "running-commit-identity",
            UNAVAILABLE,
            "no Compose containers are running for this checkout",
            detail,
        )
    observed: list[dict[str, object]] = []
    mismatched = 0
    running = 0
    for container in containers:
        service = str(container.get("Service") or container.get("Name") or "")
        identifier = str(container.get("ID") or container.get("Name") or "")
        state = str(container.get("State") or "")
        if not identifier:
            continue
        if not _container_matches_runtime_binding(context, identifier):
            observed.append(
                {
                    "service": service,
                    "state": state,
                    "candidate_component": False,
                    "excluded_as_unbound_compose_container": True,
                }
            )
            continue
        commit = _container_label(context, identifier, BUILD_COMMIT_LABEL).casefold()
        matches = commit == context.commit
        if state.casefold() == "running":
            running += 1
        if not matches:
            mismatched += 1
        observed.append(
            {
                "service": service,
                "state": state,
                "build_commit_matches_candidate": matches,
                "build_commit_present": bool(commit),
            }
        )
    detail["containers"] = observed
    detail["running_containers"] = running

    status_file = _recorder_status_path(context)
    payload = _load_json(status_file)
    if isinstance(payload, dict):
        runtime = payload.get("native_runtime")
        if isinstance(runtime, dict):
            recorder_commit = str(runtime.get("build_commit") or "").casefold()
            detail["native_recorder_build_commit_matches"] = (
                recorder_commit == context.commit
            )
            if recorder_commit and recorder_commit != context.commit:
                mismatched += 1

    if mismatched:
        return _outcome(
            "running-commit-identity",
            FAIL,
            "a running component does not carry the candidate build commit",
            detail,
        )
    if not running:
        return _outcome(
            "running-commit-identity",
            UNAVAILABLE,
            "no container is running, so no runtime identity can be proven",
            detail,
        )
    return _outcome(
        "running-commit-identity",
        PASS,
        "every running component carries the candidate build commit",
        detail,
    )


def _probe_update_status(context: ProbeContext) -> ProbeOutcome:
    """Report the update surface's own non-mutating verdict on this checkout.

    The probe never fetches, so it touches no network and mutates nothing. What
    it proves is that the update path answers deterministically about this exact
    checkout and does not offer to mutate a checkout it should refuse: a dirty
    tree or an unapproved remote is a real finding, while a candidate pinned off
    approved main is correctly refused rather than silently updated.
    """

    try:
        from catalog.federation.software_update import GitUpdateAdapter
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "update-status",
            UNAVAILABLE,
            "the update surface is not importable from this checkout",
            {},
        )
    detail: dict[str, object] = {"network_fetch_performed": False}
    try:
        verify_checkout(context.checkout, context.commit)
        detail["checkout_is_exact_clean_candidate"] = True
    except ReadinessError as exc:
        return _outcome(
            "update-status",
            FAIL,
            "the update surface is being asked about the wrong checkout",
            {**detail, "error": sanitize_text(str(exc), cwd=context.checkout)},
        )

    adapter = GitUpdateAdapter(context.checkout)
    failure, current = adapter.checkout_baseline()
    detail["baseline_code"] = None if failure is None else failure.code
    detail["current_commit_matches_candidate"] = (
        str(current or "").casefold() == context.commit
    )
    if failure is None:
        inspection = adapter.inspect(fetch=False)
        detail["state"] = inspection.state
        detail["code"] = inspection.code
        detail["mutation_refused"] = False
        if inspection.state in {"error", "unsupported_checkout"}:
            return _outcome(
                "update-status",
                UNAVAILABLE,
                "the update surface cannot describe this checkout shape",
                detail,
            )
        return _outcome(
            "update-status",
            PASS,
            f"update status is deterministic and reports {inspection.state}",
            detail,
        )

    detail["state"] = failure.state
    detail["mutation_refused"] = True
    if failure.code == "dirty":
        return _outcome(
            "update-status",
            FAIL,
            "the update surface reports an unclean checkout",
            detail,
        )
    if failure.code == "unapproved_remote":
        return _outcome(
            "update-status",
            FAIL,
            "this checkout does not track the approved update repository",
            detail,
        )
    if failure.code == "detached_head":
        return _outcome(
            "update-status",
            PASS,
            "the update surface refuses to mutate this pinned candidate checkout",
            detail,
        )
    return _outcome(
        "update-status",
        UNAVAILABLE,
        "the update surface cannot describe this checkout shape",
        detail,
    )


def _probe_update_entrypoint_disposition(context: ProbeContext) -> ProbeOutcome:
    script = context.checkout / "update.cmd"
    if not script.is_file():
        return _outcome(
            "update-entrypoint-disposition",
            FAIL,
            "update.cmd is missing from the candidate",
            {},
        )
    text = script.read_text(encoding="utf-8", errors="replace")
    retired = "retired" in text.casefold()
    refuses = "exit /b 2" in text.casefold()
    detail: dict[str, object] = {
        "declares_retired": retired,
        "refuses_with_exit_code_2": refuses,
    }
    if not (retired and refuses):
        return _outcome(
            "update-entrypoint-disposition",
            FAIL,
            "update.cmd does not match its approved retired disposition",
            detail,
        )
    if context.os_category == "windows":
        code, _output = _run(["cmd", "/c", str(script)], cwd=context.checkout)
        detail["observed_exit_code"] = code
        try:
            verify_checkout(context.checkout, context.commit)
            detail["checkout_unchanged"] = True
        except ReadinessError:
            detail["checkout_unchanged"] = False
        if code != 2 or not detail["checkout_unchanged"]:
            return _outcome(
                "update-entrypoint-disposition",
                FAIL,
                "update.cmd did not refuse without mutating the checkout",
                detail,
            )
    return _outcome(
        "update-entrypoint-disposition",
        PASS,
        "update.cmd follows its approved retired disposition",
        detail,
    )


# --------------------------------------------------------------------------
# Docker and host resources
# --------------------------------------------------------------------------


def _docker_backing_path(context: ProbeContext) -> Path | None:
    try:
        from catalog.federation.docker_resources import docker_backing_resource_path
    except ImportError:  # pragma: no cover - product package always present
        return None
    try:
        return docker_backing_resource_path(context.checkout)
    except (OSError, RuntimeError, ValueError):
        return None


def _probe_docker_state(context: ProbeContext) -> ProbeOutcome:
    if not _docker_available():
        return _outcome(
            "docker-state",
            UNAVAILABLE,
            "Docker is not available on this host",
            {},
        )
    code, usage = _run(["docker", "system", "df"], cwd=context.checkout)
    detail: dict[str, object] = {
        "system_df_returncode": code,
        "system_df": sanitize_text(usage, cwd=context.checkout),
    }
    containers = _compose_containers(context)
    detail["compose_container_count"] = len(containers)
    detail["compose_states"] = sorted(
        {str(item.get("State") or "unknown") for item in containers}
    )
    backing = _docker_backing_path(context)
    detail["backing_resource_proven"] = backing is not None
    if backing is not None:
        detail["backing_resource_alias"] = _alias(backing)
    if code != 0:
        return _outcome(
            "docker-state",
            UNAVAILABLE,
            "Docker did not answer a read-only state query",
            detail,
        )
    if backing is None:
        return _outcome(
            "docker-state",
            FAIL,
            "the host resource backing Docker persistent writes is unproven",
            detail,
        )
    return _outcome(
        "docker-state",
        PASS,
        "Docker state and its backing host resource are both observable",
        detail,
    )


def _measure(path: Path) -> dict[str, object] | None:
    try:
        from catalog.federation.host_resources import (
            PressureThresholds,
            assess_measurement,
            measure_filesystem,
        )
    except ImportError:  # pragma: no cover - product package always present
        return None
    measurement = measure_filesystem(path)
    if not measurement.available:
        return {
            "available": False,
            "error_code": measurement.error_code,
        }
    assessment = assess_measurement(
        measurement,
        thresholds=PressureThresholds(),
        reserved_bytes=0,
        reserved_inodes=0,
        now=_utc_now(),
    )
    return {
        "available": True,
        "resource_alias": _alias(measurement.resource_id),
        "total_bytes": measurement.total_bytes,
        "free_bytes": measurement.free_bytes,
        "total_inodes": measurement.total_inodes,
        "free_inodes": measurement.free_inodes,
        "pressure_level": int(assessment.level),
        "pressure_reasons": list(assessment.reasons),
    }


def _baseline_anchors(context: ProbeContext) -> dict[str, Path]:
    anchors: dict[str, Path] = {
        "data": context.data_dir,
        "results": context.results_dir,
        "logs": context.data_dir / "source_state",
    }
    # The harness checkout is only a baseline resource when it is also where
    # the runtime lives.  Under an explicit binding it is separate storage and
    # measuring it would report headroom the product cannot use.
    if not context.roots_are_bound:
        anchors["checkout"] = context.checkout
    backing = _docker_backing_path(context)
    if backing is not None:
        anchors["docker"] = backing
        anchors["model_storage"] = backing
    return anchors


def _probe_host_resource_baseline(context: ProbeContext) -> ProbeOutcome:
    anchors = _baseline_anchors(context)
    measured: dict[str, object] = {}
    for name, path in anchors.items():
        result = _measure(path)
        if result is None:
            return _outcome(
                "host-resource-baseline",
                UNAVAILABLE,
                "host resource measurement is not importable from this checkout",
                {},
            )
        result["exists"] = path.exists()
        measured[name] = result
    detail: dict[str, object] = {"resources": measured}
    detail["runtime_roots_bound"] = context.roots_are_bound
    required = (
        ("data", "results", "logs")
        if context.roots_are_bound
        else ("checkout", "data", "results", "logs")
    )
    unavailable = [
        name
        for name in required
        if not bool(dict(measured[name]).get("available"))
    ]
    detail["unmeasured"] = unavailable
    if unavailable:
        return _outcome(
            "host-resource-baseline",
            FAIL,
            "a required backing resource could not be measured",
            detail,
        )
    if "docker" not in measured:
        return _outcome(
            "host-resource-baseline",
            UNAVAILABLE,
            "the Docker/model backing resource is unproven on this host",
            detail,
        )
    if context.os_category != "windows":
        inodes_seen = any(
            dict(value).get("total_inodes") is not None for value in measured.values()
        )
        detail["inode_accounting_observed"] = inodes_seen
        if not inodes_seen:
            return _outcome(
                "host-resource-baseline",
                FAIL,
                "POSIX baseline is missing inode accounting",
                detail,
            )
    return _outcome(
        "host-resource-baseline",
        PASS,
        "host, Docker, data/results, logs and model storage are all measured",
        detail,
    )


def _probe_inode_capacity(context: ProbeContext) -> ProbeOutcome:
    anchor = context.storage_anchor()
    if anchor is None:
        return _outcome(
            "inode-capacity",
            FAIL,
            "the bound data root does not exist, so inode capacity is unproven",
            {"bound_data_root_present": False},
        )
    result = _measure(anchor)
    if result is None:
        return _outcome(
            "inode-capacity",
            UNAVAILABLE,
            "host resource measurement is not importable from this checkout",
            {},
        )
    detail = {"resource": result}
    if not result.get("available"):
        return _outcome(
            "inode-capacity",
            UNAVAILABLE,
            "the filesystem could not be measured",
            detail,
        )
    if result.get("total_inodes") is None:
        return _outcome(
            "inode-capacity",
            UNAVAILABLE,
            "this filesystem does not expose inode accounting",
            detail,
        )
    return _outcome(
        "inode-capacity",
        PASS,
        "the filesystem exposes inode/file capacity accounting",
        detail,
    )


def _probe_safe_storage_refusal(context: ProbeContext) -> ProbeOutcome:
    try:
        from catalog.federation.host_resources import (
            HostResourceRefused,
            PressureLevel,
            ProcessResourceAdmission,
        )
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "safe-storage-refusal",
            UNAVAILABLE,
            "the resource admission surface is not importable",
            {},
        )
    anchor = context.storage_anchor()
    if anchor is None:
        return _outcome(
            "safe-storage-refusal",
            FAIL,
            "the bound data root does not exist, so admission cannot be exercised",
            {"bound_data_root_present": False},
        )
    admission = ProcessResourceAdmission()
    before = admission.assessment(anchor)
    detail: dict[str, object] = {
        "resource_alias": _alias(before.resource_id),
        "pressure_level": int(before.level),
        "bytes_written": 0,
    }
    if before.effective_free_bytes is None:
        return _outcome(
            "safe-storage-refusal",
            UNAVAILABLE,
            "free capacity could not be measured, so no refusal can be induced",
            detail,
        )
    impossible = int(before.effective_free_bytes) + GIBIBYTE
    refused_code: str | None = None
    try:
        with admission.reserve(anchor, bytes_required=impossible):
            pass
    except HostResourceRefused as exc:
        refused_code = str(exc.args[0]) if exc.args else "refused"
    detail["impossible_reservation_refused"] = refused_code is not None
    detail["refusal_code"] = refused_code
    if refused_code is None:
        return _outcome(
            "safe-storage-refusal",
            FAIL,
            "an impossible reservation was admitted instead of refused",
            detail,
        )

    recovered = False
    recovery_code: str | None = None
    try:
        with admission.reserve(anchor, bytes_required=1):
            recovered = True
    except HostResourceRefused as exc:
        recovery_code = str(exc.args[0]) if exc.args else "refused"
    detail["small_reservation_admitted"] = recovered
    detail["small_reservation_refusal_code"] = recovery_code
    if recovered:
        return _outcome(
            "safe-storage-refusal",
            PASS,
            "oversized work is refused while ordinary work is still admitted",
            detail,
        )
    if before.level >= PressureLevel.PRESSURE:
        return _outcome(
            "safe-storage-refusal",
            PASS,
            "the resource is genuinely under pressure and refuses new work",
            detail,
        )
    return _outcome(
        "safe-storage-refusal",
        FAIL,
        "ordinary work was refused on a resource that is not under pressure",
        detail,
    )


def _probe_model_pull_floor(context: ProbeContext) -> ProbeOutcome:
    try:
        from catalog.federation.host_resources import (
            HostResourceRefused,
            PressureThresholds,
            ProcessResourceAdmission,
        )
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "model-pull-floor",
            UNAVAILABLE,
            "the resource admission surface is not importable",
            {},
        )
    backing = _docker_backing_path(context)
    if backing is None:
        return _outcome(
            "model-pull-floor",
            UNAVAILABLE,
            "the Docker/model backing resource is unproven on this host",
            {"backing_resource_proven": False},
        )
    thresholds = PressureThresholds()
    admission = ProcessResourceAdmission(thresholds=thresholds)
    assessment = admission.assessment(backing)
    detail: dict[str, object] = {
        "resource_alias": _alias(assessment.resource_id),
        "pressure_level": int(assessment.level),
        "bytes_downloaded": 0,
    }
    if assessment.effective_free_bytes is None:
        return _outcome(
            "model-pull-floor",
            UNAVAILABLE,
            "the model backing resource could not be measured",
            detail,
        )
    # A model pull that would leave less than the shared emergency reserve must
    # be refused before any download starts. Asking for exactly that much proves
    # the floor without writing a byte.
    across_floor = int(assessment.effective_free_bytes)
    refused: str | None = None
    try:
        with admission.reserve(backing, bytes_required=across_floor):
            pass
    except HostResourceRefused as exc:
        refused = str(exc.args[0]) if exc.args else "refused"
    detail["crossing_reservation_refused"] = refused is not None
    detail["refusal_code"] = refused
    if refused is None:
        return _outcome(
            "model-pull-floor",
            FAIL,
            "a model-sized write crossing the emergency floor was admitted",
            detail,
        )
    return _outcome(
        "model-pull-floor",
        PASS,
        "model-sized writes are refused before the host emergency floor",
        detail,
    )


# --------------------------------------------------------------------------
# Database integrity and durable progress
# --------------------------------------------------------------------------


def _sqlite_candidates(context: ProbeContext) -> list[Path]:
    root = context.path_option("path") or context.data_dir
    return _iter_files(
        root,
        ("*.sqlite3", "*.sqlite", "*.db"),
        limit=MAX_SQLITE_FILES,
    )


def _read_only_connection(path: Path) -> sqlite3.Connection:
    """Open one database read-only.

    ``as_uri`` gives the percent-encoded ``file:///`` form SQLite accepts on both
    Windows drive letters and POSIX paths, so a database under a path with a
    space or a drive prefix is still opened rather than silently reported as an
    integrity failure.
    """

    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    connection.row_factory = sqlite3.Row
    return connection


def _probe_sqlite_integrity(context: ProbeContext) -> ProbeOutcome:
    candidates = _sqlite_candidates(context)
    detail: dict[str, object] = {"database_count": len(candidates)}
    if not candidates:
        return _outcome(
            "sqlite-integrity",
            UNAVAILABLE,
            "no SQLite database was found to check",
            detail,
        )
    results: list[dict[str, object]] = []
    failures = 0
    for path in candidates:
        entry: dict[str, object] = {"database_alias": _alias(path.name)}
        try:
            with _read_only_connection(path) as connection:
                row = connection.execute("PRAGMA quick_check(1)").fetchone()
                verdict = str(row[0]) if row else "unknown"
                entry["quick_check"] = verdict
                entry["ok"] = verdict.casefold() == "ok"
        except sqlite3.Error as exc:
            entry["ok"] = False
            entry["error"] = sanitize_text(str(exc), cwd=context.checkout)
        if not entry.get("ok"):
            failures += 1
        results.append(entry)
    detail["databases"] = results
    detail["failed"] = failures
    if failures:
        return _outcome(
            "sqlite-integrity",
            FAIL,
            "a database did not pass its read-only integrity check",
            detail,
        )
    return _outcome(
        "sqlite-integrity",
        PASS,
        f"{len(results)} database(s) passed a read-only quick check",
        detail,
    )


def _table_exists(connection: sqlite3.Connection, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _outbox_databases(context: ProbeContext) -> list[Path]:
    found: list[Path] = []
    for path in _sqlite_candidates(context):
        try:
            with _read_only_connection(path) as connection:
                if _table_exists(connection, "outbox"):
                    found.append(path)
        except sqlite3.Error:
            continue
    return found


def _probe_publication_backlog(context: ProbeContext) -> ProbeOutcome:
    databases = _outbox_databases(context)
    detail: dict[str, object] = {"outbox_database_count": len(databases)}
    status_path = (
        context.data_dir
        / "source_state"
        / "mtconnect_recorder_publication_status.json"
    )
    payload = _load_json(status_path)
    if isinstance(payload, dict):
        detail["publication_status"] = {
            "status": str(payload.get("status") or ""),
            "storage_state": str(payload.get("storage_state") or ""),
            "pending_batches": payload.get("pending_batches"),
            "last_committed_count": payload.get("last_committed_count"),
        }
    if not databases:
        if "publication_status" in detail:
            return _outcome(
                "publication-backlog",
                PASS,
                "publication progress is visible through the recorder status file",
                detail,
            )
        return _outcome(
            "publication-backlog",
            UNAVAILABLE,
            "no publication outbox or status surface was found",
            detail,
        )
    states: list[dict[str, object]] = []
    total = 0
    for path in databases:
        counts: dict[str, int] = {}
        try:
            with _read_only_connection(path) as connection:
                for row in connection.execute(
                    "SELECT state, count(*) AS total FROM outbox GROUP BY state"
                ):
                    counts[str(row["state"])] = int(row["total"])
        except sqlite3.Error as exc:
            return _outcome(
                "publication-backlog",
                FAIL,
                "a publication outbox could not be read",
                {
                    **detail,
                    "error": sanitize_text(str(exc), cwd=context.checkout),
                },
            )
        total += sum(counts.values())
        states.append({"database_alias": _alias(path.name), "states": counts})
    detail["outboxes"] = states
    detail["total_entries"] = total
    return _outcome(
        "publication-backlog",
        PASS,
        "publication backlog and progress state are readable",
        detail,
    )


def _probe_publication_idempotency(context: ProbeContext) -> ProbeOutcome:
    databases = _outbox_databases(context)
    detail: dict[str, object] = {"outbox_database_count": len(databases)}
    if not databases:
        return _outcome(
            "publication-idempotency",
            UNAVAILABLE,
            "no publication outbox was found",
            detail,
        )
    checked: list[dict[str, object]] = []
    duplicates = 0
    for path in databases:
        entry: dict[str, object] = {"database_alias": _alias(path.name)}
        try:
            with _read_only_connection(path) as connection:
                row = connection.execute(
                    "SELECT count(*) AS rows_total,"
                    " count(DISTINCT session_id || char(31) || destination_id"
                    " || char(31) || idempotency_key) AS distinct_total"
                    " FROM outbox"
                ).fetchone()
                rows_total = int(row["rows_total"])
                distinct_total = int(row["distinct_total"])
                entry["rows"] = rows_total
                entry["distinct_identities"] = distinct_total
                entry["duplicates"] = rows_total - distinct_total
                index_row = connection.execute(
                    "SELECT count(*) AS total FROM sqlite_master"
                    " WHERE type='index' AND tbl_name='outbox'"
                    " AND sql LIKE '%idempotency_key%'"
                ).fetchone()
                unique_declared = int(index_row["total"]) > 0
                entry["unique_identity_index"] = unique_declared
        except sqlite3.Error as exc:
            entry["error"] = sanitize_text(str(exc), cwd=context.checkout)
            entry["duplicates"] = -1
        if int(entry.get("duplicates", 0)) != 0:
            duplicates += 1
        checked.append(entry)
    detail["outboxes"] = checked
    if duplicates:
        return _outcome(
            "publication-idempotency",
            FAIL,
            "a publication outbox contains duplicate durable identities",
            detail,
        )
    return _outcome(
        "publication-idempotency",
        PASS,
        "durable publication identities remain unique",
        detail,
    )


# --------------------------------------------------------------------------
# Worker, process and recorder health
# --------------------------------------------------------------------------


def _restart_states(context: ProbeContext) -> dict[str, object]:
    try:
        from catalog.federation.service_incarnation import (
            incarnation_state_file,
            read_restart_state,
        )
    except ImportError:  # pragma: no cover - product package always present
        return {}
    states: dict[str, object] = {}
    for service in ("flask", "recorder", "relay"):
        state = read_restart_state(
            incarnation_state_file(context.data_dir, service),
            service=service,
            now=_utc_now(),
        )
        states[service] = {
            "state": state.state,
            "consecutive_unclean": state.consecutive_unclean,
        }
    return states


def _recorder_status(context: ProbeContext) -> dict[str, object] | None:
    try:
        from catalog.mtconnect_recorder.native_update import read_recorder_status
    except ImportError:  # pragma: no cover - product package always present
        return None
    status_file = _recorder_status_path(context)
    if not status_file.is_file():
        return None
    status = read_recorder_status(status_file)
    return {
        "present": status.present,
        "heartbeat_fresh": status.is_fresh(now=_utc_now()),
        "state": status.state,
        "federation_status": status.federation_status,
        "build_commit_present": bool(status.build_commit),
    }


def _probe_service_health(context: ProbeContext) -> ProbeOutcome:
    detail: dict[str, object] = {"restart_states": _restart_states(context)}
    recorder = _recorder_status(context)
    if recorder is not None:
        detail["recorder"] = recorder
    containers = _compose_containers(context) if _docker_available() else []
    detail["compose_container_count"] = len(containers)
    running: list[str] = []
    unhealthy: list[str] = []
    for container in containers:
        service = str(container.get("Service") or container.get("Name") or "unknown")
        state = str(container.get("State") or "").casefold()
        if state == "running":
            running.append(service)
        else:
            unhealthy.append(service)
    detail["running_services"] = sorted(running)
    detail["not_running_services"] = sorted(unhealthy)
    if not containers and recorder is None:
        return _outcome(
            "service-health",
            UNAVAILABLE,
            "no running service surface was found on this host",
            detail,
        )
    crash_looping = [
        name
        for name, value in dict(detail["restart_states"]).items()
        if isinstance(value, dict) and str(value.get("state")) == "crash_looping"
    ]
    detail["crash_looping"] = crash_looping
    if unhealthy or crash_looping:
        return _outcome(
            "service-health",
            FAIL,
            "a required service is not running or is crash looping",
            detail,
        )
    if recorder is not None and recorder["present"] and not recorder["heartbeat_fresh"]:
        return _outcome(
            "service-health",
            FAIL,
            "the recorder is registered but its heartbeat is stale",
            detail,
        )
    return _outcome(
        "service-health",
        PASS,
        "every observable core worker is alive and its health is visible",
        detail,
    )


def _probe_core_availability(context: ProbeContext) -> ProbeOutcome:
    health = _probe_service_health(context)
    # Keep the summary rather than nesting the whole health detail: the
    # service-health probe records its own packet, and a deeply nested copy only
    # makes this evidence harder to read.
    detail: dict[str, object] = {
        "service_health": health.status,
        "service_health_summary": health.summary,
        "running_services": health.detail.get("running_services", []),
        "not_running_services": health.detail.get("not_running_services", []),
        "crash_looping": health.detail.get("crash_looping", []),
    }
    reads = 0
    failures = 0
    for path in _sqlite_candidates(context):
        try:
            with _read_only_connection(path) as connection:
                connection.execute("SELECT count(*) FROM sqlite_master").fetchone()
            reads += 1
        except sqlite3.Error:
            failures += 1
    detail["committed_reads_ok"] = reads
    detail["committed_reads_failed"] = failures
    if health.status == UNAVAILABLE and not reads:
        return _outcome(
            "core-availability",
            UNAVAILABLE,
            "neither a service surface nor a committed read is observable",
            detail,
        )
    if failures or health.status == FAIL:
        return _outcome(
            "core-availability",
            FAIL,
            "the healthy core did not remain readable and available",
            detail,
        )
    return _outcome(
        "core-availability",
        PASS,
        "committed reads and core control surfaces remain available",
        detail,
    )


def _checkpoint_payload(context: ProbeContext) -> dict[str, object] | None:
    state_file = context.data_dir / "source_state" / "mtconnect_recorder_state.json"
    payload = _load_json(state_file)
    if not isinstance(payload, dict):
        return None
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return None
    summary: dict[str, object] = {}
    for name, item in sorted(sources.items()):
        if not isinstance(item, dict):
            continue
        try:
            summary[_alias(name)] = {
                "agent_instance_id": int(item["agent_instance_id"]),
                "next_sequence": int(item["next_sequence"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return summary or None


def _probe_recorder_continuity(context: ProbeContext) -> ProbeOutcome:
    checkpoints = _checkpoint_payload(context)
    recorder = _recorder_status(context)
    detail: dict[str, object] = {}
    if recorder is not None:
        detail["recorder"] = recorder
    if checkpoints is None:
        return _outcome(
            "recorder-continuity",
            UNAVAILABLE,
            "no durable recorder checkpoint exists on this host",
            detail,
        )
    detail["checkpoints"] = checkpoints
    detail["source_count"] = len(checkpoints)
    earlier = _recorded_checkpoints(context)
    if earlier is None:
        return _outcome(
            "recorder-continuity",
            PASS,
            "durable recorder checkpoints exist and are readable",
            detail,
        )
    regressions: list[str] = []
    for alias, current in checkpoints.items():
        previous = earlier.get(alias)
        if not isinstance(previous, dict):
            continue
        if int(current["next_sequence"]) < int(previous.get("next_sequence", 0)):
            regressions.append(alias)
    detail["compared_against_earlier_evidence"] = True
    detail["sequence_regressions"] = regressions
    if regressions:
        return _outcome(
            "recorder-continuity",
            FAIL,
            "a recorder source lost durable sequence progress",
            detail,
        )
    return _outcome(
        "recorder-continuity",
        PASS,
        "recorder checkpoint continuity held against earlier campaign evidence",
        detail,
    )


def _probe_recorder_limits(context: ProbeContext) -> ProbeOutcome:
    try:
        from catalog.mtconnect_recorder import limits
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "recorder-limits",
            UNAVAILABLE,
            "recorder limits are not importable from this checkout",
            {},
        )
    names = (
        "MAX_CURRENT_RESPONSE_BYTES",
        "MAX_PROBE_RESPONSE_BYTES",
        "MAX_SAMPLE_RESPONSE_BYTES",
        "MAX_REQUEST_DEADLINE_SECONDS",
        "MAX_OBSERVATIONS_PER_BATCH",
        "MAX_SEQUENCE_SPAN",
        "MAX_XML_ELEMENTS",
        "MAX_XML_DEPTH",
        "MAX_OBSERVATION_ARCHIVE_BYTES",
    )
    declared: dict[str, object] = {}
    unbounded: list[str] = []
    for name in names:
        value = getattr(limits, name, None)
        declared[name] = value
        if not isinstance(value, (int, float)) or value <= 0:
            unbounded.append(name)
    detail = {"declared_limits": declared, "unbounded": unbounded}
    if unbounded:
        return _outcome(
            "recorder-limits",
            FAIL,
            "a recorder ingress bound is missing or non-positive",
            detail,
        )
    return _outcome(
        "recorder-limits",
        PASS,
        "every recorder ingress and transaction bound is finite",
        detail,
    )


def _probe_recorder_path_containment(context: ProbeContext) -> ProbeOutcome:
    try:
        from catalog.mtconnect_recorder.storage import _confined_storage_path
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "recorder-path-containment",
            UNAVAILABLE,
            "recorder storage confinement is not importable",
            {},
        )
    root = context.data_dir / "raw"
    hostile = (
        ("..", "escaped.json"),
        ("../..", "escaped.json"),
        ("2026-01-01", "..", "..", "escaped.json"),
    )
    escapes: list[str] = []
    for index, components in enumerate(hostile):
        # Any refusal is the contract; only a returned path is an escape.
        confined: Path | None = None
        try:
            confined = _confined_storage_path(root, *components)
        except Exception as exc:  # noqa: BLE001 - refusal type is not the contract
            _ = exc
        if confined is not None:
            escapes.append(f"case-{index}")
    detail = {"hostile_cases": len(hostile), "escaped": escapes}
    if escapes:
        return _outcome(
            "recorder-path-containment",
            FAIL,
            "a malformed path component escaped the recorder durable root",
            detail,
        )
    return _outcome(
        "recorder-path-containment",
        PASS,
        "malformed path components cannot escape the recorder durable root",
        detail,
    )


def _probe_bounded_logs(context: ProbeContext) -> ProbeOutcome:
    ceiling = context.int_option("max_log_bytes", 512 * MEBIBYTE)
    roots = [context.data_dir, context.results_dir]
    oversized: list[dict[str, object]] = []
    inspected = 0
    for root in roots:
        for path in _iter_files(root, ("*.log", "*.log.*"), limit=MAX_LOG_FILES):
            try:
                size = path.stat().st_size
            except OSError:
                continue
            inspected += 1
            if size > ceiling:
                oversized.append({"log_alias": _alias(path.name), "bytes": size})
    detail: dict[str, object] = {
        "log_files_inspected": inspected,
        "max_log_bytes": ceiling,
        "oversized": oversized,
    }
    health = _probe_service_health(context)
    detail["service_health"] = health.status
    if oversized:
        return _outcome(
            "bounded-logs-and-health",
            FAIL,
            "a log file grew past the declared bound",
            detail,
        )
    if health.status == UNAVAILABLE and not inspected:
        return _outcome(
            "bounded-logs-and-health",
            UNAVAILABLE,
            "neither logs nor a service surface are observable",
            detail,
        )
    if health.status == FAIL:
        return _outcome(
            "bounded-logs-and-health",
            FAIL,
            "service health is visible but reports a failed core service",
            detail,
        )
    return _outcome(
        "bounded-logs-and-health",
        PASS,
        "logs stay bounded and core health remains visible",
        detail,
    )


def _probe_host_mutation_serialization(context: ProbeContext) -> ProbeOutcome:
    try:
        from catalog.federation.host_mutation import (
            HostMutationLockError,
            host_mutation_lock,
        )
    except ImportError:  # pragma: no cover - product package always present
        return _outcome(
            "host-mutation-serialization",
            UNAVAILABLE,
            "the host mutation boundary is not importable",
            {},
        )

    outcome: dict[str, object] = {"second_actor_refused": False}
    errors: list[str] = []

    def _second_actor() -> None:
        try:
            with host_mutation_lock(context.checkout, timeout_seconds=0.5):
                outcome["second_actor_refused"] = False
        except (HostMutationLockError, RuntimeError) as exc:
            outcome["second_actor_refused"] = True
            outcome["refusal_code"] = sanitize_text(str(exc), cwd=context.checkout)
        except (OSError, ValueError) as exc:  # pragma: no cover - defensive
            errors.append(sanitize_text(str(exc), cwd=context.checkout))

    try:
        with host_mutation_lock(context.checkout, timeout_seconds=5.0):
            worker = threading.Thread(target=_second_actor, daemon=True)
            worker.start()
            worker.join(timeout=30.0)
            outcome["second_actor_completed"] = not worker.is_alive()
    except (HostMutationLockError, RuntimeError, OSError, ValueError) as exc:
        return _outcome(
            "host-mutation-serialization",
            UNAVAILABLE,
            "the host mutation lock is not usable on this host",
            {"error": sanitize_text(str(exc), cwd=context.checkout)},
        )

    reacquired = False
    try:
        with host_mutation_lock(context.checkout, timeout_seconds=5.0):
            reacquired = True
    except (HostMutationLockError, RuntimeError, OSError, ValueError):
        reacquired = False
    outcome["lock_released_after_use"] = reacquired
    if errors:
        outcome["errors"] = errors
    if not outcome.get("second_actor_completed"):
        return _outcome(
            "host-mutation-serialization",
            FAIL,
            "a competing host actor neither acquired nor was refused in time",
            outcome,
        )
    if not outcome["second_actor_refused"]:
        return _outcome(
            "host-mutation-serialization",
            FAIL,
            "two actors mutated the host checkout at the same time",
            outcome,
        )
    if not reacquired:
        return _outcome(
            "host-mutation-serialization",
            FAIL,
            "the host mutation lease was not released after use",
            outcome,
        )
    return _outcome(
        "host-mutation-serialization",
        PASS,
        "concurrent host mutation is serialized and the lease is released",
        outcome,
    )


# --------------------------------------------------------------------------
# Corpus, series and growth evidence
# --------------------------------------------------------------------------


def _probe_corpus_size(context: ProbeContext) -> ProbeOutcome:
    subject = context.option("subject", "recorder")
    if subject not in {"recorder", "history"}:
        raise ProbeError("option subject must be recorder or history")
    minimum_files = context.int_option("min_files", 100)
    minimum_bytes = context.int_option("min_bytes", MEBIBYTE)
    if subject == "recorder":
        roots = [context.data_dir / "raw", context.data_dir / "observations"]
    else:
        roots = [context.data_dir]
    totals = {"files": 0, "bytes": 0}
    for root in roots:
        measured = _tree_totals(root)
        totals["files"] += int(measured["files"])
        totals["bytes"] += int(measured["bytes"])
    databases = _sqlite_candidates(context)
    detail: dict[str, object] = {
        "subject": subject,
        "files": totals["files"],
        "bytes": totals["bytes"],
        "database_count": len(databases),
        "min_files": minimum_files,
        "min_bytes": minimum_bytes,
    }
    if totals["files"] == 0 and not databases:
        return _outcome(
            "corpus-size",
            UNAVAILABLE,
            "no corpus or history surface exists on this host yet",
            detail,
        )
    if totals["files"] < minimum_files or totals["bytes"] < minimum_bytes:
        return _outcome(
            "corpus-size",
            FAIL,
            "the installation does not carry a non-trivial aged corpus",
            detail,
        )
    return _outcome(
        "corpus-size",
        PASS,
        "the installation carries a non-trivial aged corpus",
        detail,
    )


def collect_sample_extras(
    checkout: Path,
    runtime_binding: RuntimeBinding | None = None,
) -> dict[str, object]:
    """Gather the extra series a P12 soak sample must carry.

    This is deliberately the same read-only material the individual probes use,
    reduced to counters so a 24-hour sample series stays small and redactable.
    """

    context = ProbeContext(
        checkout=checkout,
        evidence_root=checkout,
        commit="0" * 40,
        host_id="sample",
        os_category="windows" if platform.system().casefold() == "windows" else "posix",
        profile="unspecified",
        scenario="P12",
        assertion="storage-series",
        runtime_binding=runtime_binding,
    )
    extras: dict[str, object] = {}

    recorder = _recorder_status(context)
    checkpoints = _checkpoint_payload(context) or {}
    corpus = _tree_totals(context.data_dir / "raw")
    extras["recorder"] = {
        "status_present": recorder is not None,
        "heartbeat_fresh": bool(recorder and recorder.get("heartbeat_fresh")),
        "source_count": len(checkpoints),
        "next_sequence_total": sum(
            int(dict(value).get("next_sequence", 0)) for value in checkpoints.values()
        ),
        "raw_files": corpus["files"],
        "raw_bytes": corpus["bytes"],
    }

    backlog = _probe_publication_backlog(context)
    extras["publication"] = {
        "status": backlog.status,
        "total_entries": backlog.detail.get("total_entries", 0),
        "outbox_database_count": backlog.detail.get("outbox_database_count", 0),
    }

    history: dict[str, object] = {}
    for path in _sqlite_candidates(context):
        try:
            size = path.stat().st_size
        except OSError:
            continue
        history[_alias(path.name)] = {"bytes": size}
    extras["history"] = {"databases": history, "database_count": len(history)}

    staging = _tree_totals(context.data_dir / "staging")
    workspaces = _tree_totals(context.data_dir / "workspaces")
    extras["orphan"] = {
        "staging_files": staging["files"],
        "staging_bytes": staging["bytes"],
        "workspace_files": workspaces["files"],
        "workspace_bytes": workspaces["bytes"],
        "restart_states": _restart_states(context),
    }

    extras["cpu_ram"] = _cpu_ram_snapshot()
    return extras


def _cpu_ram_snapshot() -> dict[str, object]:
    snapshot: dict[str, object] = {"cpu_count": os.cpu_count()}
    if hasattr(os, "getloadavg"):
        try:
            one, five, fifteen = os.getloadavg()
            snapshot["load_average"] = [
                round(one, 3),
                round(five, 3),
                round(fifteen, 3),
            ]
        except OSError:
            pass
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        values: dict[str, int] = {}
        try:
            for line in meminfo.read_text(encoding="utf-8").splitlines():
                key, _, rest = line.partition(":")
                if key in {"MemTotal", "MemAvailable"}:
                    digits = rest.strip().split(" ")[0]
                    if digits.isdigit():
                        values[key] = int(digits) * 1024
        except OSError:
            values = {}
        if values:
            snapshot["memory_total_bytes"] = values.get("MemTotal")
            snapshot["memory_available_bytes"] = values.get("MemAvailable")
    return snapshot


SERIES_REQUIREMENTS: Final[dict[str, tuple[str, ...]]] = {
    "storage": ("resources",),
    "docker": ("docker",),
    "recorder": ("extras", "recorder"),
    "publication": ("extras", "publication"),
    "history": ("extras", "history"),
    "orphan": ("extras", "orphan"),
    "cpu_ram": ("extras", "cpu_ram"),
}


def _sample_packets(context: ProbeContext) -> list[dict[str, object]]:
    """Read the campaign's own recorded samples for this host and run."""

    from scripts.acceptance import v1_physical_campaign as campaign

    packets = campaign.read_packets(
        context.evidence_root,
        context.scenario,
        expected_commit=context.commit,
    )
    samples = [
        packet
        for packet in packets
        if packet.get("kind") == "sample" and packet.get("host_id") == context.host_id
    ]
    if context.run_id:
        samples = [
            packet for packet in samples if packet.get("run_id") == context.run_id
        ]
    return sorted(samples, key=lambda item: str(item.get("recorded_at", "")))


def _path_present(packet: Mapping[str, object], path: Sequence[str]) -> bool:
    current: object = packet
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return False
        current = current[key]
    return bool(current)


def _probe_campaign_series(context: ProbeContext) -> ProbeOutcome:
    series = context.option("series", "storage")
    required = SERIES_REQUIREMENTS.get(series)
    if required is None:
        raise ProbeError(f"unsupported series: {series}")
    minimum = context.int_option("min_samples", 2)
    samples = _sample_packets(context)
    covered = [packet for packet in samples if _path_present(packet, required)]
    detail: dict[str, object] = {
        "series": series,
        "run_id_bound": bool(context.run_id),
        "sample_count": len(samples),
        "covered_sample_count": len(covered),
        "min_samples": minimum,
    }
    if not samples:
        return _outcome(
            "campaign-series",
            UNAVAILABLE,
            "no in-run resource sample has been recorded yet",
            detail,
        )
    if len(covered) < minimum:
        return _outcome(
            "campaign-series",
            FAIL,
            f"the {series} series has fewer than {minimum} qualifying samples",
            detail,
        )
    return _outcome(
        "campaign-series",
        PASS,
        f"the {series} series is sampled across the timed run",
        detail,
    )


def _used_bytes(packet: Mapping[str, object]) -> int:
    resources = packet.get("resources")
    if not isinstance(resources, Mapping):
        return 0
    total = 0
    for value in resources.values():
        if isinstance(value, Mapping):
            used = value.get("used_bytes")
            if isinstance(used, int):
                total += used
    return total


def _probe_growth_analysis(context: ProbeContext) -> ProbeOutcome:
    from scripts.acceptance import v1_physical_campaign as campaign

    ceiling = context.int_option("max_growth_bytes_per_hour", GIBIBYTE)
    samples = _sample_packets(context)
    detail: dict[str, object] = {
        "sample_count": len(samples),
        "max_growth_bytes_per_hour": ceiling,
        "run_id_bound": bool(context.run_id),
    }
    if len(samples) < 2:
        return _outcome(
            "growth-analysis",
            UNAVAILABLE,
            "at least two in-run samples are required to measure growth",
            detail,
        )
    first, last = samples[0], samples[-1]
    started = campaign.parse_time(str(first["recorded_at"]))
    ended = campaign.parse_time(str(last["recorded_at"]))
    elapsed_hours = max((ended - started).total_seconds() / 3600.0, 0.0)
    delta = _used_bytes(last) - _used_bytes(first)
    detail["elapsed_hours"] = round(elapsed_hours, 4)
    detail["used_bytes_delta"] = delta
    if elapsed_hours <= 0:
        return _outcome(
            "growth-analysis",
            UNAVAILABLE,
            "the sampled interval is too short to measure a growth slope",
            detail,
        )
    slope = delta / elapsed_hours
    detail["growth_bytes_per_hour"] = int(slope)
    restarts = _restart_states(context)
    detail["restart_states"] = restarts
    amplified = [
        name
        for name, value in restarts.items()
        if isinstance(value, dict) and int(value.get("consecutive_unclean", 0)) > 1
    ]
    detail["restart_amplification"] = amplified
    if slope > ceiling:
        return _outcome(
            "growth-analysis",
            FAIL,
            "measured growth exceeds the declared per-hour ceiling",
            detail,
        )
    if amplified:
        return _outcome(
            "growth-analysis",
            FAIL,
            "a core service shows unclean restart amplification",
            detail,
        )
    return _outcome(
        "growth-analysis",
        PASS,
        "no unexplained growth or restart amplification across the samples",
        detail,
    )


def _probe_activation_growth(context: ProbeContext) -> ProbeOutcome:
    activations = context.int_option("activations", 0)
    ceiling = context.int_option("max_bytes_per_activation", 2 * GIBIBYTE)
    samples = _sample_packets(context)
    detail: dict[str, object] = {
        "activations": activations,
        "sample_count": len(samples),
        "max_bytes_per_activation": ceiling,
    }
    if activations < 3:
        return _outcome(
            "activation-growth",
            UNAVAILABLE,
            "declare at least three completed activations with --option activations=N",
            detail,
        )
    if len(samples) < activations + 1:
        return _outcome(
            "activation-growth",
            FAIL,
            "there is no baseline plus per-activation sample for each activation",
            detail,
        )
    delta = _used_bytes(samples[-1]) - _used_bytes(samples[0])
    per_activation = delta / activations
    detail["used_bytes_delta"] = delta
    detail["bytes_per_activation"] = int(per_activation)
    if per_activation > ceiling:
        return _outcome(
            "activation-growth",
            FAIL,
            "per-activation growth exceeds the declared bound",
            detail,
        )
    return _outcome(
        "activation-growth",
        PASS,
        "per-activation growth stays inside the declared bound",
        detail,
    )


def _recorded_checkpoints(context: ProbeContext) -> dict[str, object] | None:
    """Return the most recent recorder checkpoint set already in evidence."""

    from scripts.acceptance import v1_physical_campaign as campaign

    try:
        packets = campaign.read_packets(
            context.evidence_root,
            context.scenario,
            expected_commit=context.commit,
        )
    except campaign.CampaignError:
        return None
    best: dict[str, object] | None = None
    best_at = ""
    for packet in packets:
        if packet.get("host_id") != context.host_id:
            continue
        detail = packet.get("detail")
        if not isinstance(detail, Mapping):
            continue
        checkpoints = detail.get("checkpoints")
        if not isinstance(checkpoints, Mapping):
            continue
        recorded_at = str(packet.get("recorded_at", ""))
        if recorded_at > best_at:
            best_at = recorded_at
            best = dict(checkpoints)
    return best


# --------------------------------------------------------------------------
# Backup preflight
# --------------------------------------------------------------------------


def _probe_backup_preflight(context: ProbeContext) -> ProbeOutcome:
    destination = context.path_option("destination")
    detail: dict[str, object] = {"destination_declared": destination is not None}
    if destination is None:
        return _outcome(
            "backup-preflight",
            UNAVAILABLE,
            "declare the backup destination with --option destination=<path>",
            detail,
        )
    if not destination.exists():
        return _outcome(
            "backup-preflight",
            FAIL,
            "the declared backup destination does not exist",
            detail,
        )
    source = _measure(context.checkout)
    target = _measure(destination)
    if source is None or target is None:
        return _outcome(
            "backup-preflight",
            UNAVAILABLE,
            "host resource measurement is not importable from this checkout",
            detail,
        )
    detail["source"] = source
    detail["destination"] = target
    independent = (
        bool(source.get("available"))
        and bool(target.get("available"))
        and source.get("resource_alias") != target.get("resource_alias")
    )
    detail["destination_is_independent"] = independent
    estimate = _tree_totals(context.data_dir)
    results = _tree_totals(context.results_dir)
    required = int(estimate["bytes"]) + int(results["bytes"])
    detail["estimated_required_bytes"] = required
    free = target.get("free_bytes")
    detail["destination_free_bytes"] = free
    if not independent:
        return _outcome(
            "backup-preflight",
            FAIL,
            "the backup destination shares a backing resource with the source",
            detail,
        )
    if not isinstance(free, int) or free < required:
        return _outcome(
            "backup-preflight",
            FAIL,
            "the backup destination cannot hold the measured source estimate",
            detail,
        )
    return _outcome(
        "backup-preflight",
        PASS,
        "the backup destination is independent and preflighted before quiescence",
        detail,
    )


def _probe_backup_helper_prestage(context: ProbeContext) -> ProbeOutcome:
    required = ("git", "docker")
    present = {name: shutil.which(name) is not None for name in required}
    helper = context.checkout / "catalog" / "federation" / "backup_recovery.py"
    detail: dict[str, object] = {
        "commands": present,
        "backup_helper_present": helper.is_file(),
        "sqlite3_module_available": True,
    }
    missing = [name for name, found in present.items() if not found]
    detail["missing_commands"] = missing
    if missing or not helper.is_file():
        return _outcome(
            "backup-helper-prestage",
            FAIL,
            "a helper needed after quiescence is not available beforehand",
            detail,
        )
    return _outcome(
        "backup-helper-prestage",
        PASS,
        "every helper needed after quiescence is staged before services stop",
        detail,
    )


PROBES: Final[dict[str, ProbeSpec]] = {
    spec.probe_id: spec
    for spec in (
        ProbeSpec(
            "checkout-identity",
            "Exact clean candidate checkout",
            _probe_checkout_identity,
        ),
        ProbeSpec(
            "running-commit-identity",
            "Running runtime carries the candidate build commit",
            _probe_running_commit_identity,
        ),
        ProbeSpec(
            "update-status",
            "Update surface reports a deterministic non-mutating state",
            _probe_update_status,
        ),
        ProbeSpec(
            "update-entrypoint-disposition",
            "update.cmd follows its approved retired disposition",
            _probe_update_entrypoint_disposition,
            os_category="windows",
        ),
        ProbeSpec(
            "docker-state",
            "Docker container, cache and backing-resource state",
            _probe_docker_state,
        ),
        ProbeSpec(
            "host-resource-baseline",
            "Host, Docker, data/results, logs and model storage baseline",
            _probe_host_resource_baseline,
        ),
        ProbeSpec(
            "inode-capacity",
            "Filesystem exposes inode/file capacity accounting",
            _probe_inode_capacity,
        ),
        ProbeSpec(
            "safe-storage-refusal",
            "Oversized work refused, ordinary work still admitted",
            _probe_safe_storage_refusal,
        ),
        ProbeSpec(
            "model-pull-floor",
            "Model-sized writes refused before the emergency floor",
            _probe_model_pull_floor,
        ),
        ProbeSpec(
            "sqlite-integrity",
            "Read-only integrity check of every discoverable database",
            _probe_sqlite_integrity,
            options=frozenset({"path"}),
        ),
        ProbeSpec(
            "publication-backlog",
            "Publication backlog and forward progress state",
            _probe_publication_backlog,
            options=frozenset({"path"}),
        ),
        ProbeSpec(
            "publication-idempotency",
            "Durable publication identities remain unique",
            _probe_publication_idempotency,
            options=frozenset({"path"}),
        ),
        ProbeSpec(
            "service-health",
            "Core worker and process health",
            _probe_service_health,
        ),
        ProbeSpec(
            "core-availability",
            "Committed reads and control surfaces remain available",
            _probe_core_availability,
            options=frozenset({"path"}),
        ),
        ProbeSpec(
            "recorder-continuity",
            "Durable recorder checkpoint continuity",
            _probe_recorder_continuity,
            reads_evidence=True,
        ),
        ProbeSpec(
            "recorder-limits",
            "Finite recorder ingress and transaction bounds",
            _probe_recorder_limits,
        ),
        ProbeSpec(
            "recorder-path-containment",
            "Malformed path components cannot escape recorder roots",
            _probe_recorder_path_containment,
        ),
        ProbeSpec(
            "bounded-logs-and-health",
            "Logs stay bounded and health stays visible",
            _probe_bounded_logs,
            options=frozenset({"max_log_bytes"}),
        ),
        ProbeSpec(
            "host-mutation-serialization",
            "Concurrent host mutation is serialized",
            _probe_host_mutation_serialization,
        ),
        ProbeSpec(
            "corpus-size",
            "Non-trivial aged corpus or history exists",
            _probe_corpus_size,
            options=frozenset({"subject", "min_files", "min_bytes"}),
        ),
        ProbeSpec(
            "campaign-series",
            "A required soak series is sampled across the timed run",
            _probe_campaign_series,
            options=frozenset({"series", "min_samples"}),
            reads_evidence=True,
        ),
        ProbeSpec(
            "growth-analysis",
            "No unexplained growth or restart amplification",
            _probe_growth_analysis,
            options=frozenset({"max_growth_bytes_per_hour"}),
            reads_evidence=True,
        ),
        ProbeSpec(
            "activation-growth",
            "Per-activation growth stays inside the declared bound",
            _probe_activation_growth,
            options=frozenset({"activations", "max_bytes_per_activation"}),
            reads_evidence=True,
        ),
        ProbeSpec(
            "backup-preflight",
            "Independent, preflighted backup destination",
            _probe_backup_preflight,
            options=frozenset({"destination"}),
        ),
        ProbeSpec(
            "backup-helper-prestage",
            "Post-quiescence helpers are staged beforehand",
            _probe_backup_helper_prestage,
        ),
    )
}


def parse_options(values: Sequence[str]) -> dict[str, str]:
    """Parse ``key=value`` probe options without accepting shell input."""

    options: dict[str, str] = {}
    for item in values:
        key, separator, value = str(item).partition("=")
        if not separator:
            raise ProbeError(f"probe option must be key=value: {item}")
        key = key.strip().casefold()
        value = value.strip()
        if OPTION_KEY_RE.fullmatch(key) is None:
            raise ProbeError(f"unsupported probe option name: {key}")
        if OPTION_VALUE_RE.fullmatch(value) is None:
            raise ProbeError(f"unsupported probe option value for {key}")
        options[key] = value
    return options


def probe_spec(probe_id: str) -> ProbeSpec:
    spec = PROBES.get(probe_id)
    if spec is None:
        raise ProbeError(f"unknown probe: {probe_id}")
    return spec


def check_applicability(spec: ProbeSpec, context: ProbeContext) -> None:
    """Refuse a probe that this host is not allowed to run.

    OS and profile are checked before the probe body runs, so a probe can never
    observe the wrong machine class and then be recorded as if it had.
    """

    if spec.os_category is not None and context.os_category != spec.os_category:
        raise ProbeError(
            f"probe {spec.probe_id} may only run on {spec.os_category} hosts"
        )
    if spec.profiles and context.profile not in spec.profiles:
        allowed = ", ".join(sorted(spec.profiles))
        raise ProbeError(
            f"probe {spec.probe_id} may only run on host profiles: {allowed}"
        )
    unsupported = sorted(set(context.options) - set(spec.options))
    if unsupported:
        raise ProbeError(
            f"probe {spec.probe_id} does not accept options: {', '.join(unsupported)}"
        )


def execute(spec: ProbeSpec, context: ProbeContext) -> ProbeOutcome:
    check_applicability(spec, context)
    outcome = spec.run(context)
    if outcome.probe_id != spec.probe_id:
        raise ProbeError("probe returned a mismatched identity")
    return outcome

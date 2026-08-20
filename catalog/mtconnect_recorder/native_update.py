"""Native standalone-recorder host update lifecycle.

The Compose updater rebuilds an image and recreates a container. The physical
Windows recorder is not a container: it is one native Python process, started by
``start-tailscale-recorder.cmd``, reading and writing durable capture state
inside the same checkout the update is about to fast-forward.

That difference drives every rule in this module:

``stop before mutate``
    Fast-forwarding a checkout while the old interpreter is still importing from
    it is silent corruption, not an update. The Git mutation happens only after
    the supervised recorder process has been *proven* to have exited.
``prevalidate before stop``
    Capture is the product. It is stopped only once the update is known to be
    applicable -- clean tree, approved remote, exact fast-forward target,
    unchanged dependency inputs. Everything that can fail, fails while the
    recorder is still recording.
``correlate the activation``
    A stop request names the exact request, commit, supervisor, PID and
    process-instance nonce it is for, and expires. A marker left behind by an
    older process can therefore never stop an unrelated recorder, and a
    supervisor can never mistake a survivor for a replacement.
``prove the replacement``
    A fast-forward is not success. Success requires a *different* process --
    new PID, new nonce, same supervisor -- running the exact target commit,
    with a heartbeat newer than the activation and a healthy Federation.

No peer supplies a path, command, argument, URL or environment value anywhere in
this module. A peer may name an approved commit; everything else is local.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.federation.software_update import (
    APPROVED_BRANCH,
    APPROVED_REPOSITORY,
    OID_RE,
    GitUpdateAdapter,
)

from .native_identity import (
    NATIVE_RUNTIME_SCHEMA,
    NATIVE_RUNTIME_TYPE,
    NONCE_RE,
)

REQUEST_SCHEMA = "fcp.host-update-request.v1"
RESULT_SCHEMA = "fcp.host-update-result.v1"
ACTIVATION_SCHEMA = "fcp.recorder-update-activation.v1"
JOURNAL_SCHEMA = "fcp.recorder-update-journal.v1"

AGENT_DIRECTORY_NAME = "recorder-update-agent"
MAX_BYTES = 8192
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

#: Stages of one activation. A crash at any stage is resumable, and no stage is
#: ever re-run in a way that mutates the checkout or relaunches twice.
STAGE_CLAIMED = "claimed"
STAGE_PREVALIDATED = "prevalidated"
STAGE_STOP_REQUESTED = "stop_requested"
STAGE_RECORDER_EXITED = "recorder_exited"
STAGE_SOURCE_UPDATED = "source_updated"
STAGE_REPLACEMENT_STARTED = "replacement_started"
STAGE_REPLACEMENT_VERIFIED = "replacement_verified"
STAGE_COMPLETED = "completed"
STAGE_FAILED = "failed"

TERMINAL_STAGES = frozenset({STAGE_COMPLETED, STAGE_FAILED})

#: Retained history. Bounded so a long-lived recorder cannot grow the journal.
MAX_JOURNAL_HISTORY = 16

ACTIVATION_TTL_SECONDS = 300
RECORDER_EXIT_TIMEOUT_SECONDS = 300
REPLACEMENT_TIMEOUT_SECONDS = 300
#: A heartbeat older than this cannot prove a recorder is currently alive.
STATUS_FRESHNESS_SECONDS = 120

#: Inputs that decide whether the current interpreter can still run the target.
#: A change here is refused before capture stops rather than discovered after.
DEPENDENCY_PATHS: tuple[str, ...] = (
    "requirements.txt",
    "requirements-dev.txt",
    "constraints.txt",
    "constraints-phase2.txt",
    "pyproject.toml",
    "setup.cfg",
    "setup.py",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_stamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def process_is_running(pid: object) -> bool:
    """Probe a local PID read-only, never signalling and never killing.

    ``os.kill(pid, 0)`` is not a supported existence check on Windows, so the
    Windows branch opens a query-only handle and reads the exit code instead.
    The probe is deliberately only half the evidence: a PID can be recycled, so
    every caller also matches the process-instance nonce.
    """

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI only
        import ctypes
        import ctypes.wintypes

        synchronize = 0x00100000
        query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(
            query_limited_information | synchronize,
            False,
            pid,
        )
        if not handle:
            return False
        try:
            code = ctypes.wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == still_active
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def write_json_atomic(path: Path, value: object) -> None:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(payload) > MAX_BYTES:
        raise ValueError("recorder_update_message_too_large")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if len(raw) > MAX_BYTES * 8:
        return None
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass(frozen=True)
class RecorderRuntimeStatus:
    """One native recorder's self-reported, locally generated identity."""

    pid: int | None
    process_nonce: str | None
    supervisor_session: str | None
    build_commit: str | None
    heartbeat_at: datetime | None
    state: str | None
    federation_status: str | None
    federation_node_id: str | None
    federation_session_id: str | None

    @property
    def present(self) -> bool:
        return self.pid is not None and self.process_nonce is not None

    def is_fresh(self, *, now: datetime | None = None) -> bool:
        if self.heartbeat_at is None:
            return False
        moment = now or utc_now()
        age = (moment - self.heartbeat_at).total_seconds()
        return -STATUS_FRESHNESS_SECONDS <= age <= STATUS_FRESHNESS_SECONDS

    def is_running(self) -> bool:
        return self.present and process_is_running(self.pid)

    def federation_is_healthy(self) -> bool:
        # ``connected`` is the only state in which the recorder is actually
        # carrying its Federation membership. A recorder that came back but
        # cannot rejoin is not a successful replacement.
        return self.federation_status == "connected"


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def read_recorder_status(status_file: Path) -> RecorderRuntimeStatus:
    """Read the capture process's own heartbeat file."""

    empty = RecorderRuntimeStatus(
        None, None, None, None, None, None, None, None, None
    )
    value = read_json(status_file)
    if value is None:
        return empty
    identity = value.get("native_runtime")
    if not isinstance(identity, dict):
        return empty
    if (
        identity.get("schema") != NATIVE_RUNTIME_SCHEMA
        or identity.get("runtime_type") != NATIVE_RUNTIME_TYPE
    ):
        return empty
    pid = identity.get("pid")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        pid = None
    nonce = _text(identity.get("process_nonce"))
    if nonce is not None and not NONCE_RE.fullmatch(nonce):
        nonce = None
    session = _text(identity.get("supervisor_session"))
    if session is not None and not NONCE_RE.fullmatch(session):
        session = None
    commit = _text(identity.get("build_commit"))
    if commit is not None and not OID_RE.fullmatch(commit):
        commit = None
    federation = value.get("federation")
    federation = federation if isinstance(federation, dict) else {}
    return RecorderRuntimeStatus(
        pid=pid,
        process_nonce=nonce,
        supervisor_session=session,
        build_commit=commit,
        heartbeat_at=parse_stamp(value.get("heartbeat_at")),
        state=_text(value.get("state")),
        federation_status=_text(federation.get("status")),
        federation_node_id=_text(federation.get("node_id")),
        federation_session_id=_text(federation.get("session_id")),
    )


def pending_federation_updates(state_file: Path) -> dict[str, str]:
    """Read the recorder's durable pending Federation update requests.

    Both sides of the activation use this as evidence that a stop request
    corresponds to real, already authenticated Federation work. Neither side
    writes this file; only the update event processor does.
    """

    value = read_json(state_file)
    if value is None:
        return {}
    pending = value.get("pending")
    if not isinstance(pending, dict):
        return {}
    result: dict[str, str] = {}
    for record in pending.values():
        if not isinstance(record, dict):
            continue
        host_request_id = record.get("host_request_id")
        target = record.get("target_commit")
        if (
            isinstance(host_request_id, str)
            and REQUEST_ID_RE.fullmatch(host_request_id)
            and isinstance(target, str)
            and OID_RE.fullmatch(target)
        ):
            result[host_request_id] = target
    return result


def git_output(
    root: Path,
    arguments: Sequence[str],
    *,
    timeout: float = 20.0,
) -> tuple[int, str]:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *arguments],
            shell=False,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("git_unavailable") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("git_timeout") from exc
    return completed.returncode, completed.stdout


def dependency_changes(
    root: Path,
    current: str,
    target: str,
    *,
    paths: Iterable[str] = DEPENDENCY_PATHS,
) -> tuple[str, ...]:
    """List dependency inputs that differ between the two exact commits.

    The current process is already running on an interpreter prepared for
    ``current``. If none of these files change, that interpreter is still a
    verified environment for ``target`` and capture may be stopped safely.
    """

    if current == target:
        return ()
    code, output = git_output(
        root,
        ["diff", "--name-only", f"{current}..{target}", "--", *paths],
    )
    if code != 0:
        # An unreadable comparison is not evidence of compatibility.
        raise RuntimeError("dependency_comparison_failed")
    return tuple(sorted({line.strip() for line in output.splitlines() if line.strip()}))


class NativeUpdateJournal:
    """Bounded durable record of one activation at a time, plus its history.

    The journal is what makes a crash recoverable without ever repeating a
    mutation: it names the stage that was durably reached, so a restarted agent
    resumes rather than restarts, and a finished request is answered from its
    retained result instead of being applied twice.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def _load(self) -> dict[str, Any]:
        value = read_json(self.path)
        if value is None or value.get("schema") != JOURNAL_SCHEMA:
            return {"schema": JOURNAL_SCHEMA, "active": None, "history": []}
        value.setdefault("active", None)
        history = value.get("history")
        value["history"] = history if isinstance(history, list) else []
        return value

    def _save(self, value: dict[str, Any]) -> None:
        history = value.get("history")
        if isinstance(history, list) and len(history) > MAX_JOURNAL_HISTORY:
            value["history"] = history[-MAX_JOURNAL_HISTORY:]
        write_json_atomic(self.path, value)

    def active(self) -> dict[str, Any] | None:
        record = self._load().get("active")
        return record if isinstance(record, dict) else None

    def history_entry(self, request_id: str) -> dict[str, Any] | None:
        for record in reversed(self._load().get("history", [])):
            if isinstance(record, dict) and record.get("request_id") == request_id:
                return record
        return None

    def claim(self, *, request_id: str, target_commit: str) -> dict[str, Any]:
        value = self._load()
        active = value.get("active")
        if isinstance(active, dict) and active.get("request_id") == request_id:
            return active
        record = {
            "request_id": request_id,
            "target_commit": target_commit,
            "stage": STAGE_CLAIMED,
            "claimed_at": stamp(utc_now()),
            "updated_at": stamp(utc_now()),
        }
        value["active"] = record
        self._save(value)
        return record

    @staticmethod
    def _active_of(value: dict[str, Any]) -> dict[str, Any] | None:
        record = value.get("active")
        return record if isinstance(record, dict) else None

    def advance(self, stage: str, **fields: object) -> dict[str, Any]:
        value = self._load()
        active = self._active_of(value)
        if active is None:
            raise RuntimeError("recorder_update_journal_has_no_active_request")
        active.update(fields)
        active["stage"] = stage
        active["updated_at"] = stamp(utc_now())
        value["active"] = active
        self._save(value)
        return active

    def finish(self, stage: str, **fields: object) -> dict[str, Any]:
        if stage not in TERMINAL_STAGES:
            raise ValueError("recorder_update_journal_stage_is_not_terminal")
        value = self._load()
        active = self._active_of(value)
        if active is None:
            raise RuntimeError("recorder_update_journal_has_no_active_request")
        active.update(fields)
        active["stage"] = stage
        active["updated_at"] = stamp(utc_now())
        history = value.get("history", [])
        history.append(active)
        value["history"] = history
        value["active"] = None
        self._save(value)
        return active

    def discard_active(self) -> None:
        value = self._load()
        value["active"] = None
        self._save(value)


def activation_intent(
    *,
    request_id: str,
    target_commit: str,
    supervisor_session: str,
    recorder_pid: int,
    process_nonce: str,
    now: datetime | None = None,
    ttl_seconds: int = ACTIVATION_TTL_SECONDS,
) -> dict[str, object]:
    """Build the bounded stop request the capture process must consent to."""

    if not REQUEST_ID_RE.fullmatch(request_id):
        raise ValueError("malformed_request_id")
    if not OID_RE.fullmatch(target_commit):
        raise ValueError("malformed_target")
    if not NONCE_RE.fullmatch(supervisor_session):
        raise ValueError("malformed_supervisor_session")
    if not NONCE_RE.fullmatch(process_nonce):
        raise ValueError("malformed_process_nonce")
    valid_pid = (
        recorder_pid
        if isinstance(recorder_pid, int) and not isinstance(recorder_pid, bool)
        else 0
    )
    if valid_pid <= 0:
        raise ValueError("malformed_recorder_pid")
    moment = now or utc_now()
    return {
        "schema": ACTIVATION_SCHEMA,
        "request_id": request_id,
        "target_commit": target_commit,
        "supervisor_session": supervisor_session,
        "recorder_pid": valid_pid,
        "process_nonce": process_nonce,
        "created_at": stamp(moment),
        "expires_at": stamp(moment + timedelta(seconds=ttl_seconds)),
    }


@dataclass(frozen=True)
class ActivationDecision:
    accepted: bool
    reason: str
    request_id: str | None = None
    target_commit: str | None = None


def evaluate_activation(
    value: object,
    *,
    pid: int,
    process_nonce: str | None,
    supervisor_session: str | None,
    pending_host_request_ids: dict[str, str],
    now: datetime | None = None,
) -> ActivationDecision:
    """Decide whether this exact capture process must stop for this update.

    Every field must match: a marker for another commit, another supervisor,
    another process instance, an expired window, or an update this recorder has
    no durable pending Federation request for is refused. Refusing is safe --
    the marker is simply ignored, so a stale file can never produce a restart
    loop.
    """

    if not isinstance(value, dict) or value.get("schema") != ACTIVATION_SCHEMA:
        return ActivationDecision(False, "malformed_activation")
    request_id = value.get("request_id")
    target = value.get("target_commit")
    if not isinstance(request_id, str) or not REQUEST_ID_RE.fullmatch(request_id):
        return ActivationDecision(False, "malformed_activation")
    if not isinstance(target, str) or not OID_RE.fullmatch(target):
        return ActivationDecision(False, "malformed_activation", request_id)
    if value.get("recorder_pid") != pid:
        return ActivationDecision(False, "wrong_recorder_pid", request_id, target)
    if process_nonce is None or value.get("process_nonce") != process_nonce:
        return ActivationDecision(False, "wrong_process_nonce", request_id, target)
    if (
        supervisor_session is None
        or value.get("supervisor_session") != supervisor_session
    ):
        return ActivationDecision(
            False, "wrong_supervisor_session", request_id, target
        )
    created = parse_stamp(value.get("created_at"))
    expires = parse_stamp(value.get("expires_at"))
    moment = now or utc_now()
    if created is None or expires is None:
        return ActivationDecision(False, "malformed_activation", request_id, target)
    if (
        created > moment + timedelta(minutes=1)
        or expires <= moment
        or (expires - created) > timedelta(seconds=ACTIVATION_TTL_SECONDS + 60)
    ):
        return ActivationDecision(False, "expired_activation", request_id, target)
    if pending_host_request_ids.get(request_id) != target:
        return ActivationDecision(
            False, "no_pending_federation_update", request_id, target
        )
    return ActivationDecision(True, "accepted", request_id, target)


class NativeRecorderUpdatePaths:
    """Fixed local locations for one recorder data directory."""

    def __init__(self, data_directory: Path | str) -> None:
        self.data_directory = Path(data_directory).resolve()
        self.directory = self.data_directory / "federation" / AGENT_DIRECTORY_NAME
        self.request_file = self.directory / "request.json"
        self.result_file = self.directory / "result.json"
        self.journal_file = self.directory / "journal.json"
        self.activation_file = self.directory / "activation.json"
        self.status_file = (
            self.data_directory / "source_state" / "mtconnect_recorder_status.json"
        )
        self.federation_state_file = (
            self.data_directory
            / "federation"
            / "recorder_update"
            / "processor.json"
        )


def inspect_for_update(
    root: Path,
    target: str | None,
    *,
    adapter: GitUpdateAdapter | None = None,
) -> Any:
    resolved = adapter or GitUpdateAdapter(root)
    return resolved.inspect(target=target, fetch=True)


__all__ = [
    "ACTIVATION_SCHEMA",
    "ACTIVATION_TTL_SECONDS",
    "AGENT_DIRECTORY_NAME",
    "APPROVED_BRANCH",
    "APPROVED_REPOSITORY",
    "DEPENDENCY_PATHS",
    "JOURNAL_SCHEMA",
    "MAX_BYTES",
    "MAX_JOURNAL_HISTORY",
    "RECORDER_EXIT_TIMEOUT_SECONDS",
    "REPLACEMENT_TIMEOUT_SECONDS",
    "REQUEST_ID_RE",
    "REQUEST_SCHEMA",
    "RESULT_SCHEMA",
    "STAGE_CLAIMED",
    "STAGE_COMPLETED",
    "STAGE_FAILED",
    "STAGE_PREVALIDATED",
    "STAGE_RECORDER_EXITED",
    "STAGE_REPLACEMENT_STARTED",
    "STAGE_REPLACEMENT_VERIFIED",
    "STAGE_SOURCE_UPDATED",
    "STAGE_STOP_REQUESTED",
    "STATUS_FRESHNESS_SECONDS",
    "TERMINAL_STAGES",
    "ActivationDecision",
    "NativeRecorderUpdatePaths",
    "NativeUpdateJournal",
    "RecorderRuntimeStatus",
    "activation_intent",
    "dependency_changes",
    "evaluate_activation",
    "git_output",
    "inspect_for_update",
    "parse_stamp",
    "pending_federation_updates",
    "process_is_running",
    "read_json",
    "read_recorder_status",
    "stamp",
    "utc_now",
    "write_json_atomic",
]

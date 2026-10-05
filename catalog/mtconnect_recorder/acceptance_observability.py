"""Bounded observations of existing Recorder operations, never workload policy.

Only the latest operation per scope is retained. No file, task, timer, retry or
log is created here; the Recorder's existing heartbeat publishes a snapshot.
All observation failures are isolated from the operation being observed.
"""
from __future__ import annotations

import copy
import hashlib
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from functools import wraps
from typing import Any

SCHEMA = "fcp.recorder.acceptance-observability.v1"
MAX_OPERATIONS = 16
MAX_COUNTER = (1 << 64) - 1
_TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,160}")
_CONTEXT_FIELDS = frozenset({
    "source_alias", "agent_instance_id", "next_sequence_before",
    "session_id", "node_id", "storage_group",
})
_PROGRESS_FIELDS = frozenset({
    "next_sequence_after", "advanced_sequences", "scanned_batches",
    "eligible_batches", "publication_chunks", "enqueued", "already_enqueued", "quarantined",
})
_REQUIRED_PROGRESS = {
    "recorder-recovery": frozenset({"next_sequence_after", "advanced_sequences"}),
    "publication-reconcile": _PROGRESS_FIELDS - {"next_sequence_after", "advanced_sequences"},
}
PUBLICATION_CYCLE_SCHEMA = "fcp.recorder.publication-cycle-diagnostics.v1"
_CYCLE_STAGES = frozenset({
    "connect", "client-close", "announce", "status", "selection", "route-build",
    "delivery", "jsonl", "pending-read", "local-reconcile", "retry-pending-read",
})
_WORKER_STAGES = frozenset({
    "validate", "checkpoint", "backlog-probe", "reconcile", "delivery", "retirement",
})
_CYCLE_COUNTERS = _PROGRESS_FIELDS | {"committed", "attempted", "retired", "pending_batches"}
_RECONCILE_STAGES = frozenset({
    "legacy-scan", "legacy-frontier-write", "legacy-marker", "legacy-marker-published", "pending-discovery",
    "pending-validation", "derived-read", "outbox-enqueue", "pending-retirement",
})
_RECONCILE_COUNTERS = frozenset({"refs_total", "issues", "writes_completed", "write_attempts"})


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def source_alias(name: str) -> str:
    """Do not put private source labels or endpoints in the metric surface."""
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def _fields(value: Mapping[str, object], *, progress: bool = False) -> dict[str, object]:
    allowed = _PROGRESS_FIELDS if progress else _CONTEXT_FIELDS
    result: dict[str, object] = {}
    for key, item in value.items():
        if key not in allowed:
            continue
        if item is None:
            result[key] = None
        elif type(item) is int and 0 <= item <= MAX_COUNTER or not progress and isinstance(item, str) and _TOKEN.fullmatch(item):
            result[key] = item
        else:
            raise ValueError("unrepresentable observation field")
    return result


class ObservationRegistry:
    """Nonblocking constant-space storage; contention means missing evidence."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pid: int | None = None
        self._generation: str | None = None
        self._operations: OrderedDict[tuple[object, ...], dict[str, object]] = OrderedDict()
        self._sequence = 0
        self._dropped = 0
        self._evicted = 0

    def _drop(self) -> None:
        self._dropped = min(MAX_COUNTER, self._dropped + 1)

    def observation_failed(self, operation: str, *, started: bool) -> None:
        self._drop()
        if started or not self._lock.acquire(blocking=False):
            return
        try:
            # Context/clock failure before a new start cannot leave a previous
            # completion masquerading as the latest attempted operation.
            for key in tuple(self._operations):
                if key[0] == operation:
                    self._operations.pop(key, None)
        finally:
            self._lock.release()

    def _provenance(self) -> dict[str, object]:
        pid = os.getpid()
        if pid != self._pid:
            # A fork must never publish a parent's completed operation as local.
            inherited_process = self._pid is not None
            self._operations.clear()
            self._pid = pid
            self._generation = None
            self._sequence = 0
            if inherited_process:
                self._dropped = 0
                self._evicted = 0
        if self._generation is None:
            self._generation = uuid.uuid4().hex
        candidates = {
            value.lower() for value in (
                os.environ.get("FCP_BUILD_COMMIT", ""),
                os.environ.get("FCP_RECORDER_BUILD_COMMIT", ""),
            ) if re.fullmatch(r"[0-9a-fA-F]{40}", value)
        }
        supervisor = os.environ.get("FCP_RECORDER_SUPERVISOR_SESSION", "")
        return {
            "candidate_sha": next(iter(candidates)) if len(candidates) == 1 else None,
            "runtime_generation": self._generation,
            "supervisor_generation": supervisor.lower() if re.fullmatch(r"[0-9a-fA-F]{32}", supervisor) else None,
            "pid": pid,
        }

    def provenance(self) -> dict[str, object]:
        if not self._lock.acquire(blocking=False):
            self._drop()
            return {}
        try:
            return self._provenance()
        finally:
            self._lock.release()

    def begin(self, operation: str, context: Mapping[str, object]) -> tuple[tuple[object, ...], str] | None:
        if operation not in {"recorder-recovery", "publication-reconcile"}:
            raise ValueError("unknown observed operation")
        values = _fields(context)
        scope = (operation, values.get("source_alias"), values.get("session_id"),
                 values.get("node_id"), values.get("storage_group"))
        if not self._lock.acquire(blocking=False):
            self._drop()
            return None
        try:
            provenance = self._provenance()
            # Even an observer failure below must not keep an old completed
            # operation under this scope after a new attempt has begun.
            self._operations.pop(scope, None)
            observation_loss_count = self._dropped
            identifier = uuid.uuid4().hex
            started = time.monotonic_ns()
            utc = _utc()
            self._sequence = min(MAX_COUNTER, self._sequence + 1)
            self._operations[scope] = {
                "operation": operation, "operation_id": identifier,
                "operation_sequence": self._sequence, "provenance": provenance,
                "observation_loss_count": observation_loss_count,
                "context": values, "outcome": "incomplete",
                "started_at_utc": utc, "started_at_monotonic_ns": started,
                "ended_at_utc": None, "ended_at_monotonic_ns": None,
                "duration_ns": None, "progress": None, "error_type": None,
            }
            while len(self._operations) > MAX_OPERATIONS:
                self._operations.popitem(last=False)
                self._evicted = min(MAX_COUNTER, self._evicted + 1)
            return scope, identifier
        finally:
            self._lock.release()

    def finish(self, token: tuple[tuple[object, ...], str] | None, *,
               progress: Mapping[str, object] | None = None, error: BaseException | None = None) -> None:
        if token is None:
            return
        if not self._lock.acquire(blocking=False):
            self._drop()
            return
        try:
            scope, identifier = token
            record = self._operations.get(scope)
            if record is None or record["operation_id"] != identifier:
                return  # A newer attempt owns this scope; never overwrite it.
            ended = time.monotonic_ns()
            utc = _utc()
            started = record["started_at_monotonic_ns"]
            if type(started) is not int or type(ended) is not int or not 0 <= started <= ended:
                self._drop()
                return
            fields = _fields(progress or {}, progress=True) if error is None else None
            if error is None and any(
                type(fields.get(key)) is not int for key in _REQUIRED_PROGRESS[record["operation"]]
            ):
                raise ValueError("missing operation progress")
            # Build the entire terminal record before replacing the incomplete
            # one, so an observer exception cannot leave a partial completion.
            terminal = {
                **record, "ended_at_utc": utc, "ended_at_monotonic_ns": ended,
                "duration_ns": ended - started, "progress": fields,
                "outcome": ("completed" if error is None else
                            "failed" if isinstance(error, Exception) else "interrupted"),
                "error_type": type(error).__name__[:64] if error is not None else None,
            }
            self._operations[scope] = terminal
        finally:
            self._lock.release()

    def snapshot(self) -> dict[str, object]:
        if not self._lock.acquire(blocking=False):
            self._drop()
            return {"schema": SCHEMA, "available": False, "operations": []}
        try:
            provenance = self._provenance()
            return {
                "schema": SCHEMA, "available": True, "provenance": provenance,
                "observed_at_utc": _utc(), "observed_at_monotonic_ns": time.monotonic_ns(),
                "operations": copy.deepcopy(list(self._operations.values())),
                "dropped_updates": self._dropped,
                "max_operations": MAX_OPERATIONS, "evicted_operations": self._evicted,
                "retention_truncated": self._evicted > 0,
            }
        finally:
            self._lock.release()


class PublicationCycleObservation:
    """One owned current cycle; diagnostics never replace completed-span evidence.

    Reads/updates are nonblocking and have no I/O. A missed update makes the
    retained cycle unavailable rather than leaving its previous stage current.
    The existing duration registry and its loss/completion rules are untouched.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._record: dict[str, object] | None = None
        self._worker_generation: str | None = None
        self._worker_provenance: dict[str, object] | None = None
        self._lost = 0
        self._sequence = 0

    def _lose(self) -> None:
        self._lost = min(MAX_COUNTER, self._lost + 1)

    def new_worker(self) -> str | None:
        if not self._lock.acquire(blocking=False):
            # Replacement admission must fence the previous coroutine even when
            # its diagnostic callback currently holds the nonblocking lock.
            self._worker_generation = None
            self._lose()
            return
        try:
            self._record = None
            self._worker_generation = uuid.uuid4().hex
            self._worker_provenance = None
            self._sequence = 0
            return self._worker_generation
        except Exception:  # noqa: BLE001 - diagnostics cannot change lifecycle
            self._worker_generation = None
            self._lose()
        finally:
            self._lock.release()

    def begin(self, cycle_id: str, *, session_id: str, node_id: str,
              worker_generation: str | None) -> tuple[str, str] | None:
        if not self._lock.acquire(blocking=False):
            self._lose()
            return None
        try:
            if worker_generation is None or worker_generation != self._worker_generation:
                return None  # A superseded coroutine cannot begin a new owned cycle.
            self._record = None
            provenance = process_provenance()
            if (self._worker_generation is None
                    or re.fullmatch(r"[0-9a-f]{32}", cycle_id) is None
                    or any(not isinstance(value, str) or _TOKEN.fullmatch(value) is None
                           for value in (session_id, node_id))
                    or not self._valid_provenance(provenance)
                    or (self._worker_provenance is not None
                        and self._worker_provenance != provenance)):
                raise ValueError("unrepresentable cycle identity")
            self._worker_provenance = dict(provenance)
            started_ns = time.monotonic_ns()
            started_utc = _utc()
            if type(started_ns) is not int or started_ns <= 0:
                raise ValueError("invalid cycle start clock")
            self._sequence = min(MAX_COUNTER, self._sequence + 1)
            self._record = {
                "publication_loop_generation": self._worker_generation,
                "cycle_id": cycle_id, "cycle_sequence": self._sequence,
                "transition_sequence": 1,
                "provenance": provenance,
                "context": {"session_id": session_id, "node_id": node_id,
                            "storage_group": None, "authority_node_id": None},
                "outcome": "running", "stage": "connect", "cycle_stage": None,
                "reconciliation": None,
                "terminal_scope": "cycle-awaiting-coroutine",
                "started_at_utc": started_utc, "started_at_monotonic_ns": started_ns,
                "transition_at_utc": started_utc, "transition_at_monotonic_ns": started_ns,
                "ended_at_utc": None, "ended_at_monotonic_ns": None,
                "progress": {key: None for key in sorted(_CYCLE_COUNTERS)},
                "error_type": None, "failure_stage": None, "failure_cycle_stage": None,
                "failure_reconciliation": None,
                "last_failure": None,
                "terminal_error_type": None, "observation_loss_count": self._lost,
            }
            return self._worker_generation, cycle_id
        except Exception:  # noqa: BLE001 - invalid/missing diagnostics stay missing
            self._lose()
            return None
        finally:
            self._lock.release()

    def transition(self, token: tuple[str, str] | None, stage: str, *,
                   cycle_stage: str | None = None,
                   progress: Mapping[str, object] | None = None,
                   storage_group: str | None = None,
                   authority_node_id: str | None = None,
                   reconciliation: Mapping[str, object] | None = None) -> None:
        self._update(token, stage=stage, cycle_stage=cycle_stage, progress=progress,
                     storage_group=storage_group, authority_node_id=authority_node_id,
                     reconciliation=reconciliation)

    def finish(self, token: tuple[str, str] | None, *, outcome: str,
               error: BaseException | None = None) -> None:
        self._update(token, outcome=outcome, error=error)

    def failure(self, token: tuple[str, str] | None, error: BaseException) -> None:
        self._update(token, failure=error)

    def observation_failed(self, token: tuple[str, str] | None) -> None:
        if not self._lock.acquire(blocking=False):
            self._lose()
            return
        try:
            if self._record is not None and token == (self._record["publication_loop_generation"], self._record["cycle_id"]):
                self._lose()
        finally:
            self._lock.release()

    def _update(self, token: tuple[str, str] | None, **changes: object) -> None:
        if token is None:
            return
        if not self._lock.acquire(blocking=False):
            self._lose()
            return
        try:
            record = self._record
            if (record is None or self._worker_generation is None
                    or record["publication_loop_generation"] != self._worker_generation
                    or token != (record["publication_loop_generation"], record["cycle_id"])):
                return  # An old callback cannot modify the newer owner.
            if record["outcome"] != "running":
                return
            stamp_ns, stamp_utc = time.monotonic_ns(), _utc()
            if (type(stamp_ns) is not int
                    or stamp_ns < record["transition_at_monotonic_ns"]):
                raise ValueError("invalid cycle transition clock")
            updated = copy.deepcopy(record)
            if "stage" in changes:
                stage, worker_stage = changes["stage"], changes["cycle_stage"]
                if stage not in _CYCLE_STAGES or (worker_stage is not None and worker_stage not in _WORKER_STAGES):
                    raise ValueError("unrepresentable cycle stage")
                updated.update(stage=stage, cycle_stage=worker_stage)
                detail = changes["reconciliation"]
                updated["reconciliation"] = None
                if detail is not None:
                    if (worker_stage != "reconcile" or not isinstance(detail, Mapping)
                            or detail.get("stage") not in _RECONCILE_STAGES
                            or not isinstance(detail.get("source_alias"), str)
                            or re.fullmatch(r"[0-9a-f]{64}", detail["source_alias"]) is None
                            or not isinstance(detail.get("progress"), Mapping)):
                        raise ValueError("unrepresentable reconciliation stage")
                    counts = {}
                    for key, value in detail["progress"].items():
                        if key not in _RECONCILE_COUNTERS:
                            continue
                        if type(value) is not int or not 0 <= value <= MAX_COUNTER:
                            raise ValueError("unrepresentable reconciliation progress")
                        counts[key] = value
                    updated["reconciliation"] = {
                        "stage": detail["stage"], "source_alias": detail["source_alias"],
                        "progress": counts,
                    }
                for key in ("storage_group", "authority_node_id"):
                    value = changes[key]
                    if value is not None:
                        if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
                            raise ValueError("unrepresentable cycle context")
                        updated["context"][key] = value
                progress = changes["progress"]
                if progress is not None:
                    if not isinstance(progress, Mapping):
                        raise ValueError("invalid cycle progress")
                    for key, value in progress.items():
                        if key not in _CYCLE_COUNTERS:
                            continue
                        if type(value) is not int or not 0 <= value <= MAX_COUNTER:
                            raise ValueError("unrepresentable cycle progress")
                        updated["progress"][key] = value
            elif "failure" in changes:
                error = changes["failure"]
                if not isinstance(error, BaseException):
                    raise ValueError("invalid cycle error")
                name = type(error).__name__
                updated["last_failure"] = {
                    "error_type": name if _TOKEN.fullmatch(name) else "Exception",
                    "stage": record["stage"], "cycle_stage": record["cycle_stage"],
                    "reconciliation": copy.deepcopy(record["reconciliation"]),
                }
                if updated["error_type"] is None:
                    updated.update(error_type=name if _TOKEN.fullmatch(name) else "Exception",
                                   failure_stage=record["stage"], failure_cycle_stage=record["cycle_stage"],
                                   failure_reconciliation=copy.deepcopy(record["reconciliation"]))
            else:
                outcome, error = changes["outcome"], changes["error"]
                if outcome not in {"completed", "waiting", "failed", "interrupted"}:
                    raise ValueError("invalid cycle outcome")
                if error is not None and not isinstance(error, BaseException):
                    raise ValueError("invalid cycle error")
                name = type(error).__name__ if error is not None else None
                updated.update(outcome=outcome, ended_at_utc=stamp_utc,
                               ended_at_monotonic_ns=stamp_ns,
                               terminal_error_type=name if name is None or _TOKEN.fullmatch(name) else "Exception")
            updated.update(transition_at_utc=stamp_utc, transition_at_monotonic_ns=stamp_ns,
                           transition_sequence=min(MAX_COUNTER, record["transition_sequence"] + 1))
            # Loss earlier in this cycle stays explicit even after another update.
            self._record = updated
        except Exception:  # noqa: BLE001 - do not change publication exceptions
            self._lose()
        finally:
            self._lock.release()

    def snapshot(self, *, expected_session: str | None, expected_node: str | None) -> dict[str, object]:
        missing = {"schema": PUBLICATION_CYCLE_SCHEMA, "available": False,
                   "acceptance_completion_evidence": False}
        if not self._lock.acquire(blocking=False):
            self._lose()
            return missing
        try:
            record = self._record
            current = process_provenance()
            observed_ns, observed_utc = time.monotonic_ns(), _utc()
            if (record is None or self._worker_generation is None
                    or record["publication_loop_generation"] != self._worker_generation
                    or record["provenance"] != current
                    or not self._valid_provenance(current)
                    or record["context"]["session_id"] != expected_session
                    or record["context"]["node_id"] != expected_node
                    or record["observation_loss_count"] != self._lost
                    or type(observed_ns) is not int
                    or not 0 < record["started_at_monotonic_ns"] <= record["transition_at_monotonic_ns"] <= observed_ns):
                return {**missing, "dropped_updates": self._lost}
            stamps = [datetime.fromisoformat(value.replace("Z", "+00:00"))
                      for value in (record["started_at_utc"], record["transition_at_utc"], observed_utc)]
            if (any(stamp.tzinfo is None for stamp in stamps)
                    or not stamps[0] <= stamps[1] <= stamps[2]):
                return {**missing, "dropped_updates": self._lost}
            return {"schema": PUBLICATION_CYCLE_SCHEMA, "available": True,
                    "acceptance_completion_evidence": False, "observed_at_utc": observed_utc,
                    "observed_at_monotonic_ns": observed_ns, "dropped_updates": self._lost,
                    **copy.deepcopy(record)}
        except Exception:  # noqa: BLE001 - racing/unrepresentable evidence unavailable
            self._lose()
            return {**missing, "dropped_updates": self._lost}
        finally:
            self._lock.release()

    @staticmethod
    def _valid_provenance(value: object) -> bool:
        return (isinstance(value, dict)
                and set(value) == {"candidate_sha", "runtime_generation", "supervisor_generation", "pid"}
                and isinstance(value.get("candidate_sha"), str)
                and re.fullmatch(r"[0-9a-f]{40}", value["candidate_sha"]) is not None
                and isinstance(value.get("runtime_generation"), str)
                and re.fullmatch(r"[0-9a-f]{32}", value["runtime_generation"]) is not None
                and (value.get("supervisor_generation") is None
                     or isinstance(value["supervisor_generation"], str)
                     and re.fullmatch(r"[0-9a-f]{32}", value["supervisor_generation"]) is not None)
                and type(value.get("pid")) is int and value["pid"] > 0)


_REGISTRY = ObservationRegistry()


def process_provenance() -> dict[str, object]:
    """Shared by timing and managed health; never controls product behavior."""
    try:
        value = _REGISTRY.provenance()
        if value:
            return value
    except Exception:  # noqa: BLE001, S110 - observer failure must not affect capture or add log I/O
        pass
    return {"candidate_sha": None, "runtime_generation": None,
            "supervisor_generation": None, "pid": None}


def snapshot() -> dict[str, object]:
    try:
        return _REGISTRY.snapshot()
    except Exception:  # noqa: BLE001 - observer failure must not affect capture or add log I/O
        return {"schema": SCHEMA, "available": False, "operations": []}


def observe_operation(operation: str, *, context: Callable[..., Mapping[str, object]],
                      progress: Callable[[Any, Mapping[str, object]], Mapping[str, object]]):
    """Wrap one existing synchronous operation without changing its outcome."""
    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            token = None
            values: Mapping[str, object] = {}
            try:
                values = context(*args, **kwargs)
                token = _REGISTRY.begin(operation, values)
            except Exception:  # noqa: BLE001 - observer failure must not affect capture or add log I/O
                try:
                    _REGISTRY.observation_failed(operation, started=False)
                except Exception:  # noqa: BLE001, S110 - observer failure must not affect capture or add log I/O
                    pass
            try:
                result = function(*args, **kwargs)
            except BaseException as error:
                try:
                    _REGISTRY.finish(token, error=error)
                except Exception:  # noqa: BLE001 - observer failure must not affect capture or add log I/O
                    try:
                        _REGISTRY.observation_failed(operation, started=token is not None)
                    except Exception:  # noqa: BLE001, S110 - observer failure must not affect capture or add log I/O
                        pass
                raise
            try:
                _REGISTRY.finish(token, progress=progress(result, values))
            except Exception:  # noqa: BLE001 - observer failure must not affect capture or add log I/O
                try:
                    _REGISTRY.observation_failed(operation, started=token is not None)
                except Exception:  # noqa: BLE001, S110 - observer failure must not affect capture or add log I/O
                    pass
            return result
        return observed
    return decorate

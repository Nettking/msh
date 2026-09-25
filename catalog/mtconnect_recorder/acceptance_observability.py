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

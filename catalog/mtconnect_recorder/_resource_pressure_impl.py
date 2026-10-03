"""Host-resource admission for raw-first/checkpoint-last recorder transactions.

B01 provides the shared host-resource vocabulary and process-local reservation
controller. This module applies that contract to the MTConnect recorder without
changing its durability order:

    admission -> recovery frontier pending -> immutable raw -> derived views
    -> checkpoint -> recovery frontier clear

New capture is refused at PRESSURE/CRITICAL. Recovery of raw evidence that was
already persisted before a crash is different: at PRESSURE it may finish only
when its bounded completion reservation still leaves the CRITICAL emergency
floor intact. CRITICAL/unavailable measurements always pause recovery.

The integration is installed lazily by :mod:`catalog.mtconnect_recorder` after
launcher environment configuration has been established. Stores constructed by
tests or other callers are unaffected unless explicitly attached.
"""
from __future__ import annotations

import errno
import json
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any

from catalog.federation.host_resources import (
    HostResourceRefused,
    PressureLevel,
    ProcessResourceAdmission,
    ResourceAssessment,
    ResourceReservation,
    assess_measurement,
)

from .limits import (
    MAX_COMPATIBILITY_BATCH_BYTES,
    MAX_COMPATIBILITY_STATE_BYTES,
    MAX_OBSERVATION_ARCHIVE_BYTES,
    MAX_PROBE_RESPONSE_BYTES,
    MAX_SAMPLE_RESPONSE_BYTES,
)
from .model import ParsedBatch, ProbeModel, _slug, _utc_now, _write_bytes_atomic
from .schema_compat import (
    CHECKPOINT_SCHEMA,
    PROBE_MANIFEST_SCHEMA,
    RAW_BATCH_MANIFEST_SCHEMA,
)
from .storage import _confined_storage_path, _observation_storage_day

RESOURCE_PRESSURE_RETRY_SECONDS = 1.0
RECORDER_DATA_TRANSACTION_INODES = 32
RECORDER_STATE_TRANSACTION_INODES = 8
RECORDER_PROBE_TRANSACTION_INODES = 8


@dataclass(frozen=True)
class RecorderResourceBudget:
    """Finite maxima used to reserve one recorder transaction."""

    probe_bytes: int = MAX_PROBE_RESPONSE_BYTES
    sample_bytes: int = MAX_SAMPLE_RESPONSE_BYTES
    observation_bytes: int = MAX_OBSERVATION_ARCHIVE_BYTES
    compatibility_bytes: int = MAX_COMPATIBILITY_BATCH_BYTES
    compatibility_state_bytes: int = MAX_COMPATIBILITY_STATE_BYTES
    data_inodes: int = RECORDER_DATA_TRANSACTION_INODES
    state_inodes: int = RECORDER_STATE_TRANSACTION_INODES
    probe_inodes: int = RECORDER_PROBE_TRANSACTION_INODES

    def __post_init__(self) -> None:
        values = (
            self.probe_bytes,
            self.sample_bytes,
            self.observation_bytes,
            self.compatibility_bytes,
            self.compatibility_state_bytes,
            self.data_inodes,
            self.state_inodes,
            self.probe_inodes,
        )
        if any(not _valid_nonnegative_integer(value) for value in values):
            raise ValueError(
                "recorder resource budget values must be non-negative integers"
            )


@dataclass(frozen=True)
class RecorderResourcePause:
    """Local admission state surfaced without classifying the Agent as failed."""

    code: str
    assessment: ResourceAssessment


#: Stable code for a host filesystem that refused an already-admitted write.
STORAGE_EXHAUSTED = "storage_exhausted"

#: Errno values that mean the host has no room, rather than a genuine fault.
#: Deliberately narrow: a permission, I/O or corruption error is a real failure
#: and must keep its own semantics rather than being presented as pressure.
_STORAGE_EXHAUSTION_ERRNOS = frozenset(
    code
    for code in (getattr(errno, name, None) for name in ("ENOSPC", "EDQUOT"))
    if code is not None
)


def _is_storage_exhaustion(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _STORAGE_EXHAUSTION_ERRNOS


class RecorderResourcePaused(RuntimeError):
    """Direct-call result when recovery is paused by local host resources."""

    def __init__(self, pause: RecorderResourcePause) -> None:
        super().__init__(pause.code)
        self.pause = pause


class _RecorderPauseSignal(BaseException):
    """Internal control signal that bypasses the remote-source error boundary.

    ``RecorderRuntime.capture_source`` catches ``Exception`` because one remote
    source must never kill the worker. Host admission is not a remote-source
    failure, so the installed outer wrapper catches this signal immediately and
    publishes a local degraded state instead. The signal never crosses the
    installed recorder boundary.
    """


@dataclass(frozen=True)
class _Requirement:
    path: Path
    bytes_required: int
    inodes_required: int


@dataclass
class _GroupedRequirement:
    path: Path
    bytes_required: int
    inodes_required: int


def _valid_nonnegative_integer(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def _gzip_upper_bound(payload_bytes: int) -> int:
    """Return zlib's conservative compressBound plus the gzip wrapper."""

    # zlib compressBound(): source + source/4096 + source/16384 +
    # source/33554432 + 13. gzip adds a 10-byte header and 8-byte trailer.
    return (
        payload_bytes
        + (payload_bytes >> 12)
        + (payload_bytes >> 14)
        + (payload_bytes >> 25)
        + 31
    )


def _json_atomic_size(payload: object) -> int:
    return len(
        (
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        ).encode("utf-8")
    )


def _compact_json(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _planned_batch_paths(
    store: Any,
    *,
    source_name: str,
    batch: ParsedBatch,
    raw_sha256: str,
) -> tuple[Path, Path, Path, Path]:
    first = batch.first_observation_sequence
    last = batch.last_observation_sequence
    if first is None or last is None:
        raise ValueError("Recorder transaction requires sequence-bounded observations.")
    day = _observation_storage_day(batch)
    source_slug = _slug(source_name)
    instance = str(batch.header.instance_id)
    base_name = f"seq-{first}-{last}-next-{batch.header.next_sequence}-{raw_sha256}"
    raw_path = _confined_storage_path(
        store.raw_root,
        source_slug,
        instance,
        day,
        f"{base_name}.xml.gz",
    )
    manifest_path = raw_path.with_suffix(".manifest.json")
    observation_path = _confined_storage_path(
        store.observation_root,
        source_slug,
        instance,
        day,
        f"{base_name}.ndjson",
    )
    normalized_path = _confined_storage_path(
        store.normalized_root,
        source_slug,
        instance,
        day,
        f"{base_name}.jsonl",
    )
    return raw_path, manifest_path, observation_path, normalized_path


def _raw_manifest_size(
    store: Any,
    *,
    source_name: str,
    requested_from: int,
    batch: ParsedBatch,
    raw_sha256: str,
) -> int:
    raw_path, _manifest, _observation, _normalized = _planned_batch_paths(
        store,
        source_name=source_name,
        batch=batch,
        raw_sha256=raw_sha256,
    )
    first = batch.first_observation_sequence
    last = batch.last_observation_sequence
    if first is None or last is None:
        raise ValueError("Recorder transaction requires sequence-bounded observations.")
    return _json_atomic_size(
        {
            "schema": RAW_BATCH_MANIFEST_SCHEMA,
            "source_name": source_name,
            "agent_instance_id": batch.header.instance_id,
            "requested_from": requested_from,
            "first_observation_sequence": first,
            "last_observation_sequence": last,
            "next_sequence": batch.header.next_sequence,
            "agent_first_sequence": batch.header.first_sequence,
            "agent_last_sequence": batch.header.last_sequence,
            "observation_count": len(batch.observations),
            "received_at": batch.observations[0].get("received_at"),
            "raw_sha256": raw_sha256,
            "raw_file": str(raw_path),
        }
    )


def _probe_requirement(
    store: Any,
    budget: RecorderResourceBudget,
    *,
    source_name: str,
    instance_id: int,
    probe: ProbeModel,
) -> _Requirement:
    probe_path = _confined_storage_path(
        store.probe_root,
        _slug(source_name),
        str(instance_id),
        f"probe-{probe.sha256}.xml.gz",
    )
    manifest_size = _json_atomic_size(
        {
            "schema": PROBE_MANIFEST_SCHEMA,
            "source_name": source_name,
            "agent_instance_id": instance_id,
            "probe_sha256": probe.sha256,
            "stored_at": _utc_now(),
            "raw_file": str(probe_path),
            "device_count": len(probe.devices),
            "data_item_count": len(probe.data_items),
        }
    )
    return _Requirement(
        path=Path(store.data_dir),
        bytes_required=_gzip_upper_bound(budget.probe_bytes) + manifest_size,
        inodes_required=budget.probe_inodes,
    )


def _frontier_sizes(
    store: Any,
    *,
    source_name: str,
    requested_from: int,
    batch: ParsedBatch,
    raw_sha256: str,
) -> tuple[int, int]:
    first = batch.first_observation_sequence
    last = batch.last_observation_sequence
    if first is None or last is None:
        raise ValueError("Recorder transaction requires sequence-bounded observations.")
    day = _observation_storage_day(batch)
    manifest_name = (
        f"seq-{first}-{last}-next-{batch.header.next_sequence}-"
        f"{raw_sha256}.xml.manifest.json"
    )
    pending = _json_atomic_size(
        {
            "schema": "fcp.mtconnect.raw_recovery_frontier.v1",
            "state": "pending",
            "source_name": source_name,
            "agent_instance_id": batch.header.instance_id,
            "requested_from": int(requested_from),
            "first_sequence": int(first),
            "last_sequence": int(last),
            "next_sequence": int(batch.header.next_sequence),
            "observation_count": len(batch.observations),
            "raw_sha256": raw_sha256,
            "day": day,
            "manifest_name": manifest_name,
            "updated_at": _utc_now(),
        }
    )
    clear = _json_atomic_size(
        {
            "schema": "fcp.mtconnect.raw_recovery_frontier.v1",
            "state": "clear",
            "source_name": source_name,
            "agent_instance_id": int(batch.header.instance_id),
            "next_sequence": int(batch.header.next_sequence),
            "updated_at": _utc_now(),
        }
    )
    # Prove the pointer path itself is confined before admission begins.
    _confined_storage_path(
        store.raw_root,
        _slug(source_name),
        str(int(batch.header.instance_id)),
        ".recovery-frontier.json",
    )
    return pending, clear


class RecorderAdmissionController(ProcessResourceAdmission):
    """Shared controller with an explicit already-durable completion rule."""

    @contextmanager
    def reserve_completion(
        self,
        path: Path | str,
        *,
        bytes_required: int,
        inodes_required: int = 0,
    ) -> Iterator[ResourceReservation]:
        if not _valid_nonnegative_integer(
            bytes_required
        ) or not _valid_nonnegative_integer(inodes_required):
            raise ValueError("resource requirement must be non-negative integers")

        measurement = self._measure(path)  # noqa: SLF001
        resource_id = measurement.resource_id
        with self._lock:  # noqa: SLF001
            active_bytes, active_inodes = self._active_for(resource_id)  # noqa: SLF001
            before = assess_measurement(
                measurement,
                thresholds=self.thresholds,
                reserved_bytes=active_bytes,
                reserved_inodes=active_inodes,
                now=self.clock(),
            )
            if before.level == PressureLevel.CRITICAL:
                raise HostResourceRefused("resource_critical", before)

            after = assess_measurement(
                measurement,
                thresholds=self.thresholds,
                reserved_bytes=active_bytes + bytes_required,
                reserved_inodes=active_inodes + inodes_required,
                now=self.clock(),
            )
            if after.level == PressureLevel.CRITICAL:
                raise HostResourceRefused("emergency_reserve", after)

            self._reserved[resource_id] = (  # noqa: SLF001
                active_bytes + bytes_required,
                active_inodes + inodes_required,
            )
            reservation = ResourceReservation(
                resource_id=resource_id,
                reserved_bytes=bytes_required,
                reserved_inodes=inodes_required,
            )
        try:
            yield reservation
        finally:
            with self._lock:  # noqa: SLF001
                current_bytes, current_inodes = self._active_for(  # noqa: SLF001
                    resource_id
                )
                remaining = (
                    max(current_bytes - bytes_required, 0),
                    max(current_inodes - inodes_required, 0),
                )
                if remaining == (0, 0):
                    self._reserved.pop(resource_id, None)  # noqa: SLF001
                else:
                    self._reserved[resource_id] = remaining  # noqa: SLF001


class RecorderResourceGuard:
    """Own one process-wide controller and one transaction per worker thread."""

    def __init__(
        self,
        runtime: Any,
        *,
        controller: RecorderAdmissionController | None = None,
        budget: RecorderResourceBudget | None = None,
    ) -> None:
        self.runtime = runtime
        self.controller = controller or RecorderAdmissionController()
        self.budget = budget or RecorderResourceBudget()
        self._lock = threading.RLock()
        self._transactions: dict[int, ExitStack] = {}
        self._refusals: dict[int, RecorderResourcePause] = {}
        self._capture_urls: dict[int, tuple[str, str]] = {}
        self._store: Any = None

    def attach_store(self, store: Any) -> None:
        with self._lock:
            previous = self._store
            if previous is not None and getattr(
                previous, "_recorder_resource_guard", None
            ) is self:
                delattr(previous, "_recorder_resource_guard")
            self._store = store
            setattr(store, "_recorder_resource_guard", self)

    def attached_to(self, store: Any) -> bool:
        with self._lock:
            return self._store is store and getattr(
                store, "_recorder_resource_guard", None
            ) is self

    def begin_capture(self, source_name: str, base_url: str) -> None:
        with self._lock:
            self._capture_urls[threading.get_ident()] = (source_name, base_url)

    def end_capture(self) -> None:
        with self._lock:
            self._capture_urls.pop(threading.get_ident(), None)

    def in_capture(self) -> bool:
        with self._lock:
            return threading.get_ident() in self._capture_urls

    def in_transaction(self) -> bool:
        """Report whether this thread currently holds an admitted reservation."""

        with self._lock:
            return threading.get_ident() in self._transactions

    def record_storage_exhaustion(self, path: Path | str) -> bool:
        """Record a real filesystem refusal as a measured local pause.

        Admission reserves against an *estimate*. A concurrent writer, another
        process, or an underestimate can still leave the host with no room by
        the time the admitted write runs, and the filesystem then refuses it
        directly. That is the same local condition the controller refuses for,
        so it must reach the same pause path rather than the remote-source
        error boundary -- otherwise a healthy Agent is marked failed and backed
        off for the host's disk being full.

        Nothing is fabricated: the pause carries a fresh measurement of the
        resource that just refused. When that resource cannot be measured, the
        shared controller returns an explicitly unavailable assessment rather
        than a healthy-looking one, so the pause reports
        ``measurement_unavailable`` with no capacity figures at all. It stays a
        pause: the refusal is itself first-hand evidence that the host had no
        room, and falling back to the source error path because the follow-up
        measurement failed would blame the Agent for exactly the condition this
        boundary exists to attribute correctly. ``False`` is returned only if no
        assessment can be produced at all, which the shared controller does not
        do; the caller then keeps the original ``OSError``.
        """

        try:
            assessment = self.controller.assessment(path)
        except (OSError, ValueError, RuntimeError):
            return False
        with self._lock:
            self._refusals[threading.get_ident()] = RecorderResourcePause(
                code=STORAGE_EXHAUSTED,
                assessment=assessment,
            )
        return True

    def _capture_base_url(self, source_name: str) -> str:
        with self._lock:
            active = self._capture_urls.get(threading.get_ident())
        if active is not None and active[0] == source_name:
            return active[1]
        checkpoint = self.runtime.checkpoints.get(source_name)
        if checkpoint is not None:
            return checkpoint.base_url
        return str(self.runtime.sources.get(source_name) or "")

    def _state_bytes_upper_bound(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str,
    ) -> int:
        raw_path, _manifest, observation_path, normalized_path = _planned_batch_paths(
            self._store,
            source_name=source_name,
            batch=batch,
            raw_sha256=raw_sha256,
        )
        current = self.runtime.checkpoints.get(source_name)
        first_record = batch.observations[0]
        probe = self.runtime.probes.get(source_name)
        target = {
            "base_url": self._capture_base_url(source_name),
            "machine_id": str(first_record.get("machine_id") or source_name),
            "agent_instance_id": batch.header.instance_id,
            "next_sequence": batch.header.next_sequence,
            "probe_sha256": (
                probe.sha256
                if probe is not None
                else (current.probe_sha256 if current is not None else "0" * 64)
            ),
            "storage_aliases": (
                list(current.storage_aliases) if current is not None else []
            ),
            "last_raw_file": str(raw_path),
            "last_observation_file": str(observation_path),
            "last_normalized_file": str(normalized_path),
            "latest_values": {},
            "updated_at": _utc_now(),
        }
        sources = {
            name: checkpoint.to_dict()
            for name, checkpoint in sorted(self.runtime.checkpoints.items())
            if name != source_name
        }
        sources[source_name] = target
        skeleton = {
            "schema": CHECKPOINT_SCHEMA,
            "updated_at": _utc_now(),
            "sources": dict(sorted(sources.items())),
        }
        encoded = _compact_json(skeleton)
        # The bounded compatibility writer validates the target latest_values
        # with json.dumps' default separators. Compact checkpoint JSON is never
        # larger for the same object, so replacing the two-byte empty object in
        # the skeleton with that finite state is bounded by this addition.
        return len(encoded) - 2 + self.budget.compatibility_state_bytes

    def _requirements(
        self,
        *,
        source_name: str,
        requested_from: int,
        batch: ParsedBatch,
        xml_text: str | None,
        completion: bool,
        raw_sha256: str | None = None,
    ) -> tuple[_Requirement, _Requirement]:
        digest = raw_sha256 or sha256((xml_text or "").encode("utf-8")).hexdigest()
        pending_bytes, clear_bytes = _frontier_sizes(
            self._store,
            source_name=source_name,
            requested_from=requested_from,
            batch=batch,
            raw_sha256=digest,
        )
        data_bytes = (
            self.budget.observation_bytes
            + self.budget.compatibility_bytes
            + clear_bytes
        )
        if not completion:
            data_bytes += (
                _gzip_upper_bound(self.budget.sample_bytes)
                + _raw_manifest_size(
                    self._store,
                    source_name=source_name,
                    requested_from=requested_from,
                    batch=batch,
                    raw_sha256=digest,
                )
                + pending_bytes
            )
        state_bytes = self._state_bytes_upper_bound(
            source_name=source_name,
            batch=batch,
            raw_sha256=digest,
        )
        return (
            _Requirement(
                path=Path(self._store.data_dir),
                bytes_required=data_bytes,
                inodes_required=self.budget.data_inodes,
            ),
            _Requirement(
                path=_state_file_for_runtime(self.runtime),
                bytes_required=state_bytes,
                inodes_required=self.budget.state_inodes,
            ),
        )

    def _group_requirements(
        self,
        requirements: tuple[_Requirement, ...],
    ) -> tuple[_GroupedRequirement, ...]:
        grouped: dict[str, _GroupedRequirement] = {}
        for requirement in requirements:
            assessment = self.controller.assessment(requirement.path)
            current = grouped.get(assessment.resource_id)
            if current is None:
                grouped[assessment.resource_id] = _GroupedRequirement(
                    path=requirement.path,
                    bytes_required=requirement.bytes_required,
                    inodes_required=requirement.inodes_required,
                )
            else:
                current.bytes_required += requirement.bytes_required
                current.inodes_required += requirement.inodes_required
        return tuple(grouped.values())

    def _record_refusal(self, refusal: HostResourceRefused) -> None:
        with self._lock:
            self._refusals[threading.get_ident()] = RecorderResourcePause(
                code=refusal.code,
                assessment=refusal.assessment,
            )

    @contextmanager
    def reserve_probe(
        self,
        *,
        source_name: str,
        instance_id: int,
        probe: ProbeModel,
    ) -> Iterator[None]:
        requirement = _probe_requirement(
            self._store,
            self.budget,
            source_name=source_name,
            instance_id=instance_id,
            probe=probe,
        )
        try:
            with self.controller.reserve(
                requirement.path,
                bytes_required=requirement.bytes_required,
                inodes_required=requirement.inodes_required,
            ):
                yield
        except HostResourceRefused as exc:
            self._record_refusal(exc)
            raise _RecorderPauseSignal from None

    def _begin(
        self,
        requirements: tuple[_Requirement, ...],
        *,
        completion: bool,
    ) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            if thread_id in self._transactions:
                return
        stack = ExitStack()
        try:
            for requirement in self._group_requirements(requirements):
                reserve = (
                    self.controller.reserve_completion
                    if completion
                    else self.controller.reserve
                )
                stack.enter_context(
                    reserve(
                        requirement.path,
                        bytes_required=requirement.bytes_required,
                        inodes_required=requirement.inodes_required,
                    )
                )
        except HostResourceRefused as exc:
            stack.close()
            self._record_refusal(exc)
            raise _RecorderPauseSignal from None
        with self._lock:
            self._transactions[thread_id] = stack

    def begin_new(
        self,
        *,
        source_name: str,
        requested_from: int,
        xml_text: str,
        batch: ParsedBatch,
    ) -> None:
        self._begin(
            self._requirements(
                source_name=source_name,
                requested_from=requested_from,
                batch=batch,
                xml_text=xml_text,
                completion=False,
            ),
            completion=False,
        )

    def ensure_completion(
        self,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str,
    ) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            if thread_id in self._transactions:
                return
        requested_from = batch.first_observation_sequence
        if requested_from is None:
            raise ValueError("Recovery batch does not contain a first sequence.")
        self._begin(
            self._requirements(
                source_name=source_name,
                requested_from=requested_from,
                batch=batch,
                xml_text=None,
                completion=True,
                raw_sha256=raw_sha256,
            ),
            completion=True,
        )

    def end_transaction(self) -> None:
        with self._lock:
            stack = self._transactions.pop(threading.get_ident(), None)
        if stack is not None:
            stack.close()

    def take_refusal(self) -> RecorderResourcePause | None:
        with self._lock:
            return self._refusals.pop(threading.get_ident(), None)


def _state_file_for_runtime(runtime: Any) -> Path:
    configured = getattr(runtime, "_resource_state_file", None)
    if configured is None:
        raise RuntimeError("Recorder resource state file is unavailable.")
    return Path(configured)


def attach_runtime_resource_pressure(
    runtime: Any,
    *,
    controller: RecorderAdmissionController | None = None,
    budget: RecorderResourceBudget | None = None,
    state_file: Path | None = None,
) -> RecorderResourceGuard:
    """Attach deterministic admission to a runtime/store, primarily for tests."""

    guard = RecorderResourceGuard(
        runtime,
        controller=controller,
        budget=budget,
    )
    runtime._recorder_resource_guard = guard
    if state_file is not None:
        runtime._resource_state_file = Path(state_file)
    guard.attach_store(runtime.store)
    return guard


def _runtime_module_value(runtime: Any, name: str, default: Any) -> Any:
    module = sys.modules.get(runtime.__class__.__module__)
    return getattr(module, name, default) if module is not None else default


def _admitted_storage_write(
    guard: Any,
    path: Any,
    *,
    require_transaction: bool = False,
) -> Any:
    """Reclassify a host-storage refusal raised inside an admitted write.

    Admission is tested where the refusal actually surfaces rather than where
    the scope is entered. A wrapper that spans a whole admitted region only
    learns of the refusal on the way out, and entering it before the
    transaction exists would answer the wrong question.

    Only an admitted write may be reclassified. ``require_transaction`` demands
    a registered transaction; the default also accepts a capture, which is the
    scope holding the probe reservation -- that reservation is admitted without
    registering a transaction of its own. A ``save_state`` outside either --
    checkpoint alias reconciliation, for example -- keeps its ordinary failure,
    because the pause signal is a control flow the capture and recovery
    wrappers own and nothing else is prepared to catch.
    """

    @contextmanager
    def _scope() -> Iterator[None]:
        if guard is None:
            yield
            return
        try:
            yield
        except OSError as exc:
            if not _is_storage_exhaustion(exc):
                raise
            admitted = (
                guard.in_transaction()
                if require_transaction
                else (guard.in_capture() or guard.in_transaction())
            )
            if not admitted:
                raise
            target = getattr(exc, "filename", None) or path
            if not guard.record_storage_exhaustion(target):
                raise
            raise _RecorderPauseSignal from None

    return _scope()


def _apply_pause_status(
    runtime: Any,
    source_name: str,
    base_url: str,
    pause: RecorderResourcePause,
) -> None:
    assessment = pause.assessment
    with runtime.lock:
        source = runtime.source_status.setdefault(source_name, {"base_url": base_url})
        source.update(
            {
                "base_url": base_url,
                "last_error": "",
                "next_retry_seconds": RESOURCE_PRESSURE_RETRY_SECONDS,
                "resource_admission": {
                    "state": "paused",
                    "level": assessment.level.name.lower(),
                    "code": pause.code,
                    "reasons": list(assessment.reasons),
                    "effective_free_bytes": assessment.effective_free_bytes,
                    "effective_free_inodes": assessment.effective_free_inodes,
                    "retry_after_seconds": RESOURCE_PRESSURE_RETRY_SECONDS,
                },
            }
        )
        runtime.backoff[source_name] = _runtime_module_value(
            runtime,
            "BACKOFF_INITIAL",
            0.5,
        )
        runtime.next_attempt_at[source_name] = (
            time.monotonic() + RESOURCE_PRESSURE_RETRY_SECONDS
        )


def install_runtime_resource_pressure(runtime_module: ModuleType) -> None:
    """Install the pressure boundary once on the lazy recorder runtime module."""

    if getattr(runtime_module, "_RESOURCE_PRESSURE_INSTALLED", False):
        return

    runtime_class = runtime_module.RecorderRuntime
    original_init = runtime_class.__init__
    original_capture = runtime_class.capture_source
    original_recover = runtime_class._recover_archived_batches
    original_harvest = runtime_class._harvest_capture_results
    original_save_state = runtime_class.save_state
    store_class = runtime_module.DurableRecorderStore
    original_store_probe = store_class.store_probe
    original_store_observation = store_class.store_observation_batch
    original_store_batch = store_class.store_batch
    frontier_class = runtime_module.RecorderRecoveryFrontier

    def resource_init(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        self._resource_state_file = Path(runtime_module.STATE_FILE)
        attach_runtime_resource_pressure(
            self,
            state_file=Path(runtime_module.STATE_FILE),
        )

    def resource_save_state(self: Any) -> None:
        guard = getattr(self, "_recorder_resource_guard", None)
        if guard is None or not guard.attached_to(self.store):
            original_save_state(self)
            return
        with self.lock:
            payload = {
                "schema": runtime_module._CHECKPOINT_SCHEMA,
                "updated_at": runtime_module._utc_now(),
                "sources": {
                    name: checkpoint.to_dict()
                    for name, checkpoint in sorted(self.checkpoints.items())
                },
                # Pause/drain incidents are safety state, not optional
                # diagnostics. Keep them in the resource-admitted writer too,
                # so a restart cannot turn an unproven storage boundary into
                # an acknowledged pause.
                "capture_drain_failures": list(
                    getattr(self, "capture_drain_failures", ())
                ),
            }
            state_file = Path(runtime_module.STATE_FILE)
            with _admitted_storage_write(guard, state_file):
                _write_bytes_atomic(state_file, _compact_json(payload))

    def resource_store_probe(
        self: Any,
        *,
        source_name: str,
        instance_id: int,
        xml_text: str,
        probe: ProbeModel,
    ) -> Path:
        guard = getattr(self, "_recorder_resource_guard", None)
        if guard is None:
            return original_store_probe(
                self,
                source_name=source_name,
                instance_id=instance_id,
                xml_text=xml_text,
                probe=probe,
            )
        with guard.reserve_probe(
            source_name=source_name,
            instance_id=instance_id,
            probe=probe,
        ), _admitted_storage_write(guard, self.probe_root):
            return original_store_probe(
                self,
                source_name=source_name,
                instance_id=instance_id,
                xml_text=xml_text,
                probe=probe,
            )

    def resource_store_observation(
        self: Any,
        *,
        source_name: str,
        batch: ParsedBatch,
        raw_sha256: str | None = None,
    ) -> Path:
        guard = getattr(self, "_recorder_resource_guard", None)
        if guard is not None and raw_sha256 is not None:
            guard.ensure_completion(
                source_name=source_name,
                batch=batch,
                raw_sha256=raw_sha256,
            )
        with _admitted_storage_write(guard, self.observation_root):
            return original_store_observation(
                self,
                source_name=source_name,
                batch=batch,
                raw_sha256=raw_sha256,
            )

    class ResourceAwareRecoveryFrontier(frontier_class):
        def mark_pending(
            self,
            *,
            source_name: str,
            requested_from: int,
            xml_text: str,
            batch: ParsedBatch,
        ) -> Path:
            guard = getattr(self.store, "_recorder_resource_guard", None)
            if guard is not None:
                guard.begin_new(
                    source_name=source_name,
                    requested_from=requested_from,
                    xml_text=xml_text,
                    batch=batch,
                )
            try:
                # The pending marker is the transaction's first durable write,
                # so it is also the first thing a full host refuses. Its own
                # unwind ends the transaction before the refusal reaches the
                # capture wrapper, so the reclassification has to happen here.
                with _admitted_storage_write(
                    guard,
                    self.store.raw_root,
                    require_transaction=True,
                ):
                    return super().mark_pending(
                        source_name=source_name,
                        requested_from=requested_from,
                        xml_text=xml_text,
                        batch=batch,
                    )
            except BaseException:
                if guard is not None:
                    guard.end_transaction()
                raise

        def mark_clear(
            self,
            *,
            source_name: str,
            instance_id: int,
            next_sequence: int,
        ) -> Path:
            guard = getattr(self.store, "_recorder_resource_guard", None)
            try:
                # ``mark_clear`` also closes the transaction on the way out, and
                # a legacy migration clear runs with no transaction at all --
                # that one is not an admitted write and keeps its own failure.
                with _admitted_storage_write(
                    guard,
                    self.store.raw_root,
                    require_transaction=True,
                ):
                    return super().mark_clear(
                        source_name=source_name,
                        instance_id=instance_id,
                        next_sequence=next_sequence,
                    )
            finally:
                if guard is not None:
                    guard.end_transaction()

    def resource_store_batch(self: Any, **kwargs: Any) -> Any:
        guard = getattr(self, "_recorder_resource_guard", None)
        with _admitted_storage_write(guard, self.raw_root):
            return original_store_batch(self, **kwargs)

    def resource_capture(
        self: Any,
        source_name: str,
        base_url: str,
    ) -> tuple[str, bool, str]:
        guard = getattr(self, "_recorder_resource_guard", None)
        if guard is None or not guard.attached_to(self.store):
            return original_capture(self, source_name, base_url)
        guard.begin_capture(source_name, base_url)
        try:
            try:
                result = original_capture(self, source_name, base_url)
            except _RecorderPauseSignal:
                pause = guard.take_refusal()
                if pause is None:
                    raise RuntimeError("Recorder resource pause lost its assessment.")
                _apply_pause_status(self, source_name, base_url, pause)
                return source_name, True, ""
            with self.lock:
                source = self.source_status.get(source_name)
                if source is not None:
                    source.pop("resource_admission", None)
            return result
        finally:
            guard.end_transaction()
            guard.end_capture()

    def resource_recover(self: Any, *args: Any, **kwargs: Any) -> int:
        guard = getattr(self, "_recorder_resource_guard", None)
        if guard is None or not guard.attached_to(self.store):
            return original_recover(self, *args, **kwargs)
        try:
            # Recovery publishes through writers that compose more than the two
            # wrapped store methods -- the normalized view is written directly,
            # and the publication-discovery record is written by the derived
            # store after the wrapped observation writer has returned. This
            # catch covers the whole admitted region rather than enumerating
            # them, and reclassifies only while the reservation is still held.
            with _admitted_storage_write(
                guard,
                self.store.data_dir,
                require_transaction=True,
            ):
                return original_recover(self, *args, **kwargs)
        except _RecorderPauseSignal:
            if guard.in_capture():
                raise
            pause = guard.take_refusal()
            if pause is None:
                raise RuntimeError("Recorder recovery pause lost its assessment.")
            raise RecorderResourcePaused(pause) from None
        finally:
            guard.end_transaction()

    def resource_harvest(self: Any) -> None:
        original_harvest(self)
        with self.lock:
            paused = [
                name
                for name, status in self.source_status.items()
                if isinstance(status.get("resource_admission"), dict)
                and status["resource_admission"].get("state") == "paused"
            ]
            if paused and self.enabled and self.configuration_ready:
                self.state = "degraded"
                self.last_error = ""
                self.message = (
                    "Recorder capture is paused by local host resource pressure "
                    f"for {len(paused)} source(s); primary evidence is retained."
                )

    runtime_class.__init__ = resource_init
    runtime_class.save_state = resource_save_state
    runtime_class.capture_source = resource_capture
    runtime_class._recover_archived_batches = resource_recover
    runtime_class._harvest_capture_results = resource_harvest
    store_class.store_probe = resource_store_probe
    store_class.store_observation_batch = resource_store_observation
    store_class.store_batch = resource_store_batch
    runtime_module.RecorderRecoveryFrontier = ResourceAwareRecoveryFrontier
    runtime_module._RESOURCE_PRESSURE_INSTALLED = True


__all__ = [
    "RESOURCE_PRESSURE_RETRY_SECONDS",
    "STORAGE_EXHAUSTED",
    "RecorderAdmissionController",
    "RecorderResourceBudget",
    "RecorderResourceGuard",
    "RecorderResourcePause",
    "RecorderResourcePaused",
    "attach_runtime_resource_pressure",
    "install_runtime_resource_pressure",
]

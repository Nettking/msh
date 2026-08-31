"""Process-wide host-resource admission for bounded recorder transactions.

The detailed recorder pressure integration lives in ``_resource_pressure_impl``.
This public seam strengthens its controller semantics so recorder capture shares
the exact process-wide admission controller used by other supported writers and
admits multi-filesystem transactions atomically.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureLevel,
    ResourceReservation,
    assess_measurement,
)
from catalog.federation.process_resource_admission import (
    PROCESS_RESOURCE_ADMISSION,
    SerializedProcessResourceAdmission,
)

from . import _resource_pressure_impl as _impl


def _conservative_measurement(
    current: FilesystemMeasurement,
    candidate: FilesystemMeasurement,
) -> FilesystemMeasurement:
    if not current.available:
        return current
    if not candidate.available:
        return candidate
    if current.free_bytes is None:
        return current
    if candidate.free_bytes is None:
        return candidate
    if candidate.free_bytes < current.free_bytes:
        return candidate
    if candidate.free_bytes > current.free_bytes:
        return current
    if (
        current.free_inodes is not None
        and candidate.free_inodes is not None
        and candidate.free_inodes < current.free_inodes
    ):
        return candidate
    return current


def _validate_requirement(bytes_required: object, inodes_required: object) -> None:
    for value in (bytes_required, inodes_required):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("resource requirement must be non-negative integers")


@contextmanager
def _reserve_completion_many(
    controller: SerializedProcessResourceAdmission,
    requirements: Iterable[tuple[Path | str, int, int]],
) -> Iterator[tuple[ResourceReservation, ...]]:
    """Atomically reserve already-durable recovery completion across resources.

    Recovery may proceed from PRESSURE only when its bounded completion envelope
    still leaves the CRITICAL emergency floor intact. CRITICAL or unavailable
    measurements fail closed. Accounting is published only after every backing
    resource passes.
    """

    requested = tuple(requirements)
    for _path, bytes_required, inodes_required in requested:
        _validate_requirement(bytes_required, inodes_required)

    reservations: tuple[ResourceReservation, ...] = ()
    with controller._lock:
        grouped: dict[str, tuple[FilesystemMeasurement, int, int]] = {}
        for path, bytes_required, inodes_required in requested:
            measurement = controller._measure(path)
            existing = grouped.get(measurement.resource_id)
            if existing is None:
                grouped[measurement.resource_id] = (
                    measurement,
                    bytes_required,
                    inodes_required,
                )
                continue
            prior_measurement, prior_bytes, prior_inodes = existing
            grouped[measurement.resource_id] = (
                _conservative_measurement(prior_measurement, measurement),
                prior_bytes + bytes_required,
                prior_inodes + inodes_required,
            )

        pending: list[ResourceReservation] = []
        for resource_id, (
            measurement,
            bytes_required,
            inodes_required,
        ) in grouped.items():
            active_bytes, active_inodes = controller._active_for(resource_id)
            before = assess_measurement(
                measurement,
                thresholds=controller.thresholds,
                reserved_bytes=active_bytes,
                reserved_inodes=active_inodes,
                now=controller.clock(),
            )
            if before.level == PressureLevel.CRITICAL:
                raise HostResourceRefused("resource_critical", before)

            after = assess_measurement(
                measurement,
                thresholds=controller.thresholds,
                reserved_bytes=active_bytes + bytes_required,
                reserved_inodes=active_inodes + inodes_required,
                now=controller.clock(),
            )
            if after.level == PressureLevel.CRITICAL:
                raise HostResourceRefused("emergency_reserve", after)
            pending.append(
                ResourceReservation(
                    resource_id=resource_id,
                    reserved_bytes=bytes_required,
                    reserved_inodes=inodes_required,
                )
            )

        for reservation in pending:
            active_bytes, active_inodes = controller._active_for(
                reservation.resource_id
            )
            controller._reserved[reservation.resource_id] = (
                active_bytes + reservation.reserved_bytes,
                active_inodes + reservation.reserved_inodes,
            )
        reservations = tuple(pending)

    try:
        yield reservations
    finally:
        with controller._lock:
            for reservation in reservations:
                current_bytes, current_inodes = controller._active_for(
                    reservation.resource_id
                )
                remaining = (
                    max(current_bytes - reservation.reserved_bytes, 0),
                    max(current_inodes - reservation.reserved_inodes, 0),
                )
                if remaining == (0, 0):
                    controller._reserved.pop(reservation.resource_id, None)
                else:
                    controller._reserved[reservation.resource_id] = remaining


class RecorderAdmissionController(SerializedProcessResourceAdmission):
    """Injectable serialized controller retaining recovery-completion semantics."""

    @contextmanager
    def reserve_completion(
        self,
        path: Path | str,
        *,
        bytes_required: int,
        inodes_required: int = 0,
    ) -> Iterator[ResourceReservation]:
        with _reserve_completion_many(
            self,
            ((path, bytes_required, inodes_required),),
        ) as reservations:
            if not reservations:
                raise RuntimeError(
                    "single-resource completion admission returned no reservation"
                )
            yield reservations[0]


class RecorderResourceGuard(_impl.RecorderResourceGuard):
    """Recorder guard backed by the shared process-wide admission controller."""

    def __init__(
        self,
        runtime: Any,
        *,
        controller: SerializedProcessResourceAdmission | None = None,
        budget: _impl.RecorderResourceBudget | None = None,
    ) -> None:
        super().__init__(
            runtime,
            controller=controller or PROCESS_RESOURCE_ADMISSION,
            budget=budget,
        )

    def _begin(
        self,
        requirements: tuple[_impl._Requirement, ...],
        *,
        completion: bool,
    ) -> None:
        thread_id = __import__("threading").get_ident()
        with self._lock:
            if thread_id in self._transactions:
                return

        stack = ExitStack()
        tuples = tuple(
            (requirement.path, requirement.bytes_required, requirement.inodes_required)
            for requirement in requirements
        )
        try:
            if completion:
                stack.enter_context(_reserve_completion_many(self.controller, tuples))
            else:
                stack.enter_context(self.controller.reserve_many(tuples))
        except HostResourceRefused as exc:
            stack.close()
            self._record_refusal(exc)
            raise _impl._RecorderPauseSignal from None

        with self._lock:
            self._transactions[thread_id] = stack


def attach_runtime_resource_pressure(
    runtime: Any,
    *,
    controller: SerializedProcessResourceAdmission | None = None,
    budget: _impl.RecorderResourceBudget | None = None,
    state_file: Path | None = None,
) -> RecorderResourceGuard:
    """Attach shared admission to a runtime/store, with optional test injection."""

    guard = RecorderResourceGuard(runtime, controller=controller, budget=budget)
    runtime._recorder_resource_guard = guard
    if state_file is not None:
        runtime._resource_state_file = Path(state_file)
    guard.attach_store(runtime.store)
    return guard


# The implementation's installer resolves these globals at call time. Patch the
# seam once so lazy runtime installation receives the strengthened controller.
_impl.RecorderAdmissionController = RecorderAdmissionController
_impl.RecorderResourceGuard = RecorderResourceGuard
_impl.attach_runtime_resource_pressure = attach_runtime_resource_pressure

RESOURCE_PRESSURE_RETRY_SECONDS = _impl.RESOURCE_PRESSURE_RETRY_SECONDS
RecorderResourceBudget = _impl.RecorderResourceBudget
RecorderResourcePause = _impl.RecorderResourcePause
RecorderResourcePaused = _impl.RecorderResourcePaused
install_runtime_resource_pressure = _impl.install_runtime_resource_pressure

__all__ = [
    "RESOURCE_PRESSURE_RETRY_SECONDS",
    "RecorderAdmissionController",
    "RecorderResourceBudget",
    "RecorderResourceGuard",
    "RecorderResourcePause",
    "RecorderResourcePaused",
    "attach_runtime_resource_pressure",
    "install_runtime_resource_pressure",
]

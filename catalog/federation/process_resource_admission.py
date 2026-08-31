"""Process-wide host-resource admission shared by bounded FCP writers.

The default controller serializes filesystem measurement with reservation
accounting. This closes the measure-before-lock race for all supported writers
using the process-wide seam, and exposes one atomic multi-resource reservation
for logical transactions that span several paths.

The base ProcessResourceAdmission remains import-compatible for callers outside
this shared seam. New supported runtime writers should use
PROCESS_RESOURCE_ADMISSION or inject SerializedProcessResourceAdmission in tests.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path

from .host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureLevel,
    ProcessResourceAdmission,
    ResourceAssessment,
    ResourceReservation,
    assess_measurement,
)


def _valid_requirement(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def _conservative_measurement(
    current: FilesystemMeasurement,
    candidate: FilesystemMeasurement,
) -> FilesystemMeasurement:
    """Keep the least optimistic measurement observed for one resource."""

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


class SerializedProcessResourceAdmission(ProcessResourceAdmission):
    """Admission whose measurement and accounting decision form one critical section."""

    def assessment(self, path: Path | str) -> ResourceAssessment:
        with self._lock:
            measurement = self._measure(path)
            reserved_bytes, reserved_inodes = self._active_for(measurement.resource_id)
            return assess_measurement(
                measurement,
                thresholds=self.thresholds,
                reserved_bytes=reserved_bytes,
                reserved_inodes=reserved_inodes,
                now=self.clock(),
            )

    @contextmanager
    def reserve_many(
        self,
        requirements: Iterable[tuple[Path | str, int, int]],
    ) -> Iterator[tuple[ResourceReservation, ...]]:
        """Atomically admit and account one logical transaction across resources.

        Requirements resolving to the same backing resource are coalesced before
        pressure/emergency-reserve decisions. No accounting is published until
        every resource in the transaction has passed admission.
        """

        requested = tuple(requirements)
        for _path, bytes_required, inodes_required in requested:
            if not _valid_requirement(bytes_required) or not _valid_requirement(
                inodes_required
            ):
                raise ValueError("resource requirement must be non-negative integers")

        grouped: dict[str, tuple[FilesystemMeasurement, int, int]] = {}
        reservations: tuple[ResourceReservation, ...] = ()
        with self._lock:
            for path, bytes_required, inodes_required in requested:
                measurement = self._measure(path)
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
                active_bytes, active_inodes = self._active_for(resource_id)
                before = assess_measurement(
                    measurement,
                    thresholds=self.thresholds,
                    reserved_bytes=active_bytes,
                    reserved_inodes=active_inodes,
                    now=self.clock(),
                )
                if before.level >= PressureLevel.PRESSURE:
                    raise HostResourceRefused("resource_pressure", before)

                after = assess_measurement(
                    measurement,
                    thresholds=self.thresholds,
                    reserved_bytes=active_bytes + bytes_required,
                    reserved_inodes=active_inodes + inodes_required,
                    now=self.clock(),
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
                active_bytes, active_inodes = self._active_for(reservation.resource_id)
                self._reserved[reservation.resource_id] = (
                    active_bytes + reservation.reserved_bytes,
                    active_inodes + reservation.reserved_inodes,
                )
            reservations = tuple(pending)

        try:
            yield reservations
        finally:
            with self._lock:
                for reservation in reservations:
                    current_bytes, current_inodes = self._active_for(
                        reservation.resource_id
                    )
                    remaining = (
                        max(current_bytes - reservation.reserved_bytes, 0),
                        max(current_inodes - reservation.reserved_inodes, 0),
                    )
                    if remaining == (0, 0):
                        self._reserved.pop(reservation.resource_id, None)
                    else:
                        self._reserved[reservation.resource_id] = remaining

    @contextmanager
    def reserve(
        self,
        path: Path | str,
        *,
        bytes_required: int,
        inodes_required: int = 0,
    ) -> Iterator[ResourceReservation]:
        with self.reserve_many(((path, bytes_required, inodes_required),)) as reservations:
            if not reservations:
                raise RuntimeError("single-resource admission returned no reservation")
            yield reservations[0]


PROCESS_RESOURCE_ADMISSION = SerializedProcessResourceAdmission()

__all__ = [
    "PROCESS_RESOURCE_ADMISSION",
    "SerializedProcessResourceAdmission",
]

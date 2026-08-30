"""Shared host-resource pressure and admission primitives for FCP.

The robustness contract distinguishes four states for any backing filesystem:
NORMAL, WARNING, PRESSURE and CRITICAL. The state is deliberately conservative:
an unavailable, stale or time-invalid measurement is CRITICAL rather than healthy.

This module is intentionally narrower than a host daemon. It provides:

* path -> backing-filesystem measurement with byte and, where exposed, inode state;
* one shared pressure vocabulary and threshold policy;
* coalescing by backing resource identity, so two paths on one filesystem consume
  one resource envelope rather than two imaginary budgets; and
* in-process reservation accounting, which is sufficient to stop several bounded
  recorder/Flask worker transactions from all independently spending the same
  free-space reserve.

Cross-process/host writers still have to use the same contract at their host-owned
boundary. This module does not claim that an in-process reservation can reserve
space against an unrelated process; consumers must remeasure immediately before
large work and later host-integration deliveries must cover their actual backing
resources.
"""

from __future__ import annotations

import math
import os
import shutil
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path

GIBIBYTE = 1024**3

# Existing v1 storage/update safety work already converged on a 10 GiB emergency
# floor. Keep that value as the CRITICAL boundary rather than creating a third,
# incompatible floor. PRESSURE adds 2 GiB so bounded active recorder work can
# finish after new large writers are stopped; WARNING adds one measured update
# cycle (4 GiB) so the host signals shrinking headroom before PRESSURE.
DEFAULT_CRITICAL_FREE_BYTES = 10 * GIBIBYTE
DEFAULT_PRESSURE_FREE_BYTES = 12 * GIBIBYTE
DEFAULT_WARNING_FREE_BYTES = 16 * GIBIBYTE

# Inode/file exhaustion is independent of byte exhaustion on filesystems that
# expose inode accounting. These values are intentionally small absolute
# reserves: CRITICAL leaves enough file identities for atomic temp/final,
# checkpoint/status/WAL/journal completion; PRESSURE/WARNING stop new large work
# progressively earlier. Filesystems without inode accounting do not fabricate
# a number and are judged on bytes only.
DEFAULT_CRITICAL_FREE_INODES = 128
DEFAULT_PRESSURE_FREE_INODES = 512
DEFAULT_WARNING_FREE_INODES = 4096

DEFAULT_MAX_MEASUREMENT_AGE_SECONDS = 30.0
DEFAULT_FUTURE_MEASUREMENT_TOLERANCE_SECONDS = 5.0


class PressureLevel(IntEnum):
    """Ordered resource pressure state; larger values are more severe."""

    NORMAL = 0
    WARNING = 1
    PRESSURE = 2
    CRITICAL = 3


class HostResourceRefused(RuntimeError):
    """Raised before new large work when the emergency reserve would be unsafe."""

    def __init__(self, code: str, assessment: ResourceAssessment) -> None:
        super().__init__(code)
        self.code = code
        self.assessment = assessment


@dataclass(frozen=True)
class PressureThresholds:
    """One shared pressure policy for a measured backing filesystem."""

    critical_free_bytes: int = DEFAULT_CRITICAL_FREE_BYTES
    pressure_free_bytes: int = DEFAULT_PRESSURE_FREE_BYTES
    warning_free_bytes: int = DEFAULT_WARNING_FREE_BYTES
    critical_free_inodes: int = DEFAULT_CRITICAL_FREE_INODES
    pressure_free_inodes: int = DEFAULT_PRESSURE_FREE_INODES
    warning_free_inodes: int = DEFAULT_WARNING_FREE_INODES
    max_measurement_age_seconds: float = DEFAULT_MAX_MEASUREMENT_AGE_SECONDS
    future_measurement_tolerance_seconds: float = (
        DEFAULT_FUTURE_MEASUREMENT_TOLERANCE_SECONDS
    )

    def __post_init__(self) -> None:
        byte_values = (
            self.critical_free_bytes,
            self.pressure_free_bytes,
            self.warning_free_bytes,
        )
        inode_values = (
            self.critical_free_inodes,
            self.pressure_free_inodes,
            self.warning_free_inodes,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in byte_values
        ):
            raise ValueError("resource byte thresholds must be non-negative integers")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in inode_values
        ):
            raise ValueError("resource inode thresholds must be non-negative integers")
        if not byte_values[0] <= byte_values[1] <= byte_values[2]:
            raise ValueError("resource byte thresholds must be ordered")
        if not inode_values[0] <= inode_values[1] <= inode_values[2]:
            raise ValueError("resource inode thresholds must be ordered")
        if (
            isinstance(self.max_measurement_age_seconds, bool)
            or not isinstance(self.max_measurement_age_seconds, int | float)
            or not math.isfinite(self.max_measurement_age_seconds)
            or self.max_measurement_age_seconds <= 0
        ):
            raise ValueError("resource measurement age must be finite and positive")
        if (
            isinstance(self.future_measurement_tolerance_seconds, bool)
            or not isinstance(self.future_measurement_tolerance_seconds, int | float)
            or not math.isfinite(self.future_measurement_tolerance_seconds)
            or self.future_measurement_tolerance_seconds < 0
        ):
            raise ValueError("resource future tolerance must be finite and non-negative")


@dataclass(frozen=True)
class FilesystemMeasurement:
    """Local, non-authoritative observation of one backing filesystem."""

    resource_id: str
    observed_at: datetime
    total_bytes: int | None
    free_bytes: int | None
    total_inodes: int | None
    free_inodes: int | None
    available: bool
    error_code: str | None = None


@dataclass(frozen=True)
class ResourceAssessment:
    """Pressure state after accounting for already-reserved active work."""

    resource_id: str
    level: PressureLevel
    reasons: tuple[str, ...]
    effective_free_bytes: int | None
    effective_free_inodes: int | None
    reserved_bytes: int
    reserved_inodes: int
    observed_at: datetime


@dataclass(frozen=True)
class ResourceReservation:
    """One active reservation owned by a process-local admission controller."""

    resource_id: str
    reserved_bytes: int
    reserved_inodes: int


def _nearest_existing(path: Path) -> Path:
    candidate = path.expanduser().resolve(strict=False)
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise OSError(f"no existing ancestor for resource path: {path}")
        candidate = parent
    return candidate


def _resource_identity(path: Path) -> str:
    """Return a local identity shared by paths on the same mounted resource."""

    stat = path.stat()
    device_id = int(stat.st_dev)
    if os.name == "nt":
        if device_id:
            return f"volume:{device_id}"
        anchor = path.anchor.casefold()
        if not anchor:
            raise OSError("Windows resource path has no device id or volume/share anchor")
        return f"volume-anchor:{anchor}"
    return f"device:{device_id}"


def _inode_measurement(path: Path) -> tuple[int | None, int | None]:
    statvfs = getattr(os, "statvfs", None)
    if statvfs is None:
        return None, None
    try:
        value = statvfs(path)
    except OSError:
        return None, None
    total = int(getattr(value, "f_files", 0) or 0)
    available = getattr(value, "f_favail", None)
    if available is None:
        available = getattr(value, "f_ffree", None)
    if total <= 0 or available is None:
        return None, None
    free = int(available)
    if free < 0:
        return None, None
    return total, free


def _unavailable_measurement(
    *,
    observed_at: datetime,
    error_code: str = "measurement_unavailable",
) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id="unavailable",
        observed_at=observed_at.astimezone(timezone.utc),
        total_bytes=None,
        free_bytes=None,
        total_inodes=None,
        free_inodes=None,
        available=False,
        error_code=error_code,
    )


def measure_filesystem(
    path: Path | str,
    *,
    observed_at: datetime | None = None,
) -> FilesystemMeasurement:
    """Measure the actual filesystem backing ``path`` as seen by this process.

    Missing leaf directories are measured through their nearest existing parent,
    which lets an admission happen before a large writer creates its destination.
    Any failure to obtain byte capacity is represented as an unavailable
    measurement rather than silently substituting a healthy value.
    """

    now = observed_at or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("resource measurements require timezone-aware timestamps")
    try:
        existing = _nearest_existing(Path(path))
        usage = shutil.disk_usage(existing)
        resource_id = _resource_identity(existing)
        total_inodes, free_inodes = _inode_measurement(existing)
    except (OSError, RuntimeError, ValueError):
        return _unavailable_measurement(observed_at=now)
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=now.astimezone(timezone.utc),
        total_bytes=int(usage.total),
        free_bytes=int(usage.free),
        total_inodes=total_inodes,
        free_inodes=free_inodes,
        available=True,
        error_code=None,
    )


def _level_for_remaining(
    remaining: int,
    *,
    critical: int,
    pressure: int,
    warning: int,
) -> PressureLevel:
    if remaining <= critical:
        return PressureLevel.CRITICAL
    if remaining <= pressure:
        return PressureLevel.PRESSURE
    if remaining <= warning:
        return PressureLevel.WARNING
    return PressureLevel.NORMAL


def _valid_aware_timestamp(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _valid_capacity(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def assess_measurement(
    measurement: FilesystemMeasurement,
    *,
    thresholds: PressureThresholds | None = None,
    reserved_bytes: int = 0,
    reserved_inodes: int = 0,
    now: datetime | None = None,
) -> ResourceAssessment:
    """Classify a measurement after subtracting active FCP reservations."""

    policy = thresholds or PressureThresholds()
    if not _valid_capacity(reserved_bytes) or not _valid_capacity(reserved_inodes):
        raise ValueError("resource reservations must be non-negative integers")
    current = now or datetime.now(timezone.utc)
    if not _valid_aware_timestamp(current):
        raise ValueError("resource assessment requires a timezone-aware timestamp")
    current = current.astimezone(timezone.utc)

    reasons: list[str] = []
    effective_bytes: int | None = None
    effective_inodes: int | None = None

    if not _valid_aware_timestamp(measurement.observed_at):
        reasons.append("measurement_time_invalid")
        level = PressureLevel.CRITICAL
    elif not measurement.available or measurement.free_bytes is None:
        reasons.append(measurement.error_code or "measurement_unavailable")
        level = PressureLevel.CRITICAL
    elif not _valid_capacity(measurement.free_bytes) or (
        measurement.free_inodes is not None
        and not _valid_capacity(measurement.free_inodes)
    ):
        reasons.append("measurement_invalid")
        level = PressureLevel.CRITICAL
    else:
        observed = measurement.observed_at.astimezone(timezone.utc)
        age = (current - observed).total_seconds()
        if age < -policy.future_measurement_tolerance_seconds:
            reasons.append("measurement_time_invalid")
            level = PressureLevel.CRITICAL
        elif age > policy.max_measurement_age_seconds:
            reasons.append("measurement_stale")
            level = PressureLevel.CRITICAL
        else:
            effective_bytes = max(measurement.free_bytes - reserved_bytes, 0)
            byte_level = _level_for_remaining(
                effective_bytes,
                critical=policy.critical_free_bytes,
                pressure=policy.pressure_free_bytes,
                warning=policy.warning_free_bytes,
            )
            level = byte_level
            if byte_level != PressureLevel.NORMAL:
                reasons.append(f"bytes_{byte_level.name.lower()}")

            if measurement.free_inodes is not None:
                effective_inodes = max(measurement.free_inodes - reserved_inodes, 0)
                inode_level = _level_for_remaining(
                    effective_inodes,
                    critical=policy.critical_free_inodes,
                    pressure=policy.pressure_free_inodes,
                    warning=policy.warning_free_inodes,
                )
                level = max(level, inode_level)
                if inode_level != PressureLevel.NORMAL:
                    reasons.append(f"inodes_{inode_level.name.lower()}")

    return ResourceAssessment(
        resource_id=measurement.resource_id,
        level=level,
        reasons=tuple(dict.fromkeys(reasons)),
        effective_free_bytes=effective_bytes,
        effective_free_inodes=effective_inodes,
        reserved_bytes=reserved_bytes,
        reserved_inodes=reserved_inodes,
        observed_at=measurement.observed_at,
    )


class ProcessResourceAdmission:
    """Aggregate active bounded work sharing one process and filesystem.

    The controller deliberately refuses *new* reservations at PRESSURE or
    CRITICAL. Work that already owns a reservation keeps it until its context
    exits, which is the primitive needed by raw-first/checkpoint-last callers:
    pressure stops new transactions without invalidating completion capacity that
    was admitted before the transaction started.
    """

    def __init__(
        self,
        *,
        thresholds: PressureThresholds | None = None,
        measurer: Callable[[Path | str], FilesystemMeasurement] = measure_filesystem,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.thresholds = thresholds or PressureThresholds()
        self.measurer = measurer
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._reserved: dict[str, tuple[int, int]] = {}

    def _active_for(self, resource_id: str) -> tuple[int, int]:
        return self._reserved.get(resource_id, (0, 0))

    def _measure(self, path: Path | str) -> FilesystemMeasurement:
        try:
            return self.measurer(path)
        except (OSError, RuntimeError, ValueError):
            return _unavailable_measurement(observed_at=self.clock())

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
    def reserve(
        self,
        path: Path | str,
        *,
        bytes_required: int,
        inodes_required: int = 0,
    ) -> Iterator[ResourceReservation]:
        """Reserve one bounded transaction or refuse before it starts.

        Admission has two gates:

        * the resource must currently be NORMAL or WARNING after existing active
          reservations; and
        * subtracting this transaction's declared maximum must still leave the
          CRITICAL byte/inode reserve intact.

        The reservation is accounting, not a sparse/preallocated disk file. It
        prevents this process's own concurrent writers from double-spending the
        same measured headroom. Host-wide consumers still need fresh host-side
        measurement/serialization at their own boundary.
        """

        if not _valid_capacity(bytes_required) or not _valid_capacity(inodes_required):
            raise ValueError("resource requirement must be non-negative integers")

        with self._lock:
            measurement = self._measure(path)
            resource_id = measurement.resource_id
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

            self._reserved[resource_id] = (
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
            with self._lock:
                current_bytes, current_inodes = self._active_for(resource_id)
                remaining = (
                    max(current_bytes - bytes_required, 0),
                    max(current_inodes - inodes_required, 0),
                )
                if remaining == (0, 0):
                    self._reserved.pop(resource_id, None)
                else:
                    self._reserved[resource_id] = remaining
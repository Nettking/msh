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
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

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


_EMERGENCY_LEASE_CREATION_TOKEN = object()


class EmergencyAdmissionLeaseError(ValueError):
    """A bounded operator lease cannot authorize this reservation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class EmergencyAdmissionLease:
    """One expiring, one-transaction exception to the PRESSURE gate.

    The lease is intentionally process-local and non-rehydratable. Its durable
    audit record proves that an operator issued it, but a process restart cannot
    resurrect the authority. ``resource_ids`` is an exact set rather than a
    path prefix, so replacing a path or moving a destination cannot broaden the
    lease's backing-resource scope.
    """

    def __init__(
        self,
        *,
        override_id: str,
        actor_id: str,
        operation: str,
        resource_ids: frozenset[str],
        max_bytes: int,
        max_inodes: int,
        issued_at: datetime,
        expires_at: datetime,
        _creation_token: object,
    ) -> None:
        if _creation_token is not _EMERGENCY_LEASE_CREATION_TOKEN:
            raise TypeError("emergency admission leases must be issued by the override authority")
        if issued_at.tzinfo is None or issued_at.utcoffset() is None:
            raise ValueError("emergency lease issuance must be timezone-aware")
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise ValueError("emergency lease expiry must be timezone-aware")
        if expires_at <= issued_at:
            raise ValueError("emergency lease expiry must follow issuance")
        self.override_id = override_id
        self.actor_id = actor_id
        self.operation = operation
        self.resource_ids = resource_ids
        self.max_bytes = max_bytes
        self.max_inodes = max_inodes
        self.issued_at = issued_at
        self.expires_at = expires_at
        self._lock = RLock()
        self._used = False
        self._revoked = False

    @property
    def used(self) -> bool:
        with self._lock:
            return self._used

    @property
    def revoked(self) -> bool:
        with self._lock:
            return self._revoked

    def revoke(self) -> None:
        with self._lock:
            self._revoked = True

    def consume(
        self,
        *,
        operation: str,
        resource_ids: frozenset[str],
        bytes_required: int,
        inodes_required: int,
        now: datetime,
    ) -> None:
        """Consume the lease for one already-measured logical transaction."""

        current = now.astimezone(timezone.utc)
        with self._lock:
            if self._revoked:
                raise EmergencyAdmissionLeaseError("revoked")
            if current < self.issued_at or current >= self.expires_at:
                raise EmergencyAdmissionLeaseError("expired")
            if self._used:
                raise EmergencyAdmissionLeaseError("already_used")
            if operation != self.operation:
                raise EmergencyAdmissionLeaseError("operation_scope")
            if resource_ids != self.resource_ids:
                raise EmergencyAdmissionLeaseError("resource_scope")
            if bytes_required > self.max_bytes:
                raise EmergencyAdmissionLeaseError("byte_cap")
            if inodes_required > self.max_inodes:
                raise EmergencyAdmissionLeaseError("inode_cap")
            # Consume before the caller starts writing. A failed write does not
            # make an emergency lease reusable under pressure, but the normal
            # resource reservation still unwinds in its finally block.
            self._used = True


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
        *,
        operation: str = "unspecified",
        override: EmergencyAdmissionLease | None = None,
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
        pressured_assessment: ResourceAssessment | None = None
        requires_override = False
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
                    if before.level == PressureLevel.CRITICAL:
                        # The emergency floor is never operator-overridable.
                        raise HostResourceRefused("resource_pressure", before)
                    if override is None:
                        raise HostResourceRefused("resource_pressure", before)
                    requires_override = True
                    if pressured_assessment is None:
                        pressured_assessment = before

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

            if requires_override:
                assert override is not None
                assert pressured_assessment is not None
                try:
                    override.consume(
                        operation=operation,
                        resource_ids=frozenset(grouped),
                        bytes_required=sum(item.reserved_bytes for item in pending),
                        inodes_required=sum(item.reserved_inodes for item in pending),
                        now=self.clock(),
                    )
                except EmergencyAdmissionLeaseError as exc:
                    raise HostResourceRefused(
                        f"emergency_override_{exc.code}", pressured_assessment
                    ) from exc

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
        operation: str = "unspecified",
        override: EmergencyAdmissionLease | None = None,
    ) -> Iterator[ResourceReservation]:
        with self.reserve_many(
            ((path, bytes_required, inodes_required),),
            operation=operation,
            override=override,
        ) as reservations:
            if not reservations:
                raise RuntimeError("single-resource admission returned no reservation")
            yield reservations[0]


PROCESS_RESOURCE_ADMISSION = SerializedProcessResourceAdmission()

__all__ = [
    "PROCESS_RESOURCE_ADMISSION",
    "EmergencyAdmissionLease",
    "EmergencyAdmissionLeaseError",
    "SerializedProcessResourceAdmission",
]

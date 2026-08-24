"""Bounded, preallocated disk allocation for one local storage authority root.

A device that contributes the storage capability accepts data written by other
Federation members. Nothing bounded that: candidacy was proven by a 256-byte
write probe, the advertised capacity envelope carried no byte budget at all,
and ``ingest`` discovered exhaustion by reaching ``ENOSPC`` on a real write. A
storage contributor could therefore consume its host's entire disk, and the
first symptom was a full machine rather than a refused commit.

Two independent bounds are enforced here. A commit must satisfy both:

* a **budget** -- the bytes this device offers the Federation, taken from the
  volume *in advance* so nothing else can take them first; and
* a **floor** -- free space on the volume that FCP never consumes, whatever the
  budget says. The floor is what keeps a mis-set budget, a volume shared with
  other software, or a non-FCP writer from turning a storage contribution into
  a full host disk.

The budget is preallocated rather than merely recorded. A reservation file
holding the *unclaimed* remainder is created with real committed blocks when
the budget is set, and shrunk by exactly the size of each batch immediately
before that batch is written. Space this device promised the Federation is
therefore held from the moment the promise is made, not hoped for at commit
time. That is the difference between a quota and an allocation: a quota tells
you afterwards that you are out of room, an allocation means the room was
always yours.

What remains is read off the reservation file rather than from a counter. A
small state file records the budget the current reservation was established
under -- enough to tell a budget that grew from bytes that were consumed -- and
``used`` is derived as ``recorded_budget - reservation_size``. There is no
running total to drift away from the bytes actually held, and a restart reads
the truth off the filesystem instead of replaying a ledger.

Failure direction is deliberate. Shrinking the reservation always precedes the
write it pays for, and restoring it after a failed write is best effort: if the
restore itself fails, this device under-offers its budget until the next
reclaim. Under-offering is safe; over-offering is the bug this module exists to
prevent.

Scope, so this is not mistaken for more than it is: what is bounded here is
data *other Federation members write to this device*. This device's own
recorder capture, its outbox, the session event log and everything Docker holds
are all unbounded and untouched by any of this. A host can still fill its drive
with this module working exactly as designed. See
``docs/implementation/disk_accounting_audit.md``.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from catalog.federation.errors import FederationValidationError

#: Wire/error identity for either bound being reached. One code keeps the
#: protocol change to a single value; the message says which bound it was.
ALLOCATION_EXHAUSTED_CODE = "allocation-exhausted"

_GIB = 1024**3

# -- headroom policy -----------------------------------------------------
#
# The floor is derived, not picked. What it must cover is one complete FCP
# update plus room for the host to keep operating, because the observed
# exhaustion happened on a machine that was both recording and updating.
#
# One update rebuilds the ``relay``, ``flask`` and ``recorder`` images. None of
# them share the expensive layer: ``FCP_BUILD_COMMIT`` is written into ``ENV``
# at ``Dockerfile`` line 8, above the dependency install at lines 15-16, so a
# changed commit invalidates that layer and every layer after it. Each image
# therefore carries a full copy of the Python dependency tree, measured at
# roughly 726 MiB installed (pyarrow, scipy, pandas, duckdb, scikit-learn,
# numpy and matplotlib account for most of it), on top of a ~150 MiB base
# image and the application source.
#
#: Approximate on-disk cost of the image set one update produces, before the
#: previous set is released. Three images at ~1 GiB, plus build cache.
_UPDATE_CYCLE_BYTES = 4 * _GIB

#: Room the host needs to keep operating while that happens: SQLite WAL and
#: journals, container logs between rotations, OS paging and servicing.
_OPERATING_HEADROOM_BYTES = 2 * _GIB

#: Windows with Docker Desktop needs a second update cycle of margin. The
#: WSL2 disk image grows on demand and does not shrink when files inside it
#: are deleted, so space freed in the VM does not return to the host promptly
#: and the docker data root must be able to grow through a whole update
#: without the host reaching exhaustion. Runtime instability near exhaustion
#: is what the physical incident actually showed.
_WINDOWS_DOCKER_MARGIN_BYTES = 4 * _GIB

#: Share of the volume held back when that is larger than the absolute
#: minimum, so a bigger disk keeps proportionally more slack.
FLOOR_VOLUME_FRACTION = 0.05

#: Ceiling on the derived floor. Without it a large volume would reserve an
#: unreasonable amount of space that FCP would never have used anyway.
MAXIMUM_FLOOR_BYTES = 64 * _GIB


def minimum_headroom_bytes(*, windows: bool | None = None) -> int:
    """The absolute floor for this platform, below which FCP never operates."""

    if windows is None:
        windows = os.name == "nt"
    total = _UPDATE_CYCLE_BYTES + _OPERATING_HEADROOM_BYTES
    if windows:
        total += _WINDOWS_DOCKER_MARGIN_BYTES
    return total


def default_floor_bytes(
    root: Path | str,
    *,
    windows: bool | None = None,
) -> int:
    """Derive the free space FCP never consumes on ``root``'s volume.

    The larger of the platform minimum and a share of the volume, capped so a
    large disk does not reserve an absurd amount. An explicitly configured
    floor always wins over this: an operator who has measured their own host
    knows more than a policy can.
    """

    minimum = minimum_headroom_bytes(windows=windows)
    try:
        total = shutil.disk_usage(_nearest_existing(Path(root))).total
    except OSError:
        return minimum
    proportional = int(total * FLOOR_VOLUME_FRACTION)
    return min(max(minimum, proportional), MAXIMUM_FLOOR_BYTES)

#: Reservation resize chunk for platforms without real ``fallocate``.
_ZERO_CHUNK = 1024 * 1024

_RESERVATION_NAME = "allocation.reserve"
_STATE_NAME = "allocation.json"


@dataclass(frozen=True)
class AllocationSnapshot:
    """Bounded, non-sensitive allocation state safe to advertise."""

    budget_bytes: int | None
    used_bytes: int
    remaining_bytes: int | None
    floor_bytes: int
    volume_free_bytes: int

    def as_capacity_envelope(self) -> dict[str, object]:
        """Project into the capacity envelope published to the Federation.

        Only byte counts and a mode are exposed. No path, volume name, host
        layout or filesystem type travels with a capacity advertisement.
        """

        return {
            "allocation_mode": "preallocated" if self.budget_bytes is not None else "floor-only",
            "budget_bytes": self.budget_bytes,
            "used_bytes": self.used_bytes,
            "remaining_bytes": self.remaining_bytes,
            "floor_bytes": self.floor_bytes,
            "volume_free_bytes": self.volume_free_bytes,
        }


def _exhausted(field: str, message: str) -> FederationValidationError:
    return FederationValidationError(ALLOCATION_EXHAUSTED_CODE, field, message)


def _positive_or_zero(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-allocation",
            field,
            f"{field} must be a non-negative integer",
        )
    return value


def _allocate(path: Path, size: int) -> None:
    """Resize ``path`` to exactly ``size`` bytes with real blocks committed.

    A sparse file would defeat the entire purpose: it would report the right
    size while holding none of the space, so the volume could still be taken by
    something else between the promise and the commit. Each platform therefore
    uses the call that actually commits blocks.
    """

    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        current = os.fstat(descriptor).st_size
        if size == current:
            return
        if size < current:
            # Shrinking is always honoured and never needs new blocks.
            os.ftruncate(descriptor, size)
        elif hasattr(os, "posix_fallocate"):
            # Linux: commits blocks, and fails with ENOSPC *now* rather than
            # leaving a hole that fails during a later commit.
            os.posix_fallocate(descriptor, 0, size)
        elif os.name == "nt":
            # NTFS commits clusters when the end of file is set; the file is
            # not sparse unless explicitly marked so.
            os.ftruncate(descriptor, size)
        else:
            # macOS and anything else without fallocate: write real bytes.
            os.lseek(descriptor, current, os.SEEK_SET)
            zeros = b"\0" * _ZERO_CHUNK
            remaining = size - current
            while remaining > 0:
                written = os.write(descriptor, zeros[: min(remaining, _ZERO_CHUNK)])
                if written <= 0:  # pragma: no cover - platform write failure
                    raise OSError("reservation write made no progress")
                remaining -= written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class StorageAllocation:
    """A preallocated byte budget and a never-consumed floor for one root.

    Construct with the budget this device offers. The reservation is brought
    into line on construction, so first enable and a restart take the same
    path and neither trusts a counter that a crash could have left stale.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        budget_bytes: int | None = None,
        floor_bytes: int | None = None,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # ``None`` derives the floor from this volume and platform. An explicit
        # value -- including zero -- is taken as given: an operator who has
        # measured their own host outranks the policy.
        if floor_bytes is None:
            floor_bytes = default_floor_bytes(self.root)
        self.floor_bytes = _positive_or_zero(floor_bytes, "floor_bytes")
        self.reservation_path = self.root / _RESERVATION_NAME
        self.state_path = self.root / _STATE_NAME
        if budget_bytes is None:
            self.budget_bytes: int | None = None
            # An unbudgeted device holds nothing: release the reservation so
            # the space returns to the host rather than being stranded, and
            # forget the budget it was established under.
            self.reservation_path.unlink(missing_ok=True)
            self.state_path.unlink(missing_ok=True)
            return
        self.budget_bytes = _positive_or_zero(budget_bytes, "budget_bytes")
        self._establish()

    # -- reservation -----------------------------------------------------

    def _reservation_size(self) -> int:
        try:
            return self.reservation_path.stat().st_size
        except FileNotFoundError:
            return 0

    def _volume_free(self) -> int:
        return shutil.disk_usage(self.root).free

    def _recorded_budget(self) -> int | None:
        """The budget the reservation on disk was established under.

        A malformed or unreadable state file is treated as absent rather than
        repaired: the reservation is then re-established from the new budget,
        which can only reserve more, never silently offer space this device is
        not holding.
        """

        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        recorded = value.get("budget_bytes") if isinstance(value, dict) else None
        if isinstance(recorded, bool) or not isinstance(recorded, int) or recorded < 0:
            return None
        return recorded

    def _record_budget(self, budget: int) -> None:
        temporary = self.state_path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps({"budget_bytes": budget}, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, self.state_path)

    def _establish(self) -> None:
        """Bring the reservation into line with the bytes already consumed.

        ``used`` is derived against the budget the current reservation was
        recorded under, not against the new one, so raising or lowering a
        budget moves the reservation by the difference and leaves stored bytes
        accounted for. With no recorded budget -- first enable, or a state file
        that did not survive -- nothing is assumed to be consumed.
        """

        assert self.budget_bytes is not None
        held = self._reservation_size()
        recorded = self._recorded_budget()
        used = 0 if recorded is None else max(recorded - held, 0)
        target = max(self.budget_bytes - used, 0)
        growth = target - held
        if growth > 0:
            free = self._volume_free()
            if free - growth < self.floor_bytes:
                raise _exhausted(
                    "budget_bytes",
                    "the requested storage budget cannot be reserved without "
                    "taking the volume below its reserved free-space floor",
                )
        _allocate(self.reservation_path, target)
        self._record_budget(self.budget_bytes)

    # -- public surface --------------------------------------------------

    def snapshot(self) -> AllocationSnapshot:
        free = self._volume_free()
        if self.budget_bytes is None:
            return AllocationSnapshot(
                budget_bytes=None,
                used_bytes=0,
                remaining_bytes=max(free - self.floor_bytes, 0),
                floor_bytes=self.floor_bytes,
                volume_free_bytes=free,
            )
        remaining = self._reservation_size()
        return AllocationSnapshot(
            budget_bytes=self.budget_bytes,
            used_bytes=max(self.budget_bytes - remaining, 0),
            remaining_bytes=remaining,
            floor_bytes=self.floor_bytes,
            volume_free_bytes=free,
        )

    def claim(self, nbytes: int) -> None:
        """Take ``nbytes`` from the allocation, or refuse before any write.

        For a budgeted device the bytes come out of the reservation, so the
        volume's free space is unchanged by the pair of operations and the
        floor is only a backstop against filesystem overhead and outside
        pressure. An unbudgeted device writes straight out of the volume and
        must therefore keep the floor intact by itself.
        """

        nbytes = _positive_or_zero(nbytes, "nbytes")
        if nbytes == 0:
            return
        free = self._volume_free()
        if self.budget_bytes is None:
            if free - nbytes < self.floor_bytes:
                raise _exhausted(
                    "content",
                    "committing this batch would take the volume below its "
                    "reserved free-space floor",
                )
            return
        remaining = self._reservation_size()
        if nbytes > remaining:
            raise _exhausted(
                "content",
                "this batch does not fit in the storage budget allocated to "
                "this device",
            )
        if free < self.floor_bytes:
            raise _exhausted(
                "content",
                "the volume is already below its reserved free-space floor",
            )
        _allocate(self.reservation_path, remaining - nbytes)

    def release(self, nbytes: int) -> None:
        """Return ``nbytes`` to the reservation after a failed or undone write.

        Best effort by design. If the reservation cannot be regrown -- the
        volume was taken from outside while the write was in flight -- this
        device offers less than its budget until the next reclaim. That is the
        safe direction, so the failure is swallowed rather than converted into
        a second error on top of the one already being handled.
        """

        nbytes = _positive_or_zero(nbytes, "nbytes")
        if nbytes == 0 or self.budget_bytes is None:
            return
        target = min(self._reservation_size() + nbytes, self.budget_bytes)
        try:
            _allocate(self.reservation_path, target)
        except OSError:
            return

    @contextmanager
    def claimed(self, nbytes: int) -> Iterator[None]:
        """Hold ``nbytes`` for the duration of a write, releasing on failure."""

        self.claim(nbytes)
        try:
            yield
        except BaseException:
            self.release(nbytes)
            raise


def describe(
    root: Path | str,
    *,
    floor_bytes: int | None = None,
) -> AllocationSnapshot:
    """Read allocation state for ``root`` without reserving anything.

    Inspection and capability advertisement must never have the side effect of
    claiming disk, so this reads the reservation and the budget it was recorded
    under rather than establishing them. A root that has never been assigned an
    authority reports no budget and the volume's real free space, which is
    exactly what such a device can honestly offer.
    """

    root = Path(root)
    if floor_bytes is None:
        floor_bytes = default_floor_bytes(root)
    try:
        free = shutil.disk_usage(root).free
    except OSError:
        # A root that does not exist yet is measured at its nearest existing
        # ancestor: the volume is the same one the directory will be created on.
        free = shutil.disk_usage(_nearest_existing(root)).free
    reservation = root / _RESERVATION_NAME
    state = root / _STATE_NAME
    try:
        recorded = json.loads(state.read_text(encoding="utf-8"))
        budget = recorded.get("budget_bytes") if isinstance(recorded, dict) else None
    except (OSError, ValueError):
        budget = None
    if isinstance(budget, bool) or not isinstance(budget, int) or budget < 0:
        return AllocationSnapshot(
            budget_bytes=None,
            used_bytes=0,
            remaining_bytes=max(free - floor_bytes, 0),
            floor_bytes=floor_bytes,
            volume_free_bytes=free,
        )
    try:
        remaining = reservation.stat().st_size
    except OSError:
        remaining = 0
    return AllocationSnapshot(
        budget_bytes=budget,
        used_bytes=max(budget - remaining, 0),
        remaining_bytes=remaining,
        floor_bytes=floor_bytes,
        volume_free_bytes=free,
    )


def _nearest_existing(path: Path) -> Path:
    candidate = path.absolute()
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            return Path(os.path.abspath(os.sep))
        candidate = parent
    return candidate


__all__ = [
    "ALLOCATION_EXHAUSTED_CODE",
    "FLOOR_VOLUME_FRACTION",
    "MAXIMUM_FLOOR_BYTES",
    "AllocationSnapshot",
    "StorageAllocation",
    "default_floor_bytes",
    "describe",
    "minimum_headroom_bytes",
]

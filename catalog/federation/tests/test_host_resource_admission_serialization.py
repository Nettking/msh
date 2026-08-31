from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Self

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)

NOW = datetime(2026, 8, 30, 12, 0, tzinfo=timezone.utc)


class _GateLock:
    """Gate one candidate acquisition without blocking unrelated cleanup."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.enabled = False
        self.reached = threading.Event()
        self.release = threading.Event()
        self._gated = False

    def __enter__(self) -> Self:
        if (
            self.enabled
            and not self._gated
            and threading.current_thread().name == "stale-candidate"
        ):
            self._gated = True
            self.reached.set()
            assert self.release.wait(timeout=5), "candidate lock gate timed out"
        self._lock.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._lock.release()


def _measurement(free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id="device:1",
        observed_at=NOW,
        total_bytes=2_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def test_reserve_remeasures_after_prior_writer_consumes_and_releases() -> None:
    """A candidate must not spend a snapshot taken before the admission lock."""

    state = {"free_bytes": 350}
    measurements: list[int] = []

    def measurer(_path: object) -> FilesystemMeasurement:
        value = state["free_bytes"]
        measurements.append(value)
        return _measurement(value)

    controller = ProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
        ),
        measurer=measurer,
        clock=lambda: NOW,
    )
    gate = _GateLock()
    controller._lock = gate  # type: ignore[assignment]

    first = controller.reserve("/data/a", bytes_required=150)
    first.__enter__()

    outcome: list[str] = []

    def candidate() -> None:
        try:
            with controller.reserve("/data/b", bytes_required=150):
                outcome.append("admitted")
        except HostResourceRefused as exc:
            outcome.append(exc.code)

    gate.enabled = True
    thread = threading.Thread(target=candidate, name="stale-candidate")
    thread.start()
    assert gate.reached.wait(timeout=5), "candidate did not reach admission lock"

    # The first transaction has now durably consumed its reservation. Release its
    # accounting before the candidate proceeds. A stale pre-lock measurement of
    # 350 would admit another 150 and leave only 50 bytes, below the critical 100.
    state["free_bytes"] = 200
    first.__exit__(None, None, None)
    gate.release.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert outcome == ["resource_pressure"]
    assert measurements == [350, 200]

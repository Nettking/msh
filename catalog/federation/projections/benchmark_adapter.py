"""Adapter for CF2 benchmark evidence."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from .adapter_common import (
    _enum_value,
    _public_text,
    _safe_mapping,
    _sequence,
    _timestamp,
    _utc_now,
    _value,
)
from .safety import safe_reason_code


@dataclass(frozen=True)
class BenchmarkRecord:
    run_id: str
    device_id: str
    benchmark_id: str
    target_service_id: str
    state: str
    recommendation: str
    current: bool
    validity_reason: str
    finished_at: datetime | None
    expires_at: datetime | None
    metrics: dict[str, Any]
    diagnostics: tuple[str, ...]


@dataclass(frozen=True)
class BenchmarkSnapshot:
    available: bool
    reason_code: str
    results: tuple[BenchmarkRecord, ...] = ()


class BenchmarkStoreLike(Protocol):
    def list_results(self) -> Iterable[object]: ...


class BenchmarkResultsAdapter:
    """Read the latest CF2 evidence without turning success into authority.

    The durable benchmark store retains every run as audit history. Product
    projections expose only the newest run for each device, benchmark and
    target-service combination so expired historical evidence cannot become a
    false rerun requirement.

    Installed product services may expose ``accepted_results(snapshot)`` plus
    their inspection service. When present, that read-only product validity
    policy is reused so Federation projections agree with onboarding about
    whether persisted evidence is still structurally current. Plain stores keep
    the strict timestamp fallback used by framework-neutral projection tests.
    """

    def __init__(
        self,
        store: BenchmarkStoreLike | None,
        *,
        now: Callable[[], datetime] = _utc_now,
        validity: Callable[[object], object] | None = None,
    ) -> None:
        self._store = store
        self._now = now
        self._validity = validity

    def _resolved_validity(self) -> Callable[[object], object] | None:
        if self._validity is not None:
            return self._validity
        if self._store is None:
            return None

        accepted_supplier = getattr(self._store, "accepted_results", None)
        inspection_service = getattr(self._store, "inspection_service", None)
        snapshot_loader = getattr(inspection_service, "load", None)
        if not callable(accepted_supplier) or not callable(snapshot_loader):
            return None

        snapshot = snapshot_loader()
        if snapshot is None:
            return lambda _result: (False, "inspection-required")

        accepted_run_ids: set[str] = set()
        for accepted in accepted_supplier(snapshot):
            run_id = _value(accepted, "run_id")
            if isinstance(run_id, str) and run_id:
                accepted_run_ids.add(run_id)

        def product_validity(result: object) -> tuple[bool, str]:
            run_id = _value(result, "run_id")
            current = isinstance(run_id, str) and run_id in accepted_run_ids
            return current, "current" if current else "invalidated"

        return product_validity

    def snapshot(self) -> BenchmarkSnapshot:
        if self._store is None:
            return BenchmarkSnapshot(False, "benchmark-store-unavailable")
        try:
            now = self._now().astimezone(timezone.utc)
            validity = self._resolved_validity()
            results: list[BenchmarkRecord] = []
            for result in self._store.list_results():
                expires_at = _timestamp(_value(result, "expires_at"))
                if validity is None:
                    current = expires_at is not None and expires_at > now
                    validity_reason = "current" if current else "expired"
                else:
                    evaluated = validity(result)
                    if isinstance(evaluated, tuple) and len(evaluated) == 2:
                        current = bool(evaluated[0])
                        raw_reason = evaluated[1]
                    else:
                        current = bool(_value(evaluated, "current", False))
                        raw_reason = _value(evaluated, "reason", "invalidated")
                    validity_reason = safe_reason_code(
                        _enum_value(raw_reason, "invalidated"),
                        "invalidated",
                    )
                results.append(
                    BenchmarkRecord(
                        run_id=_public_text(_value(result, "run_id"), "unknown-run"),
                        device_id=_public_text(
                            _value(result, "device_id"),
                            "unknown-device",
                        ),
                        benchmark_id=_public_text(
                            _value(result, "benchmark_id"),
                            "unknown-benchmark",
                        ),
                        target_service_id=_public_text(
                            _value(result, "target_service_id"),
                            "unknown-service",
                        ),
                        state=_enum_value(_value(result, "state"), "unknown"),
                        recommendation=_enum_value(
                            _value(result, "recommendation"),
                            "unsupported",
                        ),
                        current=bool(current),
                        validity_reason=validity_reason,
                        finished_at=_timestamp(_value(result, "finished_at")),
                        expires_at=expires_at,
                        metrics=_safe_mapping(_value(result, "metrics", {})),
                        diagnostics=tuple(
                            _public_text(item, "Benchmark detail unavailable")
                            for item in _sequence(
                                _value(result, "diagnostics", ())
                            )
                        ),
                    )
                )
            results.sort(
                key=lambda item: (
                    item.finished_at or datetime.min.replace(tzinfo=timezone.utc),
                    item.run_id,
                ),
                reverse=True,
            )
            latest: list[BenchmarkRecord] = []
            seen: set[tuple[str, str, str]] = set()
            for result in results:
                identity = (
                    result.device_id,
                    result.benchmark_id,
                    result.target_service_id,
                )
                if identity in seen:
                    continue
                seen.add(identity)
                latest.append(result)
            return BenchmarkSnapshot(True, "current", tuple(latest))
        except Exception as exc:  # noqa: BLE001
            return BenchmarkSnapshot(
                False,
                safe_reason_code(getattr(exc, "code", None), "benchmark-projection-failed"),
            )

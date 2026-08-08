"""CFI-4 composition of bounded CF2 benchmarks into Flask onboarding."""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import current_app

from catalog.capabilities.benchmarking import (
    BenchmarkRegistry,
    BenchmarkRunner,
    BenchmarkRunRequest,
    BenchmarkValidityReason,
    CancellationToken,
    SQLiteBenchmarkResultStore,
    evaluate_result,
)
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkResult,
    BenchmarkState,
    DeviceInspectionSnapshot,
)

from .capability_inspection_service import (
    CapabilityInspectionService,
    get_capability_inspection_service,
)
from .capability_onboarding_service import (
    CapabilityOnboardingService,
    get_capability_onboarding_service,
)

_SERVICE_EXTENSION_KEY = "capability_benchmark_service"
_MAX_IDENTIFIER_BYTES = 512
_SKIPPABLE_REVIEW_STATES = frozenset({"pending", "expired", "stale"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _bounded_identifier(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise FederationValidationError(
            "invalid-benchmark-identifier",
            field,
            "must be a printable string",
        )
    normalized = value.strip()
    if (
        not normalized
        or len(normalized.encode("utf-8")) > _MAX_IDENTIFIER_BYTES
        or any(ord(char) < 32 for char in normalized)
    ):
        raise FederationValidationError(
            "invalid-benchmark-identifier",
            field,
            "must be a bounded printable string",
        )
    return normalized


@dataclass(frozen=True)
class BenchmarkPlanItem:
    benchmark_id: str
    target_service_id: str
    definition: BenchmarkDefinition
    dependency_inputs: Mapping[str, Any]
    available_prerequisites: frozenset[str]
    runnable: bool
    unavailable_reason: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return self.benchmark_id, self.target_service_id


class BenchmarkSkipStore:
    """Persist local skip decisions bound to one inspection revision.

    Skip decisions are onboarding progress only. They are not benchmark results,
    contribution intent, federation policy, provider authority, or storage/job
    authority.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = Path(database)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.database), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _create_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_skip_decision (
                device_id TEXT NOT NULL,
                inspection_revision INTEGER NOT NULL CHECK(inspection_revision > 0),
                benchmark_id TEXT NOT NULL,
                target_service_id TEXT NOT NULL,
                skipped_at TEXT NOT NULL,
                PRIMARY KEY(
                    device_id,
                    inspection_revision,
                    benchmark_id,
                    target_service_id
                )
            )
            """
        )

    def initialize(self) -> None:
        self.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                self._create_table(connection)
                connection.commit()
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "benchmark-skip-state-initialize-failed",
                "database",
                "benchmark skip state could not be initialized safely",
            ) from exc

    def list_for_revision(
        self,
        *,
        device_id: str,
        inspection_revision: int,
    ) -> frozenset[tuple[str, str]]:
        if not self.database.exists():
            return frozenset()
        try:
            with self._connect() as connection:
                table = connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='benchmark_skip_decision'
                    """
                ).fetchone()
                if table is None:
                    return frozenset()
                rows = connection.execute(
                    """
                    SELECT benchmark_id,target_service_id
                    FROM benchmark_skip_decision
                    WHERE device_id=? AND inspection_revision=?
                    ORDER BY benchmark_id,target_service_id
                    """,
                    (device_id, inspection_revision),
                ).fetchall()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "malformed-benchmark-skip-state",
                "database",
                "benchmark skip state could not be read safely",
            ) from exc
        values: set[tuple[str, str]] = set()
        for row in rows:
            values.add(
                (
                    _bounded_identifier(row["benchmark_id"], "benchmark_id"),
                    _bounded_identifier(
                        row["target_service_id"],
                        "target_service_id",
                    ),
                )
            )
        return frozenset(values)

    def skip_many(
        self,
        *,
        device_id: str,
        inspection_revision: int,
        items: Sequence[BenchmarkPlanItem],
        skipped_at: datetime,
    ) -> int:
        if not items:
            return 0
        self.initialize()
        stamp = skipped_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                self._create_table(connection)
                for item in items:
                    connection.execute(
                        """
                        INSERT INTO benchmark_skip_decision(
                            device_id,inspection_revision,benchmark_id,
                            target_service_id,skipped_at
                        ) VALUES(?,?,?,?,?)
                        ON CONFLICT(
                            device_id,inspection_revision,benchmark_id,
                            target_service_id
                        ) DO UPDATE SET skipped_at=excluded.skipped_at
                        """,
                        (
                            device_id,
                            inspection_revision,
                            item.benchmark_id,
                            item.target_service_id,
                            stamp,
                        ),
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "benchmark-skip-state-write-failed",
                "database",
                "benchmark skip state could not be persisted safely",
            ) from exc
        try:
            self.database.chmod(0o600)
        except OSError as exc:
            raise FederationValidationError(
                "benchmark-skip-state-permission-failed",
                "database",
                "benchmark state permissions could not be restricted",
            ) from exc
        return len(items)

    def clear(
        self,
        *,
        device_id: str,
        inspection_revision: int,
        benchmark_id: str,
        target_service_id: str,
    ) -> None:
        if not self.database.exists():
            return
        try:
            with self._connect() as connection:
                table = connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='benchmark_skip_decision'
                    """
                ).fetchone()
                if table is None:
                    return
                connection.execute(
                    """
                    DELETE FROM benchmark_skip_decision
                    WHERE device_id=? AND inspection_revision=?
                      AND benchmark_id=? AND target_service_id=?
                    """,
                    (
                        device_id,
                        inspection_revision,
                        benchmark_id,
                        target_service_id,
                    ),
                )
                connection.commit()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "benchmark-skip-state-write-failed",
                "database",
                "benchmark state could not be updated safely",
            ) from exc


class CapabilityBenchmarkService:
    """Run existing local benchmarks as evidence without granting authority."""

    def __init__(
        self,
        *,
        onboarding_service: CapabilityOnboardingService,
        inspection_service: CapabilityInspectionService,
        state_database: Path | str,
        clock=_utc_now,
    ) -> None:
        self.onboarding_service = onboarding_service
        self.inspection_service = inspection_service
        self.state_database = Path(state_database)
        self.skip_store = BenchmarkSkipStore(self.state_database)
        self._clock = clock
        self._lock = threading.RLock()
        self._result_store_instance: SQLiteBenchmarkResultStore | None = None
        self._active: dict[tuple[str, str, str], CancellationToken] = {}

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise FederationValidationError(
                "invalid-benchmark-clock",
                "clock",
                "benchmark clock must be timezone-aware",
            )
        return value.astimezone(timezone.utc)

    def _result_store(self) -> SQLiteBenchmarkResultStore:
        with self._lock:
            if self._result_store_instance is None:
                self.state_database.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                    mode=0o700,
                )
                self._result_store_instance = SQLiteBenchmarkResultStore(
                    self.state_database
                )
                try:
                    self.state_database.chmod(0o600)
                except OSError as exc:
                    raise FederationValidationError(
                        "benchmark-state-permission-failed",
                        "database",
                        "benchmark state permissions could not be restricted",
                    ) from exc
            return self._result_store_instance

    def _components(self) -> tuple[BenchmarkRegistry, tuple[object, ...]]:
        """Reuse the exact CFI-3 composition without rediscovering adapters."""

        inspector_factory = getattr(
            self.inspection_service,
            "_device_inspector",
            None,
        )
        if not callable(inspector_factory):
            raise FederationValidationError(
                "benchmark-composition-unavailable",
                "inspection",
                "the inspection composition is unavailable",
            )
        inspector = inspector_factory()
        registry = getattr(inspector, "_registry", None)
        adapters = getattr(inspector, "_probes", None)
        if not isinstance(registry, BenchmarkRegistry) or not isinstance(
            adapters,
            tuple,
        ):
            raise FederationValidationError(
                "benchmark-composition-unavailable",
                "inspection",
                "the inspection composition could not be reused safely",
            )
        return registry, adapters

    def _authorized_snapshot(
        self,
        *,
        require_current: bool,
    ) -> tuple[str, DeviceInspectionSnapshot]:
        context = self.onboarding_service.authorized_context()
        if context is None:
            raise FederationOperationError(
                "benchmark-federation-required",
                "a trusted federation connection is required before benchmarking",
                "binding",
            )
        snapshot = self.inspection_service.load()
        if snapshot is None:
            raise FederationOperationError(
                "benchmark-inspection-required",
                "run device inspection before benchmarking",
                "inspection",
            )
        if snapshot.device_id != context.credentials.identity.node_id:
            raise FederationValidationError(
                "benchmark-device-mismatch",
                "device_id",
                "inspection evidence belongs to another device identity",
            )
        if require_current and self.inspection_service.state(snapshot) != "current":
            raise FederationOperationError(
                "benchmark-inspection-expired",
                "rerun device inspection before benchmarking",
                "inspection",
            )
        return context.credentials.identity.node_id, snapshot

    @staticmethod
    def _candidate_ids(snapshot: DeviceInspectionSnapshot) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    *snapshot.detected_services,
                    *snapshot.registered_handlers,
                    *snapshot.detected_data_sources,
                )
            )
        )

    def plan(
        self,
        snapshot: DeviceInspectionSnapshot,
    ) -> tuple[BenchmarkPlanItem, ...]:
        registry, adapters = self._components()
        adapters_by_id: dict[str, object] = {}
        for adapter in adapters:
            definition = getattr(adapter, "definition", None)
            if not isinstance(definition, BenchmarkDefinition):
                continue
            if definition.benchmark_id in adapters_by_id:
                raise FederationValidationError(
                    "duplicate-benchmark-adapter",
                    "benchmark_id",
                    "more than one adapter owns a benchmark definition",
                )
            adapters_by_id[definition.benchmark_id] = adapter

        items: list[BenchmarkPlanItem] = []
        candidates = self._candidate_ids(snapshot)
        for benchmark_id in snapshot.recommended_benchmark_ids:
            definition = registry.get(benchmark_id).definition
            adapter = adapters_by_id.get(benchmark_id)
            dependency_supplier = getattr(adapter, "dependency_inputs", None)
            matched = False
            if callable(dependency_supplier):
                for target_service_id in candidates:
                    try:
                        dependency_inputs = dependency_supplier(target_service_id)
                    except (KeyError, TypeError, ValueError):
                        continue
                    except Exception as exc:
                        raise FederationValidationError(
                            "benchmark-dependency-inputs-unavailable",
                            "benchmark_id",
                            "benchmark dependency inputs could not be derived safely",
                        ) from exc
                    if not isinstance(dependency_inputs, Mapping):
                        continue
                    missing = set(definition.invalidation_inputs) - set(
                        dependency_inputs
                    )
                    if missing:
                        continue
                    matched = True
                    items.append(
                        BenchmarkPlanItem(
                            benchmark_id=benchmark_id,
                            target_service_id=target_service_id,
                            definition=definition,
                            dependency_inputs=dict(dependency_inputs),
                            available_prerequisites=frozenset(
                                definition.prerequisites
                            ),
                            runnable=True,
                        )
                    )
            if not matched:
                items.append(
                    BenchmarkPlanItem(
                        benchmark_id=benchmark_id,
                        target_service_id="unavailable-target",
                        definition=definition,
                        dependency_inputs={},
                        available_prerequisites=frozenset(),
                        runnable=False,
                        unavailable_reason=(
                            "The inspected target changed or is no longer available. "
                            "Run inspection again."
                        ),
                    )
                )
        return tuple(
            sorted(items, key=lambda item: (item.benchmark_id, item.target_service_id))
        )

    @staticmethod
    def _resolve_item(
        plan: Sequence[BenchmarkPlanItem],
        *,
        benchmark_id: str,
        target_service_id: str,
    ) -> BenchmarkPlanItem:
        matches = [
            item
            for item in plan
            if item.benchmark_id == benchmark_id
            and item.target_service_id == target_service_id
        ]
        if len(matches) != 1:
            raise FederationValidationError(
                "unknown-benchmark-target",
                "benchmark_id",
                "the benchmark target is not in the current inspection plan",
            )
        return matches[0]

    def list_results(self) -> tuple[BenchmarkResult, ...]:
        if not self.state_database.exists():
            return ()
        try:
            with sqlite3.connect(str(self.state_database), timeout=30) as connection:
                table = connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='benchmark_results'
                    """
                ).fetchone()
        except sqlite3.Error as exc:
            raise FederationValidationError(
                "malformed-benchmark-state",
                "database",
                "benchmark evidence could not be read safely",
            ) from exc
        if table is None:
            return ()
        try:
            return self._result_store().list_results()
        except (sqlite3.Error, ValueError, TypeError) as exc:
            raise FederationValidationError(
                "malformed-benchmark-state",
                "database",
                "benchmark evidence could not be read safely",
            ) from exc

    def run(
        self,
        *,
        benchmark_id: str,
        target_service_id: str,
    ) -> BenchmarkResult:
        device_id, snapshot = self._authorized_snapshot(require_current=True)
        benchmark_id = _bounded_identifier(benchmark_id, "benchmark_id")
        target_service_id = _bounded_identifier(
            target_service_id,
            "target_service_id",
        )
        plan = self.plan(snapshot)
        item = self._resolve_item(
            plan,
            benchmark_id=benchmark_id,
            target_service_id=target_service_id,
        )
        if not item.runnable:
            raise FederationOperationError(
                "benchmark-target-unavailable",
                item.unavailable_reason or "the benchmark target is unavailable",
                "benchmark_id",
            )

        active_key = (device_id, benchmark_id, target_service_id)
        token = CancellationToken()
        with self._lock:
            if active_key in self._active:
                raise FederationOperationError(
                    "benchmark-already-running",
                    "this benchmark target already has an active run",
                    "benchmark_id",
                )
            self._active[active_key] = token

        registry, _adapters = self._components()
        runner = BenchmarkRunner(
            registry,
            self._result_store(),
            now=self._clock,
        )
        run_id = f"benchmark-{secrets.token_urlsafe(24)}"
        try:
            result = runner.run(
                BenchmarkRunRequest(
                    run_id=run_id,
                    device_id=device_id,
                    benchmark_id=benchmark_id,
                    target_service_id=target_service_id,
                    dependency_inputs=item.dependency_inputs,
                    available_prerequisites=item.available_prerequisites,
                    cancellation_token=token,
                )
            )
            self.skip_store.clear(
                device_id=device_id,
                inspection_revision=snapshot.revision,
                benchmark_id=benchmark_id,
                target_service_id=target_service_id,
            )
            return result
        finally:
            with self._lock:
                if self._active.get(active_key) is token:
                    self._active.pop(active_key, None)

    def cancel(
        self,
        *,
        benchmark_id: str,
        target_service_id: str,
    ) -> None:
        device_id, snapshot = self._authorized_snapshot(require_current=True)
        benchmark_id = _bounded_identifier(benchmark_id, "benchmark_id")
        target_service_id = _bounded_identifier(
            target_service_id,
            "target_service_id",
        )
        self._resolve_item(
            self.plan(snapshot),
            benchmark_id=benchmark_id,
            target_service_id=target_service_id,
        )
        active_key = (device_id, benchmark_id, target_service_id)
        with self._lock:
            token = self._active.get(active_key)
        if token is None:
            raise FederationOperationError(
                "benchmark-not-running",
                "no active run exists for this benchmark target",
                "benchmark_id",
            )
        token.cancel()

    def skip_all(self) -> int:
        device_id, snapshot = self._authorized_snapshot(require_current=True)
        plan = tuple(item for item in self.plan(snapshot) if item.runnable)
        latest = self._latest_results(
            self.list_results(),
            device_id=device_id,
        )
        skipped = self.skip_store.list_for_revision(
            device_id=device_id,
            inspection_revision=snapshot.revision,
        )
        with self._lock:
            active_keys = set(self._active)
        items = tuple(
            item
            for item in plan
            if self._card_model(
                item=item,
                result=latest.get(item.key),
                skipped=item.key in skipped,
                active=(device_id, *item.key) in active_keys,
                inspection_current=True,
            )["state"]
            in _SKIPPABLE_REVIEW_STATES
        )
        return self.skip_store.skip_many(
            device_id=device_id,
            inspection_revision=snapshot.revision,
            items=items,
            skipped_at=self._now(),
        )

    @staticmethod
    def _humanize(value: str) -> str:
        return " ".join(
            part for part in value.replace("_", "-").split("-") if part
        ).title()

    @classmethod
    def _format_value(cls, value: Any) -> str:
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if isinstance(value, float):
            return f"{value:.3f}".rstrip("0").rstrip(".")
        if isinstance(value, Mapping):
            return "; ".join(
                f"{cls._humanize(str(key))}: {cls._format_value(item)}"
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            )
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return ", ".join(cls._format_value(item) for item in value)
        return str(value)

    @staticmethod
    def _capability_label(definition: BenchmarkDefinition) -> str:
        labels = {
            "language-model": "Language model",
            "recorder": "Recorder",
            "compute": "Compute",
            "storage": "Storage",
            "network-path": "Network",
        }
        return labels.get(
            definition.capability_type,
            CapabilityBenchmarkService._humanize(definition.capability_type),
        )

    @staticmethod
    def _benchmark_label(definition: BenchmarkDefinition) -> str:
        labels = {
            "benchmark.ai.ollama-inference.v1": "Interactive response check",
            "benchmark.recorder.mtconnect-source.v1": "MTConnect source suitability",
            "benchmark.compute.registered-handler.v1": "Registered handler check",
            "benchmark.storage.candidate-roundtrip.v1": "Durable round-trip check",
            "benchmark.network.authenticated-path.v1": "Authenticated path check",
        }
        return labels.get(
            definition.benchmark_id,
            CapabilityBenchmarkService._humanize(definition.benchmark_id),
        )

    @staticmethod
    def _recommendation_label(result: BenchmarkResult) -> str:
        labels = {
            BenchmarkRecommendation.RECOMMENDED: "Recommended",
            BenchmarkRecommendation.USABLE: "Usable",
            BenchmarkRecommendation.NOT_RECOMMENDED: "Not recommended",
            BenchmarkRecommendation.UNSUPPORTED: "Unsupported",
            BenchmarkRecommendation.RERUN_REQUIRED: "Rerun required",
        }
        return labels[result.recommendation]

    @staticmethod
    def _latest_results(
        results: Sequence[BenchmarkResult],
        *,
        device_id: str,
    ) -> dict[tuple[str, str], BenchmarkResult]:
        latest: dict[tuple[str, str], BenchmarkResult] = {}
        for result in results:
            if result.device_id != device_id:
                continue
            key = (result.benchmark_id, result.target_service_id)
            current = latest.get(key)
            if current is None or (
                result.finished_at,
                result.run_id,
            ) > (
                current.finished_at,
                current.run_id,
            ):
                latest[key] = result
        return latest

    def _card_model(
        self,
        *,
        item: BenchmarkPlanItem,
        result: BenchmarkResult | None,
        skipped: bool,
        active: bool,
        inspection_current: bool,
    ) -> dict[str, object]:
        state = "pending"
        state_label = "Ready"
        summary = "This bounded local check has not been run."
        diagnostic = item.unavailable_reason
        metrics: list[dict[str, str]] = []
        expires_label = None
        action_label = "Run check"

        if not item.runnable:
            state = "blocked"
            state_label = "Inspect again"
            summary = item.unavailable_reason or "The benchmark target is unavailable."
        elif result is not None:
            validity = evaluate_result(
                result,
                item.definition,
                item.dependency_inputs,
                now=self._now(),
            )
            if not validity.current:
                state = (
                    "expired"
                    if validity.reason is BenchmarkValidityReason.EXPIRED
                    else "stale"
                )
                state_label = (
                    "Expired"
                    if validity.reason is BenchmarkValidityReason.EXPIRED
                    else "Changed"
                )
                summary = (
                    "The previous result is too old to support a current recommendation."
                    if validity.reason is BenchmarkValidityReason.EXPIRED
                    else "The benchmark definition or local dependency changed."
                )
                diagnostic = "Rerun this check before using its recommendation."
                action_label = "Rerun check"
            else:
                state = result.state.value
                state_label = self._recommendation_label(result)
                summaries = {
                    BenchmarkState.PASSED: "The bounded local check completed successfully.",
                    BenchmarkState.FAILED: "The bounded local check did not support activation.",
                    BenchmarkState.SKIPPED: "The runner skipped this check safely.",
                    BenchmarkState.CANCELLED: "The benchmark was cancelled safely.",
                    BenchmarkState.EXPIRED: "The result has expired.",
                }
                summary = summaries[result.state]
                diagnostic = result.diagnostics[0] if result.diagnostics else None
                metrics = [
                    {
                        "label": self._humanize(str(key)),
                        "value": self._format_value(value),
                    }
                    for key, value in sorted(result.metrics.items())
                ]
                expires_label = (
                    "Current until "
                    + result.expires_at.astimezone(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M UTC"
                    )
                )
                action_label = "Run again"
        elif skipped:
            state = "skipped"
            state_label = "Skipped"
            summary = (
                "This optional benchmark was skipped. No contribution was enabled."
            )
            action_label = "Run check"

        if active:
            state = "running"
            state_label = "Running"
            summary = "The bounded local check is currently running."
            action_label = "Running"

        digest = hashlib.sha256(
            f"{item.benchmark_id}\0{item.target_service_id}".encode()
        ).hexdigest()[:12]
        return {
            "id": f"{item.benchmark_id}-{digest}",
            "benchmark_id": item.benchmark_id,
            "target_service_id": item.target_service_id,
            "capability_label": self._capability_label(item.definition),
            "label": self._benchmark_label(item.definition),
            "target_label": self._humanize(item.target_service_id),
            "state": state,
            "state_label": state_label,
            "summary": summary,
            "metrics": metrics,
            "diagnostic": diagnostic,
            "action_url": "/onboarding/benchmarks/run",
            "cancel_url": "/onboarding/benchmarks/cancel",
            "action_label": action_label,
            "can_run": item.runnable and inspection_current and not active,
            "can_cancel": active,
            "expires_label": expires_label,
        }

    def view_model(
        self,
        snapshot: DeviceInspectionSnapshot | None,
        *,
        connected: bool,
        error_reason: str | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]], bool]:
        if error_reason:
            return (
                {
                    "state": "degraded",
                    "label": "Benchmark evidence unavailable",
                    "can_skip": False,
                },
                [],
                False,
            )
        if not connected:
            return (
                {
                    "state": "blocked",
                    "label": "Connect a federation first",
                    "can_skip": False,
                },
                [],
                False,
            )
        if snapshot is None:
            return (
                {
                    "state": "blocked",
                    "label": "Run inspection first",
                    "can_skip": False,
                },
                [],
                False,
            )

        inspection_current = self.inspection_service.state(snapshot) == "current"
        plan = self.plan(snapshot)
        latest = self._latest_results(
            self.list_results(),
            device_id=snapshot.device_id,
        )
        skipped = self.skip_store.list_for_revision(
            device_id=snapshot.device_id,
            inspection_revision=snapshot.revision,
        )
        with self._lock:
            active_keys = set(self._active)
        cards = [
            self._card_model(
                item=item,
                result=latest.get(item.key),
                skipped=item.key in skipped,
                active=(snapshot.device_id, *item.key) in active_keys,
                inspection_current=inspection_current,
            )
            for item in plan
        ]

        if not inspection_current:
            summary = {
                "state": "blocked",
                "label": "Inspection expired",
                "can_skip": False,
            }
            return summary, cards, False
        reviewed_states = {
            "passed",
            "failed",
            "cancelled",
            "skipped",
        }
        reviewed = sum(card["state"] in reviewed_states for card in cards)
        complete = not cards or reviewed == len(cards)
        pending = len(cards) - reviewed
        summary = {
            "state": "complete" if complete else "pending",
            "label": (
                "All recommended checks reviewed"
                if complete
                else f"{pending} check{'s' if pending != 1 else ''} need review"
            ),
            "can_skip": any(
                card["state"] in _SKIPPABLE_REVIEW_STATES for card in cards
            ),
        }
        return summary, cards, complete

    def apply_to_onboarding_view_model(
        self,
        view_model: dict[str, object],
        *,
        snapshot: DeviceInspectionSnapshot | None,
        connected: bool,
        requested_step: str | None,
        error_reason: str | None = None,
    ) -> dict[str, object]:
        summary, cards, complete = self.view_model(
            snapshot,
            connected=connected,
            error_reason=error_reason,
        )
        view_model["benchmark_summary"] = summary
        view_model["benchmarks"] = cards
        view_model["page_intro"] = (
            "Inspect this device, then run or skip bounded local suitability checks. "
            "Benchmark evidence never enables a contribution by itself."
        )
        storage_key = str(view_model.get("storage_key") or "")
        view_model["storage_key"] = storage_key.replace(".cfi3", ".cfi4")

        inspection_current = (
            snapshot is not None
            and self.inspection_service.state(snapshot) == "current"
            and not error_reason
        )
        steps = view_model.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, dict) or step.get("key") != "benchmarks":
                    continue
                step["available"] = connected and inspection_current
                step["summary"] = str(summary["label"])

        completed = view_model.get("completed_steps")
        if isinstance(completed, list):
            while "benchmarks" in completed:
                completed.remove("benchmarks")
            if complete and connected and inspection_current:
                completed.append("benchmarks")

        if connected and inspection_current and requested_step in {None, "benchmarks"}:
            view_model["current_step"] = "benchmarks"

        if isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict) and step.get("key") == "contributions":
                    step["available"] = False
                    step["summary"] = "Separate integration required"

        finish = view_model.get("finish")
        if isinstance(finish, dict):
            finish["title"] = "Benchmarks integrated; contributions remain blocked"
            finish["message"] = (
                "CFI-4 adds bounded benchmark run, skip, cancel, expiry, "
                "invalidation, and rerun behavior. Contribution activation still "
                "requires a separate reviewed integration."
            )
        view_model["progress_revision"] = max(
            int(view_model.get("progress_revision") or 1),
            (snapshot.revision + 1) if snapshot is not None else 1,
        )
        return view_model


def get_capability_benchmark_service() -> CapabilityBenchmarkService:
    configured = current_app.config.get("CAPABILITY_BENCHMARK_SERVICE")
    if isinstance(configured, CapabilityBenchmarkService):
        return configured
    existing = current_app.extensions.get(_SERVICE_EXTENSION_KEY)
    if isinstance(existing, CapabilityBenchmarkService):
        return existing

    onboarding = get_capability_onboarding_service()
    inspection = get_capability_inspection_service()
    service = CapabilityBenchmarkService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        state_database=current_app.config.get(
            "CAPABILITY_ONBOARDING_BENCHMARK_DATABASE",
            current_app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"],
        ),
        clock=getattr(onboarding, "_clock", _utc_now),
    )
    current_app.extensions[_SERVICE_EXTENSION_KEY] = service
    return service


__all__ = [
    "BenchmarkPlanItem",
    "BenchmarkSkipStore",
    "CapabilityBenchmarkService",
    "get_capability_benchmark_service",
]
